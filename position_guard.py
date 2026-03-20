"""
PositionGuard — Real-time position monitor with WebSocket + pre-signed emergency sell.

Monitors open Solana token positions across four detection layers and maintains
a ready-to-fire pre-signed sell transaction for instant rug-pull response.
"""

import asyncio
import json
import time
import logging
import base64
from typing import Optional, Callable, Dict, List, Deque
from collections import deque
from dataclasses import dataclass, field

import aiohttp
import base58
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction

logger = logging.getLogger("position_guard")


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class PricePoint:
    timestamp: float
    price: float


@dataclass
class WatchedPosition:
    trade_id: int
    token_address: str
    pool_sol_vault: str
    deployer_address: str
    tokens_held: int
    entry_sol_balance: float          # lamports snapshot at entry
    entry_deployer_sig: Optional[str] = None
    # runtime
    tasks: List[asyncio.Task] = field(default_factory=list)
    price_history: Deque[PricePoint] = field(default_factory=lambda: deque(maxlen=300))
    pre_signed_tx: Optional[bytes] = None
    emergency_fired: bool = False
    stopped: bool = False


# ---------------------------------------------------------------------------
# PositionGuard
# ---------------------------------------------------------------------------

class PositionGuard:
    """Monitors positions and fires emergency sells on rug detection."""

    # SOL mint for Jupiter quotes
    SOL_MINT = "So11111111111111111111111111111111111111112"

    def __init__(
        self,
        helius_rpc: str,
        helius_ws: str,
        public_rpc: str,
        jupiter_quote_url: str,
        jupiter_swap_url: str,
        jupiter_api_key: str,
        keypair: Keypair,
        wallet_pubkey: str,
    ):
        self.helius_rpc = helius_rpc
        self.helius_ws = helius_ws
        self.public_rpc = public_rpc
        self.jupiter_quote_url = jupiter_quote_url.rstrip("/")
        self.jupiter_swap_url = jupiter_swap_url.rstrip("/")
        self.jupiter_api_key = jupiter_api_key
        self.keypair = keypair
        self.wallet_pubkey = wallet_pubkey

        self._positions: Dict[int, WatchedPosition] = {}
        self._emergency_callback: Optional[Callable] = None
        self._http: Optional[aiohttp.ClientSession] = None
        self._lock = asyncio.Lock()  # guards pre_signed_tx access

        # Check websockets availability for Layer 1
        try:
            import websockets
            self._ws_available = True
        except ImportError:
            logger.warning("websockets package not installed — Layer 1 (WebSocket monitoring) disabled")
            self._ws_available = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def set_emergency_callback(self, callback: Callable):
        """Set callback(trade_id, reason) invoked on rug detection."""
        self._emergency_callback = callback

    async def start_watching(
        self,
        trade_id: int,
        token_address: str,
        pool_sol_vault: str,
        deployer_address: str,
        tokens_held: int,
        entry_sol_balance: float,
    ):
        """Begin monitoring a position across all four layers."""
        if trade_id in self._positions:
            logger.warning("Trade %s already watched — ignoring duplicate", trade_id)
            return

        pos = WatchedPosition(
            trade_id=trade_id,
            token_address=token_address,
            pool_sol_vault=pool_sol_vault,
            deployer_address=deployer_address,
            tokens_held=tokens_held,
            entry_sol_balance=entry_sol_balance,
        )
        self._positions[trade_id] = pos

        # Snapshot deployer's latest signature at entry
        try:
            pos.entry_deployer_sig = await self._get_latest_deployer_sig(deployer_address)
        except Exception as exc:
            logger.error("Failed to snapshot deployer sig for trade %s: %s", trade_id, exc)

        # Spawn monitoring layers as tasks
        tasks = []
        if self._ws_available and pos.pool_sol_vault:
            tasks.append(asyncio.create_task(self._layer1_ws_pool(pos), name=f"L1-ws-{trade_id}"))
        # Layer 2 is driven externally via update_price()
        tasks.append(asyncio.create_task(self._layer3_deployer_watch(pos), name=f"L3-deployer-{trade_id}"))
        tasks.append(asyncio.create_task(self._layer4_pool_poll(pos), name=f"L4-poll-{trade_id}"))
        tasks.append(asyncio.create_task(self._presign_loop(pos), name=f"presign-{trade_id}"))
        pos.tasks = tasks
        logger.info("Started watching trade %s (%s)", trade_id, token_address)

    async def stop_watching(self, trade_id: int):
        """Stop monitoring a position and clean up tasks."""
        pos = self._positions.pop(trade_id, None)
        if pos is None:
            return
        pos.stopped = True
        for task in pos.tasks:
            task.cancel()
        # Wait for cancellation (suppress CancelledError)
        await asyncio.gather(*pos.tasks, return_exceptions=True)
        logger.info("Stopped watching trade %s", trade_id)

    async def stop_all(self):
        """Stop all position watchers."""
        for trade_id in list(self._positions.keys()):
            await self.stop_watching(trade_id)

    async def get_pre_signed_sell(self, trade_id: int) -> Optional[bytes]:
        """Return the latest pre-signed sell TX bytes, ready for sendTransaction."""
        pos = self._positions.get(trade_id)
        if pos is None:
            return None
        async with self._lock:
            return pos.pre_signed_tx

    async def update_price(self, trade_id: int, current_price: float):
        """Layer 2: called by external price feed every ~400ms."""
        pos = self._positions.get(trade_id)
        if pos is None or pos.stopped or pos.emergency_fired:
            return

        now = time.monotonic()
        pos.price_history.append(PricePoint(timestamp=now, price=current_price))

        # Check 10-second window (>8% drop)
        if self._check_velocity(pos, window_seconds=10, threshold_pct=8.0):
            await self._fire_emergency(pos, "Price dropped >8% in 10s")
            return

        # Check 30-second window (>15% drop)
        if self._check_velocity(pos, window_seconds=30, threshold_pct=15.0):
            await self._fire_emergency(pos, "Price dropped >15% in 30s")

    # ------------------------------------------------------------------
    # HTTP session helper
    # ------------------------------------------------------------------

    async def _get_http(self) -> aiohttp.ClientSession:
        if self._http is None or self._http.closed:
            self._http = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10)
            )
        return self._http

    async def close(self):
        """Gracefully close the HTTP session."""
        if self._http and not self._http.closed:
            await self._http.close()

    # ------------------------------------------------------------------
    # Layer 1: WebSocket pool SOL vault monitor
    # ------------------------------------------------------------------

    async def _layer1_ws_pool(self, pos: WatchedPosition):
        """Subscribe to accountChange on pool_sol_vault via Helius WS."""
        import websockets  # lazy import

        sub_id: Optional[int] = None
        request_id = 1

        while not pos.stopped:
            try:
                async with websockets.connect(
                    self.helius_ws,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    # Subscribe
                    subscribe_msg = json.dumps({
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "method": "accountSubscribe",
                        "params": [
                            pos.pool_sol_vault,
                            {"encoding": "jsonParsed", "commitment": "confirmed"},
                        ],
                    })
                    await ws.send(subscribe_msg)
                    logger.info("L1 WS subscribed to %s for trade %s", pos.pool_sol_vault, pos.trade_id)

                    async for raw_msg in ws:
                        if pos.stopped:
                            break
                        try:
                            data = json.loads(raw_msg)
                        except json.JSONDecodeError:
                            continue

                        # Subscription confirmation
                        if "result" in data and data.get("id") == request_id:
                            sub_id = data["result"]
                            continue

                        # Notification
                        params = data.get("params")
                        if params is None:
                            continue
                        value = params.get("result", {}).get("value", {})
                        lamports = value.get("lamports")
                        if lamports is None:
                            continue

                        drop_pct = (1 - lamports / pos.entry_sol_balance) * 100 if pos.entry_sol_balance > 0 else 0
                        if drop_pct > 10:
                            await self._fire_emergency(
                                pos,
                                f"L1 WS: Pool SOL dropped {drop_pct:.1f}% ({lamports} lamports vs {pos.entry_sol_balance:.0f} entry)",
                            )
                            return

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("L1 WS error for trade %s: %s — reconnecting in 2s", pos.trade_id, exc)
                await asyncio.sleep(2)

    # ------------------------------------------------------------------
    # Layer 3: Deployer wallet watch (every 5s)
    # ------------------------------------------------------------------

    async def _layer3_deployer_watch(self, pos: WatchedPosition):
        """Poll deployer for new signatures; flag potential sells."""
        while not pos.stopped:
            try:
                await asyncio.sleep(5)
                if pos.stopped or pos.emergency_fired:
                    return

                http = await self._get_http()
                payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getSignaturesForAddress",
                    "params": [
                        pos.deployer_address,
                        {"limit": 5, "commitment": "confirmed"},
                    ],
                }
                async with http.post(self.public_rpc, json=payload) as resp:
                    result = (await resp.json()).get("result", [])

                if not result:
                    continue

                latest_sig = result[0].get("signature")
                if pos.entry_deployer_sig is None:
                    # First fetch — just record and continue
                    pos.entry_deployer_sig = latest_sig
                    continue

                if latest_sig == pos.entry_deployer_sig:
                    continue  # no new activity

                # New signatures detected — check for sell indicators
                new_sigs = []
                for item in result:
                    sig = item.get("signature")
                    if sig == pos.entry_deployer_sig:
                        break
                    new_sigs.append(sig)

                if new_sigs:
                    # Update baseline to latest
                    pos.entry_deployer_sig = latest_sig

                    # Check if any new txs involve token program interactions (potential sells)
                    is_sell = await self._check_deployer_sells(new_sigs, pos)
                    if is_sell:
                        await self._fire_emergency(
                            pos,
                            f"L3: Deployer new activity detected — {len(new_sigs)} new tx(s), sell patterns found",
                        )

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("L3 deployer watch error for trade %s: %s", pos.trade_id, exc)

    async def _check_deployer_sells(self, signatures: List[str], pos: WatchedPosition) -> bool:
        """Check if any of the deployer's new transactions look like sells."""
        try:
            http = await self._get_http()
            for sig in signatures[:3]:  # check up to 3
                payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getTransaction",
                    "params": [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}],
                }
                async with http.post(self.helius_rpc, json=payload) as resp:
                    data = await resp.json()

                tx_result = data.get("result")
                if tx_result is None:
                    continue

                # Look for token balance changes involving the watched token
                meta = tx_result.get("meta", {})
                pre_balances = meta.get("preTokenBalances", [])
                post_balances = meta.get("postTokenBalances", [])

                for pre in pre_balances:
                    if pre.get("mint") == pos.token_address:
                        pre_amount = float(pre.get("uiTokenAmount", {}).get("uiAmount", 0) or 0)
                        # Find matching post balance
                        for post in post_balances:
                            if (post.get("mint") == pos.token_address and
                                    post.get("accountIndex") == pre.get("accountIndex")):
                                post_amount = float(post.get("uiTokenAmount", {}).get("uiAmount", 0) or 0)
                                if pre_amount > 0 and post_amount < pre_amount * 0.5:
                                    return True  # significant token reduction = sell
                return False
        except Exception as exc:
            logger.error("Error checking deployer sells: %s", exc)
            # On error, treat new deployer activity as suspicious
            return True

    async def _get_latest_deployer_sig(self, deployer_address: str) -> Optional[str]:
        """Get the latest transaction signature for the deployer wallet."""
        http = await self._get_http()
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignaturesForAddress",
            "params": [deployer_address, {"limit": 1, "commitment": "confirmed"}],
        }
        async with http.post(self.public_rpc, json=payload) as resp:
            result = (await resp.json()).get("result", [])
        if result:
            return result[0].get("signature")
        return None

    # ------------------------------------------------------------------
    # Layer 4: Pool SOL polling backup (every 3s)
    # ------------------------------------------------------------------

    async def _layer4_pool_poll(self, pos: WatchedPosition):
        """Backup polling of pool SOL vault balance via public RPC."""
        while not pos.stopped:
            try:
                await asyncio.sleep(3)
                if pos.stopped or pos.emergency_fired:
                    return

                http = await self._get_http()
                payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getBalance",
                    "params": [pos.pool_sol_vault, {"commitment": "confirmed"}],
                }
                async with http.post(self.public_rpc, json=payload) as resp:
                    data = await resp.json()

                lamports = data.get("result", {}).get("value", 0)
                if pos.entry_sol_balance <= 0:
                    continue

                drop_pct = (1 - lamports / pos.entry_sol_balance) * 100
                if drop_pct > 10:
                    await self._fire_emergency(
                        pos,
                        f"L4 Poll: Pool SOL dropped {drop_pct:.1f}% ({lamports} lamports vs {pos.entry_sol_balance:.0f} entry)",
                    )
                    return

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("L4 pool poll error for trade %s: %s", pos.trade_id, exc)

    # ------------------------------------------------------------------
    # Price velocity (Layer 2 helper)
    # ------------------------------------------------------------------

    @staticmethod
    def _check_velocity(pos: WatchedPosition, window_seconds: float, threshold_pct: float) -> bool:
        """Check if price dropped more than threshold_pct within window_seconds."""
        if len(pos.price_history) < 2:
            return False

        now = pos.price_history[-1].timestamp
        current_price = pos.price_history[-1].price
        cutoff = now - window_seconds

        # Find the highest price within the window
        max_price = 0.0
        for pp in pos.price_history:
            if pp.timestamp >= cutoff:
                max_price = max(max_price, pp.price)

        if max_price <= 0:
            return False

        drop_pct = (1 - current_price / max_price) * 100
        return drop_pct > threshold_pct

    # ------------------------------------------------------------------
    # Emergency fire
    # ------------------------------------------------------------------

    async def _fire_emergency(self, pos: WatchedPosition, reason: str):
        """Fire the emergency callback exactly once per position."""
        if pos.emergency_fired or pos.stopped:
            return
        pos.emergency_fired = True
        logger.critical("🚨 EMERGENCY for trade %s: %s", pos.trade_id, reason)

        if self._emergency_callback:
            try:
                result = self._emergency_callback(pos.trade_id, reason)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as exc:
                logger.error("Emergency callback error for trade %s: %s", pos.trade_id, exc)

    # ------------------------------------------------------------------
    # Pre-signed sell TX loop
    # ------------------------------------------------------------------

    async def _presign_loop(self, pos: WatchedPosition):
        """Every 15s, refresh a pre-signed sell TX ready for instant firing."""
        while not pos.stopped:
            try:
                await asyncio.sleep(15)
                if pos.stopped or pos.emergency_fired:
                    return

                tx_bytes = await self._build_presigned_sell(pos)
                if tx_bytes:
                    async with self._lock:
                        pos.pre_signed_tx = tx_bytes
                    logger.debug("Pre-signed sell refreshed for trade %s (%d bytes)", pos.trade_id, len(tx_bytes))

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("Presign loop error for trade %s: %s", pos.trade_id, exc)

    async def _build_presigned_sell(self, pos: WatchedPosition) -> Optional[bytes]:
        """Build and sign a sell TX via Jupiter."""
        http = await self._get_http()
        headers = {}
        if self.jupiter_api_key:
            headers["Authorization"] = f"Bearer {self.jupiter_api_key}"

        # Step 1: Get quote (token → SOL, 50% slippage = 5000 bps for emergency)
        quote_params = {
            "inputMint": pos.token_address,
            "outputMint": self.SOL_MINT,
            "amount": str(pos.tokens_held),
            "slippageBps": "5000",
            "onlyDirectRoutes": "false",
        }
        async with http.get(
            self.jupiter_quote_url,
            params=quote_params,
            headers=headers,
        ) as resp:
            if resp.status != 200:
                logger.warning("Jupiter quote failed (%s) for trade %s", resp.status, pos.trade_id)
                return None
            quote = await resp.json()

        # Step 2: Get swap transaction
        swap_body = {
            "quoteResponse": quote,
            "userPublicKey": self.wallet_pubkey,
            "wrapAndUnwrapSol": True,
            "dynamicComputeUnitLimit": True,
            "prioritizationFeeLamports": "auto",
        }
        async with http.post(
            self.jupiter_swap_url,
            json=swap_body,
            headers=headers,
        ) as resp:
            if resp.status != 200:
                logger.warning("Jupiter swap failed (%s) for trade %s", resp.status, pos.trade_id)
                return None
            swap_data = await resp.json()

        swap_tx_b64 = swap_data.get("swapTransaction")
        if not swap_tx_b64:
            logger.warning("No swapTransaction in Jupiter response for trade %s", pos.trade_id)
            return None

        # Step 3: Decode, sign, serialize
        try:
            raw_tx = base64.b64decode(swap_tx_b64)
            tx = VersionedTransaction.from_bytes(raw_tx)

            # Sign the transaction
            signed_tx = VersionedTransaction(tx.message, [self.keypair])
            signed_bytes = bytes(signed_tx)
            return signed_bytes
        except Exception as exc:
            logger.error("TX sign error for trade %s: %s", pos.trade_id, exc)
            return None

    # ------------------------------------------------------------------
    # Utility: send pre-signed TX
    # ------------------------------------------------------------------

    async def send_emergency_sell(self, trade_id: int) -> Optional[str]:
        """Send the cached pre-signed sell TX. Returns signature or None."""
        tx_bytes = await self.get_pre_signed_sell(trade_id)
        if tx_bytes is None:
            logger.error("No pre-signed TX available for trade %s", trade_id)
            return None

        try:
            http = await self._get_http()
            tx_b64 = base64.b64encode(tx_bytes).decode("ascii")
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "sendTransaction",
                "params": [
                    tx_b64,
                    {
                        "encoding": "base64",
                        "skipPreflight": True,
                        "maxRetries": 3,
                        "preflightCommitment": "confirmed",
                    },
                ],
            }
            async with http.post(self.helius_rpc, json=payload) as resp:
                data = await resp.json()

            if "result" in data:
                sig = data["result"]
                logger.info("Emergency sell TX sent for trade %s: %s", trade_id, sig)
                return sig
            else:
                error = data.get("error", {})
                logger.error("Emergency sell TX failed for trade %s: %s", trade_id, error)
                return None
        except Exception as exc:
            logger.error("Error sending emergency sell for trade %s: %s", trade_id, exc)
            return None
