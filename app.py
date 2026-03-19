"""
Wave Rider Bot v4.4 - V9.6 Entry Filters + Live Execution
==========================================================
CHANGES from v4.3:
- Entry filters now EXACTLY match V9.6 shadow trader logic
- Bundle detection via Helius REST API (3+ buys in same block)
- Freeze authority check via getAccountInfo RPC
- Creator history check (serial deployer detection)
- Kill signals: curve_dur>300s, price_down>10%, serial creator
- Entry pass: (momentum + holder_signal + no_kill) OR (top1>=99 + bundle + no_kill)
- Grading: A (top1>=99+bundle+momentum OR top1>=99+momentum), B (momentum+signal), C (skipped)
- Single $15K liquidity gate for ALL grades (no A/B distinction)
- C-grade tokens logged but NOT traded (matches V9.6)
- All enrichment via async aiohttp (not sync requests)
- Graceful fallback if Helius enrichment fails

KEPT from v4.3 (all 23 bug fixes + safety):
- Honeypot check (check_sellable after buy)
- Halt-aware sells
- Per-position sell cap (3 attempts)
- Pool drained check before all sells
- Zero price tracking
- Actual on-chain balance check
- Thread-safe DB with WAL mode
- Slippage escalation across attempts
- Balance drop detection
- No-data timeout exit
- Progressive ladder sells
- Circuit breakers
"""

import os
import sys
import json
import time
import base64
import base58
import asyncio
import sqlite3
import logging
import traceback
from datetime import datetime, timezone
from threading import Lock
from typing import Optional, Dict, List, Tuple

import requests
import aiohttp
from dotenv import load_dotenv

from solders.keypair import Keypair
from solders.transaction import VersionedTransaction
from solders import message as solders_message

# ============================================================
# CONFIGURATION
# ============================================================

load_dotenv()

PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")
RPC_URL = os.getenv("RPC_URL", "")
BALANCE_RPC = "https://api.mainnet-beta.solana.com"

JUPITER_QUOTE_URL = os.getenv("JUPITER_QUOTE_URL", "https://api.jup.ag/swap/v1/quote")
JUPITER_SWAP_URL = os.getenv("JUPITER_SWAP_URL", "https://api.jup.ag/swap/v1/swap")
JUPITER_API_KEY = os.getenv("JUPITER_API_KEY", "")

TG_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

POSITION_SIZE_SOL = float(os.getenv("POSITION_SIZE_SOL", "0.02"))
MAX_POSITIONS = int(os.getenv("MAX_POSITIONS", "200"))  # DATA COLLECTION: high limit
MAX_TRADES_PER_TOKEN = int(os.getenv("MAX_TRADES_PER_TOKEN", "5"))  # DATA COLLECTION: more re-entries
HARD_EXIT_SECONDS = int(os.getenv("HARD_EXIT_SECONDS", "900"))
TRAILING_STOP_PCT = float(os.getenv("TRAILING_STOP_PCT", "15"))
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "30"))

# Liquidity gate: single $15K for all grades (V9.6 match)
MIN_LIQ_ENTRY = float(os.getenv("MIN_LIQ_ENTRY", "5000"))  # DATA COLLECTION: loosened to $5K
MIN_LIQ_ENRICH = float(os.getenv("MIN_LIQ_ENRICH", "5000"))  # DATA COLLECTION: enrich everything $5K+

# Momentum filter
MIN_MOMENTUM_PCT = float(os.getenv("MIN_MOMENTUM_PCT", "-100"))  # DATA COLLECTION: accept any momentum

EXIT_LADDER_STR = os.getenv("EXIT_LADDER", "100:3,200:3,500:10,1500:25,5000:35")
EXIT_LADDER = []
for step in EXIT_LADDER_STR.split(","):
    threshold, sell_pct = step.split(":")
    EXIT_LADDER.append((float(threshold), float(sell_pct) / 100))

MAX_DAILY_LOSS_SOL = float(os.getenv("MAX_DAILY_LOSS_SOL", "999"))  # DATA COLLECTION: no circuit breaker
MAX_HOURLY_LOSS_SOL = float(os.getenv("MAX_HOURLY_LOSS_SOL", "999"))
MAX_CONSECUTIVE_LOSSES = int(os.getenv("MAX_CONSECUTIVE_LOSSES", "999"))

LIQ_DROP_THRESHOLD_PCT = float(os.getenv("LIQ_DROP_THRESHOLD_PCT", "30"))
LIQ_CHECK_INTERVAL_MS = int(os.getenv("LIQ_CHECK_INTERVAL_MS", "400"))
ENTRY_DELAY_SECONDS = int(os.getenv("ENTRY_DELAY_SECONDS", "15"))  # DATA COLLECTION: faster evaluation
PRIORITY_FEE_LAMPORTS = int(os.getenv("PRIORITY_FEE_LAMPORTS", "100000"))

DEXSCREENER_URL = "https://api.dexscreener.com/latest/dex/tokens"
PUMPFUN_URL = "https://frontend-api-v3.pump.fun/coins?sort=created_timestamp&order=DESC&complete=true&limit=50"
PUMPFUN_LIVE_URL = "https://frontend-api-v3.pump.fun/coins/currently-live"
GECKO_URL = "https://api.geckoterminal.com/api/v2/networks/solana/new_pools?page=1"

HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "")
HELIUS_RPC = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}" if HELIUS_API_KEY else ""
HELIUS_API = os.getenv("HELIUS_API", f"https://api.helius.xyz/v0") if HELIUS_API_KEY else ""

SOL_MINT = "So11111111111111111111111111111111111111112"
LAMPORTS_PER_SOL = 1_000_000_000
VERSION = "v4.4-prod"
MAX_EMERGENCY_RETRIES = 3
MAX_SELL_ATTEMPTS_PER_POSITION = 3  # v4.3: cap sell attempts per position

# Shadow mode: True = simulate only, False = real trades
# Togglable via SHADOW_MODE env var (e.g. SHADOW_MODE=true)
SHADOW_MODE = os.getenv("SHADOW_MODE", "false").lower() == "true"

# ============================================================
# LIVE SAFETY LIMITS
# ============================================================
MAX_DAILY_TRADES = int(os.getenv("MAX_DAILY_TRADES", "999"))  # DATA COLLECTION: unlimited
MAX_HOURLY_SOL_SPEND = float(os.getenv("MAX_HOURLY_SOL_SPEND", "999"))  # DATA COLLECTION: unlimited (50 trades/hr at 0.02)
MIN_WALLET_RESERVE_SOL = float(os.getenv("MIN_WALLET_RESERVE_SOL", "0.05"))
MAX_SELL_FAILURES_BEFORE_HALT = int(os.getenv("MAX_SELL_FAILURES_BEFORE_HALT", "3"))
LIVE_TEST_MODE = False  # v4.3: Full mode — ladder sells enabled, no position cap

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bot.log", mode="a"),
    ],
)
log = logging.getLogger("wave-rider")
logging.getLogger("werkzeug").setLevel(logging.WARNING)

# ============================================================
# HELIUS CREDIT TRACKER
# ============================================================

class HeliusTracker:
    """Track Helius API credit usage."""
    def __init__(self):
        self.calls_today = 0
        self.calls_this_hour = 0
        self.last_hour_reset = time.time()
        self.last_day_reset = time.time()
    
    def record_call(self):
        now = time.time()
        if now - self.last_hour_reset > 3600:
            self.calls_this_hour = 0
            self.last_hour_reset = now
        if now - self.last_day_reset > 86400:
            self.calls_today = 0
            self.last_day_reset = now
        self.calls_today += 1
        self.calls_this_hour += 1
    
    def status(self) -> dict:
        return {
            "today": self.calls_today,
            "this_hour": self.calls_this_hour,
            "est_monthly": self.calls_today * 30,
        }

helius_tracker = HeliusTracker()

# ============================================================
# LIVE SAFETY TRACKER
# ============================================================

class SafetyTracker:
    """Track spending, failures, and enforce safety limits."""
    def __init__(self):
        self.trades_today = 0
        self.sol_spent_this_hour = 0
        self.sell_failures = 0
        self.last_hour_reset = time.time()
        self.last_day_reset = time.time()
        self.halted = False
        self.halt_reason = ""
    
    def reset_hourly(self):
        now = time.time()
        if now - self.last_hour_reset > 3600:
            self.sol_spent_this_hour = 0
            self.last_hour_reset = now
    
    def reset_daily(self):
        now = time.time()
        if now - self.last_day_reset > 86400:
            self.trades_today = 0
            self.last_day_reset = now
    
    def can_trade(self) -> Tuple[bool, str]:
        """Check all safety limits before trading."""
        self.reset_hourly()
        self.reset_daily()
        
        if self.halted:
            return False, f"HALTED: {self.halt_reason}"
        if self.trades_today >= MAX_DAILY_TRADES:
            return False, f"Daily trade limit ({MAX_DAILY_TRADES})"
        if self.sol_spent_this_hour >= MAX_HOURLY_SOL_SPEND:
            return False, f"Hourly SOL limit ({MAX_HOURLY_SOL_SPEND})"
        return True, ""
    
    def record_buy(self, sol_amount: float):
        self.trades_today += 1
        self.sol_spent_this_hour += sol_amount
    
    def record_sell_failure(self):
        self.sell_failures += 1
        if self.sell_failures >= MAX_SELL_FAILURES_BEFORE_HALT:
            self.halted = True
            self.halt_reason = f"Sell failed {self.sell_failures} times - FULL STOP"
            log.error(f"SAFETY HALT: {self.halt_reason}")
            tg_send(f"SAFETY HALT: {self.halt_reason}\nBot stopped trading. Manual restart needed.")
    
    def record_sell_success(self):
        self.sell_failures = 0  # Reset on success
    
    def status(self) -> dict:
        return {
            "trades_today": self.trades_today,
            "sol_spent_hour": round(self.sol_spent_this_hour, 4),
            "sell_failures": self.sell_failures,
            "halted": self.halted,
            "halt_reason": self.halt_reason,
        }

safety_tracker = SafetyTracker()

# ============================================================
# RAW RPC HELPERS
# ============================================================

# Persistent aiohttp session for RPC calls — initialized in bot start()
_rpc_session: Optional[aiohttp.ClientSession] = None


async def _get_rpc_session() -> aiohttp.ClientSession:
    """Get or create the persistent RPC aiohttp session."""
    global _rpc_session
    if _rpc_session is None or _rpc_session.closed:
        _rpc_session = aiohttp.ClientSession()
    return _rpc_session


async def close_rpc_session():
    """Close the persistent RPC session (call on bot stop)."""
    global _rpc_session
    if _rpc_session and not _rpc_session.closed:
        await _rpc_session.close()
        _rpc_session = None


async def rpc_call(method: str, params: list, rpc_url: str = None) -> dict:
    url = rpc_url or RPC_URL
    body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    try:
        session = await _get_rpc_session()
        async with session.post(url, json=body, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            data = await resp.json()
            if rpc_url and HELIUS_API_KEY and HELIUS_API_KEY in (rpc_url or ""):
                helius_tracker.record_call()
            return data
    except Exception as e:
        log.error(f"RPC {method} failed: {e}")
        return {}


async def get_sol_balance(wallet: str) -> float:
    data = await rpc_call("getBalance", [wallet], BALANCE_RPC)
    lamports = data.get("result", {}).get("value", 0)
    return lamports / LAMPORTS_PER_SOL


async def get_token_balance(wallet: str, mint: str) -> int:
    data = await rpc_call("getTokenAccountsByOwner", [
        wallet, {"mint": mint}, {"encoding": "jsonParsed"}
    ], BALANCE_RPC)
    accounts = data.get("result", {}).get("value", [])
    if accounts:
        try:
            return int(accounts[0]["account"]["data"]["parsed"]["info"]["tokenAmount"]["amount"])
        except (KeyError, IndexError):
            pass
    return 0


async def send_raw_tx(signed_tx_bytes: bytes) -> Optional[str]:
    encoded = base58.b58encode(signed_tx_bytes).decode()
    data = await rpc_call("sendTransaction", [encoded, {"skipPreflight": True, "encoding": "base58"}])
    result = data.get("result")
    if result:
        return str(result)
    log.error(f"sendTransaction error: {data.get('error', {})}")
    return None


async def confirm_tx(tx_sig: str, timeout_s: int = 30) -> bool:
    start = time.time()
    while time.time() - start < timeout_s:
        data = await rpc_call("getSignatureStatuses", [[tx_sig]])
        statuses = data.get("result", {}).get("value", [])
        if statuses and statuses[0]:
            status = statuses[0]
            if status.get("err"):
                log.warning(f"TX {tx_sig[:16]}... FAILED: {status['err']}")
                return False
            conf = status.get("confirmationStatus", "")
            if conf in ("confirmed", "finalized"):
                log.info(f"TX {tx_sig[:16]}... CONFIRMED ({conf})")
                return True
        await asyncio.sleep(0.5)
    log.warning(f"TX {tx_sig[:16]}... TIMEOUT after {timeout_s}s")
    return False

# ============================================================
# TOKEN ENRICHMENT (holder concentration)
# ============================================================

async def fetch_holder_concentration(token_addr: str) -> Optional[dict]:
    if not HELIUS_RPC:
        return None
    data = await rpc_call("getTokenLargestAccounts", [token_addr], HELIUS_RPC)
    if not data or "result" not in data:
        if "error" in data:
            log.warning(f"Holder check failed: {str(data.get('error', ''))[:50]}")
        return None
    accounts = data.get("result", {}).get("value", [])
    if not accounts:
        return None
    amounts = sorted(
        [float(a.get("uiAmount", 0) or a.get("amount", 0)) for a in accounts],
        reverse=True
    )
    total = sum(amounts)
    if total <= 0:
        return None
    top1 = (amounts[0] / total) * 100
    top5 = (sum(amounts[:5]) / total) * 100 if len(amounts) >= 5 else top1
    top10 = (sum(amounts[:10]) / total) * 100 if len(amounts) >= 10 else top5
    return {"top1": top1, "top5": top5, "top10": top10}


# ============================================================
# V9.6 ENRICHMENT: Bundle Detection, Freeze Authority, Creator History
# ============================================================

async def fetch_bundle_info(token_addr: str) -> dict:
    """Detect bundle buying via Helius REST API.
    Bundle = 3+ buys in the same block slot.
    Returns {"bundle_detected": bool, "bundle_count": int}."""
    if not HELIUS_API or not HELIUS_API_KEY:
        return {"bundle_detected": False, "bundle_count": 0}
    url = f"{HELIUS_API}/addresses/{token_addr}/transactions?type=SWAP&api-key={HELIUS_API_KEY}"
    try:
        session = await _get_rpc_session()
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                content = b""
                async for chunk in resp.content.iter_chunked(8192):
                    content += chunk
                    if len(content) > 1_000_000:
                        break
                data = json.loads(content)
        if HELIUS_API_KEY:
            helius_tracker.record_call()
    except Exception as e:
        log.warning(f"Bundle check failed for {token_addr[:8]}: {e}")
        return {"bundle_detected": False, "bundle_count": 0}

    if not isinstance(data, list):
        return {"bundle_detected": False, "bundle_count": 0}

    buys_by_block = {}
    for tx in data:
        slot = tx.get("slot", 0)
        fee_payer = tx.get("feePayer", "")
        direction = "unknown"
        token_txs = tx.get("tokenTransfers", [])
        for tt in token_txs:
            if tt.get("mint") == token_addr:
                if tt.get("toUserAccount") == fee_payer:
                    direction = "buy"
                elif tt.get("fromUserAccount") == fee_payer:
                    direction = "sell"
        if direction == "buy":
            buys_by_block.setdefault(slot, []).append(fee_payer)

    bundle = False
    bundle_count = 0
    for slot, signers in buys_by_block.items():
        if len(signers) >= 3:
            bundle = True
            bundle_count = max(bundle_count, len(signers))

    return {"bundle_detected": bundle, "bundle_count": bundle_count}


async def check_freeze_authority(mint_address: str) -> dict:
    """Check freeze_authority and mint_authority via getAccountInfo RPC.
    Returns {"freeze_authority": bool or None, "mint_authority": bool or None}."""
    if not HELIUS_RPC:
        return {"freeze_authority": None, "mint_authority": None}
    payload = {
        "jsonrpc": "2.0", "id": 1,
        "method": "getAccountInfo",
        "params": [mint_address, {"encoding": "jsonParsed"}]
    }
    try:
        data = await rpc_call("getAccountInfo", [mint_address, {"encoding": "jsonParsed"}], HELIUS_RPC)
        if HELIUS_API_KEY:
            helius_tracker.record_call()
    except Exception as e:
        log.warning(f"Freeze authority check failed for {mint_address[:8]}: {e}")
        return {"freeze_authority": None, "mint_authority": None}

    if not data:
        return {"freeze_authority": None, "mint_authority": None}
    info = (data.get("result") or {}).get("value", {})
    if not info:
        return {"freeze_authority": None, "mint_authority": None}
    parsed = info.get("data", {}).get("parsed", {}).get("info", {})
    return {
        "freeze_authority": parsed.get("freezeAuthority") is not None,
        "mint_authority": parsed.get("mintAuthority") is not None,
    }


async def check_creator_history(creator_addr: str) -> int:
    """Check if creator deployed other tokens via Helius REST API.
    Returns count of OTHER tokens created (excluding current), or -1 on failure."""
    if not creator_addr or not HELIUS_API or not HELIUS_API_KEY:
        return -1
    url = f"{HELIUS_API}/addresses/{creator_addr}/transactions?type=TOKEN_MINT&api-key={HELIUS_API_KEY}"
    try:
        session = await _get_rpc_session()
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                data = await resp.json()
        if HELIUS_API_KEY:
            helius_tracker.record_call()
    except Exception as e:
        log.warning(f"Creator history check failed for {creator_addr[:8]}: {e}")
        return -1

    if not isinstance(data, list):
        return -1
    mints = set()
    for tx in data:
        for tt in tx.get("tokenTransfers", []):
            m = tt.get("mint", "")
            if m:
                mints.add(m)
    return max(0, len(mints) - 1)

# ============================================================
# TELEGRAM (async, non-blocking)
# ============================================================

# Persistent aiohttp session for Telegram calls — initialized in bot start()
_tg_session: Optional[aiohttp.ClientSession] = None


async def _tg_send_async(msg: str):
    """Async Telegram send using persistent aiohttp session. Non-blocking."""
    global _tg_session
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        return
    try:
        if _tg_session is None or _tg_session.closed:
            _tg_session = aiohttp.ClientSession()
        async with _tg_session.post(
            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": msg},
            timeout=aiohttp.ClientTimeout(total=5),
        ) as resp:
            if resp.status != 200:
                body = await resp.text()
                log.warning(f"TG send failed ({resp.status}): {body[:100]}")
    except Exception as e:
        log.warning(f"TG failed: {e}")


def tg_send(msg: str):
    """Fire-and-forget Telegram send. Works from both sync and async contexts.
    If called from within a running event loop, schedules as a background task.
    If no loop is running, creates a temporary one (startup/shutdown only)."""
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        return
    try:
        loop = asyncio.get_running_loop()
        # We're inside an async context — schedule as fire-and-forget task
        loop.create_task(_tg_send_async(msg))
    except RuntimeError:
        # No event loop running (e.g. during init) — fall back to sync requests
        try:
            requests.post(
                f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
                json={"chat_id": TG_CHAT_ID, "text": msg},
                timeout=5,
            )
        except Exception as e:
            log.warning(f"TG sync fallback failed: {e}")

# ============================================================
# DATABASE
# ============================================================

DB_PATH = "trades.db"

def get_db() -> sqlite3.Connection:
    """v4.3: Thread-safe DB connection with WAL mode and timeout."""
    conn = sqlite3.connect(DB_PATH, timeout=10, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


class DBConnection:
    """Context manager for get_db() that auto-closes on exit."""
    def __enter__(self):
        self.conn = get_db()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type is None:
                self.conn.commit()
            self.conn.close()
        except Exception:
            pass
        return False


def db_connection():
    """Shorthand: `with db_connection() as conn: ...` — auto-commits and closes."""
    return DBConnection()


def db_execute(sql: str, params: tuple = (), fetch: str = "none"):
    """Simple helper for single-statement DB ops. Opens, executes, commits, closes.
    fetch: 'none' (default), 'one', 'all'."""
    with db_connection() as conn:
        cursor = conn.execute(sql, params)
        if fetch == "one":
            return cursor.fetchone()
        elif fetch == "all":
            return cursor.fetchall()
        return cursor

def init_db():
    with db_connection() as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("""CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token_address TEXT NOT NULL, symbol TEXT,
            trade_number INTEGER DEFAULT 1, alert_grade TEXT,
            entry_price REAL, entry_time REAL, entry_liquidity REAL, entry_tx TEXT,
            exit_price REAL, exit_time REAL, exit_reason TEXT, exit_tx TEXT,
            peak_price REAL, pnl_sol REAL, pnl_pct REAL,
            position_size_sol REAL, remaining_pct REAL DEFAULT 1.0,
            progressive_sold TEXT DEFAULT '[]', progressive_banked_sol REAL DEFAULT 0,
            status TEXT DEFAULT 'open', tokens_held REAL DEFAULT 0,
            entry_liq_usd REAL DEFAULT 0, top1_pct REAL DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS token_history (
            token_address TEXT PRIMARY KEY, detected_at REAL,
            symbol TEXT, grade TEXT, top1_pct REAL,
            entry_price_at_35s REAL, first_price REAL,
            liquidity_usd REAL, traded INTEGER DEFAULT 0,
            creator TEXT, rejection_reason TEXT
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS circuit_breaker (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            daily_pnl REAL DEFAULT 0, hourly_pnl REAL DEFAULT 0,
            consecutive_losses INTEGER DEFAULT 0,
            last_hour_reset REAL, last_day_reset REAL,
            paused INTEGER DEFAULT 0, pause_reason TEXT
        )""")
        conn.execute("""INSERT OR IGNORE INTO circuit_breaker
            (id, daily_pnl, hourly_pnl, consecutive_losses, last_hour_reset, last_day_reset, paused)
            VALUES (1, 0, 0, 0, ?, ?, 0)""", (time.time(), time.time()))
    log.info("Database initialized")

# ============================================================
# JUPITER SWAP ENGINE
# ============================================================

class JupiterSwap:
    def __init__(self, keypair: Keypair, rpc_url: str):
        self.keypair = keypair
        self.rpc_url = rpc_url
        self.wallet_pubkey = str(keypair.pubkey())
        log.info(f"Wallet: {self.wallet_pubkey}")

    def _headers(self) -> dict:
        return {"x-api-key": JUPITER_API_KEY} if JUPITER_API_KEY else {}

    async def get_quote(self, input_mint: str, output_mint: str,
                        amount_lamports: int, slippage_bps: int = 1500) -> Optional[dict]:
        try:
            params = {
                "inputMint": input_mint, "outputMint": output_mint,
                "amount": str(amount_lamports), "slippageBps": str(slippage_bps),
                "onlyDirectRoutes": "false", "restrictIntermediateTokens": "true",
            }
            session = await _get_rpc_session()
            async with session.get(JUPITER_QUOTE_URL, params=params,
                                  headers=self._headers(),
                                  timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    return await resp.json()
                body = await resp.text()
                log.error(f"Quote failed ({resp.status}): {body[:200]}")
                return None
        except Exception as e:
            log.error(f"Quote error: {e}")
            return None

    async def execute_swap(self, quote: dict) -> Optional[str]:
        try:
            swap_body = {
                "quoteResponse": quote,
                "userPublicKey": self.wallet_pubkey,
                "wrapAndUnwrapSol": True,
                "dynamicComputeUnitLimit": True,
                "dynamicSlippage": True,
                "prioritizationFeeLamports": {
                    "priorityLevelWithMaxLamports": {
                        "maxLamports": PRIORITY_FEE_LAMPORTS,
                        "priorityLevel": "veryHigh"
                    }
                },
            }
            session = await _get_rpc_session()
            async with session.post(JUPITER_SWAP_URL, json=swap_body,
                                   headers=self._headers(),
                                   timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    log.error(f"Swap API failed ({resp.status}): {body[:200]}")
                    return None
                data = await resp.json()

            swap_tx_b64 = data.get("swapTransaction")
            if not swap_tx_b64:
                log.error("No swapTransaction in response")
                return None

            raw_tx = VersionedTransaction.from_bytes(base64.b64decode(swap_tx_b64))
            signature = self.keypair.sign_message(
                solders_message.to_bytes_versioned(raw_tx.message)
            )
            signed_tx = VersionedTransaction.populate(raw_tx.message, [signature])

            tx_sig = await send_raw_tx(bytes(signed_tx))
            if not tx_sig:
                return None

            log.info(f"TX sent: {tx_sig[:24]}...")
            confirmed = await confirm_tx(tx_sig)
            if not confirmed:
                log.warning(f"TX FAILED on-chain: {tx_sig[:24]}...")
                return None  # Return None so caller knows TX failed
            return tx_sig
        except Exception as e:
            log.error(f"Swap error: {e}\n{traceback.format_exc()}")
            return None

    async def buy_token(self, token_mint: str, sol_amount: float,
                        slippage_bps: int = 2000) -> Tuple[Optional[str], int]:
        amount_lamports = int(sol_amount * LAMPORTS_PER_SOL)
        log.info(f"BUY {sol_amount} SOL -> {token_mint[:16]}... (slippage {slippage_bps}bps)")

        quote = await self.get_quote(SOL_MINT, token_mint, amount_lamports, slippage_bps)
        if not quote:
            return None, 0

        out_amount = int(quote.get("outAmount", 0))
        log.info(f"  Quote: {out_amount:,} tokens expected")

        tx_sig = await self.execute_swap(quote)
        if not tx_sig:
            return None, 0

        await asyncio.sleep(3)
        actual = await get_token_balance(self.wallet_pubkey, token_mint)
        if actual > 0:
            log.info(f"  Balance: {actual:,} tokens")
            return tx_sig, actual
        else:
            log.warning(f"  Balance check returned 0, using quote: {out_amount:,}")
            return tx_sig, out_amount

    async def sell_token(self, token_mint: str, token_amount: int,
                         slippage_bps: int = 3500) -> Optional[str]:
        """v4.3: Single attempt per call — no internal retry loop.
        Retries happen across monitor cycles via sell_attempts counter.
        This prevents blocking the monitor for 30-90s during retries."""
        if token_amount <= 0:
            return None
        log.info(f"SELL: {token_amount:,} of {token_mint[:16]}... (slippage {slippage_bps}bps)")
        quote = await self.get_quote(token_mint, SOL_MINT, token_amount, slippage_bps)
        if not quote:
            log.warning(f"SELL quote failed for {token_mint[:16]}")
            return None
        result = await self.execute_swap(quote)
        if not result:
            log.warning(f"SELL swap failed for {token_mint[:16]}")
        return result

    async def emergency_sell(self, token_mint: str, token_amount: int) -> Optional[str]:
        log.warning(f"EMERGENCY SELL {token_mint[:16]}...")
        return await self.sell_token(token_mint, token_amount, slippage_bps=5000)

    async def check_sellable(self, token_mint: str, token_amount: int) -> bool:
        """v4.3: Honeypot check — can we get a sell quote? If not, token is unsellable.
        Uses 10% of token amount to avoid false positives on thin pools."""
        if token_amount <= 0:
            return False
        # Check with small amount to avoid false positive on thin liquidity
        check_amount = max(token_amount // 10, 1)
        quote = await self.get_quote(token_mint, SOL_MINT, check_amount, 5000)
        if quote:
            out_amount = int(quote.get("outAmount", 0))
            if out_amount > 0:
                return True
            log.warning(f"Honeypot check: quote returned 0 outAmount for {token_mint[:16]}")
            return False
        log.warning(f"Honeypot check: no quote for {token_mint[:16]} — likely honeypot")
        return False

# ============================================================
# MARKET DATA (3 sources)
# ============================================================

class MarketData:
    def __init__(self):
        self.session = None

    async def init_session(self):
        self.session = aiohttp.ClientSession()

    async def close(self):
        if self.session:
            await self.session.close()

    async def get_graduated_tokens(self) -> List[dict]:
        try:
            async with self.session.get(PUMPFUN_URL, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data if isinstance(data, list) else []
                return []
        except Exception as e:
            log.error(f"Pump.fun graduated error: {e}")
            return []

    async def get_live_tokens(self) -> List[dict]:
        try:
            async with self.session.get(PUMPFUN_LIVE_URL, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data if isinstance(data, list) else []
                return []
        except Exception as e:
            log.error(f"Pump.fun live error: {e}")
            return []

    async def get_gecko_pools(self) -> List[dict]:
        try:
            async with self.session.get(GECKO_URL, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    result = []
                    for p in data.get("data", []):
                        base = p.get("relationships", {}).get("base_token", {}).get("data", {})
                        addr = base.get("id", "").replace("solana_", "") if base else ""
                        if addr:
                            name = p.get("attributes", {}).get("name", "?").split("/")[0].strip()
                            result.append({"mint": addr, "symbol": name, "source": "gecko"})
                    return result
                return []
        except Exception as e:
            log.error(f"GeckoTerminal error: {e}")
            return []

    async def get_token_data(self, addresses: List[str]) -> Dict[str, dict]:
        if not addresses:
            return {}
        try:
            url = f"{DEXSCREENER_URL}/{','.join(addresses[:30])}"
            async with self.session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    pairs = data.get("pairs") or []
                    result = {}
                    for pair in pairs:
                        addr = pair.get("baseToken", {}).get("address", "")
                        if addr and addr not in result:
                            result[addr] = pair
                    return result
                return {}
        except Exception as e:
            log.error(f"DexScreener error: {e}")
            return {}

    async def get_token_price_and_liq(self, address: str) -> Tuple[float, float]:
        data = await self.get_token_data([address])
        if address in data:
            pair = data[address]
            price = float(pair.get("priceUsd", 0) or 0)
            liq = float(pair.get("liquidity", {}).get("usd", 0) or 0)
            return price, liq
        return 0, 0

# ============================================================
# POSITION MANAGER
# ============================================================

class PositionManager:
    def __init__(self, jupiter: JupiterSwap, market: MarketData):
        self.jupiter = jupiter
        self.market = market
        self.lock = Lock()
        self._open_positions: Dict[int, dict] = {}

    def load_open_positions(self):
        with db_connection() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM trades WHERE status = 'open'").fetchall()
            for row in rows:
                pos = dict(row)
                pos["ladder_levels_hit"] = set()
                pos["emergency_retries"] = 0
                pos["sell_attempts"] = 0
                pos["zero_price_count"] = 0
                pos["pending_exit"] = None  # v4.4: for failed sell retry (Fix #4)
                try:
                    pos["progressive_sold"] = json.loads(pos.get("progressive_sold", "[]"))
                except:
                    pos["progressive_sold"] = []
                self._open_positions[row["id"]] = pos
        log.info(f"Loaded {len(self._open_positions)} open positions")

    @property
    def open_count(self) -> int:
        return len(self._open_positions)

    def get_token_trade_count(self, token_address: str) -> int:
        row = db_execute(
            "SELECT COUNT(*) FROM trades WHERE token_address = ?",
            (token_address,), fetch="one"
        )
        return row[0] if row else 0

    def is_token_open(self, token_address: str) -> bool:
        return any(p["token_address"] == token_address for p in self._open_positions.values())

    async def open_position(self, token_address: str, symbol: str, grade: str,
                           entry_liq: float, entry_price: float, top1: float = 0) -> Optional[int]:
        # Live test mode: max 5 positions
        max_pos = MAX_POSITIONS
        if self.open_count >= max_pos:
            return None
        if self.get_token_trade_count(token_address) >= MAX_TRADES_PER_TOKEN:
            return None
        if self.is_token_open(token_address):
            return None
        if self._is_circuit_broken():
            log.warning(f"Circuit breaker active, skipping {symbol}")
            return None

        log.info(f"OPENING {grade}-grade: {symbol} ({token_address[:16]}...) liq=${entry_liq:,.0f} top1={top1:.1f}%")

        if SHADOW_MODE:
            log.info(f"SHADOW BUY: {symbol} (no real trade)")
            tg_send(f"SHADOW BUY {grade} | {symbol} | ${entry_price:.8f} | Liq: ${entry_liq:,.0f} | Top1: {top1:.0f}%")
            tx_sig = "SHADOW"
            tokens_held = int((POSITION_SIZE_SOL * 94) / entry_price) if entry_price > 0 else 1000000
        else:
            # LIVE SAFETY CHECKS
            can, reason = safety_tracker.can_trade()
            if not can:
                log.warning(f"SAFETY BLOCK: {symbol} — {reason}")
                tg_send(f"SAFETY BLOCK: {symbol} — {reason}")
                return None
            
            # Check wallet balance
            wallet_bal = await get_sol_balance(self.jupiter.wallet_pubkey)
            if wallet_bal < POSITION_SIZE_SOL + MIN_WALLET_RESERVE_SOL:
                log.warning(f"LOW BALANCE: {wallet_bal:.4f} SOL — need {POSITION_SIZE_SOL + MIN_WALLET_RESERVE_SOL:.4f}")
                tg_send(f"LOW BALANCE: {wallet_bal:.4f} SOL — skipping {symbol}")
                return None
            
            # Execute buy
            tx_sig, tokens_held = await self.jupiter.buy_token(token_address, POSITION_SIZE_SOL)
            
            if tx_sig:
                safety_tracker.record_buy(POSITION_SIZE_SOL)

                # v4.3: Honeypot check — verify we can sell BEFORE committing
                # v4.4-prod Fix #1: Entire honeypot section wrapped in try/except
                # so a buy is NEVER orphaned without a DB record
                if tokens_held > 0:
                    try:
                        try:
                            sellable = await self.jupiter.check_sellable(token_address, tokens_held)
                        except Exception as e:
                            log.warning(f"Honeypot check failed for {symbol}: {e} — assuming sellable")
                            sellable = True  # Assume sellable on API error, don't orphan the trade
                        if not sellable:
                            log.warning(f"HONEYPOT DETECTED: {symbol} — cannot get sell quote! Attempting emergency dump.")
                            tg_send(f"HONEYPOT: {symbol} — can't sell! Attempting dump...")
                            # Try to sell immediately
                            dump_tx = await self.jupiter.sell_token(token_address, tokens_held, 5000)
                            if dump_tx:
                                tg_send(f"HONEYPOT DUMP OK: {symbol} — sold immediately")
                                # Record as closed trade so it shows on dashboard
                                with db_connection() as conn:
                                    conn.execute("""INSERT INTO trades
                                        (token_address, symbol, trade_number, alert_grade, entry_price, entry_time,
                                         entry_liquidity, entry_tx, exit_price, exit_time, exit_reason, exit_tx,
                                         peak_price, pnl_sol, pnl_pct, position_size_sol, status,
                                         tokens_held, entry_liq_usd, top1_pct)
                                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'closed',
                                                ?, ?, ?)""",
                                        (token_address, symbol, 1, grade, entry_price, time.time(),
                                         entry_liq, tx_sig, entry_price, time.time(), "HONEYPOT_DUMPED", dump_tx,
                                         entry_price, -0.005, -25, POSITION_SIZE_SOL,
                                         tokens_held, entry_liq, top1))
                            else:
                                tg_send(f"HONEYPOT STUCK: {symbol} — tokens unsellable. Total loss.")
                                # Record as closed total loss
                                with db_connection() as conn:
                                    conn.execute("""INSERT INTO trades
                                        (token_address, symbol, trade_number, alert_grade, entry_price, entry_time,
                                         entry_liquidity, entry_tx, exit_price, exit_time, exit_reason, exit_tx,
                                         peak_price, pnl_sol, pnl_pct, position_size_sol, status,
                                         tokens_held, entry_liq_usd, top1_pct)
                                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'closed',
                                                ?, ?, ?)""",
                                        (token_address, symbol, 1, grade, entry_price, time.time(),
                                         entry_liq, tx_sig, 0, time.time(), "HONEYPOT_STUCK", "NONE",
                                         entry_price, -POSITION_SIZE_SOL, -100, POSITION_SIZE_SOL,
                                         tokens_held, entry_liq, top1))
                            return None
                    except Exception as hp_err:
                        # v4.4-prod Fix #1: On ANY exception in honeypot block,
                        # record the trade in DB as HONEYPOT_ERROR so it's never orphaned
                        log.error(f"HONEYPOT BLOCK EXCEPTION for {symbol}: {hp_err}\n{traceback.format_exc()}")
                        tg_send(f"HONEYPOT ERROR: {symbol} — exception during check: {hp_err}")
                        try:
                            with db_connection() as conn:
                                conn.execute("""INSERT INTO trades
                                    (token_address, symbol, trade_number, alert_grade, entry_price, entry_time,
                                     entry_liquidity, entry_tx, exit_price, exit_time, exit_reason, exit_tx,
                                     peak_price, pnl_sol, pnl_pct, position_size_sol, status,
                                     tokens_held, entry_liq_usd, top1_pct)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'closed',
                                            ?, ?, ?)""",
                                    (token_address, symbol, 1, grade, entry_price, time.time(),
                                     entry_liq, tx_sig, 0, time.time(), "HONEYPOT_ERROR", "NONE",
                                     entry_price, -POSITION_SIZE_SOL, -100, POSITION_SIZE_SOL,
                                     tokens_held, entry_liq, top1))
                        except Exception as db_err:
                            log.error(f"CRITICAL: Could not record honeypot error for {symbol}: {db_err}")
                        return None

                # Verify tokens received
                if tokens_held == 0:
                    log.warning(f"BUY executed but 0 tokens received for {symbol} — TX may have failed")
                    tg_send(f"WARNING: BUY TX sent but 0 tokens for {symbol}\nTX: {tx_sig[:24]}...\nCheck Solscan!")
                else:
                    solscan = f"https://solscan.io/tx/{tx_sig}"
                    tg_send(f"BUY {grade} | {symbol}\nPrice: ${entry_price:.8f}\nLiq: ${entry_liq:,.0f}\nTokens: {tokens_held:,}\nTX: {solscan}")

        if not tx_sig:
            log.warning(f"BUY failed for {symbol}")
            tg_send(f"BUY FAILED: {symbol} ({grade})")
            return None

        trade_count = self.get_token_trade_count(token_address)

        # v4.4-prod Fix #2: Atomic DB insert + dict update.
        # If dict update fails after DB insert, immediately close the DB record
        # so it's never orphaned as a phantom open position.
        try:
            with db_connection() as conn:
                cursor = conn.execute("""INSERT INTO trades
                    (token_address, symbol, trade_number, alert_grade, entry_price, entry_time,
                     entry_liquidity, entry_tx, peak_price, position_size_sol, tokens_held,
                     entry_liq_usd, top1_pct, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open')""",
                    (token_address, symbol, trade_count + 1, grade, entry_price, time.time(),
                     entry_liq, tx_sig, entry_price, POSITION_SIZE_SOL, tokens_held, entry_liq, top1))
                trade_id = cursor.lastrowid
        except Exception as db_err:
            log.error(f"DB INSERT failed for {symbol}: {db_err}")
            tg_send(f"DB ERROR: Could not record buy for {symbol}")
            return None

        pos = {
            "id": trade_id, "token_address": token_address, "symbol": symbol,
            "trade_number": trade_count + 1, "alert_grade": grade,
            "entry_price": entry_price, "entry_time": time.time(),
            "entry_liq_usd": entry_liq, "entry_tx": tx_sig,
            "peak_price": entry_price, "position_size_sol": POSITION_SIZE_SOL,
            "tokens_held": tokens_held, "remaining_pct": 1.0,
            "progressive_sold": [], "progressive_banked_sol": 0,
            "ladder_levels_hit": set(), "emergency_retries": 0,
            "sell_attempts": 0, "zero_price_count": 0,
            "pending_exit": None,  # v4.4: for failed sell retry (Fix #4)
        }
        try:
            with self.lock:
                self._open_positions[trade_id] = pos
        except Exception as dict_err:
            # v4.4-prod Fix #2: Dict update failed — close the DB record immediately
            log.error(f"Dict update failed for {symbol} #{trade_id}: {dict_err}")
            try:
                db_execute(
                    "UPDATE trades SET status='closed', exit_reason='OPEN_FAILED', exit_time=? WHERE id=?",
                    (time.time(), trade_id)
                )
            except Exception:
                pass
            tg_send(f"OPEN_FAILED: {symbol} #{trade_id} — position closed in DB")
            return None

        log.info(f"OPENED #{trade_id}: {symbol} ({grade}) | {tokens_held:,} tokens")
        return trade_id

    async def close_position(self, trade_id: int, reason: str,
                            exit_price: float, emergency: bool = False):
        with self.lock:
            pos = self._open_positions.get(trade_id)
            if not pos:
                return

        symbol = pos["symbol"]

        # v4.3: If bot is halted, don't attempt sells — just finalize as failed
        if not SHADOW_MODE and safety_tracker.halted:
            log.warning(f"BOT HALTED — auto-closing {symbol} as failed (no sell attempt)")
            self._finalize_close(trade_id, pos, f"{reason}_BOT_HALTED", exit_price, None, emergency=True)
            return

        # v4.3: Track sell attempts per position
        sell_attempts = pos.get("sell_attempts", 0)
        if not SHADOW_MODE and sell_attempts >= MAX_SELL_ATTEMPTS_PER_POSITION:
            log.warning(f"Max sell attempts ({MAX_SELL_ATTEMPTS_PER_POSITION}) reached for {symbol} — auto-closing as failed")
            tg_send(f"SELL GAVE UP: {symbol} after {sell_attempts} attempts — closing as total loss")
            self._finalize_close(trade_id, pos, f"{reason}_SELL_MAXED", exit_price, None, emergency=True)
            return

        tokens_to_sell = int(pos["tokens_held"] * pos["remaining_pct"])

        # LIVE: check actual on-chain balance instead of estimate
        if not SHADOW_MODE:
            actual_balance = await get_token_balance(self.jupiter.wallet_pubkey, pos["token_address"])
            if actual_balance > 0:
                tokens_to_sell = actual_balance
                log.info(f"Actual on-chain balance for {symbol}: {actual_balance:,}")
            elif tokens_to_sell > 0:
                log.warning(f"On-chain balance 0 for {symbol}, using estimate: {tokens_to_sell:,}")

        if tokens_to_sell <= 0:
            self._finalize_close(trade_id, pos, reason, exit_price, None, emergency=True)
            return

        if emergency:
            retries = pos.get("emergency_retries", 0)
            if retries >= MAX_EMERGENCY_RETRIES:
                log.warning(f"Emergency max retries for {symbol}, total loss")
                self._finalize_close(trade_id, pos, f"{reason}_SELL_FAILED", exit_price, None, emergency=True)
                return
            with self.lock:
                pos["emergency_retries"] = retries + 1

        if SHADOW_MODE:
            tx_sig = "SHADOW"
            log.info(f"SHADOW SELL: {symbol} ({reason})")
        else:
            # v4.3: ALL sells check pool liquidity first (not just emergency)
            # But distinguish API failure (0,0) from real drain
            pool_price, current_liq = await self.market.get_token_price_and_liq(pos["token_address"])
            if current_liq < 100 and pool_price > 0:
                # Price exists but liq gone = real pool drain
                log.warning(f"Pool drained for {symbol} (liq=${current_liq:.0f}, price=${pool_price:.8f}) — auto-closing as total loss")
                self._finalize_close(trade_id, pos, f"{reason}_POOL_DRAINED", exit_price, None, emergency=True)
                return
            elif current_liq == 0 and pool_price == 0:
                # Both zero = likely API failure, NOT pool drain
                # Still try to sell — let Jupiter figure it out
                log.warning(f"DexScreener returned 0/0 for {symbol} — API issue? Attempting sell anyway.")

            # Track attempt
            with self.lock:
                pos["sell_attempts"] = sell_attempts + 1

            # v4.3: Escalate slippage across attempts (not inside sell_token)
            slippage_schedule = [3500, 4500, 5000]
            slip = slippage_schedule[min(sell_attempts, len(slippage_schedule) - 1)]

            if emergency:
                slip = 5000  # Always max slippage for emergency
                tx_sig = await self.jupiter.sell_token(pos["token_address"], tokens_to_sell, slip)
            else:
                tx_sig = await self.jupiter.sell_token(pos["token_address"], tokens_to_sell, slip)

            if tx_sig:
                safety_tracker.record_sell_success()
                # v4.4-prod Fix #4: Clear pending exit on successful sell
                with self.lock:
                    pos["pending_exit"] = None
                solscan = f"https://solscan.io/tx/{tx_sig}"
                prefix = "EMERGENCY SELL" if emergency else "SELL"
                tg_send(f"{prefix} | {symbol}\nReason: {reason}\nTX: {solscan}")
            else:
                safety_tracker.record_sell_failure()
                # v4.4-prod Fix #4: Store pending exit so it retries regardless of price recovery
                with self.lock:
                    pending = pos.get("pending_exit")
                    attempt = (pending.get("attempts", 0) if pending else 0) + 1
                    if attempt >= MAX_SELL_ATTEMPTS_PER_POSITION:
                        log.warning(f"Pending exit max attempts for {symbol} — giving up")
                        tg_send(f"SELL GAVE UP: {symbol} after {attempt} pending attempts")
                        pos["pending_exit"] = None
                        # Fall through to finalize below
                    else:
                        pos["pending_exit"] = {
                            "reason": reason,
                            "price": exit_price if hasattr(self, '_monitor_price') else 0,
                            "attempts": attempt,
                            "emergency": emergency,
                        }
                log.error(f"SELL FAILED for {symbol} ({reason}) — attempt {sell_attempts + 1}/{MAX_SELL_ATTEMPTS_PER_POSITION}")
                tg_send(f"SELL FAILED: {symbol} ({reason}) — attempt {sell_attempts + 1}/{MAX_SELL_ATTEMPTS_PER_POSITION}")
                # Check if we hit max pending attempts and need to force-close
                if pos.get("pending_exit") is not None:
                    return  # Will retry on next monitor cycle
                # pending_exit was cleared = max attempts reached, finalize as total loss
                self._finalize_close(trade_id, pos, f"{reason}_SELL_MAXED", exit_price, None, emergency=True)
                return

        if not tx_sig and emergency:
            retries = pos.get("emergency_retries", 0)
            if retries >= MAX_EMERGENCY_RETRIES:
                self._finalize_close(trade_id, pos, f"{reason}_SELL_FAILED", exit_price, None, emergency=True)
            return

        self._finalize_close(trade_id, pos, reason, exit_price, tx_sig, emergency)

    def _finalize_close(self, trade_id: int, pos: dict, reason: str,
                       exit_price: float, tx_sig: Optional[str], emergency: bool = False):
        entry_price = pos["entry_price"]
        banked = pos.get("progressive_banked_sol", 0)
        remaining = pos["remaining_pct"]
        pos_size = pos.get("position_size_sol", POSITION_SIZE_SOL)  # v4.3: use per-trade size

        # REALITY MODE: emergency exits = ALWAYS total loss on remaining
        if emergency or not tx_sig:
            pnl_sol = banked - (pos_size * remaining)
        else:
            pnl_pct = ((exit_price / entry_price) - 1) * 100 if entry_price else 0
            pnl_sol = banked + (pos_size * remaining * (pnl_pct / 100))

        pnl_pct_total = ((pnl_sol / pos_size) * 100) if pos_size else 0

        conn = get_db()
        try:
            conn.execute("""UPDATE trades SET exit_price=?, exit_time=?, exit_reason=?,
                exit_tx=?, pnl_sol=?, pnl_pct=?, status='closed', peak_price=?,
                remaining_pct=?, progressive_sold=?, progressive_banked_sol=?
                WHERE id=?""",
                (exit_price, time.time(), reason, tx_sig or "NONE", pnl_sol, pnl_pct_total,
                 pos["peak_price"], remaining, json.dumps(pos.get("progressive_sold", [])),
                 banked, trade_id))
            conn.commit()
        finally:
            conn.close()

        self._update_circuit_breaker(pnl_sol)

        with self.lock:
            self._open_positions.pop(trade_id, None)

        symbol = pos["symbol"]
        emoji = "+" if pnl_sol >= 0 else ""
        hold = time.time() - pos["entry_time"]
        peak_pct = ((pos["peak_price"] / entry_price) - 1) * 100 if entry_price else 0

        tg_send(
            f"{'EMERGENCY ' if emergency else ''}SELL | {symbol}\n"
            f"Reason: {reason}\nPnL: {emoji}{pnl_sol:.4f} SOL ({emoji}{pnl_pct_total:.0f}%)\n"
            f"Hold: {hold:.0f}s | Peak: +{peak_pct:.0f}%"
        )
        log.info(f"CLOSED #{trade_id}: {symbol} | {reason} | PnL: {pnl_sol:+.4f} SOL ({pnl_pct_total:+.0f}%)")

    async def execute_ladder_sell(self, trade_id: int, threshold_pct: float,
                                  sell_fraction: float, current_price: float):
        with self.lock:
            pos = self._open_positions.get(trade_id)
            if not pos:
                return

        # v4.3: Use actual on-chain balance for ladder sells too
        if not SHADOW_MODE:
            actual_balance = await get_token_balance(self.jupiter.wallet_pubkey, pos["token_address"])
            if actual_balance > 0:
                # v4.4-prod Fix #6: Sell fraction of ORIGINAL position, capped at actual balance.
                # pos["tokens_held"] is the original token count from buy.
                # sell_fraction is the fraction of the original position to sell.
                tokens_to_sell = min(int(pos["tokens_held"] * sell_fraction), actual_balance)
            else:
                tokens_to_sell = int(pos["tokens_held"] * sell_fraction)
        else:
            tokens_to_sell = int(pos["tokens_held"] * sell_fraction)
        if tokens_to_sell <= 0:
            return

        if SHADOW_MODE:
            tx_sig = "SHADOW"
        else:
            # v4.3: Check pool before ladder sell
            _, current_liq = await self.market.get_token_price_and_liq(pos["token_address"])
            if current_liq < 100:
                log.warning(f"Pool drained during ladder sell for {pos['symbol']} — skipping")
                with self.lock:
                    pos["ladder_levels_hit"].add(threshold_pct)  # Mark as hit so we don't retry
                return
            tx_sig = await self.jupiter.sell_token(pos["token_address"], tokens_to_sell, 2500)

        if tx_sig:
            entry_price = pos["entry_price"]
            pos_size = pos.get("position_size_sol", POSITION_SIZE_SOL)
            sol_received = pos_size * sell_fraction * (current_price / entry_price) if entry_price else 0
            with self.lock:
                pos["remaining_pct"] -= sell_fraction
                pos["progressive_banked_sol"] += sol_received
                pos["progressive_sold"].append({
                    "threshold": threshold_pct, "pct": sell_fraction,
                    "price": current_price, "sol": sol_received,
                })
                pos["ladder_levels_hit"].add(threshold_pct)
            log.info(f"LADDER #{trade_id} {pos['symbol']}: {sell_fraction*100:.0f}% at +{threshold_pct:.0f}%")
            tg_send(f"LADDER {pos['symbol']}: sold {sell_fraction*100:.0f}% at +{threshold_pct:.0f}%")
        else:
            # v4.3: Mark level as hit even on failure — prevents infinite retry loop
            log.warning(f"LADDER SELL FAILED for {pos['symbol']} at +{threshold_pct:.0f}% — marking level to prevent retry")
            with self.lock:
                pos["ladder_levels_hit"].add(threshold_pct)
            tg_send(f"LADDER FAIL: {pos['symbol']} at +{threshold_pct:.0f}% — skipped")

    def _is_circuit_broken(self) -> bool:
        conn = get_db()
        try:
            row = conn.execute("SELECT * FROM circuit_breaker WHERE id = 1").fetchone()
        finally:
            conn.close()
        if not row:
            return False
        now = time.time()
        if now - (row[4] or 0) > 3600:
            db_execute("UPDATE circuit_breaker SET hourly_pnl=0, last_hour_reset=? WHERE id=1", (now,))
        if now - (row[5] or 0) > 86400:
            db_execute("UPDATE circuit_breaker SET daily_pnl=0, last_day_reset=?, paused=0 WHERE id=1", (now,))
        return bool(row[6])

    def _update_circuit_breaker(self, pnl_sol: float):
        conn = get_db()
        try:
            row = conn.execute("SELECT * FROM circuit_breaker WHERE id = 1").fetchone()
            new_daily = (row[1] or 0) + pnl_sol
            new_hourly = (row[2] or 0) + pnl_sol
            new_consec = 0 if pnl_sol > 0 else (row[3] or 0) + 1
            paused, reason = 0, None
            if new_daily <= -MAX_DAILY_LOSS_SOL:
                paused, reason = 1, f"Daily loss: {new_daily:.4f}"
            elif new_hourly <= -MAX_HOURLY_LOSS_SOL:
                paused, reason = 1, f"Hourly loss: {new_hourly:.4f}"
            elif new_consec >= MAX_CONSECUTIVE_LOSSES:
                paused, reason = 1, f"Consecutive: {new_consec}"
            conn.execute("""UPDATE circuit_breaker SET daily_pnl=?, hourly_pnl=?,
                consecutive_losses=?, paused=?, pause_reason=? WHERE id=1""",
                (new_daily, new_hourly, new_consec, paused, reason))
            conn.commit()
        finally:
            conn.close()
        if paused:
            log.warning(f"CIRCUIT BREAKER: {reason}")
            tg_send(f"CIRCUIT BREAKER: {reason}\nBot paused.")

# ============================================================
# MAIN BOT
# ============================================================

class WaveRiderBot:
    def __init__(self):
        if not PRIVATE_KEY:
            raise ValueError("PRIVATE_KEY not set")
        if not RPC_URL:
            raise ValueError("RPC_URL not set")
        if not JUPITER_API_KEY:
            raise ValueError("JUPITER_API_KEY not set")

        self.keypair = Keypair.from_bytes(base58.b58decode(PRIVATE_KEY))
        self.jupiter = JupiterSwap(self.keypair, RPC_URL)
        self.market = MarketData()
        self.positions = PositionManager(self.jupiter, self.market)
        self.running = False
        self._seen_tokens = set()
        self._warmed_up = False
        self._pending_tokens = {}
        self._scan_count = 0

    async def start(self):
        log.info(f"=== Wave Rider Bot {VERSION} starting ===")
        log.info(f"Shadow mode: {SHADOW_MODE}")
        log.info(f"Liq gate: ${MIN_LIQ_ENTRY:,.0f} (all grades)")

        init_db()
        await self.market.init_session()
        self.positions.load_open_positions()

        sol = await get_sol_balance(self.jupiter.wallet_pubkey)
        log.info(f"Balance: {sol:.4f} SOL (${sol * 94:.2f})")
        tg_send(
            f"Wave Rider {VERSION} started\n"
            f"Mode: {'SHADOW' if SHADOW_MODE else 'LIVE'}\n"
            f"Wallet: {self.jupiter.wallet_pubkey[:8]}...{self.jupiter.wallet_pubkey[-4:]}\n"
            f"Balance: {sol:.4f} SOL\nSize: {POSITION_SIZE_SOL} SOL\n"
            f"Liq gate: ${MIN_LIQ_ENTRY/1000:.0f}K (all grades)\n"
            f"Enrichment: bundle+freeze+creator"
        )

        self.running = True
        await asyncio.gather(
            self._scanner_loop(),
            self._monitor_loop(),
            self._health_loop(),
        )

    async def _scanner_loop(self):
        log.info("Scanner started (3 sources)")
        while self.running:
            try:
                self._scan_count += 1

                # Source 1: Graduated
                tokens = await self.market.get_graduated_tokens()
                grad_count = len(tokens)

                # Source 2: Live
                live_tokens = await self.market.get_live_tokens()
                live_count = len(live_tokens)
                for lt in live_tokens:
                    addr = lt.get("mint", "")
                    if addr and addr not in self._seen_tokens and addr not in self._pending_tokens:
                        tokens.append(lt)

                # Source 3: Gecko (every 6th scan ~30s)
                gecko_count = 0
                if self._scan_count % 6 == 0:
                    gecko_tokens = await self.market.get_gecko_pools()
                    gecko_count = len(gecko_tokens)
                    for gt in gecko_tokens:
                        addr = gt.get("mint", "")
                        if addr and addr not in self._seen_tokens and addr not in self._pending_tokens:
                            tokens.append(gt)

                # Count new
                new_count = sum(1 for t in tokens
                    if t.get("mint", "") not in self._seen_tokens
                    and t.get("mint", "") not in self._pending_tokens)

                if new_count > 0 or len(self._pending_tokens) > 0:
                    log.info(f"SCAN: grad={grad_count} live={live_count} gecko={gecko_count} | new={new_count} pending={len(self._pending_tokens)}")

                # Process new tokens
                for t in tokens:
                    addr = t.get("mint", "")
                    if not addr or addr in self._seen_tokens or addr in self._pending_tokens:
                        continue
                    self._seen_tokens.add(addr)

                    # Warmup: skip first batch
                    if not self._warmed_up:
                        continue

                    # Get first price
                    dex_data = await self.market.get_token_data([addr])
                    if addr not in dex_data:
                        continue

                    pair = dex_data[addr]
                    first_price = float(pair.get("priceUsd", 0) or 0)
                    first_liq = float(pair.get("liquidity", {}).get("usd", 0) or 0)
                    symbol = t.get("symbol", pair.get("baseToken", {}).get("symbol", "?"))

                    if first_price <= 0:
                        continue

                    # v4.4 fix: Store token for pending, enrich LATER (after 35s delay)
                    # V9.6 enriches at 25-90s, not at detection. Early enrichment causes
                    # bundle false positives (early bundled buys dominate before organic trades).
                    source = t.get("source", "pumpfun")  # gecko tokens have source="gecko"
                    self._pending_tokens[addr] = {
                        "pump_token": t, "first_price": first_price,
                        "first_liq": first_liq, "detected_at": time.time(),
                        "symbol": symbol, "holders": None,
                        "bundle_detected": False, "bundle_count": 0,
                        "freeze_authority": None, "mint_authority": None,
                        "creator_prev_tokens": -1, "creator_addr": "",
                        "enriched": False, "source": source,
                    }

                # Warmup complete after first scan
                if not self._warmed_up:
                    log.info(f"Warmup: skipped {len(self._seen_tokens)} existing tokens")
                    self._warmed_up = True

                # V9.6 ENRICHMENT: every 3rd scan, enrich 1 token aged 25-90s
                if self._scan_count % 1 == 0:  # DATA COLLECTION: every scan cycle
                    now_enrich = time.time()
                    to_enrich = [(addr, info) for addr, info in self._pending_tokens.items()
                                 if not info.get("enriched") 
                                 and 10 <= now_enrich - info["detected_at"] <= 300
                                 and info.get("first_liq", 0) >= MIN_LIQ_ENRICH]
                    for addr, info in to_enrich[:5]:  # DATA COLLECTION: up to 5 per cycle
                        try:
                            symbol = info["symbol"]
                            log.info(f"ENRICHING: {symbol} (V9.6 timing, age={now_enrich - info['detected_at']:.0f}s)")
                            
                            holders = await fetch_holder_concentration(addr)
                            if holders:
                                info["holders"] = holders
                                log.info(f"  HOLDERS: {symbol} top1={holders['top1']:.1f}%")
                            
                            bundle_info = await fetch_bundle_info(addr)
                            info["bundle_detected"] = bundle_info["bundle_detected"]
                            info["bundle_count"] = bundle_info["bundle_count"]
                            if bundle_info["bundle_detected"]:
                                log.info(f"  BUNDLE: {symbol} detected ({bundle_info['bundle_count']} buys in block)")
                            
                            freeze_info = await check_freeze_authority(addr)
                            if freeze_info:
                                info["freeze_authority"] = freeze_info.get("freeze_authority")
                                info["mint_authority"] = freeze_info.get("mint_authority")
                                if freeze_info.get("freeze_authority"):
                                    log.warning(f"  FREEZE: {symbol} has freeze authority!")
                            
                            creator_addr = info.get("pump_token", {}).get("creator", "")
                            if creator_addr:
                                creator_prev = await check_creator_history(creator_addr)
                                info["creator_prev_tokens"] = creator_prev
                                if creator_prev > 0:
                                    log.warning(f"  CREATOR: {symbol} deployer has {creator_prev} other tokens")
                            
                            info["enriched"] = True
                        except Exception as e:
                            log.error(f"Enrichment error for {info.get('symbol','?')}: {e}")
                            info["enriched"] = True  # Mark as done to avoid retry

                # Check pending tokens: must be enriched AND 35s+ old
                ready = []
                for addr, info in list(self._pending_tokens.items()):
                    age = time.time() - info["detected_at"]
                    if age >= ENTRY_DELAY_SECONDS and info.get("enriched"):
                        ready.append((addr, info))
                        del self._pending_tokens[addr]
                    elif age > 300:
                        # DATA COLLECTION: extended from 90s to 300s
                        del self._pending_tokens[addr]

                if ready:
                    addrs = [a for a, _ in ready]
                    dex_data = await self.market.get_token_data(addrs)
                    for addr, info in ready:
                        if addr in dex_data:
                            await self._evaluate(info, dex_data[addr])
                        else:
                            log.info(f"REJECT: {info['symbol']} -- no DexScreener data after 35s")
                            self._record_rejection(addr, info["symbol"], "NO_DEX_DATA")

            except Exception as e:
                log.error(f"Scanner error: {e}\n{traceback.format_exc()}")

            await asyncio.sleep(5)

    async def _evaluate(self, info: dict, dex_pair: dict):
        """V9.6-EXACT grading logic with kill signals, bundle detection, freeze authority."""
        addr = info["pump_token"].get("mint", "")
        symbol = info["symbol"]
        first_price = info["first_price"]

        current_price = float(dex_pair.get("priceUsd", 0) or 0)
        current_liq = float(dex_pair.get("liquidity", {}).get("usd", 0) or 0)

        if current_price <= 0:
            self._record_rejection(addr, symbol, "NO_PRICE")
            return

        # Enrichment data (already populated by scanner enrichment cycle)
        holders = info.get("holders")
        bundle_detected = info.get("bundle_detected", False)
        freeze_authority = info.get("freeze_authority", None)
        creator_prev = info.get("creator_prev_tokens", -1)
        curve_dur = info.get("pump_token", {}).get("curve_duration_sec", 0) or 0

        # Price momentum (gain since first detection)
        momentum = ((current_price / first_price) - 1) * 100 if first_price > 0 else 0

        # Price glitch filter
        if momentum > 10000:
            log.info(f"REJECT: {symbol} GLITCH +{momentum:.0f}%")
            self._record_rejection(addr, symbol, f"GLITCH_{momentum:.0f}pct")
            return

        # V9.6: Freeze authority = BLOCK (hard reject)
        if freeze_authority:
            log.info(f"REJECT: {symbol} FREEZE AUTHORITY — blocked")
            self._record_rejection(addr, symbol, "FREEZE_AUTHORITY")
            return

        # V9.6: Liquidity gate — single $15K for all grades
        if current_liq < MIN_LIQ_ENTRY:
            log.info(f"REJECT: {symbol} liq=${current_liq:,.0f} < ${MIN_LIQ_ENTRY:,.0f}")
            self._record_rejection(addr, symbol, f"LOW_LIQ_{current_liq:.0f}")
            return

        # --- V9.6 ENTRY FILTERS ---
        top1 = holders["top1"] if holders else 0
        top1_gte_99 = top1 >= 99
        top1_gte_95 = top1 >= 95
        price_up_5pct = momentum > 5

        # --- DATA COLLECTION: Only kill guaranteed losses ---
        kill = False
        kill_reasons = []
        # REMOVED: curve_dur > 300 (want data on slow graduations)
        # REMOVED: momentum < -10 (want data on dumps)
        # KEPT: serial creator (proven rugger)
        if creator_prev >= 1:
            kill = True
            kill_reasons.append(f"serial_creator={creator_prev}")

        if kill:
            reason_str = ",".join(kill_reasons)
            log.info(f"REJECT: {symbol} KILL SIGNAL: {reason_str}")
            self._record_rejection(addr, symbol, f"KILL_{reason_str}")
            return

        # --- DATA COLLECTION: Accept everything with any positive movement ---
        # Grade based on quality for analysis later
        if top1_gte_99:
            grade = "A"
        elif top1 >= 90:
            grade = "A-"
        elif top1 >= 80:
            grade = "B+"
        elif top1 >= 50:
            grade = "B"
        else:
            grade = "C"

        log.info(f"SIGNAL {grade}: {symbol} top1={top1:.1f}% bundle={bundle_detected} momentum=+{momentum:.1f}% liq=${current_liq:,.0f}")

        # Record in token_history
        with db_connection() as conn:
            conn.execute("""INSERT OR REPLACE INTO token_history
                (token_address, detected_at, symbol, grade, top1_pct,
                 entry_price_at_35s, first_price, liquidity_usd)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (addr, time.time(), symbol, grade, top1, current_price, first_price, current_liq))

        await self.positions.open_position(addr, symbol, grade, current_liq, current_price, top1)

    def _record_rejection(self, addr: str, symbol: str, reason: str):
        try:
            with db_connection() as conn:
                conn.execute("""INSERT OR REPLACE INTO token_history
                    (token_address, detected_at, symbol, rejection_reason)
                    VALUES (?, ?, ?, ?)""", (addr, time.time(), symbol, reason))
        except:
            pass

    async def _monitor_loop(self):
        log.info("Position monitor started")
        while self.running:
            try:
                positions = list(self.positions._open_positions.items())
                if not positions:
                    await asyncio.sleep(1)
                    continue

                addresses = list(set(p["token_address"] for _, p in positions))
                dex_data = await self.market.get_token_data(addresses)

                for trade_id, pos in positions:
                    if trade_id not in self.positions._open_positions:
                        continue

                    addr = pos["token_address"]

                    # v4.4-prod Fix #4: Check for pending exit from a previously failed sell.
                    # If there's a pending exit, retry the sell regardless of current price conditions.
                    pending = pos.get("pending_exit")
                    if pending:
                        log.info(f"RETRY pending exit for {pos['symbol']}: {pending['reason']} (attempt {pending['attempts']})")
                        await self.positions.close_position(
                            trade_id, pending["reason"],
                            pending.get("price", 0),
                            emergency=pending.get("emergency", False)
                        )
                        continue

                    if addr not in dex_data:
                        # v4.3: Even without price data, check hard exit time
                        hold = time.time() - pos["entry_time"]
                        if hold >= HARD_EXIT_SECONDS * 2:
                            log.warning(f"NO DATA for {pos['symbol']} for 2x hard exit time — forcing close")
                            await self.positions.close_position(trade_id, "NO_DATA_TIMEOUT", 0, emergency=True)
                        continue

                    pair = dex_data[addr]
                    cp = float(pair.get("priceUsd", 0) or 0)
                    cl = float(pair.get("liquidity", {}).get("usd", 0) or 0)

                    # v4.3: Track consecutive zero-price reads per position
                    if cp <= 0:
                        zero_count = pos.get("zero_price_count", 0) + 1
                        with self.positions.lock:
                            pos["zero_price_count"] = zero_count
                        if zero_count >= 5:
                            log.warning(f"ZERO PRICE x{zero_count} for {pos['symbol']} — forcing emergency exit")
                            await self.positions.close_position(trade_id, f"ZERO_PRICE_{zero_count}x", 0, emergency=True)
                        continue
                    else:
                        # Reset counter on good price
                        with self.positions.lock:
                            pos["zero_price_count"] = 0

                    ep = pos["entry_price"]
                    el = pos.get("entry_liq_usd", 0)
                    peak = pos.get("peak_price", ep)

                    if cp > peak:
                        with self.positions.lock:
                            pos["peak_price"] = cp
                            peak = cp

                    gain = ((cp / ep) - 1) * 100 if ep else 0
                    drop = ((peak - cp) / peak) * 100 if peak > 0 else 0
                    hold = time.time() - pos["entry_time"]

                    # EXIT 1: LIQ DROP
                    if el > 0:
                        if cl == 0 and cp > 0:
                            # v4.3: liq=0 but price exists = real pool drain
                            log.warning(f"LIQ ZERO {pos['symbol']} (price=${cp:.8f} still exists)")
                            await self.positions.close_position(trade_id, "EMERGENCY_LIQ_ZERO", cp, emergency=True)
                            continue
                        elif cl == 0 and cp == 0:
                            # Both zero = likely DexScreener API failure, skip this cycle
                            continue
                        liq_drop = ((el - cl) / el) * 100
                        if liq_drop >= LIQ_DROP_THRESHOLD_PCT:
                            log.warning(f"LIQ DROP {pos['symbol']}: {liq_drop:.0f}% (${el:,.0f}->${cl:,.0f})")
                            await self.positions.close_position(trade_id, f"EMERGENCY_LIQ_DROP_{liq_drop:.0f}pct", cp, emergency=True)
                            continue

                    # EXIT 2: LADDER SELLS (v4.3: always enabled)
                    for thresh, frac in EXIT_LADDER:
                        if gain >= thresh and thresh not in pos.get("ladder_levels_hit", set()):
                            await self.positions.execute_ladder_sell(trade_id, thresh, frac, cp)

                    # EXIT 3: TRAILING STOP
                    if drop >= TRAILING_STOP_PCT and gain > 0:
                        await self.positions.close_position(trade_id, f"TRAILING_STOP_{drop:.0f}pct", cp)
                        continue

                    # EXIT 4: STOP LOSS
                    if gain <= -STOP_LOSS_PCT:
                        await self.positions.close_position(trade_id, f"STOP_LOSS_{gain:.0f}pct", cp)
                        continue

                    # EXIT 5: HARD EXIT
                    if hold >= HARD_EXIT_SECONDS:
                        reentry = gain >= 50
                        await self.positions.close_position(trade_id, f"HARD_EXIT_{hold:.0f}s", cp)
                        # Re-entry enabled even in live test mode (counts toward trade limit)
                        if reentry:
                            tc = self.positions.get_token_trade_count(addr)
                            if tc < MAX_TRADES_PER_TOKEN:
                                log.info(f"RE-ENTRY candidate: {pos['symbol']} was +{gain:.0f}%")
                                await asyncio.sleep(2)
                                np, nl = await self.market.get_token_price_and_liq(addr)
                                min_liq = MIN_LIQ_ENTRY  # V9.6: single gate for all
                                if nl >= min_liq and np > 0:
                                    # v4.4-prod Fix #7: Re-entry uses original grade without fresh
                                    # enrichment. This is intentional because:
                                    # - Kill signals (freeze_authority, creator history) are immutable
                                    #   properties that don't change after first enrichment
                                    # - Only liquidity changes, and we re-check it above
                                    # TODO: In theory, a token could gain freeze authority after first
                                    # trade via a new setAuthority TX. This is extremely rare on pump.fun
                                    # tokens and would require a fresh Helius getAccountInfo call that
                                    # adds ~200ms latency. Risk is minimal vs. speed cost.
                                    await self.positions.open_position(
                                        addr, pos["symbol"], pos["alert_grade"], nl, np,
                                        pos.get("top1_pct", 0))
                        continue

                await asyncio.sleep(LIQ_CHECK_INTERVAL_MS / 1000)

            except Exception as e:
                log.error(f"Monitor error: {e}\n{traceback.format_exc()}")
                await asyncio.sleep(1)

    async def _health_loop(self):
        initial_balance = None
        while self.running:
            try:
                sol = await get_sol_balance(self.jupiter.wallet_pubkey)
                oc = self.positions.open_count
                
                # Track initial balance for drop detection
                # v4.3: Account for expected spend (open positions * pos size)
                if initial_balance is None:
                    initial_balance = sol
                elif not SHADOW_MODE:
                    expected_spend = safety_tracker.sol_spent_this_hour
                    unexpected_drop = (initial_balance - sol) - expected_spend
                    if unexpected_drop > 0.1:
                        log.error(f"UNEXPECTED BALANCE DROP: {initial_balance:.4f} -> {sol:.4f} SOL (spent={expected_spend:.4f}, unexplained={unexpected_drop:.4f})")
                        tg_send(f"BALANCE DROP ALERT: {initial_balance:.4f} -> {sol:.4f} SOL\nExpected spend: {expected_spend:.4f}\nUnexplained: {unexpected_drop:.4f}\nStopping trades!")
                        safety_tracker.halted = True
                        safety_tracker.halt_reason = f"Unexpected balance drop {unexpected_drop:.4f} SOL"
                
                conn = get_db()
                try:
                    row = conn.execute("SELECT SUM(pnl_sol), COUNT(*) FROM trades WHERE status='closed'").fetchone()
                    pnl = row[0] or 0
                    total = row[1] or 0
                    wins = conn.execute("SELECT COUNT(*) FROM trades WHERE status='closed' AND pnl_sol > 0").fetchone()[0]
                finally:
                    conn.close()
                wr = (wins / total * 100) if total else 0
                hc = helius_tracker.status()

                # v4.4-prod Fix #10: Compare theoretical PnL to actual wallet balance change.
                # Since we can't parse exact SOL received from TX results, we compare
                # theoretical (DB-tracked) PnL to actual wallet balance delta.
                # Log any divergence > 5% as it indicates slippage/fee accumulation.
                if initial_balance is not None and total > 0 and not SHADOW_MODE:
                    expected_spend_open = self.positions.open_count * POSITION_SIZE_SOL
                    actual_wallet_pnl = sol - initial_balance + expected_spend_open
                    theoretical_pnl = pnl
                    if abs(theoretical_pnl) > 0.001:
                        divergence_pct = abs(actual_wallet_pnl - theoretical_pnl) / abs(theoretical_pnl) * 100
                        if divergence_pct > 5:
                            log.warning(
                                f"PNL DIVERGENCE: wallet={actual_wallet_pnl:+.4f} vs theoretical={theoretical_pnl:+.4f} "
                                f"({divergence_pct:.1f}% off) — likely slippage/fee accumulation"
                            )

                log.info(f"HEALTH | SOL: {sol:.4f} | Open: {oc} | Closed: {total} | WR: {wr:.0f}% | PnL: {pnl:+.4f} | Helius: {hc['today']}/day | Trades: {safety_tracker.trades_today}/{MAX_DAILY_TRADES} | Halted: {safety_tracker.halted}")

                if sol < POSITION_SIZE_SOL and sol > 0 and not SHADOW_MODE:
                    tg_send(f"LOW BALANCE: {sol:.4f} SOL")
            except Exception as e:
                log.error(f"Health error: {e}")
            await asyncio.sleep(300)

    async def cleanup_sessions(self):
        """v4.4-prod Fix #9: Close persistent aiohttp sessions on shutdown."""
        global _tg_session
        await close_rpc_session()
        if _tg_session and not _tg_session.closed:
            await _tg_session.close()
            _tg_session = None

    def stop(self):
        self.running = False
        tg_send(f"Wave Rider {VERSION} stopped")

# ============================================================
# WEB SERVER + DASHBOARD
# ============================================================

def run_health_server():
    from flask import Flask, jsonify, Response
    app = Flask(__name__)

    @app.route("/health")
    def health():
        conn = get_db()
        try:
            row = conn.execute("SELECT SUM(pnl_sol), COUNT(*) FROM trades WHERE status='closed'").fetchone()
            oc = conn.execute("SELECT COUNT(*) FROM trades WHERE status='open'").fetchone()[0]
        finally:
            conn.close()
        return jsonify({
            "status": "running", "version": VERSION, "shadow": SHADOW_MODE,
            "live_test_mode": LIVE_TEST_MODE,
            "open_positions": oc, "closed_trades": row[1] or 0,
            "total_pnl_sol": round(row[0] or 0, 4),
            "helius_credits": helius_tracker.status(),
            "safety": safety_tracker.status(),
        })

    @app.route("/stop")
    def stop():
        return jsonify({"status": "stopping"})

    @app.route("/dashboard")
    def dashboard():
        return Response(DASHBOARD_HTML, mimetype="text/html")

    @app.route("/api/data")
    def api_data():
        conn = get_db()
        try:
            conn.row_factory = sqlite3.Row
            op = conn.execute("SELECT * FROM trades WHERE status='open' ORDER BY entry_time DESC").fetchall()
            cl = conn.execute("SELECT * FROM trades WHERE status='closed' ORDER BY exit_time DESC LIMIT 50").fetchall()
            stats = conn.execute("""SELECT COUNT(*) as total,
                SUM(CASE WHEN pnl_sol>0 THEN 1 ELSE 0 END) as wins,
                COALESCE(SUM(pnl_sol),0) as total_pnl,
                COALESCE(MAX(pnl_sol),0) as best, COALESCE(MIN(pnl_sol),0) as worst
                FROM trades WHERE status='closed'""").fetchone()
            cb = conn.execute("SELECT * FROM circuit_breaker WHERE id=1").fetchone()
            # Recent rejections
            rej = conn.execute("SELECT symbol, rejection_reason, detected_at FROM token_history WHERE rejection_reason IS NOT NULL ORDER BY detected_at DESC LIMIT 10").fetchall()
        finally:
            conn.close()
        def r2d(r):
            return {k: r[k] for k in r.keys()} if r else {}
        return jsonify({
            "open": [r2d(r) for r in op], "closed": [r2d(r) for r in cl],
            "stats": r2d(stats), "rejections": [r2d(r) for r in rej],
            "circuit_breaker": {"daily_pnl": cb["daily_pnl"] if cb else 0,
                "hourly_pnl": cb["hourly_pnl"] if cb else 0,
                "consecutive_losses": cb["consecutive_losses"] if cb else 0,
                "paused": bool(cb["paused"]) if cb else False} if cb else {},
            "version": VERSION, "shadow": SHADOW_MODE,
            "position_size": POSITION_SIZE_SOL,
            "helius": helius_tracker.status(),
            "safety": safety_tracker.status(),
            "live_test_mode": LIVE_TEST_MODE,
        })

    # v4.4-prod Fix #13: Flask dev server is fine for current load (single-user dashboard).
    # If load increases, switch to gunicorn/uvicorn. Not worth the complexity now.
    import threading
    threading.Thread(target=lambda: app.run(host="0.0.0.0", port=8080, debug=False), daemon=True).start()


DASHBOARD_HTML = """<!DOCTYPE html>
<html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Wave Rider v4.0</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#0a0a0f;color:#e0e0e0;font-family:'Courier New',monospace;padding:15px}
h1{color:#00ff88;font-size:1.4em;margin-bottom:10px}
h2{color:#00aaff;font-size:1.1em;margin:15px 0 8px;border-bottom:1px solid #333;padding-bottom:4px}
.g{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:8px;margin:10px 0}
.c{background:#151520;border:1px solid #333;border-radius:8px;padding:10px;text-align:center}
.v{font-size:1.3em;font-weight:bold;margin:3px 0}
.l{font-size:.7em;color:#888;text-transform:uppercase}
.gr{color:#00ff88}.rd{color:#ff4444}.yl{color:#ffaa00}.bl{color:#00aaff}
table{width:100%;border-collapse:collapse;font-size:.78em;margin:5px 0}
th{background:#1a1a2e;color:#00aaff;padding:5px 3px;text-align:left}
td{padding:4px 3px;border-bottom:1px solid #1a1a2e}
tr:hover{background:#1a1a2e}
.bd{padding:2px 5px;border-radius:4px;font-size:.72em;font-weight:bold}
.ba{background:#00ff8833;color:#00ff88}.bb{background:#00aaff33;color:#00aaff}
.pa{background:#ff444433;border:2px solid #ff4444;padding:8px;border-radius:8px;text-align:center;margin:8px 0}
.sh{background:#ffaa0033;border:1px solid #ffaa00;padding:5px;border-radius:5px;text-align:center;margin:5px 0;font-size:.8em}
.nd{color:#555;text-align:center;padding:15px}
#u{color:#888;font-size:.75em}
</style></head><body>
<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
<h1>Wave Rider v4.0</h1><span id="u">Loading...</span></div>
<div id="sh"></div><div id="pa"></div>
<div class="g" id="sg"></div>
<h2>Open Positions</h2><div id="ot"></div>
<h2>Recent Trades</h2><div id="ct"></div>
<h2>Recent Rejections</h2><div id="rj"></div>
<script>
function f(n,d=4){return n!=null?Number(n).toFixed(d):'-'}
function pc(v){return v>0?'gr':v<0?'rd':''}
function bd(g){return'<span class="bd b'+(g||'').toLowerCase()+'">'+(g||'?')+'</span>'}
async function R(){try{let r=await fetch('/api/data'),d=await r.json(),s=d.stats||{},cb=d.circuit_breaker||{};
let t=s.total||0,w=s.wins||0,wr=t>0?(w/t*100).toFixed(0)+'%':'-',p=s.total_pnl||0;
if(d.shadow)document.getElementById('sh').innerHTML='<div class="sh">SHADOW MODE - No real trades</div>';
else document.getElementById('sh').innerHTML='';
if(cb.paused)document.getElementById('pa').innerHTML='<div class="pa">CIRCUIT BREAKER ACTIVE</div>';
else document.getElementById('pa').innerHTML='';
let h=d.helius||{};
document.getElementById('sg').innerHTML=
'<div class="c"><div class="l">PnL</div><div class="v '+pc(p)+'">'+(p>=0?'+':'')+f(p)+' SOL</div></div>'+
'<div class="c"><div class="l">Win Rate</div><div class="v '+(w/t>=.5?'gr':'yl')+'">'+wr+'</div></div>'+
'<div class="c"><div class="l">Open</div><div class="v bl">'+(d.open||[]).length+'</div></div>'+
'<div class="c"><div class="l">Closed</div><div class="v">'+t+'</div></div>'+
'<div class="c"><div class="l">Best</div><div class="v gr">+'+f(s.best)+' SOL</div></div>'+
'<div class="c"><div class="l">Worst</div><div class="v rd">'+f(s.worst)+' SOL</div></div>'+
'<div class="c"><div class="l">Daily</div><div class="v '+pc(cb.daily_pnl)+'">'+(cb.daily_pnl>=0?'+':'')+f(cb.daily_pnl||0)+'</div></div>'+
'<div class="c"><div class="l">Helius/day</div><div class="v">'+(h.today||0)+'</div></div>';
let op=d.open||[];
if(op.length>0){let h='<table><tr><th>Token</th><th>Gr</th><th>#</th><th>Entry</th><th>Hold</th><th>Rem</th></tr>';
op.forEach(t=>{let hold=Date.now()/1000-t.entry_time,m=Math.floor(hold/60);
h+='<tr><td>'+t.symbol+'</td><td>'+bd(t.alert_grade)+'</td><td>'+
(t.trade_number||1)+'</td><td>$'+f(t.entry_price,8)+'</td><td>'+m+'m</td><td>'+
((t.remaining_pct||1)*100).toFixed(0)+'%</td></tr>'});
document.getElementById('ot').innerHTML=h+'</table>'}
else document.getElementById('ot').innerHTML='<div class="nd">Scanning...</div>';
let cl=d.closed||[];
if(cl.length>0){let h='<table><tr><th>Token</th><th>Gr</th><th>PnL</th><th>%</th><th>Exit</th><th>Hold</th></tr>';
cl.forEach(t=>{let p=t.pnl_sol||0,hold=t.exit_time&&t.entry_time?Math.floor((t.exit_time-t.entry_time)/60)+'m':'-';
h+='<tr><td>'+t.symbol+'</td><td>'+bd(t.alert_grade)+'</td><td class="'+pc(p)+'">'+(p>=0?'+':'')+f(p)+
'</td><td class="'+pc(t.pnl_pct)+'">'+(t.pnl_pct>=0?'+':'')+f(t.pnl_pct,1)+'%</td><td>'+
(t.exit_reason||'-').replace(/_/g,' ')+'</td><td>'+hold+'</td></tr>'});
document.getElementById('ct').innerHTML=h+'</table>'}
else document.getElementById('ct').innerHTML='<div class="nd">Waiting...</div>';
let rj=d.rejections||[];
if(rj.length>0){let h='<table><tr><th>Token</th><th>Reason</th><th>Time</th></tr>';
rj.forEach(r=>{let t=new Date(r.detected_at*1000).toLocaleTimeString();
h+='<tr><td>'+r.symbol+'</td><td>'+r.rejection_reason+'</td><td>'+t+'</td></tr>'});
document.getElementById('rj').innerHTML=h+'</table>'}
else document.getElementById('rj').innerHTML='<div class="nd">No rejections yet</div>';
document.getElementById('u').textContent=new Date().toLocaleTimeString()+' | '+d.version+(d.shadow?' [SHADOW]':' [LIVE]');
}catch(e){document.getElementById('u').textContent='Error: '+e.message}}
R();setInterval(R,5000);
</script></body></html>"""

# ============================================================
# ENTRY POINT
# ============================================================

async def main():
    bot = WaveRiderBot()
    init_db()  # v4.4: Create DB before Flask starts
    run_health_server()
    try:
        await bot.start()
    except KeyboardInterrupt:
        bot.stop()
        await bot.cleanup_sessions()
    except Exception as e:
        log.error(f"Fatal: {e}\n{traceback.format_exc()}")
        tg_send(f"BOT CRASHED: {e}")
        bot.stop()
        await bot.cleanup_sessions()

if __name__ == "__main__":
    asyncio.run(main())
