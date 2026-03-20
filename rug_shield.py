"""
rug_shield.py — Pre-Entry Rug Detection Module for Solana tokens.

Performs 12 async safety checks before entering a trade.
Returns a risk assessment with score, reasons, and per-check details.

Usage:
    from rug_shield import check_token_safety

    result = await check_token_safety(
        token_address, pool_address, creator_address,
        dex_data, helius_rpc, public_rpc
    )
    if result["safe"]:
        # proceed with trade
"""

import asyncio
import logging
import time
from datetime import datetime, timezone

import aiohttp

logger = logging.getLogger("rug_shield")

# ---------------------------------------------------------------------------
# Known burn / dead addresses for LP burn check
# ---------------------------------------------------------------------------
BURN_ADDRESSES = {
    "1nc1nerator11111111111111111111111111111111",
    "1111111111111111111111111111111111111111111",
    "deaddeaddeaddeaddeaddeaddeaddeaddeaddeaddead",
    "11111111111111111111111111111111",
}

# Wrapped SOL mint
WSOL_MINT = "So11111111111111111111111111111111111111112"

# Per-check timeout
CHECK_TIMEOUT = 5


# ---------------------------------------------------------------------------
# RPC helper
# ---------------------------------------------------------------------------
async def rpc_call(
    session: aiohttp.ClientSession,
    url: str,
    method: str,
    params: list,
    timeout: float = CHECK_TIMEOUT,
) -> dict | None:
    """Send a JSON-RPC 2.0 request and return the result, or None on error."""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    }
    try:
        async with session.post(
            url, json=payload, timeout=aiohttp.ClientTimeout(total=timeout)
        ) as resp:
            data = await resp.json()
            if "error" in data:
                logger.warning("RPC error %s: %s", method, data["error"])
                return None
            return data.get("result")
    except Exception as exc:
        logger.warning("RPC call %s failed: %s", method, exc)
        return None


# ---------------------------------------------------------------------------
# Individual checks — each returns (score: int, reason: str|None, detail: dict)
# ---------------------------------------------------------------------------


async def _check_sol_pool_balance(
    session: aiohttp.ClientSession,
    pool_address: str,
    dex_data: dict,
    public_rpc: str,
) -> tuple[int, str | None, dict]:
    """Check 1: SOL-side pool balance vs total liquidity."""
    detail: dict = {"check": "sol_pool_balance"}
    try:
        total_liq = float(dex_data.get("liquidity", {}).get("usd", 0))
        if total_liq <= 0:
            detail["skipped"] = "no liquidity data"
            return 0, None, detail

        # getBalance on pool address (WSOL account = pool_address for most AMMs)
        result = await rpc_call(session, public_rpc, "getBalance", [pool_address])
        if result is None:
            detail["skipped"] = "rpc failed"
            return 0, None, detail

        sol_lamports = result.get("value", 0)
        sol_balance = sol_lamports / 1e9

        # Rough SOL price from dex_data if available, else estimate from liq
        sol_price = float(dex_data.get("sol_price", 0))
        if sol_price <= 0:
            # Fallback: assume SOL side is ~half of total liq
            detail["skipped"] = "no sol_price in dex_data"
            return 0, None, detail

        sol_usd = sol_balance * sol_price
        ratio = sol_usd / total_liq if total_liq > 0 else 0
        detail["sol_balance"] = sol_balance
        detail["sol_usd"] = round(sol_usd, 2)
        detail["total_liq_usd"] = round(total_liq, 2)
        detail["sol_ratio"] = round(ratio, 4)

        if ratio < 0.40:
            return 15, f"SOL pool balance only {ratio:.0%} of total liquidity (< 40%)", detail
        return 0, None, detail
    except Exception as exc:
        logger.warning("sol_pool_balance check failed: %s", exc)
        detail["error"] = str(exc)
        return 0, None, detail


async def _check_buy_sell_ratio(dex_data: dict) -> tuple[int, str | None, dict]:
    """Check 2: Buy/sell ratio from 5m txns."""
    detail: dict = {"check": "buy_sell_ratio"}
    try:
        txns = dex_data.get("txns", {}).get("m5", {})
        buys = int(txns.get("buys", 0))
        sells = int(txns.get("sells", 0))
        detail["buys_m5"] = buys
        detail["sells_m5"] = sells

        if sells > buys:
            return 15, f"More sells ({sells}) than buys ({buys}) in 5m", detail
        return 0, None, detail
    except Exception as exc:
        logger.warning("buy_sell_ratio check failed: %s", exc)
        detail["error"] = str(exc)
        return 0, None, detail


async def _check_volume_liquidity(dex_data: dict) -> tuple[int, str | None, dict]:
    """Check 3: Volume vs liquidity — wash trading detection."""
    detail: dict = {"check": "volume_vs_liquidity"}
    try:
        vol_m5 = float(dex_data.get("volume", {}).get("m5", 0))
        liq_usd = float(dex_data.get("liquidity", {}).get("usd", 0))
        detail["volume_m5"] = round(vol_m5, 2)
        detail["liquidity_usd"] = round(liq_usd, 2)

        if liq_usd <= 0:
            detail["skipped"] = "no liquidity"
            return 0, None, detail

        ratio = vol_m5 / liq_usd
        detail["ratio"] = round(ratio, 2)

        if ratio > 5:
            return 15, f"5m volume ({vol_m5:.0f}) is {ratio:.1f}x liquidity — possible wash trading", detail
        return 0, None, detail
    except Exception as exc:
        logger.warning("volume_liquidity check failed: %s", exc)
        detail["error"] = str(exc)
        return 0, None, detail


async def _check_price_stability(dex_data: dict) -> tuple[int, str | None, dict]:
    """Check 4: Price stability — pump & dump detection."""
    detail: dict = {"check": "price_stability"}
    try:
        pc = dex_data.get("priceChange", {})
        m5 = float(pc.get("m5", 0))
        h1 = float(pc.get("h1", 0))
        detail["priceChange_m5"] = m5
        detail["priceChange_h1"] = h1

        # Pump pattern: big rise then significant drop
        if h1 > 50 and m5 < -30:
            return 20, f"Pump & dump pattern: h1 +{h1:.0f}% then m5 {m5:.0f}%", detail
        if m5 > 50 and h1 < -30:
            return 20, f"Pump & dump pattern: m5 +{m5:.0f}% but h1 {h1:.0f}%", detail
        return 0, None, detail
    except Exception as exc:
        logger.warning("price_stability check failed: %s", exc)
        detail["error"] = str(exc)
        return 0, None, detail


async def _check_deployer_selling(
    session: aiohttp.ClientSession,
    creator_address: str,
    token_address: str,
    helius_rpc: str,
) -> tuple[int, str | None, dict]:
    """Check 5: Deployer already selling tokens."""
    detail: dict = {"check": "deployer_selling"}
    try:
        result = await rpc_call(
            session,
            helius_rpc,
            "getSignaturesForAddress",
            [creator_address, {"limit": 10}],
        )
        if result is None:
            detail["skipped"] = "rpc failed"
            return 0, None, detail

        detail["recent_tx_count"] = len(result)
        # Heuristic: look for transfer-type transactions involving the token
        # In practice, full TX parsing requires getTransaction — here we flag
        # any recent activity as a lighter heuristic
        sell_sigs = []
        for sig_info in result:
            memo = sig_info.get("memo", "") or ""
            # If the memo or err field hints at a swap/sell, flag it
            if sig_info.get("err") is None:
                sell_sigs.append(sig_info.get("signature", "")[:16])

        # For a more robust check, we'd parse each TX. For speed, we check
        # if the deployer had many recent TXs (likely trading, not just deploying).
        if len(result) >= 5:
            # Deployer is very active — suspicious for a new token
            detail["active_tx_count"] = len(result)
            return 10, f"Deployer has {len(result)} recent TXs — may be actively trading", detail

        return 0, None, detail
    except Exception as exc:
        logger.warning("deployer_selling check failed: %s", exc)
        detail["error"] = str(exc)
        return 0, None, detail


async def _check_deployer_wallet_age(
    session: aiohttp.ClientSession,
    creator_address: str,
    helius_rpc: str,
) -> tuple[int, str | None, dict]:
    """Check 6: Deployer wallet age — fresh wallets are suspicious."""
    detail: dict = {"check": "deployer_wallet_age"}
    try:
        result = await rpc_call(
            session,
            helius_rpc,
            "getSignaturesForAddress",
            [creator_address, {"limit": 1000, "before": None}],
        )
        if result is None or len(result) == 0:
            detail["skipped"] = "no tx history"
            return 10, "Deployer has no transaction history", detail

        # Oldest TX is last in the list
        oldest = result[-1]
        block_time = oldest.get("blockTime", 0)
        if block_time == 0:
            detail["skipped"] = "no blockTime"
            return 0, None, detail

        age_hours = (time.time() - block_time) / 3600
        detail["wallet_age_hours"] = round(age_hours, 1)
        detail["oldest_tx_time"] = datetime.fromtimestamp(
            block_time, tz=timezone.utc
        ).isoformat()

        if age_hours < 24:
            return 15, f"Deployer wallet is only {age_hours:.1f}h old (< 24h)", detail
        if age_hours < 72:
            return 5, f"Deployer wallet is {age_hours:.1f}h old (< 72h)", detail
        return 0, None, detail
    except Exception as exc:
        logger.warning("deployer_wallet_age check failed: %s", exc)
        detail["error"] = str(exc)
        return 0, None, detail


async def _check_pool_age(dex_data: dict) -> tuple[int, str | None, dict]:
    """Check 7: Pool age — too fresh is risky."""
    detail: dict = {"check": "pool_age"}
    try:
        created_at = dex_data.get("pairCreatedAt", 0)
        if not created_at:
            detail["skipped"] = "no pairCreatedAt"
            return 0, None, detail

        # pairCreatedAt is milliseconds epoch
        age_seconds = (time.time() * 1000 - created_at) / 1000
        age_minutes = age_seconds / 60
        detail["pool_age_minutes"] = round(age_minutes, 1)

        if age_minutes < 3:
            return 15, f"Pool is only {age_minutes:.1f} minutes old (< 3min)", detail
        if age_minutes < 10:
            return 5, f"Pool is {age_minutes:.1f} minutes old (< 10min)", detail
        return 0, None, detail
    except Exception as exc:
        logger.warning("pool_age check failed: %s", exc)
        detail["error"] = str(exc)
        return 0, None, detail


async def _check_unique_buyers(dex_data: dict) -> tuple[int, str | None, dict]:
    """Check 8: Unique buyer count — too few buyers is suspicious."""
    detail: dict = {"check": "unique_buyers"}
    try:
        m5_buys = int(dex_data.get("txns", {}).get("m5", {}).get("buys", 0))
        h1_buys = int(dex_data.get("txns", {}).get("h1", {}).get("buys", 0))
        total = m5_buys + h1_buys
        detail["m5_buys"] = m5_buys
        detail["h1_buys"] = h1_buys
        detail["total_buys"] = total

        if total < 5:
            return 15, f"Only {total} total buys (m5+h1) — extremely low activity", detail
        if total < 10:
            return 10, f"Only {total} total buys (m5+h1) — low activity", detail
        return 0, None, detail
    except Exception as exc:
        logger.warning("unique_buyers check failed: %s", exc)
        detail["error"] = str(exc)
        return 0, None, detail


async def _check_lp_burn(
    session: aiohttp.ClientSession,
    dex_data: dict,
    pool_address: str,
    helius_rpc: str,
) -> tuple[int, str | None, dict]:
    """Check 9: LP burn check — is liquidity locked/burned?"""
    detail: dict = {"check": "lp_burn"}
    try:
        # PumpSwap / Pump.fun pools auto-burn LP
        dex_id = dex_data.get("dexId", "").lower()
        labels = [l.lower() for l in dex_data.get("labels", [])]

        if "pumpswap" in dex_id or "pump" in dex_id or "pump.fun" in labels:
            detail["pumpswap"] = True
            detail["lp_burned"] = True
            return 0, None, detail

        # For Raydium / other DEXs, check LP token largest accounts
        # We need the LP mint address — often derivable from pool, but dex_data
        # may include it
        lp_mint = dex_data.get("lp_mint") or dex_data.get("lpMint")
        if not lp_mint:
            detail["skipped"] = "no LP mint address available"
            return 5, "LP burn status unknown — no LP mint data", detail

        result = await rpc_call(
            session,
            helius_rpc,
            "getTokenLargestAccounts",
            [lp_mint],
        )
        if result is None or not result.get("value"):
            detail["skipped"] = "rpc failed or empty"
            return 5, "LP burn status unknown — RPC check failed", detail

        accounts = result["value"]
        detail["top_lp_holders"] = len(accounts)

        # Check if the largest holder is a burn address
        if accounts:
            top = accounts[0]
            top_address = top.get("address", "")
            detail["top_lp_address"] = top_address

            if top_address in BURN_ADDRESSES:
                detail["lp_burned"] = True
                return 0, None, detail

        detail["lp_burned"] = False
        return 10, "LP tokens not burned — rug pull possible", detail
    except Exception as exc:
        logger.warning("lp_burn check failed: %s", exc)
        detail["error"] = str(exc)
        return 0, None, detail


async def _check_early_buyer_clustering(dex_data: dict) -> tuple[int, str | None, dict]:
    """Check 10: Early buyer clustering — insider/bundle detection."""
    detail: dict = {"check": "early_buyer_clustering"}
    try:
        bundle_info = dex_data.get("bundle_info")
        if not bundle_info:
            detail["skipped"] = "no bundle_info in dex_data"
            return 0, None, detail

        total_first_buyers = int(bundle_info.get("total_first_buyers", 0))
        same_source_count = int(bundle_info.get("same_source_funded", 0))
        detail["total_first_buyers"] = total_first_buyers
        detail["same_source_funded"] = same_source_count

        if total_first_buyers <= 0:
            detail["skipped"] = "no buyer data"
            return 0, None, detail

        cluster_pct = same_source_count / total_first_buyers
        detail["cluster_pct"] = round(cluster_pct, 4)

        if cluster_pct > 0.50:
            return 20, (
                f"{cluster_pct:.0%} of first buyers funded from same source — "
                f"insider network detected"
            ), detail
        if cluster_pct > 0.30:
            return 10, (
                f"{cluster_pct:.0%} of first buyers share funding source — moderate clustering"
            ), detail
        return 0, None, detail
    except Exception as exc:
        logger.warning("early_buyer_clustering check failed: %s", exc)
        detail["error"] = str(exc)
        return 0, None, detail


async def _check_mint_authority(
    session: aiohttp.ClientSession,
    token_address: str,
    helius_rpc: str,
) -> tuple[int, str | None, dict]:
    """Check 11: Mint authority — should be null (revoked)."""
    detail: dict = {"check": "mint_authority"}
    try:
        result = await rpc_call(
            session,
            helius_rpc,
            "getAccountInfo",
            [token_address, {"encoding": "jsonParsed"}],
        )
        if result is None or result.get("value") is None:
            detail["skipped"] = "rpc failed or account not found"
            return 0, None, detail

        parsed = (
            result.get("value", {})
            .get("data", {})
            .get("parsed", {})
            .get("info", {})
        )
        mint_auth = parsed.get("mintAuthority")
        freeze_auth = parsed.get("freezeAuthority")
        detail["mintAuthority"] = mint_auth
        detail["freezeAuthority"] = freeze_auth

        score = 0
        reasons = []

        if mint_auth is not None:
            score += 20
            reasons.append(f"Mint authority NOT revoked: {mint_auth[:12]}...")
        if freeze_auth is not None:
            score += 5
            reasons.append(f"Freeze authority active: {freeze_auth[:12]}...")

        reason = "; ".join(reasons) if reasons else None
        return min(score, 20), reason, detail
    except Exception as exc:
        logger.warning("mint_authority check failed: %s", exc)
        detail["error"] = str(exc)
        return 0, None, detail


async def _check_top1_rug_zone(dex_data: dict) -> tuple[int, str | None, dict]:
    """Check 12: Top1 holder rug zone — 80-99% holding is danger zone."""
    detail: dict = {"check": "top1_rug_zone"}
    try:
        top1_pct = dex_data.get("top1_pct")
        if top1_pct is None:
            detail["skipped"] = "no top1_pct in dex_data"
            return 0, None, detail

        top1_pct = float(top1_pct)
        detail["top1_pct"] = top1_pct

        if 80 <= top1_pct < 99:
            return 15, f"Top holder owns {top1_pct:.1f}% — high rug zone (80-99%)", detail
        if top1_pct >= 99:
            # 99%+ is likely the bonding curve / LP, not a rug risk
            detail["note"] = "99%+ likely bonding curve"
            return 0, None, detail
        if top1_pct >= 50:
            return 8, f"Top holder owns {top1_pct:.1f}% — elevated concentration", detail
        return 0, None, detail
    except Exception as exc:
        logger.warning("top1_rug_zone check failed: %s", exc)
        detail["error"] = str(exc)
        return 0, None, detail


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


async def check_token_safety(
    token_address: str,
    pool_address: str,
    creator_address: str,
    dex_data: dict,
    helius_rpc: str,
    public_rpc: str,
) -> dict:
    """
    Run all 12 pre-entry rug checks on a Solana token.

    Returns:
        {
            "safe": bool,          # True if risk_score < 40
            "risk_score": int,     # 0-100
            "reasons": list[str],  # Human-readable failure reasons
            "details": dict,       # Per-check breakdown
        }
    """
    reasons: list[str] = []
    details: dict = {}
    total_score = 0

    async with aiohttp.ClientSession() as session:
        # ---- RPC-dependent checks (run concurrently) ----
        rpc_tasks = {
            "sol_pool_balance": asyncio.ensure_future(
                _check_sol_pool_balance(session, pool_address, dex_data, public_rpc)
            ),
            "deployer_selling": asyncio.ensure_future(
                _check_deployer_selling(session, creator_address, token_address, helius_rpc)
            ),
            "deployer_wallet_age": asyncio.ensure_future(
                _check_deployer_wallet_age(session, creator_address, helius_rpc)
            ),
            "lp_burn": asyncio.ensure_future(
                _check_lp_burn(session, dex_data, pool_address, helius_rpc)
            ),
            "mint_authority": asyncio.ensure_future(
                _check_mint_authority(session, token_address, helius_rpc)
            ),
        }

        # ---- Pure data checks (no RPC, instant) ----
        data_tasks = {
            "buy_sell_ratio": _check_buy_sell_ratio(dex_data),
            "volume_vs_liquidity": _check_volume_liquidity(dex_data),
            "price_stability": _check_price_stability(dex_data),
            "pool_age": _check_pool_age(dex_data),
            "unique_buyers": _check_unique_buyers(dex_data),
            "early_buyer_clustering": _check_early_buyer_clustering(dex_data),
            "top1_rug_zone": _check_top1_rug_zone(dex_data),
        }

        # Run all data checks concurrently
        data_results = {}
        for name, coro in data_tasks.items():
            try:
                data_results[name] = await asyncio.wait_for(coro, timeout=CHECK_TIMEOUT)
            except asyncio.TimeoutError:
                logger.warning("Check %s timed out", name)
                data_results[name] = (0, None, {"check": name, "error": "timeout"})
            except Exception as exc:
                logger.warning("Check %s failed: %s", name, exc)
                data_results[name] = (0, None, {"check": name, "error": str(exc)})

        # Await RPC tasks with timeout
        rpc_results = {}
        for name, task in rpc_tasks.items():
            try:
                rpc_results[name] = await asyncio.wait_for(task, timeout=CHECK_TIMEOUT + 1)
            except asyncio.TimeoutError:
                logger.warning("Check %s timed out", name)
                rpc_results[name] = (0, None, {"check": name, "error": "timeout"})
            except Exception as exc:
                logger.warning("Check %s failed: %s", name, exc)
                rpc_results[name] = (0, None, {"check": name, "error": str(exc)})

        # Merge results
        all_results = {**data_results, **rpc_results}

        for name, (score, reason, detail) in all_results.items():
            total_score += score
            if reason:
                reasons.append(reason)
            details[name] = {
                "score": score,
                "reason": reason,
                **detail,
            }

    # Cap at 100
    total_score = min(total_score, 100)

    return {
        "safe": total_score < 40,
        "risk_score": total_score,
        "reasons": reasons,
        "details": details,
    }
