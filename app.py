"""
Wave Rider V9.6 - Reality Mirror Shadow Trader
Changes from V9.5:
  1. Progressive exit ladder: 3%@+100, 3%@+200, 10%@+500, 25%@+1500, 35%@+5000 (24% rides TS)
  2. Re-entry after profitable hard exit (gain >+50%), max 2 trades per token
  3. Min liquidity gate: $15,000 (skip entry if liq < $15K)
  4. Hard exit extended: 15min (was 11min)
  5. Reality mode: 12% buy/sell slippage, 15% TX failure sim, liq exit = -100%
  6. Circuit breakers: -1 SOL daily, 5 consecutive loss pause, -0.5 SOL hourly
  7. Force price fetch every tick for ALL open positions
  8. Track peak for missed opportunities
  9. Two P&L lines: shadow_pnl (old 3% slippage) + realistic_pnl (12% slippage)
  10. Improved hindsight: track post-exit for re-entry candidates
"""

import json
import logging
import os
import random
import sqlite3
import threading
import time
from collections import deque
from contextlib import contextmanager
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify, request, g

# --- Constants ---------------------------------------------------------------

VERSION = "v9.6-reality"
HELIUS_KEY = "6ac59b75-262f-4f22-ae79-749c4bcefdc2"
HELIUS_RPC = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_KEY}"
HELIUS_API = f"https://api.helius.xyz/v0"
DEXSCREENER_BATCH = "https://api.dexscreener.com/latest/dex/tokens"
PUMPFUN_GRADUATED = "https://frontend-api-v3.pump.fun/coins?sort=created_timestamp&order=DESC&complete=true&includeNsfw=false&limit=50"
PUMPFUN_LIVE = "https://frontend-api-v3.pump.fun/coins?sort=created_timestamp&order=DESC&complete=false&includeNsfw=false&limit=50"
GECKOTERMINAL_POOLS = "https://api.geckoterminal.com/api/v2/networks/solana/new_pools?page=1"
COINGECKO_SOL = "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd"
FNG_URL = "https://api.alternative.me/fng/?limit=1"

FIRST_MINUTE_SECS = 65
EXTENDED_MONITOR_SECS = 600
POSITION_SIZE_SOL = 0.1

# Shadow slippage (old-style, for shadow_pnl)
SHADOW_BUY_SLIPPAGE = 0.03
SHADOW_SELL_SLIPPAGE = 0.03

# Reality slippage (for realistic_pnl)
REALITY_BUY_SLIPPAGE = 0.12
REALITY_SELL_SLIPPAGE = 0.12

# TX failure simulation rate
TX_FAILURE_RATE = 0.15

TRAILING_STOP_PCT = 15
STOP_LOSS_PCT = 30
HARD_EXIT_SECONDS = 900  # 15 min (was 660)
MAX_HOLD_SECONDS = 900
EMERGENCY_LIQ_DROP_PCT = 30
ALERT_SECOND = 35
HUMAN_DELAY = 5
CLEANUP_AGE_HOURS = 24
LOG_MAX_LINES = 200
DB_PATH = os.environ.get("DB_PATH", "wave_rider_v9.db")
TICK_INTERVAL = 3

# Min liquidity gate
MIN_LIQUIDITY_USD = 15000

# Progressive exit ladder
PROGRESSIVE_LADDER = [
    (100, 0.03),   # +100%: sell 3%
    (200, 0.03),   # +200%: sell 3%
    (500, 0.10),   # +500%: sell 10%
    (1500, 0.25),  # +1500%: sell 25%
    (5000, 0.35),  # +5000%: sell 35%
]
# Remaining 24% rides with trailing stop

# Re-entry config
MAX_TRADES_PER_TOKEN = 2
REENTRY_MIN_GAIN_PCT = 50  # only re-enter if first trade exited with > +50%

# Circuit breakers
DAILY_LOSS_LIMIT_SOL = -1.0
HOURLY_LOSS_LIMIT_SOL = -0.5
CONSECUTIVE_LOSS_PAUSE = 5

# --- Logging -----------------------------------------------------------------

log_buffer = deque(maxlen=LOG_MAX_LINES)

class BufferHandler(logging.Handler):
    def emit(self, record):
        log_buffer.append(self.format(record))

logger = logging.getLogger("wr9")
logger.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
sh = logging.StreamHandler()
sh.setFormatter(fmt)
bh = BufferHandler()
bh.setFormatter(fmt)
logger.addHandler(sh)
logger.addHandler(bh)

# --- Shared State ------------------------------------------------------------

state_lock = threading.Lock()
state = {
    "sol_price": 0.0,
    "fng": 0,
    "last_sol_refresh": 0,
    "last_fng_refresh": 0,
    "last_graduated_poll": 0,
    "last_live_poll": 0,
    "last_gecko_poll": 0,
    "tokens": {},
    "open_positions": {},
    "started": False,
    "tick_count": 0,
    "start_time": 0,
    "traded_addresses": {},  # addr -> trade_count (was set, now dict for re-entry)
    "glitch_logged": set(),
    # Circuit breaker state
    "daily_pnl": 0.0,
    "hourly_pnl": 0.0,
    "hourly_reset_time": 0,
    "consecutive_losses": 0,
    "circuit_breaker_active": False,
    "circuit_breaker_reason": "",
    # Re-entry candidates
    "reentry_candidates": {},  # addr -> {grade, exit_gain_pct, exit_time, ...}
}

# --- Database ----------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS tokens (
    address TEXT PRIMARY KEY,
    symbol TEXT, name TEXT,
    detected_at REAL, status TEXT DEFAULT 'collecting',
    first_price REAL, peak_price REAL, current_price REAL,
    peak_gain_pct REAL DEFAULT 0, final_gain_pct REAL DEFAULT 0,
    time_to_peak_seconds REAL DEFAULT 0,
    pre_participants INTEGER DEFAULT 0, pre_replies INTEGER DEFAULT 0,
    pre_velocity REAL DEFAULT 0, pre_ath_mcap REAL DEFAULT 0,
    pre_creator TEXT DEFAULT '',
    top1_holder_pct REAL DEFAULT 0, top5_holder_pct REAL DEFAULT 0, top10_holder_pct REAL DEFAULT 0,
    bundle_detected INTEGER DEFAULT -1, bundle_buy_count INTEGER DEFAULT 0,
    freeze_authority INTEGER DEFAULT -1,
    mint_authority INTEGER DEFAULT -1,
    curve_duration_sec REAL DEFAULT 0, graduation_hour INTEGER DEFAULT -1, graduation_dow INTEGER DEFAULT -1,
    sol_price_at_grad REAL DEFAULT 0, fng_at_grad INTEGER DEFAULT 0,
    has_website INTEGER DEFAULT 0, has_twitter INTEGER DEFAULT 0, has_telegram INTEGER DEFAULT 0,
    creator_prev_tokens INTEGER DEFAULT -1,
    dex_source TEXT DEFAULT 'unknown',
    wave_type TEXT DEFAULT '', snapshot_count INTEGER DEFAULT 0,
    initial_liquidity REAL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    token_address TEXT, ts REAL, price REAL, liquidity_usd REAL,
    volume_5m REAL, fdv REAL, mcap REAL
);
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    token_address TEXT, ts REAL, direction TEXT,
    sol_amount REAL, block_slot INTEGER, signer TEXT
);
CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    token_address TEXT, symbol TEXT, ts REAL,
    grade TEXT,
    entry_filters TEXT,
    safety_checks TEXT,
    price_at_alert REAL,
    action TEXT DEFAULT 'pending'
);
CREATE TABLE IF NOT EXISTS shadow_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    token_address TEXT, symbol TEXT,
    alert_grade TEXT,
    trade_number INTEGER DEFAULT 1,
    entry_time REAL, entry_price REAL, entry_liquidity REAL,
    entry_filters TEXT DEFAULT '',
    position_size_sol REAL DEFAULT 0.1,
    progressive_sold TEXT DEFAULT '[]',
    progressive_banked_sol REAL DEFAULT 0,
    remaining_pct REAL DEFAULT 1.0,
    exit_time REAL DEFAULT 0, exit_price REAL DEFAULT 0,
    exit_reason TEXT DEFAULT '',
    shadow_pnl_sol REAL DEFAULT 0,
    realistic_pnl_sol REAL DEFAULT 0,
    pnl_sol REAL DEFAULT 0, pnl_pct REAL DEFAULT 0,
    peak_price_during REAL DEFAULT 0,
    status TEXT DEFAULT 'open',
    tx_failure_simulated INTEGER DEFAULT 0,
    price_2min_after_exit REAL DEFAULT 0,
    price_5min_after_exit REAL DEFAULT 0,
    price_10min_after_exit REAL DEFAULT 0,
    hindsight_checked INTEGER DEFAULT 0,
    reentry_candidate INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS missed_opportunities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    token_address TEXT, symbol TEXT, ts REAL,
    alert_grade TEXT, reason TEXT,
    peak_gain_pct REAL DEFAULT 0, final_gain_pct REAL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_snapshots_addr ON snapshots(token_address);
CREATE INDEX IF NOT EXISTS idx_trades_addr ON trades(token_address);
CREATE INDEX IF NOT EXISTS idx_alerts_action ON alerts(action);
CREATE INDEX IF NOT EXISTS idx_shadow_status ON shadow_trades(status);
"""


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.executescript(SCHEMA)
    # Migration: add new columns if upgrading from V9.5
    migrations = [
        ("shadow_trades", "trade_number", "INTEGER DEFAULT 1"),
        ("shadow_trades", "progressive_sold", "TEXT DEFAULT '[]'"),
        ("shadow_trades", "progressive_banked_sol", "REAL DEFAULT 0"),
        ("shadow_trades", "remaining_pct", "REAL DEFAULT 1.0"),
        ("shadow_trades", "shadow_pnl_sol", "REAL DEFAULT 0"),
        ("shadow_trades", "realistic_pnl_sol", "REAL DEFAULT 0"),
        ("shadow_trades", "tx_failure_simulated", "INTEGER DEFAULT 0"),
        ("shadow_trades", "reentry_candidate", "INTEGER DEFAULT 0"),
        ("shadow_trades", "entry_filters", "TEXT DEFAULT ''"),
    ]
    for table, col, coltype in migrations:
        try:
            conn.execute(f"SELECT {col} FROM {table} LIMIT 1")
        except sqlite3.OperationalError:
            try:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype}")
            except Exception:
                pass
    conn.close()
    logger.info("DB initialized: %s", DB_PATH)


@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


# --- HTTP helpers ------------------------------------------------------------

sess = requests.Session()
sess.headers.update({"User-Agent": "WaveRider/9.6"})


def safe_get(url, timeout=8, params=None):
    try:
        r = sess.get(url, timeout=timeout, params=params)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        logger.debug("GET %s failed: %s", url[:80], e)
    return None


def safe_post(url, payload, timeout=8):
    try:
        r = sess.post(url, json=payload, timeout=timeout)
        if r.status_code == 200:
            return r.json()
        if r.status_code == 429:
            logger.warning("Rate limited on POST %s", url[:80])
    except Exception as e:
        logger.debug("POST %s failed: %s", url[:80], e)
    return None


# --- Data Collection Functions -----------------------------------------------

def poll_graduated():
    data = safe_get(PUMPFUN_GRADUATED)
    if not data:
        return []
    now = time.time()
    new_tokens = []
    for coin in data:
        addr = coin.get("mint", "")
        if not addr:
            continue
        with state_lock:
            already_known = addr in state["tokens"]
        if already_known:
            continue
        symbol = coin.get("symbol", "???")
        name = coin.get("name", "")
        creator = coin.get("creator", "")
        created_ts = coin.get("created_timestamp", 0) / 1000.0 if coin.get("created_timestamp") else now
        curve_dur = now - created_ts if created_ts else 0
        hour = datetime.now(timezone.utc).hour
        dow = datetime.now(timezone.utc).weekday()
        has_web = 1 if coin.get("website") else 0
        has_tw = 1 if coin.get("twitter") else 0
        has_tg = 1 if coin.get("telegram") else 0
        ath_mcap = coin.get("usd_market_cap", 0) or 0
        token_info = {
            "detected_at": now, "status": "collecting",
            "symbol": symbol, "name": name, "pre_creator": creator,
            "curve_duration_sec": curve_dur, "graduation_hour": hour, "graduation_dow": dow,
            "has_website": has_web, "has_twitter": has_tw, "has_telegram": has_tg,
            "pre_ath_mcap": ath_mcap, "dex_source": "pumpfun", "wave_type": "graduated",
            "alert_evaluated": False, "enriched": False, "shadow_entry_scheduled": False,
        }
        with state_lock:
            token_info["sol_price_at_grad"] = state["sol_price"]
            token_info["fng_at_grad"] = state["fng"]
            state["tokens"][addr] = token_info
        with get_db() as db:
            db.execute("""INSERT OR IGNORE INTO tokens
                (address,symbol,name,detected_at,status,pre_creator,curve_duration_sec,
                 graduation_hour,graduation_dow,has_website,has_twitter,has_telegram,
                 pre_ath_mcap,dex_source,wave_type,sol_price_at_grad,fng_at_grad)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (addr, symbol, name, now, "collecting", creator, curve_dur,
                 hour, dow, has_web, has_tw, has_tg, ath_mcap,
                 "pumpfun", "graduated", token_info["sol_price_at_grad"], token_info["fng_at_grad"]))
        new_tokens.append(addr)
    if new_tokens:
        logger.info("Graduated: +%d tokens", len(new_tokens))
    return new_tokens


def poll_currently_live():
    data = safe_get(PUMPFUN_LIVE)
    if not data:
        return
    with state_lock:
        known = set(state["tokens"].keys())
    updated = 0
    for coin in data:
        addr = coin.get("mint", "")
        if addr not in known:
            continue
        participants = coin.get("num_holders", 0) or 0
        replies = coin.get("reply_count", 0) or 0
        created_ts = coin.get("created_timestamp", 0) / 1000.0 if coin.get("created_timestamp") else 0
        velocity = 0
        if created_ts and participants:
            age = time.time() - created_ts
            velocity = participants / max(age, 1)
        with state_lock:
            tok = state["tokens"].get(addr)
            if tok:
                tok["pre_participants"] = participants
                tok["pre_replies"] = replies
                tok["pre_velocity"] = velocity
        with get_db() as db:
            db.execute("UPDATE tokens SET pre_participants=?, pre_replies=?, pre_velocity=? WHERE address=?",
                       (participants, replies, velocity, addr))
        updated += 1
    if updated:
        logger.debug("Live data updated: %d tokens", updated)


def poll_geckoterminal():
    data = safe_get(GECKOTERMINAL_POOLS)
    if not data or "data" not in data:
        return []
    now = time.time()
    new_tokens = []
    for pool in data["data"]:
        attrs = pool.get("attributes", {})
        rels = pool.get("relationships", {})
        base = rels.get("base_token", {}).get("data", {}).get("id", "")
        addr = base.split("_")[-1] if "_" in base else base
        if not addr or len(addr) < 30:
            continue
        with state_lock:
            already_known = addr in state["tokens"]
        if already_known:
            continue
        name_str = attrs.get("name", "")
        symbol = name_str.split("/")[0].strip() if "/" in name_str else name_str[:10]
        dex_id = rels.get("dex", {}).get("data", {}).get("id", "unknown")
        hour = datetime.now(timezone.utc).hour
        dow = datetime.now(timezone.utc).weekday()
        with state_lock:
            sol = state["sol_price"]
            fng = state["fng"]
        token_info = {
            "detected_at": now, "status": "collecting",
            "symbol": symbol, "name": name_str, "pre_creator": "",
            "curve_duration_sec": 0, "graduation_hour": hour, "graduation_dow": dow,
            "dex_source": dex_id, "wave_type": "gecko_pool",
            "sol_price_at_grad": sol, "fng_at_grad": fng,
            "alert_evaluated": False, "enriched": False, "shadow_entry_scheduled": False,
        }
        with state_lock:
            state["tokens"][addr] = token_info
        with get_db() as db:
            db.execute("""INSERT OR IGNORE INTO tokens
                (address,symbol,name,detected_at,status,dex_source,wave_type,
                 graduation_hour,graduation_dow,sol_price_at_grad,fng_at_grad)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (addr, symbol, name_str, now, "collecting", dex_id, "gecko_pool",
                 hour, dow, sol, fng))
        new_tokens.append(addr)
    if new_tokens:
        logger.info("GeckoTerminal: +%d pools", len(new_tokens))
    return new_tokens


# --- Price Snapshots ---------------------------------------------------------

def collect_price_snapshots():
    now = time.time()
    with state_lock:
        active = []
        for addr, tok in state["tokens"].items():
            if tok["status"] in ("collecting", "first_minute", "extended"):
                age = now - tok["detected_at"]
                if age <= FIRST_MINUTE_SECS:
                    active.append(addr)
                elif tok["status"] == "extended":
                    last_snap = tok.get("last_snapshot", 0)
                    if age < 130 and now - last_snap >= 30:
                        active.append(addr)
                    elif age < 310 and now - last_snap >= 60:
                        active.append(addr)
                    elif now - last_snap >= 120:
                        active.append(addr)
        # V9.6: Force price fetch for ALL open positions every tick
        for addr in state["open_positions"]:
            if addr not in active:
                active.append(addr)

    if not active:
        return

    for i in range(0, len(active), 30):
        batch = active[i:i+30]
        url = f"{DEXSCREENER_BATCH}/{','.join(batch)}"
        data = safe_get(url, timeout=10)
        if not data or "pairs" not in data:
            continue
        pairs_by_token = {}
        for pair in data["pairs"]:
            base_addr = pair.get("baseToken", {}).get("address", "")
            if base_addr in batch:
                if base_addr not in pairs_by_token or (pair.get("liquidity", {}).get("usd", 0) or 0) > (pairs_by_token[base_addr].get("liquidity", {}).get("usd", 0) or 0):
                    pairs_by_token[base_addr] = pair
        for addr, pair in pairs_by_token.items():
            price = float(pair.get("priceUsd", 0) or 0)
            liq = pair.get("liquidity", {}).get("usd", 0) or 0
            vol = pair.get("volume", {}).get("m5", 0) or 0
            fdv = pair.get("fdv", 0) or 0
            mcap = pair.get("marketCap", 0) or 0
            if price <= 0:
                continue
            ts = time.time()
            price_is_glitch = False
            with state_lock:
                tok = state["tokens"].get(addr)
                if tok:
                    if not tok.get("first_price"):
                        tok["first_price"] = price
                    last_price = tok.get("current_price") or tok.get("first_price")
                    price_is_glitch = last_price and last_price > 0 and price / last_price > 100
                    if price_is_glitch:
                        if addr not in state["glitch_logged"]:
                            state["glitch_logged"].add(addr)
                            logger.warning("GLITCH rejected %s: $%.6f -> $%.6f (%.0fx jump)",
                                           tok.get("symbol", addr[:8]), last_price, price, price / last_price)
                    else:
                        tok["current_price"] = price
                        if price > (tok.get("peak_price") or 0):
                            tok["peak_price"] = price
                            tok["time_to_peak_seconds"] = ts - tok["detected_at"]
                        first_p = tok.get("first_price", price)
                        tok["peak_gain_pct"] = ((tok.get("peak_price", price) / first_p) - 1) * 100 if first_p else 0
                        tok["current_gain_pct"] = ((price / first_p) - 1) * 100 if first_p else 0
                        tok["current_liquidity"] = liq
                        if not tok.get("initial_liquidity"):
                            tok["initial_liquidity"] = liq
                        tok["last_snapshot"] = ts
                        tok["snapshot_count"] = tok.get("snapshot_count", 0) + 1
            if not price_is_glitch:
                with get_db() as db:
                    db.execute("INSERT INTO snapshots (token_address,ts,price,liquidity_usd,volume_5m,fdv,mcap) VALUES (?,?,?,?,?,?,?)",
                               (addr, ts, price, liq, vol, fdv, mcap))
                    db.execute("""UPDATE tokens SET first_price=COALESCE(NULLIF(first_price,0),?),
                        current_price=?, peak_price=MAX(COALESCE(peak_price,0),?),
                        initial_liquidity=COALESCE(NULLIF(initial_liquidity,0),?),
                        snapshot_count=snapshot_count+1 WHERE address=?""",
                        (price, price, price, liq, addr))


# --- Enrichment --------------------------------------------------------------

def fetch_holder_concentration(addr):
    payload = {
        "jsonrpc": "2.0", "id": 1,
        "method": "getTokenLargestAccounts",
        "params": [addr]
    }
    data = safe_post(HELIUS_RPC, payload)
    if not data or "result" not in data:
        return None
    accounts = data["result"].get("value", [])
    if not accounts:
        return None
    amounts = sorted([float(a.get("uiAmount", 0) or a.get("amount", 0)) for a in accounts], reverse=True)
    total = sum(amounts)
    if total <= 0:
        return None
    top1 = (amounts[0] / total) * 100 if len(amounts) >= 1 else 0
    top5 = (sum(amounts[:5]) / total) * 100 if len(amounts) >= 5 else top1
    top10 = (sum(amounts[:10]) / total) * 100 if len(amounts) >= 10 else top5
    return {"top1": top1, "top5": top5, "top10": top10}


def fetch_trades_and_bundles(addr):
    url = f"{HELIUS_API}/addresses/{addr}/transactions?type=SWAP&api-key={HELIUS_KEY}"
    try:
        r = sess.get(url, timeout=10, stream=True)
        content = b""
        for chunk in r.iter_content(8192):
            content += chunk
            if len(content) > 1_000_000:
                break
        data = json.loads(content)
    except Exception:
        return None, False, 0

    if not isinstance(data, list):
        return None, False, 0

    buys_by_block = {}
    trade_rows = []
    for tx in data:
        slot = tx.get("slot", 0)
        ts = tx.get("timestamp", 0)
        fee_payer = tx.get("feePayer", "")
        sol_amount = 0
        direction = "unknown"
        native_txs = tx.get("nativeTransfers", [])
        for nt in native_txs:
            amt = nt.get("amount", 0) / 1e9
            if amt > 0.001:
                sol_amount = max(sol_amount, amt)
        token_txs = tx.get("tokenTransfers", [])
        for tt in token_txs:
            if tt.get("mint") == addr:
                if tt.get("toUserAccount") == fee_payer:
                    direction = "buy"
                elif tt.get("fromUserAccount") == fee_payer:
                    direction = "sell"
        if direction == "buy":
            buys_by_block.setdefault(slot, []).append(fee_payer)
        trade_rows.append((addr, ts, direction, sol_amount, slot, fee_payer))

    bundle = False
    bundle_count = 0
    for slot, signers in buys_by_block.items():
        if len(signers) >= 3:
            bundle = True
            bundle_count = max(bundle_count, len(signers))

    return trade_rows, bundle, bundle_count


def check_token_safety(mint_address):
    payload = {
        "jsonrpc": "2.0", "id": 1,
        "method": "getAccountInfo",
        "params": [mint_address, {"encoding": "jsonParsed"}]
    }
    data = safe_post(HELIUS_RPC, payload, timeout=5)
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


def check_creator_history(creator_addr):
    if not creator_addr:
        return -1
    url = f"{HELIUS_API}/addresses/{creator_addr}/transactions?type=TOKEN_MINT&api-key={HELIUS_KEY}"
    data = safe_get(url, timeout=8)
    if not data or not isinstance(data, list):
        return -1
    mints = set()
    for tx in data:
        for tt in tx.get("tokenTransfers", []):
            m = tt.get("mint", "")
            if m:
                mints.add(m)
    return max(0, len(mints) - 1)


def enrich_token(addr):
    with state_lock:
        tok = state["tokens"].get(addr)
        if not tok or tok.get("enriched"):
            return
        tok["enriched"] = True
        creator = tok.get("pre_creator", "")

    holders = fetch_holder_concentration(addr)
    if holders:
        with state_lock:
            tok = state["tokens"].get(addr)
            if tok:
                tok["top1_holder_pct"] = holders["top1"]
                tok["top5_holder_pct"] = holders["top5"]
                tok["top10_holder_pct"] = holders["top10"]
        with get_db() as db:
            db.execute("UPDATE tokens SET top1_holder_pct=?,top5_holder_pct=?,top10_holder_pct=? WHERE address=?",
                       (holders["top1"], holders["top5"], holders["top10"], addr))

    trade_rows, bundle, bundle_count = fetch_trades_and_bundles(addr)
    if trade_rows:
        with get_db() as db:
            db.executemany("INSERT INTO trades (token_address,ts,direction,sol_amount,block_slot,signer) VALUES (?,?,?,?,?,?)",
                          trade_rows)
    with state_lock:
        tok = state["tokens"].get(addr)
        if tok:
            tok["bundle_detected"] = 1 if bundle else 0
            tok["bundle_buy_count"] = bundle_count
    with get_db() as db:
        db.execute("UPDATE tokens SET bundle_detected=?,bundle_buy_count=? WHERE address=?",
                   (1 if bundle else 0, bundle_count, addr))

    safety = check_token_safety(addr)
    freeze = 1 if safety.get("freeze_authority") else 0
    mint = 1 if safety.get("mint_authority") else 0
    with state_lock:
        tok = state["tokens"].get(addr)
        if tok:
            tok["freeze_authority"] = freeze
            tok["mint_authority"] = mint
    with get_db() as db:
        db.execute("UPDATE tokens SET freeze_authority=?,mint_authority=? WHERE address=?",
                   (freeze, mint, addr))

    prev = check_creator_history(creator)
    if prev >= 0:
        with state_lock:
            tok = state["tokens"].get(addr)
            if tok:
                tok["creator_prev_tokens"] = prev
        with get_db() as db:
            db.execute("UPDATE tokens SET creator_prev_tokens=? WHERE address=?", (prev, addr))

    logger.info("Enriched %s: top1=%.1f%% bundle=%s freeze=%d creator_prev=%d",
                addr[:8], holders["top1"] if holders else -1, bundle, freeze, prev)


# --- Alert Engine ------------------------------------------------------------

def evaluate_alert(addr):
    with state_lock:
        tok = state["tokens"].get(addr)
        if not tok or tok.get("alert_evaluated"):
            return None, None
        tok["alert_evaluated"] = True
        first_price = tok.get("first_price", 0)
        current_price = tok.get("current_price", 0)
        top1 = tok.get("top1_holder_pct", 0)
        bundle = tok.get("bundle_detected", -1)
        freeze = tok.get("freeze_authority", -1)
        mint = tok.get("mint_authority", -1)
        curve_dur = tok.get("curve_duration_sec", 0)
        current_gain = tok.get("current_gain_pct", 0)
        creator_prev = tok.get("creator_prev_tokens", -1)
        liq = tok.get("current_liquidity", 0)
        symbol = tok.get("symbol", "???")

    entry_filters = {}
    safety_checks = {}

    entry_filters["top1_gte_99"] = top1 >= 99
    entry_filters["top1_gte_95"] = top1 >= 95
    entry_filters["bundle_detected"] = bundle == 1
    entry_filters["price_up_5pct"] = current_gain > 5

    kill = False
    kill_reasons = []
    if curve_dur > 300:
        kill = True
        kill_reasons.append(f"curve_dur={curve_dur:.0f}s")
    if current_gain < -10:
        kill = True
        kill_reasons.append(f"price_down={current_gain:.1f}%")
    if creator_prev >= 1:
        kill = True
        kill_reasons.append(f"serial_creator={creator_prev}")

    entry_filters["no_kill_signal"] = not kill

    safety_checks["freeze_authority"] = "BLOCK" if freeze == 1 else "PASS"
    safety_checks["mint_authority"] = "WARN" if mint == 1 else "PASS"
    safety_checks["liquidity"] = f"${liq:.0f}" if liq else "$0"
    safety_checks["zero_liquidity"] = "WARN" if liq == 0 else "PASS"

    has_momentum = entry_filters["price_up_5pct"]
    has_holder_signal = entry_filters["top1_gte_95"] or entry_filters["bundle_detected"]
    no_kill = entry_filters["no_kill_signal"]

    entry_pass = (has_momentum and has_holder_signal and no_kill) or \
                 (entry_filters["top1_gte_99"] and entry_filters["bundle_detected"] and no_kill)

    blocked = safety_checks["freeze_authority"] == "BLOCK"

    if not entry_pass or blocked:
        return None, None

    if entry_filters["top1_gte_99"] and entry_filters["bundle_detected"] and has_momentum:
        grade = "A"
    elif has_momentum and entry_filters["top1_gte_99"]:
        grade = "A"
    elif has_momentum and has_holder_signal:
        grade = "B"
    else:
        grade = "C"

    entry_filters_json = json.dumps(entry_filters)

    ts = time.time()
    with get_db() as db:
        db.execute("""INSERT INTO alerts (token_address,symbol,ts,grade,entry_filters,safety_checks,price_at_alert,action)
            VALUES (?,?,?,?,?,?,?,?)""",
            (addr, symbol, ts, grade, entry_filters_json, json.dumps(safety_checks), current_price, "pending"))

    logger.info("ALERT %s-grade: %s (%s) price=$%.6f gain=%.1f%% liq=$%.0f",
                grade, symbol, addr[:8], current_price, current_gain, liq)
    return grade, entry_filters_json


# --- Circuit Breakers --------------------------------------------------------

def check_circuit_breaker():
    """Check if circuit breakers are tripped. Returns (active, reason)."""
    with state_lock:
        now = time.time()
        # Reset hourly counter
        if now - state["hourly_reset_time"] > 3600:
            state["hourly_pnl"] = 0.0
            state["hourly_reset_time"] = now

        if state["daily_pnl"] <= DAILY_LOSS_LIMIT_SOL:
            state["circuit_breaker_active"] = True
            state["circuit_breaker_reason"] = f"Daily loss limit hit: {state['daily_pnl']:+.4f} SOL"
            return True, state["circuit_breaker_reason"]

        if state["hourly_pnl"] <= HOURLY_LOSS_LIMIT_SOL:
            state["circuit_breaker_active"] = True
            state["circuit_breaker_reason"] = f"Hourly loss limit hit: {state['hourly_pnl']:+.4f} SOL"
            return True, state["circuit_breaker_reason"]

        if state["consecutive_losses"] >= CONSECUTIVE_LOSS_PAUSE:
            state["circuit_breaker_active"] = True
            state["circuit_breaker_reason"] = f"Consecutive loss streak: {state['consecutive_losses']}"
            return True, state["circuit_breaker_reason"]

        state["circuit_breaker_active"] = False
        state["circuit_breaker_reason"] = ""
        return False, ""


def update_circuit_breaker_pnl(pnl_sol):
    """Update circuit breaker counters after a trade closes."""
    with state_lock:
        state["daily_pnl"] += pnl_sol
        state["hourly_pnl"] += pnl_sol
        if pnl_sol < 0:
            state["consecutive_losses"] += 1
        else:
            state["consecutive_losses"] = 0


# --- Shadow Trading Engine ---------------------------------------------------

def get_trade_count(addr):
    """Get number of trades already done for this token."""
    with state_lock:
        count = state["traded_addresses"].get(addr, 0)
    if count == 0:
        with get_db() as db:
            row = db.execute("SELECT COUNT(*) as c FROM shadow_trades WHERE token_address=?", (addr,)).fetchone()
            count = row["c"]
            if count > 0:
                with state_lock:
                    state["traded_addresses"][addr] = count
    return count


def can_trade(addr, is_reentry=False):
    """Check if we can trade this token (respects max trades per token)."""
    count = get_trade_count(addr)
    if is_reentry:
        return count < MAX_TRADES_PER_TOKEN
    else:
        return count == 0


def open_shadow_position(addr, grade, entry_filters_json="", is_reentry=False):
    with state_lock:
        tok = state["tokens"].get(addr)
        if not tok:
            return
        if not is_reentry:
            tok["shadow_entry_scheduled"] = True
            tok["_entry_filters_json"] = entry_filters_json
        symbol = tok.get("symbol", "???")
        liq = tok.get("current_liquidity", 0)

    # V9.6: Min liquidity gate
    if liq < MIN_LIQUIDITY_USD and liq > 0:
        log_missed_opportunity(addr, grade, f"low_liq_${liq:.0f}")
        logger.info("Skip %s: liq=$%.0f < $%d minimum", symbol, liq, MIN_LIQUIDITY_USD)
        return

    tag = "RE-ENTRY" if is_reentry else "BUY"
    logger.info("Shadow %s scheduled: %s (%s) grade=%s liq=$%.0f", tag, symbol, addr[:8], grade, liq)

    if is_reentry:
        # For re-entries, execute immediately (no delay)
        execute_shadow_entry(addr, is_reentry=True, grade_override=grade)


def execute_shadow_entry(addr, is_reentry=False, grade_override=None):
    # Check circuit breaker
    cb_active, cb_reason = check_circuit_breaker()
    if cb_active:
        logger.warning("CIRCUIT BREAKER active, skipping entry: %s", cb_reason)
        return

    with state_lock:
        tok = state["tokens"].get(addr)
        if not tok:
            return
        price = tok.get("current_price", 0)
        liq = tok.get("current_liquidity", 0)
        symbol = tok.get("symbol", "???")
        entry_filters_json = tok.get("_entry_filters_json", "")

    if not price:
        return

    # V9.6: Min liq gate check at execution time too
    if liq < MIN_LIQUIDITY_USD and liq > 0:
        logger.info("Skip entry %s at exec: liq=$%.0f < $%d", symbol, liq, MIN_LIQUIDITY_USD)
        return

    # V9.6: TX failure simulation
    tx_failed = random.random() < TX_FAILURE_RATE
    if tx_failed:
        logger.info("TX FAILURE simulated for %s (%.0f%% chance)", symbol, TX_FAILURE_RATE * 100)

    # Shadow entry price (3% slippage)
    shadow_entry_price = price * (1 + SHADOW_BUY_SLIPPAGE)
    # Reality entry price (12% slippage)
    reality_entry_price = price * (1 + REALITY_BUY_SLIPPAGE)

    ts = time.time()

    grade = grade_override or "C"
    if not grade_override:
        with get_db() as db:
            row = db.execute("SELECT grade, entry_filters FROM alerts WHERE token_address=? ORDER BY ts DESC LIMIT 1", (addr,)).fetchone()
            if row:
                grade = row["grade"]
                if not entry_filters_json:
                    entry_filters_json = row["entry_filters"] or ""

    trade_number = get_trade_count(addr) + 1

    with get_db() as db:
        db.execute("""INSERT INTO shadow_trades
            (token_address,symbol,alert_grade,trade_number,entry_time,entry_price,entry_liquidity,
             entry_filters,position_size_sol,peak_price_during,status,
             remaining_pct,progressive_sold,progressive_banked_sol,tx_failure_simulated)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (addr, symbol, grade, trade_number, ts, shadow_entry_price, liq,
             entry_filters_json, POSITION_SIZE_SOL, shadow_entry_price, "open",
             1.0, "[]", 0.0, 1 if tx_failed else 0))
        trade_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]

    with state_lock:
        state["open_positions"][addr] = {
            "trade_id": trade_id,
            "entry_price": shadow_entry_price,
            "reality_entry_price": reality_entry_price,
            "entry_time": ts,
            "entry_liquidity": liq,
            "position_size_sol": POSITION_SIZE_SOL,
            "peak_price": shadow_entry_price,
            "symbol": symbol,
            "grade": grade,
            "trade_number": trade_number,
            "tx_failed": tx_failed,
            # Progressive exit tracking
            "progressive_sold": [],  # list of (threshold_pct, sell_pct, price, sol_banked)
            "progressive_banked_sol": 0.0,
            "remaining_pct": 1.0,
        }
        state["traded_addresses"][addr] = trade_number

    tag = f"#{trade_number}" if trade_number > 1 else ""
    logger.info("Shadow BUY%s executed: %s @ $%.6f (shadow) / $%.6f (reality) liq=$%.0f grade=%s%s",
                tag, symbol, shadow_entry_price, reality_entry_price, liq, grade,
                " [TX FAILED]" if tx_failed else "")


def manage_shadow_positions():
    now = time.time()
    with state_lock:
        positions = dict(state["open_positions"])

    for addr, pos in positions.items():
        with state_lock:
            tok = state["tokens"].get(addr)
        if not tok:
            continue

        current_price = tok.get("current_price", 0)
        current_liq = tok.get("current_liquidity", 0)
        if not current_price:
            continue

        entry_price = pos["entry_price"]
        entry_time = pos["entry_time"]
        entry_liq = pos["entry_liquidity"]
        peak = pos["peak_price"]
        trade_id = pos["trade_id"]
        symbol = pos["symbol"]
        remaining_pct = pos["remaining_pct"]
        progressive_banked = pos["progressive_banked_sol"]
        progressive_sold = pos["progressive_sold"]
        age = now - entry_time
        gain_pct = ((current_price / entry_price) - 1) * 100 if entry_price else 0

        if current_price > peak:
            peak = current_price
            with state_lock:
                if addr in state["open_positions"]:
                    state["open_positions"][addr]["peak_price"] = peak
            with get_db() as db:
                db.execute("UPDATE shadow_trades SET peak_price_during=? WHERE id=?", (peak, trade_id))

        # V9.6: Progressive exit ladder
        for threshold_pct, sell_pct in PROGRESSIVE_LADDER:
            already_sold_this = any(s[0] == threshold_pct for s in progressive_sold)
            if not already_sold_this and gain_pct >= threshold_pct and remaining_pct > 0.01:
                actual_sell = min(sell_pct, remaining_pct)
                sell_price = current_price * (1 - SHADOW_SELL_SLIPPAGE)
                sol_from_sell = POSITION_SIZE_SOL * actual_sell * (sell_price / entry_price)
                cost_basis = POSITION_SIZE_SOL * actual_sell
                banked_profit = sol_from_sell - cost_basis

                progressive_sold.append((threshold_pct, actual_sell, sell_price, banked_profit))
                progressive_banked += banked_profit
                remaining_pct -= actual_sell

                with state_lock:
                    if addr in state["open_positions"]:
                        state["open_positions"][addr]["progressive_sold"] = progressive_sold
                        state["open_positions"][addr]["progressive_banked_sol"] = progressive_banked
                        state["open_positions"][addr]["remaining_pct"] = remaining_pct

                with get_db() as db:
                    db.execute("""UPDATE shadow_trades SET progressive_sold=?, progressive_banked_sol=?,
                        remaining_pct=? WHERE id=?""",
                        (json.dumps(progressive_sold), progressive_banked, remaining_pct, trade_id))

                logger.info("PROGRESSIVE SELL %.0f%% of %s at +%.0f%% @ $%.6f | banked=%.4f SOL | remaining=%.0f%%",
                            actual_sell * 100, symbol, threshold_pct, sell_price, banked_profit, remaining_pct * 100)

        exit_reason = ""
        exit_price = 0

        # EMERGENCY: liquidity drop
        if entry_liq > 0 and current_liq > 0:
            liq_drop = ((entry_liq - current_liq) / entry_liq) * 100
            if liq_drop > EMERGENCY_LIQ_DROP_PCT:
                exit_reason = f"EMERGENCY_LIQ_DROP_{liq_drop:.0f}pct"
                exit_price = current_price * (1 - SHADOW_SELL_SLIPPAGE)

        # Stop loss
        if not exit_reason and gain_pct <= -STOP_LOSS_PCT:
            exit_reason = f"STOP_LOSS_{gain_pct:.1f}pct"
            exit_price = current_price * (1 - SHADOW_SELL_SLIPPAGE)

        # Hard exit at 15 min
        if not exit_reason and age >= HARD_EXIT_SECONDS:
            exit_reason = f"HARD_EXIT_{age:.0f}s"
            exit_price = current_price * (1 - SHADOW_SELL_SLIPPAGE)

        # Max hold
        if not exit_reason and age >= MAX_HOLD_SECONDS:
            exit_reason = f"MAX_HOLD_{age:.0f}s"
            exit_price = current_price * (1 - SHADOW_SELL_SLIPPAGE)

        # Trailing stop at 15% from peak (on remaining position)
        if not exit_reason and peak > entry_price:
            drop_from_peak = ((peak - current_price) / peak) * 100
            if drop_from_peak >= TRAILING_STOP_PCT:
                exit_reason = f"TRAILING_STOP_{drop_from_peak:.1f}pct_from_peak"
                exit_price = current_price * (1 - SHADOW_SELL_SLIPPAGE)

        if exit_reason:
            if not exit_price:
                exit_price = current_price * (1 - SHADOW_SELL_SLIPPAGE)

            # Shadow PnL (old-style 3% slippage on remaining)
            remaining_exit_pnl = POSITION_SIZE_SOL * remaining_pct * ((exit_price / entry_price) - 1) if entry_price else 0
            shadow_pnl = progressive_banked + remaining_exit_pnl

            # Reality PnL (12% slippage)
            reality_entry = pos.get("reality_entry_price", entry_price)
            is_liq_exit = "EMERGENCY_LIQ" in exit_reason
            tx_failed = pos.get("tx_failed", False)

            if tx_failed:
                # TX failed at entry, no position opened, no gain no loss (just lost gas)
                realistic_pnl = -0.005  # ~gas cost
            elif is_liq_exit:
                # V9.6: Liq exit with reality slippage
                # If liq dropped to near 0, sell would fail -> -100% on remaining
                if current_liq < 100:  # effectively zero liquidity
                    realistic_remaining = -POSITION_SIZE_SOL * remaining_pct  # total loss on remaining
                else:
                    realistic_remaining = POSITION_SIZE_SOL * remaining_pct * ((current_price * (1 - REALITY_SELL_SLIPPAGE) / reality_entry) - 1)
                # Progressive sells already banked (with reality slippage adjustment)
                reality_progressive = progressive_banked * (1 - REALITY_SELL_SLIPPAGE) / (1 - SHADOW_SELL_SLIPPAGE) if progressive_banked else 0
                realistic_pnl = reality_progressive + realistic_remaining
            else:
                reality_exit = current_price * (1 - REALITY_SELL_SLIPPAGE)
                realistic_remaining = POSITION_SIZE_SOL * remaining_pct * ((reality_exit / reality_entry) - 1) if reality_entry else 0
                reality_progressive = progressive_banked * (1 - REALITY_SELL_SLIPPAGE) / (1 - SHADOW_SELL_SLIPPAGE) if progressive_banked else 0
                realistic_pnl = reality_progressive + realistic_remaining

            shadow_pnl_pct = (shadow_pnl / POSITION_SIZE_SOL) * 100

            # Check if this is a re-entry candidate (for future re-entry)
            reentry_candidate = 0
            if "HARD_EXIT" in exit_reason and shadow_pnl_pct > REENTRY_MIN_GAIN_PCT:
                reentry_candidate = 1

            with get_db() as db:
                db.execute("""UPDATE shadow_trades SET exit_time=?,exit_price=?,exit_reason=?,
                    pnl_sol=?,pnl_pct=?,shadow_pnl_sol=?,realistic_pnl_sol=?,
                    peak_price_during=?,status='closed',
                    remaining_pct=?,progressive_sold=?,progressive_banked_sol=?,
                    reentry_candidate=? WHERE id=?""",
                    (now, exit_price, exit_reason, shadow_pnl, shadow_pnl_pct,
                     shadow_pnl, realistic_pnl, peak, remaining_pct,
                     json.dumps(progressive_sold), progressive_banked,
                     reentry_candidate, trade_id))

            with state_lock:
                state["open_positions"].pop(addr, None)

            # Update circuit breaker with realistic PnL
            update_circuit_breaker_pnl(realistic_pnl)

            color = "WIN" if shadow_pnl >= 0 else "LOSS"
            logger.info("%s Shadow EXIT %s: %s pnl=%.4f SOL (%.1f%%) reality=%.4f SOL | %s%s%s",
                        color, symbol, addr[:8], shadow_pnl, shadow_pnl_pct, realistic_pnl,
                        exit_reason,
                        " [TX_FAIL]" if tx_failed else "",
                        " [REENTRY_CANDIDATE]" if reentry_candidate else "")

            # V9.6: Schedule re-entry if candidate
            if reentry_candidate and can_trade(addr, is_reentry=True):
                with state_lock:
                    grade = pos.get("grade", "B")
                    state["reentry_candidates"][addr] = {
                        "grade": grade,
                        "exit_gain_pct": shadow_pnl_pct,
                        "exit_time": now,
                        "symbol": symbol,
                    }
                logger.info("RE-ENTRY queued: %s (exited at +%.1f%%, grade=%s)", symbol, shadow_pnl_pct, grade)


def process_reentries():
    """Process pending re-entry candidates."""
    now = time.time()
    with state_lock:
        candidates = dict(state["reentry_candidates"])

    for addr, info in candidates.items():
        # Wait 5 seconds after exit before re-entering
        if now - info["exit_time"] < 5:
            continue

        # Check if still eligible
        if not can_trade(addr, is_reentry=True):
            with state_lock:
                state["reentry_candidates"].pop(addr, None)
            continue

        # Check if already in a position
        skip_in_position = False
        with state_lock:
            if addr in state["open_positions"]:
                skip_in_position = True
        if skip_in_position:
            continue

        # Check circuit breaker
        cb_active, _ = check_circuit_breaker()
        if cb_active:
            continue

        # Check current price is still moving up
        skip_no_tok = False
        liq = 0
        with state_lock:
            tok = state["tokens"].get(addr)
            if not tok:
                state["reentry_candidates"].pop(addr, None)
                skip_no_tok = True
            else:
                liq = tok.get("current_liquidity", 0)
        if skip_no_tok:
            continue

        # Check liq gate
        if liq < MIN_LIQUIDITY_USD and liq > 0:
            logger.info("Re-entry skip %s: liq=$%.0f < $%d", info["symbol"], liq, MIN_LIQUIDITY_USD)
            with state_lock:
                state["reentry_candidates"].pop(addr, None)
            continue

        # Execute re-entry
        open_shadow_position(addr, info["grade"], is_reentry=True)
        with state_lock:
            state["reentry_candidates"].pop(addr, None)


def log_missed_opportunity(addr, grade, reason):
    with state_lock:
        tok = state["tokens"].get(addr)
        symbol = tok.get("symbol", "???") if tok else "???"
    ts = time.time()
    with get_db() as db:
        db.execute("INSERT INTO missed_opportunities (token_address,symbol,ts,alert_grade,reason) VALUES (?,?,?,?,?)",
                   (addr, symbol, ts, grade, reason))
    logger.info("Missed opportunity: %s (%s) grade=%s reason=%s", symbol, addr[:8], grade, reason)


def update_missed_outcomes():
    with get_db() as db:
        rows = db.execute("SELECT id,token_address FROM missed_opportunities WHERE peak_gain_pct=0").fetchall()
        for row in rows:
            tok_row = db.execute("SELECT peak_gain_pct,final_gain_pct FROM tokens WHERE address=?",
                                (row["token_address"],)).fetchone()
            if tok_row:
                db.execute("UPDATE missed_opportunities SET peak_gain_pct=?,final_gain_pct=? WHERE id=?",
                           (tok_row["peak_gain_pct"], tok_row["final_gain_pct"], row["id"]))


# --- SOL price and FNG -------------------------------------------------------

def refresh_sol_price():
    data = safe_get(COINGECKO_SOL)
    if data and "solana" in data:
        price = data["solana"].get("usd", 0)
        if price:
            with state_lock:
                state["sol_price"] = price
                state["last_sol_refresh"] = time.time()


def refresh_fng():
    data = safe_get(FNG_URL)
    if data and "data" in data and data["data"]:
        val = int(data["data"][0].get("value", 0))
        with state_lock:
            state["fng"] = val
            state["last_fng_refresh"] = time.time()


# --- Lifecycle Management ----------------------------------------------------

def manage_lifecycle():
    now = time.time()
    with state_lock:
        tokens_copy = list(state["tokens"].items())

    for addr, tok in tokens_copy:
        age = now - tok["detected_at"]
        status = tok.get("status", "collecting")

        new_status = status
        if status == "collecting" and age >= FIRST_MINUTE_SECS:
            new_status = "extended"
        elif status == "extended" and age >= EXTENDED_MONITOR_SECS:
            new_status = "complete"
            first_p = tok.get("first_price", 0)
            curr_p = tok.get("current_price", 0)
            final_gain = ((curr_p / first_p) - 1) * 100 if first_p and curr_p else 0
            with get_db() as db:
                db.execute("UPDATE tokens SET final_gain_pct=?,status='complete' WHERE address=?",
                           (final_gain, addr))
            with state_lock:
                if addr in state["tokens"]:
                    state["tokens"][addr]["final_gain_pct"] = final_gain

        if new_status != status:
            with state_lock:
                if addr in state["tokens"]:
                    state["tokens"][addr]["status"] = new_status
            if new_status != "complete":
                with get_db() as db:
                    db.execute("UPDATE tokens SET status=? WHERE address=?", (new_status, addr))


def cleanup_old():
    now = time.time()
    cutoff = now - CLEANUP_AGE_HOURS * 3600
    with state_lock:
        to_remove = [addr for addr, tok in state["tokens"].items()
                     if tok["detected_at"] < cutoff and tok["status"] == "complete"
                     and addr not in state["open_positions"]]
        for addr in to_remove:
            del state["tokens"][addr]
    if to_remove:
        logger.info("Cleaned up %d old tokens from memory", len(to_remove))


def post_trade_hindsight():
    now = time.time()
    with get_db() as db:
        trades_to_check = db.execute("""
            SELECT id, token_address, symbol, exit_time, entry_price, exit_price, exit_reason, pnl_pct
            FROM shadow_trades
            WHERE status='closed' AND hindsight_checked=0 AND exit_time > 0
            AND ? - exit_time > 600
            LIMIT 5
        """, (now,)).fetchall()

    if not trades_to_check:
        return

    for trade in trades_to_check:
        addr = trade["token_address"]
        exit_time = trade["exit_time"]
        entry_price = trade["entry_price"]
        symbol = trade["symbol"]

        try:
            url = f"{DEXSCREENER_BATCH}/{addr}"
            resp = requests.get(url, timeout=5)
            data = resp.json()
            pairs = data.get("pairs") or []
            current_price = float(pairs[0].get("priceUsd", 0) or 0) if pairs else 0
        except Exception:
            current_price = 0

        with get_db() as db:
            p2 = p5 = p10 = 0
            for offset_sec, col in [(120, "price_2min_after_exit"), (300, "price_5min_after_exit"), (600, "price_10min_after_exit")]:
                target_ts = exit_time + offset_sec
                row = db.execute("""
                    SELECT price FROM snapshots WHERE token_address=?
                    AND ts BETWEEN ? AND ? ORDER BY ABS(ts - ?) LIMIT 1
                """, (addr, target_ts - 15, target_ts + 15, target_ts)).fetchone()
                if row and row["price"]:
                    if col == "price_2min_after_exit": p2 = row["price"]
                    elif col == "price_5min_after_exit": p5 = row["price"]
                    else: p10 = row["price"]

            if not p10 and current_price:
                p10 = current_price

            db.execute("""
                UPDATE shadow_trades SET
                    price_2min_after_exit=?, price_5min_after_exit=?, price_10min_after_exit=?,
                    hindsight_checked=1
                WHERE id=?
            """, (p2, p5, p10, trade["id"]))

        pnl_actual = trade["pnl_pct"]
        pnl_if_held_10 = ((p10 / entry_price) - 1) * 100 if entry_price and p10 else 0

        verdict = "GOOD EXIT" if pnl_actual >= pnl_if_held_10 else "LEFT MONEY"
        logger.info("Hindsight %s: exited at %+.1f%% (%s) | 10min later: %+.1f%% | %s",
                     symbol, pnl_actual, trade["exit_reason"], pnl_if_held_10, verdict)


# --- Main Loop ---------------------------------------------------------------

def main_loop():
    logger.info("Wave Rider %s starting main loop", VERSION)

    refresh_sol_price()
    refresh_fng()

    with state_lock:
        state["started"] = True
        state["start_time"] = time.time()
        state["hourly_reset_time"] = time.time()

    while True:
        tick_start = time.time()
        with state_lock:
            state["tick_count"] += 1
            tick_num = state["tick_count"]

        # 1. Scan new tokens
        try:
            now = time.time()
            with state_lock:
                last_grad = state["last_graduated_poll"]
                last_live = state["last_live_poll"]
                last_gecko = state["last_gecko_poll"]
            if now - last_grad >= 3:
                poll_graduated()
                with state_lock:
                    state["last_graduated_poll"] = now
            if now - last_live >= 6:
                poll_currently_live()
                with state_lock:
                    state["last_live_poll"] = now
            if now - last_gecko >= 30:
                poll_geckoterminal()
                with state_lock:
                    state["last_gecko_poll"] = now
        except Exception as e:
            logger.error("Scan error: %s", e)

        # 2. Price snapshots (V9.6: forces fetch for all open positions)
        try:
            collect_price_snapshots()
        except Exception as e:
            logger.error("Snapshot error: %s", e)

        # 3. Enrichment
        try:
            if tick_num % 3 == 0:
                now = time.time()
                with state_lock:
                    to_enrich = [(addr, tok) for addr, tok in state["tokens"].items()
                                 if not tok.get("enriched") and 25 <= now - tok["detected_at"] <= 90]
                for addr, tok in to_enrich[:1]:
                    try:
                        enrich_token(addr)
                    except Exception as e:
                        logger.error("Enrich %s error: %s", addr[:8], e)
        except Exception as e:
            logger.error("Enrichment error: %s", e)

        # 4. Alert engine
        try:
            now = time.time()
            with state_lock:
                to_evaluate = [(addr, tok) for addr, tok in state["tokens"].items()
                               if not tok.get("alert_evaluated") and tok.get("enriched")
                               and now - tok["detected_at"] >= ALERT_SECOND]
            for addr, tok in to_evaluate:
                try:
                    result = evaluate_alert(addr)
                    grade = result[0] if result else None
                    entry_filters_json = result[1] if result else ""
                    if grade:
                        if grade == "C":
                            log_missed_opportunity(addr, grade, "c_grade_shadow_only")
                        elif not can_trade(addr):
                            logger.info("Skip duplicate: %s already traded", tok.get("symbol","?"))
                        else:
                            # Check circuit breaker before opening
                            cb_active, cb_reason = check_circuit_breaker()
                            if cb_active:
                                log_missed_opportunity(addr, grade, f"circuit_breaker:{cb_reason[:30]}")
                            else:
                                open_shadow_position(addr, grade, entry_filters_json)
                except Exception as e:
                    logger.error("Alert eval %s error: %s", addr[:8], e)
        except Exception as e:
            logger.error("Alert engine error: %s", e)

        # 5. Shadow trader
        try:
            now = time.time()
            with state_lock:
                scheduled = [(addr, tok) for addr, tok in state["tokens"].items()
                             if tok.get("shadow_entry_scheduled") and not tok.get("shadow_entered")
                             and now - tok["detected_at"] >= ALERT_SECOND + HUMAN_DELAY]
            for addr, tok in scheduled:
                try:
                    execute_shadow_entry(addr)
                    with state_lock:
                        if addr in state["tokens"]:
                            state["tokens"][addr]["shadow_entered"] = True
                except Exception as e:
                    logger.error("Shadow entry %s error: %s", addr[:8], e)

            manage_shadow_positions()

            # V9.6: Process re-entries
            process_reentries()

        except Exception as e:
            logger.error("Shadow trader error: %s", e)

        # 6. Lifecycle
        try:
            manage_lifecycle()
        except Exception as e:
            logger.error("Lifecycle error: %s", e)

        # 7. Cleanup
        try:
            if tick_num % 100 == 0:
                cleanup_old()
                update_missed_outcomes()
        except Exception as e:
            logger.error("Cleanup error: %s", e)

        # 7b. Post-trade hindsight
        try:
            if tick_num % 20 == 0:
                post_trade_hindsight()
        except Exception as e:
            logger.error("Hindsight error: %s", e)

        # 8. SOL price + FNG
        try:
            now = time.time()
            with state_lock:
                last_sol = state["last_sol_refresh"]
                last_fng = state["last_fng_refresh"]
            if now - last_sol >= 60:
                refresh_sol_price()
            if now - last_fng >= 300:
                refresh_fng()
        except Exception as e:
            logger.error("Price/FNG refresh error: %s", e)

        elapsed = time.time() - tick_start
        sleep_time = max(0, TICK_INTERVAL - elapsed)
        if sleep_time > 0:
            time.sleep(sleep_time)


# --- Flask App ---------------------------------------------------------------

app = Flask(__name__)
_bg_thread_started = False


@app.before_request
def startup():
    global _bg_thread_started
    if not _bg_thread_started:
        _bg_thread_started = True
        init_db()
        t = threading.Thread(target=main_loop, daemon=True)
        t.start()
        logger.info("Background thread started")


# --- Dashboard ---------------------------------------------------------------

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Wave Rider V9.6 - Reality Mirror</title>
<meta http-equiv="refresh" content="10">
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#0d1117;color:#c9d1d9;font-family:'SF Mono',Consolas,monospace;font-size:13px;padding:16px}
h1{color:#58a6ff;font-size:20px;margin-bottom:4px}
.sub{color:#8b949e;font-size:12px;margin-bottom:16px}
.stats{display:flex;gap:12px;flex-wrap:wrap;margin-bottom:16px}
.stat{background:#161b22;border:1px solid #30363d;border-radius:6px;padding:10px 14px;min-width:120px}
.stat .label{color:#8b949e;font-size:10px;text-transform:uppercase}
.stat .value{font-size:18px;font-weight:700;margin-top:2px}
.green{color:#3fb950}.red{color:#f85149}.yellow{color:#d29922}.blue{color:#58a6ff}
table{width:100%;border-collapse:collapse;margin-bottom:20px}
th{background:#161b22;color:#8b949e;text-align:left;padding:8px;font-size:11px;text-transform:uppercase;border-bottom:1px solid #30363d}
td{padding:6px 8px;border-bottom:1px solid #21262d}
tr:hover{background:#161b22}
.section-title{color:#58a6ff;font-size:15px;font-weight:700;margin:16px 0 8px;border-bottom:1px solid #30363d;padding-bottom:4px}
.badge{display:inline-block;padding:2px 6px;border-radius:3px;font-size:10px;font-weight:700}
.badge-a{background:#238636;color:#fff}.badge-b{background:#1f6feb;color:#fff}.badge-c{background:#6e7681;color:#fff}
.pnl-pos{color:#3fb950;font-weight:700}.pnl-neg{color:#f85149;font-weight:700}
.empty{color:#484f58;text-align:center;padding:20px}
.changes{background:#161b22;border:1px solid #30363d;border-radius:6px;padding:12px;margin-bottom:16px;font-size:11px;color:#8b949e}
.changes b{color:#58a6ff}
.cb-active{background:#f8514922;border:1px solid #f85149;border-radius:6px;padding:10px;margin-bottom:16px;color:#f85149;font-weight:700}
</style></head><body>
<h1>Wave Rider V9.6 - Reality Mirror Shadow Trader</h1>
<div class="sub">A+B only | TS=15%% | 15min hold | Progressive exit | Re-entry | $15K liq gate | Circuit breakers | {{time_utc}}</div>
<div class="changes">
<b>V9.6 changes:</b> Progressive exit ladder (3%%@+100/3%%@+200/10%%@+500/25%%@+1500/35%%@+5000) | Re-entry after profitable hard exit | $15K min liq | 15min hold (was 11min) | Reality PnL (12%% slippage) | TX failure sim (15%%) | Circuit breakers (-1 SOL daily, 5 streak, -0.5 SOL hourly)
</div>
{{circuit_breaker_html}}
<div class="stats">
<div class="stat"><div class="label">SOL Price</div><div class="value blue">${{sol_price}}</div></div>
<div class="stat"><div class="label">FNG</div><div class="value {{fng_color}}">{{fng}}</div></div>
<div class="stat"><div class="label">Tracking</div><div class="value">{{tracking}}</div></div>
<div class="stat"><div class="label">Open Positions</div><div class="value yellow">{{open_pos}}</div></div>
<div class="stat"><div class="label">Total Trades</div><div class="value">{{total_trades}}</div></div>
<div class="stat"><div class="label">Win Rate</div><div class="value {{wr_color}}">{{win_rate}}</div></div>
<div class="stat"><div class="label">Shadow P&L</div><div class="value {{pnl_color}}">{{total_pnl}} SOL</div></div>
<div class="stat"><div class="label">Reality P&L</div><div class="value {{rpnl_color}}">{{reality_pnl}} SOL</div></div>
<div class="stat"><div class="label">Alerts Today</div><div class="value">{{alerts_today}}</div></div>
<div class="stat"><div class="label">Uptime</div><div class="value">{{uptime}}</div></div>
</div>

<div class="section-title">Open Shadow Positions</div>
{{open_table}}

<div class="section-title">Closed Shadow Trades (Recent 20)</div>
{{closed_table}}

<div class="section-title">Recent Alerts (20)</div>
{{alerts_table}}

<div class="section-title">Missed / C-grade Shadow (Recent 10)</div>
{{missed_table}}

</body></html>"""


def render_dashboard():
    now = time.time()
    with state_lock:
        sol_price = state["sol_price"]
        fng = state["fng"]
        tracking = len(state["tokens"])
        open_pos = len(state["open_positions"])
        start_time = state["start_time"]
        cb_active = state["circuit_breaker_active"]
        cb_reason = state["circuit_breaker_reason"]
        daily_pnl = state["daily_pnl"]
        hourly_pnl = state["hourly_pnl"]
        consec = state["consecutive_losses"]

    uptime_s = now - start_time if start_time else 0
    uptime_h = int(uptime_s // 3600)
    uptime_m = int((uptime_s % 3600) // 60)
    uptime = f"{uptime_h}h {uptime_m}m"
    fng_color = "green" if fng >= 60 else ("red" if fng <= 30 else "yellow")

    circuit_breaker_html = ""
    if cb_active:
        circuit_breaker_html = f'<div class="cb-active">CIRCUIT BREAKER ACTIVE: {cb_reason} | Daily: {daily_pnl:+.4f} SOL | Hourly: {hourly_pnl:+.4f} SOL | Streak: {consec}</div>'

    with get_db() as db:
        closed = db.execute("SELECT * FROM shadow_trades WHERE status='closed' ORDER BY exit_time DESC").fetchall()
        open_trades = db.execute("SELECT * FROM shadow_trades WHERE status='open' ORDER BY entry_time DESC").fetchall()
        alerts = db.execute("SELECT * FROM alerts ORDER BY ts DESC LIMIT 20").fetchall()
        missed = db.execute("SELECT * FROM missed_opportunities ORDER BY ts DESC LIMIT 10").fetchall()
        alerts_today_count = db.execute(
            "SELECT COUNT(*) as c FROM alerts WHERE ts > ?", (now - 86400,)).fetchone()["c"]

    total_trades = len(closed)
    wins = sum(1 for t in closed if t["pnl_sol"] > 0)
    win_rate = f"{(wins/total_trades*100):.0f}%" if total_trades else "N/A"
    wr_color = "green" if total_trades and wins/total_trades > 0.5 else "red" if total_trades else ""
    total_pnl = sum(t["pnl_sol"] for t in closed)
    pnl_color = "green" if total_pnl >= 0 else "red"
    try:
        reality_total = sum((t["realistic_pnl_sol"] or 0) for t in closed)
    except (KeyError, IndexError):
        reality_total = 0
    rpnl_color = "green" if reality_total >= 0 else "red"

    if open_trades:
        rows = ""
        for t in open_trades:
            with state_lock:
                tok = state["tokens"].get(t["token_address"], {})
                pos_data = state["open_positions"].get(t["token_address"], {})
            curr = tok.get("current_price", 0)
            gain = ((curr / t["entry_price"]) - 1) * 100 if t["entry_price"] and curr else 0
            age = now - t["entry_time"]
            pclass = "pnl-pos" if gain >= 0 else "pnl-neg"
            grade_class = f"badge-{t['alert_grade'].lower()}" if t['alert_grade'] else "badge-c"
            remaining = pos_data.get("remaining_pct", 1.0)
            banked = pos_data.get("progressive_banked_sol", 0)
            try:
                trade_num = t["trade_number"] or 1
            except (KeyError, IndexError):
                trade_num = 1
            tag = f" #{trade_num}" if trade_num > 1 else ""
            rows += f"""<tr>
                <td>{t['symbol']}{tag}</td>
                <td><span class="badge {grade_class}">{t['alert_grade']}</span></td>
                <td>${t['entry_price']:.6f}</td><td>${curr:.6f}</td>
                <td class="{pclass}">{gain:+.1f}%</td><td>{age:.0f}s</td>
                <td>{remaining*100:.0f}%</td><td>{banked:+.4f}</td>
                <td>${t['entry_liquidity']:.0f}</td>
            </tr>"""
        open_table = f"""<table><tr><th>Token</th><th>Grade</th><th>Entry</th><th>Current</th>
            <th>P&L</th><th>Age</th><th>Remaining</th><th>Banked</th><th>Liq</th></tr>{rows}</table>"""
    else:
        open_table = '<div class="empty">No open positions</div>'

    if closed[:20]:
        rows = ""
        for t in closed[:20]:
            pclass = "pnl-pos" if t["pnl_sol"] >= 0 else "pnl-neg"
            grade_class = f"badge-{t['alert_grade'].lower()}" if t['alert_grade'] else "badge-c"
            hold = t["exit_time"] - t["entry_time"] if t["exit_time"] and t["entry_time"] else 0
            ts_str = datetime.fromtimestamp(t["entry_time"], tz=timezone.utc).strftime("%H:%M:%S") if t["entry_time"] else ""
            try:
                rpnl = t["realistic_pnl_sol"] or 0
            except (KeyError, IndexError):
                rpnl = 0
            rpclass = "pnl-pos" if rpnl >= 0 else "pnl-neg"
            try:
                trade_num = t["trade_number"] or 1
            except (KeyError, IndexError):
                trade_num = 1
            tag = f" #{trade_num}" if trade_num > 1 else ""
            try:
                reentry = " [RE]" if t["reentry_candidate"] else ""
            except (KeyError, IndexError):
                reentry = ""
            rows += f"""<tr>
                <td>{ts_str}</td><td>{t['symbol']}{tag}</td>
                <td><span class="badge {grade_class}">{t['alert_grade']}</span></td>
                <td>${t['entry_price']:.6f}</td><td>${t['exit_price']:.6f}</td>
                <td class="{pclass}">{t['pnl_sol']:+.4f}</td>
                <td class="{rpclass}">{rpnl:+.4f}</td>
                <td>{hold:.0f}s</td><td>{t['exit_reason'][:30]}{reentry}</td>
            </tr>"""
        closed_table = f"""<table><tr><th>Time</th><th>Token</th><th>Grade</th><th>Entry</th>
            <th>Exit</th><th>Shadow P&L</th><th>Reality P&L</th><th>Hold</th><th>Reason</th></tr>{rows}</table>"""
    else:
        closed_table = '<div class="empty">No closed trades yet</div>'

    if alerts:
        rows = ""
        for a in alerts:
            grade_class = f"badge-{a['grade'].lower()}" if a['grade'] else "badge-c"
            ts_str = datetime.fromtimestamp(a["ts"], tz=timezone.utc).strftime("%H:%M:%S") if a["ts"] else ""
            rows += f"""<tr>
                <td>{ts_str}</td><td>{a['symbol']}</td>
                <td><span class="badge {grade_class}">{a['grade']}</span></td>
                <td>${a['price_at_alert']:.6f}</td><td>{a['action']}</td>
                <td style="max-width:200px;overflow:hidden;text-overflow:ellipsis">{a['entry_filters'][:60]}</td>
            </tr>"""
        alerts_table = f"""<table><tr><th>Time</th><th>Token</th><th>Grade</th>
            <th>Price</th><th>Action</th><th>Filters</th></tr>{rows}</table>"""
    else:
        alerts_table = '<div class="empty">No alerts yet</div>'

    if missed:
        rows = ""
        for m in missed:
            ts_str = datetime.fromtimestamp(m["ts"], tz=timezone.utc).strftime("%H:%M:%S") if m["ts"] else ""
            peak_class = "pnl-pos" if m["peak_gain_pct"] > 0 else ""
            rows += f"""<tr>
                <td>{ts_str}</td><td>{m['symbol']}</td><td>{m['alert_grade']}</td>
                <td>{m['reason']}</td>
                <td class="{peak_class}">{m['peak_gain_pct']:+.1f}%</td>
                <td>{m['final_gain_pct']:+.1f}%</td>
            </tr>"""
        missed_table = f"""<table><tr><th>Time</th><th>Token</th><th>Grade</th>
            <th>Reason</th><th>Peak</th><th>Final</th></tr>{rows}</table>"""
    else:
        missed_table = '<div class="empty">No missed opportunities</div>'

    time_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    html = DASHBOARD_HTML
    for k, v in [("time_utc", time_utc), ("sol_price", f"{sol_price:.2f}"), ("fng", str(fng)),
                 ("fng_color", fng_color), ("tracking", str(tracking)), ("open_pos", str(open_pos)),
                 ("total_trades", str(total_trades)), ("win_rate", win_rate), ("wr_color", wr_color),
                 ("total_pnl", f"{total_pnl:+.4f}"), ("pnl_color", pnl_color),
                 ("reality_pnl", f"{reality_total:+.4f}"), ("rpnl_color", rpnl_color),
                 ("alerts_today", str(alerts_today_count)), ("uptime", uptime),
                 ("circuit_breaker_html", circuit_breaker_html),
                 ("open_table", open_table), ("closed_table", closed_table),
                 ("alerts_table", alerts_table), ("missed_table", missed_table)]:
        html = html.replace("{{" + k + "}}", v)
    return html


# --- Routes ------------------------------------------------------------------

@app.route("/")
def dashboard():
    return render_dashboard(), 200, {"Content-Type": "text/html"}


@app.route("/health")
def health():
    with state_lock:
        return jsonify({
            "status": "ok", "version": VERSION,
            "started": state["started"], "sol_price": state["sol_price"],
            "fng": state["fng"], "tracking": len(state["tokens"]),
            "open_positions": len(state["open_positions"]),
            "tick_count": state["tick_count"],
            "uptime_seconds": time.time() - state["start_time"] if state["start_time"] else 0,
            "circuit_breaker": state["circuit_breaker_active"],
            "daily_pnl": state["daily_pnl"],
            "consecutive_losses": state["consecutive_losses"],
        })


@app.route("/api/shadow")
def api_shadow():
    with get_db() as db:
        open_trades = [dict(r) for r in db.execute(
            "SELECT * FROM shadow_trades WHERE status='open' ORDER BY entry_time DESC").fetchall()]
        closed_trades = [dict(r) for r in db.execute(
            "SELECT * FROM shadow_trades WHERE status='closed' ORDER BY exit_time DESC LIMIT 50").fetchall()]
        total_pnl = db.execute("SELECT COALESCE(SUM(pnl_sol),0) as s FROM shadow_trades WHERE status='closed'").fetchone()["s"]
        reality_pnl = db.execute("SELECT COALESCE(SUM(realistic_pnl_sol),0) as s FROM shadow_trades WHERE status='closed'").fetchone()["s"]
        total_closed = db.execute("SELECT COUNT(*) as c FROM shadow_trades WHERE status='closed'").fetchone()["c"]
        wins = db.execute("SELECT COUNT(*) as c FROM shadow_trades WHERE status='closed' AND pnl_sol>0").fetchone()["c"]
    return jsonify({
        "open": open_trades, "closed": closed_trades,
        "stats": {"total_pnl_sol": total_pnl, "reality_pnl_sol": reality_pnl,
                  "total_trades": total_closed,
                  "wins": wins, "win_rate": (wins / total_closed * 100) if total_closed else 0}
    })


@app.route("/api/alerts")
def api_alerts():
    with get_db() as db:
        rows = [dict(r) for r in db.execute("SELECT * FROM alerts ORDER BY ts DESC LIMIT 50").fetchall()]
    return jsonify(rows)


@app.route("/api/waves")
def api_waves():
    limit = request.args.get("limit", 50, type=int)
    with get_db() as db:
        rows = [dict(r) for r in db.execute(
            "SELECT * FROM tokens ORDER BY detected_at DESC LIMIT ?", (limit,)).fetchall()]
    return jsonify(rows)


@app.route("/api/hindsight")
def api_hindsight():
    with get_db() as db:
        trades_data = [dict(r) for r in db.execute("""
            SELECT symbol, alert_grade, trade_number, entry_price, exit_price, exit_reason,
                   pnl_pct, peak_price_during, shadow_pnl_sol, realistic_pnl_sol,
                   price_2min_after_exit, price_5min_after_exit, price_10min_after_exit,
                   hindsight_checked, reentry_candidate
            FROM shadow_trades WHERE status='closed' ORDER BY exit_time DESC LIMIT 50
        """).fetchall()]

    results = []
    good_exits = 0
    left_money = 0
    for t in trades_data:
        entry = t["entry_price"] or 0
        r = dict(t)
        if entry and t["price_10min_after_exit"]:
            r["pnl_if_held_10min"] = ((t["price_10min_after_exit"] / entry) - 1) * 100
            r["verdict"] = "GOOD_EXIT" if t["pnl_pct"] >= r["pnl_if_held_10min"] else "LEFT_MONEY"
            if r["verdict"] == "GOOD_EXIT": good_exits += 1
            else: left_money += 1
        else:
            r["pnl_if_held_10min"] = None
            r["verdict"] = "pending"
        results.append(r)

    return jsonify({
        "trades": results,
        "summary": {"good_exits": good_exits, "left_money": left_money,
                     "exit_accuracy": f"{good_exits*100/max(good_exits+left_money,1):.0f}%"}
    })


@app.route("/api/export")
def api_export():
    with get_db() as db:
        tokens = [dict(r) for r in db.execute("SELECT * FROM tokens ORDER BY detected_at DESC LIMIT 500").fetchall()]
        shadows = [dict(r) for r in db.execute("SELECT * FROM shadow_trades ORDER BY entry_time DESC").fetchall()]
        alerts_data = [dict(r) for r in db.execute("SELECT * FROM alerts ORDER BY ts DESC LIMIT 200").fetchall()]
        missed_data = [dict(r) for r in db.execute("SELECT * FROM missed_opportunities ORDER BY ts DESC").fetchall()]
    return jsonify({
        "version": VERSION, "exported_at": time.time(),
        "tokens": tokens, "shadow_trades": shadows,
        "alerts": alerts_data, "missed_opportunities": missed_data,
    })


@app.route("/logs")
def logs():
    lines = list(log_buffer)
    html = f"""<!DOCTYPE html><html><head><meta charset="utf-8">
    <title>WR9.6 Logs</title><meta http-equiv="refresh" content="5">
    <style>body{{background:#0d1117;color:#c9d1d9;font-family:monospace;font-size:12px;padding:16px}}
    pre{{white-space:pre-wrap}}</style></head>
    <body><h2 style="color:#58a6ff">Wave Rider V9.6 Logs ({len(lines)} lines)</h2>
    <pre>{"<br>".join(lines[-LOG_MAX_LINES:])}</pre></body></html>"""
    return html, 200, {"Content-Type": "text/html"}


# --- Entry point -------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False)
