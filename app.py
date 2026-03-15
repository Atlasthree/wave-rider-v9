"""
Wave Rider V9 — Shadow Trader
Solana memecoin trading alerts with shadow (paper) trading engine.
Single Flask app, SQLite DB, background thread, Render free tier.
"""

import json
import logging
import os
import sqlite3
import threading
import time
from collections import deque
from contextlib import contextmanager
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify, request, g

# ─── Constants ───────────────────────────────────────────────────────────────

VERSION = "v9-shadow"
HELIUS_KEY = "b85d5357-36d9-4e26-b945-f38a0677b391"
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
MAX_POSITIONS = 3
COOLDOWN_SECONDS = 10
BUY_SLIPPAGE = 0.03
SELL_SLIPPAGE = 0.03
TRAILING_STOP_PCT = 30
STOP_LOSS_PCT = 30
TAKE_PROFIT_1_PCT = 100
HARD_EXIT_SECONDS = 300
MAX_HOLD_SECONDS = 600
EMERGENCY_LIQ_DROP_PCT = 30
SWEET_SPOT_LOW = 0.001
SWEET_SPOT_HIGH = 0.005
ALERT_SECOND = 35
HUMAN_DELAY = 5
CLEANUP_AGE_HOURS = 24
LOG_MAX_LINES = 200
DB_PATH = os.environ.get("DB_PATH", "wave_rider_v9.db")
TICK_INTERVAL = 3

# ─── Logging ─────────────────────────────────────────────────────────────────

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

# ─── Shared State ────────────────────────────────────────────────────────────

state_lock = threading.Lock()
state = {
    "sol_price": 0.0,
    "fng": 0,
    "last_sol_refresh": 0,
    "last_fng_refresh": 0,
    "last_graduated_poll": 0,
    "last_live_poll": 0,
    "last_gecko_poll": 0,
    "tokens": {},          # addr -> {detected_at, status, symbol, name, ...}
    "open_positions": {},   # addr -> shadow trade info
    "last_close_time": 0,  # for cooldown
    "started": False,
    "tick_count": 0,
    "start_time": 0,
}

# ─── Database ────────────────────────────────────────────────────────────────

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
    entry_time REAL, entry_price REAL, entry_liquidity REAL,
    position_size_sol REAL DEFAULT 0.1,
    sold_25_at REAL DEFAULT 0, sold_25_price REAL DEFAULT 0, sold_25_time REAL DEFAULT 0,
    exit_time REAL DEFAULT 0, exit_price REAL DEFAULT 0,
    exit_reason TEXT DEFAULT '',
    pnl_sol REAL DEFAULT 0, pnl_pct REAL DEFAULT 0,
    peak_price_during REAL DEFAULT 0,
    status TEXT DEFAULT 'open'
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


# ─── HTTP helpers ────────────────────────────────────────────────────────────

sess = requests.Session()
sess.headers.update({"User-Agent": "WaveRider/9.0"})


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
    except Exception as e:
        logger.debug("POST %s failed: %s", url[:80], e)
    return None


# ─── Data Collection Functions ───────────────────────────────────────────────

def poll_graduated():
    """Poll Pump.fun graduated tokens."""
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
            if addr in state["tokens"]:
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
            "detected_at": now,
            "status": "collecting",
            "symbol": symbol,
            "name": name,
            "pre_creator": creator,
            "curve_duration_sec": curve_dur,
            "graduation_hour": hour,
            "graduation_dow": dow,
            "has_website": has_web,
            "has_twitter": has_tw,
            "has_telegram": has_tg,
            "pre_ath_mcap": ath_mcap,
            "dex_source": "pumpfun",
            "wave_type": "graduated",
            "alert_evaluated": False,
            "enriched": False,
            "shadow_entry_scheduled": False,
        }
        with state_lock:
            sol = state["sol_price"]
            fng = state["fng"]
        token_info["sol_price_at_grad"] = sol
        token_info["fng_at_grad"] = fng
        with state_lock:
            state["tokens"][addr] = token_info
        with get_db() as db:
            db.execute("""INSERT OR IGNORE INTO tokens
                (address,symbol,name,detected_at,status,pre_creator,curve_duration_sec,
                 graduation_hour,graduation_dow,has_website,has_twitter,has_telegram,
                 pre_ath_mcap,dex_source,wave_type,sol_price_at_grad,fng_at_grad)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (addr, symbol, name, now, "collecting", creator, curve_dur,
                 hour, dow, has_web, has_tw, has_tg, ath_mcap,
                 "pumpfun", "graduated", sol, fng))
        new_tokens.append(addr)
    if new_tokens:
        logger.info("Graduated: +%d tokens", len(new_tokens))
    return new_tokens


def poll_currently_live():
    """Poll Pump.fun currently-live for pre-graduation data."""
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
    """Poll GeckoTerminal new pools for all DEXes."""
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
            if addr in state["tokens"]:
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
            "detected_at": now,
            "status": "collecting",
            "symbol": symbol,
            "name": name_str,
            "pre_creator": "",
            "curve_duration_sec": 0,
            "graduation_hour": hour,
            "graduation_dow": dow,
            "dex_source": dex_id,
            "wave_type": "gecko_pool",
            "sol_price_at_grad": sol,
            "fng_at_grad": fng,
            "alert_evaluated": False,
            "enriched": False,
            "shadow_entry_scheduled": False,
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


# ─── Price Snapshots ─────────────────────────────────────────────────────────

def collect_price_snapshots():
    """Batch DexScreener price fetch for active tokens."""
    now = time.time()
    with state_lock:
        active = []
        for addr, tok in state["tokens"].items():
            if tok["status"] in ("collecting", "first_minute", "extended"):
                age = now - tok["detected_at"]
                # every 3s in first 60s, checkpoints at 2, 5, 10 min
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
        # also include open positions for shadow trading
        for addr in state["open_positions"]:
            if addr not in active:
                active.append(addr)

    if not active:
        return

    # batch in groups of 30
    for i in range(0, len(active), 30):
        batch = active[i:i+30]
        url = f"{DEXSCREENER_BATCH}/{','.join(batch)}"
        data = safe_get(url, timeout=10)
        if not data or "pairs" not in data:
            continue
        # group pairs by base token
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
            with state_lock:
                tok = state["tokens"].get(addr)
                if tok:
                    if not tok.get("first_price"):
                        tok["first_price"] = price
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
                    snap_count = tok.get("snapshot_count", 0) + 1
                    tok["snapshot_count"] = snap_count
            with get_db() as db:
                db.execute("INSERT INTO snapshots (token_address,ts,price,liquidity_usd,volume_5m,fdv,mcap) VALUES (?,?,?,?,?,?,?)",
                           (addr, ts, price, liq, vol, fdv, mcap))
                db.execute("""UPDATE tokens SET first_price=COALESCE(NULLIF(first_price,0),?),
                    current_price=?, peak_price=MAX(COALESCE(peak_price,0),?),
                    initial_liquidity=COALESCE(NULLIF(initial_liquidity,0),?),
                    snapshot_count=snapshot_count+1 WHERE address=?""",
                    (price, price, price, liq, addr))


# ─── Enrichment ──────────────────────────────────────────────────────────────

def fetch_holder_concentration(addr):
    """Get top holder percentages via Helius RPC."""
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
    """Get trades from Helius + detect bundles."""
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
        desc = tx.get("description", "")
        # Try to parse swaps from native transfers or token transfers
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

    # Bundle detection: 3+ buys in same block
    bundle = False
    bundle_count = 0
    for slot, signers in buys_by_block.items():
        if len(signers) >= 3:
            bundle = True
            bundle_count = max(bundle_count, len(signers))

    return trade_rows, bundle, bundle_count


def check_token_safety(mint_address):
    """Check freeze/mint authority via Helius RPC."""
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
    """Check if creator has previous tokens (serial creator)."""
    if not creator_addr:
        return -1
    url = f"{HELIUS_API}/addresses/{creator_addr}/transactions?type=TOKEN_MINT&api-key={HELIUS_KEY}"
    data = safe_get(url, timeout=8)
    if not data or not isinstance(data, list):
        return -1
    # count unique mints (subtract 1 for the current token)
    mints = set()
    for tx in data:
        for tt in tx.get("tokenTransfers", []):
            m = tt.get("mint", "")
            if m:
                mints.add(m)
    return max(0, len(mints) - 1)


def enrich_token(addr):
    """Run full enrichment: holders, trades, safety, creator history."""
    with state_lock:
        tok = state["tokens"].get(addr)
        if not tok or tok.get("enriched"):
            return
        tok["enriched"] = True
        creator = tok.get("pre_creator", "")

    # Holder concentration
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

    # Trades + bundles
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

    # Safety check
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

    # Creator history
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


# ─── Alert Engine ────────────────────────────────────────────────────────────

def evaluate_alert(addr):
    """Evaluate token at t=35s. Return grade or None."""
    with state_lock:
        tok = state["tokens"].get(addr)
        if not tok or tok.get("alert_evaluated"):
            return None
        tok["alert_evaluated"] = True
        # Copy needed values
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
        hour = tok.get("graduation_hour", 12)
        symbol = tok.get("symbol", "???")

    # Build filter results
    entry_filters = {}
    safety_checks = {}

    # ENTRY FILTERS
    entry_filters["top1_gte_99"] = top1 >= 99
    entry_filters["bundle_detected"] = bundle == 1
    entry_filters["price_up_5pct"] = current_gain > 5
    
    # KILL SIGNALS
    kill = False
    kill_reasons = []
    if curve_dur > 300:
        kill = True
        kill_reasons.append(f"curve_dur={curve_dur:.0f}s")
    # Price at 30s - we use current price at ~35s as proxy
    # Check if price went down >10% (current_gain < -10)
    if current_gain < -10:
        kill = True
        kill_reasons.append(f"price_down={current_gain:.1f}%")
    if creator_prev >= 1:
        kill = True
        kill_reasons.append(f"serial_creator={creator_prev}")
    
    entry_filters["no_kill_signal"] = not kill

    # SAFETY CHECKS
    safety_checks["freeze_authority"] = "BLOCK" if freeze == 1 else "PASS"
    safety_checks["mint_authority"] = "WARN" if mint == 1 else "PASS"
    safety_checks["liquidity"] = f"${liq:.0f}" if liq else "$0"

    # All entry filters must pass
    all_entry = all(entry_filters.values())
    # Safety blocks — only freeze authority is a hard block
    blocked = safety_checks["freeze_authority"] == "BLOCK"

    if not all_entry or blocked:
        logger.info("Alert SKIP %s (%s): entry=%s safety=%s kill=%s",
                     symbol, addr[:8], entry_filters, safety_checks, kill_reasons)
        return None

    # Determine grade
    is_night = hour >= 22 or hour < 7
    fast_curve = curve_dur < 60
    if all_entry and is_night and fast_curve:
        grade = "A"
    elif all_entry and fast_curve:
        grade = "B"
    else:
        grade = "C"

    # Store alert
    ts = time.time()
    with get_db() as db:
        db.execute("""INSERT INTO alerts (token_address,symbol,ts,grade,entry_filters,safety_checks,price_at_alert,action)
            VALUES (?,?,?,?,?,?,?,?)""",
            (addr, symbol, ts, grade, json.dumps(entry_filters), json.dumps(safety_checks), current_price, "pending"))

    logger.info("🚨 ALERT %s-grade: %s (%s) price=$%.6f gain=%.1f%%",
                grade, symbol, addr[:8], current_price, current_gain)
    return grade


# ─── Shadow Trading Engine ───────────────────────────────────────────────────

def can_open_position():
    """Check if we can open a new shadow position."""
    with state_lock:
        if len(state["open_positions"]) >= MAX_POSITIONS:
            return False, "max_positions"
        if time.time() - state["last_close_time"] < COOLDOWN_SECONDS:
            return False, "cooldown"
    return True, ""


def open_shadow_position(addr, grade):
    """Schedule a shadow buy at t=40s (5s human delay after alert at t=35s)."""
    with state_lock:
        tok = state["tokens"].get(addr)
        if not tok:
            return
        tok["shadow_entry_scheduled"] = True
        symbol = tok.get("symbol", "???")

    # We schedule the actual entry in the tick loop when age >= 40s
    logger.info("📊 Shadow BUY scheduled: %s (%s) grade=%s at t+40s", symbol, addr[:8], grade)


def execute_shadow_entry(addr):
    """Execute shadow buy at current price + slippage."""
    with state_lock:
        tok = state["tokens"].get(addr)
        if not tok:
            return
        price = tok.get("current_price", 0)
        liq = tok.get("current_liquidity", 0)
        symbol = tok.get("symbol", "???")
        grade = "C"  # default

    if not price:
        return

    entry_price = price * (1 + BUY_SLIPPAGE)
    ts = time.time()

    # Get alert grade
    with get_db() as db:
        row = db.execute("SELECT grade FROM alerts WHERE token_address=? ORDER BY ts DESC LIMIT 1", (addr,)).fetchone()
        if row:
            grade = row["grade"]

    # Insert shadow trade
    with get_db() as db:
        db.execute("""INSERT INTO shadow_trades
            (token_address,symbol,alert_grade,entry_time,entry_price,entry_liquidity,position_size_sol,peak_price_during,status)
            VALUES (?,?,?,?,?,?,?,?,?)""",
            (addr, symbol, grade, ts, entry_price, liq, POSITION_SIZE_SOL, entry_price, "open"))
        trade_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]

    with state_lock:
        state["open_positions"][addr] = {
            "trade_id": trade_id,
            "entry_price": entry_price,
            "entry_time": ts,
            "entry_liquidity": liq,
            "position_size_sol": POSITION_SIZE_SOL,
            "sold_25": False,
            "peak_price": entry_price,
            "symbol": symbol,
            "grade": grade,
        }

    logger.info("📊 Shadow BUY executed: %s @ $%.6f (entry with %.0f%% slippage) liq=$%.0f",
                symbol, entry_price, BUY_SLIPPAGE * 100, liq)


def manage_shadow_positions():
    """Check all open positions for exit conditions."""
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
        sold_25 = pos["sold_25"]
        trade_id = pos["trade_id"]
        symbol = pos["symbol"]
        age = now - entry_time
        gain_pct = ((current_price / entry_price) - 1) * 100 if entry_price else 0

        # Update peak
        if current_price > peak:
            peak = current_price
            with state_lock:
                if addr in state["open_positions"]:
                    state["open_positions"][addr]["peak_price"] = peak
            with get_db() as db:
                db.execute("UPDATE shadow_trades SET peak_price_during=? WHERE id=?", (peak, trade_id))

        exit_reason = ""
        exit_price = 0

        # EMERGENCY: liquidity drop > 30%
        if entry_liq > 0 and current_liq > 0:
            liq_drop = ((entry_liq - current_liq) / entry_liq) * 100
            if liq_drop > EMERGENCY_LIQ_DROP_PCT:
                exit_reason = f"EMERGENCY_LIQ_DROP_{liq_drop:.0f}pct"
                exit_price = current_price * (1 - SELL_SLIPPAGE)

        # Stop loss: -30% from entry
        if not exit_reason and gain_pct <= -STOP_LOSS_PCT:
            exit_reason = f"STOP_LOSS_{gain_pct:.1f}pct"
            exit_price = current_price * (1 - SELL_SLIPPAGE)

        # Hard exit at 5 minutes
        if not exit_reason and age >= HARD_EXIT_SECONDS:
            exit_reason = f"HARD_EXIT_{age:.0f}s"
            exit_price = current_price * (1 - SELL_SLIPPAGE)

        # Max hold at 10 minutes
        if not exit_reason and age >= MAX_HOLD_SECONDS:
            exit_reason = f"MAX_HOLD_{age:.0f}s"
            exit_price = current_price * (1 - SELL_SLIPPAGE)

        # Take profit 25% at +100%
        if not exit_reason and not sold_25 and gain_pct >= TAKE_PROFIT_1_PCT:
            # Sell 25% — partial exit
            sell_price = current_price * (1 - SELL_SLIPPAGE)
            partial_sol = POSITION_SIZE_SOL * 0.25
            partial_pnl = partial_sol * (gain_pct / 100)
            with state_lock:
                if addr in state["open_positions"]:
                    state["open_positions"][addr]["sold_25"] = True
            with get_db() as db:
                db.execute("UPDATE shadow_trades SET sold_25_at=?,sold_25_price=?,sold_25_time=? WHERE id=?",
                           (sell_price, current_price, now, trade_id))
            logger.info("📊 Shadow SELL 25%%: %s @ $%.6f (+%.1f%%) partial_pnl=%.4f SOL",
                        symbol, sell_price, gain_pct, partial_pnl)
            sold_25 = True

        # Trailing stop for remaining 75% (after 25% sold) or full position
        if not exit_reason and peak > entry_price:
            drop_from_peak = ((peak - current_price) / peak) * 100
            if drop_from_peak >= TRAILING_STOP_PCT:
                exit_reason = f"TRAILING_STOP_{drop_from_peak:.1f}pct_from_peak"
                exit_price = current_price * (1 - SELL_SLIPPAGE)

        # Execute full exit
        if exit_reason:
            if not exit_price:
                exit_price = current_price * (1 - SELL_SLIPPAGE)
            # Calculate P&L
            remaining_pct = 0.75 if sold_25 else 1.0
            remaining_sol = POSITION_SIZE_SOL * remaining_pct
            pnl_remaining = remaining_sol * ((exit_price / entry_price) - 1) if entry_price else 0
            # Add partial profit if sold 25%
            partial_pnl = 0
            if sold_25:
                with get_db() as db:
                    row = db.execute("SELECT sold_25_at FROM shadow_trades WHERE id=?", (trade_id,)).fetchone()
                    if row and row["sold_25_at"]:
                        partial_pnl = POSITION_SIZE_SOL * 0.25 * ((row["sold_25_at"] / entry_price) - 1)
            total_pnl = pnl_remaining + partial_pnl
            total_pnl_pct = (total_pnl / POSITION_SIZE_SOL) * 100

            with get_db() as db:
                db.execute("""UPDATE shadow_trades SET exit_time=?,exit_price=?,exit_reason=?,
                    pnl_sol=?,pnl_pct=?,peak_price_during=?,status='closed' WHERE id=?""",
                    (now, exit_price, exit_reason, total_pnl, total_pnl_pct, peak, trade_id))

            with state_lock:
                state["open_positions"].pop(addr, None)
                state["last_close_time"] = now

            color = "🟢" if total_pnl >= 0 else "🔴"
            logger.info("%s Shadow EXIT %s: %s @ $%.6f pnl=%.4f SOL (%.1f%%) reason=%s",
                        color, symbol, addr[:8], exit_price, total_pnl, total_pnl_pct, exit_reason)


def log_missed_opportunity(addr, grade, reason):
    """Log a missed trade opportunity."""
    with state_lock:
        tok = state["tokens"].get(addr)
        symbol = tok.get("symbol", "???") if tok else "???"
    ts = time.time()
    with get_db() as db:
        db.execute("INSERT INTO missed_opportunities (token_address,symbol,ts,alert_grade,reason) VALUES (?,?,?,?,?)",
                   (addr, symbol, ts, grade, reason))
    logger.info("⏭️ Missed opportunity: %s (%s) grade=%s reason=%s", symbol, addr[:8], grade, reason)


def update_missed_outcomes():
    """Update peak/final gains for missed opportunities."""
    with get_db() as db:
        rows = db.execute("SELECT id,token_address FROM missed_opportunities WHERE peak_gain_pct=0").fetchall()
        for row in rows:
            tok_row = db.execute("SELECT peak_gain_pct,final_gain_pct FROM tokens WHERE address=?",
                                (row["token_address"],)).fetchone()
            if tok_row:
                db.execute("UPDATE missed_opportunities SET peak_gain_pct=?,final_gain_pct=? WHERE id=?",
                           (tok_row["peak_gain_pct"], tok_row["final_gain_pct"], row["id"]))


# ─── SOL price & FNG ────────────────────────────────────────────────────────

def refresh_sol_price():
    data = safe_get(COINGECKO_SOL)
    if data and "solana" in data:
        price = data["solana"].get("usd", 0)
        if price:
            with state_lock:
                state["sol_price"] = price
                state["last_sol_refresh"] = time.time()
            logger.debug("SOL price: $%.2f", price)


def refresh_fng():
    data = safe_get(FNG_URL)
    if data and "data" in data and data["data"]:
        val = int(data["data"][0].get("value", 0))
        with state_lock:
            state["fng"] = val
            state["last_fng_refresh"] = time.time()
        logger.debug("FNG: %d", val)


# ─── Lifecycle Management ───────────────────────────────────────────────────

def manage_lifecycle():
    """Transition tokens through lifecycle stages."""
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
            # Finalize gains
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
    """Remove tokens older than 24h from memory."""
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


# ─── Main Loop ───────────────────────────────────────────────────────────────

def main_loop():
    """Background thread running every 3 seconds."""
    logger.info("🌊 Wave Rider %s starting main loop", VERSION)

    # Initial data fetch
    refresh_sol_price()
    refresh_fng()

    with state_lock:
        state["started"] = True
        state["start_time"] = time.time()

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

        # 2. Price snapshots
        try:
            collect_price_snapshots()
        except Exception as e:
            logger.error("Snapshot error: %s", e)

        # 3. Enrichment at 30-35s age
        try:
            now = time.time()
            with state_lock:
                to_enrich = [(addr, tok) for addr, tok in state["tokens"].items()
                             if not tok.get("enriched") and 30 <= now - tok["detected_at"] <= 40]
            for addr, tok in to_enrich[:3]:  # max 3 per tick to avoid rate limits
                try:
                    enrich_token(addr)
                except Exception as e:
                    logger.error("Enrich %s error: %s", addr[:8], e)
        except Exception as e:
            logger.error("Enrichment error: %s", e)

        # 4. Alert engine (evaluate at 35s)
        try:
            now = time.time()
            with state_lock:
                to_evaluate = [(addr, tok) for addr, tok in state["tokens"].items()
                               if not tok.get("alert_evaluated") and tok.get("enriched")
                               and now - tok["detected_at"] >= ALERT_SECOND]
            for addr, tok in to_evaluate:
                try:
                    grade = evaluate_alert(addr)
                    if grade:
                        can_open, reason = can_open_position()
                        if can_open:
                            open_shadow_position(addr, grade)
                        else:
                            log_missed_opportunity(addr, grade, reason)
                except Exception as e:
                    logger.error("Alert eval %s error: %s", addr[:8], e)
        except Exception as e:
            logger.error("Alert engine error: %s", e)

        # 5. Shadow trader — execute scheduled entries + manage positions
        try:
            now = time.time()
            with state_lock:
                scheduled = [(addr, tok) for addr, tok in state["tokens"].items()
                             if tok.get("shadow_entry_scheduled") and not tok.get("shadow_entered")
                             and now - tok["detected_at"] >= ALERT_SECOND + HUMAN_DELAY]
            for addr, tok in scheduled:
                try:
                    can_open, reason = can_open_position()
                    if can_open:
                        execute_shadow_entry(addr)
                        with state_lock:
                            if addr in state["tokens"]:
                                state["tokens"][addr]["shadow_entered"] = True
                    else:
                        with state_lock:
                            if addr in state["tokens"]:
                                state["tokens"][addr]["shadow_entered"] = True
                        # Get grade from alert
                        with get_db() as db:
                            row = db.execute("SELECT grade FROM alerts WHERE token_address=? ORDER BY ts DESC LIMIT 1",
                                           (addr,)).fetchone()
                        grade = row["grade"] if row else "C"
                        log_missed_opportunity(addr, grade, reason)
                except Exception as e:
                    logger.error("Shadow entry %s error: %s", addr[:8], e)

            manage_shadow_positions()
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

        # 8. SOL price + FNG refresh
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

        # Sleep remainder of tick
        elapsed = time.time() - tick_start
        sleep_time = max(0, TICK_INTERVAL - elapsed)
        if sleep_time > 0:
            time.sleep(sleep_time)


# ─── Flask App ───────────────────────────────────────────────────────────────

app = Flask(__name__)
_bg_thread_started = False


@app.before_request
def startup():
    global _bg_thread_started
    if not _bg_thread_started:
        _bg_thread_started = True
        init_db()  # Create DB before any route can query it
        t = threading.Thread(target=main_loop, daemon=True)
        t.start()
        logger.info("Background thread started")


# ─── Dashboard ───────────────────────────────────────────────────────────────

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Wave Rider V9 — Shadow Trader</title>
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
.badge-open{background:#d29922;color:#000}.badge-closed{background:#30363d;color:#c9d1d9}
.pnl-pos{color:#3fb950;font-weight:700}.pnl-neg{color:#f85149;font-weight:700}
.empty{color:#484f58;text-align:center;padding:20px}
</style></head><body>
<h1>🌊 Wave Rider V9 — Shadow Trader</h1>
<div class="sub">Paper trading engine • Auto-refresh 10s • {{time_utc}}</div>
<div class="stats">
<div class="stat"><div class="label">SOL Price</div><div class="value blue">${{sol_price}}</div></div>
<div class="stat"><div class="label">FNG</div><div class="value {{fng_color}}">{{fng}}</div></div>
<div class="stat"><div class="label">Tracking</div><div class="value">{{tracking}}</div></div>
<div class="stat"><div class="label">Open Positions</div><div class="value yellow">{{open_pos}}</div></div>
<div class="stat"><div class="label">Total Trades</div><div class="value">{{total_trades}}</div></div>
<div class="stat"><div class="label">Win Rate</div><div class="value {{wr_color}}">{{win_rate}}</div></div>
<div class="stat"><div class="label">Total P&L</div><div class="value {{pnl_color}}">{{total_pnl}} SOL</div></div>
<div class="stat"><div class="label">Alerts Today</div><div class="value">{{alerts_today}}</div></div>
<div class="stat"><div class="label">Uptime</div><div class="value">{{uptime}}</div></div>
</div>

<div class="section-title">📊 Open Shadow Positions</div>
{{open_table}}

<div class="section-title">📈 Closed Shadow Trades (Recent 20)</div>
{{closed_table}}

<div class="section-title">🚨 Recent Alerts (20)</div>
{{alerts_table}}

<div class="section-title">⏭️ Missed Opportunities (Recent 10)</div>
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

    uptime_s = now - start_time if start_time else 0
    uptime_h = int(uptime_s // 3600)
    uptime_m = int((uptime_s % 3600) // 60)
    uptime = f"{uptime_h}h {uptime_m}m"

    fng_color = "green" if fng >= 60 else ("red" if fng <= 30 else "yellow")

    with get_db() as db:
        # Shadow stats
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

    # Open positions table
    if open_trades:
        rows = ""
        for t in open_trades:
            with state_lock:
                tok = state["tokens"].get(t["token_address"], {})
            curr = tok.get("current_price", 0)
            gain = ((curr / t["entry_price"]) - 1) * 100 if t["entry_price"] and curr else 0
            age = now - t["entry_time"]
            pclass = "pnl-pos" if gain >= 0 else "pnl-neg"
            grade_class = f"badge-{t['alert_grade'].lower()}" if t['alert_grade'] else "badge-c"
            sold = "✅" if t["sold_25_at"] else "—"
            rows += f"""<tr>
                <td>{t['symbol']}</td>
                <td><span class="badge {grade_class}">{t['alert_grade']}</span></td>
                <td>${t['entry_price']:.6f}</td>
                <td>${curr:.6f}</td>
                <td class="{pclass}">{gain:+.1f}%</td>
                <td>{age:.0f}s</td>
                <td>{t['position_size_sol']} SOL</td>
                <td>{sold}</td>
                <td>${t['entry_liquidity']:.0f}</td>
            </tr>"""
        open_table = f"""<table><tr><th>Token</th><th>Grade</th><th>Entry</th><th>Current</th>
            <th>P&L</th><th>Age</th><th>Size</th><th>25% Sold</th><th>Liq</th></tr>{rows}</table>"""
    else:
        open_table = '<div class="empty">No open positions</div>'

    # Closed trades table
    if closed[:20]:
        rows = ""
        for t in closed[:20]:
            pclass = "pnl-pos" if t["pnl_sol"] >= 0 else "pnl-neg"
            grade_class = f"badge-{t['alert_grade'].lower()}" if t['alert_grade'] else "badge-c"
            hold = t["exit_time"] - t["entry_time"] if t["exit_time"] and t["entry_time"] else 0
            ts_str = datetime.fromtimestamp(t["entry_time"], tz=timezone.utc).strftime("%H:%M:%S") if t["entry_time"] else ""
            rows += f"""<tr>
                <td>{ts_str}</td>
                <td>{t['symbol']}</td>
                <td><span class="badge {grade_class}">{t['alert_grade']}</span></td>
                <td>${t['entry_price']:.6f}</td>
                <td>${t['exit_price']:.6f}</td>
                <td class="{pclass}">{t['pnl_sol']:+.4f}</td>
                <td class="{pclass}">{t['pnl_pct']:+.1f}%</td>
                <td>{hold:.0f}s</td>
                <td>{t['exit_reason'][:30]}</td>
            </tr>"""
        closed_table = f"""<table><tr><th>Time</th><th>Token</th><th>Grade</th><th>Entry</th>
            <th>Exit</th><th>P&L SOL</th><th>P&L %</th><th>Hold</th><th>Reason</th></tr>{rows}</table>"""
    else:
        closed_table = '<div class="empty">No closed trades yet</div>'

    # Alerts table
    if alerts:
        rows = ""
        for a in alerts:
            grade_class = f"badge-{a['grade'].lower()}" if a['grade'] else "badge-c"
            ts_str = datetime.fromtimestamp(a["ts"], tz=timezone.utc).strftime("%H:%M:%S") if a["ts"] else ""
            rows += f"""<tr>
                <td>{ts_str}</td>
                <td>{a['symbol']}</td>
                <td><span class="badge {grade_class}">{a['grade']}</span></td>
                <td>${a['price_at_alert']:.6f}</td>
                <td>{a['action']}</td>
                <td style="max-width:200px;overflow:hidden;text-overflow:ellipsis">{a['entry_filters'][:60]}</td>
            </tr>"""
        alerts_table = f"""<table><tr><th>Time</th><th>Token</th><th>Grade</th>
            <th>Price</th><th>Action</th><th>Filters</th></tr>{rows}</table>"""
    else:
        alerts_table = '<div class="empty">No alerts yet</div>'

    # Missed table
    if missed:
        rows = ""
        for m in missed:
            ts_str = datetime.fromtimestamp(m["ts"], tz=timezone.utc).strftime("%H:%M:%S") if m["ts"] else ""
            peak_class = "pnl-pos" if m["peak_gain_pct"] > 0 else ""
            rows += f"""<tr>
                <td>{ts_str}</td>
                <td>{m['symbol']}</td>
                <td>{m['alert_grade']}</td>
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
    html = html.replace("{{time_utc}}", time_utc)
    html = html.replace("{{sol_price}}", f"{sol_price:.2f}")
    html = html.replace("{{fng}}", str(fng))
    html = html.replace("{{fng_color}}", fng_color)
    html = html.replace("{{tracking}}", str(tracking))
    html = html.replace("{{open_pos}}", str(open_pos))
    html = html.replace("{{total_trades}}", str(total_trades))
    html = html.replace("{{win_rate}}", win_rate)
    html = html.replace("{{wr_color}}", wr_color)
    html = html.replace("{{total_pnl}}", f"{total_pnl:+.4f}")
    html = html.replace("{{pnl_color}}", pnl_color)
    html = html.replace("{{alerts_today}}", str(alerts_today_count))
    html = html.replace("{{uptime}}", uptime)
    html = html.replace("{{open_table}}", open_table)
    html = html.replace("{{closed_table}}", closed_table)
    html = html.replace("{{alerts_table}}", alerts_table)
    html = html.replace("{{missed_table}}", missed_table)
    return html


# ─── Routes ──────────────────────────────────────────────────────────────────

@app.route("/")
def dashboard():
    return render_dashboard(), 200, {"Content-Type": "text/html"}


@app.route("/health")
def health():
    with state_lock:
        return jsonify({
            "status": "ok",
            "version": VERSION,
            "started": state["started"],
            "sol_price": state["sol_price"],
            "fng": state["fng"],
            "tracking": len(state["tokens"]),
            "open_positions": len(state["open_positions"]),
            "tick_count": state["tick_count"],
            "uptime_seconds": time.time() - state["start_time"] if state["start_time"] else 0,
        })


@app.route("/api/shadow")
def api_shadow():
    with get_db() as db:
        open_trades = [dict(r) for r in db.execute(
            "SELECT * FROM shadow_trades WHERE status='open' ORDER BY entry_time DESC").fetchall()]
        closed_trades = [dict(r) for r in db.execute(
            "SELECT * FROM shadow_trades WHERE status='closed' ORDER BY exit_time DESC LIMIT 50").fetchall()]
        total_pnl = db.execute("SELECT COALESCE(SUM(pnl_sol),0) as s FROM shadow_trades WHERE status='closed'").fetchone()["s"]
        total_closed = db.execute("SELECT COUNT(*) as c FROM shadow_trades WHERE status='closed'").fetchone()["c"]
        wins = db.execute("SELECT COUNT(*) as c FROM shadow_trades WHERE status='closed' AND pnl_sol>0").fetchone()["c"]
    return jsonify({
        "open": open_trades,
        "closed": closed_trades,
        "stats": {
            "total_pnl_sol": total_pnl,
            "total_trades": total_closed,
            "wins": wins,
            "win_rate": (wins / total_closed * 100) if total_closed else 0,
        }
    })


@app.route("/api/alerts")
def api_alerts():
    with get_db() as db:
        rows = [dict(r) for r in db.execute("SELECT * FROM alerts ORDER BY ts DESC LIMIT 50").fetchall()]
    return jsonify(rows)


@app.route("/api/alerts/pending")
def api_alerts_pending():
    with get_db() as db:
        rows = [dict(r) for r in db.execute(
            "SELECT * FROM alerts WHERE action='pending' ORDER BY ts DESC").fetchall()]
    return jsonify(rows)


@app.route("/api/waves")
def api_waves():
    limit = request.args.get("limit", 50, type=int)
    with get_db() as db:
        rows = [dict(r) for r in db.execute(
            "SELECT * FROM tokens ORDER BY detected_at DESC LIMIT ?", (limit,)).fetchall()]
    return jsonify(rows)


@app.route("/api/export")
def api_export():
    with get_db() as db:
        tokens = [dict(r) for r in db.execute("SELECT * FROM tokens ORDER BY detected_at DESC LIMIT 500").fetchall()]
        shadows = [dict(r) for r in db.execute("SELECT * FROM shadow_trades ORDER BY entry_time DESC").fetchall()]
        alerts = [dict(r) for r in db.execute("SELECT * FROM alerts ORDER BY ts DESC LIMIT 200").fetchall()]
        missed = [dict(r) for r in db.execute("SELECT * FROM missed_opportunities ORDER BY ts DESC").fetchall()]
    return jsonify({
        "version": VERSION,
        "exported_at": time.time(),
        "tokens": tokens,
        "shadow_trades": shadows,
        "alerts": alerts,
        "missed_opportunities": missed,
    })


@app.route("/logs")
def logs():
    lines = list(log_buffer)
    html = f"""<!DOCTYPE html><html><head><meta charset="utf-8">
    <title>WR9 Logs</title><meta http-equiv="refresh" content="5">
    <style>body{{background:#0d1117;color:#c9d1d9;font-family:monospace;font-size:12px;padding:16px}}
    pre{{white-space:pre-wrap}}</style></head>
    <body><h2 style="color:#58a6ff">Wave Rider V9 Logs ({len(lines)} lines)</h2>
    <pre>{"<br>".join(lines[-LOG_MAX_LINES:])}</pre></body></html>"""
    return html, 200, {"Content-Type": "text/html"}


# ─── Entry point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False)
