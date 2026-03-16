#!/usr/bin/env python3
"""
Graduation Detector v2 — Full Price Curve Tracker

Tracks every token from graduation (second 0) through minute 5.
Captures price snapshots at: 0s, 30s, 1min, 2min, 3min, 5min.
This data reveals the optimal buy/sell window.

Endpoints:
  GET /                         → Status
  GET /api/graduations          → Recent graduates with all snapshots
  GET /api/graduations/{addr}   → Single token full curve
  GET /api/curves               → Aggregated buy/sell timing analysis
  GET /api/stats                → Scanner stats
"""

import os
import time
import json
import threading
import sqlite3
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from flask import Flask, jsonify, request as flask_request

app = Flask(__name__)

DB_PATH = os.environ.get("DB_PATH", "graduations.db")
DEXSCREENER_BASE = "https://api.dexscreener.com"

scan_count = 0
last_scan_time = None
tracker_stats = {"tracked": 0, "snapshots_taken": 0, "errors": 0}

# Snapshot schedule: seconds after detection
SNAPSHOT_SCHEDULE = [30, 60, 120, 180, 300]  # 30s, 1m, 2m, 3m, 5m


# ── Database ────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS tokens (
            address TEXT PRIMARY KEY,
            symbol TEXT,
            name TEXT,
            dex_id TEXT,
            pair_address TEXT,
            pair_created_at INTEGER,
            
            detected_at TEXT,
            detected_ts INTEGER,
            age_at_detection_sec REAL,
            
            -- Snapshot at detection (t=0)
            t0_price TEXT,
            t0_liq REAL,
            t0_vol_24h REAL,
            t0_vol_h1 REAL,
            t0_vol_m5 REAL,
            t0_fdv REAL,
            t0_mcap REAL,
            t0_buys_m5 INTEGER,
            t0_sells_m5 INTEGER,
            t0_buys_h1 INTEGER,
            t0_sells_h1 INTEGER,
            t0_price_change_m5 REAL,
            t0_price_change_h1 REAL,
            t0_has_socials INTEGER,
            t0_boost_amount INTEGER,
            
            -- Snapshots at intervals
            t30s_ts INTEGER, t30s_price TEXT, t30s_liq REAL, t30s_vol_m5 REAL, t30s_buys_m5 INTEGER, t30s_sells_m5 INTEGER,
            t1m_ts INTEGER, t1m_price TEXT, t1m_liq REAL, t1m_vol_m5 REAL, t1m_buys_m5 INTEGER, t1m_sells_m5 INTEGER,
            t2m_ts INTEGER, t2m_price TEXT, t2m_liq REAL, t2m_vol_m5 REAL, t2m_buys_m5 INTEGER, t2m_sells_m5 INTEGER,
            t3m_ts INTEGER, t3m_price TEXT, t3m_liq REAL, t3m_vol_m5 REAL, t3m_buys_m5 INTEGER, t3m_sells_m5 INTEGER,
            t5m_ts INTEGER, t5m_price TEXT, t5m_liq REAL, t5m_vol_m5 REAL, t5m_buys_m5 INTEGER, t5m_sells_m5 INTEGER,
            
            -- Computed returns at each interval
            return_30s REAL,
            return_1m REAL,
            return_2m REAL,
            return_3m REAL,
            return_5m REAL,
            
            -- Peak tracking
            peak_price TEXT,
            peak_return REAL,
            peak_time_sec REAL,
            
            -- Classification (filled later by analysis)
            pattern TEXT,  -- 'rocket', 'pump_dump', 'slow_climb', 'flat', 'rug'
            best_buy_sec REAL,
            best_sell_sec REAL,
            max_profit_pct REAL
        );
        
        CREATE INDEX IF NOT EXISTS idx_tokens_detected ON tokens(detected_ts);
        CREATE INDEX IF NOT EXISTS idx_tokens_pending ON tokens(t30s_ts, t1m_ts, t2m_ts, t3m_ts, t5m_ts);
    """)
    conn.commit()
    conn.close()


# ── API Helpers ─────────────────────────────────────────────────────────

def fetch_json(url, timeout=8):
    try:
        req = Request(url, headers={"User-Agent": "GradDetector/2.0"})
        with urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        tracker_stats["errors"] += 1
        return None


def get_pair_data(address):
    """Fetch current pair data for a token."""
    data = fetch_json(f"{DEXSCREENER_BASE}/token-pairs/v1/solana/{address}")
    if not data or not isinstance(data, list) or len(data) == 0:
        return None
    return max(data, key=lambda p: (p.get("liquidity") or {}).get("usd", 0))


def calc_return(initial_price, current_price):
    try:
        ip = float(initial_price)
        cp = float(current_price)
        if ip > 0:
            return round((cp - ip) / ip * 100, 2)
    except (ValueError, TypeError):
        pass
    return None


# ── Discovery Loop ──────────────────────────────────────────────────────

def discover_new_tokens():
    """Find new graduated tokens from multiple sources."""
    global scan_count, last_scan_time
    
    conn = get_db()
    known = set(r[0] for r in conn.execute("SELECT address FROM tokens").fetchall())
    new_tokens = []
    candidates = {}  # address -> source
    
    # Source 1: Latest profiles
    profiles = fetch_json(f"{DEXSCREENER_BASE}/token-profiles/latest/v1")
    if profiles and isinstance(profiles, list):
        for p in profiles:
            if p.get("chainId") == "solana":
                addr = p.get("tokenAddress", "")
                if addr and addr not in known:
                    candidates[addr] = "profiles"
    
    # Source 2: Latest boosts
    boosts = fetch_json(f"{DEXSCREENER_BASE}/token-boosts/latest/v1")
    if boosts and isinstance(boosts, list):
        for b in boosts:
            if b.get("chainId") == "solana":
                addr = b.get("tokenAddress", "")
                if addr and addr not in known:
                    candidates[addr] = f"boost({b.get('amount', 0)})"
    
    # Source 3: Top boosts
    top_boosts = fetch_json(f"{DEXSCREENER_BASE}/token-boosts/top/v1")
    if top_boosts and isinstance(top_boosts, list):
        for b in top_boosts:
            if b.get("chainId") == "solana":
                addr = b.get("tokenAddress", "")
                if addr and addr not in known:
                    candidates[addr] = f"top_boost({b.get('totalAmount', 0)})"
    
    # Check each candidate for graduation
    for addr, source in list(candidates.items())[:40]:  # Max 40 per scan
        if addr in known:
            continue
        
        pair = get_pair_data(addr)
        if not pair:
            continue
        
        liq = (pair.get("liquidity") or {}).get("usd", 0)
        dex = pair.get("dexId", "")
        created = pair.get("pairCreatedAt", 0)
        
        # Must be graduated with real liquidity
        if dex not in ("raydium", "pumpswap", "orca"):
            continue
        if liq < 5000:
            continue
        
        now = int(time.time())
        age_sec = (now * 1000 - created) / 1000 if created > 0 else 0
        
        base = pair.get("baseToken", {})
        txns = pair.get("txns", {})
        pc = pair.get("priceChange", {})
        vol = pair.get("volume", {})
        info = pair.get("info", {}) or {}
        socials = info.get("socials", []) or []
        websites = info.get("websites", []) or []
        
        # Record birth
        conn.execute("""
            INSERT OR IGNORE INTO tokens
            (address, symbol, name, dex_id, pair_address, pair_created_at,
             detected_at, detected_ts, age_at_detection_sec,
             t0_price, t0_liq, t0_vol_24h, t0_vol_h1, t0_vol_m5, t0_fdv, t0_mcap,
             t0_buys_m5, t0_sells_m5, t0_buys_h1, t0_sells_h1,
             t0_price_change_m5, t0_price_change_h1,
             t0_has_socials, t0_boost_amount)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            addr, base.get("symbol", "???"), base.get("name", "Unknown"),
            dex, pair.get("pairAddress", ""), created,
            datetime.now(timezone.utc).isoformat(), now, age_sec,
            pair.get("priceUsd", "0"), liq,
            vol.get("h24", 0), vol.get("h1", 0), vol.get("m5", 0),
            pair.get("fdv", 0), pair.get("marketCap", 0),
            (txns.get("m5") or {}).get("buys", 0),
            (txns.get("m5") or {}).get("sells", 0),
            (txns.get("h1") or {}).get("buys", 0),
            (txns.get("h1") or {}).get("sells", 0),
            pc.get("m5", 0) or 0, pc.get("h1", 0) or 0,
            1 if (len(socials) > 0 or len(websites) > 0) else 0,
            0,
        ))
        
        known.add(addr)
        new_tokens.append({"symbol": base.get("symbol", "???"), "address": addr, "age_sec": age_sec, "liq": liq})
        tracker_stats["tracked"] += 1
        
        time.sleep(0.25)
    
    conn.commit()
    conn.close()
    
    scan_count += 1
    last_scan_time = datetime.now(timezone.utc).isoformat()
    
    return new_tokens


# ── Snapshot Loop ───────────────────────────────────────────────────────

def take_snapshots():
    """Check back on recently discovered tokens at scheduled intervals."""
    conn = get_db()
    now = int(time.time())
    
    # Find tokens needing snapshots
    snapshots_config = [
        ("t30s", 30, 45),    # target 30s, max 45s
        ("t1m", 60, 90),
        ("t2m", 120, 180),
        ("t3m", 180, 270),
        ("t5m", 300, 450),
    ]
    
    for prefix, target_sec, max_sec in snapshots_config:
        col_ts = f"{prefix}_ts"
        col_price = f"{prefix}_price"
        
        # Tokens where this snapshot is due but not taken
        rows = conn.execute(f"""
            SELECT address, detected_ts, t0_price
            FROM tokens
            WHERE {col_ts} IS NULL
              AND (? - detected_ts) >= ?
              AND (? - detected_ts) <= ?
            ORDER BY detected_ts DESC
            LIMIT 10
        """, (now, target_sec, now, max_sec)).fetchall()
        
        for row in rows:
            addr = row["address"]
            t0_price = row["t0_price"]
            
            pair = get_pair_data(addr)
            if not pair:
                continue
            
            price = pair.get("priceUsd", "0")
            liq = (pair.get("liquidity") or {}).get("usd", 0)
            vol_m5 = (pair.get("volume") or {}).get("m5", 0)
            buys_m5 = (pair.get("txns", {}).get("m5") or {}).get("buys", 0)
            sells_m5 = (pair.get("txns", {}).get("m5") or {}).get("sells", 0)
            ret = calc_return(t0_price, price)
            
            return_col = f"return_{prefix.replace('t', '').replace('s', 's').replace('m', 'm')}"
            # Map prefix to return column name
            ret_map = {"t30s": "return_30s", "t1m": "return_1m", "t2m": "return_2m", "t3m": "return_3m", "t5m": "return_5m"}
            
            conn.execute(f"""
                UPDATE tokens SET
                    {col_ts} = ?, {col_price} = ?, {prefix}_liq = ?,
                    {prefix}_vol_m5 = ?, {prefix}_buys_m5 = ?, {prefix}_sells_m5 = ?,
                    {ret_map[prefix]} = ?
                WHERE address = ?
            """, (now, price, liq, vol_m5, buys_m5, sells_m5, ret, addr))
            
            # Update peak
            if ret is not None:
                current_peak = conn.execute("SELECT peak_return FROM tokens WHERE address = ?", (addr,)).fetchone()
                if current_peak and (current_peak["peak_return"] is None or ret > (current_peak["peak_return"] or -999)):
                    elapsed = now - row["detected_ts"]
                    conn.execute("""
                        UPDATE tokens SET peak_price = ?, peak_return = ?, peak_time_sec = ?
                        WHERE address = ?
                    """, (price, ret, elapsed, addr))
            
            tracker_stats["snapshots_taken"] += 1
            time.sleep(0.3)
    
    conn.commit()
    conn.close()


# ── Pattern Classification ──────────────────────────────────────────────

def classify_patterns():
    """Classify token price patterns after 5-min data is complete."""
    conn = get_db()
    
    rows = conn.execute("""
        SELECT address, return_30s, return_1m, return_2m, return_3m, return_5m, peak_return
        FROM tokens
        WHERE t5m_ts IS NOT NULL AND pattern IS NULL
    """).fetchall()
    
    for row in rows:
        returns = [row["return_30s"] or 0, row["return_1m"] or 0, row["return_2m"] or 0, row["return_3m"] or 0, row["return_5m"] or 0]
        peak = row["peak_return"] or 0
        final = returns[-1]
        
        # Find best buy/sell points
        best_buy_idx = 0  # Always at detection
        best_sell_idx = returns.index(max(returns))
        time_map = [30, 60, 120, 180, 300]
        max_profit = max(returns) - 0  # Buy at t0, sell at peak
        
        # Classify
        if peak > 50 and final < peak * 0.3:
            pattern = "pump_dump"
        elif all(r > returns[i-1] if i > 0 else True for i, r in enumerate(returns)) and final > 20:
            pattern = "rocket"
        elif final > 10:
            pattern = "slow_climb"
        elif final < -30:
            pattern = "rug"
        elif abs(final) < 10:
            pattern = "flat"
        else:
            pattern = "volatile"
        
        conn.execute("""
            UPDATE tokens SET pattern = ?, best_buy_sec = 0, best_sell_sec = ?, max_profit_pct = ?
            WHERE address = ?
        """, (pattern, time_map[best_sell_idx] if best_sell_idx < len(time_map) else 0, max_profit, row["address"]))
    
    conn.commit()
    conn.close()


# ── Main Loop ───────────────────────────────────────────────────────────

def main_loop():
    """Background: discover every 30s, snapshot continuously."""
    cycle = 0
    while True:
        try:
            # Discover new tokens every cycle
            new = discover_new_tokens()
            if new:
                syms = ", ".join(f"{t['symbol']}({t['age_sec']:.0f}s)" for t in new[:5])
                print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] NEW: {len(new)} — {syms}")
            
            # Take snapshots for tracked tokens
            take_snapshots()
            
            # Classify patterns every 10 cycles
            if cycle % 10 == 0:
                classify_patterns()
            
            cycle += 1
        except Exception as e:
            print(f"Loop error: {e}")
            tracker_stats["errors"] += 1
        
        time.sleep(30)


# ── API Routes ──────────────────────────────────────────────────────────

@app.route("/")
def index():
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) as c FROM tokens").fetchone()["c"]
    with_5m = conn.execute("SELECT COUNT(*) as c FROM tokens WHERE t5m_ts IS NOT NULL").fetchone()["c"]
    conn.close()
    return jsonify({
        "status": "running",
        "scans": scan_count,
        "last_scan": last_scan_time,
        "total_tracked": total,
        "with_full_curve": with_5m,
        "stats": tracker_stats,
    })


@app.route("/health")
def health():
    return "OK"


@app.route("/api/graduations")
def get_graduations():
    conn = get_db()
    since = flask_request.args.get("since", type=int, default=0)
    limit = flask_request.args.get("limit", type=int, default=50)
    max_age = flask_request.args.get("max_age_min", type=int, default=30)
    
    now = int(time.time())
    cutoff = max(since, now - max_age * 60)
    
    rows = conn.execute("""
        SELECT address, symbol, name, dex_id, detected_at, detected_ts, age_at_detection_sec,
               t0_price, t0_liq, t0_vol_24h, t0_fdv,
               t0_buys_m5, t0_sells_m5, t0_buys_h1, t0_sells_h1,
               t0_has_socials, t0_boost_amount,
               return_30s, return_1m, return_2m, return_3m, return_5m,
               peak_return, peak_time_sec, pattern, max_profit_pct
        FROM tokens
        WHERE detected_ts >= ?
        ORDER BY detected_ts DESC
        LIMIT ?
    """, (cutoff, limit)).fetchall()
    
    results = [dict(r) for r in rows]
    conn.close()
    
    return jsonify({"count": len(results), "graduations": results})


@app.route("/api/graduations/<address>")
def get_graduation(address):
    conn = get_db()
    row = conn.execute("SELECT * FROM tokens WHERE address = ?", (address,)).fetchone()
    conn.close()
    if not row:
        return jsonify({"error": "not found"}), 404
    return jsonify(dict(row))


@app.route("/api/curves")
def get_curves():
    """Aggregated analysis: when to buy, when to sell."""
    conn = get_db()
    
    # Only analyze tokens with full 5-min data
    rows = conn.execute("""
        SELECT return_30s, return_1m, return_2m, return_3m, return_5m,
               peak_return, peak_time_sec, pattern, max_profit_pct,
               t0_liq, t0_vol_24h, t0_buys_m5, t0_has_socials
        FROM tokens
        WHERE t5m_ts IS NOT NULL
    """).fetchall()
    
    if not rows:
        conn.close()
        return jsonify({"message": "No complete curves yet. Data collecting...", "total": 0})
    
    total = len(rows)
    
    # Average returns at each interval
    avg = lambda col: sum(r[col] or 0 for r in rows) / total
    
    # Win rates at each interval
    wr = lambda col: sum(1 for r in rows if (r[col] or 0) > 0) / total * 100
    
    # By pattern
    patterns = {}
    for r in rows:
        p = r["pattern"] or "unknown"
        if p not in patterns:
            patterns[p] = {"count": 0, "avg_peak": 0, "avg_5m": 0}
        patterns[p]["count"] += 1
        patterns[p]["avg_peak"] += r["peak_return"] or 0
        patterns[p]["avg_5m"] += r["return_5m"] or 0
    
    for p in patterns:
        c = patterns[p]["count"]
        patterns[p]["avg_peak"] = round(patterns[p]["avg_peak"] / c, 1)
        patterns[p]["avg_5m"] = round(patterns[p]["avg_5m"] / c, 1)
    
    # Optimal timing
    intervals = ["30s", "1m", "2m", "3m", "5m"]
    cols = ["return_30s", "return_1m", "return_2m", "return_3m", "return_5m"]
    
    curve = {intervals[i]: {"avg_return": round(avg(cols[i]), 2), "win_rate": round(wr(cols[i]), 1)} for i in range(5)}
    
    # Best sell point (highest average return)
    best_sell = max(curve.items(), key=lambda x: x[1]["avg_return"])
    
    conn.close()
    
    return jsonify({
        "total_tokens_analyzed": total,
        "price_curve": curve,
        "optimal_sell_point": best_sell[0],
        "optimal_sell_avg_return": best_sell[1]["avg_return"],
        "patterns": patterns,
        "insight": f"Based on {total} tokens: best average exit at {best_sell[0]} ({best_sell[1]['avg_return']:+.1f}%, {best_sell[1]['win_rate']:.0f}% win rate)"
    })


@app.route("/api/stats")
def get_stats():
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) as c FROM tokens").fetchone()["c"]
    with_30s = conn.execute("SELECT COUNT(*) as c FROM tokens WHERE t30s_ts IS NOT NULL").fetchone()["c"]
    with_5m = conn.execute("SELECT COUNT(*) as c FROM tokens WHERE t5m_ts IS NOT NULL").fetchone()["c"]
    classified = conn.execute("SELECT COUNT(*) as c FROM tokens WHERE pattern IS NOT NULL").fetchone()["c"]
    
    patterns = dict(conn.execute("SELECT pattern, COUNT(*) FROM tokens WHERE pattern IS NOT NULL GROUP BY pattern").fetchall())
    
    conn.close()
    return jsonify({
        "scans": scan_count,
        "last_scan": last_scan_time,
        "total_tracked": total,
        "with_30s_data": with_30s,
        "with_full_5m_curve": with_5m,
        "classified": classified,
        "patterns": patterns,
        "tracker": tracker_stats,
    })


# ── Start ───────────────────────────────────────────────────────────────

init_db()

if __name__ == "__main__":
    loop_thread = threading.Thread(target=main_loop, daemon=True)
    loop_thread.start()
    
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
