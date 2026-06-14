"""
StockUpside.io — Offline Data Generator
========================================
Run this script separately from the Flask server to fetch fresh stock data
and write it to the SQLite cache. The server never calls this directly.

Usage:
    python3 server/generate.py

Recommended schedule (cron example — runs at 01:00 every night):
    0 1 * * * /usr/bin/python3 /path/to/server/generate.py >> /var/log/stockupside-generate.log 2>&1

The server will serve the previous day's data while this script runs,
then automatically pick up the new data on the next cache miss.
"""

import json, sqlite3, time, datetime, os, random, urllib.request, sys, signal
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

import yfinance as yf

# ── Paths ──────────────────────────────────────────────────────────────────────
# Support running as: python3 server/generate.py  OR  python3 generate.py
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
# If we're inside server/, BASE_DIR is one level up; otherwise same dir.
if os.path.basename(_THIS_DIR) == "server":
    BASE_DIR = os.path.dirname(_THIS_DIR)
else:
    BASE_DIR = _THIS_DIR

DB_PATH = os.path.join(BASE_DIR, "server", "cache.db")

# ── Hardcoded fallback universe ────────────────────────────────────────────────
UNIVERSE_FALLBACK = [
    "NVDA","AAPL","MSFT","GOOGL","AMZN","META","TSLA","AVGO","ORCL","CRM",
    "AMD","INTC","QCOM","NOW","INTU","ADBE","SNOW","PLTR","NET","DDOG",
    "ZS","CRWD","PANW","FTNT","MDB","UBER","LYFT","SPOT","RBLX","U",
    "JPM","BAC","WFC","GS","MS","C","AXP","V","MA","PYPL",
    "BRK-B","BLK","SCHW","JNJ","UNH","LLY","PFE","ABBV","MRK","TMO",
    "DHR","ISRG","AMGN","GILD","MRNA","REGN","BIIB","VRTX","XOM","CVX",
    "COP","EOG","SLB","MPC","OXY","WMT","COST","PG","KO","PEP",
    "MCD","SBUX","NKE","TGT","HD","LOW","GM","F","RIVN","LUV",
    "DAL","UAL","BA","GE","CAT","DE","HON","RTX","LMT","NOC",
    "UPS","FDX","NFLX","DIS","CMCSA","T","VZ","TMUS","ABNB","BKNG",
    "NEE","DUK","SO","AMT","PLD","EQIX","SPG","NEM","FCX","APD","SHW",
]

CONSENSUS_SCORE = {
    "Strong Buy": 5, "Buy": 4, "Hold": 3, "Underperform": 2, "Sell": 1,
}

# ── DB helpers ─────────────────────────────────────────────────────────────────
def get_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=5000")
    return con

def init_db():
    con = get_db()
    con.execute("""CREATE TABLE IF NOT EXISTS cache(
        date TEXT PRIMARY KEY, data TEXT, ts INTEGER)""")
    con.execute("""CREATE TABLE IF NOT EXISTS subscribers(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT UNIQUE, plan TEXT DEFAULT 'free',
        stripe_id TEXT, created_at INTEGER)""")
    con.execute("""CREATE TABLE IF NOT EXISTS snapshots(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL, ticker TEXT NOT NULL, rank INTEGER,
        current_price REAL, target_price REAL, upside_pct REAL,
        consensus TEXT, analyst_count INTEGER,
        UNIQUE(date, ticker))""")
    con.execute("""CREATE TABLE IF NOT EXISTS performance(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        snapshot_date TEXT NOT NULL, ticker TEXT NOT NULL,
        days_later INTEGER NOT NULL, price_then REAL, price_now REAL,
        actual_return REAL, hit_target INTEGER, checked_date TEXT,
        UNIQUE(snapshot_date, ticker, days_later))""")
    # Checkpoint table: stores per-ticker rows for the run currently in
    # progress, keyed by run_date. If generate.py is killed (timeout,
    # OOM, server restart, power loss, etc.) before finishing, the next
    # run picks up exactly where it left off instead of starting over —
    # and a partial merge into `cache` happens periodically so the site
    # is never stuck on yesterday's data for the full 3+ hour run.
    con.execute("""CREATE TABLE IF NOT EXISTS progress(
        run_date TEXT NOT NULL, ticker TEXT NOT NULL,
        row_json TEXT NOT NULL, ts INTEGER,
        PRIMARY KEY (run_date, ticker))""")

    # ── One-time migration: purge stale snapshots ───────────────────────────
    # Until this fix, `consensus` was derived from Yahoo's recommendationKey,
    # which could disagree with the actual sb/b/h/s vote breakdown (e.g.
    # recommendationKey="hold" with 1 Strong Buy + 7 Buy and 0 Hold/Sell).
    # generate_stocks() now derives consensus from the vote counts instead,
    # but `snapshots` written under the old logic have incorrect consensus
    # values baked in. get_momentum() compares today's (correct) consensus
    # against these stale baselines, producing false "upgrade"/"downgrade"
    # signals (e.g. a fabricated "Hold → Strong Buy" for a stock whose
    # rating hasn't actually changed).
    #
    # We can't recompute the historical consensus (vote breakdowns weren't
    # stored), so the only correct fix is to purge old snapshots — momentum
    # will repopulate naturally over the next 7/30/90 days using correct
    # values. This runs once, gated by a flag in `meta`.
    con.execute("""CREATE TABLE IF NOT EXISTS meta(
        key TEXT PRIMARY KEY, value TEXT)""")
    migrated = con.execute(
        "SELECT value FROM meta WHERE key='snapshots_consensus_fix_v1'"
    ).fetchone()
    if not migrated:
        today = datetime.date.today().isoformat()
        cur = con.execute("DELETE FROM snapshots WHERE date < ?", (today,))
        con.execute(
            "INSERT OR REPLACE INTO meta VALUES ('snapshots_consensus_fix_v1', ?)",
            (today,),
        )
        print(f"  ↻  One-time migration: purged {cur.rowcount} stale snapshot "
              f"row(s) from before the consensus-derivation fix")

    con.commit()
    con.close()

def save_cache(data: list):
    today = datetime.date.today().isoformat()
    con = get_db()
    con.execute("INSERT OR REPLACE INTO cache VALUES(?,?,?)",
                (today, json.dumps(data), int(time.time())))
    con.commit()
    con.close()
    print(f"  ✓  Saved {len(data)} stocks to cache for {today}")

# ── Checkpointing ──────────────────────────────────────────────────────────────
def load_progress(run_date: str) -> dict:
    """Return {ticker: row_dict} for every ticker already processed in
    today's run. Used to resume after a kill/timeout without redoing work."""
    con = get_db()
    rows = con.execute(
        "SELECT ticker, row_json FROM progress WHERE run_date=?", (run_date,)
    ).fetchall()
    con.close()
    return {ticker: json.loads(row_json) for ticker, row_json in rows}

def save_progress_row(run_date: str, ticker: str, row: dict):
    """Persist a single ticker's result immediately so it survives a crash."""
    con = get_db()
    con.execute(
        "INSERT OR REPLACE INTO progress VALUES (?, ?, ?, ?)",
        (run_date, ticker, json.dumps(row), int(time.time())),
    )
    con.commit()
    con.close()

def get_most_recent_full_cache(exclude_date: str | None = None) -> list:
    """Return the most recent cached stock list (any date), optionally
    excluding a specific date. Used so a partial in-progress run can be
    overlaid onto yesterday's full dataset instead of replacing it."""
    con = get_db()
    if exclude_date:
        row = con.execute(
            "SELECT data FROM cache WHERE date != ? ORDER BY ts DESC LIMIT 1",
            (exclude_date,)
        ).fetchone()
    else:
        row = con.execute(
            "SELECT data FROM cache ORDER BY ts DESC LIMIT 1"
        ).fetchone()
    con.close()
    return json.loads(row[0]) if row else []

def merge_progress_into_cache(run_date: str):
    """Write everything processed so far into the live `cache` table so the
    site can serve partial/fresher data while the run is still going.

    Rather than replacing the cache with ONLY the tickers processed so far
    (which would shrink the site to e.g. 50 stocks mid-run), this overlays
    the new/updated rows on top of the most recent full cache: tickers
    re-processed today get their fresh data, tickers not yet reached this
    run keep yesterday's data. Ranks are recomputed over the merged set.
    """
    new_rows = list(load_progress(run_date).values())
    if not new_rows:
        return

    base_rows = get_most_recent_full_cache(exclude_date=run_date)

    merged: dict = {r["ticker"]: r for r in base_rows}
    for r in new_rows:
        merged[r["ticker"]] = r  # fresh data overrides stale

    rows = sorted(merged.values(), key=lambda x: x["upside_pct"], reverse=True)
    for i, r in enumerate(rows):
        r["rank"] = i + 1

    con = get_db()
    con.execute("INSERT OR REPLACE INTO cache VALUES(?,?,?)",
                (run_date, json.dumps(rows), int(time.time())))
    con.commit()
    con.close()
    print(f"  ↻  Checkpoint: merged {len(new_rows)} fresh + "
          f"{len(rows)-len(new_rows)} carried-over stocks "
          f"({len(rows)} total) into cache for {run_date}")

def clear_progress(run_date: str):
    """Remove checkpoint rows for a completed run."""
    con = get_db()
    con.execute("DELETE FROM progress WHERE run_date=?", (run_date,))
    con.commit()
    con.close()

def save_snapshot(stocks: list):
    today = datetime.date.today().isoformat()
    con = get_db()
    for s in stocks:
        try:
            con.execute("""
                INSERT OR IGNORE INTO snapshots
                (date, ticker, rank, current_price, target_price, upside_pct, consensus, analyst_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (today, s["ticker"], s["rank"], s["current_price"],
                  s["target_price"], s["upside_pct"], s["consensus"], s["analyst_count"]))
        except Exception as e:
            print(f"  ⚠  Snapshot failed for {s['ticker']}: {e}")
    con.commit()
    con.close()
    print(f"  ✓  Snapshot saved: {len(stocks)} stocks for {today}")

# ── Universe fetch ─────────────────────────────────────────────────────────────
def get_full_universe() -> list:
    JUNK_SUFFIXES   = ("W", "WS", "U", "R")
    JUNK_SUBSTRINGS = ("ETF","FUND","TRUST","REIT","NOTE","BOND",
                       "SPAC","BLANK","ACQUISITION","HOLDINGS","BLANK CHECK")
    try:
        url = "https://www.sec.gov/files/company_tickers.json"
        req = urllib.request.Request(url, headers={"User-Agent": "stockupside@example.com"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        tickers = []
        for entry in data.values():
            t    = entry.get("ticker", "").strip().upper()
            name = entry.get("title",  "").strip().upper()
            if not t or len(t) > 5 or "." in t or "-" in t:
                continue
            if any(t.endswith(sfx) for sfx in JUNK_SUFFIXES) and len(t) > 2:
                continue
            if any(kw in name for kw in JUNK_SUBSTRINGS):
                continue
            tickers.append(t)

        tickers = list(dict.fromkeys(tickers))
        print(f"  →  Universe: {len(tickers)} tickers from SEC EDGAR")
        return tickers

    except Exception as e:
        print(f"  ⚠  SEC EDGAR failed: {e} — using fallback list")
        return []

# ── Momentum helper ────────────────────────────────────────────────────────────
def get_momentum(ticker: str, current_consensus: str, current_count: int) -> dict:
    con = get_db()
    today = datetime.date.today()
    history = {}
    for days in [7, 30, 90]:
        target = (today - datetime.timedelta(days=days)).isoformat()
        row = con.execute("""
            SELECT consensus, analyst_count, date FROM snapshots
            WHERE ticker = ? AND date <= ? AND date >= ?
            ORDER BY date DESC LIMIT 1
        """, (ticker, target,
              (today - datetime.timedelta(days=days+3)).isoformat())).fetchone()
        if row:
            history[days] = {
                "consensus": row[0], "analyst_count": row[1], "date": row[2],
                "score": CONSENSUS_SCORE.get(row[0], 3),
            }
    con.close()

    current_score = CONSENSUS_SCORE.get(current_consensus, 3)
    trend = "neutral"
    trend_detail = ""
    score_delta = 0

    if 30 in history:
        past_score = history[30]["score"]
        score_delta = current_score - past_score
        past_consensus = history[30]["consensus"]
        if score_delta > 0:
            trend = "up";   trend_detail = f"{past_consensus} → {current_consensus}"
        elif score_delta < 0:
            trend = "down"; trend_detail = f"{past_consensus} → {current_consensus}"
        else:
            count_delta = current_count - history[30]["analyst_count"]
            if count_delta >= 2:
                trend = "up";   trend_detail = f"+{count_delta} new analysts"
            elif count_delta <= -2:
                trend = "down"; trend_detail = f"{count_delta} analysts dropped coverage"
            else:
                trend_detail = "unchanged"
    elif 7 in history:
        score_delta = current_score - history[7]["score"]
        trend = "up" if score_delta > 0 else "down" if score_delta < 0 else "neutral"
        trend_detail = (f"{history[7]['consensus']} → {current_consensus}"
                        if score_delta != 0 else "unchanged")

    streak = 0
    if trend == "up" and 90 in history:
        if history[90]["score"] < history[30].get("score", current_score) <= current_score:
            streak = 90
        elif 30 in history and history[30]["score"] < current_score:
            streak = 30
        elif 7 in history and history[7]["score"] < current_score:
            streak = 7

    return {"trend": trend, "trend_detail": trend_detail,
            "score_delta": score_delta, "streak_days": streak,
            "history": {str(k): v for k, v in history.items()}}

# ── Performance checker ────────────────────────────────────────────────────────
CHECKPOINTS = [30, 60, 90]

def check_performance():
    con = get_db()
    today = datetime.date.today()
    for days in CHECKPOINTS:
        target_date = (today - datetime.timedelta(days=days)).isoformat()
        rows = con.execute("""
            SELECT s.ticker, s.current_price, s.target_price
            FROM snapshots s
            LEFT JOIN performance p
                ON p.snapshot_date = s.date AND p.ticker = s.ticker AND p.days_later = ?
            WHERE s.date = ? AND p.id IS NULL
        """, (days, target_date)).fetchall()

        if not rows:
            continue

        print(f"  →  Checking {len(rows)} stocks from {target_date} ({days}d ago)...")
        for ticker, price_then, target_price in rows:
            try:
                time.sleep(1.5 + random.uniform(0, 0.5))
                info = yf.Ticker(ticker).info
                price_now = info.get("currentPrice") or info.get("regularMarketPrice") or 0
                if price_now <= 0:
                    continue
                actual_return = round((price_now / price_then - 1) * 100, 2)
                hit_target    = 1 if price_now >= target_price * 0.95 else 0
                con.execute("""
                    INSERT OR IGNORE INTO performance
                    (snapshot_date, ticker, days_later, price_then, price_now,
                     actual_return, hit_target, checked_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (target_date, ticker, days, price_then, price_now,
                      actual_return, hit_target, today.isoformat()))
                con.commit()
            except Exception as e:
                print(f"  ⚠  Performance check failed {ticker}: {e}")
        print(f"  ✓  Done checking {days}-day performance for {target_date}")
    con.close()

# ── Helpers ────────────────────────────────────────────────────────────────────
def _normalize_yield(v):
    if v is None: return None
    if v > 0.2:   return v / 100
    return v

def _fmt_cap(mc):
    if not mc: return "N/A"
    if mc >= 1e12: return f"${mc/1e12:.2f}T"
    if mc >= 1e9:  return f"${mc/1e9:.0f}B"
    return f"${mc/1e6:.0f}M"

# ── Shared rate-limit backoff state ─────────────────────────────────────────
# Several worker threads call Yahoo Finance concurrently. If ANY of them gets
# rate-limited, we want ALL workers to back off together — otherwise one
# thread sleeping 30s while two others keep hammering defeats the point.
_rate_limit_lock = threading.Lock()
_rate_limit_streak = 0
_rate_limit_pause_until = 0.0  # monotonic time; workers sleep until this passes

def _wait_for_shared_pause():
    """Block until any shared rate-limit pause (set by another worker) expires."""
    while True:
        with _rate_limit_lock:
            remaining = _rate_limit_pause_until - time.monotonic()
        if remaining <= 0:
            return
        time.sleep(min(remaining, 5))

def _register_rate_limit():
    """Called when a worker hits a rate limit. Escalates a SHARED pause that
    all worker threads will wait out, so the whole pool backs off together."""
    global _rate_limit_streak, _rate_limit_pause_until
    with _rate_limit_lock:
        _rate_limit_streak += 1
        streak = _rate_limit_streak
        wait = min(60, 10 * streak) + random.uniform(0, 5)
        if streak >= 5:
            wait = 60 + random.uniform(0, 10)
            _rate_limit_streak = 0
        _rate_limit_pause_until = max(_rate_limit_pause_until, time.monotonic() + wait)
        return wait, streak

def _register_rate_limit_ok():
    global _rate_limit_streak
    with _rate_limit_lock:
        _rate_limit_streak = 0


def fetch_ticker_row(ticker: str) -> dict | None:
    """Fetch and parse one ticker. Returns a row dict, or None if the
    ticker should be skipped (no valid analyst target, rate-limited out
    of retries, etc.). Designed to be called from a worker thread —
    rate-limit backoff is coordinated across threads via the shared
    state above.
    """
    # Small jitter even on the happy path, per-thread, so N workers don't
    # all hit Yahoo in lockstep every loop iteration.
    time.sleep(0.3 + random.uniform(0.2, 0.8))

    retries = 3
    info = None
    t_obj = None
    for attempt in range(retries):
        _wait_for_shared_pause()
        try:
            t_obj = yf.Ticker(ticker)
            info  = t_obj.info
            if info and len(info) < 10:
                raise ValueError("Stub response — likely rate limited")
            _register_rate_limit_ok()
            break
        except Exception as e:
            err = str(e)
            if "Too Many Requests" in err or "Rate limited" in err or "Stub response" in err:
                wait, streak = _register_rate_limit()
                print(f"  ⚠  Rate limited ({ticker}), shared pause {wait:.0f}s "
                      f"(streak: {streak})")
                _wait_for_shared_pause()
            else:
                print(f"  ⚠  Skipped {ticker}: {e}")
                break

    if not info or len(info) < 10 or t_obj is None:
        return None

    try:
        current_price = info.get("currentPrice") or info.get("regularMarketPrice") or 0
        target_price  = info.get("targetMeanPrice") or 0
        analyst_count = info.get("numberOfAnalystOpinions") or 0

        if current_price <= 0 or target_price <= 0 or analyst_count < 1:
            return None

        upside_pct = round((target_price / current_price - 1) * 100, 1)
        # Include downside stocks too (negative upside_pct) — excluding them
        # meant a stock that ran past its average target would silently
        # disappear from the dataset (confusing, especially for stocks on
        # a user's watchlist) instead of just showing a negative number.

        high_target = info.get("targetHighPrice") or 0
        low_target  = info.get("targetLowPrice")  or 0

        # ── Analyst rating breakdown ─────────────────────────────────────────
        sb = b = h = s = 0
        try:
            rec = t_obj.recommendations
            if rec is not None and not rec.empty:
                latest  = rec.tail(1).iloc[0]
                raw_sb  = int(latest.get("strongBuy",  0))
                raw_b   = int(latest.get("buy",        0))
                raw_h   = int(latest.get("hold",       0))
                raw_s   = int(latest.get("sell",       0)) + int(latest.get("strongSell", 0))
                raw_tot = raw_sb + raw_b + raw_h + raw_s
                if raw_tot > 0:
                    n  = analyst_count
                    sb = round(n * raw_sb / raw_tot)
                    b  = round(n * raw_b  / raw_tot)
                    h  = round(n * raw_h  / raw_tot)
                    s  = max(0, n - sb - b - h)
        except Exception:
            pass

        if sb + b + h + s == 0:
            n        = analyst_count
            rec_mean = info.get("recommendationMean") or 3.0
            if   rec_mean <= 1.5: sb = round(n*.70); b = round(n*.20); h = round(n*.08)
            elif rec_mean <= 2.0: sb = round(n*.35); b = round(n*.45); h = round(n*.15)
            elif rec_mean <= 2.5: sb = round(n*.15); b = round(n*.40); h = round(n*.35)
            elif rec_mean <= 3.0: sb = round(n*.05); b = round(n*.25); h = round(n*.55)
            else:                 sb = 0;            b = round(n*.10); h = round(n*.40)
            s = max(0, n - sb - b - h)

        consensus_map = {
            "strong_buy": "Strong Buy", "buy": "Buy", "hold": "Hold",
            "underperform": "Underperform", "sell": "Sell", "none": "Hold",
        }
        # Derive consensus from the actual sb/b/h/s vote counts so the
        # label always matches the breakdown bars shown to users. Yahoo's
        # recommendationKey can disagree with the individual counts (e.g.
        # recommendationKey="hold" while the breakdown is 1 Strong Buy +
        # 7 Buy, 0 Hold, 0 Sell — clearly not a "Hold").
        total_votes = sb + b + h + s
        if total_votes > 0:
            score = (sb * 1 + b * 2 + h * 3 + s * 4) / total_votes
            if   score <= 1.5: consensus = "Strong Buy"
            elif score <= 2.5: consensus = "Buy"
            elif score <= 3.2: consensus = "Hold"
            else:              consensus = "Underperform"
        else:
            consensus = consensus_map.get(
                (info.get("recommendationKey") or "none").lower(), "Hold")

        momentum = get_momentum(ticker, consensus, analyst_count)

        ytd_change = 0.0
        try:
            hist = t_obj.history(period="ytd")
            if not hist.empty and hist["Close"].iloc[0] > 0:
                ytd_change = round(
                    (hist["Close"].iloc[-1] / hist["Close"].iloc[0] - 1) * 100, 1)
        except Exception:
            pass

        return dict(
            ticker=ticker,
            name=info.get("longName") or info.get("shortName") or ticker,
            sector=info.get("sector") or "Unknown",
            current_price=round(current_price, 2),
            target_price=round(target_price, 2),
            upside_pct=upside_pct,
            high_target=round(high_target, 2),
            low_target=round(low_target, 2),
            analyst_count=analyst_count,
            consensus=consensus,
            strong_buy=sb, buy=b, hold=h, sell=s,
            market_cap=_fmt_cap(info.get("marketCap") or 0),
            market_cap_raw=info.get("marketCap") or 0,
            pe_ratio=round(info.get("trailingPE") or 0, 1),
            ytd_change=ytd_change,
            week52_low=round(info.get("fiftyTwoWeekLow")  or 0, 2),
            week52_high=round(info.get("fiftyTwoWeekHigh") or 0, 2),
            avg_volume=info.get("averageVolume") or 0,
            last_updated=datetime.date.today().isoformat(),
            eps=info.get("trailingEps"),
            forward_pe=round(info.get("forwardPE") or 0, 1),
            peg_ratio=round(info.get("pegRatio") or 0, 2),
            dividend_yield=_normalize_yield(info.get("dividendYield")),
            revenue=info.get("totalRevenue"),
            profit_margin=info.get("profitMargins"),
            momentum_trend=momentum["trend"],
            momentum_detail=momentum["trend_detail"],
            momentum_streak=momentum["streak_days"],
            momentum_history=momentum["history"],
        )
    except Exception as e:
        print(f"  ⚠  Skipped {ticker} (parse error): {e}")
        return None

# ── Main generation ──────────────────────────────────────────────
def generate_stocks(run_date: str) -> list:
    # For production: use get_full_universe() (full SEC EDGAR list).
    # For development: use get_full_universe()[:500] or UNIVERSE_FALLBACK.
    tickers = get_full_universe()
    if not tickers:
        print("  ⚠  SEC EDGAR fetch failed — using hardcoded fallback list")
        tickers = UNIVERSE_FALLBACK

    # ── Uncomment ONE of the lines below to control scope ──
    # tickers = tickers[:2000]
    # tickers = tickers[:500]   # dev: ~15-30 min
    # tickers = tickers[:100]   # dev: ~3-5 min
    # (leave commented for full production run)

    # ── Resume support ──────────────────────────────────────────
    # If a previous run today was killed partway through (timeout, crash,
    # restart), `progress` already has results for some tickers. Skip those
    # and only fetch the remaining ones.
    done = load_progress(run_date)
    if done:
        print(f"  ↻  Resuming: {len(done)} tickers already completed today, "
              f"{len(tickers) - len(done)} remaining")
        remaining_tickers = [t for t in tickers if t not in done]
    else:
        remaining_tickers = tickers

    # ── Conservative concurrency ────────────────────────────────────────
    # yfinance/requests are synchronous, but threads still give real
    # concurrency for network-bound I/O (the GIL is released while waiting
    # on sockets). 3 workers is a conservative ~2-3x speedup over fully
    # sequential, with shared rate-limit backoff (see _wait_for_shared_pause
    # / _register_rate_limit) so if Yahoo starts throttling, ALL workers
    # back off together rather than one thread pausing while others keep
    # hammering. If you see sustained rate-limiting in generate.log, drop
    # MAX_WORKERS to 2 or 1.
    MAX_WORKERS = 4

    print(f"  →  Fetching analyst targets for {len(remaining_tickers)} tickers "
          f"({len(tickers)} total) with {MAX_WORKERS} concurrent workers...")
    rows = list(done.values())
    total = len(remaining_tickers)
    CHECKPOINT_EVERY = 50  # merge into live cache every N newly-processed tickers
    processed_since_checkpoint = 0
    completed = 0
    progress_lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_ticker = {
            executor.submit(fetch_ticker_row, t): t for t in remaining_tickers
        }
        for future in as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            try:
                row = future.result()
            except Exception as e:
                print(f"  ⚠  Worker error on {ticker}: {e}")
                row = None

            with progress_lock:
                completed += 1
                if completed % 25 == 0 or completed == total:
                    print(f"  →  Progress: {completed}/{total} remaining "
                          f"({len(done) + completed} of {len(tickers)} total, "
                          f"{len(rows)} valid so far)")

                if row is not None:
                    rows.append(row)
                    # Checkpoint immediately — this single ticker's result
                    # survives even if the process is killed right after.
                    save_progress_row(run_date, ticker, row)
                    processed_since_checkpoint += 1
                    if processed_since_checkpoint >= CHECKPOINT_EVERY:
                        merge_progress_into_cache(run_date)
                        processed_since_checkpoint = 0

    rows.sort(key=lambda x: x["upside_pct"], reverse=True)
    for i, r in enumerate(rows):
        r["rank"] = i + 1

    print(f"  ✓  Done. {len(rows)} stocks with valid analyst targets.")
    return rows

# ── Entry point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    start_time = time.time()
    print(f"\n  ▲  StockUpside.io — Data Generator")
    print(f"  →  Started at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  →  DB path: {DB_PATH}\n")

    init_db()

    # Determine which "run" we're continuing. If there's unfinished progress
    # from a prior run within the last 20 hours, resume that run_date instead
    # of starting a new one for today — this handles a run that started just
    # before midnight and got killed shortly after.
    today = datetime.date.today().isoformat()
    con = get_db()
    existing = con.execute(
        "SELECT run_date, MAX(ts) FROM progress GROUP BY run_date ORDER BY MAX(ts) DESC LIMIT 1"
    ).fetchone()
    con.close()

    run_date = today
    if existing and existing[0]:
        prev_run_date, last_ts = existing
        age_hours = (time.time() - (last_ts or 0)) / 3600
        if age_hours < 20:
            run_date = prev_run_date
            print(f"  ↻  Found unfinished run from {run_date} "
                  f"(last checkpoint {age_hours:.1f}h ago) — resuming it.")
        else:
            print(f"  →  Stale progress from {prev_run_date} ({age_hours:.1f}h old) "
                  f"— ignoring, starting fresh run for {today}.")

    # SIGTERM (e.g. systemd stop, timeout subprocess.terminate(), service
    # restart) doesn't raise KeyboardInterrupt — without this handler the
    # process would die mid-ticker with nothing beyond the last periodic
    # checkpoint. Translate it into a clean, immediate partial merge + exit.
    def _on_sigterm(signum, frame):
        print(f"\n  ✗  Received signal {signum} — checkpointing before exit.")
        merge_progress_into_cache(run_date)
        print(f"  ↻  Partial progress saved — re-run to resume from where this left off.")
        sys.exit(1)

    signal.signal(signal.SIGTERM, _on_sigterm)

    try:
        data = generate_stocks(run_date)
        if not data:
            print("  ✗  No stocks generated — aborting cache write.")
            sys.exit(1)

        # Final write under TODAY's date (in case run_date was yesterday),
        # then clean up the checkpoint table for the completed run.
        save_cache(data)
        if run_date != today:
            # Also remove the stale run_date row we wrote via checkpoints
            con = get_db()
            con.execute("DELETE FROM cache WHERE date=?", (run_date,))
            con.commit()
            con.close()
        clear_progress(run_date)

        save_snapshot(data)
        check_performance()

        elapsed = time.time() - start_time
        print(f"\n  ✓  All done in {elapsed/60:.1f} min. "
              f"{len(data)} stocks written to cache.\n")
        sys.exit(0)

    except KeyboardInterrupt:
        print("\n  ✗  Interrupted by user.")
        merge_progress_into_cache(run_date)
        print(f"  ↻  Partial progress saved — re-run to resume from where this left off.")
        sys.exit(1)
    except Exception as e:
        print(f"\n  ✗  Fatal error: {e}")
        merge_progress_into_cache(run_date)
        print(f"  ↻  Partial progress saved — re-run to resume from where this left off.")
        sys.exit(1)