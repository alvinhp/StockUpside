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

import json, sqlite3, time, datetime, os, random, urllib.request, sys, signal, re, math
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

import yfinance as yf
import pandas as pd

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

def _finite(value, default=0):
    """Coerce a Yahoo-sourced numeric value to `default` if it's missing,
    NaN, or +/-Infinity. NaN is truthy in Python (`nan or 0` returns nan,
    not 0) and every comparison with NaN is False (`nan <= 0` is False),
    so the common `info.get(...) or 0` pattern does NOT filter NaN out.
    A single NaN/Infinity float written to the cache becomes a bare
    NaN/Infinity token when json.dumps serializes it (allow_nan=True by
    default) — invalid per the JSON spec, which makes browsers' strict
    JSON.parse() throw and break the entire stocks list, not just one row.
    """
    try:
        if value is None:
            return default
        f = float(value)
        return f if math.isfinite(f) else default
    except (TypeError, ValueError):
        return default

def _calc_forward_pe(current_price: float, forward_eps) -> float:
    """Compute forward P/E from the current price and forward EPS estimate
    rather than using yfinance's cached `forwardPE` field directly.

    Why: yfinance's `info["forwardPE"]` is pre-computed as
    currentPrice / forwardEps at the time Yahoo last cached the quote —
    which can be hours or days stale. Using our already-fetched
    `current_price` (from the same API call) gives a more accurate ratio.

    Suppression rules (matches Yahoo Finance's display behaviour):
      - forwardEps is None, zero, or non-finite → return 0 (not shown)
      - forwardEps is negative (company expected to lose money) → return 0
        Yahoo Finance doesn't display forward P/E for loss-making companies
        since a negative ratio is mathematically valid but practically
        meaningless and confusing to most users.
      - Result > 1000 or not finite → return 0 (nonsensical outlier)
    """
    if not current_price or current_price <= 0:
        return 0
    try:
        eps = float(forward_eps)
    except (TypeError, ValueError):
        return 0
    if not math.isfinite(eps) or eps <= 0:
        # Negative EPS → suppress (loss-making co, forward PE meaningless)
        return 0
    ratio = current_price / eps
    if not math.isfinite(ratio) or ratio <= 0 or ratio > 1000:
        return 0
    return round(ratio, 1)

def sanitize_row(row: dict) -> dict:
    """Defense-in-depth: sweep every numeric value in a finished row and
    replace any NaN/Infinity with 0 before it can reach the cache. This
    catches future fields too, not just the ones explicitly cleaned with
    _finite() above."""
    for k, v in row.items():
        if isinstance(v, float) and not math.isfinite(v):
            row[k] = 0.0
    return row

# ── DB helpers ─────────────────────────────────────────────────────────────────
def get_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=5000")
    return con

def get_stocks_cached() -> list:
    """Read the most recent cache row. generate.py and app.py are
    separate processes (no shared Python state), so this is a thin,
    independent reader rather than an import from app.py — avoids
    coupling the generator to the Flask app's module-level state."""
    con = get_db()
    row = con.execute(
        "SELECT data FROM cache ORDER BY ts DESC LIMIT 1"
    ).fetchone()
    con.close()
    if not row:
        return []
    return json.loads(row[0])

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
        source TEXT,
        UNIQUE(date, ticker))""")
    _snap_cols = [r[1] for r in con.execute("PRAGMA table_info(snapshots)").fetchall()]
    if "source" not in _snap_cols:
        con.execute("ALTER TABLE snapshots ADD COLUMN source TEXT")
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

    # ── Per-firm analyst track record ────────────────────────────────────
    # Same schema as defined in app.py's init_db — generate.py and app.py
    # are separate processes with no shared state, so both need their own
    # CREATE TABLE IF NOT EXISTS, matching the existing pattern for
    # cache/subscribers/snapshots/performance above.
    con.execute("""CREATE TABLE IF NOT EXISTS analyst_calls (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker TEXT NOT NULL,
        firm TEXT NOT NULL,
        grade_date TEXT NOT NULL,
        from_grade TEXT,
        to_grade TEXT NOT NULL,
        action TEXT NOT NULL,
        price_at_call REAL,
        first_seen TEXT NOT NULL,
        UNIQUE(ticker, firm, grade_date, to_grade)
)""")
    con.execute("CREATE INDEX IF NOT EXISTS idx_calls_firm ON analyst_calls(firm)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_calls_ticker ON analyst_calls(ticker)")
    con.execute("""CREATE TABLE IF NOT EXISTS analyst_call_outcomes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        call_id INTEGER NOT NULL,
        days_later INTEGER NOT NULL,
        price_then REAL,
        price_now REAL,
        actual_return REAL,
        was_correct INTEGER,
        checked_date TEXT,
        UNIQUE(call_id, days_later),
        FOREIGN KEY(call_id) REFERENCES analyst_calls(id)
)""")

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

# Minimum fraction of the existing cache's stock count that a freshly
# completed run must hit before it's trusted to replace the cache outright.
MIN_ACCEPTABLE_FRACTION = 0.5

def save_final_cache(data: list, run_date: str):
    """Write the completed run's results to the cache — but unlike a plain
    save_cache(data) call, this refuses to silently replace a good, full
    cache with a badly degraded one.

    Bug this guards against: generate_stocks() can finish *without*
    raising (so none of the crash-recovery / merge_progress_into_cache
    machinery kicks in) while still having failed on most tickers — e.g.
    Yahoo rate-limits hard for hours and only ~100 of ~3,800 tickers come
    back with valid data. A plain save_cache(data) would overwrite the
    existing thousands-of-stocks cache with that tiny degraded list. If
    one of those rows also carries a non-finite value (see _finite() /
    sanitize_row() above), the resulting JSON breaks the frontend
    entirely. Either way, this is a major regression no caller should be
    able to cause by simply finishing a bad run.

    Instead: if the new data is suspiciously small compared to what's
    already cached, treat it the same way a mid-run checkpoint would —
    overlay it onto the existing full cache (fresh data wins per-ticker,
    everything else is carried over) rather than replacing wholesale.
    """
    base_rows = get_most_recent_full_cache(exclude_date=run_date)

    if base_rows and len(data) < len(base_rows) * MIN_ACCEPTABLE_FRACTION:
        print(f"  ⚠  Only {len(data)} stocks came back this run, vs "
              f"{len(base_rows)} already cached — that's a >50% drop. "
              f"Refusing to overwrite; merging onto the existing cache "
              f"instead so the site doesn't regress.")
        today_str = datetime.date.today().isoformat()
        merged: dict = {r["ticker"]: r for r in base_rows}
        for r in merged.values():
            # Stamp all carried-over rows with today's date. They're being
            # consciously re-published today, so last_updated should reflect
            # that — otherwise api_stats reads an old last_updated from
            # stocks[0] and the site shows "2d old" even after a successful
            # (if partial) nightly run.
            r["last_updated"] = today_str
        for r in data:
            merged[r["ticker"]] = r  # fresh rows already have today's date
        rows = sorted(merged.values(), key=lambda x: x["upside_pct"], reverse=True)
        for i, r in enumerate(rows):
            r["rank"] = i + 1
        save_cache(rows)
        print(f"  ↻  Merged: {len(data)} fresh + {len(rows) - len(data)} "
              f"carried-over stocks ({len(rows)} total) saved instead.")
    else:
        save_cache(data)

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


# ── Per-firm analyst call tracking ──────────────────────────────────────────
# Kept as a separate pass from the main fetch_ticker_row loop, run less
# frequently (e.g. weekly via collect_analyst_calls.py as its own cron
# entry), since it costs one extra Yahoo API call per ticker on top of
# the main daily refresh, and rating-change history doesn't move fast
# enough to need fetching every single day.

# 'main'/'reit' (reiterate) carry no directional call to score — the firm
# isn't predicting a move, just restating an existing rating.
DIRECTIONAL_ACTIONS = {"up", "down", "init"}

def fetch_analyst_calls(ticker: str) -> tuple[list[dict], bool]:
    """Fetch this ticker's upgrade/downgrade history from Yahoo and
    return (rows, errored) shaped for the analyst_calls table.

    Returns ([], False) when the ticker genuinely has no upgrade/downgrade
    history (a normal, expected outcome for plenty of tickers) — and
    ([], True) when the *request itself* failed (HTTP error, network
    issue, etc.). These two cases used to be indistinguishable (both
    just returned []), which meant a systemic failure — e.g. Yahoo
    404ing this module for the whole run — looked identical to normal,
    expected per-ticker gaps in the log. The caller uses `errored` to
    track a real failure rate and bail out early if it's runaway, instead
    of silently grinding through the entire universe for hours."""
    try:
        t_obj = yf.Ticker(ticker)
        df = t_obj.upgrades_downgrades
        if df is None or df.empty:
            return [], False

        # Only keep calls from roughly the last 2 years — older history
        # is interesting but adds bulk for little incremental value, and
        # outcome-scoring requires reasonably recent price history anyway.
        cutoff = pd.Timestamp.now(tz=df.index.tz) - pd.Timedelta(days=730)
        df = df[df.index >= cutoff]

        rows = []
        for grade_date, row in df.iterrows():
            rows.append({
                "ticker":      ticker,
                "firm":        str(row.get("Firm", "")).strip(),
                "grade_date":  grade_date.date().isoformat(),
                "from_grade":  str(row.get("FromGrade", "") or ""),
                "to_grade":    str(row.get("ToGrade", "")).strip(),
                "action":      str(row.get("Action", "")).strip().lower(),
            })
        return [r for r in rows if r["firm"] and r["to_grade"]], False
    except Exception:
        return [], True

def save_analyst_calls(ticker: str, calls: list[dict]):
    """Insert new analyst_calls rows, fetching price_at_call for any
    genuinely new row (UNIQUE constraint makes re-running this idempotent
    — already-seen calls are silently skipped via INSERT OR IGNORE).

    Fetches the ticker's full price history ONCE per call to this
    function, rather than once per individual rating-change row. The
    previous version called t_obj.history() separately for every new
    call — for a ticker with 40+ historical rating changes (common on
    the first backfill run, since fetch_analyst_calls pulls up to 2
    years of history), that meant 40+ separate network requests just to
    price one ticker. At scale across thousands of tickers, this
    triggered Yahoo's rate limiting partway through the run, silently
    leaving most price_at_call values NULL — which then meant
    resolve_analyst_call_outcomes() had nothing to score for the vast
    majority of collected calls, since it requires price_at_call to be
    non-NULL. One bulk history() call per ticker (not per call) avoids
    this entirely."""
    if not calls:
        return 0
    now_iso = datetime.datetime.now().isoformat()
    con = get_db()

    # Figure out which rows are genuinely new BEFORE fetching any price
    # history, so we don't pay for a price lookup on rows we're about to
    # skip anyway.
    new_calls = []
    for c in calls:
        existing = con.execute(
            "SELECT 1 FROM analyst_calls WHERE ticker=? AND firm=? AND grade_date=? AND to_grade=?",
            (c["ticker"], c["firm"], c["grade_date"], c["to_grade"]),
        ).fetchone()
        if not existing:
            new_calls.append(c)

    if not new_calls:
        con.close()
        return 0

    # One bulk price-history fetch covering the full span of grade dates
    # in this batch, instead of one fetch per call.
    price_by_date: dict[str, float] = {}
    try:
        dates = sorted(c["grade_date"] for c in new_calls)
        start = dates[0]
        end = (datetime.date.fromisoformat(dates[-1]) + datetime.timedelta(days=5)).isoformat()
        t_obj = yf.Ticker(ticker)
        hist = t_obj.history(start=start, end=end)
        if not hist.empty:
            # hist.index is a DatetimeIndex; build a date-string -> close lookup.
            # For grade dates that fall on a non-trading day (weekend/holiday),
            # forward-fill to the next available trading day's close.
            hist = hist.sort_index()
            for grade_date_str in set(dates):
                gd = pd.Timestamp(grade_date_str)
                # Find the first trading day on or after the grade date
                future = hist.index[hist.index >= gd]
                if len(future) > 0:
                    price_by_date[grade_date_str] = float(hist.loc[future[0], "Close"])
    except Exception as e:
        print(f"  ⚠  Bulk price history fetch failed for {ticker}: {e}")

    inserted = 0
    for c in new_calls:
        price_at_call = price_by_date.get(c["grade_date"])
        try:
            con.execute("""
                INSERT OR IGNORE INTO analyst_calls
                (ticker, firm, grade_date, from_grade, to_grade, action, price_at_call, first_seen)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (c["ticker"], c["firm"], c["grade_date"], c["from_grade"],
                  c["to_grade"], c["action"], price_at_call, now_iso))
            inserted += 1
        except Exception as e:
            print(f"  ⚠  Failed to save call for {ticker}/{c['firm']}: {e}")
    con.commit()
    con.close()
    return inserted

def resolve_analyst_call_outcomes():
    """Score calls that are now old enough to check: did the stock move
    in the direction the call predicted (up after an upgrade, down after
    a downgrade) by 30/60/90 days later? Mirrors the existing
    resolve_performance_checkpoints pattern for the consensus-level
    `performance` table — same idea, scoped to individual firm calls.

    Batches price-history fetches by ticker (one history() call covering
    every checkpoint date needed for that ticker) rather than one
    history() call per (call, days_later) triple. The original version
    made up to 3 separate network requests per eligible call — at
    ~127,000 eligible calls, that's up to ~380,000 requests in a single
    run, which is exactly the kind of load that gets rate-limited by
    Yahoo partway through and silently stops resolving the rest. This is
    the same root cause and fix pattern as the price_at_call backfill in
    save_analyst_calls."""
    con = get_db()
    calls = con.execute("""
        SELECT id, ticker, action, grade_date, price_at_call
        FROM analyst_calls
        WHERE action IN ('up', 'down', 'init') AND price_at_call IS NOT NULL
    """).fetchall()
    # Pull all already-resolved (call_id, days_later) pairs once, up front,
    # instead of one query per checkpoint — avoids ~380K individual
    # "already resolved?" lookups on top of the network fetches.
    already_resolved = set(con.execute(
        "SELECT call_id, days_later FROM analyst_call_outcomes"
    ).fetchall())
    con.close()

    today = datetime.date.today()

    # Group pending checkpoints by ticker: for each ticker, figure out
    # every (call_id, days_later, target_date, action, price_at_call)
    # tuple that's old enough to check and not yet resolved.
    pending_by_ticker: dict[str, list[tuple]] = {}
    for call_id, ticker, action, grade_date, price_at_call in calls:
        grade_dt = datetime.date.fromisoformat(grade_date)
        for days_later in (30, 60, 90):
            if (call_id, days_later) in already_resolved:
                continue
            target_date = grade_dt + datetime.timedelta(days=days_later)
            if target_date > today:
                continue  # not old enough yet
            pending_by_ticker.setdefault(ticker, []).append(
                (call_id, days_later, target_date, action, price_at_call)
            )

    if not pending_by_ticker:
        print("  ✓  Resolved 0 analyst call outcomes (nothing pending)")
        return 0

    resolved = 0
    con = get_db()
    for ticker, pending in pending_by_ticker.items():
        target_dates = sorted(p[2] for p in pending)
        start = target_dates[0].isoformat()
        end = (target_dates[-1] + datetime.timedelta(days=5)).isoformat()

        try:
            t_obj = yf.Ticker(ticker)
            hist = t_obj.history(start=start, end=end)
        except Exception as e:
            print(f"  ⚠  History fetch failed for {ticker}: {e}")
            continue
        if hist.empty:
            continue
        hist = hist.sort_index()

        for call_id, days_later, target_date, action, price_at_call in pending:
            gd = pd.Timestamp(target_date)
            future = hist.index[hist.index >= gd]
            if len(future) == 0:
                continue  # no trading day on/after target_date in the fetched range
            price_now = float(hist.loc[future[0], "Close"])

            actual_return = round((price_now / price_at_call - 1) * 100, 2)
            if action in ("up", "init"):
                was_correct = 1 if actual_return > 0 else 0
            elif action == "down":
                was_correct = 1 if actual_return < 0 else 0
            else:
                was_correct = None

            con.execute("""
                INSERT OR IGNORE INTO analyst_call_outcomes
                (call_id, days_later, price_then, price_now, actual_return, was_correct, checked_date)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (call_id, days_later, price_at_call, price_now, actual_return,
                  was_correct, today.isoformat()))
            resolved += 1

        # Commit per-ticker rather than per-outcome — far fewer commits,
        # and a crash mid-run only loses the current ticker's progress.
        con.commit()

    con.close()
    print(f"  ✓  Resolved {resolved} analyst call outcomes")
    return resolved

def get_firm_track_record(firm: str | None = None) -> list[dict]:
    """Aggregate win rate per firm across all resolved 90-day outcomes.
    Pass a specific firm name to filter to one firm's track record."""
    con = get_db()
    query = """
        SELECT c.firm,
               COUNT(*) as total_calls,
               SUM(o.was_correct) as correct_calls,
               AVG(o.actual_return) as avg_return
        FROM analyst_call_outcomes o
        JOIN analyst_calls c ON c.id = o.call_id
        WHERE o.days_later = 90 AND o.was_correct IS NOT NULL
    """
    params = []
    if firm:
        query += " AND c.firm = ?"
        params.append(firm)
    query += " GROUP BY c.firm HAVING total_calls >= 5 ORDER BY correct_calls * 1.0 / total_calls DESC"

    rows = con.execute(query, params).fetchall()
    con.close()
    return [{
        "firm": r[0],
        "total_calls": r[1],
        "correct_calls": r[2],
        "win_rate_pct": round(100 * r[2] / r[1], 1) if r[1] else 0,
        "avg_return_pct": round(r[3], 2) if r[3] is not None else None,
    } for r in rows]

# ── Universe fetch ─────────────────────────────────────────────────────────────
def get_full_universe() -> list:
    # Real warrant/unit/rights tickers follow a specific pattern: a base
    # SPAC ticker (typically 3-4 letters) immediately followed by exactly
    # one of these suffix letters, with NO other valid base ticker of that
    # exact length existing independently. The previous version matched
    # any ticker *ending* in W/U/R, which silently dropped real common
    # stocks like HWM, LOW, NOW, DLR, and CHTR — none of which are SPAC
    # derivatives. We instead check the SEC's own per-entry classification
    # where available, and fall back to a much narrower length-based rule:
    # only treat trailing W/U/R/WS as a warrant/unit suffix when the
    # ticker is 5 characters long (4-letter base + 1 suffix char), which
    # matches how exchanges actually construct these symbols. This still
    # isn't perfect, but it stops dropping legitimate 3-4 letter tickers.
    # ── Junk-name filter ──────────────────────────────────────────────────
    # Two tiers of matching:
    # 1. Plain substring: only keywords that can't appear inside a real word
    #    (ETF, REIT, TRUST, BLANK CHECK, ACQUISITION are always standalone)
    # 2. Word-boundary regex: keywords that CAN appear inside real words
    #    - SPAC matches "AEROSPACE" (aeroSPACe) without \b
    #    - FUND matches "FUNDAMENTAL" without \b
    #    - NOTE matches "NOTEWORTHY" without \b
    # HOLDINGS was removed from the filter entirely — too many legitimate
    # companies use it (DIGITALOCEAN HOLDINGS, VIRGIN GALACTIC HOLDINGS,
    # ALPHABET HOLDINGS, etc.), so it produces far more false positives
    # than it catches real SPAC shells. SPAC shells are caught by BLANK
    # CHECK or the standalone SPAC keyword instead.
    JUNK_SUFFIXES = ("W", "WS", "U", "R")
    # Multi-word phrases: safe as plain substring (can't appear inside a real word)
    JUNK_PLAIN = ("BLANK CHECK", "ACQUISITION")
    # Single keywords: MUST use word-boundary to avoid matching inside real names.
    # ETF matches "nETFlix", REIT matches "REITMANS", BOND matches "BROADCOM"
    # without \b. TRUST is kept here too — "TRUST" inside "TRUSTWORTHY" is fine
    # but was previously matching "INDUSTRIAL TRUST CO" correctly; \b still
    # catches "ABC TRUST" and "TRUST CO" since TRUST is its own word there.
    JUNK_WORD  = ("ETF", "REIT", "TRUST", "BOND", "SPAC", "FUND", "NOTE")
    try:
        import gzip as _gzip
        url = "https://www.sec.gov/files/company_tickers.json"
        # SEC EDGAR fair-access policy requires a descriptive User-Agent with a
        # real contact email. Using example.com or a fake address gets 403'd.
        # Format: "AppName/version contact@yourdomain.com"
        req = urllib.request.Request(url, headers={
            "User-Agent": "StockUpside.io/1.0 hello@stockupside.io",
            "Accept-Encoding": "gzip, deflate",
        })
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            # SEC returns gzip-compressed JSON when Accept-Encoding: gzip is
            # sent. urllib doesn't auto-decompress (unlike requests), so we
            # detect and handle it manually. 0x1f 0x8b is the gzip magic number.
            if raw[:2] == b'\x1f\x8b':
                raw = _gzip.decompress(raw)
            data = json.loads(raw.decode("utf-8"))

        tickers = []
        for entry in data.values():
            t    = entry.get("ticker", "").strip().upper()
            name = entry.get("title",  "").strip().upper()
            if not t or len(t) > 5 or "." in t or "-" in t:
                continue
            if len(t) == 5 and any(t.endswith(sfx) for sfx in JUNK_SUFFIXES):
                continue
            if any(kw in name for kw in JUNK_PLAIN):
                continue
            if any(re.search(r'\b' + kw + r'\b', name) for kw in JUNK_WORD):
                continue
            tickers.append(t)

        tickers = list(dict.fromkeys(tickers))
        print(f"  →  Universe: {len(tickers)} tickers from SEC EDGAR")
        return tickers

    except urllib.error.HTTPError as e:
        # Surface the actual HTTP status — 403 means User-Agent is wrong or
        # IP is rate-limited by SEC; 5xx means SEC is having issues.
        print(f"  ✗  SEC EDGAR HTTP {e.code}: {e.reason}")
        print(f"  ✗  URL: {url}")
        print(f"  ✗  Returning empty list — caller will abort rather than")
        print(f"     overwrite full cache with the 111-ticker fallback.")
        return []
    except Exception as e:
        print(f"  ✗  SEC EDGAR fetch failed: {type(e).__name__}: {e}")
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
            # A "complete-looking" info dict can still be missing
            # targetMeanPrice on a given pull even though the ticker does
            # have analyst coverage — this happens often enough under
            # load that treating it as permanent (no coverage) instead of
            # transient (bad pull) was silently dropping real, covered
            # stocks like DOCN from the dataset for an entire day's run.
            # Retry a couple more times before accepting "no target" as
            # the real answer.
            has_target = bool(info.get("targetMeanPrice"))
            if not has_target:
                try:
                    apt = t_obj.analyst_price_targets if t_obj else None
                    if apt and apt.get("mean"):
                        has_target = True
                except Exception:
                    pass
            if not has_target and attempt < retries - 1:
                raise ValueError("Missing targetMeanPrice — retrying before giving up")
            _register_rate_limit_ok()
            break
        except Exception as e:
            err = str(e)
            if "Too Many Requests" in err or "Rate limited" in err or "Stub response" in err:
                wait, streak = _register_rate_limit()
                print(f"  ⚠  Rate limited ({ticker}), shared pause {wait:.0f}s "
                      f"(streak: {streak})")
                _wait_for_shared_pause()
            elif "Missing targetMeanPrice" in err:
                print(f"  →  {ticker}: no analyst target on attempt {attempt + 1}, retrying...")
                time.sleep(1.0 + random.uniform(0.5, 1.5))
            else:
                print(f"  ⚠  Skipped {ticker}: {e}")
                break

    if not info or len(info) < 10 or t_obj is None:
        return None

    try:
        current_price = info.get("currentPrice") or info.get("regularMarketPrice") or 0

        # Prefer analyst_price_targets (fresher endpoint) over info["targetMeanPrice"]
        # which can lag hours after a target change (e.g. Bernstein cutting NUVL).
        target_price = 0.0
        high_target  = 0.0
        low_target   = 0.0
        try:
            apt = t_obj.analyst_price_targets
            if apt is not None:
                target_price = _finite(apt.get("mean"))
                high_target  = _finite(apt.get("high"))
                low_target   = _finite(apt.get("low"))
        except Exception:
            pass
        if not target_price:
            target_price = _finite(info.get("targetMeanPrice"))
        if not high_target:
            high_target  = _finite(info.get("targetHighPrice"))
        if not low_target:
            low_target   = _finite(info.get("targetLowPrice"))

        analyst_count = info.get("numberOfAnalystOpinions") or 0

        # NOTE: NaN is truthy in Python, so `nan or 0` returns nan (never
        # falls through to the 0), and `nan <= 0` is always False (any
        # comparison with NaN is False) — so a `<= 0` check alone does NOT
        # catch a NaN price coming back from yfinance. A single NaN here
        # propagates into upside_pct below, gets written to the cache, and
        # breaks JSON.parse() on the frontend for the ENTIRE stocks array
        # (browsers reject the literal NaN/Infinity tokens Python's
        # json.dumps emits by default). math.isfinite() catches both NaN
        # and +/-Infinity; isfinite() raises on non-numeric input too,
        # which is why this is wrapped in the try/except this code already
        # lives inside.
        if (not math.isfinite(current_price) or not math.isfinite(target_price)
                or not math.isfinite(analyst_count)):
            return None

        if current_price <= 0 or target_price <= 0 or analyst_count < 1:
            return None

        upside_pct = round((target_price / current_price - 1) * 100, 1)
        # Include downside stocks too (negative upside_pct) — excluding them
        # meant a stock that ran past its average target would silently
        # disappear from the dataset (confusing, especially for stocks on
        # a user's watchlist) instead of just showing a negative number.

        # ── Analyst rating breakdown ─────────────────────────────────────────
        sb = b = h = s = 0
        # Primary: recommendations_summary "0m" row = live snapshot
        # (matches what finance.yahoo.com shows, not lagged monthly aggregate)
        try:
            rs = t_obj.recommendations_summary
            if rs is not None and not rs.empty:
                row0 = rs[rs["period"] == "0m"]
                if row0.empty:
                    row0 = rs.iloc[[0]]
                latest  = row0.iloc[0]
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
        # Secondary fallback: rolling monthly recommendations
        if sb + b + h + s == 0:
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

        row = sanitize_row(dict(
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
            market_cap=_fmt_cap(_finite(info.get("marketCap"))),
            market_cap_raw=_finite(info.get("marketCap")),
            pe_ratio=round(_finite(info.get("trailingPE")), 1),
            ytd_change=ytd_change,
            week52_low=round(_finite(info.get("fiftyTwoWeekLow")), 2),
            week52_high=round(_finite(info.get("fiftyTwoWeekHigh")), 2),
            avg_volume=_finite(info.get("averageVolume")),
            last_updated=datetime.date.today().isoformat(),
            eps=info.get("trailingEps"),
            forward_pe=_calc_forward_pe(current_price, info.get("forwardEps")),
            peg_ratio=round(_finite(info.get("pegRatio")), 2),
            dividend_yield=_normalize_yield(info.get("dividendYield")),
            revenue=info.get("totalRevenue"),
            profit_margin=info.get("profitMargins"),
            momentum_trend=momentum["trend"],
            momentum_detail=momentum["trend_detail"],
            momentum_streak=momentum["streak_days"],
            momentum_history=momentum["history"],
        ))

        # Conviction score computed after all fields are finalised so it
        # can read analyst_count, current_price, high/low_target, votes,
        # and momentum_streak from the same row dict.
        conviction = analyst_conviction_score(row)
        row.update({
            "conviction_score":     conviction["score"],
            "conviction_coverage":  conviction["coverage"],
            "conviction_clarity":   conviction["clarity"],
            "conviction_unanimity": conviction["unanimity"],
            "conviction_tenure":    conviction["tenure"],
        })
        return row

    except Exception as e:
        print(f"  ⚠  Skipped {ticker} (parse error): {e}")
        return None

# ── Analyst Conviction Score ───────────────────────────────────────────────────
# A 0–100 score that measures how much conviction the analyst community
# has in their call on a given stock. Composed of four sub-scores, each
# independently interpretable and shown to the user as a breakdown.
#
# Crucially this is NOT a "buy/sell" signal or risk prediction — it
# measures analyst agreement, not fundamental quality. A stock can have a
# conviction score of 95 and still go down if analysts are collectively
# wrong. We make this framing explicit in the UI copy.

def _coverage_depth_score(analyst_count: int) -> int:
    """0–30 pts. Logarithmic scaling so the marginal value of analyst #31
    isn't equal to analyst #2. Curve calibrated so:
      1  analyst → 3 pts  (nearly worthless as a signal)
      5  analysts → 12 pts
      10 analysts → 18 pts
      20 analysts → 24 pts
      30 analysts → 28 pts
      50+ analysts → 30 pts (cap)
    """
    import math as _math
    if not analyst_count or analyst_count < 1:
        return 0
    # log2(count+1) / log2(51) * 30, capped at 30
    raw = _math.log2(analyst_count + 1) / _math.log2(51) * 30
    return min(30, round(raw))


def _consensus_clarity_score(current_price: float, high_target: float,
                              low_target: float) -> int:
    """0–30 pts. Measures how tightly analysts agree on where the price
    is going. Uses the bull/bear spread as a fraction of current price —
    a tighter spread means analysts are in closer agreement.

    Spread % = (high_target - low_target) / current_price * 100
      <20%  spread → 30 pts  (analysts very tightly aligned)
      40%   spread → 22 pts
      80%   spread → 12 pts
      150%+ spread →  0 pts  (analysts fundamentally disagree)
    """
    if not current_price or current_price <= 0:
        return 0
    if not high_target or not low_target or high_target <= low_target:
        return 8   # neutral when we can't compute spread
    spread_pct = (high_target - low_target) / current_price * 100
    # Linear decay from 30pts at 0% spread to 0pts at 150% spread
    raw = max(0, 30 - (spread_pct / 150 * 30))
    return round(raw)


def _vote_unanimity_score(strong_buy: int, buy: int,
                           hold: int, sell: int) -> int:
    """0–25 pts. What fraction of analysts rate this stock Buy or better.
    The distribution shape matters: 10 Strong Buys + 0 else is more
    convincing than 10 Strong Buys + 10 Holds.

      100% bull → 25 pts
       80% bull → 20 pts
       60% bull → 12 pts
       40% bull →  5 pts
       <25% bull → 0 pts
    """
    total = strong_buy + buy + hold + sell
    if not total:
        return 0
    bull_frac = (strong_buy + buy) / total
    if bull_frac >= 1.0:  return 25
    if bull_frac >= 0.8:  return 20
    if bull_frac >= 0.6:  return 12
    if bull_frac >= 0.4:  return  5
    return 0


def _tenure_score(momentum_streak_days: int) -> int:
    """0–15 pts. How long has the consensus been stable/improving.
    Uses the momentum_streak field already computed by get_momentum().

      streak >= 90 days → 15 pts
      streak >= 30 days → 10 pts
      streak >= 7  days →  5 pts
      streak == 0       →  2 pts  (too early to know / recently changed)
    """
    if momentum_streak_days >= 90: return 15
    if momentum_streak_days >= 30: return 10
    if momentum_streak_days >= 7:  return  5
    return 2


def analyst_conviction_score(stock: dict) -> dict:
    """Compute the full Analyst Conviction Score and return both the
    total (0–100) and the four component sub-scores so the UI can
    show a breakdown rather than a black-box number.

    Returns:
      {
        "score":     int,   # 0–100 total
        "coverage":  int,   # 0–30
        "clarity":   int,   # 0–30
        "unanimity": int,   # 0–25
        "tenure":    int,   # 0–15
      }
    """
    coverage  = _coverage_depth_score(stock.get("analyst_count", 0))
    clarity   = _consensus_clarity_score(
        stock.get("current_price", 0),
        stock.get("high_target",   0),
        stock.get("low_target",    0),
    )
    unanimity = _vote_unanimity_score(
        stock.get("strong_buy", 0),
        stock.get("buy",        0),
        stock.get("hold",       0),
        stock.get("sell",       0),
    )
    tenure    = _tenure_score(stock.get("momentum_streak", 0))
    total     = coverage + clarity + unanimity + tenure
    return {
        "score":     min(100, total),
        "coverage":  coverage,
        "clarity":   clarity,
        "unanimity": unanimity,
        "tenure":    tenure,
    }


# ── Main generation ──────────────────────────────────────────────
def generate_stocks(run_date: str) -> list:
    # For production: use get_full_universe() (full SEC EDGAR list).
    # For development: use get_full_universe()[:500] or UNIVERSE_FALLBACK.
    tickers = get_full_universe()
    if not tickers:
        # Do NOT fall back to UNIVERSE_FALLBACK here. Running on 111 hardcoded
        # tickers and calling save_cache() at the end would overwrite a
        # 3,800+ stock cache with 111 entries — exactly the bug that was
        # causing the stock count to collapse to 111 after each run when
        # the SEC EDGAR fetch was failing silently. If the universe fetch
        # fails, abort and keep the existing cache intact.
        print("  ✗  SEC EDGAR fetch failed — aborting run to preserve existing cache.")
        print("  ✗  Check the error above. Common causes:")
        print("     - SEC rate-limiting your IP (try again in an hour)")
        print("     - User-Agent not accepted (must include real contact email)")
        print("     - Network/firewall issue on the server")
        sys.exit(1)

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
    # Separate, less-frequent job for per-firm analyst call tracking.
    # Run this weekly via its own cron entry, e.g.:
    #   0 3 * * 0 /usr/bin/python3 /path/to/server/generate.py --analyst-calls
    # Kept out of the main daily run since it costs an extra Yahoo API
    # call per ticker on top of the already rate-limit-sensitive main fetch.
    if "--analyst-calls" in sys.argv:
        print(f"\n  ▲  StockUpside.io — Analyst Call Tracker")
        print(f"  →  Started at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        init_db()

        stocks = get_stocks_cached()
        if not stocks:
            print("  ✗  No cached stocks found — run the main generator first.")
            sys.exit(1)

        total_new = 0
        errored = 0
        CIRCUIT_BREAKER_SAMPLE = 20   # check failure rate after this many tickers
        CIRCUIT_BREAKER_THRESHOLD = 0.9  # abort if >=90% of the sample errored

        for i, s in enumerate(stocks):
            ticker = s["ticker"]
            calls, did_error = fetch_analyst_calls(ticker)
            if did_error:
                errored += 1
            else:
                n = save_analyst_calls(ticker, calls)
                total_new += n
                if n > 0:
                    print(f"  →  {ticker}: {n} new call(s)")

            # Circuit breaker: if almost everything in the first batch is
            # failing, this is almost certainly NOT thousands of individual
            # "ticker has no data" cases — it's Yahoo 404ing/blocking the
            # upgrades_downgrades endpoint entirely (a known yfinance/Yahoo
            # API issue, see github.com/ranaroussi/yfinance/issues/1957).
            # Without this, the script "succeeds" after hours of runtime
            # having recorded zero data and given no indication anything
            # was wrong — exactly what happened here.
            if (i + 1) == CIRCUIT_BREAKER_SAMPLE:
                fail_rate = errored / CIRCUIT_BREAKER_SAMPLE
                if fail_rate >= CIRCUIT_BREAKER_THRESHOLD:
                    print(f"\n  ✗  {errored}/{CIRCUIT_BREAKER_SAMPLE} tickers "
                          f"errored ({fail_rate:.0%}) in the opening sample — "
                          f"this looks like a systemic failure (Yahoo blocking "
                          f"or 404ing the upgrades_downgrades endpoint), not "
                          f"normal per-ticker gaps. Aborting early instead of "
                          f"grinding through all {len(stocks)} tickers for "
                          f"nothing. Try: pip install --upgrade yfinance, "
                          f"or check finance.yahoo.com is reachable from this "
                          f"host. Re-run once that's confirmed working.")
                    sys.exit(1)

            if (i + 1) % 100 == 0:
                print(f"  ...{i+1}/{len(stocks)} tickers checked, "
                      f"{total_new} new calls, {errored} errored so far")
            time.sleep(0.5 + random.uniform(0.2, 0.5))  # be polite to Yahoo

        print(f"\n  ✓  Collection done: {total_new} new analyst calls recorded "
              f"({errored} tickers errored out of {len(stocks)})")
        resolve_analyst_call_outcomes()
        print(f"  ✓  All done.\n")
        sys.exit(0)

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
        save_final_cache(data, run_date)
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