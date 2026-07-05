#!/usr/bin/env python3
"""
backfill_accuracy_fmp.py — Historical accuracy backfill using REAL point-in-time targets
==========================================================================================
Replaces the old backfill_accuracy.py approximation (which applied TODAY's
analyst target retroactively to every historical snapshot) with actual
historical consensus targets pulled from FMP's Price Target News API.

Why this matters
-----------------
The old script's own docstring flagged the limitation: "these fields are
used as-is for the synthetic snapshots — they're today's values, not what
they were historically." Since targets drift upward as stocks rise, that
approximation systematically UNDER-counts "hits" the further back a
snapshot goes (a stock could have beaten its real 2019 target easily, but
still fail against a much higher 2026 target). Hence the artificially low
16.9%/12.8% hit-rates on the 1yr/2yr accuracy tabs despite strong avg
returns.

This version fetches every individual analyst price-target update FMP has
for a ticker (each has a `publishedDate`), then reconstructs the consensus
target that was actually in effect on each historical snapshot date —
using only updates published on/before that date, taking the latest
update per analyst firm (so a firm's stale 2-year-old target doesn't
count forever), within a trailing lookback window.

Strategy
--------
  1. For each ticker, fetch its full price-target-news history from FMP
     (paginated), sorted ascending by publishedDate.

  2. For each ticker, fetch 2+ years of daily closing prices via yfinance
     (unchanged from before — Yahoo's price history is accurate; only the
     target-price side was the problem).

  3. For each snapshot date (every ~30 days going back LOOKBACK_DAYS):
       - Build point-in-time consensus = average of the most recent
         price-target update from each analyst firm, restricted to
         updates published in [snapshot_date - CONSENSUS_WINDOW_DAYS,
         snapshot_date]. This mirrors how "current consensus" is normally
         computed (stale/expired targets roll off).
       - If no updates exist in that window, skip the snapshot (better to
         have no data than a wrong number).
       - Record current_price (from Yahoo) + the real point-in-time
         consensus target price for that date.

  4. For each snapshot, look up the closing price at each CHECKPOINT
     (30/90/180/365/730 days later) and compute real return + hit test
     against the REAL historical target, not today's.

  5. All inserts use INSERT OR IGNORE so re-running is safe.

Usage
-----
    export FMP_API_KEY=your_key_here
    python3 backfill_accuracy_fmp.py
    python3 backfill_accuracy_fmp.py --limit 100
    python3 backfill_accuracy_fmp.py --tickers AAPL MSFT NVDA

Notes on FMP plan requirements
-------------------------------
price-target-news is on FMP's paid tiers for full historical depth (the
free plan's trailing lookback is limited — check your plan's docs). Start
with --limit / --tickers on a small set to confirm your plan returns
history deep enough to matter before running the full universe, since
Price Target Consensus/legacy plans may also have symbol-count caps.
"""

import json, sqlite3, time, datetime, os, sys, random, argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from collections import defaultdict

import requests
import yfinance as yf

# ── Paths (same as generate.py / old backfill_accuracy.py) ──────────────────
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
if os.path.basename(_THIS_DIR) == "server":
    BASE_DIR = os.path.dirname(_THIS_DIR)
else:
    BASE_DIR = _THIS_DIR
DB_PATH = os.path.join(BASE_DIR, "server", "cache.db")

FMP_API_KEY = os.environ.get("FMP_API_KEY", "")
FMP_BASE = "https://financialmodelingprep.com/stable/price-target-news"

CHECKPOINTS = [30, 90, 180, 365, 730]
LOOKBACK_DAYS = 900
SNAPSHOT_INTERVAL_DAYS = 30
# How far back from a snapshot date to look for still-valid analyst targets
# when reconstructing point-in-time consensus. 365 days is a reasonably
# standard "still current" window for an analyst rating before it's
# considered stale — matches how most consensus feeds roll off old targets.
CONSENSUS_WINDOW_DAYS = 365
MAX_WORKERS = 4
FMP_PAGE_LIMIT = 1000  # max records per page FMP allows for this endpoint


def get_db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=5000")
    return con


def get_stocks_cached() -> list:
    con = get_db()
    row = con.execute("SELECT data FROM cache ORDER BY ts DESC LIMIT 1").fetchone()
    con.close()
    return json.loads(row[0]) if row else []


def ensure_source_column(con: sqlite3.Connection):
    cols = [r[1] for r in con.execute("PRAGMA table_info(snapshots)").fetchall()]
    if "source" not in cols:
        con.execute("ALTER TABLE snapshots ADD COLUMN source TEXT")
        con.commit()
        print("  ✓  Added 'source' column to snapshots table")


def already_backfilled(ticker: str, con: sqlite3.Connection) -> bool:
    row = con.execute(
        "SELECT 1 FROM snapshots WHERE ticker=? AND source='backfill_fmp' LIMIT 1",
        (ticker,)
    ).fetchone()
    return row is not None


def fetch_price_target_history(ticker: str) -> list[dict]:
    """Fetch ALL price-target-news records for a ticker from FMP, paginated,
    sorted ascending by publishedDate. Each record: {date, priceTarget, analystCompany}."""
    if not FMP_API_KEY:
        raise RuntimeError("FMP_API_KEY environment variable not set")

    records = []
    page = 0
    while True:
        try:
            resp = requests.get(
                FMP_BASE,
                params={"symbol": ticker, "page": page, "limit": FMP_PAGE_LIMIT, "apikey": FMP_API_KEY},
                timeout=20,
            )
            resp.raise_for_status()
            batch = resp.json()
        except Exception as e:
            print(f"  ⚠  {ticker}: FMP fetch failed on page {page} — {e}")
            break

        if not batch:
            break

        for rec in batch:
            pub = rec.get("publishedDate")
            target = rec.get("priceTarget")
            firm = rec.get("analystCompany") or rec.get("newsPublisher") or "unknown"
            if not pub or target is None:
                continue
            try:
                date_str = pub[:10]  # publishedDate is an ISO-ish timestamp; take the date part
                datetime.date.fromisoformat(date_str)
            except Exception:
                continue
            records.append({"date": date_str, "target": float(target), "firm": firm})

        if len(batch) < FMP_PAGE_LIMIT:
            break
        page += 1
        time.sleep(0.15)  # be polite to the API between pages

    records.sort(key=lambda r: r["date"])
    return records


def consensus_asof(target_history: list[dict], asof_date: datetime.date) -> tuple[float, int] | None:
    """Reconstruct point-in-time consensus as of asof_date: average of the
    most recent target from each analyst firm, restricted to targets
    published within [asof_date - CONSENSUS_WINDOW_DAYS, asof_date].
    Returns (avg_target, analyst_count) or None if no eligible targets."""
    window_start = (asof_date - datetime.timedelta(days=CONSENSUS_WINDOW_DAYS)).isoformat()
    asof_str = asof_date.isoformat()

    latest_per_firm: dict[str, tuple[str, float]] = {}
    for rec in target_history:
        if rec["date"] > asof_str:
            break  # history is sorted ascending; nothing further qualifies
        if rec["date"] < window_start:
            continue
        firm = rec["firm"]
        if firm not in latest_per_firm or rec["date"] >= latest_per_firm[firm][0]:
            latest_per_firm[firm] = (rec["date"], rec["target"])

    if not latest_per_firm:
        return None

    targets = [t for _, t in latest_per_firm.values()]
    return sum(targets) / len(targets), len(targets)


def backfill_ticker(stock: dict, today: datetime.date) -> tuple[int, int]:
    ticker = stock["ticker"]

    try:
        target_history = fetch_price_target_history(ticker)
    except RuntimeError:
        raise
    except Exception as e:
        print(f"  ⚠  {ticker}: target history fetch failed — {e}")
        return 0, 0

    if not target_history:
        return 0, 0

    start_date = today - datetime.timedelta(days=LOOKBACK_DAYS + 740)
    try:
        hist = yf.Ticker(ticker).history(start=start_date.isoformat(), end=today.isoformat())
    except Exception as e:
        print(f"  ⚠  {ticker}: price history fetch failed — {e}")
        return 0, 0

    if hist is None or hist.empty:
        return 0, 0

    hist = hist.sort_index()
    close_by_date: dict[str, float] = {ts.date().isoformat(): float(row["Close"]) for ts, row in hist.iterrows()}

    def price_on_or_after(target_dt: datetime.date) -> float | None:
        for offset in range(7):
            d = (target_dt + datetime.timedelta(days=offset)).isoformat()
            if d in close_by_date:
                return close_by_date[d]
        return None

    snapshot_dates = []
    d = today - datetime.timedelta(days=30)
    while d >= today - datetime.timedelta(days=LOOKBACK_DAYS):
        snapshot_dates.append(d)
        d -= datetime.timedelta(days=SNAPSHOT_INTERVAL_DAYS)

    con = get_db()
    snaps_written = 0
    perf_written = 0

    for snap_date in snapshot_dates:
        snap_date_str = snap_date.isoformat()
        price_then = price_on_or_after(snap_date)
        if not price_then or price_then <= 0:
            continue

        consensus_result = consensus_asof(target_history, snap_date)
        if consensus_result is None:
            continue  # no real target data available for this date — skip rather than guess
        target_price_then, analyst_count_then = consensus_result

        hist_upside = round((target_price_then / price_then - 1) * 100, 1)

        try:
            con.execute("""
                INSERT OR IGNORE INTO snapshots
                (date, ticker, rank, current_price, target_price, upside_pct,
                 consensus, analyst_count, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'backfill_fmp')
            """, (snap_date_str, ticker, stock.get("rank", 0) or 0, round(price_then, 2),
                  round(target_price_then, 2), hist_upside, stock.get("consensus", "Hold"),
                  analyst_count_then))
            if con.execute("SELECT changes()").fetchone()[0] > 0:
                snaps_written += 1
        except Exception as e:
            print(f"  ⚠  {ticker} snapshot {snap_date_str}: {e}")
            continue

        for days_later in CHECKPOINTS:
            checkpoint_date = snap_date + datetime.timedelta(days=days_later)
            if checkpoint_date >= today:
                continue
            price_at_checkpoint = price_on_or_after(checkpoint_date)
            if not price_at_checkpoint or price_at_checkpoint <= 0:
                continue

            actual_return = round((price_at_checkpoint / price_then - 1) * 100, 2)
            # Hit test now uses the REAL point-in-time target, not today's.
            hit_target = 1 if price_at_checkpoint >= target_price_then * 0.95 else 0

            try:
                con.execute("""
                    INSERT OR IGNORE INTO performance
                    (snapshot_date, ticker, days_later, price_then, price_now,
                     actual_return, hit_target, checked_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (snap_date_str, ticker, days_later,
                      round(price_then, 2), round(price_at_checkpoint, 2),
                      actual_return, hit_target, today.isoformat()))
                if con.execute("SELECT changes()").fetchone()[0] > 0:
                    perf_written += 1
            except Exception as e:
                print(f"  ⚠  {ticker} perf {snap_date_str}+{days_later}d: {e}")

    con.commit()
    con.close()
    return snaps_written, perf_written


def main():
    parser = argparse.ArgumentParser(description="Backfill accuracy data using FMP point-in-time targets")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--tickers", nargs="*", default=[])
    parser.add_argument("--workers", type=int, default=MAX_WORKERS)
    args = parser.parse_args()

    if not FMP_API_KEY:
        print("  ✗  FMP_API_KEY environment variable not set. Get a key at financialmodelingprep.com and:")
        print("       export FMP_API_KEY=your_key_here")
        sys.exit(1)

    print(f"\n  ▲  StockUpside.io — Accuracy Backfill (FMP point-in-time targets)")
    print(f"  →  Started at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  →  DB: {DB_PATH}\n")

    stocks = get_stocks_cached()
    if not stocks:
        print("  ✗  No cached stocks found — run generate.py first.")
        sys.exit(1)

    if args.tickers:
        ticker_set = {t.upper() for t in args.tickers}
        stocks = [s for s in stocks if s["ticker"] in ticker_set]
        print(f"  →  Filtered to {len(stocks)} specified tickers")
    elif args.limit:
        stocks = stocks[:args.limit]
        print(f"  →  Limited to first {len(stocks)} tickers")

    con = get_db()
    ensure_source_column(con)

    already_done = {s["ticker"] for s in stocks if already_backfilled(s["ticker"], con)}
    con.close()
    if already_done:
        print(f"  ↻  Skipping {len(already_done)} tickers already backfilled with real targets")
        stocks = [s for s in stocks if s["ticker"] not in already_done]

    if not stocks:
        print("  ✓  All tickers already backfilled — nothing to do.")
        sys.exit(0)

    print(f"  →  Backfilling {len(stocks)} tickers with {args.workers} workers...")
    print(f"     Using REAL point-in-time consensus targets from FMP (not today's target).")
    print(f"     Snapshots skipped entirely where no historical target data exists,")
    print(f"     rather than approximated.\n")

    today = datetime.date.today()
    total_snaps = total_perf = completed = 0
    lock = threading.Lock()
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(backfill_ticker, s, today): s["ticker"] for s in stocks}
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                snaps, perf = future.result()
            except Exception as e:
                print(f"  ⚠  Worker error on {ticker}: {e}")
                snaps, perf = 0, 0

            with lock:
                completed += 1
                total_snaps += snaps
                total_perf += perf
                if completed % 50 == 0 or completed == len(stocks):
                    elapsed = time.time() - start_time
                    rate = completed / elapsed * 60
                    remaining = (len(stocks) - completed) / (rate / 60) / 60 if rate > 0 else 0
                    print(f"  →  {completed}/{len(stocks)} tickers | "
                          f"{total_snaps:,} snapshots | {total_perf:,} perf rows | "
                          f"~{remaining:.0f} min remaining")

            time.sleep(0.1 + random.uniform(0, 0.2))

    elapsed = time.time() - start_time
    print(f"\n  ✓  Done in {elapsed/60:.1f} min")
    print(f"     {total_snaps:,} snapshot rows written (source='backfill_fmp')")
    print(f"     {total_perf:,} performance rows written")
    print(f"\n  ℹ  These snapshots use REAL historical consensus targets, so the")
    print(f"     6mo/1yr/2yr accuracy tabs should now reflect actual analyst")
    print(f"     performance instead of the old today's-target approximation.")
    print(f"     Tickers with insufficient FMP target history were skipped for")
    print(f"     dates lacking real data rather than guessed at.\n")


if __name__ == "__main__":
    main()