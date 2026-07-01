#!/usr/bin/env python3
"""
backfill_accuracy.py — One-shot historical accuracy backfill
=============================================================
Populates the `snapshots` and `performance` tables with up to 2 years of
historical data so the accuracy tab has meaningful data immediately, without
waiting months for nightly snapshots to accumulate.

Strategy
--------
  1. Read the current cache for the list of tickers (target_price, consensus,
     analyst_count, upside_pct, rank). These fields are used as-is for the
     synthetic snapshots — they're today's values, not what they were
     historically, so treat this as an approximation. The price data (what
     the stock actually did) comes from real Yahoo Finance history and is
     100% accurate. The inaccuracy is only in the "consensus at the time"
     column, which is a known limitation documented in the output.

  2. For each ticker, fetch 2 years of daily closing prices in one bulk
     history() call.

  3. For each "snapshot date" (every ~30 days going back 2 years), record
     the closing price on that date as current_price and write a snapshots row.

  4. For each snapshot, look up the closing price 30/60/90 days later and
     write the corresponding performance rows.

  5. All inserts use INSERT OR IGNORE so re-running is safe — existing rows
     (from real nightly snapshots) are never overwritten.

Usage
-----
    python3 server/backfill_accuracy.py

    # Limit to fewer tickers for a quick test:
    python3 server/backfill_accuracy.py --limit 100

    # Only backfill specific tickers:
    python3 server/backfill_accuracy.py --tickers AAPL MSFT NVDA

Recommended: run once after deploying, then let nightly generate.py take over.
Expected runtime: 2–4 hours for the full universe (one history() call per ticker,
rate-limited to be polite to Yahoo).
"""

import json, sqlite3, time, datetime, os, sys, random, math, argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

import yfinance as yf
import pandas as pd

# ── Paths (same as generate.py) ───────────────────────────────────────────────
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
if os.path.basename(_THIS_DIR) == "server":
    BASE_DIR = os.path.dirname(_THIS_DIR)
else:
    BASE_DIR = _THIS_DIR
DB_PATH = os.path.join(BASE_DIR, "server", "cache.db")

CHECKPOINTS = [30, 60, 90]
# How far back to synthesise snapshot dates. 2 years gives enough data
# for the 90-day performance checkpoint to have ~8 resolved snapshots per
# ticker right away.
LOOKBACK_DAYS = 730
# Spacing between synthetic snapshot dates. 30-day intervals strike a balance
# between data density and avoiding the appearance of fake daily granularity.
SNAPSHOT_INTERVAL_DAYS = 30
# Concurrency — history() is network-bound; 4 workers gives a real speedup
# without hammering Yahoo hard enough to trigger sustained rate limiting.
MAX_WORKERS = 4

def get_db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=5000")
    return con

def get_stocks_cached() -> list:
    con = get_db()
    row = con.execute(
        "SELECT data FROM cache ORDER BY ts DESC LIMIT 1"
    ).fetchone()
    con.close()
    return json.loads(row[0]) if row else []

def already_backfilled(ticker: str, con: sqlite3.Connection) -> bool:
    """Return True if this ticker already has at least one backfilled
    snapshot (marked with source='backfill'). Used to skip tickers on
    a re-run without checking every individual row."""
    row = con.execute(
        "SELECT 1 FROM snapshots WHERE ticker=? AND source='backfill' LIMIT 1",
        (ticker,)
    ).fetchone()
    return row is not None

def ensure_source_column(con: sqlite3.Connection):
    """Add a `source` TEXT column to snapshots if it doesn't already exist.
    Existing rows from real nightly snapshots get source=NULL (treated as
    'live'), backfilled rows get source='backfill'. This lets the accuracy
    page optionally distinguish the two, and lets already_backfilled() skip
    tickers efficiently on re-runs."""
    cols = [r[1] for r in con.execute("PRAGMA table_info(snapshots)").fetchall()]
    if "source" not in cols:
        con.execute("ALTER TABLE snapshots ADD COLUMN source TEXT")
        con.commit()
        print("  ✓  Added 'source' column to snapshots table")

def backfill_ticker(stock: dict, today: datetime.date) -> tuple[int, int]:
    """Fetch 2 years of price history for one ticker and write synthetic
    snapshots + performance rows. Returns (snapshots_written, perf_written).

    All inserts use INSERT OR IGNORE so this is safe to re-run — real
    nightly data and previously-backfilled rows are never overwritten.
    """
    ticker      = stock["ticker"]
    target_price = stock.get("target_price", 0) or 0
    consensus   = stock.get("consensus", "Hold")
    analyst_count = stock.get("analyst_count", 0) or 0
    upside_pct  = stock.get("upside_pct", 0) or 0
    rank        = stock.get("rank", 0) or 0

    if not target_price or target_price <= 0:
        return 0, 0

    start_date = today - datetime.timedelta(days=LOOKBACK_DAYS + 95)  # extra buffer for 90d checkpoint

    try:
        t_obj = yf.Ticker(ticker)
        hist = t_obj.history(start=start_date.isoformat(), end=today.isoformat())
    except Exception as e:
        print(f"  ⚠  {ticker}: history fetch failed — {e}")
        return 0, 0

    if hist is None or hist.empty:
        return 0, 0

    hist = hist.sort_index()
    # Build a date -> closing price lookup. For non-trading days (weekends/
    # holidays) we forward-fill to the next available trading day.
    # Store as {date_str: float} for fast lookup.
    close_by_date: dict[str, float] = {}
    for ts, row in hist.iterrows():
        close_by_date[ts.date().isoformat()] = float(row["Close"])

    def price_on_or_after(target_dt: datetime.date) -> float | None:
        """Return the closing price on target_dt or the next trading day."""
        for offset in range(7):  # look up to a week forward (covers long weekends)
            d = (target_dt + datetime.timedelta(days=offset)).isoformat()
            if d in close_by_date:
                return close_by_date[d]
        return None

    # Generate snapshot dates: every SNAPSHOT_INTERVAL_DAYS going back
    # LOOKBACK_DAYS from today. Stop at 90 days ago so there's at least
    # one checkpoint (30d) resolvable for the most recent snapshots.
    snapshot_dates = []
    d = today - datetime.timedelta(days=90)  # most recent snapshot that can have a 30d checkpoint
    while d >= today - datetime.timedelta(days=LOOKBACK_DAYS):
        snapshot_dates.append(d)
        d -= datetime.timedelta(days=SNAPSHOT_INTERVAL_DAYS)

    con = get_db()
    snaps_written = 0
    perf_written  = 0

    for snap_date in snapshot_dates:
        snap_date_str = snap_date.isoformat()
        price_then = price_on_or_after(snap_date)
        if not price_then or price_then <= 0:
            continue

        # Synthesise upside_pct from historical price + today's target.
        # This is the acknowledged approximation: target is today's value.
        hist_upside = round((target_price / price_then - 1) * 100, 1)

        try:
            con.execute("""
                INSERT OR IGNORE INTO snapshots
                (date, ticker, rank, current_price, target_price, upside_pct,
                 consensus, analyst_count, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'backfill')
            """, (snap_date_str, ticker, rank, round(price_then, 2),
                  round(target_price, 2), hist_upside, consensus, analyst_count))
            if con.execute("SELECT changes()").fetchone()[0] > 0:
                snaps_written += 1
        except Exception as e:
            print(f"  ⚠  {ticker} snapshot {snap_date_str}: {e}")
            continue

        # Performance checkpoints: for each CHECKPOINT days after snap_date,
        # look up the actual closing price and compute the return.
        for days_later in CHECKPOINTS:
            checkpoint_date = snap_date + datetime.timedelta(days=days_later)
            if checkpoint_date >= today:
                continue  # not enough time has passed — skip
            price_at_checkpoint = price_on_or_after(checkpoint_date)
            if not price_at_checkpoint or price_at_checkpoint <= 0:
                continue

            actual_return = round((price_at_checkpoint / price_then - 1) * 100, 2)
            hit_target    = 1 if price_at_checkpoint >= target_price * 0.95 else 0

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
    parser = argparse.ArgumentParser(description="Backfill accuracy data from price history")
    parser.add_argument("--limit",   type=int, default=0,   help="Only process first N tickers (0 = all)")
    parser.add_argument("--tickers", nargs="*", default=[], help="Only process specific tickers")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS, help=f"Concurrent workers (default {MAX_WORKERS})")
    args = parser.parse_args()

    print(f"\n  ▲  StockUpside.io — Accuracy Backfill")
    print(f"  →  Started at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  →  DB: {DB_PATH}\n")

    stocks = get_stocks_cached()
    if not stocks:
        print("  ✗  No cached stocks found — run generate.py first.")
        sys.exit(1)

    # Apply filters
    if args.tickers:
        ticker_set = {t.upper() for t in args.tickers}
        stocks = [s for s in stocks if s["ticker"] in ticker_set]
        print(f"  →  Filtered to {len(stocks)} specified tickers")
    elif args.limit:
        stocks = stocks[:args.limit]
        print(f"  →  Limited to first {len(stocks)} tickers")

    # Add source column if needed (one-time migration)
    con = get_db()
    ensure_source_column(con)

    # Skip tickers already backfilled (safe to re-run)
    already_done = {s["ticker"] for s in stocks if already_backfilled(s["ticker"], con)}
    con.close()
    if already_done:
        print(f"  ↻  Skipping {len(already_done)} tickers already backfilled")
        stocks = [s for s in stocks if s["ticker"] not in already_done]

    if not stocks:
        print("  ✓  All tickers already backfilled — nothing to do.")
        sys.exit(0)

    print(f"  →  Backfilling {len(stocks)} tickers with {args.workers} workers...")
    print(f"     (2 years of history, snapshots every {SNAPSHOT_INTERVAL_DAYS} days,")
    print(f"      30/60/90-day performance checkpoints)")
    print(f"     Using today's target prices as approximation for historical targets.\n")

    today = datetime.date.today()
    total_snaps = 0
    total_perf  = 0
    completed   = 0
    lock        = threading.Lock()
    start_time  = time.time()

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
                completed   += 1
                total_snaps += snaps
                total_perf  += perf
                if completed % 50 == 0 or completed == len(stocks):
                    elapsed = time.time() - start_time
                    rate = completed / elapsed * 60
                    remaining = (len(stocks) - completed) / (rate / 60) / 60 if rate > 0 else 0
                    print(f"  →  {completed}/{len(stocks)} tickers done | "
                          f"{total_snaps:,} snapshots | {total_perf:,} perf rows | "
                          f"~{remaining:.0f} min remaining")

            # Small jitter to avoid thundering-herd on Yahoo
            time.sleep(0.1 + random.uniform(0, 0.2))

    elapsed = time.time() - start_time
    print(f"\n  ✓  Done in {elapsed/60:.1f} min")
    print(f"     {total_snaps:,} snapshot rows written")
    print(f"     {total_perf:,} performance rows written")
    print(f"\n  ℹ  Note: target prices in backfilled snapshots reflect TODAY's")
    print(f"     analyst consensus targets, not historical targets. Price data")
    print(f"     (what stocks actually did) is 100% accurate from Yahoo history.")
    print(f"     The accuracy tab will now show data immediately.\n")


if __name__ == "__main__":
    main()