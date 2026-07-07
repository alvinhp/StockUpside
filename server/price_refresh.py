#!/usr/bin/env python3
"""
price_refresh.py — Lightweight live-ish price refresh
========================================================
generate.py does a full run once a day (one yf.Ticker(ticker).info call per
ticker — slow, and the reason it's a daily batch job, not something you'd
want running every few minutes).

This script does NOT re-run that pipeline. It does one thing, cheaply and
often: bulk-fetch current prices for every ticker already in today's cache,
then patch just the price-derived fields (current_price, upside_pct,
conviction_score) into the existing cache row. Everything else — targets,
fundamentals, sector, momentum, etc. — is left untouched until the next
full generate.py run.

Why yf.download() instead of yf.Ticker(t).info:
  yf.download() accepts a list of tickers and fetches them in batched HTTP
  requests, rather than one full info-scrape per ticker. It's the same
  approach yfinance recommends for bulk price checks and is dramatically
  lighter/faster than calling .info thousands of times, which is why
  generate.py can only afford to do that once a day but this script can
  run every few minutes without hammering Yahoo or blowing your rate limit.

What gets updated per ticker:
  - current_price        (from the bulk price fetch)
  - upside_pct            = (target_price / current_price - 1) * 100
  - conviction_score + component sub-scores (clarity depends on current_price)

What does NOT get updated (needs the full generate.py run):
  - target_price, high/low_target, analyst_count, consensus, votes
  - forward_pe (needs forward EPS, which isn't stored in the cache row)
  - market_cap, pe_ratio, fundamentals, sector, momentum, ytd_change

Usage
-----
    python3 price_refresh.py

Run this on a schedule (cron, systemd timer, APScheduler, etc.), e.g. every
2-5 minutes during market hours:
    */3 9-16 * * 1-5  cd /path/to/project && python3 price_refresh.py
(adjust hours for your timezone / market open-close, and note yfinance
prices are typically the same ~15-min-delayed feed as Yahoo's own site —
this is NOT a paid real-time feed.)
"""

import os, sys, json, sqlite3, datetime, time, fcntl

import yfinance as yf

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
if os.path.basename(_THIS_DIR) == "server":
    BASE_DIR = os.path.dirname(_THIS_DIR)
else:
    BASE_DIR = _THIS_DIR
DB_PATH = os.path.join(BASE_DIR, "server", "cache.db")

# Import the real conviction-score logic + sanitizer from generate.py so
# this script can't drift out of sync with how scores are actually computed.
sys.path.insert(0, os.path.join(BASE_DIR, "server"))
try:
    from generate import analyst_conviction_score, sanitize_row
except ImportError as e:
    print(f"  ✗  Could not import from generate.py — run this from the project root "
          f"(or alongside server/generate.py). Error: {e}")
    sys.exit(1)

# How many tickers per yf.download() batch. yfinance/Yahoo can choke on
# extremely large single requests; a few hundred per call is a safe size
# that still keeps total request count low.
# Tuned down for small droplets (e.g. 512MB RAM). A bigger batch size and
# threads=True below increase peak memory/CPU per run — if a run takes
# longer than the cron interval, jobs stack up and can swamp a small box
# (this is the #1 cause of "site + SSH both slow" on tiny droplets).
BATCH_SIZE = 100


def get_db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=5000")
    return con


def load_today_cache() -> tuple[str, list] | tuple[None, None]:
    """Load the most recent cache row (whatever date it's tagged with —
    generate.py may not have run today yet, and that's fine; we still
    want to refresh prices on top of whatever's currently live)."""
    con = get_db()
    row = con.execute(
        "SELECT date, data FROM cache ORDER BY ts DESC LIMIT 1"
    ).fetchone()
    con.close()
    if not row:
        return None, None
    date, data = row
    return date, json.loads(data)


def save_cache(date: str, data: list):
    con = get_db()
    con.execute(
        "INSERT OR REPLACE INTO cache VALUES (?, ?, strftime('%s','now'))",
        (date, json.dumps(data))
    )
    con.commit()
    con.close()


def fetch_bulk_prices(tickers: list[str]) -> dict[str, float]:
    """Bulk-fetch the latest close price for each ticker via yf.download(),
    batched to stay well under any single-request limits. Returns
    {ticker: price} only for tickers where a valid price was found."""
    prices: dict[str, float] = {}

    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i:i + BATCH_SIZE]
        try:
            df = yf.download(
                tickers=batch,
                period="1d",
                interval="1m",
                group_by="ticker",
                progress=False,
                threads=False,  # lower peak CPU on small droplets; batches are already small
            )
        except Exception as e:
            print(f"  ⚠  Batch {i // BATCH_SIZE + 1} download failed — {e}")
            continue

        if df is None or df.empty:
            continue

        for ticker in batch:
            try:
                if len(batch) == 1:
                    # yfinance returns a flat frame (no ticker-level column)
                    # when only one ticker is requested.
                    closes = df["Close"].dropna()
                else:
                    if ticker not in df.columns.get_level_values(0):
                        continue
                    closes = df[ticker]["Close"].dropna()
                if closes.empty:
                    continue
                price = float(closes.iloc[-1])
                if price > 0:
                    prices[ticker] = round(price, 2)
            except Exception:
                continue  # missing/delisted ticker — skip, don't crash the batch

        time.sleep(0.5)  # small pause between batches

    return prices


LOCK_PATH = os.path.join(BASE_DIR, "price_refresh.lock")


def main():
    # Refuse to start if a previous run is still in progress. Without this,
    # if a run ever takes longer than the cron interval (very possible on a
    # small droplet under load), cron stacks up overlapping runs, each
    # eating more memory/CPU — that compounding is the usual cause of a
    # tiny droplet becoming sluggish across the board, SSH included.
    lock_file = open(LOCK_PATH, "w")
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print(f"  ⏭  A previous price_refresh.py run is still in progress — skipping this cycle.")
        sys.exit(0)

    try:
        _run()
    finally:
        fcntl.flock(lock_file, fcntl.LOCK_UN)
        lock_file.close()


def _run():
    date, stocks = load_today_cache()
    if not stocks:
        print("  ✗  No cached stocks found — run generate.py first.")
        sys.exit(1)

    print(f"\n  ▲  StockUpside.io — Price Refresh")
    print(f"  →  Started at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  →  Refreshing prices for {len(stocks)} cached tickers...\n")

    tickers = [s["ticker"] for s in stocks]
    prices = fetch_bulk_prices(tickers)

    print(f"  →  Got fresh prices for {len(prices)}/{len(tickers)} tickers")

    updated = 0
    for s in stocks:
        ticker = s["ticker"]
        new_price = prices.get(ticker)
        if not new_price or new_price <= 0:
            continue  # leave this row untouched — better stale than wrong

        target_price = s.get("target_price", 0) or 0
        if target_price <= 0:
            continue

        s["current_price"] = new_price
        s["upside_pct"] = round((target_price / new_price - 1) * 100, 1)

        # Conviction score's "clarity" component depends on current_price
        # relative to high/low target, so recompute it alongside the price.
        conviction = analyst_conviction_score(s)
        s["conviction_score"] = conviction["score"]
        s["conviction_coverage"] = conviction["coverage"]
        s["conviction_clarity"] = conviction["clarity"]
        s["conviction_unanimity"] = conviction["unanimity"]
        s["conviction_tenure"] = conviction["tenure"]

        s.update(sanitize_row({k: s[k] for k in ("current_price", "upside_pct")}))
        updated += 1

    # Re-rank by upside_pct since prices moving changes the ordering.
    stocks.sort(key=lambda x: x.get("upside_pct", 0), reverse=True)
    for i, s in enumerate(stocks):
        s["rank"] = i + 1

    save_cache(date, stocks)
    print(f"  ✓  Updated {updated} tickers' prices and re-ranked the cache.\n")


if __name__ == "__main__":
    main()