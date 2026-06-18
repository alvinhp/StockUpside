#!/usr/bin/env python3
"""
restore_cache.py — One-shot recovery for the 111-stock cache regression.

The 3,843-stock cache from the last full run is still sitting in the DB
under a different date row. This script promotes it to today's date so
the site serves that data immediately, without waiting for a full re-run.

Run from the project root:
    python3 server/restore_cache.py
"""
import sys, os, json, datetime, sqlite3

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH  = os.path.join(BASE_DIR, "server", "cache.db")

def get_db():
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL")
    return con

def main():
    today = datetime.date.today().isoformat()
    con = get_db()

    # Show all rows in the cache table so we can see what's available
    rows = con.execute(
        "SELECT date, LENGTH(data), ts FROM cache ORDER BY ts DESC"
    ).fetchall()

    print(f"\n  Cache table contents:")
    for date, data_len, ts in rows:
        import datetime as dt
        age = dt.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")
        # Estimate stock count from JSON (rough: count "rank": occurrences)
        full = con.execute("SELECT data FROM cache WHERE date=?", (date,)).fetchone()
        count = full[0].count('"rank":') if full else 0
        marker = " ← current (broken)" if date == today else ""
        print(f"    {date}  ~{count} stocks  (saved {age}){marker}")

    print()

    # Find the best previous row — highest stock count, not today's broken 111
    best_row = con.execute(
        "SELECT date, data FROM cache WHERE date != ? ORDER BY ts DESC LIMIT 1",
        (today,)
    ).fetchone()

    if not best_row:
        print("  ✗  No previous cache row found. You need to run generate.py.")
        con.close()
        sys.exit(1)

    prev_date, prev_data = best_row
    prev_stocks = json.loads(prev_data)
    print(f"  →  Best previous cache: {prev_date} with {len(prev_stocks)} stocks")
    print(f"  →  Promoting to today ({today})...")

    con.execute(
        "INSERT OR REPLACE INTO cache VALUES (?, ?, strftime('%s','now'))",
        (today, prev_data)
    )
    con.commit()
    con.close()

    print(f"  ✓  Done. The site will now serve {len(prev_stocks)} stocks.")
    print(f"     Run generate.py tonight to get a fully fresh dataset.")
    print()

if __name__ == "__main__":
    main()