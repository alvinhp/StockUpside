#!/usr/bin/env python3
"""
cleanup_old_backfill.py — Remove the old today's-target-approximation rows
============================================================================
The original backfill_accuracy.py wrote snapshots/performance rows tagged
source='backfill', using TODAY's analyst target applied retroactively to
historical dates. backfill_accuracy_fmp.py replaces these with real
point-in-time targets tagged source='backfill_fmp'.

Because both scripts use INSERT OR IGNORE keyed on (date, ticker) /
(snapshot_date, ticker, days_later), the old 'backfill' rows will silently
block the new, more accurate 'backfill_fmp' rows from being written for
the same date/ticker/checkpoint. This script deletes the old approximated
rows first so the new run isn't blocked.

Real nightly data (source IS NULL, i.e. 'live') is never touched.

Usage
-----
    # Preview what would be deleted, without deleting anything:
    python3 cleanup_old_backfill.py --dry-run

    # Actually delete:
    python3 cleanup_old_backfill.py

    # Then re-run the FMP backfill:
    export FMP_API_KEY=your_key_here
    python3 backfill_accuracy_fmp.py
"""

import os, sqlite3, argparse

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
if os.path.basename(_THIS_DIR) == "server":
    BASE_DIR = os.path.dirname(_THIS_DIR)
else:
    BASE_DIR = _THIS_DIR
DB_PATH = os.path.join(BASE_DIR, "server", "cache.db")


def get_db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=5000")
    return con


def main():
    parser = argparse.ArgumentParser(description="Remove old approximated backfill rows")
    parser.add_argument("--dry-run", action="store_true", help="Show counts only, delete nothing")
    args = parser.parse_args()

    con = get_db()

    # snapshots rows tagged as the old approximation
    snap_rows = con.execute(
        "SELECT COUNT(*) FROM snapshots WHERE source='backfill'"
    ).fetchone()[0]

    # performance rows don't carry a source column themselves — they're
    # linked to snapshots by (snapshot_date, ticker). Delete performance
    # rows whose snapshot_date+ticker pair matches an old 'backfill' snapshot,
    # since those performance numbers were computed against the same
    # today's-target approximation.
    perf_rows = con.execute("""
        SELECT COUNT(*) FROM performance p
        WHERE EXISTS (
            SELECT 1 FROM snapshots s
            WHERE s.date = p.snapshot_date
              AND s.ticker = p.ticker
              AND s.source = 'backfill'
        )
    """).fetchone()[0]

    print(f"\n  Old 'backfill' (today's-target approximation) rows found:")
    print(f"    snapshots:   {snap_rows:,}")
    print(f"    performance: {perf_rows:,}")

    if snap_rows == 0 and perf_rows == 0:
        print("\n  ✓  Nothing to clean up.\n")
        con.close()
        return

    if args.dry_run:
        print("\n  →  Dry run — nothing deleted. Re-run without --dry-run to delete.\n")
        con.close()
        return

    con.execute("""
        DELETE FROM performance
        WHERE EXISTS (
            SELECT 1 FROM snapshots s
            WHERE s.date = performance.snapshot_date
              AND s.ticker = performance.ticker
              AND s.source = 'backfill'
        )
    """)
    deleted_perf = con.execute("SELECT changes()").fetchone()[0]

    con.execute("DELETE FROM snapshots WHERE source='backfill'")
    deleted_snap = con.execute("SELECT changes()").fetchone()[0]

    con.commit()
    con.close()

    print(f"\n  ✓  Deleted {deleted_snap:,} snapshot rows and {deleted_perf:,} performance rows.")
    print(f"     Real nightly data (source IS NULL) was left untouched.")
    print(f"     You can now run backfill_accuracy_fmp.py to fill these back in")
    print(f"     with real point-in-time targets.\n")


if __name__ == "__main__":
    main()