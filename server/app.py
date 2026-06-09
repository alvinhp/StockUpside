"""
StockUpside.io — Flask Backend
Run: python3 server/app.py
Serves the REST API on :5000 and static files from /public
"""

import json, sqlite3, time, datetime, hashlib, os, math, threading, webbrowser
import smtplib, email.mime.multipart, email.mime.text
from flask import Flask, jsonify, request, send_from_directory, Response
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

import yfinance as yf
import pandas as pd
import urllib.request
import csv
import io
import threading

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PUBLIC_DIR = os.path.join(BASE_DIR, "public")
DB_PATH    = os.path.join(BASE_DIR, "server", "cache.db")

app = Flask(__name__, static_folder=PUBLIC_DIR, static_url_path="")
limiter = Limiter(get_remote_address, app=app, default_limits=["200 per hour"])

# ── In-memory stock cache (populated by get_stocks_cached, invalidated nightly) ─
_cache_lock: threading.Lock = threading.Lock()
_cache: dict = {"data": None, "date": None}


def nightly_refresh():
    """
    Runs in a background daemon thread. At 01:00 each night it launches
    generate.py as a subprocess so the Flask process is never blocked.
    generate.py writes directly to the SQLite DB; once it exits we
    invalidate the in-memory cache so the next request picks up fresh data.
    """
    import subprocess, sys
    generate_script = os.path.join(BASE_DIR, "server", "generate.py")

    while True:
        now    = datetime.datetime.now()
        target = now.replace(hour=1, minute=0, second=0, microsecond=0)
        if target <= now:
            target += datetime.timedelta(days=1)

        sleep_secs = (target - datetime.datetime.now()).total_seconds()
        print(f"  →  Next refresh at {target.strftime('%Y-%m-%d %H:%M')} ({sleep_secs/3600:.1f}h)")

        # Sleep in short chunks so machine wakeup doesn't cause a long skip
        while (target - datetime.datetime.now()).total_seconds() > 0:
            time.sleep(min(60, max(0, (target - datetime.datetime.now()).total_seconds())))

        print("  →  Launching generate.py as offline job...")
        set_generating(True)
        try:
            result = subprocess.run(
                [sys.executable, generate_script],
                timeout=4 * 3600,   # 4-hour hard cap
                capture_output=False,
            )
            if result.returncode == 0:
                invalidate_memory_cache()
                print("  ✓  Nightly refresh complete — cache invalidated.")
            else:
                print(f"  ⚠  generate.py exited with code {result.returncode}")
        except subprocess.TimeoutExpired:
            print("  ⚠  generate.py timed out after 4 hours")
        except Exception as e:
            print(f"  ⚠  Nightly refresh failed: {e}")
        finally:
            set_generating(False)

# ── Stock universe ─────────────────────────────────────────────────────────────
UNIVERSE = [
    ("NVDA","NVIDIA Corporation","Technology",2.89e12),
    ("AAPL","Apple Inc.","Technology",3.1e12),
    ("MSFT","Microsoft Corporation","Technology",3.0e12),
    ("GOOGL","Alphabet Inc.","Technology",2.1e12),
    ("AMZN","Amazon.com Inc.","Consumer Cyclical",1.9e12),
    ("META","Meta Platforms Inc.","Technology",1.4e12),
    ("TSLA","Tesla Inc.","Consumer Cyclical",1.1e12),
    ("AVGO","Broadcom Inc.","Technology",780e9),
    ("ORCL","Oracle Corporation","Technology",420e9),
    ("CRM","Salesforce Inc.","Technology",310e9),
    ("AMD","Advanced Micro Devices","Technology",245e9),
    ("INTC","Intel Corporation","Technology",95e9),
    ("QCOM","Qualcomm Inc.","Technology",175e9),
    ("NOW","ServiceNow Inc.","Technology",205e9),
    ("INTU","Intuit Inc.","Technology",175e9),
    ("ADBE","Adobe Inc.","Technology",145e9),
    ("SNOW","Snowflake Inc.","Technology",55e9),
    ("PLTR","Palantir Technologies","Technology",280e9),
    ("NET","Cloudflare Inc.","Technology",70e9),
    ("DDOG","Datadog Inc.","Technology",55e9),
    ("ZS","Zscaler Inc.","Technology",45e9),
    ("CRWD","CrowdStrike Holdings","Technology",120e9),
    ("PANW","Palo Alto Networks","Technology",130e9),
    ("FTNT","Fortinet Inc.","Technology",65e9),
    ("MDB","MongoDB Inc.","Technology",20e9),
    ("UBER","Uber Technologies","Technology",175e9),
    ("LYFT","Lyft Inc.","Technology",7e9),
    ("SPOT","Spotify Technology","Technology",88e9),
    ("RBLX","Roblox Corporation","Technology",30e9),
    ("U","Unity Software","Technology",10e9),
    ("JPM","JPMorgan Chase & Co.","Financial Services",680e9),
    ("BAC","Bank of America Corp.","Financial Services",350e9),
    ("WFC","Wells Fargo & Co.","Financial Services",215e9),
    ("GS","Goldman Sachs Group","Financial Services",190e9),
    ("MS","Morgan Stanley","Financial Services",195e9),
    ("C","Citigroup Inc.","Financial Services",130e9),
    ("AXP","American Express Co.","Financial Services",195e9),
    ("V","Visa Inc.","Financial Services",620e9),
    ("MA","Mastercard Inc.","Financial Services",495e9),
    ("PYPL","PayPal Holdings","Financial Services",72e9),
    ("BRK-B","Berkshire Hathaway","Financial Services",1.0e12),
    ("BLK","BlackRock Inc.","Financial Services",155e9),
    ("SCHW","Charles Schwab Corp.","Financial Services",130e9),
    ("JNJ","Johnson & Johnson","Healthcare",390e9),
    ("UNH","UnitedHealth Group","Healthcare",430e9),
    ("LLY","Eli Lilly and Co.","Healthcare",720e9),
    ("PFE","Pfizer Inc.","Healthcare",155e9),
    ("ABBV","AbbVie Inc.","Healthcare",325e9),
    ("MRK","Merck & Co. Inc.","Healthcare",260e9),
    ("TMO","Thermo Fisher Scientific","Healthcare",195e9),
    ("DHR","Danaher Corporation","Healthcare",160e9),
    ("ISRG","Intuitive Surgical","Healthcare",195e9),
    ("AMGN","Amgen Inc.","Healthcare",155e9),
    ("GILD","Gilead Sciences","Healthcare",115e9),
    ("MRNA","Moderna Inc.","Healthcare",14e9),
    ("REGN","Regeneron Pharmaceuticals","Healthcare",85e9),
    ("BIIB","Biogen Inc.","Healthcare",25e9),
    ("VRTX","Vertex Pharmaceuticals","Healthcare",115e9),
    ("XOM","Exxon Mobil Corp.","Energy",480e9),
    ("CVX","Chevron Corporation","Energy",275e9),
    ("COP","ConocoPhillips","Energy",115e9),
    ("EOG","EOG Resources","Energy",68e9),
    ("SLB","SLB (Schlumberger)","Energy",60e9),
    ("MPC","Marathon Petroleum","Energy",58e9),
    ("OXY","Occidental Petroleum","Energy",46e9),
    ("WMT","Walmart Inc.","Consumer Defensive",760e9),
    ("COST","Costco Wholesale","Consumer Defensive",430e9),
    ("PG","Procter & Gamble","Consumer Defensive",365e9),
    ("KO","Coca-Cola Co.","Consumer Defensive",290e9),
    ("PEP","PepsiCo Inc.","Consumer Defensive",200e9),
    ("MCD","McDonald's Corporation","Consumer Cyclical",200e9),
    ("SBUX","Starbucks Corporation","Consumer Cyclical",82e9),
    ("NKE","Nike Inc.","Consumer Cyclical",95e9),
    ("TGT","Target Corporation","Consumer Defensive",56e9),
    ("HD","Home Depot Inc.","Consumer Cyclical",385e9),
    ("LOW","Lowe's Companies","Consumer Cyclical",155e9),
    ("GM","General Motors Co.","Consumer Cyclical",48e9),
    ("F","Ford Motor Company","Consumer Cyclical",43e9),
    ("RIVN","Rivian Automotive","Consumer Cyclical",14e9),
    ("LUV","Southwest Airlines","Industrials",17e9),
    ("DAL","Delta Air Lines","Industrials",25e9),
    ("UAL","United Airlines","Industrials",20e9),
    ("BA","Boeing Company","Industrials",130e9),
    ("GE","GE Aerospace","Industrials",250e9),
    ("CAT","Caterpillar Inc.","Industrials",165e9),
    ("DE","Deere & Company","Industrials",110e9),
    ("HON","Honeywell International","Industrials",140e9),
    ("RTX","RTX Corporation","Industrials",175e9),
    ("LMT","Lockheed Martin","Industrials",115e9),
    ("NOC","Northrop Grumman","Industrials",65e9),
    ("UPS","United Parcel Service","Industrials",95e9),
    ("FDX","FedEx Corporation","Industrials",57e9),
    ("NFLX","Netflix Inc.","Communication Services",500e9),
    ("DIS","Walt Disney Company","Communication Services",205e9),
    ("CMCSA","Comcast Corporation","Communication Services",150e9),
    ("T","AT&T Inc.","Communication Services",165e9),
    ("VZ","Verizon Communications","Communication Services",170e9),
    ("TMUS","T-Mobile US Inc.","Communication Services",275e9),
    ("ABNB","Airbnb Inc.","Consumer Cyclical",88e9),
    ("BKNG","Booking Holdings","Consumer Cyclical",165e9),
    ("NEE","NextEra Energy","Utilities",145e9),
    ("DUK","Duke Energy Corp.","Utilities",88e9),
    ("SO","Southern Company","Utilities",88e9),
    ("AMT","American Tower Corp.","Real Estate",95e9),
    ("PLD","Prologis Inc.","Real Estate",115e9),
    ("EQIX","Equinix Inc.","Real Estate",85e9),
    ("SPG","Simon Property Group","Real Estate",55e9),
    ("NEM","Newmont Corporation","Basic Materials",52e9),
    ("FCX","Freeport-McMoRan Inc.","Basic Materials",68e9),
    ("APD","Air Products & Chemicals","Basic Materials",58e9),
    ("SHW","Sherwin-Williams Co.","Basic Materials",90e9),
]

import time
import random

def get_full_universe() -> list[str]:
    """
    Fetch all US-listed tickers from SEC EDGAR, filtered to remove
    ETFs, funds, SPACs, warrants, and other non-operating companies.
    """
    # Patterns that reliably indicate non-operating securities
    JUNK_SUFFIXES = (
        "W", "WS",      # warrants
        "U",            # units (pre-merger SPACs)
        "R",            # rights
    )
    JUNK_SUBSTRINGS = (
        "ETF", "FUND", "TRUST", "REIT", "NOTE", "BOND",
        "SPAC", "BLANK", "ACQUISITION", "HOLDINGS", "BLANK CHECK",
    )

    try:
        url = "https://www.sec.gov/files/company_tickers.json"
        req = urllib.request.Request(url, headers={"User-Agent": "stockupside@example.com"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        tickers = []
        for entry in data.values():
            t    = entry.get("ticker", "").strip().upper()
            name = entry.get("title",  "").strip().upper()

            # Basic format filter
            if not t or len(t) > 5 or "." in t or "-" in t:
                continue

            # Drop warrants, units, rights by ticker suffix
            if any(t.endswith(sfx) for sfx in JUNK_SUFFIXES) and len(t) > 2:
                continue

            # Drop by company name keywords
            if any(kw in name for kw in JUNK_SUBSTRINGS):
                continue

            tickers.append(t)

        tickers = list(dict.fromkeys(tickers))  # deduplicate, preserve order
        print(f"  →  Universe: {len(tickers)} tickers from SEC EDGAR (after filter)")
        return tickers

    except Exception as e:
        print(f"  ⚠  SEC EDGAR failed: {e}")
        return []

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
                  s["target_price"], s["upside_pct"], s["consensus"],
                  s["analyst_count"]))
        except Exception as e:
            print(f"  ⚠  Snapshot failed for {s['ticker']}: {e}")
    con.commit()
    con.close()
    print(f"  ✓  Snapshot saved: {len(stocks)} stocks for {today}")

CONSENSUS_SCORE = {
    "Strong Buy": 5,
    "Buy": 4,
    "Hold": 3,
    "Underperform": 2,
    "Sell": 1,
}

def get_momentum(ticker: str, current_consensus: str, current_count: int) -> dict:
    """
    Compare current consensus against snapshots from 7, 30, and 90 days ago.
    Returns a momentum signal and the historical data points.
    """
    con = get_db()
    today = datetime.date.today()

    history = {}
    for days in [7, 30, 90]:
        target = (today - datetime.timedelta(days=days)).isoformat()
        # Find the nearest snapshot within a 3-day window
        row = con.execute("""
            SELECT consensus, analyst_count, date
            FROM snapshots
            WHERE ticker = ? AND date <= ? AND date >= ?
            ORDER BY date DESC LIMIT 1
        """, (ticker, target,
              (today - datetime.timedelta(days=days+3)).isoformat()
        )).fetchone()
        if row:
            history[days] = {
                "consensus": row[0],
                "analyst_count": row[1],
                "date": row[2],
                "score": CONSENSUS_SCORE.get(row[0], 3),
            }

    con.close()

    current_score = CONSENSUS_SCORE.get(current_consensus, 3)

    # Determine trend direction using 30-day comparison as primary signal
    trend = "neutral"
    trend_detail = ""
    score_delta = 0

    if 30 in history:
        past_score = history[30]["score"]
        score_delta = current_score - past_score
        past_consensus = history[30]["consensus"]

        if score_delta > 0:
            trend = "up"
            trend_detail = f"{past_consensus} → {current_consensus}"
        elif score_delta < 0:
            trend = "down"
            trend_detail = f"{past_consensus} → {current_consensus}"
        else:
            # Same consensus — check if analyst count is growing
            count_delta = current_count - history[30]["analyst_count"]
            if count_delta >= 2:
                trend = "up"
                trend_detail = f"+{count_delta} new analysts"
            elif count_delta <= -2:
                trend = "down"
                trend_detail = f"{count_delta} analysts dropped coverage"
            else:
                trend = "neutral"
                trend_detail = "unchanged"

    elif 7 in history:
        # Fall back to 7-day if no 30-day data yet
        past_score = history[7]["score"]
        score_delta = current_score - past_score
        trend = "up" if score_delta > 0 else "down" if score_delta < 0 else "neutral"
        trend_detail = f"{history[7]['consensus']} → {current_consensus}" if score_delta != 0 else "unchanged"

    # Streak: how many consecutive days has the consensus been improving?
    streak = 0
    if trend == "up" and 90 in history:
        if history[90]["score"] < history[30].get("score", current_score) <= current_score:
            streak = 90
        elif 30 in history and history[30]["score"] < current_score:
            streak = 30
        elif 7 in history and history[7]["score"] < current_score:
            streak = 7

    return {
        "trend": trend,           # "up", "down", "neutral"
        "trend_detail": trend_detail,
        "score_delta": score_delta,
        "streak_days": streak,
        "history": {
            str(k): v for k, v in history.items()
        },
    }

def _normalize_yield(v):
    """yfinance returns dividend yield inconsistently.
    Some tickers return 0.0087 (correct decimal), others return 0.87 or even 87.0.
    Normalize everything to a 0-1 decimal (e.g. 0.0087 for 0.87%)."""
    if v is None: return None
    if v > 0.2:   return v / 100  # was already in percentage form
    return v

def generate_stocks():
    def _fmt_cap(mc):
        if not mc: return "N/A"
        if mc >= 1e12: return f"${mc/1e12:.2f}T"
        if mc >= 1e9:  return f"${mc/1e9:.0f}B"
        return f"${mc/1e6:.0f}M"

    #tickers = get_full_universe() # use for production only, takes 3-6 hours to run
    tickers = get_full_universe()[:500] # remove for production
    if not tickers:
        print("  ⚠  Universe fetch failed, using hardcoded list")
        tickers = [row[0] for row in UNIVERSE]

    print(f"  →  Fetching analyst targets for {len(tickers)} tickers...")

    rows = []
    total = len(tickers)
    rate_limit_streak = 0  # track consecutive rate limit hits

    for i, ticker in enumerate(tickers):
        if i % 25 == 0:
            print(f"  →  Progress: {i}/{total} ({len(rows)} valid so far)")

        # Base delay — yfinance allows ~2000 req/hour without cookies
        # 2 seconds per ticker = 1800/hour, safely under the limit
        time.sleep(0 + random.uniform(0, 0.5)) # change to 2 secs for production

        retries = 3
        info = None
        for attempt in range(retries):
            try:
                t    = yf.Ticker(ticker)
                info = t.info

                # If we got rate limited, info will be a minimal dict
                # Yahoo returns {"trailingPegRatio": None} or similar stub
                if info and len(info) < 10:
                    raise ValueError("Stub response — likely rate limited")

                rate_limit_streak = 0
                break

            except Exception as e:
                err = str(e)
                if "Too Many Requests" in err or "Rate limited" in err or "Stub response" in err:
                    rate_limit_streak += 1
                    wait = min(60, 10 * (attempt + 1)) + random.uniform(0, 5)
                    print(f"  ⚠  Rate limited ({ticker}), waiting {wait:.0f}s... (streak: {rate_limit_streak})")
                    time.sleep(wait)

                    # If rate limiting is sustained, take a longer break
                    if rate_limit_streak >= 5:
                        print(f"  ⚠  Extended rate limit pause (60s)...")
                        time.sleep(60)
                        rate_limit_streak = 0
                else:
                    print(f"  ⚠  Skipped {ticker}: {e}")
                    break

        if not info or len(info) < 10:
            continue

        try:
            current_price = info.get("currentPrice") or info.get("regularMarketPrice") or 0
            target_price  = info.get("targetMeanPrice") or 0
            analyst_count = info.get("numberOfAnalystOpinions") or 0

            if current_price <= 0 or target_price <= 0 or analyst_count < 2:
                continue

            upside_pct = round((target_price / current_price - 1) * 100, 1)

            if upside_pct < 0 or upside_pct > 2000:
                continue

            high_target = info.get("targetHighPrice") or 0
            low_target  = info.get("targetLowPrice") or 0
            # ── Analyst rating breakdown ───────────────────────────────────────────
            sb = b = h = s = 0
            try:
                rec = t.recommendations
                if rec is not None and not rec.empty:
                    # Use most recent single period only (not cumulative sum)
                    latest = rec.tail(1).iloc[0]
                    raw_sb = int(latest.get("strongBuy", 0))
                    raw_b  = int(latest.get("buy", 0))
                    raw_h  = int(latest.get("hold", 0))
                    raw_s  = int(latest.get("sell", 0)) + int(latest.get("strongSell", 0))
                    raw_total = raw_sb + raw_b + raw_h + raw_s

                    if raw_total > 0:
                        # Scale ratios to match the authoritative analyst_count
                        n = analyst_count
                        sb = round(n * raw_sb / raw_total)
                        b  = round(n * raw_b  / raw_total)
                        h  = round(n * raw_h  / raw_total)
                        s  = max(0, n - sb - b - h)
            except Exception:
                pass

            # Fallback to mean-based estimation if DataFrame unavailable
            if sb + b + h + s == 0:
                n        = analyst_count
                rec_mean = info.get("recommendationMean") or 3.0
                if rec_mean <= 1.5:
                    sb = round(n * 0.70); b = round(n * 0.20); h = round(n * 0.08)
                elif rec_mean <= 2.0:
                    sb = round(n * 0.35); b = round(n * 0.45); h = round(n * 0.15)
                elif rec_mean <= 2.5:
                    sb = round(n * 0.15); b = round(n * 0.40); h = round(n * 0.35)
                elif rec_mean <= 3.0:
                    sb = round(n * 0.05); b = round(n * 0.25); h = round(n * 0.55)
                else:
                    sb = 0; b = round(n * 0.10); h = round(n * 0.40)
                s = max(0, n - sb - b - h)
            consensus_map = {
                "strong_buy": "Strong Buy", "buy": "Buy", "hold": "Hold",
                "underperform": "Underperform", "sell": "Sell", "none": "Hold"
            }
            consensus = consensus_map.get(
                (info.get("recommendationKey") or "none").lower(), "Hold"
            )
            momentum = get_momentum(ticker, consensus, analyst_count)

            ytd_change = 0.0
            try:
                hist = t.history(period="ytd")
                if not hist.empty and hist["Close"].iloc[0] > 0:
                    ytd_change = round(
                        (hist["Close"].iloc[-1] / hist["Close"].iloc[0] - 1) * 100, 1
                    )
            except Exception:
                pass
            
            rows.append(dict(
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
                pe_ratio=round(info.get("trailingPE") or info.get("forwardPE") or 0, 1),
                ytd_change=ytd_change,
                week52_low=round(info.get("fiftyTwoWeekLow") or 0, 2),
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
            ))

        except Exception as e:
            print(f"  ⚠  Skipped {ticker} (parse error): {e}")
            continue

    rows.sort(key=lambda x: x["upside_pct"], reverse=True)
    for i, r in enumerate(rows):
        r["rank"] = i + 1

    print(f"  ✓  Done. {len(rows)} stocks with valid analyst targets.")
    #return rows[:100]
    save_snapshot(rows)
    return rows

# ── Generation lock ────────────────────────────────────────────────────────────
_generating = False
_generating_lock = threading.Lock()

def is_generating() -> bool:
    with _generating_lock:
        return _generating

def set_generating(val: bool):
    global _generating
    with _generating_lock:
        _generating = val

CHECKPOINTS = [30, 60, 90]

def check_performance():
    """
    For each checkpoint (30/60/90 days), find snapshots that are exactly
    that many days old and haven't been checked yet, then fetch current
    prices and store the result.
    """
    con = get_db()
    today = datetime.date.today()

    for days in CHECKPOINTS:
        target_date = (today - datetime.timedelta(days=days)).isoformat()

        # Find snapshots from that date not yet checked at this interval
        rows = con.execute("""
            SELECT s.ticker, s.current_price, s.target_price
            FROM snapshots s
            LEFT JOIN performance p
                ON p.snapshot_date = s.date
                AND p.ticker = s.ticker
                AND p.days_later = ?
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
                # "Hit target" = price came within 5% of the analyst target at any point
                # We use current price as a proxy — for exact tracking you'd need
                # intraday history, but this is a good approximation
                hit_target = 1 if price_now >= target_price * 0.95 else 0

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
                continue

        print(f"  ✓  Done checking {days}-day performance for {target_date}")

    con.close()

# ── SQLite helpers ─────────────────────────────────────────────────────────────
def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = get_db()

    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=5000")

    con.execute("""CREATE TABLE IF NOT EXISTS cache(
        date TEXT PRIMARY KEY, data TEXT, ts INTEGER)""")
    con.execute("""CREATE TABLE IF NOT EXISTS subscribers(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT UNIQUE, plan TEXT DEFAULT 'free',
        stripe_id TEXT, created_at INTEGER)""")

    # Daily snapshot of every stock's ranking and targets
    con.execute("""CREATE TABLE IF NOT EXISTS snapshots(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        ticker TEXT NOT NULL,
        rank INTEGER,
        current_price REAL,
        target_price REAL,
        upside_pct REAL,
        consensus TEXT,
        analyst_count INTEGER,
        UNIQUE(date, ticker))""")

    # Price lookups added later when we check performance
    con.execute("""CREATE TABLE IF NOT EXISTS performance(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        snapshot_date TEXT NOT NULL,
        ticker TEXT NOT NULL,
        days_later INTEGER NOT NULL,
        price_then REAL,
        price_now REAL,
        actual_return REAL,
        hit_target INTEGER,       -- 1 if price reached target, 0 if not
        checked_date TEXT,
        UNIQUE(snapshot_date, ticker, days_later))""")

    con.commit()
    con.close()

def get_cached():
    today = datetime.date.today().isoformat()
    con = get_db()
    row = con.execute("SELECT data FROM cache WHERE date=?", (today,)).fetchone()
    con.close()
    return json.loads(row[0]) if row else None

def save_cache(data):
    today = datetime.date.today().isoformat()
    con = get_db()
    con.execute("INSERT OR REPLACE INTO cache VALUES(?,?,?)",
                (today, json.dumps(data), int(time.time())))
    con.commit(); con.close()

def get_stocks():
    # Deprecated — use get_stocks_cached(). Data is now written only by
    # the offline generate.py job, never triggered from a request handler.
    return get_stocks_cached()

def get_db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=5000")
    return con

# get_stocks_cached — NEVER blocks. Returns [] if no data is available yet.
# Data is only written by the offline generate.py job or nightly_refresh thread.
def get_stocks_cached() -> list:
    today = datetime.date.today().isoformat()

    # 1. Fast path: in-memory cache is fresh
    with _cache_lock:
        if _cache["date"] == today and _cache["data"]:
            return _cache["data"]

    # 2. Try today's data from DB
    data = get_cached()

    # 3. If no data for today, serve the most recent available snapshot
    #    (covers: first boot, mid-refresh, or generate.py hasn't run yet today)
    if not data:
        data = get_any_cached()

    # 4. Still nothing — DB is empty (generate.py has never run)
    if not data:
        return []

    # Warm the in-memory cache
    with _cache_lock:
        _cache["data"] = data
        _cache["date"] = today

    return data

def get_any_cached():
    """Return the most recently cached data regardless of date. Used as
    a fallback while a generation run is in progress."""
    con = get_db()
    row = con.execute(
        "SELECT data FROM cache ORDER BY ts DESC LIMIT 1"
    ).fetchone()
    con.close()
    return json.loads(row[0]) if row else None

def render_changes_page(mode: str) -> str:
    titles = {
        "upgraded":   "Most Upgraded Stocks",
        "downgraded": "Most Downgraded Stocks",
        "both":       "Analyst Rating Changes",
    }
    descs = {
        "upgraded":   "Stocks where analyst consensus improved most over the selected period.",
        "downgraded": "Stocks where analyst consensus deteriorated most over the selected period.",
        "both":       "Stocks with the biggest analyst consensus upgrades and downgrades.",
    }
    title = titles[mode]
    desc  = descs[mode]
    yr    = datetime.date.today().year

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>{title} | StockUpside.io</title>
  <meta name="description" content="{desc}"/>
  <meta name="robots" content="index, follow"/>
  <link rel="canonical" href="https://stockupside.io/changes"/>
  <meta property="og:type"        content="website"/>
  <meta property="og:title"       content="Analyst Rating Changes | StockUpside.io"/>
  <meta property="og:description" content="Stocks with the biggest analyst consensus upgrades and downgrades. Updated daily."/>
  <meta property="og:url"         content="https://stockupside.io/changes"/>
  <meta property="og:image"       content="https://stockupside.io/og-image.png"/>
  <meta name="twitter:card"       content="summary_large_image"/>
  <meta name="twitter:title"      content="Analyst Rating Changes | StockUpside.io"/>
  <meta name="twitter:image"      content="https://stockupside.io/og-image.png"/>
  <link rel="stylesheet" href="/style.css"/>
  <style>
    .ch-wrap  {{ max-width:1100px;margin:0 auto;padding:32px 20px 64px; }}
    .ch-wrap h1 {{ font-family:var(--font-mono);font-size:22px;margin-bottom:6px; }}
    .ch-sub   {{ color:var(--text2);font-size:13px;margin-bottom:24px; }}
    .ch-tabs  {{ display:flex;gap:0;margin-bottom:24px;
                 border:1px solid var(--border);border-radius:6px;
                 overflow:hidden;width:fit-content; }}
    .ch-tab   {{ padding:8px 20px;font-family:var(--font-mono);font-size:11px;
                 font-weight:600;letter-spacing:.06em;cursor:pointer;
                 background:var(--bg2);color:var(--text2);
                 border:none;transition:all .15s; }}
    .ch-tab:hover  {{ background:var(--bg3);color:var(--text); }}
    .ch-tab.active {{ background:var(--accent);color:#000; }}
    .ch-period {{ display:flex;gap:8px;margin-bottom:28px;align-items:center; }}
    .ch-period span {{ font-family:var(--font-mono);font-size:11px;color:var(--text3); }}
    .period-btn {{ padding:5px 14px;font-family:var(--font-mono);font-size:11px;
                   background:var(--bg3);border:1px solid var(--border2);
                   color:var(--text2);border-radius:4px;cursor:pointer;
                   transition:all .15s; }}
    .period-btn:hover  {{ border-color:var(--accent);color:var(--text); }}
    .period-btn.active {{ background:var(--bg4);border-color:var(--accent);color:var(--accent); }}
    .ch-grid  {{ display:grid;grid-template-columns:1fr 1fr;gap:24px; }}
    @media(max-width:768px){{ .ch-grid{{ grid-template-columns:1fr; }} }}
    .ch-panel {{ background:var(--bg2);border:1px solid var(--border);border-radius:8px;overflow:hidden; }}
    .ch-panel-hdr {{ padding:14px 18px;border-bottom:1px solid var(--border);
                     display:flex;align-items:center;justify-content:space-between; }}
    .ch-panel-title {{ font-family:var(--font-mono);font-size:11px;
                        font-weight:700;letter-spacing:.1em; }}
    .ch-panel-count {{ font-family:var(--font-mono);font-size:11px;color:var(--text3); }}
    .ch-row   {{ display:flex;align-items:center;gap:12px;
                 padding:11px 18px;border-bottom:1px solid var(--border);
                 transition:background .1s;text-decoration:none; }}
    .ch-row:last-child {{ border-bottom:none; }}
    .ch-row:hover {{ background:var(--bg3); }}
    .ch-rank  {{ font-family:var(--font-mono);font-size:11px;
                 color:var(--text3);width:20px;flex-shrink:0; }}
    .ch-ticker{{ font-family:var(--font-mono);font-size:14px;
                 font-weight:700;color:var(--accent);width:52px;flex-shrink:0; }}
    .ch-change{{ flex:1;font-size:12px; }}
    .ch-from  {{ color:var(--text3); }}
    .ch-arrow {{ font-size:11px;margin:0 4px; }}
    .ch-to    {{ font-weight:600; }}
    .ch-upside{{ font-family:var(--font-mono);font-size:12px;
                 font-weight:600;width:54px;text-align:right;flex-shrink:0; }}
    .ch-delta {{ font-family:var(--font-mono);font-size:10px;
                 width:70px;text-align:right;flex-shrink:0;color:var(--text3); }}
    .no-data  {{ padding:40px;text-align:center;color:var(--text3);
                 font-family:var(--font-mono);font-size:12px; }}
    .ch-new-panel {{ background:var(--bg2);border:1px solid var(--border);
                     border-radius:8px;overflow:hidden;margin-top:24px; }}
    .as-of    {{ font-family:var(--font-mono);font-size:10px;
                 color:var(--text3);margin-bottom:20px; }}
  </style>
</head>
<body>
<header class="hdr">
  <div class="hdr-l">
    <a href="/" class="brand" style="text-decoration:none">
      <span class="brand-mark">▲</span>
      <div>
        <div class="brand-name">STOCKUPSIDE<span class="brand-io">.IO</span></div>
        <div class="brand-tag">Analyst Price Target Intelligence</div>
      </div>
    </a>
  </div>
  <div class="hdr-r">
    <a href="/stocks" style="font-family:var(--font-mono);font-size:11px;
       color:var(--text2);margin-right:16px">All Stocks</a>
    <a href="/" style="font-family:var(--font-mono);font-size:11px;
       color:var(--text2)">← Dashboard</a>
  </div>
</header>

<div class="ch-wrap">
  <h1>{title}</h1>
  <p class="ch-sub">{desc}</p>

  <div class="ch-period">
    <span>PERIOD:</span>
    <button class="period-btn active" data-days="7">7 days</button>
    <button class="period-btn" data-days="30">30 days</button>
    <button class="period-btn" data-days="90">90 days</button>
  </div>

  <div id="as-of" class="as-of"></div>
  <div id="ch-content"><div class="no-data">Loading...</div></div>
</div>

<footer class="ftr">
  <div>© {yr} StockUpside.io · Updated nightly · Not financial advice</div>
  <div class="ftr-r">
    <a href="/changes">Changes</a> ·
    <a href="/accuracy">Accuracy</a> ·
    <a href="/stocks">All Stocks</a>
  </div>
</footer>

<script>
const MODE = "{mode}";
let currentDays = 7;

const CONSENSUS_COLOR = {{
  "Strong Buy":"#00e676","Buy":"#69f0ae","Hold":"#ffd740",
  "Underperform":"#ff5252","Sell":"#d50000"
}};

function scoreLabel(delta) {{
  if (delta >= 2)  return "↑↑ Major upgrade";
  if (delta === 1) return "↑ Upgraded";
  if (delta === 0) return "→ More coverage";
  if (delta === -1)return "↓ Downgraded";
  return "↓↓ Major downgrade";
}}

function renderTable(items, type) {{
  if (!items.length) {{
    return '<div class="no-data">No ' + type + ' stocks in this period</div>';
  }}
  return items.map((s, i) => {{
    const upCol   = s.curr_upside >= 0 ? "#69f0ae" : "#f85149";
    const toColor = CONSENSUS_COLOR[s.curr_consensus] || "#aaa";
    const fromCol = CONSENSUS_COLOR[s.past_consensus] || "var(--text3)";
    const isNew   = s.count_delta > 0 && s.score_delta === 0;
    const deltaStr = s.score_delta !== 0
      ? scoreLabel(s.score_delta)
      : (s.count_delta > 0 ? `+${{s.count_delta}} analysts` : `${{s.count_delta}} analysts`);
    const deltaCol = type === "upgraded" ? "#00e676" : "#f85149";

    return `<a href="/stocks/${{s.ticker}}" class="ch-row">
      <span class="ch-rank">${{i+1}}</span>
      <span class="ch-ticker">${{s.ticker}}</span>
      <span class="ch-change">
        <span class="ch-from" style="color:${{fromCol}}">${{s.past_consensus || "—"}}</span>
        <span class="ch-arrow" style="color:${{deltaCol}}">→</span>
        <span class="ch-to"   style="color:${{toColor}}">${{s.curr_consensus}}</span>
      </span>
      <span class="ch-upside" style="color:${{upCol}}">
        ${{s.curr_upside >= 0 ? '+' : ''}}${{s.curr_upside}}%
      </span>
      <span class="ch-delta" style="color:${{deltaCol}}">${{deltaStr}}</span>
    </a>`;
  }}).join('');
}}

function renderNewCoverage(items) {{
  if (!items.length) return '';
  const rows = items.map((s, i) => {{
    const col = CONSENSUS_COLOR[s.consensus] || "#aaa";
    return `<a href="/stocks/${{s.ticker}}" class="ch-row">
      <span class="ch-rank">${{i+1}}</span>
      <span class="ch-ticker">${{s.ticker}}</span>
      <span class="ch-change" style="color:var(--text2)">New analyst coverage</span>
      <span class="ch-upside" style="color:#69f0ae">+${{s.curr_upside || s.upside_pct}}%</span>
      <span class="ch-delta"  style="color:${{col}}">${{s.consensus}}</span>
    </a>`;
  }}).join('');

  return `<div class="ch-new-panel">
    <div class="ch-panel-hdr">
      <span class="ch-panel-title" style="color:var(--blue)">★ NEW COVERAGE</span>
      <span class="ch-panel-count">${{items.length}} stocks</span>
    </div>
    ${{rows}}
  </div>`;
}}

function fetchAndRender(days) {{
  currentDays = days;
  fetch('/api/changes?days=' + days)
    .then(r => r.json())
    .then(data => {{
      const asOf = document.getElementById('as-of');
      if (asOf) asOf.textContent =
        'Comparing ' + data.as_of + ' vs ' + data.compared_to;

      let html = '';

      if (MODE === 'both') {{
        html = `<div class="ch-grid">
          <div class="ch-panel">
            <div class="ch-panel-hdr">
              <span class="ch-panel-title" style="color:#00e676">↑ MOST UPGRADED</span>
              <span class="ch-panel-count">${{data.upgraded.length}} stocks</span>
            </div>
            ${{renderTable(data.upgraded, 'upgraded')}}
          </div>
          <div class="ch-panel">
            <div class="ch-panel-hdr">
              <span class="ch-panel-title" style="color:#f85149">↓ MOST DOWNGRADED</span>
              <span class="ch-panel-count">${{data.downgraded.length}} stocks</span>
            </div>
            ${{renderTable(data.downgraded, 'downgraded')}}
          </div>
        </div>
        ${{renderNewCoverage(data.new_coverage)}}`;

      }} else if (MODE === 'upgraded') {{
        html = `<div class="ch-panel">
          <div class="ch-panel-hdr">
            <span class="ch-panel-title" style="color:#00e676">↑ MOST UPGRADED</span>
            <span class="ch-panel-count">${{data.upgraded.length}} stocks</span>
          </div>
          ${{renderTable(data.upgraded, 'upgraded')}}
        </div>`;

      }} else {{
        html = `<div class="ch-panel">
          <div class="ch-panel-hdr">
            <span class="ch-panel-title" style="color:#f85149">↓ MOST DOWNGRADED</span>
            <span class="ch-panel-count">${{data.downgraded.length}} stocks</span>
          </div>
          ${{renderTable(data.downgraded, 'downgraded')}}
        </div>`;
      }}

      document.getElementById('ch-content').innerHTML = html;
    }});
}}

// Period buttons
document.querySelectorAll('.period-btn').forEach(btn => {{
  btn.addEventListener('click', () => {{
    document.querySelectorAll('.period-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    fetchAndRender(parseInt(btn.dataset.days));
  }});
}});

// Initial load
fetchAndRender(7);
</script>
</body>
</html>"""

# ── CORS ───────────────────────────────────────────────────────────────────────
ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "http://localhost:5000")

@app.after_request
def cors(r):
    origin = request.headers.get("Origin", "")
    if origin == ALLOWED_ORIGIN:
        r.headers["Access-Control-Allow-Origin"] = origin
    r.headers["Access-Control-Allow-Headers"] = "Content-Type"
    r.headers["Vary"] = "Origin"
    return r

# ── Email ─────────────────────────────────────────────────────────────────────
# Set these environment variables to enable real email sending:
#   SMTP_HOST      e.g. smtp.gmail.com  or  smtp.sendgrid.net
#   SMTP_PORT      e.g. 587  (TLS) or 465 (SSL)
#   SMTP_USER      your SMTP login / API key username
#   SMTP_PASS      your SMTP password / API key
#   EMAIL_FROM     e.g. hello@stockupside.io
#
# If SMTP_HOST is not set, send_email() prints to stdout (dev mode).

_SMTP_HOST  = os.environ.get("SMTP_HOST",  "")
_SMTP_PORT  = int(os.environ.get("SMTP_PORT", "587"))
_SMTP_USER  = os.environ.get("SMTP_USER",  "")
_SMTP_PASS  = os.environ.get("SMTP_PASS",  "")
_EMAIL_FROM = os.environ.get("EMAIL_FROM", "hello@stockupside.io")
_SITE_URL   = os.environ.get("ALLOWED_ORIGIN", "https://stockupside.io")


def send_email(to: str, subject: str, html: str, text: str = "") -> bool:
    """Send a single email. Returns True on success, False on failure.
    Falls back to stdout if SMTP is not configured (dev mode)."""
    if not _SMTP_HOST:
        print(f"  [EMAIL DEV] To: {to} | Subject: {subject}")
        return True
    try:
        msg = email.mime.multipart.MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = _EMAIL_FROM
        msg["To"]      = to
        if text:
            msg.attach(email.mime.text.MIMEText(text, "plain"))
        msg.attach(email.mime.text.MIMEText(html, "html"))

        with smtplib.SMTP(_SMTP_HOST, _SMTP_PORT) as s:
            s.ehlo()
            s.starttls()
            s.login(_SMTP_USER, _SMTP_PASS)
            s.sendmail(_EMAIL_FROM, [to], msg.as_string())
        return True
    except Exception as e:
        print(f"  ⚠  Email failed to {to}: {e}")
        return False


def _unsubscribe_url(email_addr: str) -> str:
    token = hashlib.sha256(f"unsub:{email_addr}:{_SMTP_PASS}".encode()).hexdigest()[:24]
    return f"{_SITE_URL}/unsubscribe?email={email_addr}&token={token}"


def _unsubscribe_token_valid(email_addr: str, token: str) -> bool:
    expected = hashlib.sha256(f"unsub:{email_addr}:{_SMTP_PASS}".encode()).hexdigest()[:24]
    return token == expected


def welcome_email_html(email_addr: str) -> str:
    unsub = _unsubscribe_url(email_addr)
    return f"""<!DOCTYPE html>
<html><body style="font-family:monospace;background:#0d1117;color:#e6edf3;padding:32px;max-width:560px;margin:0 auto">
  <div style="font-size:28px;font-weight:700;margin-bottom:4px">▲ StockUpside.io</div>
  <div style="color:#00e676;font-size:12px;letter-spacing:.1em;margin-bottom:32px">ANALYST PRICE TARGET INTELLIGENCE</div>
  <p style="font-size:15px;line-height:1.7">You're on the list! Every week we'll send you the
  <strong>top 10 stocks by analyst upside</strong> — completely free.</p>
  <p style="font-size:13px;color:#8b949e;line-height:1.7">
    Data is sourced from analyst consensus price targets and updated daily.
    This is not financial advice.
  </p>
  <div style="margin-top:32px;padding-top:16px;border-top:1px solid #30363d;
              font-size:11px;color:#8b949e">
    <a href="{_SITE_URL}" style="color:#58a6ff">stockupside.io</a> ·
    <a href="{unsub}" style="color:#8b949e">Unsubscribe</a>
  </div>
</body></html>"""


def digest_email_html(stocks: list, email_addr: str) -> tuple[str, str]:
    """Returns (subject, html) for the weekly digest."""
    top10  = [s for s in stocks if not s.get("locked")][:10]
    today  = datetime.date.today().strftime("%B %d, %Y")
    unsub  = _unsubscribe_url(email_addr)

    rows_html = ""
    rows_txt  = ""
    for s in top10:
        color = "#00e676" if s["upside_pct"] >= 40 else "#69f0ae" if s["upside_pct"] >= 20 else "#ffd740"
        rows_html += f"""
        <tr>
          <td style="padding:10px 12px;font-weight:700;color:#58a6ff;
                     font-family:monospace">{s['ticker']}</td>
          <td style="padding:10px 12px;color:#8b949e;font-size:12px;
                     max-width:160px">{s['name']}</td>
          <td style="padding:10px 12px;font-family:monospace;font-weight:700;
                     color:{color}">+{s['upside_pct']}%</td>
          <td style="padding:10px 12px;color:#8b949e;font-size:12px">{s['consensus']}</td>
          <td style="padding:10px 12px;font-family:monospace;
                     color:#8b949e;font-size:11px">{s['analyst_count']} analysts</td>
        </tr>"""
        rows_txt += f"  {s['rank']:>2}. {s['ticker']:<6}  +{s['upside_pct']}%  {s['consensus']}\n"

    subject = f"▲ Top 10 Stock Picks — {today}"
    html = f"""<!DOCTYPE html>
<html><body style="font-family:monospace;background:#0d1117;color:#e6edf3;
                   padding:32px;max-width:620px;margin:0 auto">
  <div style="font-size:24px;font-weight:700;margin-bottom:4px">▲ StockUpside.io</div>
  <div style="color:#00e676;font-size:11px;letter-spacing:.1em;
              margin-bottom:8px">WEEKLY TOP 10</div>
  <div style="color:#8b949e;font-size:12px;margin-bottom:28px">{today}</div>

  <table style="width:100%;border-collapse:collapse;font-size:13px">
    <thead>
      <tr style="border-bottom:1px solid #30363d">
        <th style="padding:8px 12px;text-align:left;font-size:9px;
                   color:#8b949e;letter-spacing:.1em">TICKER</th>
        <th style="padding:8px 12px;text-align:left;font-size:9px;
                   color:#8b949e;letter-spacing:.1em">COMPANY</th>
        <th style="padding:8px 12px;text-align:left;font-size:9px;
                   color:#8b949e;letter-spacing:.1em">UPSIDE</th>
        <th style="padding:8px 12px;text-align:left;font-size:9px;
                   color:#8b949e;letter-spacing:.1em">CONSENSUS</th>
        <th style="padding:8px 12px;text-align:left;font-size:9px;
                   color:#8b949e;letter-spacing:.1em">ANALYSTS</th>
      </tr>
    </thead>
    <tbody>{rows_html}</tbody>
  </table>

  <div style="margin-top:28px">
    <a href="{_SITE_URL}" style="display:inline-block;padding:10px 20px;
       background:#00e676;color:#000;font-weight:700;font-family:monospace;
       font-size:12px;text-decoration:none;border-radius:4px">
      View Full Rankings →
    </a>
  </div>

  <div style="margin-top:32px;padding-top:16px;border-top:1px solid #30363d;
              font-size:11px;color:#8b949e">
    <a href="{_SITE_URL}" style="color:#58a6ff">stockupside.io</a> ·
    Not financial advice ·
    <a href="{unsub}" style="color:#8b949e">Unsubscribe</a>
  </div>
</body></html>"""

    text = f"▲ StockUpside.io — Top 10 Stock Picks — {today}\n\n{rows_txt}\nView full list: {_SITE_URL}\nUnsubscribe: {unsub}\n"
    return subject, html, text


# ── API Routes ─────────────────────────────────────────────────────────────────
@app.route("/api/stocks")
@limiter.limit("60 per hour")
def api_stocks():
    tier   = request.args.get("tier", "free")
    stocks = get_stocks_cached()
    today  = datetime.date.today()
    nxt    = (today + datetime.timedelta(days=1)).isoformat()

    # No data yet — generate.py hasn't run. Return a clear pending state.
    if not stocks:
        return jsonify({"stocks": [], "total": 0, "tier": tier,
                        "last_updated": None, "next_update": nxt,
                        "pending": True,
                        "message": "Data is being generated. Run generate.py and check back shortly."}), 503

    if tier == "pro":
        return jsonify({"stocks": stocks, "total": len(stocks),
                        "tier": "pro", "last_updated": today.isoformat(),
                        "next_update": nxt})

    free   = stocks[:10]
    teaser = [{"rank": s["rank"], "ticker": "???", "name": "Unlock Pro to reveal",
               "upside_pct": s["upside_pct"], "consensus": s["consensus"],
               "sector": s["sector"], "locked": True} for s in stocks[10:]]
    return jsonify({"stocks": free + teaser, "total": len(stocks),
                    "tier": "free", "last_updated": today.isoformat(),
                    "next_update": nxt})

@app.route("/api/stats")
@limiter.limit("60 per hour")
def api_stats():
    stocks = get_stocks_cached()

    # No data yet — generate.py hasn't run
    if not stocks:
        return jsonify({
            "total_stocks": 0, "avg_upside": 0, "top_upside": 0,
            "strong_buy_count": 0, "sectors": {},
            "last_updated": None, "days_old": 0, "freshness": "pending",
            "generating": is_generating(), "pending": True,
        })

    sectors: dict = {}
    for s in stocks:
        sec = s["sector"]
        if sec not in sectors: sectors[sec] = {"count": 0, "avg_upside": 0.0}
        sectors[sec]["count"] += 1
        sectors[sec]["avg_upside"] += s["upside_pct"]
    for sec in sectors:
        sectors[sec]["avg_upside"] = round(
            sectors[sec]["avg_upside"] / sectors[sec]["count"], 1)

    # Use the timestamp from the actual data, not today's date
    last_updated = stocks[0].get("last_updated", datetime.date.today().isoformat())

    # Surface how stale the data is
    try:
        data_date = datetime.date.fromisoformat(last_updated)
        days_old  = (datetime.date.today() - data_date).days
        freshness = "fresh" if days_old == 0 else "stale" if days_old >= 2 else "aging"
    except Exception:
        days_old  = 0
        freshness = "fresh"

    return jsonify({
        "total_stocks":     len(stocks),
        "avg_upside":       round(sum(s["upside_pct"] for s in stocks) / len(stocks), 1),
        "top_upside":       stocks[0]["upside_pct"],
        "strong_buy_count": sum(1 for s in stocks if s["consensus"] in ("Strong Buy", "Buy")),
        "sectors":          sectors,
        "last_updated":     last_updated,
        "days_old":         days_old,
        "freshness":        freshness,   # "fresh" | "aging" | "stale"
        "generating":       is_generating(),
    })

import stripe
stripe.api_key = os.environ.get("STRIPE_SECRET_KEY", "")

@app.route("/api/subscribe", methods=["POST", "OPTIONS"])
@limiter.limit("10 per hour")
def api_subscribe():
    if request.method == "OPTIONS":
        return Response(status=200)
    body  = request.get_json(force=True) or {}
    email = body.get("email", "").strip().lower()
    plan  = body.get("plan", "monthly")

    if not email or "@" not in email:
        return jsonify({"error": "Invalid email"}), 400

    stripe.api_key = os.environ.get("STRIPE_SECRET_KEY", "")
    price_id = (
        os.environ.get("STRIPE_PRICE_ANNUAL")
        if plan == "annual"
        else os.environ.get("STRIPE_PRICE_MONTHLY")
    )

    try:
        session = stripe.checkout.Session.create(
            payment_method_types=["card"],
            line_items=[{"price": price_id, "quantity": 1}],
            mode="subscription",
            customer_email=email,
            success_url="https://stockupside.io/success?session_id={CHECKOUT_SESSION_ID}",
            cancel_url="https://stockupside.io/",
        )
        return jsonify({"checkout_url": session.url})
    except Exception as e:
        print(f"  ⚠  Stripe error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/success")
def success_page():
    session_id = request.args.get("session_id", "")
    if not session_id:
        return redirect("/")

    try:
        session    = stripe.checkout.Session.retrieve(session_id)
        email      = session.customer_details.email.strip().lower()
        stripe_id  = session.customer

        # Generate a token tied to their email
        token = hashlib.sha256(
            f"{email}:{stripe_id}:{os.environ.get('STRIPE_SECRET_KEY','')}".encode()
        ).hexdigest()[:32]

        con = get_db()
        con.execute("""
            INSERT OR REPLACE INTO subscribers (email, plan, stripe_id, created_at)
            VALUES (?, 'pro', ?, ?)
        """, (email, stripe_id, int(time.time())))
        con.commit()
        con.close()

        # Pass the token to the frontend via URL fragment so it lands in localStorage
        return redirect(f"/?pro_token={token}&welcome=1")

    except Exception as e:
        print(f"  ⚠  Success page error: {e}")
        return redirect("/")

@app.route("/api/subscribe-free", methods=["POST", "OPTIONS"])
@limiter.limit("10 per hour")
def api_subscribe_free():
    if request.method == "OPTIONS":
        return Response(status=200)
    body       = request.get_json(force=True) or {}
    addr       = body.get("email", "").strip().lower()
    if not addr or "@" not in addr:
        return jsonify({"error": "Invalid email"}), 400
    is_new = False
    try:
        con = get_db()
        # INSERT OR IGNORE — don't downgrade an existing pro subscriber
        cur = con.execute("""
            INSERT OR IGNORE INTO subscribers (email, plan, created_at)
            VALUES (?, 'free', ?)
        """, (addr, int(time.time())))
        is_new = cur.rowcount > 0
        con.commit()
        con.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    # Send welcome email to genuinely new subscribers only
    if is_new:
        threading.Thread(
            target=send_email,
            args=(addr, "▲ You're on the StockUpside.io list!",
                  welcome_email_html(addr)),
            daemon=True,
        ).start()

    return jsonify({"success": True, "message": "You're on the list!"})


@app.route("/api/send-digest", methods=["POST"])
@limiter.limit("5 per day")
def api_send_digest():
    """Admin-only: send the weekly top-10 digest to all free subscribers."""
    secret = os.environ.get("ADMIN_SECRET", "")
    if not secret or request.headers.get("X-Admin-Key") != secret:
        return jsonify({"error": "Unauthorized"}), 401

    stocks = get_stocks_cached()
    if not stocks:
        return jsonify({"error": "No stock data available yet"}), 503

    con  = get_db()
    subs = con.execute(
        "SELECT email FROM subscribers WHERE plan='free'"
    ).fetchall()
    con.close()

    if not subs:
        return jsonify({"success": True, "sent": 0, "message": "No free subscribers yet"})

    def _send_all():
        sent = 0
        for (addr,) in subs:
            subj, html, text = digest_email_html(stocks, addr)
            if send_email(addr, subj, html, text):
                sent += 1
            time.sleep(0.1)   # gentle throttle
        print(f"  ✓  Digest sent to {sent}/{len(subs)} subscribers")

    threading.Thread(target=_send_all, daemon=True).start()
    return jsonify({"success": True, "queued": len(subs),
                    "message": f"Digest queued for {len(subs)} subscribers"})


@app.route("/unsubscribe")
def unsubscribe_page():
    addr  = request.args.get("email", "").strip().lower()
    token = request.args.get("token", "")
    yr    = datetime.date.today().year

    if not addr or not _unsubscribe_token_valid(addr, token):
        return Response(f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"/><title>Invalid Link | StockUpside.io</title>
<link rel="stylesheet" href="/style.css"/></head>
<body><div style="max-width:480px;margin:80px auto;padding:0 20px;text-align:center">
  <div style="font-family:var(--font-mono);font-size:24px;font-weight:700;
              margin-bottom:16px">▲ StockUpside.io</div>
  <p style="color:var(--text2)">This unsubscribe link is invalid or has expired.</p>
  <a href="/" style="color:var(--accent)">← Back to dashboard</a>
</div></body></html>""", mimetype="text/html"), 400

    # Remove from subscribers
    con = get_db()
    con.execute("DELETE FROM subscribers WHERE email=?", (addr,))
    con.commit()
    con.close()

    return Response(f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"/>
<title>Unsubscribed | StockUpside.io</title>
<link rel="stylesheet" href="/style.css"/>
</head>
<body>
<header class="hdr">
  <div class="hdr-l">
    <a href="/" class="brand" style="text-decoration:none">
      <span class="brand-mark">▲</span>
      <div><div class="brand-name">STOCKUPSIDE<span class="brand-io">.IO</span></div></div>
    </a>
  </div>
</header>
<div style="max-width:480px;margin:80px auto;padding:0 20px;text-align:center">
  <div style="font-family:var(--font-mono);font-size:18px;margin-bottom:16px;
              color:var(--accent)">✓ Unsubscribed</div>
  <p style="color:var(--text2)">
    <strong style="color:var(--text)">{addr}</strong> has been removed from
    all StockUpside.io mailing lists.
  </p>
  <a href="/" style="color:var(--accent);font-family:var(--font-mono);
     font-size:12px">← Back to dashboard</a>
</div>
<footer class="ftr"><div>© {yr} StockUpside.io</div></footer>
</body></html>""", mimetype="text/html")

@app.route("/api/verify-token", methods=["POST", "OPTIONS"])
@limiter.limit("30 per hour")
def api_verify():
    if request.method == "OPTIONS":
        return Response(status=200)
    body  = request.get_json(force=True) or {}
    token = body.get("token", "").strip()
    if not token:
        return jsonify({"valid": False, "plan": "free"})

    # Recompute expected token for every pro subscriber and compare
    con  = get_db()
    subs = con.execute(
        "SELECT email, stripe_id FROM subscribers WHERE plan='pro'"
    ).fetchall()
    con.close()

    stripe_key = os.environ.get("STRIPE_SECRET_KEY", "")
    for email, stripe_id in subs:
        expected = hashlib.sha256(
            f"{email}:{stripe_id}:{stripe_key}".encode()
        ).hexdigest()[:32]
        if token == expected:
            return jsonify({"valid": True, "plan": "pro"})

    return jsonify({"valid": False, "plan": "free"})

@app.route("/api/refresh", methods=["POST"])
@limiter.limit("5 per hour")
def api_refresh():
    """Admin-only: kick off a data refresh without waiting for 01:00.
    Spawns generate.py in a background thread so the HTTP response is instant."""
    import subprocess, sys
    secret = os.environ.get("ADMIN_SECRET", "")
    if not secret or request.headers.get("X-Admin-Key") != secret:
        return jsonify({"error": "Unauthorized"}), 401
    if is_generating():
        return jsonify({"error": "Refresh already in progress"}), 429

    generate_script = os.path.join(BASE_DIR, "server", "generate.py")

    def _run():
        set_generating(True)
        try:
            result = subprocess.run(
                [sys.executable, generate_script],
                timeout=4 * 3600,
                capture_output=False,
            )
            if result.returncode == 0:
                invalidate_memory_cache()
                print("  ✓  Manual refresh complete.")
            else:
                print(f"  ⚠  generate.py exited with code {result.returncode}")
        except Exception as e:
            print(f"  ⚠  Manual refresh failed: {e}")
        finally:
            set_generating(False)

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"success": True, "message": "Refresh started in background.",
                    "last_updated": datetime.date.today().isoformat()})

@app.route("/stocks/<ticker>")
def stock_page(ticker):
    ticker = ticker.upper()
    stocks = get_stocks_cached()
    stock  = next((s for s in stocks if s["ticker"] == ticker), None)

    if not stock:
        return Response(render_404_page(f"/stocks/{ticker}"), mimetype="text/html"), 404

    # Build fully server-rendered HTML for SEO
    html = render_stock_page(stock)
    return Response(html, mimetype="text/html")

@app.route("/stocks")
def stocks_index():
    stocks = get_stocks_cached()
    html   = render_stocks_index(stocks)
    return Response(html, mimetype="text/html")

@app.route("/accuracy")
def accuracy_page():
    return Response(render_accuracy_page(), mimetype="text/html")

@app.route("/privacy")
def privacy_page():
    return Response(render_privacy_page(), mimetype="text/html")

@app.route("/disclaimer")
def disclaimer_page():
    return Response(render_disclaimer_page(), mimetype="text/html")

def render_accuracy_page() -> str:
    yr = datetime.date.today().year
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Analyst Accuracy Tracker | StockUpside.io</title>
  <meta name="description" content="Track how accurate Wall Street analyst price targets have been over 30, 60, and 90 days."/>
  <meta property="og:type"        content="website"/>
  <meta property="og:title"       content="Analyst Accuracy Tracker | StockUpside.io"/>
  <meta property="og:description" content="How often do Wall Street price targets actually come true? Track analyst accuracy at 30, 60, and 90 days."/>
  <meta property="og:url"         content="https://stockupside.io/accuracy"/>
  <meta property="og:image"       content="https://stockupside.io/og-image.png"/>
  <meta name="twitter:card"       content="summary_large_image"/>
  <meta name="twitter:title"      content="Analyst Accuracy Tracker | StockUpside.io"/>
  <meta name="twitter:image"      content="https://stockupside.io/og-image.png"/>
  <link rel="stylesheet" href="/style.css"/>
  <style>
    .ac-wrap {{ max-width:1000px;margin:0 auto;padding:32px 20px 64px; }}
    .ac-wrap h1 {{ font-family:var(--font-mono);font-size:22px;margin-bottom:6px; }}
    .ac-sub {{ color:var(--text2);font-size:13px;margin-bottom:32px; }}
    .ac-grid {{ display:grid;grid-template-columns:repeat(3,1fr);gap:16px;margin-bottom:32px; }}
    @media(max-width:600px){{ .ac-grid{{grid-template-columns:1fr;}} }}
    .ac-card {{ background:var(--bg2);border:1px solid var(--border);border-radius:8px;padding:20px; }}
    .ac-card-title {{ font-family:var(--font-mono);font-size:9px;color:var(--text3);
                      letter-spacing:.12em;margin-bottom:16px; }}
    .ac-big {{ font-family:var(--font-mono);font-size:40px;font-weight:700;
               color:var(--green-b);line-height:1;margin-bottom:4px; }}
    .ac-big-sub {{ font-size:12px;color:var(--text2);margin-bottom:16px; }}
    .ac-stat {{ display:flex;justify-content:space-between;padding:7px 0;
                border-bottom:1px solid var(--border);font-size:12px; }}
    .ac-stat:last-child {{ border-bottom:none; }}
    .ac-stat-l {{ color:var(--text2); }}
    .ac-stat-v {{ font-family:var(--font-mono);font-weight:600;color:var(--text); }}
    .ac-section {{ margin-bottom:32px; }}
    .ac-section h2 {{ font-family:var(--font-mono);font-size:11px;color:var(--text3);
                      letter-spacing:.12em;margin-bottom:16px; }}
    .ac-table {{ width:100%;border-collapse:collapse;font-family:var(--font-mono);font-size:12px; }}
    .ac-table th {{ padding:8px 12px;text-align:left;font-size:9px;color:var(--text3);
                    letter-spacing:.1em;background:var(--bg2);border-bottom:1px solid var(--border); }}
    .ac-table td {{ padding:10px 12px;border-bottom:1px solid var(--border); }}
    .no-data {{ color:var(--text3);font-family:var(--font-mono);font-size:13px;
                padding:48px;text-align:center;background:var(--bg2);
                border:1px solid var(--border);border-radius:8px; }}
  </style>
</head>
<body>
<header class="hdr">
  <div class="hdr-l">
    <a href="/" class="brand" style="text-decoration:none">
      <span class="brand-mark">▲</span>
      <div><div class="brand-name">STOCKUPSIDE<span class="brand-io">.IO</span></div>
        <div class="brand-tag">Analyst Price Target Intelligence</div></div>
    </a>
  </div>
  <div class="hdr-r">
    <a href="/stocks" style="font-family:var(--font-mono);font-size:11px;
       color:var(--text2);margin-right:16px">All Stocks</a>
    <a href="/" style="font-family:var(--font-mono);font-size:11px;color:var(--text2)">← Dashboard</a>
  </div>
</header>

<div class="ac-wrap">
  <h1>Analyst Accuracy Tracker</h1>
  <p class="ac-sub">
    How often do Wall Street price targets actually come true?
    We track every prediction in our database and measure real outcomes at 30, 60, and 90 days.
  </p>

  <div id="ac-content">
    <div class="no-data">
      ⏳ Accuracy data builds up over time. Check back in 30 days once we have
      enough historical snapshots to measure. Data collection started today.
    </div>
  </div>
</div>

<footer class="ftr">
  <div>© {yr} StockUpside.io · Accuracy data updates nightly · Not financial advice</div>
</footer>

<script>
fetch('/api/accuracy')
  .then(r => r.json())
  .then(data => {{
    const el = document.getElementById('ac-content');
    const cp = data.checkpoints;
    const days = ['30','60','90'];
    const hasData = days.some(d => cp[d] && cp[d].total > 0);

    if (!hasData) {{
        const started = data.collection_started;
        if (started) {{
            const d    = new Date(started + "T00:00:00");
            const fmt  = d.toLocaleDateString("en-US", {{ month: "long", day: "numeric", year: "numeric" }});
            const due  = new Date(d); due.setDate(due.getDate() + 30);
            const dueFmt = due.toLocaleDateString("en-US", {{ month: "long", day: "numeric" }});
            el.innerHTML = `<div class="no-data">
                ⏳ Accuracy data builds up over time. Data collection started <strong>${{fmt}}</strong> —
                check back around <strong>${{dueFmt}}</strong> once we have enough historical
                snapshots to measure.
            </div>`;
        }}
        return;
    }}

    // ── Checkpoint cards ──
    let cards = '<div class="ac-grid">';
    for (const d of days) {{
      const c = cp[d];
      if (!c || c.total === 0) {{
        cards += `<div class="ac-card">
          <div class="ac-card-title">${{d}}-DAY ACCURACY</div>
          <div style="color:var(--text3);font-size:12px">No data yet</div>
        </div>`;
        continue;
      }}
      const col = c.hit_rate >= 60 ? '#00e676' : c.hit_rate >= 40 ? '#ffd740' : '#f85149';
      cards += `<div class="ac-card">
        <div class="ac-card-title">${{d}}-DAY ACCURACY</div>
        <div class="ac-big" style="color:${{col}}">${{c.hit_rate}}%</div>
        <div class="ac-big-sub">of targets reached within ${{d}} days</div>
        <div class="ac-stat"><span class="ac-stat-l">Stocks tracked</span>
          <span class="ac-stat-v">${{c.total}}</span></div>
        <div class="ac-stat"><span class="ac-stat-l">Targets hit</span>
          <span class="ac-stat-v">${{c.hits}}</span></div>
        <div class="ac-stat"><span class="ac-stat-l">Avg return (all)</span>
          <span class="ac-stat-v" style="color:${{c.avg_return>=0?'#69f0ae':'#f85149'}}">
            ${{c.avg_return>=0?'+':''}}${{c.avg_return}}%</span></div>
        <div class="ac-stat"><span class="ac-stat-l">Avg return (hits)</span>
          <span class="ac-stat-v" style="color:#69f0ae">
            +${{c.avg_return_hits}}%</span></div>
      </div>`;
    }}
    cards += '</div>';

    // ── By consensus ──
    let byConsensus = '';
    if (data.by_consensus.length > 0) {{
      byConsensus = `<div class="ac-section">
        <h2>ACCURACY BY CONSENSUS RATING (90-DAY)</h2>
        <table class="ac-table">
          <thead><tr>
            <th>CONSENSUS</th><th>STOCKS</th><th>HIT RATE</th><th>AVG RETURN</th>
          </tr></thead>
          <tbody>
            ${{data.by_consensus.map(r => `<tr>
              <td style="color:${{ratingColor(r.consensus)}}">${{r.consensus}}</td>
              <td>${{r.total}}</td>
              <td>${{r.hit_rate}}%</td>
              <td style="color:${{r.avg_return>=0?'#69f0ae':'#f85149'}}">
                ${{r.avg_return>=0?'+':''}}${{r.avg_return}}%</td>
            </tr>`).join('')}}
          </tbody>
        </table>
      </div>`;
    }}

    // ── By sector ──
    let bySector = '';
    if (data.by_sector.length > 0) {{
      bySector = `<div class="ac-section">
        <h2>ACCURACY BY SECTOR (90-DAY)</h2>
        <table class="ac-table">
          <thead><tr>
            <th>SECTOR</th><th>STOCKS</th><th>HIT RATE</th><th>AVG RETURN</th>
          </tr></thead>
          <tbody>
            ${{data.by_sector.map(r => `<tr>
              <td>${{r.sector}}</td>
              <td>${{r.total}}</td>
              <td>${{r.hit_rate}}%</td>
              <td style="color:${{r.avg_return>=0?'#69f0ae':'#f85149'}}">
                ${{r.avg_return>=0?'+':''}}${{r.avg_return}}%</td>
            </tr>`).join('')}}
          </tbody>
        </table>
      </div>`;
    }}

    el.innerHTML = cards + byConsensus + bySector;
  }});

function ratingColor(c) {{
  return {{"Strong Buy":"#00e676","Buy":"#69f0ae","Hold":"#ffd740",
           "Underperform":"#ff5252","Sell":"#d50000"}}[c] || "#aaa";
}}
</script>
</body>
</html>"""

def render_404_page(path: str = "") -> str:
    yr = datetime.date.today().year
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>404 — Page Not Found | StockUpside.io</title>
  <meta name="robots" content="noindex"/>
  <link rel="stylesheet" href="/style.css"/>
  <style>
    .e404-wrap {{
      max-width:480px; margin:100px auto; padding:0 24px; text-align:center;
    }}
    .e404-code {{
      font-family:var(--font-mono); font-size:72px; font-weight:700;
      color:var(--accent); line-height:1; margin-bottom:8px;
    }}
    .e404-msg {{
      font-family:var(--font-mono); font-size:14px; color:var(--text2);
      margin-bottom:32px; line-height:1.6;
    }}
    .e404-path {{
      font-family:var(--font-mono); font-size:12px; color:var(--text3);
      background:var(--bg2); border:1px solid var(--border);
      border-radius:4px; padding:6px 12px; display:inline-block;
      margin-bottom:32px;
    }}
    .e404-links {{ display:flex; gap:12px; justify-content:center; flex-wrap:wrap; }}
    .e404-btn {{
      padding:10px 20px; font-family:var(--font-mono); font-size:12px;
      font-weight:700; border-radius:4px; text-decoration:none;
      transition:opacity .15s;
    }}
    .e404-btn:hover {{ opacity:.8; }}
    .e404-btn-primary {{ background:var(--accent); color:#000; }}
    .e404-btn-secondary {{ background:var(--bg2); color:var(--text2);
                           border:1px solid var(--border); }}
  </style>
</head>
<body>
<header class="hdr">
  <div class="hdr-l">
    <a href="/" class="brand" style="text-decoration:none">
      <span class="brand-mark">▲</span>
      <div><div class="brand-name">STOCKUPSIDE<span class="brand-io">.IO</span></div>
        <div class="brand-tag">Analyst Price Target Intelligence</div></div>
    </a>
  </div>
</header>

<div class="e404-wrap">
  <div class="e404-code">404</div>
  <div class="e404-msg">Page not found.<br>
    This stock may have been delisted, or the URL might be wrong.</div>
  {f'<div class="e404-path">{path}</div>' if path else ""}
  <div class="e404-links">
    <a href="/" class="e404-btn e404-btn-primary">← Dashboard</a>
    <a href="/stocks" class="e404-btn e404-btn-secondary">All Stocks</a>
  </div>
</div>

<footer class="ftr">
  <div>© {yr} StockUpside.io · <a href="/disclaimer" style="color:var(--text3)">Not financial advice</a></div>
</footer>
</body>
</html>"""


@app.errorhandler(404)
def not_found(e):
    path = request.path
    return Response(render_404_page(path), mimetype="text/html"), 404


@app.errorhandler(429)
def rate_limited(e):
    return jsonify({"error": "Too many requests. Please slow down.", "retry_after": 60}), 429


def render_privacy_page() -> str:
    yr = datetime.date.today().year
    updated = "2026-06-07"  # update this when you make material changes
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Privacy Policy | StockUpside.io</title>
  <meta name="description" content="Privacy policy for StockUpside.io — how we collect, use, and protect your data."/>
  <meta name="robots" content="index, follow"/>
  <link rel="canonical" href="https://stockupside.io/privacy"/>
  <link rel="stylesheet" href="/style.css"/>
  <style>
    .legal-wrap {{
      max-width: 760px; margin: 0 auto; padding: 48px 24px 80px;
    }}
    .legal-wrap h1 {{
      font-family: var(--font-mono); font-size: 24px;
      font-weight: 700; margin-bottom: 6px; color: var(--text);
    }}
    .legal-meta {{
      font-family: var(--font-mono); font-size: 11px;
      color: var(--text3); margin-bottom: 40px; letter-spacing: .04em;
    }}
    .legal-wrap h2 {{
      font-family: var(--font-mono); font-size: 12px; font-weight: 700;
      letter-spacing: .1em; color: var(--accent);
      margin: 36px 0 12px; text-transform: uppercase;
    }}
    .legal-wrap p {{
      font-size: 13px; color: var(--text2); line-height: 1.8;
      margin-bottom: 14px;
    }}
    .legal-wrap ul {{
      margin: 0 0 14px 20px; display: flex; flex-direction: column; gap: 6px;
    }}
    .legal-wrap li {{
      font-size: 13px; color: var(--text2); line-height: 1.7;
    }}
    .legal-wrap a {{ color: var(--accent); }}
    .legal-divider {{
      border: none; border-top: 1px solid var(--border); margin: 40px 0;
    }}
  </style>
</head>
<body>
<header class="hdr">
  <div class="hdr-l">
    <a href="/" class="brand" style="text-decoration:none">
      <span class="brand-mark">▲</span>
      <div>
        <div class="brand-name">STOCKUPSIDE<span class="brand-io">.IO</span></div>
        <div class="brand-tag">Analyst Price Target Intelligence</div>
      </div>
    </a>
  </div>
  <div class="hdr-r">
    <a href="/" class="hdr-link">← Dashboard</a>
  </div>
</header>

<div class="legal-wrap">
  <h1>Privacy Policy</h1>
  <p class="legal-meta">Last updated: {updated} &nbsp;·&nbsp; Effective immediately</p>

  <p>StockUpside.io ("we", "us", or "our") operates the website at stockupside.io. This policy
  explains what information we collect, how we use it, and your rights regarding that
  information. By using StockUpside.io you agree to the practices described below.</p>

  <h2>1. Information We Collect</h2>
  <p>We collect only what is necessary to provide the service:</p>
  <ul>
    <li><strong>Email address</strong> — when you sign up for our free weekly digest or
    purchase a Pro subscription. We use this to send you the content you requested and,
    for Pro subscribers, to manage your account.</li>
    <li><strong>Payment information</strong> — Pro subscriptions are processed by Stripe.
    We never see or store your full card number. Stripe's privacy policy governs how
    payment data is handled: <a href="https://stripe.com/privacy" target="_blank">stripe.com/privacy</a>.</li>
    <li><strong>Usage data</strong> — standard server logs (IP address, browser type,
    pages visited, time of visit). We use these to diagnose errors and understand
    aggregate traffic patterns. Logs are retained for 30 days.</li>
  </ul>

  <h2>2. How We Use Your Information</h2>
  <ul>
    <li>To send the weekly top-10 stock picks digest (free list subscribers).</li>
    <li>To manage your Pro subscription and provide access to gated content.</li>
    <li>To diagnose technical problems and improve the service.</li>
    <li>We do <strong>not</strong> sell your email address or personal data to any
    third party, ever.</li>
    <li>We do <strong>not</strong> use your data to serve advertising.</li>
  </ul>

  <h2>3. Email Communications</h2>
  <p>If you join the free list, you will receive a weekly email containing the current
  top 10 stocks by analyst upside. You can unsubscribe at any time by clicking the
  unsubscribe link in any email, or by contacting us at
  <a href="mailto:hello@stockupside.io">hello@stockupside.io</a>.</p>
  <p>Pro subscribers may receive transactional emails (receipts, renewal notices,
  service updates). These are necessary for the service and cannot be opted out of
  while your subscription is active.</p>

  <h2>4. Data Storage and Security</h2>
  <p>Email addresses and subscription records are stored in a secured database on our
  hosting provider's infrastructure. We use industry-standard practices to protect
  this data, including encrypted connections (HTTPS) and access controls.</p>
  <p>No method of transmission or storage is 100% secure. While we take reasonable
  steps to protect your information, we cannot guarantee absolute security.</p>

  <h2>5. Cookies and Local Storage</h2>
  <p>StockUpside.io uses browser localStorage (not cookies) to remember whether you
  have a Pro token and whether you have subscribed to the free list. This data lives
  entirely in your browser, is never transmitted to our servers except as part of
  normal API calls, and can be cleared at any time by clearing your browser's site
  data.</p>
  <p>We do not use third-party tracking cookies or analytics cookies.</p>

  <h2>6. Third-Party Services</h2>
  <ul>
    <li><strong>Stripe</strong> — payment processing for Pro subscriptions.</li>
    <li><strong>Yahoo Finance (via yfinance)</strong> — source of analyst price target
    and consensus data. We do not share your personal data with Yahoo Finance.</li>
    <li><strong>SEC EDGAR</strong> — source of the stock ticker universe. Public
    government data; no personal data is involved.</li>
  </ul>

  <h2>7. Your Rights</h2>
  <p>You have the right to:</p>
  <ul>
    <li>Request a copy of the personal data we hold about you.</li>
    <li>Request deletion of your email address and subscription record.</li>
    <li>Unsubscribe from all communications at any time.</li>
  </ul>
  <p>To exercise any of these rights, email
  <a href="mailto:hello@stockupside.io">hello@stockupside.io</a> and we will respond
  within 5 business days.</p>

  <h2>8. Children's Privacy</h2>
  <p>StockUpside.io is not directed at children under 13. We do not knowingly collect
  personal information from children. If you believe a child has provided us with
  their email address, please contact us and we will delete it promptly.</p>

  <h2>9. Changes to This Policy</h2>
  <p>We may update this policy from time to time. When we do, we will update the
  "Last updated" date at the top of this page. Continued use of the service after
  changes are posted constitutes acceptance of the revised policy.</p>

  <h2>10. Contact</h2>
  <p>Questions about this policy? Email us at
  <a href="mailto:hello@stockupside.io">hello@stockupside.io</a>.</p>

  <hr class="legal-divider"/>
  <p style="font-size:11px;color:var(--text3);font-family:var(--font-mono)">
    See also: <a href="/disclaimer">Financial Disclaimer</a> &nbsp;·&nbsp;
    <a href="/">← Back to Dashboard</a>
  </p>
</div>

<footer class="ftr">
  <div>© {yr} StockUpside.io · <a href="/disclaimer" style="color:var(--text3)">Not financial advice</a></div>
  <div class="ftr-r">
    <a href="/privacy">Privacy</a> ·
    <a href="/disclaimer">Disclaimer</a> ·
    <a href="mailto:hello@stockupside.io">Contact</a>
  </div>
</footer>
</body>
</html>"""


def render_disclaimer_page() -> str:
    yr = datetime.date.today().year
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Financial Disclaimer | StockUpside.io</title>
  <meta name="description" content="Important information about the nature of analyst data on StockUpside.io. This is not financial advice."/>
  <meta name="robots" content="index, follow"/>
  <link rel="canonical" href="https://stockupside.io/disclaimer"/>
  <link rel="stylesheet" href="/style.css"/>
  <style>
    .legal-wrap {{
      max-width: 760px; margin: 0 auto; padding: 48px 24px 80px;
    }}
    .legal-wrap h1 {{
      font-family: var(--font-mono); font-size: 24px;
      font-weight: 700; margin-bottom: 6px; color: var(--text);
    }}
    .legal-meta {{
      font-family: var(--font-mono); font-size: 11px;
      color: var(--text3); margin-bottom: 40px; letter-spacing: .04em;
    }}
    .legal-wrap h2 {{
      font-family: var(--font-mono); font-size: 12px; font-weight: 700;
      letter-spacing: .1em; color: var(--accent);
      margin: 36px 0 12px; text-transform: uppercase;
    }}
    .legal-wrap p {{
      font-size: 13px; color: var(--text2); line-height: 1.8;
      margin-bottom: 14px;
    }}
    .legal-wrap ul {{
      margin: 0 0 14px 20px; display: flex; flex-direction: column; gap: 6px;
    }}
    .legal-wrap li {{
      font-size: 13px; color: var(--text2); line-height: 1.7;
    }}
    .legal-wrap a {{ color: var(--accent); }}
    .legal-divider {{
      border: none; border-top: 1px solid var(--border); margin: 40px 0;
    }}
    .disc-callout {{
      background: rgba(240,180,41,.06);
      border: 1px solid rgba(240,180,41,.25);
      border-radius: 8px;
      padding: 20px 24px;
      margin-bottom: 32px;
    }}
    .disc-callout p {{
      margin-bottom: 0; color: var(--text);
      font-size: 13px; line-height: 1.7;
    }}
    .disc-callout strong {{ color: var(--accent); }}
  </style>
</head>
<body>
<header class="hdr">
  <div class="hdr-l">
    <a href="/" class="brand" style="text-decoration:none">
      <span class="brand-mark">▲</span>
      <div>
        <div class="brand-name">STOCKUPSIDE<span class="brand-io">.IO</span></div>
        <div class="brand-tag">Analyst Price Target Intelligence</div>
      </div>
    </a>
  </div>
  <div class="hdr-r">
    <a href="/" class="hdr-link">← Dashboard</a>
  </div>
</header>

<div class="legal-wrap">
  <h1>Financial Disclaimer</h1>
  <p class="legal-meta">Please read this before making any investment decisions.</p>

  <div class="disc-callout">
    <p><strong>StockUpside.io is not a financial advisor, broker, or investment service.
    Nothing on this website constitutes financial advice, investment advice, trading
    advice, or any other form of professional advice.</strong> All content is provided
    for informational and educational purposes only.</p>
  </div>

  <h2>What StockUpside.io Is</h2>
  <p>StockUpside.io is a data aggregation and display tool. We collect publicly available
  analyst consensus price targets from third-party sources and present them in a ranked,
  filterable format. We do not generate, verify, or endorse any of the analyst estimates
  shown on this site.</p>
  <p>The "upside potential" figures shown are simple arithmetic — the percentage difference
  between an analyst's consensus price target and the current market price. They are not
  predictions, forecasts, or recommendations.</p>

  <h2>What StockUpside.io Is Not</h2>
  <ul>
    <li>We are <strong>not</strong> registered as an investment adviser with the SEC or
    any other regulatory body.</li>
    <li>We are <strong>not</strong> a broker-dealer.</li>
    <li>We do <strong>not</strong> manage money or assets on behalf of anyone.</li>
    <li>We do <strong>not</strong> provide personalised investment recommendations.</li>
  </ul>

  <h2>About Analyst Price Targets</h2>
  <p>Analyst price targets are estimates produced by third-party financial analysts at
  investment banks and research firms. They represent an analyst's opinion of what a
  stock may be worth over a given time horizon, typically 12 months. They are not
  guarantees of future performance.</p>
  <p>Analyst estimates are frequently wrong. They are subject to conflicts of interest,
  model assumptions, and market conditions that can change rapidly. Our own accuracy
  tracker exists to make this transparency explicit — past accuracy data on this site
  shows that analyst targets are missed as often as they are hit.</p>
  <p>A high ranking on StockUpside.io means only that a stock has a large gap between
  its current price and the average analyst price target. It does not mean the stock
  is a good investment, undervalued, or likely to rise.</p>

  <h2>Data Accuracy and Timeliness</h2>
  <p>Data on this site is sourced from Yahoo Finance via automated collection and is
  updated daily. We make reasonable efforts to ensure accuracy, but we cannot guarantee
  that all data is complete, current, or free of errors. Data may be delayed, incorrect,
  or missing for some securities.</p>
  <p><strong>Do not make investment decisions based solely on data from this site.</strong>
  Always verify figures through authoritative sources such as SEC filings, exchange data
  feeds, or your broker's platform before acting.</p>

  <h2>Risk Warning</h2>
  <p>Investing in stocks and securities involves significant risk, including the possible
  loss of the entire amount invested. Past performance of any security, analyst, or
  strategy is not indicative of future results. Market prices can and do fall
  substantially, sometimes to zero.</p>
  <p>Before making any investment decision you should consider whether it is appropriate
  for your personal financial situation, risk tolerance, and investment objectives. If
  you are unsure, consult a licensed financial adviser in your jurisdiction.</p>

  <h2>No Liability</h2>
  <p>To the fullest extent permitted by law, StockUpside.io, its operators, and
  contributors accept no liability for any loss or damage — financial or otherwise —
  arising directly or indirectly from your use of, or reliance on, any information
  on this website.</p>

  <h2>Third-Party Content</h2>
  <p>This site displays data sourced from Yahoo Finance, SEC EDGAR, and third-party
  analyst reports. We are not responsible for the accuracy or completeness of
  third-party data. Links to external sites are provided for convenience only and
  do not constitute endorsement.</p>

  <hr class="legal-divider"/>
  <p style="font-size:11px;color:var(--text3);font-family:var(--font-mono)">
    See also: <a href="/privacy">Privacy Policy</a> &nbsp;·&nbsp;
    <a href="/">← Back to Dashboard</a>
  </p>
</div>

<footer class="ftr">
  <div>© {yr} StockUpside.io · Not financial advice</div>
  <div class="ftr-r">
    <a href="/privacy">Privacy</a> ·
    <a href="/disclaimer">Disclaimer</a> ·
    <a href="mailto:hello@stockupside.io">Contact</a>
  </div>
</footer>
</body>
</html>"""

@app.route("/api/accuracy")
@limiter.limit("60 per hour")
def api_accuracy():
    con = get_db()   # use get_db(), not sqlite3.connect directly

    # Overall hit rate per checkpoint
    checkpoints = {}
    for days in CHECKPOINTS:
        row = con.execute("""
            SELECT
                COUNT(*) as total,
                SUM(hit_target) as hits,
                AVG(actual_return) as avg_return,
                AVG(CASE WHEN hit_target=1 THEN actual_return END) as avg_return_hits,
                AVG(CASE WHEN hit_target=0 THEN actual_return END) as avg_return_misses
            FROM performance WHERE days_later = ?
        """, (days,)).fetchone()

        if row and row[0] > 0:
            checkpoints[str(days)] = {
                "total":             row[0],
                "hits":              row[1] or 0,
                "hit_rate":          round((row[1] or 0) / row[0] * 100, 1),
                "avg_return":        round(row[2] or 0, 2),
                "avg_return_hits":   round(row[3] or 0, 2),
                "avg_return_misses": round(row[4] or 0, 2),
            }

    # Best and worst performing predictions
    top = con.execute("""
        SELECT ticker, snapshot_date, days_later, actual_return, hit_target
        FROM performance ORDER BY actual_return DESC LIMIT 10
    """).fetchall()

    worst = con.execute("""
        SELECT ticker, snapshot_date, days_later, actual_return, hit_target
        FROM performance ORDER BY actual_return ASC LIMIT 10
    """).fetchall()

    # Accuracy by consensus rating — join on snapshots.date (not snapshot_date)
    by_consensus = con.execute("""
        SELECT s.consensus,
               COUNT(*) as total,
               SUM(p.hit_target) as hits,
               AVG(p.actual_return) as avg_return
        FROM performance p
        JOIN snapshots s ON s.ticker = p.ticker AND s.date = p.snapshot_date
        WHERE p.days_later = 90
        GROUP BY s.consensus
        ORDER BY avg_return DESC
    """).fetchall()

    # Accuracy by sector — snapshots has no sector column, so pull it from
    # the cached stock data instead
    stocks      = get_stocks_cached()
    sector_map  = {s["ticker"]: s["sector"] for s in stocks}

    by_sector_raw: dict = {}
    perf_rows = con.execute("""
        SELECT p.ticker, p.hit_target, p.actual_return
        FROM performance p
        WHERE p.days_later = 90
    """).fetchall()

    for ticker, hit, ret in perf_rows:
        sector = sector_map.get(ticker, "Unknown")
        if sector not in by_sector_raw:
            by_sector_raw[sector] = {"total": 0, "hits": 0, "returns": []}
        by_sector_raw[sector]["total"] += 1
        by_sector_raw[sector]["hits"]  += hit or 0
        by_sector_raw[sector]["returns"].append(ret or 0)

    by_sector = sorted([
        {
            "sector":     sec,
            "total":      v["total"],
            "hits":       v["hits"],
            "hit_rate":   round(v["hits"] / v["total"] * 100, 1),
            "avg_return": round(sum(v["returns"]) / len(v["returns"]), 2),
        }
        for sec, v in by_sector_raw.items() if v["total"] > 0
    ], key=lambda x: x["avg_return"], reverse=True)

    # Actual collection start date from the snapshots table
    first = con.execute("SELECT MIN(date) FROM snapshots").fetchone()
    collection_started = first[0] if first and first[0] else datetime.date.today().isoformat()

    con.close()

    def fmt_row(r):
        return {"ticker": r[0], "snapshot_date": r[1],
                "days_later": r[2], "actual_return": r[3], "hit_target": bool(r[4])}

    return jsonify({
        "checkpoints":      checkpoints,
        "top_performers":   [fmt_row(r) for r in top],
        "worst_performers": [fmt_row(r) for r in worst],
        "by_consensus":     [{"consensus": r[0], "total": r[1], "hits": r[2] or 0,
                              "hit_rate": round((r[2] or 0) / r[1] * 100, 1),
                              "avg_return": round(r[3] or 0, 2)} for r in by_consensus],
        "by_sector":        by_sector,
        "collection_started": collection_started,
    })

@app.route("/upgraded")
def upgraded_page():
    return Response(render_changes_page("upgraded"), mimetype="text/html")

@app.route("/downgraded")
def downgraded_page():
    return Response(render_changes_page("downgraded"), mimetype="text/html")

@app.route("/changes")
def changes_page():
    return Response(render_changes_page("both"), mimetype="text/html")

@app.route("/api/changes")
@limiter.limit("60 per hour")
def api_changes():
    days = int(request.args.get("days", 30))
    con  = sqlite3.connect(DB_PATH)
    cutoff = (datetime.date.today() - datetime.timedelta(days=days)).isoformat()

    # Get the most recent snapshot date
    latest_date = con.execute(
        "SELECT MAX(date) FROM snapshots"
    ).fetchone()[0]

    if not latest_date:
        con.close()
        return jsonify({"upgraded": [], "downgraded": [], "new_coverage": []})

    # All stocks in latest snapshot joined with their state N days ago
    results = con.execute("""
        SELECT
            curr.ticker,
            curr.consensus      AS curr_consensus,
            curr.analyst_count  AS curr_count,
            curr.upside_pct     AS curr_upside,
            past.consensus      AS past_consensus,
            past.analyst_count  AS past_count,
            past.date           AS past_date
        FROM snapshots curr
        LEFT JOIN snapshots past
            ON past.ticker = curr.ticker
            AND past.date = (
                SELECT date FROM snapshots
                WHERE ticker = curr.ticker
                  AND date <= ?
                ORDER BY date DESC LIMIT 1
            )
        WHERE curr.date = ?
    """, (cutoff, latest_date)).fetchall()

    con.close()

    upgraded    = []
    downgraded  = []
    new_coverage = []

    for row in results:
        ticker, curr_con, curr_n, curr_up, past_con, past_n, past_date = row

        curr_score = CONSENSUS_SCORE.get(curr_con, 3)

        # No past data — brand new coverage
        if past_con is None:
            new_coverage.append({
                "ticker":          ticker,
                "consensus":       curr_con,
                "upside_pct":      curr_up,
                "analyst_count":   curr_n,
            })
            continue

        past_score  = CONSENSUS_SCORE.get(past_con, 3)
        score_delta = curr_score - past_score
        count_delta = curr_n - (past_n or 0)

        entry = {
            "ticker":          ticker,
            "curr_consensus":  curr_con,
            "past_consensus":  past_con,
            "score_delta":     score_delta,
            "curr_upside":     curr_up,
            "curr_count":      curr_n,
            "count_delta":     count_delta,
            "past_date":       past_date,
        }

        if score_delta > 0:
            upgraded.append(entry)
        elif score_delta < 0:
            downgraded.append(entry)
        elif count_delta >= 3:
            # Same consensus but meaningfully more analysts — treat as bullish signal
            upgraded.append(entry)
        elif count_delta <= -3:
            downgraded.append(entry)

    # Sort upgraded by score delta desc, then by count delta desc
    upgraded.sort(key=lambda x: (x["score_delta"], x["count_delta"]), reverse=True)
    # Sort downgraded by score delta asc (most negative first)
    downgraded.sort(key=lambda x: (x["score_delta"], x["count_delta"]))
    # Sort new coverage by upside desc
    new_coverage.sort(key=lambda x: x["upside_pct"] or 0, reverse=True)

    return jsonify({
        "upgraded":     upgraded[:25],
        "downgraded":   downgraded[:25],
        "new_coverage": new_coverage[:10],
        "as_of":        latest_date,
        "compared_to":  cutoff,
        "days":         days,
    })

@app.route("/api/accuracy/<ticker>")
@limiter.limit("60 per hour")
def api_accuracy_ticker(ticker):
    """Per-stock accuracy history — used on individual stock pages."""
    ticker = ticker.upper()
    con = get_db()

    history = con.execute("""
        SELECT p.snapshot_date, p.days_later, p.price_then, p.price_now,
               p.actual_return, p.hit_target, s.target_price, s.upside_pct
        FROM performance p
        JOIN snapshots s ON s.ticker = p.ticker AND s.date = p.snapshot_date
        WHERE p.ticker = ?
        ORDER BY p.snapshot_date DESC, p.days_later ASC
    """, (ticker,)).fetchall()

    con.close()

    return jsonify([{
        "snapshot_date": r[0], "days_later": r[1],
        "price_then": r[2], "price_now": r[3],
        "actual_return": r[4], "hit_target": bool(r[5]),
        "target_price": r[6], "predicted_upside": r[7],
    } for r in history])

def _consensus_color(c):
    return {"Strong Buy":"#00e676","Buy":"#69f0ae","Hold":"#ffd740",
            "Underperform":"#ff5252","Sell":"#d50000"}.get(c, "#aaa")

def _range_dot(s):
    lo  = min(s["week52_low"], s["low_target"], s["current_price"]) * 0.97
    hi  = max(s["week52_high"], s["high_target"], s["current_price"]) * 1.03
    rng = hi - lo or 1
    def pos(v): return round(max(2, min(98, (v - lo) / rng * 100)), 1)
    return (
        f'<div title="Current" style="position:absolute;top:50%;left:{pos(s["current_price"])}%;'
        f'transform:translate(-50%,-50%);width:12px;height:12px;border-radius:50%;'
        f'background:var(--text);border:2px solid var(--bg);z-index:2"></div>'
        f'<div title="Target" style="position:absolute;top:50%;left:{pos(s["target_price"])}%;'
        f'transform:translate(-50%,-50%);width:12px;height:12px;border-radius:50%;'
        f'background:var(--accent);border:2px solid var(--bg);z-index:2"></div>'
    )

# Add above render_stock_page
def _momentum_html(s: dict) -> str:
    trend   = s.get("momentum_trend", "neutral")
    detail  = s.get("momentum_detail", "No prior data")
    streak  = s.get("momentum_streak", 0)
    history = s.get("momentum_history", {})

    arrow = "↑" if trend == "up" else "↓" if trend == "down" else "→"
    color = "#00e676" if trend == "up" else "#f85149" if trend == "down" else "var(--text3)"
    label = "Improving" if trend == "up" else "Weakening" if trend == "down" else "Neutral"

    history_rows = ""
    for days in ["7", "30", "90"]:
        if days in history:
            h = history[days]
            score = CONSENSUS_SCORE.get(h["consensus"], 3)
            current_score = CONSENSUS_SCORE.get(s.get("consensus", "Hold"), 3)
            delta = current_score - score
            dcol = "#00e676" if delta > 0 else "#f85149" if delta < 0 else "var(--text3)"
            dsym = f'+{delta}' if delta > 0 else str(delta)
            history_rows += f"""
            <div class="sp-stat">
              <span class="sp-stat-l">{days} days ago</span>
              <span class="sp-stat-v" style="font-size:12px">
                {h["consensus"]}
                <span style="color:{dcol};font-size:11px;margin-left:6px">{dsym if delta != 0 else "="}</span>
              </span>
            </div>"""
        else:
            history_rows += f"""
            <div class="sp-stat">
              <span class="sp-stat-l">{days} days ago</span>
              <span class="sp-stat-v" style="color:var(--text3);font-size:11px">No data yet</span>
            </div>"""

    streak_line = (f'<div style="font-size:11px;color:var(--text2);margin-top:4px">'
                   f'Trending for {streak} days</div>') if streak > 0 else ""

    return f"""
    <div class="sp-card">
      <div class="sp-card-title">CONSENSUS MOMENTUM</div>
      <div style="font-size:32px;font-weight:700;font-family:var(--font-mono);
                  color:{color};line-height:1;margin-bottom:4px">{arrow} {label}</div>
      <div style="font-size:12px;color:var(--text2);margin-bottom:{
          '4' if streak else '16'}px">{detail}</div>
      {streak_line}
      <div style="margin-top:16px">
        <div class="sp-stat">
          <span class="sp-stat-l">Current</span>
          <span class="sp-stat-v" style="color:{color}">{s.get("consensus","—")}</span>
        </div>
        {history_rows}
      </div>
    </div>"""

def render_stock_page(s: dict) -> str:
    total_analysts = s["strong_buy"] + s["buy"] + s["hold"] + s["sell"] or 1
    bull_pct = round((s["strong_buy"] + s["buy"]) / total_analysts * 100)
    # Helper at top of render_stock_page — add these two lines after the bull_pct line:
    def _fmt_cap(mc):
        if not mc: return "N/A"
        if mc >= 1e12: return f"${mc/1e12:.2f}T"
        if mc >= 1e9:  return f"${mc/1e9:.1f}B"
        return f"${mc/1e6:.0f}M"

    def _na(val, fmt):
        if not val or val == 0: return '<span style="color:var(--text3)">N/A</span>'
        try: return fmt(val)
        except: return '<span style="color:var(--text3)">N/A</span>'
    upside_color = "#00e676" if s["upside_pct"] >= 20 else "#69f0ae" if s["upside_pct"] >= 10 else "#ffd740"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>{s["ticker"]} Analyst Price Target — {s["name"]} Stock Forecast | StockUpside.io</title>
  <meta name="description" content="Wall Street analysts have a consensus price target of ${s["target_price"]} for {s["name"]} ({s["ticker"]}), implying {s["upside_pct"]}% upside from the current price of ${s["current_price"]}. {s["analyst_count"]} analysts covered. Consensus: {s["consensus"]}."/>
  <meta property="og:type"        content="article"/>
  <meta property="og:title"       content="{s["ticker"]} — {s["upside_pct"]}% Analyst Upside | StockUpside.io"/>
  <meta property="og:description" content="{s["analyst_count"]} analysts. Target: ${s["target_price"]}. Current: ${s["current_price"]}. Consensus: {s["consensus"]}."/>
  <meta property="og:url"         content="https://stockupside.io/stocks/{s["ticker"]}"/>
  <meta property="og:image"       content="https://stockupside.io/og-image.png"/>
  <meta name="twitter:card"       content="summary_large_image"/>
  <meta name="twitter:title"      content="{s["ticker"]} — {s["upside_pct"]}% Analyst Upside | StockUpside.io"/>
  <meta name="twitter:description" content="{s["analyst_count"]} analysts covering {s["ticker"]}. Consensus: {s["consensus"]}. Target: ${s["target_price"]}."/>
  <meta name="twitter:image"      content="https://stockupside.io/og-image.png"/>
  <meta name="robots" content="index, follow"/>
  <link rel="canonical" href="https://stockupside.io/stocks/{s["ticker"]}"/>
  <link rel="stylesheet" href="/style.css"/>
  <style>
    .sp-wrap  {{ max-width: 860px; margin: 0 auto; padding: 32px 20px 64px; }}
    .sp-back  {{ font-family: var(--font-mono); font-size: 12px; color: var(--text2);
                 text-decoration: none; display: inline-flex; align-items: center;
                 gap: 6px; margin-bottom: 28px; }}
    .sp-back:hover {{ color: var(--text); text-decoration: none; }}
    .sp-hero  {{ margin-bottom: 32px; }}
    .sp-ticker{{ font-family: var(--font-mono); font-size: 42px; font-weight: 700;
                 color: var(--accent); line-height: 1; }}
    .sp-name  {{ font-size: 20px; font-weight: 600; color: var(--text); margin: 6px 0 4px; }}
    .sp-meta  {{ font-size: 13px; color: var(--text2); }}
    .sp-grid  {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 16px; }}
    .sp-grid-3{{ display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 16px; margin-bottom: 32px; }}
.sp-stat-v{{ font-family: var(--font-mono); font-size: 13px; font-weight: 600; color: var(--text); text-align: right; word-break: break-all; }}
    @media(max-width:700px) {{ .sp-grid-3 {{ grid-template-columns: 1fr; }} }}
    @media(max-width:600px) {{ .sp-grid {{ grid-template-columns: 1fr; }} }}
    .sp-card  {{ background: var(--bg2); border: 1px solid var(--border); border-radius: 8px; padding: 20px; }}
    .sp-card-title {{ font-family: var(--font-mono); font-size: 9px; color: var(--text3);
                      letter-spacing: .12em; margin-bottom: 14px; }}
    .sp-stat  {{ display: flex; justify-content: space-between; align-items: baseline;
                 padding: 8px 0; border-bottom: 1px solid var(--border); }}
    .sp-stat:last-child {{ border-bottom: none; }}
    .sp-stat-l{{ font-size: 12px; color: var(--text2); }}
    .sp-stat-v{{ font-family: var(--font-mono); font-size: 14px; font-weight: 600; color: var(--text); }}
    .sp-stat-v.pos {{ color: #69f0ae; }}
    .sp-stat-v.neg {{ color: #f85149; }}
    .sp-upside{{ font-size: 48px; font-weight: 700; color: {upside_color};
                 font-family: var(--font-mono); line-height: 1; margin-bottom: 4px; }}
    .sp-upside-sub {{ font-size: 13px; color: var(--text2); }}
    .rbar-wrap {{ margin-top: 10px; }}
    .rbar-row {{ display:flex; align-items:center; gap:8px; margin-bottom:8px; }}
    .rbar-lbl {{ font-size:11px; width:80px; flex-shrink:0; }}
    .rbar-bg  {{ flex:1; height:6px; background:var(--bg3); border-radius:3px; overflow:hidden; }}
    .rbar-fill{{ height:100%; border-radius:3px; }}
    .rbar-n   {{ font-family:var(--font-mono); font-size:11px; color:var(--text2); width:20px; text-align:right; }}
    .sp-prose {{ background: var(--bg2); border: 1px solid var(--border); border-radius: 8px;
                 padding: 24px; margin-bottom: 32px; line-height: 1.8; color: var(--text2); font-size: 14px; }}
    .sp-prose h2 {{ font-family: var(--font-mono); font-size: 11px; letter-spacing: .1em;
                    color: var(--text3); margin-bottom: 12px; }}
    .sp-prose strong {{ color: var(--text); }}
    .sp-cta   {{ background: linear-gradient(135deg, rgba(240,180,41,.12), rgba(240,180,41,.04));
                 border: 1px solid rgba(240,180,41,.3); border-radius: 8px;
                 padding: 28px; text-align: center; }}
    .sp-cta h3{{ font-family: var(--font-mono); font-size: 16px; margin-bottom: 8px; }}
    .sp-cta p {{ font-size: 13px; color: var(--text2); margin-bottom: 20px; }}
    .sp-cta-btn {{ display: inline-block; padding: 11px 28px; background: var(--accent);
                   color: #000; border-radius: 4px; font-family: var(--font-mono);
                   font-size: 12px; font-weight: 700; letter-spacing: .06em;
                   text-decoration: none; transition: background .2s; }}
    .sp-cta-btn:hover {{ background: #e8912d; text-decoration: none; }}
    .sp-rank  {{ font-family: var(--font-mono); font-size: 12px; color: var(--text2); margin-bottom: 8px; }}
  </style>
</head>
<body>
<header class="hdr">
  <div class="hdr-l">
    <a href="/" class="brand" style="text-decoration:none">
      <span class="brand-mark">▲</span>
      <div><div class="brand-name">STOCKUPSIDE<span class="brand-io">.IO</span></div>
        <div class="brand-tag">Analyst Price Target Intelligence</div></div>
    </a>
  </div>
  <div class="hdr-r">
    <a href="/" style="font-family:var(--font-mono);font-size:11px;color:var(--text2);">← Full Rankings</a>
  </div>
</header>

<div class="sp-wrap">
  <a href="/" class="sp-back">← Back to rankings</a>

  <div class="sp-hero">
    <div class="sp-rank">Ranked #{s["rank"]} by analyst upside · {s["last_updated"]}</div>
    <div class="sp-ticker">{s["ticker"]}</div>
    <div class="sp-name">{s["name"]}</div>
    <div class="sp-meta">{s["sector"]} · Market Cap {s["market_cap"]} · P/E {s["pe_ratio"]}x</div>
  </div>

  <div class="sp-grid">

    <div class="sp-card">
      <div class="sp-card-title">ANALYST PRICE TARGET</div>
      <div class="sp-upside">+{s["upside_pct"]}%</div>
      <div class="sp-upside-sub">implied upside to consensus target</div>
      <div style="margin-top:16px">
        <div class="sp-stat">
          <span class="sp-stat-l">Current Price</span>
          <span class="sp-stat-v">${s["current_price"]}</span>
        </div>
        <div class="sp-stat">
          <span class="sp-stat-l">Consensus Target</span>
          <span class="sp-stat-v pos">${s["target_price"]}</span>
        </div>
        <div class="sp-stat">
          <span class="sp-stat-l">Bull Target</span>
          <span class="sp-stat-v">${s["high_target"]}</span>
        </div>
        <div class="sp-stat">
          <span class="sp-stat-l">Bear Target</span>
          <span class="sp-stat-v">${s["low_target"]}</span>
        </div>
        <div class="sp-stat">
          <span class="sp-stat-l">Analysts Covering</span>
          <span class="sp-stat-v">{s["analyst_count"]}</span>
        </div>
      </div>
    </div>

    <div class="sp-card">
      <div class="sp-card-title">ANALYST CONSENSUS — {s["consensus"].upper()}</div>
      <div class="rbar-wrap">
        <div class="rbar-row">
          <span class="rbar-lbl" style="color:#00e676">Strong Buy</span>
          <div class="rbar-bg"><div class="rbar-fill" style="width:{round(s["strong_buy"]/total_analysts*100)}%;background:#00e676"></div></div>
          <span class="rbar-n">{s["strong_buy"]}</span>
        </div>
        <div class="rbar-row">
          <span class="rbar-lbl" style="color:#69f0ae">Buy</span>
          <div class="rbar-bg"><div class="rbar-fill" style="width:{round(s["buy"]/total_analysts*100)}%;background:#69f0ae"></div></div>
          <span class="rbar-n">{s["buy"]}</span>
        </div>
        <div class="rbar-row">
          <span class="rbar-lbl" style="color:#ffd740">Hold</span>
          <div class="rbar-bg"><div class="rbar-fill" style="width:{round(s["hold"]/total_analysts*100)}%;background:#ffd740"></div></div>
          <span class="rbar-n">{s["hold"]}</span>
        </div>
        <div class="rbar-row">
          <span class="rbar-lbl" style="color:#f85149">Sell</span>
          <div class="rbar-bg"><div class="rbar-fill" style="width:{round(s["sell"]/total_analysts*100)}%;background:#f85149"></div></div>
          <span class="rbar-n">{s["sell"]}</span>
        </div>
      </div>
      <div style="margin-top:16px">
        <div class="sp-stat">
          <span class="sp-stat-l">52-Week Low</span>
          <span class="sp-stat-v">${s["week52_low"]}</span>
        </div>
        <div class="sp-stat">
          <span class="sp-stat-l">52-Week High</span>
          <span class="sp-stat-v">${s["week52_high"]}</span>
        </div>
        <div class="sp-stat">
          <span class="sp-stat-l">YTD Return</span>
          <span class="sp-stat-v {'pos' if s['ytd_change'] >= 0 else 'neg'}">{'+' if s['ytd_change'] >= 0 else ''}{s["ytd_change"]}%</span>
        </div>
      </div>
    </div>

  

  </div>

  <div class="sp-grid-3">
    
    <div class="sp-card">
      <div class="sp-card-title">FUNDAMENTALS</div>
      <div class="sp-stat">
        <span class="sp-stat-l">Market Cap</span>
        <span class="sp-stat-v">{s["market_cap"]}</span>
      </div>
      <div class="sp-stat">
        <span class="sp-stat-l">P/E Ratio (TTM)</span>
        <span class="sp-stat-v">{_na(s.get("pe_ratio"), lambda v: f"{v}x")}</span>
      </div>
      <div class="sp-stat">
        <span class="sp-stat-l">Forward P/E</span>
        <span class="sp-stat-v">{_na(s.get("forward_pe"), lambda v: f"{v}x")}</span>
      </div>
      <div class="sp-stat">
        <span class="sp-stat-l">EPS (TTM)</span>
        <span class="sp-stat-v">{_na(s.get("eps"), lambda v: f"${v:.2f}")}</span>
      </div>
      <div class="sp-stat">
        <span class="sp-stat-l">PEG Ratio</span>
        <span class="sp-stat-v">{_na(s.get("peg_ratio"), lambda v: f"{v:.2f}")}</span>
      </div>
      <div class="sp-stat">
        <span class="sp-stat-l">Dividend Yield</span>
        <span class="sp-stat-v">{_na(s.get("dividend_yield"), lambda v: f"{v*100:.2f}%")}</span>
      </div>
      <div class="sp-stat">
        <span class="sp-stat-l">Revenue (TTM)</span>
        <span class="sp-stat-v">{_na(s.get("revenue"), _fmt_cap)}</span>
      </div>
      <div class="sp-stat">
        <span class="sp-stat-l">Profit Margin</span>
        <span class="sp-stat-v">{_na(s.get("profit_margin"), lambda v: f"{v*100:.1f}%")}</span>
      </div>
    </div>

    <div class="sp-card">
      <div class="sp-card-title">52-WEEK RANGE</div>
      <div class="sp-stat">
        <span class="sp-stat-l">52-Week Low</span>
        <span class="sp-stat-v">${s["week52_low"]}</span>
      </div>
      <div class="sp-stat">
        <span class="sp-stat-l">52-Week High</span>
        <span class="sp-stat-v">${s["week52_high"]}</span>
      </div>
      <div class="sp-stat">
        <span class="sp-stat-l">Current Price</span>
        <span class="sp-stat-v">${s["current_price"]}</span>
      </div>
      <div class="sp-stat">
        <span class="sp-stat-l">Bear Target</span>
        <span class="sp-stat-v">${s["low_target"]}</span>
      </div>
      <div class="sp-stat">
        <span class="sp-stat-l">Bull Target</span>
        <span class="sp-stat-v">${s["high_target"]}</span>
      </div>
      <div style="margin-top:16px;position:relative;height:6px;
                  background:linear-gradient(to right,#f85149,#ffd740,#00e676);
                  border-radius:4px">
        {_range_dot(s)}
      </div>
      <div style="display:flex;justify-content:space-between;margin-top:20px;
                  font-family:var(--font-mono);font-size:10px;color:var(--text3)">
        <span>● Now ${s["current_price"]}</span>
        <span style="color:var(--accent)">● Target ${s["target_price"]}</span>
      </div>
    </div>

    <div class="sp-card">
      <div class="sp-card-title">TRADING DATA</div>
      <div class="sp-stat">
        <span class="sp-stat-l">Avg Volume</span>
        <span class="sp-stat-v">{_na(s.get("avg_volume"), lambda v: f"{v/1e6:.1f}M" if v >= 1e6 else f"{v/1e3:.0f}K")}</span>
      </div>
      <div class="sp-stat">
        <span class="sp-stat-l">YTD Return</span>
        <span class="sp-stat-v {'pos' if s['ytd_change'] >= 0 else 'neg'}">{'+' if s['ytd_change'] >= 0 else ''}{s["ytd_change"]}%</span>
      </div>
      <div class="sp-stat">
        <span class="sp-stat-l">Analyst Count</span>
        <span class="sp-stat-v">{s["analyst_count"]}</span>
      </div>
      <div class="sp-stat">
        <span class="sp-stat-l">Consensus</span>
        <span class="sp-stat-v" style="color:{_consensus_color(s['consensus'])}">{s["consensus"]}</span>
      </div>
      <div class="sp-stat">
        <span class="sp-stat-l">Upside to Target</span>
        <span class="sp-stat-v pos">+{s["upside_pct"]}%</span>
      </div>
      <div class="sp-stat">
        <span class="sp-stat-l">Last Updated</span>
        <span class="sp-stat-v" style="font-size:11px">{s["last_updated"]}</span>
      </div>
    </div>

  </div>

  <div class="sp-prose">
    <h2>ANALYST SUMMARY</h2>
    <p>
      <strong>{s["name"]} ({s["ticker"]})</strong> is currently trading at
      <strong>${s["current_price"]}</strong> with a Wall Street consensus price target of
      <strong>${s["target_price"]}</strong>, implying potential upside of
      <strong>{s["upside_pct"]}%</strong> over the next 12 months.
      {s["analyst_count"]} analysts currently cover {s["ticker"]}, with a consensus rating of
      <strong>{s["consensus"]}</strong> — {bull_pct}% of analysts rate the stock a Buy or Strong Buy.
      The most bullish analyst has a price target of <strong>${s["high_target"]}</strong>,
      while the most cautious has a target of <strong>${s["low_target"]}</strong>.
    </p>
    <p style="margin-top:12px">
      {s["ticker"]} operates in the <strong>{s["sector"]}</strong> sector with a market
      capitalisation of <strong>{s["market_cap"]}</strong>.
      The stock is {'up' if s['ytd_change'] >= 0 else 'down'}
      <strong>{abs(s["ytd_change"])}% year-to-date</strong> and is currently trading
      {'near the lower end' if s['current_price'] < (s['week52_low'] + s['week52_high']) / 2 else 'near the upper end'}
      of its 52-week range of ${s["week52_low"]} – ${s["week52_high"]}.
    </p>
  </div>
    <div id="stock-accuracy"></div>
  <div class="sp-cta">
    <h3>See all {"{100}"} stocks ranked by analyst upside</h3>
    <p>StockUpside.io tracks analyst consensus price targets across thousands of US-listed stocks,
       updated every day. The full ranked list is available with a Pro subscription.</p>
    <a href="/" class="sp-cta-btn">View Full Rankings →</a>
  </div>

</div>

<footer class="ftr" style="margin-top:0">
  <div>© {datetime.date.today().year} StockUpside.io · Data updated daily · Not financial advice</div>
  <div class="ftr-r"><a href="/">Home</a> · <a href="/stocks">All Stocks</a></div>
</footer>
<script>
fetch('/api/accuracy/{s["ticker"]}')
  .then(r => r.json())
  .then(data => {{
    if (!data.length) return;
    const el = document.getElementById('stock-accuracy');
    if (!el) return;
    const rows = data.slice(0, 9).map(r => {{
      const col = r.actual_return >= 0 ? '#69f0ae' : '#f85149';
      const hit = r.hit_target
        ? '<span style="color:#00e676">✓ Hit</span>'
        : '<span style="color:#f85149">✗ Miss</span>';
      return `<tr style="border-bottom:1px solid var(--border)">
        <td style="padding:8px 10px;color:var(--text3);font-size:11px">${{r.snapshot_date}}</td>
        <td style="padding:8px 10px;font-family:var(--font-mono)">${{r.days_later}}d</td>
        <td style="padding:8px 10px;font-family:var(--font-mono)">${{r.predicted_upside}}%</td>
        <td style="padding:8px 10px;font-family:var(--font-mono);color:${{col}}">
          ${{r.actual_return >= 0 ? '+' : ''}}${{r.actual_return}}%</td>
        <td style="padding:8px 10px">${{hit}}</td>
      </tr>`;
    }}).join('');
    el.innerHTML = `
      <div class="sp-card" style="margin-bottom:24px">
        <div class="sp-card-title">ANALYST ACCURACY HISTORY FOR {s["ticker"]}</div>
        <table style="width:100%;border-collapse:collapse;font-size:12px">
          <thead><tr style="border-bottom:1px solid var(--border)">
            <th style="padding:8px 10px;text-align:left;font-size:9px;
                color:var(--text3);letter-spacing:.1em">DATE</th>
            <th style="padding:8px 10px;text-align:left;font-size:9px;
                color:var(--text3);letter-spacing:.1em">PERIOD</th>
            <th style="padding:8px 10px;text-align:left;font-size:9px;
                color:var(--text3);letter-spacing:.1em">PREDICTED</th>
            <th style="padding:8px 10px;text-align:left;font-size:9px;
                color:var(--text3);letter-spacing:.1em">ACTUAL</th>
            <th style="padding:8px 10px;text-align:left;font-size:9px;
                color:var(--text3);letter-spacing:.1em">RESULT</th>
          </tr></thead>
          <tbody>${{rows}}</tbody>
        </table>
      </div>`;
  }});
</script>
</body>
</html>"""


def render_stocks_index(stocks: list) -> str:
    rows = ""
    for s in stocks:
        ytd_color = "#69f0ae" if s["ytd_change"] >= 0 else "#f85149"
        upside_color = "#00e676" if s["upside_pct"] >= 40 else "#69f0ae" if s["upside_pct"] >= 20 else "#ffd740"
        rows += f"""
        <tr>
          <td style="color:var(--text3);font-size:11px;padding:10px 12px">{s["rank"]}</td>
          <td style="padding:10px 12px">
            <a href="/stocks/{s["ticker"]}" style="color:var(--accent);font-family:var(--font-mono);
               font-weight:700;text-decoration:none">{s["ticker"]}</a>
          </td>
          <td style="padding:10px 12px;color:var(--text2);font-size:12px;max-width:200px;
               overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{s["name"]}</td>
          <td style="padding:10px 12px;color:var(--text2);font-size:11px">{s["sector"]}</td>
          <td style="padding:10px 12px;font-family:var(--font-mono);font-size:12px">${s["current_price"]}</td>
          <td style="padding:10px 12px;font-family:var(--font-mono);font-size:13px;
               font-weight:700;color:{upside_color}">+{s["upside_pct"]}%</td>
          <td style="padding:10px 12px;font-size:11px;color:var(--text2)">{s["consensus"]}</td>
          <td style="padding:10px 12px;font-family:var(--font-mono);font-size:12px;
               color:{ytd_color}">{'+' if s['ytd_change'] >= 0 else ''}{s["ytd_change"]}%</td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Top Stocks by Analyst Upside Potential — Full List | StockUpside.io</title>
  <meta name="description" content="Complete list of stocks ranked by Wall Street analyst consensus price target upside. Updated daily. Includes analyst count, consensus rating, and price targets."/>
  <meta name="robots" content="index, follow"/>
  <link rel="canonical" href="https://stockupside.io/stocks"/>
  <meta property="og:type"        content="website"/>
  <meta property="og:title"       content="Top Stocks by Analyst Upside | StockUpside.io"/>
  <meta property="og:description" content="Complete list of stocks ranked by Wall Street analyst consensus price target upside. Updated daily."/>
  <meta property="og:url"         content="https://stockupside.io/stocks"/>
  <meta property="og:image"       content="https://stockupside.io/og-image.png"/>
  <meta name="twitter:card"       content="summary_large_image"/>
  <meta name="twitter:title"      content="Top Stocks by Analyst Upside | StockUpside.io"/>
  <meta name="twitter:image"      content="https://stockupside.io/og-image.png"/>
  <link rel="stylesheet" href="/style.css"/>
</head>
<body>
<header class="hdr">
  <div class="hdr-l">
    <a href="/" class="brand" style="text-decoration:none">
      <span class="brand-mark">▲</span>
      <div><div class="brand-name">STOCKUPSIDE<span class="brand-io">.IO</span></div></div>
    </a>
  </div>
  <div class="hdr-r">
    <a href="/" style="font-family:var(--font-mono);font-size:11px;color:var(--text2);">← Dashboard</a>
  </div>
</header>
<div style="max-width:1100px;margin:0 auto;padding:32px 20px 64px">
  <h1 style="font-family:var(--font-mono);font-size:22px;margin-bottom:8px">
    Stocks Ranked by Analyst Upside
  </h1>
  <p style="color:var(--text2);font-size:13px;margin-bottom:24px">
    {len(stocks)} stocks with analyst coverage · Updated {stocks[0]["last_updated"] if stocks else "daily"}
  </p>
  <div style="overflow-x:auto">
    <table style="width:100%;border-collapse:collapse;font-family:var(--font-mono);font-size:12px">
      <thead>
        <tr style="background:var(--bg2);border-bottom:1px solid var(--border)">
          <th style="padding:10px 12px;text-align:left;font-size:9px;color:var(--text3);letter-spacing:.1em">#</th>
          <th style="padding:10px 12px;text-align:left;font-size:9px;color:var(--text3);letter-spacing:.1em">TICKER</th>
          <th style="padding:10px 12px;text-align:left;font-size:9px;color:var(--text3);letter-spacing:.1em">COMPANY</th>
          <th style="padding:10px 12px;text-align:left;font-size:9px;color:var(--text3);letter-spacing:.1em">SECTOR</th>
          <th style="padding:10px 12px;text-align:left;font-size:9px;color:var(--text3);letter-spacing:.1em">PRICE</th>
          <th style="padding:10px 12px;text-align:left;font-size:9px;color:var(--text3);letter-spacing:.1em">UPSIDE</th>
          <th style="padding:10px 12px;text-align:left;font-size:9px;color:var(--text3);letter-spacing:.1em">CONSENSUS</th>
          <th style="padding:10px 12px;text-align:left;font-size:9px;color:var(--text3);letter-spacing:.1em">YTD</th>
        </tr>
      </thead>
      <tbody>
        {rows}
      </tbody>
    </table>
  </div>
</div>
<footer class="ftr">
  <div>© {datetime.date.today().year} StockUpside.io · Not financial advice</div>
</footer>
</body>
</html>"""

# ── Static files ───────────────────────────────────────────────────────────────
@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def static_files(path):
    full = os.path.join(PUBLIC_DIR, path)
    if path and os.path.isfile(full):
        return send_from_directory(PUBLIC_DIR, path)
    return send_from_directory(PUBLIC_DIR, "index.html")

# ── Main ───────────────────────────────────────────────────────────────────────
#threading.Thread(target=nightly_refresh, daemon=True).start()
if __name__ == "__main__":
    init_db()
    threading.Thread(target=nightly_refresh, daemon=True).start()
    port = int(os.environ.get("PORT", 5000))
    print(f"\n🚀  StockUpside.io is running at http://localhost:{port}\n")
    # Only open a browser tab in local dev — never on a headless server
    if os.environ.get("ENV", "development") == "development":
        threading.Timer(1.2, lambda: webbrowser.open(f"http://localhost:{port}")).start()
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)