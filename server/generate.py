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

import json, sqlite3, time, datetime, os, random, urllib.request, sys

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

# ── Main generation ────────────────────────────────────────────────────────────
def generate_stocks() -> list:
    # For production: use get_full_universe() (full SEC EDGAR list, ~3–6 hrs).
    # For development: use get_full_universe()[:500] or UNIVERSE_FALLBACK.
    tickers = get_full_universe()
    if not tickers:
        print("  ⚠  SEC EDGAR fetch failed — using hardcoded fallback list")
        tickers = UNIVERSE_FALLBACK

    # ── Uncomment ONE of the lines below to control scope ──
    tickers = tickers[:4000]
    # tickers = tickers[:500]   # dev: ~30-60 min
    # tickers = tickers[:100]   # dev: ~5-10 min
    # (leave commented for full production run)

    print(f"  →  Fetching analyst targets for {len(tickers)} tickers...")
    rows = []
    total = len(tickers)
    rate_limit_streak = 0

    for i, ticker in enumerate(tickers):
        if i % 25 == 0:
            print(f"  →  Progress: {i}/{total} ({len(rows)} valid so far)")

        # 2 seconds per ticker keeps us safely under Yahoo's ~2000 req/hr limit
        time.sleep(0.5 + random.uniform(0.5, 2.0))

        retries = 3
        info = None
        t_obj = None
        for attempt in range(retries):
            try:
                t_obj = yf.Ticker(ticker)
                info  = t_obj.info
                if info and len(info) < 10:
                    raise ValueError("Stub response — likely rate limited")
                rate_limit_streak = 0
                break
            except Exception as e:
                err = str(e)
                if "Too Many Requests" in err or "Rate limited" in err or "Stub response" in err:
                    rate_limit_streak += 1
                    wait = min(60, 10 * (attempt + 1)) + random.uniform(0, 5)
                    print(f"  ⚠  Rate limited ({ticker}), waiting {wait:.0f}s "
                          f"(streak: {rate_limit_streak})")
                    time.sleep(wait)
                    if rate_limit_streak >= 5:
                        print("  ⚠  Extended pause (60s)...")
                        time.sleep(60)
                        rate_limit_streak = 0
                else:
                    print(f"  ⚠  Skipped {ticker}: {e}")
                    break

        if not info or len(info) < 10 or t_obj is None:
            continue

        try:
            current_price = info.get("currentPrice") or info.get("regularMarketPrice") or 0
            target_price  = info.get("targetMeanPrice") or 0
            analyst_count = info.get("numberOfAnalystOpinions") or 0

            if current_price <= 0 or target_price <= 0 or analyst_count < 2:
                continue

            upside_pct = round((target_price / current_price - 1) * 100, 1)
            if upside_pct < 0:
                continue

            high_target = info.get("targetHighPrice") or 0
            low_target  = info.get("targetLowPrice")  or 0

            # ── Analyst rating breakdown ───────────────────────────────────────
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
            ))

        except Exception as e:
            print(f"  ⚠  Skipped {ticker} (parse error): {e}")
            continue

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

    try:
        data = generate_stocks()
        if not data:
            print("  ✗  No stocks generated — aborting cache write.")
            sys.exit(1)

        save_cache(data)
        save_snapshot(data)
        check_performance()

        elapsed = time.time() - start_time
        print(f"\n  ✓  All done in {elapsed/60:.1f} min. "
              f"{len(data)} stocks written to cache.\n")
        sys.exit(0)

    except KeyboardInterrupt:
        print("\n  ✗  Interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n  ✗  Fatal error: {e}")
        sys.exit(1)