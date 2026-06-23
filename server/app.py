"""
StockUpside.io — Flask Backend
Run: python3 server/app.py
Serves the REST API on :5000 and static files from /public
"""

import json, sqlite3, time, datetime, hashlib, hmac, secrets, os, math, threading, webbrowser, re
import smtplib, email.mime.multipart, email.mime.text
from functools import wraps
from flask import Flask, jsonify, request, send_from_directory, Response, redirect
from markupsafe import escape
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from werkzeug.middleware.proxy_fix import ProxyFix

import yfinance as yf
import pandas as pd
import urllib.request
import urllib.error
import csv
import io
import threading

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PUBLIC_DIR = os.path.join(BASE_DIR, "public")
DB_PATH    = os.path.join(BASE_DIR, "server", "cache.db")
LOG_PATH   = os.path.join(BASE_DIR, "server", "generate.log")

app = Flask(__name__, static_folder=PUBLIC_DIR, static_url_path="")

# Trust exactly one layer of reverse proxy (Digital Ocean / Nginx) for the
# X-Forwarded-For and X-Forwarded-Proto headers. Without this, get_remote_address()
# below sees the proxy's IP for every visitor, so all traffic shares one rate-limit
# bucket. x_for=1 means "trust one hop" — increase only if you add another proxy
# layer (e.g. a CDN) in front of this one, or it becomes spoofable.
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)

limiter = Limiter(get_remote_address, app=app, default_limits=["200 per hour"])

# Dedicated secret for HMAC-derived tokens (unsubscribe links, etc.) and
# Flask's own session signing. Set APP_SECRET_KEY in production so tokens
# and signed cookies survive a restart; falls back to a random per-process
# value in dev (fine — just means existing unsubscribe links break on restart).
_APP_SECRET = os.environ.get("APP_SECRET_KEY") or secrets.token_hex(32)
if not os.environ.get("APP_SECRET_KEY"):
    print("  ⚠  APP_SECRET_KEY not set — using a random per-process secret. "
          "Set APP_SECRET_KEY in production so unsubscribe links don't break on restart.")
app.secret_key = _APP_SECRET

# ── In-memory stock cache (populated by get_stocks_cached, invalidated nightly) ─
_cache_lock: threading.Lock = threading.Lock()
_cache: dict = {"data": None, "date": None, "ts": None, "checked_at": 0.0}

# How often (seconds) to re-check the DB for a newer row while serving from
# the in-memory cache. generate.py runs in a SEPARATE PROCESS and checkpoints
# every ~50 tickers via merge_progress_into_cache() — it has no way to call
# invalidate_memory_cache() in this process directly. Without this check,
# the site would keep serving the snapshot from the moment _cache was last
# warmed (e.g. 50 stocks from early in a 3-hour run) until the whole run
# finishes and nightly_refresh()'s invalidate_memory_cache() fires.
_FRESHNESS_CHECK_INTERVAL = 10.0  # seconds

def invalidate_memory_cache():
    """Clear the in-memory stock cache so the next request re-reads from DB."""
    with _cache_lock:
        _cache["data"] = None
        _cache["date"] = None
        _cache["ts"] = None
        _cache["checked_at"] = 0.0


def nightly_refresh():
    """
    Runs in a background daemon thread. At 01:00 each night it launches
    generate.py as a subprocess so the Flask process is never blocked.
    generate.py writes directly to the SQLite DB; once it exits we
    invalidate the in-memory cache so the next request picks up fresh data.
    All output — including errors — is written to server/generate.log.

    generate.py checkpoints its progress to the DB after every ticker, so:
      - On timeout, we send SIGTERM (not SIGKILL) and give it a grace
        period to checkpoint and exit cleanly.
      - If a run doesn't finish (timeout, non-zero exit), we immediately
        relaunch — resume makes the next pass fast since only the
        remaining tickers are fetched. Capped at a few attempts per night
        so a persistent failure doesn't loop forever.
    """
    import subprocess, sys

    def log(msg: str):
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts}] {msg}"
        print(line, flush=True)
        try:
            with open(LOG_PATH, "a") as f:
                f.write(line + "\n")
        except Exception:
            pass

    generate_script = os.path.join(BASE_DIR, "server", "generate.py")
    RUN_TIMEOUT     = 3 * 3600   # per-attempt wall clock cap
    GRACE_PERIOD    = 60         # seconds to let SIGTERM checkpoint before SIGKILL
    MAX_ATTEMPTS    = 3          # cap relaunches per night

    while True:
        now    = datetime.datetime.now()
        target = now.replace(hour=1, minute=0, second=0, microsecond=0)
        if target <= now:
            target += datetime.timedelta(days=1)

        sleep_secs = (target - datetime.datetime.now()).total_seconds()
        log(f"Next refresh at {target.strftime('%Y-%m-%d %H:%M')} ({sleep_secs/3600:.1f}h away)")

        while (target - datetime.datetime.now()).total_seconds() > 0:
            time.sleep(min(60, max(0, (target - datetime.datetime.now()).total_seconds())))

        set_generating(True)
        try:
            for attempt in range(1, MAX_ATTEMPTS + 1):
                log(f"Launching generate.py (attempt {attempt}/{MAX_ATTEMPTS})...")
                try:
                    with open(LOG_PATH, "a") as logfile:
                        proc = subprocess.Popen(
                            [sys.executable, generate_script],
                            stdout=logfile, stderr=logfile,
                        )
                        try:
                            returncode = proc.wait(timeout=RUN_TIMEOUT)
                        except subprocess.TimeoutExpired:
                            log(f"generate.py exceeded {RUN_TIMEOUT/3600:.1f}h — "
                                f"sending SIGTERM to checkpoint and exit.")
                            proc.terminate()
                            try:
                                returncode = proc.wait(timeout=GRACE_PERIOD)
                            except subprocess.TimeoutExpired:
                                log("generate.py did not exit after SIGTERM — sending SIGKILL.")
                                proc.kill()
                                returncode = proc.wait()

                    if returncode == 0:
                        invalidate_memory_cache()
                        log("Nightly refresh complete — cache invalidated (exit 0).")
                        log("Running alert checker...")
                        try:
                            check_and_fire_alerts()
                        except Exception as e:
                            log(f"Alert check failed: {e}")
                        break
                    else:
                        log(f"generate.py exited with code {returncode} "
                            f"— check {LOG_PATH}. Will retry (resume) if attempts remain.")
                        invalidate_memory_cache()  # checkpoint merges may have updated cache
                except FileNotFoundError:
                    log(f"generate.py not found at {generate_script} — check path.")
                    break
                except Exception as e:
                    log(f"Nightly refresh attempt failed with unexpected error: {e}")
            else:
                log(f"Gave up after {MAX_ATTEMPTS} attempts tonight — "
                    f"cache holds whatever was checkpointed. Will try again tomorrow.")
        finally:
            set_generating(False)

def weekly_digest():
    import schedule
    schedule.every().monday.at("08:00").do(send_digest_job)
    while True:
        schedule.run_pending()
        time.sleep(60)

def send_digest_job():
    """Send the weekly digest to every subscriber (free and pro).

    - Free subscribers get the unfiltered top-10.
    - Pro subscribers get their top-10 filtered by their saved
      email_preferences (if any); if they have no preferences saved,
      or their filters return no matches, they get the unfiltered top-10
      as a fallback.
    """
    stocks = get_stocks_cached()
    if not stocks:
        print("  ⚠  Weekly digest skipped — no stock data available yet")
        return

    con  = get_db()
    subs = con.execute("SELECT email, plan FROM subscribers").fetchall()
    con.close()

    sent = 0
    for addr, plan in subs:
        prefs = get_email_prefs(addr) if plan == "pro" else None
        subj, html, text = digest_email_html(stocks, addr, prefs=prefs)
        if send_email(addr, subj, html, text):
            sent += 1
    print(f"  ✓  Weekly digest sent to {sent}/{len(subs)} subscribers")

# ── Pro alerts engine ──────────────────────────────────────────────────────────
ALERT_COOLDOWN_DAYS = 1
ALERT_MAX_PER_USER  = 20

ALERT_TYPES = {
    "upside_above":     "Upside % rises above threshold",
    "upside_below":     "Upside % falls below threshold",
    "rank_above":       "Rank number rises above threshold (rank worsens)",
    "rank_below":       "Rank improves to at or below threshold",
    "consensus_change": "Consensus rating changes",
    "target_hit":       "Price within 1% of analyst consensus target",
}

def _alert_condition_met(alert: dict, stock: dict) -> bool:
    atype = alert["alert_type"]
    val   = alert.get("alert_value")
    if atype == "upside_above":
        return val is not None and stock["upside_pct"] >= val
    if atype == "upside_below":
        return val is not None and stock["upside_pct"] <= val
    if atype == "rank_above":
        return val is not None and stock["rank"] >= val
    if atype == "rank_below":
        return val is not None and stock["rank"] <= val
    if atype == "consensus_change":
        prev = alert.get("alert_value_text", "")
        return bool(prev) and stock.get("consensus", "") != prev
    if atype == "target_hit":
        cp = stock.get("current_price", 0)
        tp = stock.get("target_price", 0)
        return tp > 0 and cp >= tp * 0.99
    return False

def _alert_email_html(triggered: list) -> tuple:
    count   = len(triggered)
    subject = (f"▲ StockUpside alert — {triggered[0]['ticker']} triggered"
               if count == 1
               else f"▲ StockUpside — {count} alerts triggered")
    rows_html = ""
    rows_text = ""
    for t in triggered:
        s      = t["stock"]
        atype  = t["alert_type"]
        ticker = t["ticker"]
        upside_sign  = "+" if s["upside_pct"] >= 0 else ""
        upside_color = ("#00b85c" if s["upside_pct"] >= 40 else
                        "#1a9e5c" if s["upside_pct"] >= 20 else
                        "#e6a817" if s["upside_pct"] >= 0  else "#d93025")
        if atype == "upside_above":
            trigger_desc = f"Upside rose above {t['alert_value']}% → now {upside_sign}{s['upside_pct']}%"
        elif atype == "upside_below":
            trigger_desc = f"Upside fell below {t['alert_value']}% → now {upside_sign}{s['upside_pct']}%"
        elif atype == "rank_above":
            trigger_desc = f"Rank worsened above #{int(t['alert_value'])} → now #{s['rank']}"
        elif atype == "rank_below":
            trigger_desc = f"Rank improved into top #{int(t['alert_value'])} → now #{s['rank']}"
        elif atype == "consensus_change":
            trigger_desc = f"Consensus changed from {t['alert_value_text']} → {s['consensus']}"
        elif atype == "target_hit":
            trigger_desc = f"Price ${s['current_price']} is within 1% of analyst target ${s['target_price']}"
        else:
            trigger_desc = atype
        rows_html += f"""
        <tr style="border-bottom:1px solid #2a2a2a">
          <td style="padding:14px 16px;vertical-align:top">
            <a href="https://stockupside.io/stocks/{ticker}"
               style="font-family:monospace;font-size:15px;font-weight:700;color:#00e5ff;text-decoration:none">{ticker}</a>
            <div style="font-size:12px;color:#888;margin-top:2px">{s.get('name','')}</div>
          </td>
          <td style="padding:14px 16px;vertical-align:top">
            <div style="font-size:13px;color:#ccc">{trigger_desc}</div>
          </td>
          <td style="padding:14px 16px;vertical-align:top;text-align:right;white-space:nowrap">
            <div style="font-family:monospace;font-size:14px;font-weight:700;color:{upside_color}">{upside_sign}{s['upside_pct']}%</div>
            <div style="font-size:11px;color:#666;margin-top:2px">upside</div>
          </td>
          <td style="padding:14px 16px;vertical-align:top;text-align:right;white-space:nowrap">
            <div style="font-family:monospace;font-size:13px;color:#ccc">${s['current_price']}</div>
            <div style="font-size:11px;color:#666;margin-top:2px">price</div>
          </td>
          <td style="padding:14px 16px;vertical-align:top;text-align:right;white-space:nowrap">
            <div style="font-family:monospace;font-size:13px;color:#aaa">#{s['rank']}</div>
            <div style="font-size:11px;color:#666;margin-top:2px">rank</div>
          </td>
        </tr>"""
        rows_text += (f"\n{ticker} — {trigger_desc}\n"
                      f"  Upside: {upside_sign}{s['upside_pct']}%  "
                      f"Price: ${s['current_price']}  Rank: #{s['rank']}\n"
                      f"  https://stockupside.io/stocks/{ticker}\n")
    html = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"/></head>
<body style="margin:0;padding:0;background:#0d0d0d;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#e0e0e0">
  <table width="100%" cellpadding="0" cellspacing="0" style="max-width:640px;margin:0 auto;padding:32px 16px">
    <tr><td>
      <div style="margin-bottom:24px">
        <span style="font-family:monospace;font-size:18px;font-weight:700;color:#00e5ff">▲ STOCKUPSIDE.IO</span>
        <span style="font-size:12px;color:#555;margin-left:12px">Pro Alerts</span>
      </div>
      <h1 style="font-size:16px;font-weight:500;color:#e0e0e0;margin:0 0 6px">{count} alert{'s' if count != 1 else ''} triggered</h1>
      <p style="font-size:12px;color:#666;margin:0 0 24px">{datetime.date.today().strftime('%B %d, %Y')} · Updated daily</p>
      <table width="100%" cellpadding="0" cellspacing="0"
             style="border:1px solid #2a2a2a;border-radius:6px;border-collapse:collapse;font-size:13px">
        <thead>
          <tr style="background:#1a1a1a;border-bottom:1px solid #333">
            <th style="padding:10px 16px;text-align:left;font-size:9px;color:#555;letter-spacing:.1em;font-weight:500">TICKER</th>
            <th style="padding:10px 16px;text-align:left;font-size:9px;color:#555;letter-spacing:.1em;font-weight:500">TRIGGER</th>
            <th style="padding:10px 16px;text-align:right;font-size:9px;color:#555;letter-spacing:.1em;font-weight:500">UPSIDE</th>
            <th style="padding:10px 16px;text-align:right;font-size:9px;color:#555;letter-spacing:.1em;font-weight:500">PRICE</th>
            <th style="padding:10px 16px;text-align:right;font-size:9px;color:#555;letter-spacing:.1em;font-weight:500">RANK</th>
          </tr>
        </thead>
        <tbody>{rows_html}</tbody>
      </table>
      <div style="margin-top:24px;padding:16px;background:#111;border-radius:6px;border:1px solid #1e1e1e">
        <p style="margin:0 0 8px;font-size:12px;color:#888">Manage your alerts</p>
        <a href="https://stockupside.io/alerts" style="font-size:13px;color:#00e5ff;text-decoration:none">stockupside.io/alerts →</a>
      </div>
      <p style="margin-top:24px;font-size:11px;color:#444;line-height:1.6">
        You're receiving this because you set up price alerts on StockUpside.io Pro.
        <a href="{_unsubscribe_url(triggered[0]['email'])}" style="color:#555">Unsubscribe</a>
      </p>
    </td></tr>
  </table>
</body></html>"""
    text  = f"StockUpside.io Pro Alerts — {datetime.date.today()}\n"
    text += f"{count} alert{'s' if count != 1 else ''} triggered:\n"
    text += rows_text
    text += f"\nManage alerts: https://stockupside.io/alerts\n"
    return subject, html, text


def check_and_fire_alerts():
    stocks = get_stocks_cached()
    if not stocks:
        print("  ⚠  Alert check skipped — no stock data")
        return
    stock_map  = {s["ticker"]: s for s in stocks}
    con        = get_db()
    alerts     = con.execute(
        "SELECT id, email, ticker, alert_type, alert_value, alert_value_text, last_triggered "
        "FROM alerts WHERE active=1"
    ).fetchall()
    con.close()
    if not alerts:
        return
    now        = int(time.time())
    cooldown_s = ALERT_COOLDOWN_DAYS * 86400
    fired_by_email: dict = {}
    for row in alerts:
        alert_id, email_addr, ticker, atype, aval, aval_text, last_triggered = row
        if last_triggered and (now - last_triggered) < cooldown_s:
            continue
        stock = stock_map.get(ticker)
        if not stock:
            continue
        alert = {"id": alert_id, "email": email_addr, "ticker": ticker,
                 "alert_type": atype, "alert_value": aval, "alert_value_text": aval_text}
        if _alert_condition_met(alert, stock):
            alert["stock"] = stock
            fired_by_email.setdefault(email_addr, []).append(alert)
    if not fired_by_email:
        print("  ✓  Alert check complete — no alerts triggered")
        return
    total_sent = 0
    con = get_db()
    for email_addr, triggered in fired_by_email.items():
        subject, html, text = _alert_email_html(triggered)
        if send_email(email_addr, subject, html, text):
            total_sent += 1
            fired_ids = [t["id"] for t in triggered]
            con.executemany("UPDATE alerts SET last_triggered=? WHERE id=?",
                            [(now, aid) for aid in fired_ids])
            for t in triggered:
                if t["alert_type"] == "consensus_change":
                    con.execute("UPDATE alerts SET alert_value_text=? WHERE id=?",
                                (t["stock"]["consensus"], t["id"]))
    con.commit()
    con.close()
    total_fired = sum(len(v) for v in fired_by_email.values())
    print(f"  ✓  Alert check complete — {total_fired} rules fired, "
          f"emails sent to {total_sent}/{len(fired_by_email)} subscribers")


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

    # Session-based Pro access tokens. Replaces the old scheme where a
    # token was a deterministic hash of email+stripe_id+secret (permanent,
    # shareable, no expiry, no way to revoke). Tokens here are random,
    # tied to one subscriber, expire after SESSION_TTL_DAYS, and can be
    # revoked by deleting the row (e.g. on logout or "sign out everywhere").
    con.execute("""CREATE TABLE IF NOT EXISTS sessions(
        token TEXT PRIMARY KEY,
        email TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        expires_at INTEGER NOT NULL,
        last_seen INTEGER)""")
    con.execute("CREATE INDEX IF NOT EXISTS idx_sessions_email ON sessions(email)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions(expires_at)")

    # Pro-only watchlist: arbitrary tickers a subscriber wants to track.
    con.execute("""CREATE TABLE IF NOT EXISTS watchlists(
        email TEXT NOT NULL,
        ticker TEXT NOT NULL,
        added_at INTEGER NOT NULL,
        PRIMARY KEY (email, ticker))""")
    con.execute("CREATE INDEX IF NOT EXISTS idx_watchlists_email ON watchlists(email)")

    # Per-subscriber digest filter preferences (Pro only).
    # If no row exists for an email, the default top-10 (no filters) is sent.
    con.execute("""CREATE TABLE IF NOT EXISTS email_preferences(
        email TEXT PRIMARY KEY,
        sector TEXT DEFAULT 'All',
        consensus TEXT DEFAULT 'All',
        min_analysts INTEGER DEFAULT 0,
        max_pe REAL DEFAULT 0,
        max_peg REAL DEFAULT 0,
        momentum TEXT DEFAULT 'All',
        updated_at INTEGER)""")

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

    # ── Per-firm analyst track record ────────────────────────────────────
    # Originally designed to store per-firm price targets, but Yahoo's
    # free upgrade/downgrade feed (yfinance's upgrades_downgrades) only
    # provides rating changes — firm, from-grade, to-grade, action, date —
    # not a per-firm price target. Price-target accuracy can only be
    # measured at the consensus level (see `performance` table above).
    # What IS measurable per-firm: whether an upgrade/downgrade call was
    # directionally right — did the stock move the predicted way in the
    # following weeks. That's what this table is actually for now.
    con.execute("""CREATE TABLE IF NOT EXISTS analyst_calls (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker TEXT NOT NULL,
        firm TEXT NOT NULL,
        grade_date TEXT NOT NULL,      -- ISO date of the rating change
        from_grade TEXT,
        to_grade TEXT NOT NULL,
        action TEXT NOT NULL,          -- 'up' | 'down' | 'main' | 'init' | 'reit'
        price_at_call REAL,            -- closing price on/near grade_date
        first_seen TEXT NOT NULL,      -- when our scraper first recorded this row
        UNIQUE(ticker, firm, grade_date, to_grade)
)""")
    con.execute("CREATE INDEX IF NOT EXISTS idx_calls_firm ON analyst_calls(firm)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_calls_ticker ON analyst_calls(ticker)")

    # Outcome of each call, checked 30/60/90 days out — same pattern as
    # the existing `performance` table, but scoped to the individual call
    # rather than the aggregate consensus.
    con.execute("""CREATE TABLE IF NOT EXISTS analyst_call_outcomes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        call_id INTEGER NOT NULL,
        days_later INTEGER NOT NULL,
        price_then REAL,
        price_now REAL,
        actual_return REAL,
        was_correct INTEGER,           -- 1 if price moved the predicted direction, 0 if not, NULL if 'main'/'reit' (no directional call)
        checked_date TEXT,
        UNIQUE(call_id, days_later),
        FOREIGN KEY(call_id) REFERENCES analyst_calls(id)
)""")

    # ── API tier ──────────────────────────────────────────────────────────
    # Separate from the web `sessions` table on purpose: API keys are
    # long-lived (no expiry by default, since rotating them breaks
    # integrations), revocable independently of web login, and need their
    # own per-key usage tracking for rate limiting + the future usage
    # dashboard. One email can hold multiple keys (e.g. dev + prod).
    con.execute("""CREATE TABLE IF NOT EXISTS api_keys(
        api_key TEXT PRIMARY KEY,
        email TEXT NOT NULL,
        stripe_id TEXT,
        plan TEXT DEFAULT 'active',   -- 'active' | 'revoked'
        created_at INTEGER NOT NULL,
        last_used_at INTEGER,
        label TEXT)""")
    con.execute("CREATE INDEX IF NOT EXISTS idx_api_keys_email ON api_keys(email)")

    # Per-day request counts, keyed by (api_key, date). Used both for
    # rate limiting (read here instead of trusting flask-limiter's
    # in-memory store, which resets on restart and doesn't share state
    # across multiple server processes) and for an eventual "usage this
    # month" display for API customers.
    con.execute("""CREATE TABLE IF NOT EXISTS api_usage(
        api_key TEXT NOT NULL,
        date TEXT NOT NULL,
        request_count INTEGER DEFAULT 0,
        PRIMARY KEY (api_key, date))""")

    con.execute("""CREATE TABLE IF NOT EXISTS alerts(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT NOT NULL,
        ticker TEXT NOT NULL,
        alert_type TEXT NOT NULL,
        alert_value REAL,
        alert_value_text TEXT,
        created_at INTEGER NOT NULL,
        last_triggered INTEGER,
        active INTEGER DEFAULT 1)""")
    con.execute("CREATE INDEX IF NOT EXISTS idx_alerts_email ON alerts(email)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_alerts_ticker ON alerts(ticker)")

    con.commit()
    con.close()

def get_cached_with_ts():
    """Like get_cached() but also returns the row's `ts`, so the in-memory
    cache can be invalidated by comparing timestamps instead of re-parsing
    the full JSON blob on every request."""
    today = datetime.date.today().isoformat()
    con = get_db()
    row = con.execute("SELECT data, ts FROM cache WHERE date=?", (today,)).fetchone()
    con.close()
    if row:
        return json.loads(row[0]), row[1]
    return None, None

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
    now   = time.monotonic()

    # 1. Fast path: in-memory cache is fresh for today AND we've checked
    #    recently enough that it's unlikely a checkpoint merge from
    #    generate.py (running in another process) has landed since.
    with _cache_lock:
        if (_cache["date"] == today and _cache["data"]
                and (now - _cache["checked_at"]) < _FRESHNESS_CHECK_INTERVAL):
            return _cache["data"]

    # 2. Cheap freshness check: has a newer row landed in the DB than what
    #    we have cached? generate.py's periodic checkpoints update `ts` on
    #    every merge, so this catches mid-run progress without re-reading
    #    the full (potentially large) JSON blob each time.
    latest_ts = get_latest_cache_ts(today)
    with _cache_lock:
        if (_cache["date"] == today and _cache["data"]
                and latest_ts is not None and latest_ts <= (_cache["ts"] or 0)):
            _cache["checked_at"] = now
            return _cache["data"]

    # 3. Try today's data from DB
    data, data_ts = get_cached_with_ts()
    data_date = today

    # 4. If no data for today, serve the most recent available snapshot
    #    (covers: first boot, mid-refresh, or generate.py hasn't run yet today)
    if not data:
        data, data_date, data_ts = get_any_cached_with_date()

    # 5. Still nothing — DB is empty (generate.py has never run)
    if not data:
        return []

    # Warm the in-memory cache using the data's actual date, not today.
    # This ensures tomorrow's request re-checks the DB for a fresh row
    # rather than serving this stale data indefinitely.
    with _cache_lock:
        _cache["data"] = data
        _cache["date"] = data_date
        _cache["ts"] = data_ts
        _cache["checked_at"] = now

    return data

def get_latest_cache_ts(date_str: str) -> int | None:
    """Cheap check: return the `ts` of the cache row for `date_str`,
    without fetching the (potentially multi-MB) `data` column. Used to
    detect that generate.py has checkpointed newer data since we last
    warmed the in-memory cache, without paying for a full JSON re-parse
    on every request."""
    con = get_db()
    row = con.execute("SELECT ts FROM cache WHERE date=?", (date_str,)).fetchone()
    con.close()
    return row[0] if row else None

def get_any_cached_with_date():
    """Like get_any_cached() but also returns the row's date and ts so the
    in-memory cache can be keyed to the data's actual date/timestamp rather
    than today — preventing stale data from being served indefinitely."""
    con = get_db()
    row = con.execute(
        "SELECT data, date, ts FROM cache ORDER BY ts DESC LIMIT 1"
    ).fetchone()
    con.close()
    if row:
        return json.loads(row[0]), row[1], row[2]
    return None, None, None

def render_analyst_track_record() -> str:
    yr = datetime.date.today().year
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Analyst Track Record | StockUpside.io</title>
  <meta name="description" content="How accurate have Wall Street analyst price targets been? Track consensus accuracy by stock, sector, and rating type."/>
  <meta property="og:title"       content="Analyst Track Record | StockUpside.io"/>
  <meta property="og:description" content="Wall Street analyst accuracy tracked at 30, 60, and 90 days. See which sectors and ratings have the best track records."/>
  <meta property="og:url"         content="https://stockupside.io/analyst-track-record"/>
  <meta property="og:image"       content="https://stockupside.io/og-image.png"/>
  <meta name="twitter:card"       content="summary_large_image"/>
  <link rel="stylesheet" href="/style.css"/>
  <style>
    .atr-wrap {{ max-width:1100px; margin:0 auto; padding:32px 20px 64px; }}
    .atr-wrap h1 {{ font-family:var(--font-mono); font-size:22px; margin-bottom:6px; }}
    .atr-sub {{ color:var(--text2); font-size:13px; margin-bottom:32px; line-height:1.6; }}

    /* ── Summary cards ── */
    .atr-cards {{ display:grid; grid-template-columns:repeat(4,1fr); gap:16px; margin-bottom:32px; }}
    @media(max-width:768px) {{ .atr-cards {{ grid-template-columns:1fr 1fr; }} }}
    @media(max-width:480px) {{ .atr-cards {{ grid-template-columns:1fr; }} }}
    .atr-card {{ background:var(--bg2); border:1px solid var(--border); border-radius:8px; padding:20px; }}
    .atr-card-lbl {{ font-family:var(--font-mono); font-size:9px; color:var(--text3);
                     letter-spacing:.12em; margin-bottom:10px; }}
    .atr-card-val {{ font-family:var(--font-mono); font-size:36px; font-weight:700;
                     line-height:1; margin-bottom:4px; }}
    .atr-card-sub {{ font-size:11px; color:var(--text2); }}

    /* ── Period tabs ── */
    .atr-tabs {{ display:flex; gap:0; margin-bottom:28px;
                 border:1px solid var(--border); border-radius:6px;
                 overflow:hidden; width:fit-content; }}
    .atr-tab {{ padding:8px 20px; font-family:var(--font-mono); font-size:11px;
                font-weight:600; letter-spacing:.06em; cursor:pointer;
                background:var(--bg2); color:var(--text2); border:none;
                transition:all .15s; }}
    .atr-tab:hover {{ background:var(--bg3); color:var(--text); }}
    .atr-tab.active {{ background:var(--accent); color:#000; }}

    /* ── Tables ── */
    .atr-section {{ margin-bottom:36px; }}
    .atr-section-title {{ font-family:var(--font-mono); font-size:11px; color:var(--text3);
                          letter-spacing:.12em; margin-bottom:16px; }}
    .atr-table {{ width:100%; border-collapse:collapse; font-size:12px; }}
    .atr-table th {{ padding:9px 14px; text-align:left; font-family:var(--font-mono);
                     font-size:9px; color:var(--text3); letter-spacing:.1em;
                     background:var(--bg2); border-bottom:2px solid var(--border); }}
    .atr-table td {{ padding:10px 14px; border-bottom:1px solid var(--border); }}
    .atr-table tr:hover td {{ background:var(--bg2); }}
    .atr-table tr:last-child td {{ border-bottom:none; }}

    /* ── Hit rate bar ── */
    .hr-bar {{ display:flex; align-items:center; gap:10px; }}
    .hr-track {{ flex:1; height:6px; background:var(--bg3); border-radius:3px; overflow:hidden; min-width:80px; }}
    .hr-fill {{ height:100%; border-radius:3px; transition:width .3s; }}
    .hr-pct {{ font-family:var(--font-mono); font-size:12px; font-weight:700; width:42px; }}

    /* ── Best/Worst picks ── */
    .picks-grid {{ display:grid; grid-template-columns:1fr 1fr; gap:24px; margin-bottom:32px; }}
    @media(max-width:640px) {{ .picks-grid {{ grid-template-columns:1fr; }} }}
    .picks-panel {{ background:var(--bg2); border:1px solid var(--border); border-radius:8px; overflow:hidden; }}
    .picks-hdr {{ padding:14px 18px; border-bottom:1px solid var(--border);
                  font-family:var(--font-mono); font-size:11px; font-weight:700;
                  letter-spacing:.08em; }}
    .pick-row {{ display:flex; align-items:center; gap:12px; padding:10px 18px;
                 border-bottom:1px solid var(--border); text-decoration:none; transition:background .1s; }}
    .pick-row:last-child {{ border-bottom:none; }}
    .pick-row:hover {{ background:var(--bg3); }}
    .pick-tk {{ font-family:var(--font-mono); font-size:13px; font-weight:700;
                color:var(--accent); width:52px; flex-shrink:0; }}
    .pick-ret {{ font-family:var(--font-mono); font-size:13px; font-weight:700; margin-left:auto; }}
    .pick-detail {{ font-size:11px; color:var(--text3); flex:1; }}

    /* ── No data state ── */
    .no-data-card {{ background:var(--bg2); border:1px solid var(--border); border-radius:8px;
                     padding:48px; text-align:center; }}
    .no-data-icon {{ font-size:32px; margin-bottom:16px; }}
    .no-data-title {{ font-family:var(--font-mono); font-size:14px; margin-bottom:8px; color:var(--text); }}
    .no-data-sub {{ font-size:13px; color:var(--text2); line-height:1.6; max-width:420px; margin:0 auto; }}
    .no-data-date {{ display:inline-block; margin-top:16px; font-family:var(--font-mono);
                     font-size:11px; color:var(--accent); background:rgba(240,180,41,.1);
                     border:1px solid rgba(240,180,41,.2); border-radius:4px; padding:6px 14px; }}
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
    <a href="/accuracy" style="font-family:var(--font-mono);font-size:11px;
       color:var(--text2);margin-right:16px">Accuracy</a>
    <a href="/" style="font-family:var(--font-mono);font-size:11px;color:var(--text2)">← Dashboard</a>
  </div>
</header>

<div class="atr-wrap">
  <h1>Analyst Track Record</h1>
  <p class="atr-sub">
    How often do Wall Street price targets actually get hit?
    We track every analyst consensus prediction in our database and measure
    real outcomes at 30, 60, and 90 days. Data builds automatically —
    check back as snapshots accumulate.
  </p>

  <div id="atr-content">
    <div style="text-align:center;padding:40px;color:var(--text3);
                font-family:var(--font-mono);font-size:12px">Loading…</div>
  </div>
</div>

<footer class="ftr">
  <div>© {yr} StockUpside.io · Not financial advice</div>
  <div class="ftr-r">
    <a href="/accuracy">Accuracy</a> ·
    <a href="/changes">Changes</a> ·
    <a href="/stocks">All Stocks</a>
  </div>
</footer>

<script>
let currentDays = 30;

function hitColor(rate) {{
  if (rate >= 60) return '#00e676';
  if (rate >= 40) return '#ffd740';
  return '#f85149';
}}

function retColor(r) {{
  return r >= 0 ? '#69f0ae' : '#f85149';
}}

function renderSummaryCards(data) {{
  const cp = data.checkpoints[30] || {{}};
  if (!cp.total) return `
    <div class="no-data-card">
      <div class="no-data-icon">⏳</div>
      <div class="no-data-title">Building accuracy data...</div>
      <div class="no-data-sub">
        We need 30 days of price history to measure analyst accuracy
        at this interval. Data collection started on
        <strong>${{data.collection_started}}</strong>.
      </div>
      <div class="no-data-date">
        Check back after ${{getCheckDate(data.collection_started, 30)}}
      </div>
    </div>`;

  const col = hitColor(cp.hit_rate);
  return `
    <div class="atr-cards">
      <div class="atr-card">
        <div class="atr-card-lbl">HIT RATE ({30}D)</div>
        <div class="atr-card-val" style="color:${{col}}">${{cp.hit_rate}}%</div>
        <div class="atr-card-sub">of targets reached</div>
      </div>
      <div class="atr-card">
        <div class="atr-card-lbl">STOCKS TRACKED</div>
        <div class="atr-card-val">${{cp.total}}</div>
        <div class="atr-card-sub">${{cp.hits}} targets hit</div>
      </div>
      <div class="atr-card">
        <div class="atr-card-lbl">AVG RETURN (ALL)</div>
        <div class="atr-card-val" style="color:${{retColor(cp.avg_return)}}">
          ${{cp.avg_return >= 0 ? '+' : ''}}${{cp.avg_return}}%
        </div>
        <div class="atr-card-sub">following consensus</div>
      </div>
      <div class="atr-card">
        <div class="atr-card-lbl">AVG RETURN (HITS)</div>
        <div class="atr-card-val" style="color:#69f0ae">
          +${{cp.avg_return_hits}}%
        </div>
        <div class="atr-card-sub">when target was reached</div>
      </div>
    </div>`;
}}

function renderByConsensus(data) {{
  if (!data.by_consensus.length) return '';
  return `
    <div class="atr-section">
      <div class="atr-section-title">ACCURACY BY CONSENSUS RATING ({30}-DAY)</div>
      <table class="atr-table">
        <thead><tr>
          <th>RATING</th>
          <th>STOCKS TRACKED</th>
          <th>HIT RATE</th>
          <th>AVG RETURN</th>
          <th>AVG RETURN (HITS)</th>
        </tr></thead>
        <tbody>
          ${{data.by_consensus.map(r => {{
            const col = hitColor(r.hit_rate);
            const ratingColor = {{"Strong Buy":"#00e676","Buy":"#69f0ae","Hold":"#ffd740",
                                  "Underperform":"#ff5252","Sell":"#d50000"}}[r.consensus] || "#aaa";
            return `<tr>
              <td><span style="color:${{ratingColor}};font-weight:700">${{r.consensus}}</span></td>
              <td style="font-family:var(--font-mono)">${{r.total}}</td>
              <td>
                <div class="hr-bar">
                  <div class="hr-track">
                    <div class="hr-fill" style="width:${{r.hit_rate}}%;background:${{col}}"></div>
                  </div>
                  <span class="hr-pct" style="color:${{col}}">${{r.hit_rate}}%</span>
                </div>
              </td>
              <td style="font-family:var(--font-mono);color:${{retColor(r.avg_return)}}">
                ${{r.avg_return >= 0 ? '+' : ''}}${{r.avg_return}}%
              </td>
              <td style="font-family:var(--font-mono);color:#69f0ae">
                +${{r.avg_return_hits || 0}}%
              </td>
            </tr>`;
          }}).join('')}}
        </tbody>
      </table>
    </div>`;
}}

function renderBySector(data) {{
  if (!data.by_sector.length) return '';
  return `
    <div class="atr-section">
      <div class="atr-section-title">ACCURACY BY SECTOR (90-DAY)</div>
      <table class="atr-table">
        <thead><tr>
          <th>SECTOR</th>
          <th>STOCKS</th>
          <th>HIT RATE</th>
          <th>AVG RETURN</th>
        </tr></thead>
        <tbody>
          ${{data.by_sector.map(r => {{
            const col = hitColor(r.hit_rate);
            return `<tr>
              <td style="color:var(--text)">${{r.sector}}</td>
              <td style="font-family:var(--font-mono)">${{r.total}}</td>
              <td>
                <div class="hr-bar">
                  <div class="hr-track">
                    <div class="hr-fill" style="width:${{r.hit_rate}}%;background:${{col}}"></div>
                  </div>
                  <span class="hr-pct" style="color:${{col}}">${{r.hit_rate}}%</span>
                </div>
              </td>
              <td style="font-family:var(--font-mono);color:${{retColor(r.avg_return)}}">
                ${{r.avg_return >= 0 ? '+' : ''}}${{r.avg_return}}%
              </td>
            </tr>`;
          }}).join('')}}
        </tbody>
      </table>
    </div>`;
}}

function renderTopPicks(data) {{
  if (!data.top_performers.length && !data.worst_performers.length) return '';
  
  const topRows = data.top_performers.map(r => `
    <a href="/stocks/${{r.ticker}}" class="pick-row">
      <span class="pick-tk">${{r.ticker}}</span>
      <span class="pick-detail">${{r.days_later}}d · target ${{r.hit_target ? 'hit ✓' : 'missed'}}</span>
      <span class="pick-ret" style="color:#69f0ae">
        +${{r.actual_return}}%
      </span>
    </a>`).join('');

  const worstRows = data.worst_performers.map(r => `
    <a href="/stocks/${{r.ticker}}" class="pick-row">
      <span class="pick-tk">${{r.ticker}}</span>
      <span class="pick-detail">${{r.days_later}}d · target ${{r.hit_target ? 'hit ✓' : 'missed'}}</span>
      <span class="pick-ret" style="color:#f85149">
        ${{r.actual_return}}%
      </span>
    </a>`).join('');

  return `
    <div class="picks-grid">
      <div class="picks-panel">
        <div class="picks-hdr" style="color:#00e676">★ BEST CALLS</div>
        ${{topRows || '<div style="padding:20px;color:var(--text3);font-size:12px">No data yet</div>'}}
      </div>
      <div class="picks-panel">
        <div class="picks-hdr" style="color:#f85149">✗ WORST CALLS</div>
        ${{worstRows || '<div style="padding:20px;color:var(--text3);font-size:12px">No data yet</div>'}}
      </div>
    </div>`;
}}

function getCheckDate(startDate, days) {{
  const d = new Date(startDate + "T00:00:00");
  d.setDate(d.getDate() + days);
  return d.toLocaleDateString("en-US", {{ month: "long", day: "numeric" }});
}}

function renderAll(data) {{
  const hasSomeData = Object.values(data.checkpoints).some(cp => cp.total > 0);
  
  const tabsHtml = `
    <div class="atr-tabs" style="margin-bottom:28px">
      <button class="atr-tab ${{30===30?'active':''}}" data-days="30">30 Days</button>
      <button class="atr-tab ${{30===60?'active':''}}" data-days="60">60 Days</button>
      <button class="atr-tab ${{30===90?'active':''}}" data-days="90">90 Days</button>
    </div>`;

  document.getElementById('atr-content').innerHTML =
    tabsHtml +
    renderSummaryCards(data) +
    (hasSomeData ? renderTopPicks(data) : '') +
    renderByConsensus(data) +
    renderBySector(data);

  // Bind tabs
  document.querySelectorAll('.atr-tab').forEach(btn => {{
    btn.addEventListener('click', () => {{
      30 = parseInt(btn.dataset.days);
      renderAll(data);
    }});
  }});
}}

fetch('/api/accuracy')
  .then(r => r.json())
  .then(data => renderAll(data))
  .catch(() => {{
    document.getElementById('atr-content').innerHTML =
      '<div class="no-data-card"><div class="no-data-title">Failed to load data</div></div>';
  }});
</script>
</body>
</html>"""

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
    const upVal = s.curr_upside || s.upside_pct;
    const upCol = upVal >= 0 ? "#69f0ae" : "#f85149";
    return `<a href="/stocks/${{s.ticker}}" class="ch-row">
      <span class="ch-rank">${{i+1}}</span>
      <span class="ch-ticker">${{s.ticker}}</span>
      <span class="ch-change" style="color:var(--text2)">New analyst coverage</span>
      <span class="ch-upside" style="color:${{upCol}}">${{upVal >= 0 ? '+' : ''}}${{upVal}}%</span>
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

@app.after_request
def security_headers(r):
    # Baseline hardening headers. HSTS is set at the nginx layer (only
    # appropriate there, since it must not be sent over plain HTTP).
    r.headers["X-Content-Type-Options"] = "nosniff"
    r.headers["X-Frame-Options"]        = "DENY"
    r.headers["Referrer-Policy"]        = "strict-origin-when-cross-origin"
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

# Resend (https://resend.com) — preferred email provider. Uses their HTTP
# API (no SMTP connection overhead, better deliverability diagnostics).
# Get an API key from https://resend.com/api-keys and verify your sending
# domain at https://resend.com/domains before setting this.
_RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
_RESEND_API_URL = "https://api.resend.com/emails"


def send_email(to: str, subject: str, html: str, text: str = "") -> bool:
    """Send a single email. Returns True on success, False on failure.

    Provider priority:
      1. Resend HTTP API (if RESEND_API_KEY is set) — recommended.
      2. Generic SMTP (if SMTP_HOST is set) — works with Resend's SMTP
         relay too (smtp.resend.com, port 587, user "resend",
         password = your API key) if you'd rather not use the HTTP API.
      3. Dev mode (neither configured) — logs to stdout, returns True so
         the calling code's flow (digest counts, etc.) isn't affected.
    """
    if _RESEND_API_KEY:
        return _send_via_resend(to, subject, html, text)
    if _SMTP_HOST:
        return _send_via_smtp(to, subject, html, text)

    print(f"  [EMAIL DEV] To: {to} | Subject: {subject}")
    return True


def _send_via_resend(to: str, subject: str, html: str, text: str = "") -> bool:
    """Send via Resend's HTTP API: https://resend.com/docs/api-reference/emails/send-email"""
    payload = {
        "from": _EMAIL_FROM,
        "to": [to],
        "subject": subject,
        "html": html,
    }
    if text:
        payload["text"] = text

    req = urllib.request.Request(
        _RESEND_API_URL,
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "Authorization": f"Bearer {_RESEND_API_KEY}",
            "Content-Type": "application/json",
            "User-Agent": "StockUpside.io/1.0 (+https://stockupside.io)",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            if 200 <= resp.status < 300:
                return True
            print(f"  ⚠  Resend returned HTTP {resp.status} for {to}")
            return False
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        print(f"  ⚠  Resend error sending to {to}: HTTP {e.code} — {body}")
        return False
    except Exception as e:
        print(f"  ⚠  Resend request failed for {to}: {e}")
        return False


def _send_via_smtp(to: str, subject: str, html: str, text: str = "") -> bool:
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
    token = hashlib.sha256(f"unsub:{email_addr}:{_APP_SECRET}".encode()).hexdigest()[:24]
    return f"{_SITE_URL}/unsubscribe?email={email_addr}&token={token}"


# ── Per-subscriber digest filter preferences (Pro feature) ────────────────────
DEFAULT_EMAIL_PREFS = {
    "sector": "All", "consensus": "All", "min_analysts": 0,
    "max_pe": 0, "max_peg": 0, "momentum": "All",
}

def get_email_prefs(email_addr: str) -> dict:
    """Return saved digest filter preferences for a subscriber, or the
    defaults (no filters, top-10 overall) if none have been saved."""
    con = get_db()
    row = con.execute(
        "SELECT sector, consensus, min_analysts, max_pe, max_peg, momentum "
        "FROM email_preferences WHERE email=?", (email_addr,)
    ).fetchone()
    con.close()
    if not row:
        return dict(DEFAULT_EMAIL_PREFS)
    return {
        "sector": row[0], "consensus": row[1], "min_analysts": row[2],
        "max_pe": row[3], "max_peg": row[4], "momentum": row[5],
    }

def set_email_prefs(email_addr: str, prefs: dict) -> None:
    """Upsert digest filter preferences for a subscriber."""
    con = get_db()
    con.execute("""
        INSERT INTO email_preferences
            (email, sector, consensus, min_analysts, max_pe, max_peg, momentum, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(email) DO UPDATE SET
            sector=excluded.sector, consensus=excluded.consensus,
            min_analysts=excluded.min_analysts, max_pe=excluded.max_pe,
            max_peg=excluded.max_peg, momentum=excluded.momentum,
            updated_at=excluded.updated_at
    """, (
        email_addr,
        prefs.get("sector", "All"), prefs.get("consensus", "All"),
        int(prefs.get("min_analysts", 0) or 0),
        float(prefs.get("max_pe", 0) or 0), float(prefs.get("max_peg", 0) or 0),
        prefs.get("momentum", "All"), int(time.time()),
    ))
    con.commit()
    con.close()

def apply_email_filters(stocks: list, prefs: dict, limit: int = 10) -> list:
    """Filter the full stock list per a subscriber's saved preferences and
    return up to `limit` results, preserving the existing upside-based
    ranking order. Mirrors the frontend's applyFilters() logic in main.ts.

    If the filtered set has fewer than `limit` results, it is NOT padded —
    the caller decides whether to send a shorter list or fall back to
    the unfiltered top-10. See digest_email_html() for the fallback note.
    """
    s = [x for x in stocks if not x.get("locked")]

    sector = prefs.get("sector", "All")
    if sector and sector != "All":
        s = [x for x in s if x.get("sector") == sector]

    consensus = prefs.get("consensus", "All")
    if consensus and consensus != "All":
        s = [x for x in s if x.get("consensus") == consensus]

    min_analysts = int(prefs.get("min_analysts", 0) or 0)
    if min_analysts > 0:
        s = [x for x in s if x.get("analyst_count", 0) >= min_analysts]

    max_pe = float(prefs.get("max_pe", 0) or 0)
    if max_pe > 0:
        s = [x for x in s if 0 < x.get("pe_ratio", 0) <= max_pe]

    max_peg = float(prefs.get("max_peg", 0) or 0)
    if max_peg > 0:
        s = [x for x in s if 0 < x.get("peg_ratio", 0) <= max_peg]

    momentum = prefs.get("momentum", "All")
    if momentum and momentum != "All":
        s = [x for x in s if x.get("momentum_trend") == momentum]

    return s[:limit]

def _resolve_token_email(token: str) -> str | None:
    """Resolve a Pro session token to a subscriber email. Returns None if
    the token doesn't exist, has expired, or the subscriber is no longer
    on the Pro plan (e.g. they cancelled)."""
    if not token:
        return None
    now = int(time.time())
    con = get_db()
    row = con.execute(
        "SELECT s.email, s.expires_at, sub.plan "
        "FROM sessions s LEFT JOIN subscribers sub ON sub.email = s.email "
        "WHERE s.token = ?", (token,)
    ).fetchone()
    if not row:
        con.close()
        return None
    email_addr, expires_at, plan = row
    if expires_at < now or plan != "pro":
        # Expired or no longer Pro — clean up the dangling session.
        con.execute("DELETE FROM sessions WHERE token=?", (token,))
        con.commit()
        con.close()
        return None
    con.execute("UPDATE sessions SET last_seen=? WHERE token=?", (now, token))
    con.commit()
    con.close()
    return email_addr

# ── Session-based Pro access tokens ────────────────────────────────────────────
# Tokens are cryptographically random (32 bytes, urlsafe), stored server-side
# in `sessions` with an expiry, and resolved by direct DB lookup rather than
# by recomputing a deterministic hash. This means:
#   - A leaked token has no PII and stops working after SESSION_TTL_DAYS.
#   - Tokens can be revoked individually (e.g. "log out everywhere").
#   - Possessing a token proves nothing about WHO you are beyond "had a
#     valid session at some point" — same as any other session cookie scheme.
SESSION_TTL_DAYS = 30

def create_session(email_addr: str) -> str:
    """Issue a new random session token for a Pro subscriber."""
    token = secrets.token_urlsafe(32)
    now   = int(time.time())
    expires = now + SESSION_TTL_DAYS * 86400
    con = get_db()
    con.execute(
        "INSERT INTO sessions (token, email, created_at, expires_at, last_seen) "
        "VALUES (?, ?, ?, ?, ?)",
        (token, email_addr, now, expires, now),
    )
    con.commit()
    con.close()
    return token

def revoke_session(token: str) -> bool:
    """Invalidate a single session token (logout)."""
    con = get_db()
    cur = con.execute("DELETE FROM sessions WHERE token=?", (token,))
    con.commit()
    con.close()
    return cur.rowcount > 0

def revoke_all_sessions(email_addr: str) -> int:
    """Invalidate every session for a subscriber (e.g. 'sign out everywhere',
    or automatically when a subscription is cancelled)."""
    con = get_db()
    cur = con.execute("DELETE FROM sessions WHERE email=?", (email_addr,))
    con.commit()
    con.close()
    return cur.rowcount

def downgrade_subscriber(email_addr: str) -> bool:
    """Move a subscriber back to the free plan and revoke all of their
    Pro sessions. Called when Stripe reports a subscription was cancelled
    or payment failed permanently. Returns True if a row was updated."""
    con = get_db()
    cur = con.execute(
        "UPDATE subscribers SET plan='free' WHERE email=? AND plan='pro'",
        (email_addr,),
    )
    con.commit()
    con.close()
    revoke_all_sessions(email_addr)
    revoke_all_api_keys(email_addr)
    return cur.rowcount > 0


# ── API tier: key management ───────────────────────────────────────────────
# Keys are prefixed (su_live_...) so they're recognizable in logs, support
# tickets, and accidental commits — a bare random token gives no hint about
# what it is or where it leaked from. No expiry by default (unlike web
# sessions): API integrations break silently if a key rotates out from
# under them, so revocation is explicit (cancel subscription, or a future
# "regenerate key" action) rather than time-based.
API_KEY_PREFIX = "su_live_"

def create_api_key(email_addr: str, stripe_id: str | None = None, label: str | None = None) -> str:
    """Issue a new API key for an email. Does not check for existing keys —
    callers should decide whether to allow multiple keys per email."""
    api_key = API_KEY_PREFIX + secrets.token_urlsafe(32)
    now = int(time.time())
    con = get_db()
    con.execute(
        "INSERT INTO api_keys (api_key, email, stripe_id, plan, created_at, label) "
        "VALUES (?, ?, ?, 'active', ?, ?)",
        (api_key, email_addr, stripe_id, now, label),
    )
    con.commit()
    con.close()
    return api_key

def resolve_api_key(api_key: str) -> str | None:
    """Resolve an API key to its owning email. Returns None if the key
    doesn't exist or has been revoked. Updates last_used_at on success."""
    if not api_key:
        return None
    con = get_db()
    row = con.execute(
        "SELECT email, plan FROM api_keys WHERE api_key=?", (api_key,)
    ).fetchone()
    if not row or row[1] != "active":
        con.close()
        return None
    email_addr = row[0]
    con.execute(
        "UPDATE api_keys SET last_used_at=? WHERE api_key=?",
        (int(time.time()), api_key),
    )
    con.commit()
    con.close()
    return email_addr

def revoke_api_key(api_key: str) -> bool:
    """Mark a key as revoked rather than deleting it, so api_usage history
    (and any future billing-by-usage reporting) stays intact."""
    con = get_db()
    cur = con.execute(
        "UPDATE api_keys SET plan='revoked' WHERE api_key=?", (api_key,)
    )
    con.commit()
    con.close()
    return cur.rowcount > 0

def revoke_all_api_keys(email_addr: str) -> int:
    """Revoke every API key for an email — called when the underlying
    Stripe subscription is cancelled, mirroring revoke_all_sessions."""
    con = get_db()
    cur = con.execute(
        "UPDATE api_keys SET plan='revoked' WHERE email=?", (email_addr,)
    )
    con.commit()
    con.close()
    return cur.rowcount

# Daily request cap enforced server-side, independent of (and in addition
# to) the per-IP flask-limiter rate limit below. This is the number that
# actually matters for the $99/mo tier's fair-use policy, and unlike
# flask-limiter's in-memory store, it survives server restarts and is
# correct even if you later run multiple worker processes.
API_DAILY_REQUEST_LIMIT = 10_000

def check_and_record_api_usage(api_key: str) -> tuple[bool, int]:
    """Increment today's request count for this key and return
    (allowed, count_after_increment). Allowed is False once the key has
    hit API_DAILY_REQUEST_LIMIT for the day — callers should return 429."""
    today = datetime.date.today().isoformat()
    con = get_db()
    con.execute(
        "INSERT INTO api_usage (api_key, date, request_count) VALUES (?, ?, 1) "
        "ON CONFLICT(api_key, date) DO UPDATE SET request_count = request_count + 1",
        (api_key, today),
    )
    row = con.execute(
        "SELECT request_count FROM api_usage WHERE api_key=? AND date=?",
        (api_key, today),
    ).fetchone()
    con.commit()
    con.close()
    count = row[0] if row else 1
    return (count <= API_DAILY_REQUEST_LIMIT, count)


def _admin_authorized() -> bool:
    """Constant-time admin secret check. Plain `!=` on strings of
    different lengths returns immediately, leaking how many leading
    characters matched via response timing — `hmac.compare_digest`
    avoids this."""
    secret = os.environ.get("ADMIN_SECRET", "")
    provided = request.headers.get("X-Admin-Key", "")
    if not secret:
        return False
    return hmac.compare_digest(provided, secret)


def _unsubscribe_token_valid(email_addr: str, token: str) -> bool:
    expected = hashlib.sha256(f"unsub:{email_addr}:{_APP_SECRET}".encode()).hexdigest()[:24]
    # Constant-time comparison — plain `==` leaks timing information that
    # can be used to brute-force the token byte-by-byte.
    return hmac.compare_digest(token, expected)


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


def digest_email_html(stocks: list, email_addr: str, prefs: dict | None = None) -> tuple[str, str, str]:
    """Returns (subject, html, text) for the weekly digest.

    If `prefs` is provided (Pro subscribers with saved filters), the top
    picks are filtered accordingly. If the filtered set is empty, falls
    back to the unfiltered top-10 and notes this in the email so the
    subscriber isn't left with a blank digest.
    """
    today      = datetime.date.today().strftime("%B %d, %Y")
    unsub      = _unsubscribe_url(email_addr)
    is_custom  = bool(prefs) and prefs != DEFAULT_EMAIL_PREFS

    picks = apply_email_filters(stocks, prefs, limit=10) if prefs else None
    fell_back = False
    if not picks:
        picks     = [s for s in stocks if not s.get("locked")][:10]
        fell_back = is_custom

    rows_html = ""
    rows_txt  = ""
    for s in picks:
        color = ("#00e676" if s["upside_pct"] >= 40 else
                 "#69f0ae" if s["upside_pct"] >= 20 else
                 "#ffd740" if s["upside_pct"] >= 0  else
                 "#f85149")
        sign  = "+" if s["upside_pct"] >= 0 else ""
        rows_html += f"""
        <tr>
          <td style="padding:10px 12px;font-weight:700;color:#58a6ff;
                     font-family:monospace">{s['ticker']}</td>
          <td style="padding:10px 12px;color:#8b949e;font-size:12px;
                     max-width:160px">{s['name']}</td>
          <td style="padding:10px 12px;font-family:monospace;font-weight:700;
                     color:{color}">{sign}{s['upside_pct']}%</td>
          <td style="padding:10px 12px;color:#8b949e;font-size:12px">{s['consensus']}</td>
          <td style="padding:10px 12px;font-family:monospace;
                     color:#8b949e;font-size:11px">{s['analyst_count']} analysts</td>
        </tr>"""
        rows_txt += f"  {s['rank']:>2}. {s['ticker']:<6}  {sign}{s['upside_pct']}%  {s['consensus']}\n"

    if is_custom and not fell_back:
        eyebrow   = "YOUR TOP 10"
        subject   = f"▲ Your Top 10 Stock Picks — {today}"
    else:
        eyebrow   = "WEEKLY TOP 10"
        subject   = f"▲ Top 10 Stock Picks — {today}"

    fallback_notice_html = ""
    fallback_notice_txt  = ""
    if fell_back:
        fallback_notice_html = """
  <div style="background:#1f1500;border:1px solid #f0b429;border-radius:4px;
              padding:10px 14px;font-size:12px;color:#ffd740;margin-bottom:20px">
    ⚠ No stocks matched your saved filters this week — showing the overall
    Top 10 instead. Adjust your filters anytime from your account settings.
  </div>"""
        fallback_notice_txt = (
            "Note: No stocks matched your saved filters this week — "
            "showing the overall Top 10 instead.\n\n"
        )

    html = f"""<!DOCTYPE html>
<html><body style="font-family:monospace;background:#0d1117;color:#e6edf3;
                   padding:32px;max-width:620px;margin:0 auto">
  <div style="font-size:24px;font-weight:700;margin-bottom:4px">▲ StockUpside.io</div>
  <div style="color:#00e676;font-size:11px;letter-spacing:.1em;
              margin-bottom:8px">{eyebrow}</div>
  <div style="color:#8b949e;font-size:12px;margin-bottom:28px">{today}</div>
{fallback_notice_html}
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

    text_title = "Your Top 10 Stock Picks" if (is_custom and not fell_back) else "Top 10 Stock Picks"
    text = f"▲ StockUpside.io — {text_title} — {today}\n\n{fallback_notice_txt}{rows_txt}\nView full list: {_SITE_URL}\nUnsubscribe: {unsub}\n"
    return subject, html, text


# ── Market cap tiers (raw USD thresholds) ───────────────────────────────────
MARKET_CAP_TIERS = {
    "nano":   0,
    "micro":  50_000_000,
    "small":  250_000_000,
    "mid":    2_000_000_000,
    "large":  10_000_000_000,
}

# Defaults applied to the FREE tier's top-10 so low-quality/high-risk
# nano/micro caps with thin analyst coverage don't dominate the free list.
# Pro users can adjust both filters; these are just the free-tier baseline.
FREE_DEFAULT_MIN_MARKET_CAP = MARKET_CAP_TIERS["small"]  # >$250M
FREE_DEFAULT_MIN_ANALYSTS   = 5


# ── CSV export ─────────────────────────────────────────────────────────────────
CSV_COLUMNS = [
    ("Rank",             "rank",            None),
    ("Ticker",           "ticker",          None),
    ("Company",          "name",            None),
    ("Sector",           "sector",          None),
    ("Current Price",    "current_price",   None),
    ("Analyst Target",   "target_price",    None),
    ("Upside %",         "upside_pct",      None),
    ("Bull Target",      "high_target",     None),
    ("Bear Target",      "low_target",      None),
    ("# Analysts",       "analyst_count",   None),
    ("Consensus",        "consensus",       None),
    ("Strong Buy",       "strong_buy",      None),
    ("Buy",              "buy",             None),
    ("Hold",             "hold",            None),
    ("Sell",             "sell",            None),
    ("Market Cap",       "market_cap",      None),
    ("Market Cap (Raw)", "market_cap_raw",  None),
    ("P/E (TTM)",        "pe_ratio",        None),
    ("Forward P/E",      "forward_pe",      None),
    ("PEG Ratio",        "peg_ratio",       None),
    ("EPS (TTM)",        "eps",             None),
    ("Revenue",          "revenue",         None),
    ("Profit Margin",    "profit_margin",   lambda v: f"{round(v*100, 2)}%" if v else ""),
    ("Dividend Yield",   "dividend_yield",  None),
    ("YTD Change %",     "ytd_change",      None),
    ("52W Low",          "week52_low",      None),
    ("52W High",         "week52_high",     None),
    ("Avg Volume",       "avg_volume",      None),
    ("Momentum",         "momentum_trend",  None),
    ("Momentum Detail",  "momentum_detail", None),
    ("Last Updated",     "last_updated",    None),
]


@app.route("/api/export/csv")
@limiter.limit("20 per hour")
def api_export_csv():
    token      = (request.args.get("token", "")
                  or request.headers.get("Authorization", "").removeprefix("Bearer ").strip())
    email_addr = _resolve_token_email(token)
    if not email_addr:
        # If this looks like a browser navigating directly to the URL
        # (rather than a fetch() call from the frontend), redirect to the
        # site instead of returning a raw JSON 403 that looks like a bug.
        accepts_html = "text/html" in request.headers.get("Accept", "")
        if accepts_html:
            return redirect("/?export=login", code=302)
        return jsonify({"error": "Pro subscription required — pass ?token=YOUR_TOKEN"}), 403

    stocks = get_stocks_cached()
    if not stocks:
        return jsonify({"error": "Stock data not yet available — try again shortly"}), 503

    scope = request.args.get("scope", "all").strip().lower()
    if scope == "watchlist":
        con  = get_db()
        rows = con.execute(
            "SELECT ticker FROM watchlists WHERE email=?", (email_addr,)
        ).fetchall()
        con.close()
        wl_tickers = {r[0] for r in rows}
        stocks = [s for s in stocks if s["ticker"] in wl_tickers]
        if not stocks:
            return jsonify({"error": "Your watchlist is empty"}), 404
        filename = f"stockupside-watchlist-{datetime.date.today()}.csv"
    else:
        filename = f"stockupside-export-{datetime.date.today()}.csv"

    buf    = io.StringIO()
    writer = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
    writer.writerow([col[0] for col in CSV_COLUMNS])
    for s in stocks:
        row = []
        for _header, key, transform in CSV_COLUMNS:
            val = s.get(key, "")
            if val is None:
                val = ""
            elif transform:
                val = transform(val)
            row.append(val)
        writer.writerow(row)

    csv_bytes = buf.getvalue().encode("utf-8-sig")
    return Response(
        csv_bytes,
        mimetype="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Type": "text/csv; charset=utf-8",
            "Cache-Control": "no-store",
        },
    )


# ── API Routes ─────────────────────────────────────────────────────────────────
@app.route("/api/stocks")
@limiter.limit("600 per hour")
def api_stocks():
    # SECURITY: tier is determined server-side from a verified session
    # token — never trust a client-supplied `?tier=pro` query param.
    # Previously this endpoint returned the full Pro dataset to anyone
    # who requested `?tier=pro` directly, with zero authentication.
    token = (
        request.args.get("token", "")
        or request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
    )
    tier = "pro" if _resolve_token_email(token) else "free"

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

    # Free tier: apply default quality filters (min market cap + min
    # analyst coverage) so high-risk, thinly-covered nano/micro caps don't
    # dominate the free top-10. Original global `rank` is preserved so
    # free users see where these stocks actually rank overall.
    min_cap      = FREE_DEFAULT_MIN_MARKET_CAP
    min_analysts = FREE_DEFAULT_MIN_ANALYSTS

    eligible = [
        s for s in stocks
        if (s.get("market_cap_raw") or 0) >= min_cap
        and (s.get("analyst_count") or 0) >= min_analysts
    ]

    free_set   = eligible[:10]
    free_idx   = {s["ticker"] for s in free_set}
    remainder  = [s for s in stocks if s["ticker"] not in free_idx]

    teaser = [{"rank": s["rank"], "ticker": "???", "name": "Unlock Pro to reveal",
               "upside_pct": s["upside_pct"], "consensus": s["consensus"],
               "sector": s["sector"], "locked": True} for s in remainder]
    return jsonify({"stocks": free_set + teaser, "total": len(stocks),
                    "tier": "free", "last_updated": today.isoformat(),
                    "next_update": nxt,
                    "free_filters": {"min_market_cap": min_cap,
                                     "min_analysts": min_analysts}})

@app.route("/api/stats")
@limiter.limit("600 per hour")
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


# ── API v1 — programmatic access tier ($99/mo) ────────────────────────────
# Separate namespace (/api/v1/...) from the existing /api/* routes, which
# are the SPA's own internal endpoints (session-token auth, free/pro
# tiering baked in, shapes tuned for the frontend). v1 is a distinct
# contract aimed at external consumers: API-key auth, stable field names,
# and no silent shape changes without bumping to v2.

def _extract_api_key() -> str:
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        return auth.removeprefix("Bearer ").strip()
    # Also accept ?api_key=... for convenience (e.g. quick browser testing),
    # though the header form is what we document and recommend — query
    # params end up in server logs and browser history.
    return request.args.get("api_key", "").strip()

def require_api_key(view_func):
    """Decorator: resolves the API key, enforces the daily usage cap, and
    records the request — or returns the appropriate 401/429 JSON error.
    Wraps the view rather than duplicating this in every v1 route."""
    @wraps(view_func)
    def wrapped(*args, **kwargs):
        api_key = _extract_api_key()
        if not api_key:
            return jsonify({
                "error": "Missing API key. Pass it as 'Authorization: Bearer YOUR_KEY'.",
                "docs": "https://stockupside.io/api/docs",
            }), 401

        email_addr = resolve_api_key(api_key)
        if not email_addr:
            return jsonify({
                "error": "Invalid or revoked API key.",
                "docs": "https://stockupside.io/api/docs",
            }), 401

        allowed, count_today = check_and_record_api_usage(api_key)
        if not allowed:
            return jsonify({
                "error": f"Daily request limit of {API_DAILY_REQUEST_LIMIT} exceeded.",
                "requests_today": count_today,
                "limit": API_DAILY_REQUEST_LIMIT,
                "resets": "midnight UTC",
            }), 429

        return view_func(*args, **kwargs)
    return wrapped

# Per-key rate limiting (bursty protection, independent of the daily cap
# above which guards total volume). Keyed on the API key itself rather
# than IP, since legitimate API customers may call from a shared
# server/cloud IP with many other unrelated tenants behind it.
def _api_key_for_limiter() -> str:
    return _extract_api_key() or get_remote_address()

@app.route("/api/v1/stocks")
@limiter.limit("120 per minute", key_func=_api_key_for_limiter)
@require_api_key
def api_v1_stocks():
    """List stocks ranked by analyst consensus upside. No free/pro tiering
    here — the API tier itself is the paywall, so an authenticated request
    always gets the full dataset, just paginated.

    Query params:
      limit   — max rows to return (default 100, max 500)
      offset  — pagination offset (default 0)
      sector  — exact sector match, e.g. "Technology"
      min_analysts — minimum analyst_count
    """
    stocks = get_stocks_cached()
    if not stocks:
        return jsonify({"error": "Data is being generated, check back shortly.", "stocks": []}), 503

    sector = request.args.get("sector", "").strip()
    if sector:
        stocks = [s for s in stocks if s.get("sector", "").lower() == sector.lower()]

    try:
        min_analysts = int(request.args.get("min_analysts", 0))
    except ValueError:
        min_analysts = 0
    if min_analysts > 0:
        stocks = [s for s in stocks if (s.get("analyst_count") or 0) >= min_analysts]

    try:
        limit = max(1, min(500, int(request.args.get("limit", 100))))
    except ValueError:
        limit = 100
    try:
        offset = max(0, int(request.args.get("offset", 0)))
    except ValueError:
        offset = 0

    page = stocks[offset:offset + limit]
    return jsonify({
        "stocks": page,
        "count": len(page),
        "total_matching": len(stocks),
        "limit": limit,
        "offset": offset,
        "last_updated": stocks[0].get("last_updated") if stocks else None,
    })

@app.route("/api/v1/stocks/<ticker>")
@limiter.limit("120 per minute", key_func=_api_key_for_limiter)
@require_api_key
def api_v1_stock_detail(ticker):
    """Full data for a single ticker, including the same similar-stocks
    list shown on the public /stocks/<ticker> page."""
    ticker = ticker.upper()
    stocks = get_stocks_cached()
    stock  = next((s for s in stocks if s["ticker"] == ticker), None)
    if not stock:
        return jsonify({"error": f"Ticker '{ticker}' not found in tracked universe."}), 404

    similar = similar_stocks(stock, stocks)
    return jsonify({
        "stock": stock,
        "similar_stocks": similar,
    })

@app.route("/api/v1/usage")
@limiter.limit("30 per minute", key_func=_api_key_for_limiter)
@require_api_key
def api_v1_usage():
    """Self-service usage check, so customers can see how close they are
    to the daily cap without emailing support."""
    api_key = _extract_api_key()
    today = datetime.date.today().isoformat()
    con = get_db()
    row = con.execute(
        "SELECT request_count FROM api_usage WHERE api_key=? AND date=?",
        (api_key, today),
    ).fetchone()
    con.close()
    return jsonify({
        "requests_today": row[0] if row else 0,
        "daily_limit": API_DAILY_REQUEST_LIMIT,
        "date": today,
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

def _track_server_event(event_name: str, props: dict | None = None):
    """Fire a custom event to Plausible's events API from the server.
    Used for events that happen across a redirect (Stripe checkout
    completion), where client-side JS can't confirm the outcome. Best
    effort — failures here never block the actual request."""
    domain = os.environ.get("PLAUSIBLE_DOMAIN", "stockupside.io")
    payload = json.dumps({
        "domain": domain, "name": event_name,
        "url": f"https://{domain}/", "props": props or {},
    }).encode("utf-8")
    try:
        req = urllib.request.Request(
            "https://plausible.io/api/event", data=payload,
            headers={"Content-Type": "application/json", "User-Agent": "stockupside-server/1.0"},
        )
        urllib.request.urlopen(req, timeout=3)
    except Exception as e:
        print(f"  ⚠  Plausible server event '{event_name}' failed (non-fatal): {e}")

@app.route("/api/docs")
def api_docs_page():
    """Public, indexable API documentation. No auth required to view —
    the docs page itself is a top-of-funnel SEO asset (per the marketing
    plan), separate from the actual v1 endpoints which do require a key."""
    return Response(_render_api_docs_page(), mimetype="text/html")

def _render_api_docs_page() -> str:
    return """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>API Documentation | StockUpside.io</title>
  <meta name="description" content="Programmatic access to analyst consensus upside data — ranked stocks, price targets, and analyst counts via a simple REST API. $99/mo."/>
  <meta property="og:title"       content="API Documentation | StockUpside.io"/>
  <meta property="og:description" content="Pull analyst consensus upside data programmatically. REST API, JSON responses, $99/mo flat."/>
  <meta property="og:url"         content="https://stockupside.io/api/docs"/>
  <meta property="og:image"       content="https://stockupside.io/og-image.png"/>
  <meta name="twitter:card"       content="summary_large_image"/>
  <link rel="stylesheet" href="/style.css"/>
  <style>
    .docs-wrap { max-width:920px; margin:0 auto; padding:32px 20px 80px; }
    .docs-wrap h1 { font-family:var(--font-mono); font-size:22px; margin-bottom:6px; }
    .docs-sub { color:var(--text2); font-size:13px; margin-bottom:32px; line-height:1.6; }
    .docs-wrap h2 { font-family:var(--font-mono); font-size:16px; margin:40px 0 14px;
                    padding-top:24px; border-top:1px solid var(--border); }
    .docs-wrap h2:first-of-type { border-top:none; padding-top:0; }
    .docs-wrap p { color:var(--text2); font-size:13px; line-height:1.7; margin-bottom:14px; }
    .docs-wrap code { font-family:var(--font-mono); font-size:12px; background:var(--bg2);
                       padding:2px 6px; border-radius:4px; color:var(--text); }
    pre { background:var(--bg2); border:1px solid var(--border); border-radius:8px;
          padding:16px 18px; overflow-x:auto; font-family:var(--font-mono); font-size:12px;
          line-height:1.6; margin:16px 0; }
    pre code { background:none; padding:0; }
    table { width:100%; border-collapse:collapse; margin:16px 0; font-size:12.5px; }
    th { text-align:left; color:var(--text3); font-family:var(--font-mono); font-size:10px;
         letter-spacing:.08em; padding:8px 12px; border-bottom:1px solid var(--border); }
    td { padding:8px 12px; border-bottom:1px solid var(--border); color:var(--text2); }
    td code { font-size:11px; }
    .docs-cta { background:var(--bg2); border:1px solid var(--border); border-radius:8px;
                padding:24px; margin:32px 0; text-align:center; }
    .docs-cta-price { font-family:var(--font-mono); font-size:28px; font-weight:700; margin-bottom:6px; }
    .docs-cta-sub { color:var(--text2); font-size:13px; margin-bottom:16px; }
    .docs-cta input { width:280px; max-width:100%; padding:10px 14px; border-radius:6px;
                       border:1px solid var(--border); background:var(--bg); color:var(--text);
                       font-size:13px; margin-right:8px; }
    .docs-cta button { padding:10px 20px; border-radius:var(--radius); border:none; background:var(--accent);
                        color:#000; font-weight:700; font-family:var(--font-mono); font-size:12px;
                        letter-spacing:.06em; cursor:pointer; transition:background .2s; }
    .docs-cta button:hover { background:var(--accent2); }
    .docs-toc { display:flex; flex-wrap:wrap; gap:8px; margin-bottom:32px; }
    .docs-toc a { font-family:var(--font-mono); font-size:11px; color:var(--text2);
                  border:1px solid var(--border); border-radius:6px; padding:6px 12px;
                  text-decoration:none; }
    .docs-toc a:hover { color:var(--text); border-color:var(--text2); }
  </style>
</head>
<body>
  <div class="docs-wrap">
    <h1>▲ StockUpside API</h1>
    <div class="docs-sub">
      Programmatic access to the same analyst consensus upside data that powers the site —
      ranked stocks, price targets, analyst counts, and sector breakdowns. JSON over REST,
      no SDK required.
    </div>

    <div class="docs-toc">
      <a href="#auth">Authentication</a>
      <a href="#stocks">GET /stocks</a>
      <a href="#detail">GET /stocks/:ticker</a>
      <a href="#usage">GET /usage</a>
      <a href="#limits">Rate limits</a>
      <a href="#pricing">Pricing</a>
    </div>

    <h2 id="auth">Authentication</h2>
    <p>
      Every request needs your API key in the <code>Authorization</code> header as a Bearer
      token. Keys look like <code>su_live_...</code> and are issued once, immediately after
      checkout — there's no separate "generate key" step.
    </p>
    <pre><code>curl -H "Authorization: Bearer su_live_YOUR_KEY" \\
  https://stockupside.io/api/v1/stocks</code></pre>
    <p>
      Lost your key? Email <a href="mailto:hello@stockupside.io">hello@stockupside.io</a> from
      the address you subscribed with and we'll revoke the old one and issue a new one.
    </p>

    <h2 id="stocks">GET /api/v1/stocks</h2>
    <p>
      Returns stocks ranked by analyst consensus upside. Unlike the free/Pro web tiers, an
      authenticated API request always gets the full dataset — the API subscription itself is
      the gate, so there's no separate top-10 limit to work around.
    </p>
    <table>
      <thead><tr><th>Param</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
      <tbody>
        <tr><td><code>limit</code></td><td>int</td><td>100</td><td>Max rows to return (max 500)</td></tr>
        <tr><td><code>offset</code></td><td>int</td><td>0</td><td>Pagination offset</td></tr>
        <tr><td><code>sector</code></td><td>string</td><td>—</td><td>Exact sector match, e.g. <code>Technology</code></td></tr>
        <tr><td><code>min_analysts</code></td><td>int</td><td>0</td><td>Minimum analyst coverage count</td></tr>
      </tbody>
    </table>
    <pre><code>curl -H "Authorization: Bearer su_live_YOUR_KEY" \\
  "https://stockupside.io/api/v1/stocks?sector=Technology&limit=20"</code></pre>
    <pre><code>{
  "stocks": [
    {
      "ticker": "EXAMPLE",
      "name": "Example Corp",
      "sector": "Technology",
      "current_price": 100.0,
      "target_price": 130.0,
      "upside_pct": 30.0,
      "analyst_count": 12,
      "consensus": "Buy",
      "market_cap_raw": 50000000000,
      "pe_ratio": 22.0,
      "peg_ratio": 1.2,
      ...
    }
  ],
  "count": 20,
  "total_matching": 142,
  "limit": 20,
  "offset": 0,
  "last_updated": "2026-06-18"
}</code></pre>

    <h2 id="detail">GET /api/v1/stocks/:ticker</h2>
    <p>
      Full data for a single ticker, including the same similar-stocks list shown on the
      public stock detail pages.
    </p>
    <pre><code>curl -H "Authorization: Bearer su_live_YOUR_KEY" \\
  https://stockupside.io/api/v1/stocks/DOCN</code></pre>
    <p>Returns <code>404</code> with a JSON error if the ticker isn't in the tracked universe.</p>

    <h2 id="usage">GET /api/v1/usage</h2>
    <p>Check how many requests you've made today against your daily limit, without digging through logs.</p>
    <pre><code>curl -H "Authorization: Bearer su_live_YOUR_KEY" \\
  https://stockupside.io/api/v1/usage</code></pre>
    <pre><code>{ "requests_today": 412, "daily_limit": 10000, "date": "2026-06-18" }</code></pre>

    <h2 id="limits">Rate Limits</h2>
    <p>
      Two limits apply, both keyed to your API key rather than your IP (so it's fine to call
      from a shared server or cloud function): a burst limit of <strong>120 requests/minute</strong>,
      and a fair-use cap of <strong>10,000 requests/day</strong>, resetting at midnight UTC.
      Exceeding either returns <code>429</code> with a JSON body explaining which limit was hit.
    </p>

    <h2 id="pricing">Pricing</h2>
    <div class="docs-cta">
      <div class="docs-cta-price">$99<span style="font-size:14px;color:var(--text2)">/mo</span></div>
      <div class="docs-cta-sub">Full dataset, no row limits, 10,000 requests/day</div>
      <input type="email" id="api-docs-email" placeholder="your@email.com" />
      <button id="api-docs-subscribe">Get API Access →</button>
    </div>
    <p>
      Built for quant hobbyists, small funds, and anyone building a tool on top of analyst
      consensus data who'd rather not scrape it by hand. Cancel anytime — your key stops working
      immediately on cancellation, no partial-month proration headaches on our end to worry about.
    </p>
  </div>
  <script>
    document.getElementById('api-docs-subscribe').onclick = async function() {
      var btn = this;
      var email = document.getElementById('api-docs-email').value.trim();
      if (!email || !email.includes('@')) { alert('Enter a valid email'); return; }
      btn.textContent = 'Redirecting…'; btn.disabled = true;
      try {
        var r = await fetch('/api/api-tier/subscribe', {
          method: 'POST', headers: {'Content-Type':'application/json'},
          body: JSON.stringify({email: email})
        });
        var d = await r.json();
        if (d.checkout_url) { window.location.href = d.checkout_url; }
        else { alert(d.error || 'Something went wrong'); btn.textContent = 'Get API Access →'; btn.disabled = false; }
      } catch (e) {
        alert('Could not connect'); btn.textContent = 'Get API Access →'; btn.disabled = false;
      }
    };
  </script>
</body>
</html>"""


@app.route("/success")
def success_page():
    session_id = request.args.get("session_id", "")
    if not session_id:
        return redirect("/")

    try:
        session    = stripe.checkout.Session.retrieve(session_id, expand=["line_items"])
        email      = session.customer_details.email.strip().lower()
        stripe_id  = session.customer

        con = get_db()
        con.execute("""
            INSERT OR REPLACE INTO subscribers (email, plan, stripe_id, created_at)
            VALUES (?, 'pro', ?, ?)
        """, (email, stripe_id, int(time.time())))
        con.commit()
        con.close()

        annual_price_id = os.environ.get("STRIPE_PRICE_ANNUAL", "")
        try:
            price_id_used = session.line_items.data[0].price.id
        except Exception:
            price_id_used = ""
        plan_label = "annual" if price_id_used == annual_price_id else "monthly"
        _track_server_event("Checkout Completed", {"plan": plan_label})

        # Issue a fresh random session token (replaces the old deterministic,
        # permanent, shareable token scheme).
        token = create_session(email)

        # Pass the token to the frontend via URL fragment so it lands in localStorage
        return redirect(f"/?pro_token={token}&welcome=1")

    except Exception as e:
        print(f"  ⚠  Success page error: {e}")
        return redirect("/")


# ── API tier ─────────────────────────────────────────────────────────────────
@app.route("/api/api-tier/subscribe", methods=["POST", "OPTIONS"])
@limiter.limit("10 per hour")
def api_tier_subscribe():
    """Start a Stripe Checkout session for the $99/mo API tier. Kept as a
    separate flow from /api/subscribe (the $29/$199 Pro web tier) since
    they're different products with different price IDs and different
    post-checkout behavior (issuing an API key vs. a web session token)."""
    if request.method == "OPTIONS":
        return Response(status=200)
    body  = request.get_json(force=True) or {}
    email = body.get("email", "").strip().lower()

    if not email or "@" not in email:
        return jsonify({"error": "Invalid email"}), 400

    price_id = os.environ.get("STRIPE_PRICE_API_TIER")
    if not price_id:
        return jsonify({"error": "API tier is not configured yet"}), 503

    try:
        session = stripe.checkout.Session.create(
            payment_method_types=["card"],
            line_items=[{"price": price_id, "quantity": 1}],
            mode="subscription",
            customer_email=email,
            success_url="https://stockupside.io/api-success?session_id={CHECKOUT_SESSION_ID}",
            cancel_url="https://stockupside.io/api/docs",
        )
        return jsonify({"checkout_url": session.url})
    except Exception as e:
        print(f"  ⚠  Stripe error (API tier): {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/api-success")
def api_tier_success_page():
    """Post-checkout landing for the API tier. Unlike /success (which
    issues a short-lived web session token), this issues a long-lived
    API key and shows it exactly once — Stripe/our DB never displays the
    full key again after this page, matching how most API products
    (Stripe itself included) handle key reveal."""
    session_id = request.args.get("session_id", "")
    if not session_id:
        return redirect("/api/docs")

    try:
        session   = stripe.checkout.Session.retrieve(session_id)
        email     = session.customer_details.email.strip().lower()
        stripe_id = session.customer

        api_key = create_api_key(email, stripe_id=stripe_id, label="default")
        _track_server_event("API Tier Checkout Completed")

        return Response(_render_api_key_reveal_page(api_key, email), mimetype="text/html")

    except Exception as e:
        print(f"  ⚠  API tier success page error: {e}")
        return redirect("/api/docs")

def _render_api_key_reveal_page(api_key: str, email: str) -> str:
    """One-time key reveal page. Plain server-rendered HTML (no main.js
    dependency) so it works even if something's wrong with the SPA bundle
    — this page only needs to ever do one thing correctly."""
    safe_email = escape(email)
    safe_key   = escape(api_key)
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<title>Your StockUpside API Key</title>
<meta name="robots" content="noindex">
<style>
  body {{ background:#0d1117; color:#c9d1d9; font-family:-apple-system,sans-serif;
         max-width:640px; margin:60px auto; padding:0 24px; line-height:1.6; }}
  .key-box {{ background:#161b22; border:1px solid #30363d; border-radius:8px;
              padding:20px; font-family:monospace; font-size:15px; word-break:break-all;
              margin:20px 0; user-select:all; }}
  .warn {{ color:#e3b341; font-size:14px; }}
  a {{ color:#58a6ff; }}
  code {{ background:#161b22; padding:2px 6px; border-radius:4px; }}
</style></head>
<body>
  <h1>▲ Your API Key</h1>
  <p>Subscribed as <strong>{safe_email}</strong>. Save this key now — for security, we only
  show it once and can't display it again later.</p>
  <div class="key-box">{safe_key}</div>
  <p class="warn">⚠ Copy this somewhere safe before leaving this page.</p>
  <h3>Quick start</h3>
  <pre><code>curl -H "Authorization: Bearer {safe_key}" \\
  https://stockupside.io/api/v1/stocks?limit=10</code></pre>
  <p>Full documentation: <a href="/api/docs">stockupside.io/api/docs</a></p>
  <p>Lost a key later? Email <a href="mailto:hello@stockupside.io">hello@stockupside.io</a> from
  the address you subscribed with and we'll revoke and reissue one.</p>
</body></html>"""

@app.route("/api/stripe-webhook", methods=["POST"])
@limiter.exempt
def stripe_webhook():
    """Handle Stripe webhook events — specifically subscription cancellations.

    When a subscriber cancels (or their subscription lapses due to failed
    payment, etc.), Stripe sends `customer.subscription.deleted`. Without
    this handler, a cancelled subscriber would stay `plan='pro'` in our DB
    indefinitely, and their session tokens would keep working for the full
    SESSION_TTL_DAYS — i.e. they'd retain Pro access after cancelling.

    On `customer.subscription.deleted`:
      - Downgrade the matching subscriber (by stripe_id / customer id) to 'free'.
      - Revoke all their active session tokens immediately.

    Setup: in the Stripe dashboard, add an endpoint pointing at
    https://yourdomain.com/api/stripe-webhook, subscribed to the
    `customer.subscription.deleted` event, and set STRIPE_WEBHOOK_SECRET
    to the signing secret Stripe gives you for that endpoint.
    """
    payload    = request.get_data()
    sig_header = request.headers.get("Stripe-Signature", "")
    webhook_secret = os.environ.get("STRIPE_WEBHOOK_SECRET", "")

    if not webhook_secret:
        print("  ⚠  STRIPE_WEBHOOK_SECRET not set — rejecting webhook (cannot verify signature).")
        return jsonify({"error": "Webhook not configured"}), 503

    try:
        event = stripe.Webhook.construct_event(payload, sig_header, webhook_secret)
    except ValueError:
        return jsonify({"error": "Invalid payload"}), 400
    except stripe.error.SignatureVerificationError:
        return jsonify({"error": "Invalid signature"}), 400

    event_type = event.get("type", "")

    if event_type == "customer.subscription.deleted":
        sub = event["data"]["object"]
        customer_id = sub.get("customer")
        if not customer_id:
            return jsonify({"success": True})

        con = get_db()
        row = con.execute(
            "SELECT email FROM subscribers WHERE stripe_id=?", (customer_id,)
        ).fetchone()
        con.close()

        if row:
            email_addr = row[0]
            downgrade_subscriber(email_addr)
            print(f"  ✓  Subscription cancelled for {email_addr} — "
                  f"downgraded to free, sessions and API keys revoked.")
        else:
            # Not a Pro web subscriber — check whether this customer_id
            # belongs to an API tier subscription instead. These are
            # billed as separate Stripe subscriptions/customers from the
            # Pro web tier, so they won't show up in `subscribers` at all.
            con = get_db()
            api_row = con.execute(
                "SELECT email FROM api_keys WHERE stripe_id=? AND plan='active'",
                (customer_id,),
            ).fetchone()
            con.close()
            if api_row:
                api_email = api_row[0]
                revoke_all_api_keys(api_email)
                print(f"  ✓  API tier subscription cancelled for {api_email} — "
                      f"API key(s) revoked.")
            else:
                print(f"  ⚠  Webhook: customer.subscription.deleted for unknown "
                      f"stripe_id={customer_id}")

    # Acknowledge all other event types without action (Stripe retries on
    # non-2xx, so we always return 200 for events we don't care about).
    return jsonify({"success": True})



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
    """Admin-only: manually trigger the weekly digest to all subscribers
    (free + pro, with pro subscribers getting their personalized picks).
    Primarily for testing — the normal cadence runs via weekly_digest()."""
    if not _admin_authorized():
        return jsonify({"error": "Unauthorized"}), 401

    stocks = get_stocks_cached()
    if not stocks:
        return jsonify({"error": "No stock data available yet"}), 503

    con  = get_db()
    subs = con.execute("SELECT email, plan FROM subscribers").fetchall()
    con.close()

    if not subs:
        return jsonify({"success": True, "sent": 0, "message": "No subscribers yet"})

    def _send_all():
        sent = 0
        for addr, plan in subs:
            prefs = get_email_prefs(addr) if plan == "pro" else None
            subj, html, text = digest_email_html(stocks, addr, prefs=prefs)
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
    # XSS fix: `addr` is attacker-controlled (a query param) and is
    # reflected into the page below. Even though a valid token is
    # required, an attacker can request a token for an email address
    # they control that *contains* HTML/JS, then send that crafted
    # unsubscribe link to a victim. Escape before interpolating.
    safe_addr = escape(addr)

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
    <strong style="color:var(--text)">{safe_addr}</strong> has been removed from
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

    email_addr = _resolve_token_email(token)
    if email_addr:
        return jsonify({"valid": True, "plan": "pro"})

    return jsonify({"valid": False, "plan": "free"})

@app.route("/api/get-token", methods=["POST", "OPTIONS"])
@limiter.limit("5 per hour")
def api_get_token():
    """Request a Pro login link by email ('forgot access' / cross-device login).

    SECURITY: This endpoint deliberately does NOT return a token directly.
    Email addresses are not secret — if we returned a fresh session token
    to anyone who typed in a known subscriber's email, that would be an
    account-takeover vector. Instead we email a one-time login link to the
    address on file. The response is identical whether or not the email
    is a Pro subscriber, so this endpoint can't be used to enumerate
    subscribers either.
    """
    if request.method == "OPTIONS":
        return Response(status=200)
    body  = request.get_json(force=True) or {}
    email = body.get("email", "").strip().lower()
    if not email or "@" not in email:
        return jsonify({"error": "Invalid email"}), 400

    con = get_db()
    row = con.execute(
        "SELECT email, plan FROM subscribers WHERE email=?", (email,)
    ).fetchone()
    con.close()

    if row and row[1] == "pro":
        sub_email = row[0]
        token   = create_session(sub_email)
        link    = f"{_SITE_URL}/login?token={token}"
        subject = "▲ Your StockUpside.io Pro login link"
        html = f"""<!DOCTYPE html>
<html><body style="font-family:monospace;background:#0d1117;color:#e6edf3;
                   padding:32px;max-width:480px;margin:0 auto">
  <div style="font-size:20px;font-weight:700;margin-bottom:20px">▲ StockUpside.io</div>
  <p style="color:#8b949e;font-size:13px;line-height:1.6">
    Click the link below to log in to your Pro account on this device.
    This link expires once used and works for {SESSION_TTL_DAYS} days.
  </p>
  <a href="{link}" style="display:inline-block;margin:16px 0;padding:10px 20px;
     background:#00e676;color:#000;font-weight:700;text-decoration:none;
     border-radius:4px;font-size:13px">Log In to StockUpside.io →</a>
  <p style="color:#8b949e;font-size:11px;line-height:1.6">
    If you didn't request this, you can safely ignore this email.
  </p>
</body></html>"""
        text = f"Log in to StockUpside.io Pro: {link}\n\nIf you didn't request this, ignore this email."
        send_email(sub_email, subject, html, text)

    # Same response regardless of whether the email matched — prevents
    # using this endpoint to check which emails are Pro subscribers.
    return jsonify({"success": True,
                    "message": "If that email has a Pro subscription, "
                               "we've sent a login link to it."})


@app.route("/login")
@limiter.limit("30 per hour")
def login_via_token():
    """Land here from the magic-link email. Validates the token (proving
    the click came from the inbox it was sent to) then hands it to the
    frontend the same way Stripe checkout does."""
    token = request.args.get("token", "")
    email_addr = _resolve_token_email(token)
    if not email_addr:
        return redirect("/?login_error=invalid_or_expired")
    return redirect(f"/?pro_token={token}&welcome=1")


@app.route("/api/logout", methods=["POST", "OPTIONS"])
@limiter.limit("30 per hour")
def api_logout():
    """Revoke the current session token (this device only)."""
    if request.method == "OPTIONS":
        return Response(status=200)
    body  = request.get_json(force=True) or {}
    token = body.get("token", "").strip()
    if token:
        revoke_session(token)
    return jsonify({"success": True})


@app.route("/api/logout-everywhere", methods=["POST", "OPTIONS"])
@limiter.limit("10 per hour")
def api_logout_everywhere():
    """Revoke ALL sessions for the subscriber tied to the given token —
    use if a token may have been shared/leaked."""
    if request.method == "OPTIONS":
        return Response(status=200)
    body  = request.get_json(force=True) or {}
    token = body.get("token", "").strip()
    email_addr = _resolve_token_email(token)
    if not email_addr:
        return jsonify({"error": "Invalid or expired session"}), 401
    n = revoke_all_sessions(email_addr)
    return jsonify({"success": True, "revoked": n})


@app.route("/api/email-prefs", methods=["GET", "POST", "OPTIONS"])
@limiter.limit("30 per hour")
def api_email_prefs():
    """Get or set a Pro subscriber's weekly digest filter preferences.

    Auth: pass the Pro access token as either the 'token' query/body param
    or an 'Authorization: Bearer <token>' header. Resolves to the
    subscriber's email server-side — the email itself is never trusted
    from the client for write operations.
    """
    if request.method == "OPTIONS":
        return Response(status=200)

    token = (
        request.args.get("token")
        or (request.get_json(silent=True) or {}).get("token")
        or request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
    )
    email_addr = _resolve_token_email(token)
    if not email_addr:
        return jsonify({"error": "Invalid or expired Pro token"}), 401

    if request.method == "GET":
        return jsonify({"prefs": get_email_prefs(email_addr)})

    # POST — save new preferences
    body  = request.get_json(force=True) or {}
    prefs = body.get("prefs", {})

    valid_consensus = {"All", "Strong Buy", "Buy", "Hold", "Underperform"}
    valid_momentum  = {"All", "up", "down", "neutral"}

    cleaned = {
        "sector":       str(prefs.get("sector", "All") or "All"),
        "consensus":    prefs.get("consensus", "All") if prefs.get("consensus") in valid_consensus else "All",
        "min_analysts": max(0, int(prefs.get("min_analysts", 0) or 0)),
        "max_pe":       max(0.0, float(prefs.get("max_pe", 0) or 0)),
        "max_peg":      max(0.0, float(prefs.get("max_peg", 0) or 0)),
        "momentum":     prefs.get("momentum", "All") if prefs.get("momentum") in valid_momentum else "All",
    }

    set_email_prefs(email_addr, cleaned)

    # Tell the caller how many stocks currently match, so the UI can warn
    # if the combination is too narrow (e.g. "only 3 stocks match").
    stocks  = get_stocks_cached()
    matches = len(apply_email_filters(stocks, cleaned, limit=10_000)) if stocks else 0

    return jsonify({"success": True, "prefs": cleaned, "matching_stocks": matches})


@app.route("/api/watchlist", methods=["GET", "POST", "DELETE", "OPTIONS"])
@limiter.limit("120 per hour")
def api_watchlist():
    """Pro-only watchlist of tickers.

    Auth: Pro access token via 'token' query/body param or
    'Authorization: Bearer <token>' header — same pattern as
    /api/email-prefs. Email is resolved server-side, never trusted
    from the client.

    GET    -> list watchlisted tickers joined with current stock data
    POST   {ticker} -> add a ticker
    DELETE {ticker} -> remove a ticker
    """
    if request.method == "OPTIONS":
        return Response(status=200)

    token = (
        request.args.get("token")
        or (request.get_json(silent=True) or {}).get("token")
        or request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
    )
    email_addr = _resolve_token_email(token)
    if not email_addr:
        return jsonify({"error": "Pro subscription required"}), 401

    con = get_db()

    if request.method == "GET":
        rows = con.execute(
            "SELECT ticker FROM watchlists WHERE email=? ORDER BY added_at DESC",
            (email_addr,)
        ).fetchall()
        con.close()
        tickers = {r[0] for r in rows}

        stocks = get_stocks_cached()
        by_ticker = {s["ticker"]: s for s in stocks}
        matched   = [by_ticker[t] for t in tickers if t in by_ticker]
        missing   = sorted(tickers - by_ticker.keys())

        return jsonify({"stocks": matched, "tickers": sorted(tickers),
                        "missing": missing, "total": len(tickers)})

    body   = request.get_json(force=True) or {}
    ticker = str(body.get("ticker", "")).strip().upper()
    if not ticker or not re.match(r"^[A-Z0-9.\-]{1,10}$", ticker):
        con.close()
        return jsonify({"error": "Invalid ticker"}), 400

    if request.method == "POST":
        con.execute(
            "INSERT OR IGNORE INTO watchlists (email, ticker, added_at) VALUES (?, ?, ?)",
            (email_addr, ticker, int(time.time()))
        )
        con.commit()
    else:  # DELETE
        con.execute(
            "DELETE FROM watchlists WHERE email=? AND ticker=?",
            (email_addr, ticker)
        )
        con.commit()

    count = con.execute(
        "SELECT COUNT(*) FROM watchlists WHERE email=?", (email_addr,)
    ).fetchone()[0]
    con.close()
    return jsonify({"success": True, "ticker": ticker, "total": count})


@app.route("/watchlist")
def watchlist_page():
    # Same pattern as /stocks — let the frontend (main.js) detect the path
    # and render the watchlist view client-side.
    return send_from_directory(PUBLIC_DIR, "index.html")


@app.route("/api/alerts", methods=["GET", "POST", "DELETE", "OPTIONS"])
@limiter.limit("60 per hour")
def api_alerts():
    if request.method == "OPTIONS":
        return jsonify({}), 200

    token      = request.headers.get("X-Token") or request.args.get("token", "")
    email_addr = _resolve_token_email(token)
    if not email_addr:
        return jsonify({"error": "Pro subscription required"}), 403

    con = get_db()

    if request.method == "GET":
        rows = con.execute(
            "SELECT id, ticker, alert_type, alert_value, alert_value_text, "
            "created_at, last_triggered "
            "FROM alerts WHERE email=? AND active=1 ORDER BY created_at DESC",
            (email_addr,)
        ).fetchall()
        con.close()
        stocks    = get_stocks_cached()
        stock_map = {s["ticker"]: s for s in stocks} if stocks else {}
        result = []
        for row in rows:
            aid, ticker, atype, aval, aval_text, created_at, last_triggered = row
            stock = stock_map.get(ticker, {})
            result.append({
                "id": aid, "ticker": ticker, "alert_type": atype,
                "alert_value": aval, "alert_value_text": aval_text,
                "created_at": created_at, "last_triggered": last_triggered,
                "current_upside":    stock.get("upside_pct"),
                "current_price":     stock.get("current_price"),
                "current_rank":      stock.get("rank"),
                "current_consensus": stock.get("consensus"),
                "stock_name":        stock.get("name"),
                "currently_true": bool(stock and _alert_condition_met(
                    {"alert_type": atype, "alert_value": aval,
                     "alert_value_text": aval_text}, stock
                )),
            })
        return jsonify(result)

    if request.method == "POST":
        body      = request.get_json(silent=True) or {}
        ticker    = str(body.get("ticker", "")).upper().strip()
        atype     = str(body.get("alert_type", "")).strip()
        aval      = body.get("alert_value")
        aval_text = str(body.get("alert_value_text", "") or "").strip()

        if not ticker or not atype:
            con.close()
            return jsonify({"error": "ticker and alert_type are required"}), 400
        if atype not in ALERT_TYPES:
            con.close()
            return jsonify({"error": f"Unknown alert_type. Valid: {list(ALERT_TYPES)}"}), 400

        if atype in ("upside_above", "upside_below", "rank_above", "rank_below"):
            if aval is None:
                con.close()
                return jsonify({"error": f"alert_value required for {atype}"}), 400
            try:
                aval = float(aval)
            except (TypeError, ValueError):
                con.close()
                return jsonify({"error": "alert_value must be a number"}), 400

        if atype == "consensus_change" and not aval_text:
            stocks    = get_stocks_cached()
            stock_map = {s["ticker"]: s for s in stocks} if stocks else {}
            aval_text = stock_map.get(ticker, {}).get("consensus", "")

        stocks  = get_stocks_cached()
        tickers = {s["ticker"] for s in stocks} if stocks else set()
        if ticker not in tickers:
            con.close()
            return jsonify({"error": f"{ticker} not found in current stock universe"}), 404

        count = con.execute(
            "SELECT COUNT(*) FROM alerts WHERE email=? AND active=1", (email_addr,)
        ).fetchone()[0]
        if count >= ALERT_MAX_PER_USER:
            con.close()
            return jsonify({"error": f"Alert limit reached ({ALERT_MAX_PER_USER} max). Delete one to add another."}), 429

        now = int(time.time())
        cur = con.execute(
            "INSERT INTO alerts (email, ticker, alert_type, alert_value, "
            "alert_value_text, created_at, active) VALUES (?,?,?,?,?,?,1)",
            (email_addr, ticker, atype, aval, aval_text or None, now)
        )
        con.commit()
        new_id = cur.lastrowid
        con.close()
        return jsonify({"success": True, "id": new_id}), 201

    if request.method == "DELETE":
        body     = request.get_json(silent=True) or {}
        alert_id = body.get("id")
        if not alert_id:
            con.close()
            return jsonify({"error": "id required"}), 400
        con.execute("UPDATE alerts SET active=0 WHERE id=? AND email=?",
                    (int(alert_id), email_addr))
        con.commit()
        con.close()
        return jsonify({"success": True})

    con.close()
    return jsonify({"error": "method not allowed"}), 405


@app.route("/api/alerts/test", methods=["POST"])
@limiter.limit("5 per hour")
def api_alerts_test():
    body  = request.get_json(silent=True) or {}
    token = body.get("token") or request.headers.get("X-Admin-Token", "")
    admin = os.environ.get("ADMIN_TOKEN", "")
    if not admin or not hmac.compare_digest(token, admin):
        return jsonify({"error": "forbidden"}), 403
    try:
        check_and_fire_alerts()
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/alerts")
def alerts_page():
    return send_from_directory(PUBLIC_DIR, "index.html")


@app.route("/api/refresh", methods=["POST"])
@limiter.limit("5 per hour")
def api_refresh():
    """Admin-only: kick off a data refresh without waiting for 01:00.
    Spawns generate.py in a background thread so the HTTP response is instant.
    Returns the last 10 lines of generate.log so you can see what the
    previous run did without having to SSH in."""
    import subprocess, sys
    if not _admin_authorized():
        return jsonify({"error": "Unauthorized"}), 401
    if is_generating():
        return jsonify({"error": "Refresh already in progress",
                        "tip": "Poll /api/refresh-status to watch progress"}), 429

    generate_script = os.path.join(BASE_DIR, "server", "generate.py")
    if not os.path.exists(generate_script):
        return jsonify({"error": f"generate.py not found at {generate_script}"}), 500

    def _run():
        set_generating(True)
        RUN_TIMEOUT  = 3 * 3600
        GRACE_PERIOD = 60
        try:
            with open(LOG_PATH, "a") as logfile:
                ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                logfile.write(f"[{ts}] Manual refresh triggered via /api/refresh\n")
                proc = subprocess.Popen(
                    [sys.executable, "-u", generate_script],
                    stdout=logfile, stderr=logfile,
                )
                try:
                    returncode = proc.wait(timeout=RUN_TIMEOUT)
                except subprocess.TimeoutExpired:
                    logfile.write(f"[{ts}] Manual refresh exceeded "
                                   f"{RUN_TIMEOUT/3600:.1f}h — sending SIGTERM.\n")
                    proc.terminate()
                    try:
                        returncode = proc.wait(timeout=GRACE_PERIOD)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        returncode = proc.wait()
            if returncode == 0:
                invalidate_memory_cache()
                print("  ✓  Manual refresh complete — cache invalidated.")
            else:
                invalidate_memory_cache()
                print(f"  ⚠  generate.py exited with code {returncode} — check {LOG_PATH}.")
        except Exception as e:
            print(f"  ⚠  Manual refresh failed: {e}")
        finally:
            set_generating(False)

    threading.Thread(target=_run, daemon=True).start()

    # Return the last 20 lines of the log so the caller can see what the
    # *previous* run did — useful for diagnosing why the last refresh failed
    # without needing SSH access.
    log_tail = []
    try:
        with open(LOG_PATH, "r") as f:
            log_tail = f.readlines()[-20:]
    except FileNotFoundError:
        log_tail = ["(no log file yet — this is the first refresh)"]

    return jsonify({
        "success": True,
        "message": "Refresh started in background. Poll /api/refresh-status to watch.",
        "generate_script": generate_script,
        "log_path": LOG_PATH,
        "previous_log_tail": [l.rstrip() for l in log_tail],
    })


@app.route("/api/refresh-status", methods=["GET"])
@limiter.limit("60 per minute")
def api_refresh_status():
    """Admin-only: check whether a refresh is currently running and
    tail the log. Safe to poll every few seconds."""
    if not _admin_authorized():
        return jsonify({"error": "Unauthorized"}), 401

    log_tail = []
    try:
        with open(LOG_PATH, "r") as f:
            log_tail = f.readlines()[-30:]
    except FileNotFoundError:
        log_tail = ["(no log file yet)"]

    stocks = get_stocks_cached()
    return jsonify({
        "generating": is_generating(),
        "stocks_in_cache": len(stocks) if stocks else 0,
        "last_updated": stocks[0].get("last_updated") if stocks else None,
        "log_tail": [l.rstrip() for l in log_tail],
    })

def similar_stocks(target: dict, stocks: list, n: int = 5) -> list:
    """Find stocks with similar fundamentals to `target`.

    Approach: restrict to the same sector (cross-sector valuation
    comparisons are rarely meaningful — a PEG of 1.5 means something very
    different in Utilities vs. Technology), then rank by a simple weighted
    distance over a few normalized fundamental features. This is a
    deterministic similarity score, not a trained model — no training data,
    storage, or pipeline required, and it's explainable to users ("similar
    sector, size, and valuation").

    Features compared (each z-scored within the same-sector candidate pool):
      - market cap (log scale, since caps span orders of magnitude)
      - trailing P/E
      - PEG ratio (only if both stocks have a valid PEG)
      - upside %

    Returns up to `n` stocks, closest first. Returns fewer (or none) if
    there isn't enough same-sector data with valid fundamentals — this is
    expected for thinly-covered sectors and is not an error.
    """
    sector = target.get("sector")
    if not sector or sector == "Unknown":
        return []

    candidates = [
        s for s in stocks
        if s["ticker"] != target["ticker"]
        and s.get("sector") == sector
        and (s.get("market_cap_raw") or 0) > 0
        and (s.get("pe_ratio") or 0) > 0
    ]
    if len(candidates) < 2:
        return []

    import math

    def _log_cap(s):
        return math.log10(s["market_cap_raw"])

    # Build feature vectors. PEG is included only when BOTH the target and
    # a candidate have a valid (>0) PEG, since most stocks have peg_ratio=0
    # (missing data) and including zeros would distort the distance.
    use_peg = (target.get("peg_ratio") or 0) > 0

    features = ["_log_cap", "pe_ratio", "upside_pct"] + (["peg_ratio"] if use_peg else [])
    pool = candidates + [target]

    # z-score each feature across the candidate pool (+ target) within this sector
    stats = {}
    for f in features:
        vals = [_log_cap(s) if f == "_log_cap" else (s.get(f) or 0) for s in pool]
        if use_peg and f == "peg_ratio":
            vals = [v for s, v in zip(pool, vals) if (s.get("peg_ratio") or 0) > 0]
        mean = sum(vals) / len(vals) if vals else 0
        std  = (sum((v - mean) ** 2 for v in vals) / len(vals)) ** 0.5 if vals else 0
        stats[f] = (mean, std or 1)  # avoid div-by-zero for constant features

    def _z(s, f):
        val = _log_cap(s) if f == "_log_cap" else (s.get(f) or 0)
        mean, std = stats[f]
        return (val - mean) / std

    target_vec = {f: _z(target, f) for f in features}

    scored = []
    for c in candidates:
        if use_peg and (c.get("peg_ratio") or 0) <= 0:
            continue  # skip candidates missing PEG when target has one
        dist = sum((target_vec[f] - _z(c, f)) ** 2 for f in features) ** 0.5
        scored.append((dist, c))

    scored.sort(key=lambda x: x[0])
    return [c for _, c in scored[:n]]


# ── Sector landing pages ─────────────────────────────────────────────────────
# yfinance's `sector` field uses these canonical GICS-derived names.
# Map each to a URL-friendly slug for /sectors/<slug> SEO pages.
SECTOR_SLUGS = {
    "Technology":            "technology",
    "Healthcare":            "healthcare",
    "Financial Services":    "financial-services",
    "Consumer Cyclical":     "consumer-cyclical",
    "Consumer Defensive":    "consumer-defensive",
    "Communication Services":"communication-services",
    "Industrials":           "industrials",
    "Energy":                "energy",
    "Basic Materials":       "basic-materials",
    "Real Estate":           "real-estate",
    "Utilities":             "utilities",
}
SECTOR_SLUG_TO_NAME = {v: k for k, v in SECTOR_SLUGS.items()}


# ── Blog ─────────────────────────────────────────────────────────────────────
# Posts live as individual files in server/blog_posts/, each with a simple
# frontmatter header (key: value lines between --- markers) followed by the
# post body as raw HTML. This means writing a new post is just creating a
# new file and restarting (or, since we reload on every blog request below,
# not even that) — no code changes, no redeploy of app.py needed.
#
# Example file: server/blog_posts/my-new-post.html
# ---
# title: My Post Title
# date: 2026-06-20
# slug: my-new-post
# excerpt: One or two sentences shown on the blog index and in meta tags.
# ---
# <p>Your HTML content here. Use the same tags as other posts (h2, p,
# strong, a) — they already match the site's styling.</p>

BLOG_POSTS_DIR = os.path.join(BASE_DIR, "server", "blog_posts")


def _parse_post_file(path: str) -> dict | None:
    """Parse a single post file: '---\\nkey: value\\n...\\n---\\n<html body>'.
    Returns None (and prints a warning) if the file is malformed, rather
    than crashing the whole blog on one bad post."""
    try:
        raw = open(path, "r", encoding="utf-8").read()
    except OSError as e:
        print(f"  ⚠  Blog: couldn't read {path}: {e}")
        return None

    parts = raw.split("---", 2)
    if len(parts) < 3:
        print(f"  ⚠  Blog: {path} is missing '---' frontmatter delimiters — skipped")
        return None

    _, frontmatter, body = parts
    meta: dict = {}
    for line in frontmatter.strip().splitlines():
        line = line.strip()
        if not line or ":" not in line:
            continue
        key, _, val = line.partition(":")
        meta[key.strip()] = val.strip()

    required = ("title", "date", "slug", "excerpt")
    missing  = [k for k in required if not meta.get(k)]
    if missing:
        print(f"  ⚠  Blog: {path} is missing required field(s) {missing} — skipped")
        return None

    meta["content_html"] = body.strip()
    return meta


def load_blog_posts() -> list:
    """Load and parse every .html post file in BLOG_POSTS_DIR. Re-reads
    from disk on every call (cheap — a handful of small files) so new
    posts appear immediately without restarting the server."""
    if not os.path.isdir(BLOG_POSTS_DIR):
        return []
    posts = []
    for fname in sorted(os.listdir(BLOG_POSTS_DIR)):
        if not fname.endswith(".html"):
            continue
        post = _parse_post_file(os.path.join(BLOG_POSTS_DIR, fname))
        if post:
            posts.append(post)
    return posts


@app.route("/blog")
def blog_index():
    return Response(render_blog_index(), mimetype="text/html")


@app.route("/blog/<slug>")
def blog_post(slug):
    posts = load_blog_posts()
    post  = next((p for p in posts if p["slug"] == slug), None)
    if not post:
        return Response(render_404_page(f"/blog/{slug}"), mimetype="text/html"), 404
    return Response(render_blog_post(post), mimetype="text/html")


@app.route("/sectors")
def sectors_index():
    stocks = get_stocks_cached()
    return Response(render_sectors_index(stocks), mimetype="text/html")


@app.route("/sectors/<slug>")
def sector_page(slug):
    sector_name = SECTOR_SLUG_TO_NAME.get(slug.lower())
    if not sector_name:
        return Response(render_404_page(f"/sectors/{slug}"), mimetype="text/html"), 404

    stocks = get_stocks_cached()
    sector_stocks = [s for s in stocks if s.get("sector") == sector_name]
    if not sector_stocks:
        return Response(render_404_page(f"/sectors/{slug}"), mimetype="text/html"), 404

    # sector_stocks inherits the global upside-descending sort from the cache
    return Response(render_sector_page(sector_name, slug, sector_stocks), mimetype="text/html")


@app.route("/stocks/<ticker>")
@limiter.limit("300 per hour")
def stock_page(ticker):
    ticker = ticker.upper()
    stocks = get_stocks_cached()
    stock  = next((s for s in stocks if s["ticker"] == ticker), None)

    if not stock:
        return Response(render_404_page(f"/stocks/{ticker}"), mimetype="text/html"), 404

    similar = similar_stocks(stock, stocks)

    # Build fully server-rendered HTML for SEO
    html = render_stock_page(stock, similar)
    return Response(html, mimetype="text/html")

@app.route("/stocks")
def stocks_index():
    # Just return the main index.html — let the frontend handle tier/filtering
    return send_from_directory(PUBLIC_DIR, "index.html")

@app.route("/accuracy")
def accuracy_page():
    return Response(render_accuracy_page(), mimetype="text/html")

@app.route("/sitemap.xml")
def sitemap_xml():
    stocks = get_stocks_cached()
    base = "https://stockupside.io"
    today = datetime.date.today().isoformat()

    urls = [
        (f"{base}/",          "daily", "1.0"),
        (f"{base}/stocks",    "daily", "0.9"),
        (f"{base}/sectors",   "daily", "0.8"),
        (f"{base}/blog",      "weekly", "0.7"),
        (f"{base}/changes",   "daily", "0.7"),
        (f"{base}/accuracy",  "weekly", "0.6"),
        (f"{base}/analyst-track-record", "weekly", "0.6"),
        (f"{base}/watchlist", "monthly", "0.3"),
        (f"{base}/terms",     "monthly", "0.2"),
        (f"{base}/privacy",   "monthly", "0.2"),
        (f"{base}/disclaimer","monthly", "0.2"),
    ]
    for slug in SECTOR_SLUGS.values():
        urls.append((f"{base}/sectors/{slug}", "daily", "0.7"))
    for p in load_blog_posts():
        urls.append((f"{base}/blog/{p['slug']}", "monthly", "0.6"))
    for s in stocks:
        urls.append((f"{base}/stocks/{s['ticker']}", "daily", "0.6"))

    body = "".join(
        f"  <url><loc>{loc}</loc><lastmod>{today}</lastmod>"
        f"<changefreq>{freq}</changefreq><priority>{pri}</priority></url>\n"
        for loc, freq, pri in urls
    )
    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        f"{body}"
        "</urlset>"
    )
    return Response(xml, mimetype="application/xml")


@app.route("/robots.txt")
def robots_txt():
    body = (
        "User-agent: *\n"
        "Allow: /\n"
        "Disallow: /api/\n"
        "\n"
        "Sitemap: https://stockupside.io/sitemap.xml\n"
    )
    return Response(body, mimetype="text/plain")


@app.route("/terms")
def terms_page():
    return Response(render_terms_page(), mimetype="text/html")

@app.route("/privacy")
def privacy_page():
    return Response(render_privacy_page(), mimetype="text/html")

@app.route("/disclaimer")
def disclaimer_page():
    return Response(render_disclaimer_page(), mimetype="text/html")

def render_blog_index() -> str:
    yr = datetime.date.today().year
    posts = sorted(load_blog_posts(), key=lambda p: p["date"], reverse=True)

    cards = ""
    for p in posts:
        cards += f"""
        <a href="/blog/{p['slug']}" class="blog-card">
          <div class="blog-card-date">{p['date']}</div>
          <div class="blog-card-title">{p['title']}</div>
          <div class="blog-card-excerpt">{p['excerpt']}</div>
        </a>"""

    if not cards:
        cards = '<p style="color:var(--text3);font-size:13px">No posts yet — check back soon.</p>'

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Blog | StockUpside.io</title>
  <meta name="description" content="Insights on analyst price targets, sector trends, and how we build StockUpside.io's stock rankings."/>
  <meta property="og:type"        content="website"/>
  <meta property="og:title"       content="Blog | StockUpside.io"/>
  <meta property="og:description" content="Insights on analyst price targets, sector trends, and how we build StockUpside.io's stock rankings."/>
  <meta property="og:url"         content="https://stockupside.io/blog"/>
  <meta property="og:image"       content="https://stockupside.io/og-image.png"/>
  <meta name="twitter:card"       content="summary_large_image"/>
  <meta name="robots" content="index, follow"/>
  <link rel="canonical" href="https://stockupside.io/blog"/>
  <link rel="stylesheet" href="/style.css"/>
  <style>
    .blog-wrap {{ max-width:760px;margin:0 auto;padding:32px 20px 64px; }}
    .blog-wrap h1 {{ font-family:var(--font-mono);font-size:22px;margin-bottom:6px; }}
    .blog-sub {{ color:var(--text2);font-size:13px;margin-bottom:32px; }}
    .blog-list {{ display:flex;flex-direction:column;gap:14px; }}
    .blog-card {{ display:block;background:var(--bg2);border:1px solid var(--border);
                  border-radius:8px;padding:20px;text-decoration:none;transition:border-color .15s; }}
    .blog-card:hover {{ border-color:var(--accent); }}
    .blog-card-date {{ font-family:var(--font-mono);font-size:10px;color:var(--text3);
                       letter-spacing:.08em;margin-bottom:8px; }}
    .blog-card-title {{ font-size:16px;font-weight:600;color:var(--text);margin-bottom:6px; }}
    .blog-card-excerpt {{ font-size:13px;color:var(--text2);line-height:1.6; }}
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
    <a href="/" style="font-family:var(--font-mono);font-size:11px;color:var(--text2)">← Dashboard</a>
  </div>
</header>

<div class="blog-wrap">
  <h1>Blog</h1>
  <p class="blog-sub">Notes on analyst data, methodology, and what we're building.</p>
  <div class="blog-list">{cards}
  </div>
</div>

<footer class="ftr">
  <div>© {yr} StockUpside.io · Not financial advice</div>
  <div class="ftr-r"><a href="/">Home</a> · <a href="/stocks">All Stocks</a> · <a href="/sectors">Sectors</a></div>
</footer>
</body>
</html>"""


def render_blog_post(post: dict) -> str:
    yr = datetime.date.today().year
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>{post['title']} | StockUpside.io Blog</title>
  <meta name="description" content="{post['excerpt']}"/>
  <meta property="og:type"        content="article"/>
  <meta property="og:title"       content="{post['title']}"/>
  <meta property="og:description" content="{post['excerpt']}"/>
  <meta property="og:url"         content="https://stockupside.io/blog/{post['slug']}"/>
  <meta property="og:image"       content="https://stockupside.io/og-image.png"/>
  <meta property="article:published_time" content="{post['date']}"/>
  <meta name="twitter:card"       content="summary_large_image"/>
  <meta name="robots" content="index, follow"/>
  <link rel="canonical" href="https://stockupside.io/blog/{post['slug']}"/>
  <link rel="stylesheet" href="/style.css"/>
  <script type="application/ld+json">
  {{
    "@context": "https://schema.org",
    "@type": "BlogPosting",
    "headline": "{post['title']}",
    "datePublished": "{post['date']}",
    "dateModified": "{post['date']}",
    "author": {{"@type": "Organization", "name": "StockUpside.io"}},
    "publisher": {{"@type": "Organization", "name": "StockUpside.io"}},
    "mainEntityOfPage": "https://stockupside.io/blog/{post['slug']}"
  }}
  </script>
  <style>
    .post-wrap {{ max-width:720px;margin:0 auto;padding:32px 20px 64px; }}
    .post-date {{ font-family:var(--font-mono);font-size:10px;color:var(--text3);
                  letter-spacing:.08em;margin-bottom:10px; }}
    .post-wrap h1 {{ font-size:26px;font-weight:700;color:var(--text);margin-bottom:24px;
                     line-height:1.3; }}
    .post-body {{ font-size:14px;color:var(--text2);line-height:1.85; }}
    .post-body h2 {{ font-family:var(--font-mono);font-size:13px;letter-spacing:.08em;
                     color:var(--accent);text-transform:uppercase;margin:32px 0 12px; }}
    .post-body p {{ margin-bottom:16px; }}
    .post-body strong {{ color:var(--text); }}
    .post-body a {{ color:var(--accent); }}
    .post-back {{ display:inline-block;margin-top:32px;font-family:var(--font-mono);
                  font-size:12px;color:var(--text2);text-decoration:none; }}
    .post-back:hover {{ color:var(--text); }}
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
    <a href="/blog" style="font-family:var(--font-mono);font-size:11px;
       color:var(--text2);margin-right:16px">Blog</a>
    <a href="/" style="font-family:var(--font-mono);font-size:11px;color:var(--text2)">← Dashboard</a>
  </div>
</header>

<div class="post-wrap">
  <div class="post-date">{post['date']}</div>
  <h1>{post['title']}</h1>
  <div class="post-body">{post['content_html']}</div>
  <a href="/blog" class="post-back">← Back to all posts</a>
</div>

<footer class="ftr">
  <div>© {yr} StockUpside.io · Not financial advice</div>
  <div class="ftr-r"><a href="/">Home</a> · <a href="/stocks">All Stocks</a> · <a href="/sectors">Sectors</a></div>
</footer>
</body>
</html>"""


def render_sectors_index(stocks: list) -> str:
    yr = datetime.date.today().year

    # Aggregate per-sector stats from the live cache
    agg: dict = {}
    for s in stocks:
        sec = s.get("sector")
        if sec not in SECTOR_SLUGS:
            continue
        if sec not in agg:
            agg[sec] = {"count": 0, "upside_sum": 0.0, "top": None}
        agg[sec]["count"] += 1
        agg[sec]["upside_sum"] += s["upside_pct"]
        if agg[sec]["top"] is None or s["upside_pct"] > agg[sec]["top"]["upside_pct"]:
            agg[sec]["top"] = s

    cards = ""
    # Stable order: by stock count, descending, so larger sectors lead
    for sec, slug in sorted(SECTOR_SLUGS.items(), key=lambda kv: -agg.get(kv[0], {"count": 0})["count"]):
        data = agg.get(sec)
        if not data or data["count"] == 0:
            continue
        avg_upside = round(data["upside_sum"] / data["count"], 1)
        top = data["top"]
        sign = "+" if avg_upside >= 0 else ""
        color = "var(--green)" if avg_upside >= 0 else "var(--red)"
        cards += f"""
        <a href="/sectors/{slug}" class="sec-card">
          <div class="sec-card-top">
            <span class="sec-card-name">{sec}</span>
            <span class="sec-card-avg" style="color:{color}">{sign}{avg_upside}%</span>
          </div>
          <div class="sec-card-meta">{data["count"]} stocks tracked · avg analyst upside</div>
          <div class="sec-card-top-pick">Top pick: <strong>{top["ticker"]}</strong> ({"+" if top["upside_pct"]>=0 else ""}{top["upside_pct"]}%)</div>
        </a>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Stocks by Sector — Analyst Upside Rankings | StockUpside.io</title>
  <meta name="description" content="Browse the top stocks by analyst price target upside in each sector — Technology, Healthcare, Energy, and more. Updated daily."/>
  <meta property="og:type"        content="website"/>
  <meta property="og:title"       content="Stocks by Sector — Analyst Upside Rankings | StockUpside.io"/>
  <meta property="og:description" content="See which sectors Wall Street analysts are most bullish on, and the top-ranked stock in each."/>
  <meta property="og:url"         content="https://stockupside.io/sectors"/>
  <meta property="og:image"       content="https://stockupside.io/og-image.png"/>
  <meta name="twitter:card"       content="summary_large_image"/>
  <link rel="stylesheet" href="/style.css"/>
  <style>
    .secs-wrap {{ max-width:1000px;margin:0 auto;padding:32px 20px 64px; }}
    .secs-wrap h1 {{ font-family:var(--font-mono);font-size:22px;margin-bottom:6px; }}
    .secs-sub {{ color:var(--text2);font-size:13px;margin-bottom:32px; }}
    .secs-grid {{ display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));gap:14px; }}
    .sec-card {{ display:block;background:var(--bg2);border:1px solid var(--border);
                 border-radius:8px;padding:18px;text-decoration:none;transition:border-color .15s; }}
    .sec-card:hover {{ border-color:var(--accent); }}
    .sec-card-top {{ display:flex;justify-content:space-between;align-items:baseline;margin-bottom:8px; }}
    .sec-card-name {{ font-family:var(--font-mono);font-weight:700;font-size:14px;color:var(--text); }}
    .sec-card-avg {{ font-family:var(--font-mono);font-weight:700;font-size:14px; }}
    .sec-card-meta {{ font-size:11px;color:var(--text3);margin-bottom:10px; }}
    .sec-card-top-pick {{ font-size:12px;color:var(--text2);font-family:var(--font-mono); }}
    .sec-card-top-pick strong {{ color:var(--accent); }}
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

<div class="secs-wrap">
  <h1>Stocks by Sector</h1>
  <p class="secs-sub">
    Average analyst price target upside by sector, updated daily. Click a sector to see
    its full ranked list.
  </p>
  <div class="secs-grid">{cards}
  </div>
</div>

<footer class="ftr">
  <div>© {yr} StockUpside.io · Updated daily · Not financial advice</div>
  <div class="ftr-r"><a href="/">Home</a> · <a href="/stocks">All Stocks</a></div>
</footer>
</body>
</html>"""


def render_sector_page(sector_name: str, slug: str, sector_stocks: list) -> str:
    yr = datetime.date.today().year
    n  = len(sector_stocks)
    avg_upside = round(sum(s["upside_pct"] for s in sector_stocks) / n, 1)
    strong_buy_pct = round(
        100 * sum(1 for s in sector_stocks if s["consensus"] in ("Strong Buy", "Buy")) / n, 0
    )

    # sector_stocks already inherits the global upside-descending sort
    top_n = sector_stocks[:25]

    rows = ""
    for i, s in enumerate(top_n):
        sign  = "+" if s["upside_pct"] >= 0 else ""
        color = "var(--green)" if s["upside_pct"] >= 0 else "var(--red)"
        rows += f"""
        <tr>
          <td style="padding:10px 12px;color:var(--text3);font-family:var(--font-mono);font-size:11px">{i+1}</td>
          <td style="padding:10px 12px"><a href="/stocks/{s['ticker']}" style="font-family:var(--font-mono);
              font-weight:700;color:var(--accent);text-decoration:none">{s['ticker']}</a></td>
          <td style="padding:10px 12px;color:var(--text2);font-size:12px;max-width:220px;
              overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{s['name']}</td>
          <td style="padding:10px 12px;font-family:var(--font-mono);text-align:right">${s['current_price']}</td>
          <td style="padding:10px 12px;font-family:var(--font-mono);text-align:right">${s['target_price']}</td>
          <td style="padding:10px 12px;font-family:var(--font-mono);font-weight:700;
              text-align:right;color:{color}">{sign}{s['upside_pct']}%</td>
          <td style="padding:10px 12px;text-align:right"><span style="color:{_consensus_color(s['consensus'])};
              font-family:var(--font-mono);font-size:11px;font-weight:700">{s['consensus']}</span></td>
        </tr>"""

    sign_avg = "+" if avg_upside >= 0 else ""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Best {sector_name} Stocks by Analyst Upside | StockUpside.io</title>
  <meta name="description" content="Top {sector_name} stocks ranked by Wall Street analyst consensus price target upside. {n} stocks tracked, average upside {sign_avg}{avg_upside}%. Updated daily."/>
  <meta property="og:type"        content="website"/>
  <meta property="og:title"       content="Best {sector_name} Stocks by Analyst Upside | StockUpside.io"/>
  <meta property="og:description" content="{n} {sector_name} stocks ranked by analyst price target upside. Average upside {sign_avg}{avg_upside}%. Updated daily."/>
  <meta property="og:url"         content="https://stockupside.io/sectors/{slug}"/>
  <meta property="og:image"       content="https://stockupside.io/og-image.png"/>
  <meta name="twitter:card"       content="summary_large_image"/>
  <link rel="canonical" href="https://stockupside.io/sectors/{slug}"/>
  <link rel="stylesheet" href="/style.css"/>
  <style>
    .secp-wrap {{ max-width:1100px;margin:0 auto;padding:32px 20px 64px; }}
    .secp-wrap h1 {{ font-family:var(--font-mono);font-size:22px;margin-bottom:6px; }}
    .secp-sub {{ color:var(--text2);font-size:13px;margin-bottom:24px; }}
    .secp-stats {{ display:grid;grid-template-columns:repeat(3,1fr);gap:14px;margin-bottom:28px; }}
    @media(max-width:600px){{ .secp-stats{{grid-template-columns:1fr;}} }}
    .secp-stat {{ background:var(--bg2);border:1px solid var(--border);border-radius:8px;padding:16px; }}
    .secp-stat-l {{ font-family:var(--font-mono);font-size:9px;color:var(--text3);
                    letter-spacing:.1em;margin-bottom:8px; }}
    .secp-stat-v {{ font-family:var(--font-mono);font-size:24px;font-weight:700; }}
    .secp-tbl-wrap {{ background:var(--bg2);border:1px solid var(--border);border-radius:8px;
                      overflow-x:auto;margin-bottom:28px; }}
    .secp-tbl {{ width:100%;border-collapse:collapse;font-size:13px; }}
    .secp-tbl th {{ padding:10px 12px;text-align:left;font-family:var(--font-mono);font-size:9px;
                    color:var(--text3);letter-spacing:.1em;border-bottom:1px solid var(--border);
                    white-space:nowrap; }}
    .secp-tbl th:nth-child(n+4) {{ text-align:right; }}
    .secp-tbl tr:not(:last-child) td {{ border-bottom:1px solid var(--border); }}
    .secp-prose {{ background:var(--bg2);border:1px solid var(--border);border-radius:8px;
                   padding:24px;line-height:1.8;color:var(--text2);font-size:14px; }}
    .secp-prose strong {{ color:var(--text); }}
    .secp-other {{ margin-top:28px;font-size:12px;font-family:var(--font-mono); }}
    .secp-other a {{ color:var(--text2);margin-right:14px; }}
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
    <a href="/sectors" style="font-family:var(--font-mono);font-size:11px;
       color:var(--text2);margin-right:16px">All Sectors</a>
    <a href="/" style="font-family:var(--font-mono);font-size:11px;color:var(--text2)">← Dashboard</a>
  </div>
</header>

<div class="secp-wrap">
  <h1>Best {sector_name} Stocks by Analyst Upside</h1>
  <p class="secp-sub">
    Ranked by Wall Street analyst consensus price target upside. Updated daily from
    {n} {sector_name} stocks we track.
  </p>

  <div class="secp-stats">
    <div class="secp-stat">
      <div class="secp-stat-l">STOCKS TRACKED</div>
      <div class="secp-stat-v">{n}</div>
    </div>
    <div class="secp-stat">
      <div class="secp-stat-l">AVG ANALYST UPSIDE</div>
      <div class="secp-stat-v" style="color:{'var(--green)' if avg_upside>=0 else 'var(--red)'}">{sign_avg}{avg_upside}%</div>
    </div>
    <div class="secp-stat">
      <div class="secp-stat-l">BUY / STRONG BUY</div>
      <div class="secp-stat-v">{strong_buy_pct:.0f}%</div>
    </div>
  </div>

  <div class="secp-tbl-wrap">
    <table class="secp-tbl">
      <thead><tr>
        <th>#</th><th>TICKER</th><th>COMPANY</th><th>PRICE</th><th>TARGET</th><th>UPSIDE</th><th>CONSENSUS</th>
      </tr></thead>
      <tbody>{rows}
      </tbody>
    </table>
  </div>

  <div class="secp-prose">
    <p>
      We track <strong>{n} {sector_name} stocks</strong> with active Wall Street analyst
      coverage. On average, analysts see <strong>{sign_avg}{avg_upside}% upside</strong> to
      current consensus price targets across the sector, with
      <strong>{strong_buy_pct:.0f}%</strong> of stocks rated Buy or Strong Buy.
      The table above shows the top {len(top_n)} {sector_name} stocks ranked by upside —
      click any ticker for full analyst breakdowns, price target ranges, and accuracy history.
    </p>
  </div>

  <div class="secp-other">
    Other sectors:
    {" ".join(f'<a href="/sectors/{s}">{n2}</a>' for n2, s in SECTOR_SLUGS.items() if s != slug)}
  </div>
</div>

<footer class="ftr">
  <div>© {yr} StockUpside.io · Updated daily · Not financial advice</div>
  <div class="ftr-r"><a href="/">Home</a> · <a href="/stocks">All Stocks</a> · <a href="/sectors">All Sectors</a></div>
</footer>
</body>
</html>"""


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
    # XSS fix: `path` is attacker-controlled (the requested URL). Escape it
    # before interpolating into HTML — previously this allowed reflected XSS
    # via e.g. /stocks/<script>alert(1)</script>.
    safe_path = escape(path)
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
  {f'<div class="e404-path">{safe_path}</div>' if path else ""}
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


def render_terms_page() -> str:
    yr = datetime.date.today().year
    updated = "2026-06-14"
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Terms of Service | StockUpside.io</title>
  <meta name="description" content="Terms of Service for StockUpside.io."/>
  <meta name="robots" content="index, follow"/>
  <link rel="canonical" href="https://stockupside.io/terms"/>
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
  <h1>Terms of Service</h1>
  <p class="legal-meta">Last updated: {updated} &nbsp;·&nbsp; Effective immediately</p>

  <p>These Terms of Service ("Terms") govern your use of StockUpside.io (the "Service"),
  operated by StockUpside.io ("we", "us", or "our"). By accessing or using the Service,
  you agree to be bound by these Terms. If you do not agree, please do not use the
  Service.</p>

  <h2>1. Description of Service</h2>
  <p>StockUpside.io provides aggregated, publicly available analyst price target and
  consensus data for publicly traded stocks, ranked by potential upside. A free tier
  provides limited access; a paid "Pro" subscription provides expanded access and
  additional features.</p>

  <h2>2. Not Financial Advice</h2>
  <p>The Service is provided for informational and educational purposes only. Nothing
  on StockUpside.io constitutes financial, investment, legal, or tax advice, or a
  recommendation to buy, sell, or hold any security. Analyst price targets and
  consensus ratings reflect third-party opinions and are not guarantees of future
  performance. See our <a href="/disclaimer">Financial Disclaimer</a> for more detail.
  You are solely responsible for your own investment decisions and should consult a
  licensed financial professional before making any investment.</p>

  <h2>3. Accounts and Access</h2>
  <p>Pro access is granted via a secure login link sent to the email address used at
  signup. You are responsible for keeping access to that email account secure. We
  are not liable for unauthorized access resulting from a compromised email account.</p>

  <h2>4. Subscriptions, Billing, and Cancellation</h2>
  <ul>
    <li>Pro subscriptions are billed on a recurring basis (monthly or annual, as
    selected at signup) via Stripe, our third-party payment processor.</li>
    <li>You may cancel your subscription at any time. Cancellation takes effect at
    the end of the current billing period; you will retain Pro access until then,
    and will not be charged again afterward.</li>
    <li>Fees are non-refundable except where required by law. If you believe you were
    charged in error, contact <a href="mailto:hello@stockupside.io">hello@stockupside.io</a>
    and we will review the request.</li>
    <li>We reserve the right to change subscription pricing with reasonable advance
    notice. Price changes will not apply to a billing period that has already been
    paid for.</li>
  </ul>

  <h2>5. Acceptable Use</h2>
  <p>You agree not to: (a) attempt to gain unauthorized access to the Service or its
  underlying systems; (b) scrape, reproduce, or redistribute Service data at scale
  for a competing commercial product; (c) share a Pro account's access link with
  others; or (d) use the Service in any way that violates applicable law.</p>

  <h2>6. Data Sources and Accuracy</h2>
  <p>Stock data is sourced from third parties, including Yahoo Finance (via the
  yfinance library) and SEC EDGAR, and is refreshed periodically (typically daily).
  Data may be delayed, incomplete, or inaccurate. We make no warranty as to the
  accuracy, completeness, or timeliness of any data presented on the Service.</p>

  <h2>7. Service Availability</h2>
  <p>We aim to keep the Service available but do not guarantee uninterrupted access.
  The Service may be unavailable from time to time due to maintenance, technical
  issues, or factors outside our control.</p>

  <h2>8. Intellectual Property</h2>
  <p>The Service's design, branding, and original written content are owned by
  StockUpside.io. Underlying financial data is sourced from third parties as
  described above and remains subject to those parties' terms.</p>

  <h2>9. Limitation of Liability</h2>
  <p>To the maximum extent permitted by law, StockUpside.io and its operators shall
  not be liable for any indirect, incidental, special, consequential, or punitive
  damages, including but not limited to investment losses, arising from your use of
  the Service or reliance on any information presented on it.</p>

  <h2>10. Termination</h2>
  <p>We may suspend or terminate access to the Service for any account that violates
  these Terms, without prior notice.</p>

  <h2>11. Changes to These Terms</h2>
  <p>We may update these Terms from time to time. Continued use of the Service after
  changes are posted constitutes acceptance of the revised Terms. The "Last updated"
  date above reflects the most recent revision.</p>

  <h2>12. Contact</h2>
  <p>Questions about these Terms? Email us at
  <a href="mailto:hello@stockupside.io">hello@stockupside.io</a>.</p>

  <hr class="legal-divider"/>
  <p style="font-size:11px;color:var(--text3);font-family:var(--font-mono)">
    See also: <a href="/privacy">Privacy Policy</a> &nbsp;·&nbsp;
    <a href="/disclaimer">Financial Disclaimer</a> &nbsp;·&nbsp;
    <a href="/">← Back to Dashboard</a>
  </p>
</div>

<footer class="ftr">
  <div>© {yr} StockUpside.io · <a href="/disclaimer" style="color:var(--text3)">Not financial advice</a></div>
  <div class="ftr-r">
    <a href="/terms">Terms</a> ·
    <a href="/privacy">Privacy</a> ·
    <a href="/disclaimer">Disclaimer</a> ·
    <a href="mailto:hello@stockupside.io">Contact</a>
  </div>
</footer>
</body>
</html>"""


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
    <a href="/terms">Terms</a> ·
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
    <a href="/terms">Terms</a> ·
    <a href="/privacy">Privacy</a> ·
    <a href="/disclaimer">Disclaimer</a> ·
    <a href="mailto:hello@stockupside.io">Contact</a>
  </div>
</footer>
</body>
</html>"""

@app.route("/firm-track-record")
def firm_track_record_page():
    """Public leaderboard of analyst firm win rates — the page the
    'See full firm rankings →' link on each stock page points to."""
    con = get_db()
    rows = con.execute("""
        SELECT c.firm,
               COUNT(*) as total_calls,
               SUM(o.was_correct) as correct_calls,
               AVG(o.actual_return) as avg_return
        FROM analyst_call_outcomes o
        JOIN analyst_calls c ON c.id = o.call_id
        WHERE o.days_later = 90 AND o.was_correct IS NOT NULL
        GROUP BY c.firm
        HAVING total_calls >= 5
        ORDER BY correct_calls * 1.0 / total_calls DESC
        LIMIT 100
    """).fetchall()
    con.close()

    rows_html = ""
    if not rows:
        rows_html = """<tr><td colspan="4" style="text-align:center;color:var(--text2);padding:32px">
            No firms have 5+ resolved calls yet. Check back soon — this builds up automatically
            as we track upgrade/downgrade calls over time.</td></tr>"""
    else:
        for i, (firm, total, correct, avg_ret) in enumerate(rows, 1):
            win_rate = round(100 * correct / total, 1)
            color = "var(--green)" if win_rate >= 60 else "var(--amber)" if win_rate >= 45 else "var(--red)"
            avg_ret_str = f"{avg_ret:+.1f}%" if avg_ret is not None else "—"
            rows_html += f"""<tr>
                <td style="font-family:var(--font-mono);color:var(--text3)">{i}</td>
                <td>{escape(firm)}</td>
                <td style="font-family:var(--font-mono);color:{color};font-weight:700">{win_rate}%</td>
                <td style="font-family:var(--font-mono);color:var(--text2)">{total}</td>
                <td style="font-family:var(--font-mono);color:var(--text2)">{avg_ret_str}</td>
            </tr>"""

    yr = datetime.date.today().year
    return Response(f"""<!DOCTYPE html>
<html lang="en"><head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Analyst Firm Track Record | StockUpside.io</title>
  <meta name="description" content="Which Wall Street firms actually call it right? Win rates for analyst upgrade/downgrade calls, tracked 90 days out."/>
  <link rel="stylesheet" href="/style.css"/>
  <style>
    .ftr-wrap {{ max-width:760px;margin:0 auto;padding:32px 20px 64px; }}
    .ftr-wrap h1 {{ font-family:var(--font-mono);font-size:20px;margin-bottom:6px; }}
    .ftr-sub {{ color:var(--text2);font-size:13px;margin-bottom:28px;line-height:1.6; }}
    table {{ width:100%;border-collapse:collapse;font-size:13px; }}
    th {{ text-align:left;color:var(--text3);font-family:var(--font-mono);font-size:10px;
          letter-spacing:.08em;padding:10px 12px;border-bottom:1px solid var(--border); }}
    td {{ padding:10px 12px;border-bottom:1px solid var(--border); }}
  </style>
</head>
<body>
  <div class="ftr-wrap">
    <h1>▲ Analyst Firm Track Record</h1>
    <div class="ftr-sub">
      Win rate = % of upgrade/downgrade calls where the stock moved in the predicted direction
      90 days later, across every stock we track. Requires 5+ resolved calls to appear — newer
      or less-frequent firms will show up here as we accumulate more history.
      <a href="/blog/how-we-rank-stocks-by-analyst-upside">Read our full methodology →</a>
    </div>
    <table>
      <thead><tr><th>#</th><th>Firm</th><th>Win Rate</th><th>Calls</th><th>Avg Return</th></tr></thead>
      <tbody>{rows_html}</tbody>
    </table>
  </div>
  <footer class="ftr">
    <div>© {yr} StockUpside.io · <a href="/disclaimer" style="color:var(--text3)">Not financial advice</a></div>
  </footer>
</body></html>""", mimetype="text/html")


@app.route("/api/firm-track-record")
@limiter.limit("600 per hour")
def api_firm_track_record():
    """Leaderboard: win rate per analyst firm, based on whether their
    upgrade/downgrade calls were directionally right 90 days later.
    Requires >=5 resolved calls to appear, to avoid a firm with 1 lucky
    call looking like a top performer."""
    con = get_db()
    rows = con.execute("""
        SELECT c.firm,
               COUNT(*) as total_calls,
               SUM(o.was_correct) as correct_calls,
               AVG(o.actual_return) as avg_return
        FROM analyst_call_outcomes o
        JOIN analyst_calls c ON c.id = o.call_id
        WHERE o.days_later = 90 AND o.was_correct IS NOT NULL
        GROUP BY c.firm
        HAVING total_calls >= 5
        ORDER BY correct_calls * 1.0 / total_calls DESC
        LIMIT 100
    """).fetchall()
    con.close()
    return jsonify({
        "firms": [{
            "firm": r[0],
            "total_calls": r[1],
            "correct_calls": r[2],
            "win_rate_pct": round(100 * r[2] / r[1], 1) if r[1] else 0,
            "avg_return_pct": round(r[3], 2) if r[3] is not None else None,
        } for r in rows],
        "methodology": "Win rate = % of upgrade/downgrade calls where the stock moved "
                        "in the predicted direction 90 days later. Requires 5+ resolved "
                        "calls. 'Reiterate' ratings aren't scored since they carry no "
                        "directional prediction.",
    })


@app.route("/api/firm-track-record/<path:firm>")
@limiter.limit("600 per hour")
def api_firm_track_record_detail(firm):
    """Single firm's full call history with outcomes, for a firm detail page."""
    con = get_db()
    calls = con.execute("""
        SELECT c.ticker, c.grade_date, c.from_grade, c.to_grade, c.action,
               o.days_later, o.actual_return, o.was_correct
        FROM analyst_calls c
        LEFT JOIN analyst_call_outcomes o ON o.call_id = c.id AND o.days_later = 90
        WHERE c.firm = ?
        ORDER BY c.grade_date DESC
        LIMIT 200
    """, (firm,)).fetchall()
    con.close()

    if not calls:
        return jsonify({"error": f"No calls found for firm '{firm}'"}), 404

    resolved = [c for c in calls if c[7] is not None]
    win_rate = round(100 * sum(c[7] for c in resolved) / len(resolved), 1) if resolved else None

    return jsonify({
        "firm": firm,
        "total_calls": len(calls),
        "resolved_calls": len(resolved),
        "win_rate_pct": win_rate,
        "calls": [{
            "ticker": c[0], "grade_date": c[1], "from_grade": c[2],
            "to_grade": c[3], "action": c[4], "days_later": c[5],
            "actual_return_pct": c[6], "was_correct": bool(c[7]) if c[7] is not None else None,
        } for c in calls],
    })


@app.route("/api/stocks/<ticker>/calls")
@limiter.limit("600 per hour")
def api_stock_calls(ticker):
    """Recent analyst calls for a specific ticker, with each firm's
    overall track record inlined — shown on the stock detail page."""
    ticker = ticker.upper()
    con = get_db()
    calls = con.execute("""
        SELECT firm, grade_date, from_grade, to_grade, action, price_at_call
        FROM analyst_calls
        WHERE ticker = ?
        ORDER BY grade_date DESC
        LIMIT 50
    """, (ticker,)).fetchall()

    result = []
    for firm, grade_date, from_grade, to_grade, action, price_at_call in calls:
        track = con.execute("""
            SELECT COUNT(*), SUM(o.was_correct)
            FROM analyst_call_outcomes o
            JOIN analyst_calls c ON c.id = o.call_id
            WHERE c.firm = ? AND o.days_later = 90 AND o.was_correct IS NOT NULL
        """, (firm,)).fetchone()
        total, correct = track if track else (0, 0)
        result.append({
            "firm": firm, "grade_date": grade_date, "from_grade": from_grade,
            "to_grade": to_grade, "action": action, "price_at_call": price_at_call,
            "firm_win_rate_pct": round(100 * correct / total, 1) if total and total >= 5 else None,
            "firm_total_calls": total or 0,
        })
    con.close()
    return jsonify({"ticker": ticker, "calls": result})


@app.route("/api/accuracy")
@limiter.limit("600 per hour")
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

@app.route("/analyst-track-record")
def analyst_track_record_page():
    return Response(render_analyst_track_record(), mimetype="text/html")

@app.route("/api/changes")
@limiter.limit("600 per hour")
def api_changes():
    days = int(request.args.get("days", 30))
    con  = sqlite3.connect(DB_PATH)
    cutoff = (datetime.date.today() - datetime.timedelta(days=days)).isoformat()

    # Require the past snapshot to actually fall within the claimed window.
    # Without this, a ticker with only one old snapshot from months ago
    # compares today vs. that old date, showing spurious "Major upgrade"
    # for what is really just Yahoo's rolling aggregate drifting over time
    # (e.g. a stale analyst's rating expiring out of Yahoo's 12-month window).
    # Tolerance of 50% handles weekends and days generate.py didn't run.
    tolerance       = days * 0.5
    earliest_cutoff = (datetime.date.today() - datetime.timedelta(days=days + tolerance)).isoformat()

    latest_date = con.execute(
        "SELECT MAX(date) FROM snapshots"
    ).fetchone()[0]

    if not latest_date:
        con.close()
        return jsonify({"upgraded": [], "downgraded": [], "new_coverage": []})

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
          AND (past.date IS NULL OR past.date >= ?)
    """, (cutoff, latest_date, earliest_cutoff)).fetchall()

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
        "upgraded":       upgraded[:25],
        "downgraded":     downgraded[:25],
        "new_coverage":   new_coverage[:10],
        "as_of":          latest_date,
        "compared_to":    cutoff,
        "earliest_past":  earliest_cutoff,
        "days":           days,
    })

@app.route("/api/accuracy/<ticker>")
@limiter.limit("600 per hour")
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

def _render_analyst_calls(ticker: str) -> str:
    """Render the 'Recent Analyst Calls' card showing per-firm rating
    changes with each firm's track record inlined. Returns '' if there's
    no call history yet — expected for the first weeks after this
    feature launches, since the weekly collector job needs time to
    accumulate and resolve enough 90-day outcomes to be meaningful."""
    con = get_db()
    calls = con.execute("""
        SELECT firm, grade_date, from_grade, to_grade, action
        FROM analyst_calls
        WHERE ticker = ?
        ORDER BY grade_date DESC
        LIMIT 8
    """, (ticker,)).fetchall()

    if not calls:
        con.close()
        return ""

    rows_html = ""
    for firm, grade_date, from_grade, to_grade, action in calls:
        track = con.execute("""
            SELECT COUNT(*), SUM(o.was_correct)
            FROM analyst_call_outcomes o
            JOIN analyst_calls c ON c.id = o.call_id
            WHERE c.firm = ? AND o.days_later = 90 AND o.was_correct IS NOT NULL
        """, (firm,)).fetchone()
        total, correct = track if track else (0, 0)

        if total and total >= 5:
            win_rate = round(100 * correct / total, 0)
            track_html = f'<span class="ac-track" title="{correct} of {total} calls correct, 90 days out">{win_rate:.0f}% win rate ({total} calls)</span>'
        else:
            track_html = '<span class="ac-track ac-track-new">Building track record</span>'

        action_color = {"up": "var(--green)", "down": "var(--red)"}.get(action, "var(--text2)")
        action_label = {"up": "↑ Upgrade", "down": "↓ Downgrade",
                         "init": "● Initiated", "main": "— Reiterated"}.get(action, action)
        firm_safe = escape(firm)
        grade_safe = escape(f"{from_grade} → {to_grade}" if from_grade else to_grade)

        rows_html += f"""
        <div class="ac-row">
          <div class="ac-row-top">
            <span class="ac-firm">{firm_safe}</span>
            <span class="ac-action" style="color:{action_color}">{action_label}</span>
          </div>
          <div class="ac-row-bottom">
            <span class="ac-grade">{grade_safe}</span>
            <span class="ac-date">{grade_date}</span>
            {track_html}
          </div>
        </div>"""
    con.close()

    return f"""
  <div class="sp-calls">
    <h2>RECENT ANALYST CALLS</h2>
    <p class="sp-calls-sub">Upgrade/downgrade history for {ticker}, with each firm's 90-day
       track record across all stocks we track. <a href="/firm-track-record">See full firm rankings →</a></p>
    <div class="ac-list">{rows_html}
    </div>
  </div>"""


def _render_similar_stocks(similar: list | None) -> str:
    """Render the 'Similar Stocks' card for the stock detail page.
    Returns '' (renders nothing) if there's nothing to show — this is
    expected for thinly-covered sectors."""
    if not similar:
        return ""

    cards = ""
    for c in similar:
        sign  = "+" if c["upside_pct"] >= 0 else ""
        color = "var(--green)" if c["upside_pct"] >= 0 else "var(--red)"
        cards += f"""
        <a href="/stocks/{c['ticker']}" class="sim-card">
          <div class="sim-card-top">
            <span class="sim-ticker">{c['ticker']}</span>
            <span class="sim-upside" style="color:{color}">{sign}{c['upside_pct']}%</span>
          </div>
          <div class="sim-name">{c['name']}</div>
          <div class="sim-meta">{c['market_cap']} · P/E {c['pe_ratio']}x · {c['consensus']}</div>
        </a>"""

    return f"""
  <div class="sp-similar">
    <h2>SIMILAR STOCKS</h2>
    <p class="sp-similar-sub">Other {similar[0]['sector']} stocks with comparable size and valuation.</p>
    <div class="sim-grid">{cards}
    </div>
  </div>"""


def render_stock_page(s: dict, similar: list | None = None) -> str:
    # Defense in depth: `s` comes from generate.py's Yahoo Finance fetch.
    # That data isn't directly attacker-controlled, but company names/
    # tickers are still external input rendered into HTML attributes,
    # <title>, and <meta> tags below — escape the string fields so a
    # stray `<`, `"`, or `&` in upstream data can't break out of context.
    s = dict(s)
    for _field in ("name", "ticker", "sector", "consensus", "market_cap"):
        if isinstance(s.get(_field), str):
            s[_field] = str(escape(s[_field]))

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
    upside_color = ("#00e676" if s["upside_pct"] >= 20 else
                    "#69f0ae" if s["upside_pct"] >= 10 else
                    "#ffd740" if s["upside_pct"] >= 0  else
                    "#ff5252")
    upside_sign = "+" if s["upside_pct"] >= 0 else ""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>{s["ticker"]} Analyst Price Target — {s["name"]} Stock Forecast | StockUpside.io</title>
  <meta name="description" content="Wall Street analysts have a consensus price target of ${s["target_price"]} for {s["name"]} ({s["ticker"]}), {f'implying {s["upside_pct"]}% upside' if s["upside_pct"] >= 0 else f'which is {abs(s["upside_pct"])}% below'} the current price of ${s["current_price"]}. {s["analyst_count"]} analysts covered. Consensus: {s["consensus"]}. {s["sector"]} sector."/>
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
  <script type="application/ld+json">
  {{
    "@context": "https://schema.org",
    "@type": "BreadcrumbList",
    "itemListElement": [
      {{"@type": "ListItem", "position": 1, "name": "Home", "item": "https://stockupside.io/"}},
      {{"@type": "ListItem", "position": 2, "name": "{s["sector"]}", "item": "https://stockupside.io/sectors/{SECTOR_SLUGS.get(s["sector"], "")}"}},
      {{"@type": "ListItem", "position": 3, "name": "{s["ticker"]}", "item": "https://stockupside.io/stocks/{s["ticker"]}"}}
    ]
  }}
  </script>
  <script type="application/ld+json">
  {{
    "@context": "https://schema.org",
    "@type": "Dataset",
    "name": "{s["ticker"]} Analyst Price Target Consensus",
    "description": "Wall Street analyst consensus price target, upside, and rating breakdown for {s["name"]} ({s["ticker"]}), updated daily.",
    "url": "https://stockupside.io/stocks/{s["ticker"]}",
    "dateModified": "{s["last_updated"]}",
    "creator": {{"@type": "Organization", "name": "StockUpside.io", "url": "https://stockupside.io"}},
    "variableMeasured": ["Analyst Consensus Price Target", "Upside Percentage", "Analyst Rating Consensus"]
  }}
  </script>
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
    .sp-similar {{ background: var(--bg2); border: 1px solid var(--border); border-radius: 8px;
                   padding: 24px; margin-bottom: 32px; }}
    .sp-similar h2 {{ font-family: var(--font-mono); font-size: 11px; letter-spacing: .1em;
                      color: var(--text3); margin-bottom: 4px; }}
    .sp-similar-sub {{ font-size: 12px; color: var(--text2); margin-bottom: 16px; }}
    .sim-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));
                 gap: 12px; }}
    .sim-card {{ display: block; background: var(--bg3); border: 1px solid var(--border2);
                 border-radius: 6px; padding: 12px 14px; text-decoration: none;
                 transition: border-color .15s; }}
    .sim-card:hover {{ border-color: var(--accent); }}
    .sim-card-top {{ display: flex; justify-content: space-between; align-items: baseline;
                     margin-bottom: 4px; }}
    .sim-ticker {{ font-family: var(--font-mono); font-weight: 700; font-size: 13px;
                   color: var(--accent); letter-spacing: .04em; }}
    .sim-upside {{ font-family: var(--font-mono); font-weight: 700; font-size: 13px; }}
    .sim-name {{ font-size: 11px; color: var(--text2); overflow: hidden; text-overflow: ellipsis;
                 white-space: nowrap; margin-bottom: 4px; }}
    .sim-meta {{ font-size: 10px; color: var(--text3); font-family: var(--font-mono); }}
    .sp-calls {{ background: var(--bg2); border: 1px solid var(--border); border-radius: 8px;
                 padding: 24px; margin-bottom: 32px; }}
    .sp-calls h2 {{ font-family: var(--font-mono); font-size: 11px; letter-spacing: .1em;
                    color: var(--text3); margin-bottom: 4px; }}
    .sp-calls-sub {{ font-size: 12px; color: var(--text2); margin-bottom: 16px; }}
    .sp-calls-sub a {{ color: var(--accent); text-decoration: none; }}
    .ac-list {{ display: flex; flex-direction: column; gap: 1px; background: var(--border); }}
    .ac-row {{ background: var(--bg2); padding: 12px 14px; }}
    .ac-row-top {{ display: flex; justify-content: space-between; align-items: baseline;
                   margin-bottom: 4px; }}
    .ac-firm {{ font-size: 13px; font-weight: 600; color: var(--text); }}
    .ac-action {{ font-family: var(--font-mono); font-size: 11px; font-weight: 700; }}
    .ac-row-bottom {{ display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }}
    .ac-grade {{ font-size: 11px; color: var(--text2); }}
    .ac-date {{ font-family: var(--font-mono); font-size: 10px; color: var(--text3); }}
    .ac-track {{ font-family: var(--font-mono); font-size: 10px; color: var(--accent);
                 margin-left: auto; }}
    .ac-track-new {{ color: var(--text3); font-style: italic; }}
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
    <div class="sp-meta">{f'<a href="/sectors/{SECTOR_SLUGS[s["sector"]]}" style="color:inherit;text-decoration:underline;text-decoration-color:var(--border2)">{s["sector"]}</a>' if s["sector"] in SECTOR_SLUGS else s["sector"]} · Market Cap {s["market_cap"]} · P/E {s["pe_ratio"]}x</div>
  </div>

  <div class="sp-grid">

    <div class="sp-card">
      <div class="sp-card-title">ANALYST PRICE TARGET</div>
      <div class="sp-upside">{upside_sign}{s["upside_pct"]}%</div>
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
        <span class="sp-stat-v {'pos' if s['upside_pct'] >= 0 else 'neg'}">{upside_sign}{s["upside_pct"]}%</span>
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
      <strong>${s["target_price"]}</strong>, {"implying potential upside of <strong>" + str(s["upside_pct"]) + "%</strong>" if s["upside_pct"] >= 0 else "which is <strong>" + str(abs(s["upside_pct"])) + "%</strong> below the current price"}
      over the next 12 months.
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

  {_render_similar_stocks(similar)}

  {_render_analyst_calls(s["ticker"])}

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
        upside_color = ("#00e676" if s["upside_pct"] >= 40 else
                        "#69f0ae" if s["upside_pct"] >= 20 else
                        "#ffd740" if s["upside_pct"] >= 0  else
                        "#f85149")
        upside_sign  = "+" if s["upside_pct"] >= 0 else ""
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
               font-weight:700;color:{upside_color}">{upside_sign}{s["upside_pct"]}%</td>
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

# ── Startup (runs once whether launched via `python app.py` or gunicorn) ───────
init_db()
threading.Thread(target=nightly_refresh, daemon=True).start()
threading.Thread(target=weekly_digest, daemon=True).start()

# ── Main (only used for local `python server/app.py` dev runs) ─────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"\n🚀  StockUpside.io is running at http://localhost:{port}\n")
    # Only open a browser tab in local dev — never on a headless server
    if os.environ.get("ENV", "development") == "development":
        threading.Timer(1.2, lambda: webbrowser.open(f"http://localhost:{port}")).start()
    # Bind to localhost only — nginx (reverse proxy) connects via
    # localhost:5000. Binding to 0.0.0.0 would expose the Flask dev
    # server directly to the internet over plain HTTP, bypassing
    # TLS, HSTS, and nginx-level protections entirely.
    app.run(host="127.0.0.1", port=port, debug=False, threaded=True)