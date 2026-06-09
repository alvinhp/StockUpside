# ▲ StockUpside.io

**Top stocks ranked by analyst consensus price target upside. Updated daily.**

A Bloomberg-terminal-style financial dashboard with a freemium model designed to generate $1k–$10k MRR.

---

## Quick Start

### Prerequisites
- Python 3.9+ (`python3 --version`)
- TypeScript (optional, pre-compiled JS included): `npm install -g typescript`

### Run (Mac / Linux)
```bash
chmod +x start.sh
./start.sh
```

### Run (Windows)
```
start.bat
```

### Manual start
```bash
pip3 install flask
python3 server/app.py
```

Then open **http://localhost:5000**

---

## Project Structure

```
stockupside-io/
├── src/
│   └── main.ts          # TypeScript frontend (source)
├── public/
│   ├── index.html       # HTML shell
│   ├── style.css        # Full Bloomberg-terminal CSS
│   └── main.js          # Compiled from main.ts (pre-built)
├── server/
│   ├── app.py           # Flask backend + REST API
│   └── cache.db         # SQLite cache (auto-created)
├── tsconfig.json
├── package.json
├── start.sh             # Mac/Linux launcher
├── start.bat            # Windows launcher
└── README.md
```

---

## How It Works

- **Backend** (`server/app.py`): Python Flask app serving a JSON REST API
  - `/api/stocks?tier=free|pro` — ranked stock list (10 for free, unlimited for pro)
  - `/api/stats` — aggregate stats for the dashboard header
  - `/api/subscribe` — email + plan capture (mock Stripe, ready to wire up)
  - `/api/verify-token` — token validation
  - Caches data in SQLite, regenerates once per day
- **Frontend** (`src/main.ts` → `public/main.js`): Vanilla TypeScript, no React
  - Full filtering, sorting, search
  - Stock detail modal with analyst breakdown + price range chart
  - Paywall modal with freemium gate (top 10 free, 100 for Pro)

---

## Recompile TypeScript

```bash
npm run build
# or
tsc
```

---

## Monetization — Reaching $1k–$10k MRR

### Current freemium gate
- **Free**: Top 10 stocks (visible)
- **Pro ($29/mo) / ($199/yr)**: All stocks + details

### Wire up real payments (Stripe)

1. Install Stripe: `pip3 install stripe`
2. In `server/app.py`, replace the `# MOCK` block in `/api/subscribe`:

```python
import stripe
stripe.api_key = "sk_live_..."

session = stripe.checkout.Session.create(
    payment_method_types=["card"],
    line_items=[{"price": "price_xxx", "quantity": 1}],
    mode="subscription",
    success_url="http://yourdomain.com/success?token={CHECKOUT_SESSION_ID}",
    cancel_url="http://yourdomain.com/cancel",
    customer_email=email,
)
return jsonify({"checkout_url": session.url})
```

3. In `src/main.ts`, redirect to the Stripe checkout URL instead of calling `doSubscribe`.

### Real stock data (Yahoo Finance)

Replace `generate_stocks()` in `server/app.py` with live data: #ALREADY DONE

```bash
pip3 install yfinance pandas
```

```python
import yfinance as yf

def fetch_live_data():
    tickers = [t for t,*_ in UNIVERSE]
    data = yf.download(tickers, period="1d", group_by="ticker")
    # parse analyst targets from yf.Ticker(t).info["targetMeanPrice"]
    ...
```

### Growth levers

| Lever | Target MRR Impact |
|-------|------------------|
| $29/mo plan × 35 users | $1,015/mo |
| $29/mo plan × 100 users | $2,900/mo |
| $199/yr plan × 50 users | ~$830/mo |
| Add $99 API tier | +$500–2k/mo |
| SEO content (stock analysis blog) | organic acquisition |
| Affiliate: broker referrals | passive revenue |

### Recommended deployment
- Hetzner CX22 | ~$4/mo | Best value, 2 vCPU, 4GB RAM, plenty for Flask + SQLite
- DigitalOcean Droplet | $6/mo | Slightly easier UI, same specs
- Render.com | $7/mo | Managed, auto-deploys from Git
- Railway.app | ~$5/mo | Similar to Render
- Custom domain: stockupside.io (~$12/yr)

---

## Environment Variables (Production)

```bash
STRIPE_SECRET_KEY=sk_live_...
STRIPE_WEBHOOK_SECRET=whsec_...
ALLOWED_ORIGIN=https://stockupside.io
PORT=5000
```

---

## License
MIT — use commercially, modify freely.

## CURRENT PROGRESS
- Front page displays a list of stocks gathered from SEC EDGAR and ranked based on their upside potential from analyst data from yahoo finance
- Clicking on a ticker sends you to a /stocks/TICKER page with its data such as fundamentals and analyst consensus, along with a summary at the bottom
- Front page has a momentum tracker that tracks how analyst ratings have changed over time. Currently no data -- momentum is stored in a snapshots table, data will be gathered over time
- Front page has an accuracy tab that gives data on how accurate analyst consensus has been in the past. Currently no data -- data is still being stored in the SQLite database
- Sector and consensum filters
- Email list: Get emails from free users and send weekly "top 10 stock picks"
- "Last updated" timestamp + data freshness indicator (Done)
- Make sure site is useable on mobile

## TO DO LIST
- Implement Stripe payment functions (Requires domain URL for business verification, website deployment should be done first)
- Weekly email digest
- Filter by number of analysts (>1, >5, >10, >25, ...)
- Deploy onto a website hoster
- Better SEO optimization
- Post on Reddit (Discuss top 10 stock picks)
- Post on Twitter/X (Discuss top 10 stock picks)
- Get first paying users

## TO DO LIST (Further in the future)
- Affiliate revenue
- Filter by market cap
- Historical tracking: "Was this analyst right?"
- Watchlist: Let free users save 5 stocks, pro users can save unlimited
- CSV/Excel export
- API access tier: Charge $99/mo to target quant hobbyist and small funds
- Email alerts
- Sector-specific landing pages (i.e. /sectors/technology, ...)
- Sector rotation signals
- Plain English summaries on the stock pages

## Growth roadmap
- $0 MRR | Current; site is not yet deployed
- $100+ MRR | 4-6 users paying for the monthly subscription
- $1k MRR | 35-60 paying users
- $10k MRR (Target) | 345-603 paying users -- eqivalent to a high-paying salary
- $100k MRR (Reach) | 3449-6030 paying users, unlikely to reach this point