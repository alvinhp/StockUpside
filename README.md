# ▲ StockUpside.io

**Top stocks ranked by analyst consensus price target upside. Updated daily.**

A Bloomberg-terminal-style financial dashboard with a freemium model designed to generate $1k–$10k MRR.

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

## Monetization — Reaching $1k–$10k MRR

### Current freemium gate
- **Free**: Top 10 stocks (visible)
- **Pro ($29/mo) / ($199/yr)**: All stocks + details


### Growth levers

| Lever | Target MRR Impact |
|-------|------------------|
| $29/mo plan × 35 users | $1,015/mo |
| $29/mo plan × 100 users | $2,900/mo |
| $199/yr plan × 50 users | ~$830/mo |
| Add $99 API tier | +$500–2k/mo |
| SEO content (stock analysis blog) | organic acquisition |
| Affiliate: broker referrals | passive revenue |

## License
MIT — use commercially, modify freely.

## CURRENT PROGRESS
- Front page displays a list of stocks gathered from SEC EDGAR and ranked based on their upside potential from analyst data from yahoo finance
- Clicking on a ticker sends you to a /stocks/TICKER page with its data such as fundamentals and analyst consensus, along with a summary at the bottom
- Front page has a momentum tracker that tracks how analyst ratings have changed over time. Currently no data -- momentum is stored in a snapshots table, data will be gathered over time
- Front page has an accuracy tab that gives data on how accurate analyst consensus has been in the past. Currently no data -- data is still being stored in the SQLite database
- Sector and consensum filters
- Email list: Get emails from free users and send weekly "top 10 stock picks"
- "Last updated" timestamp + data freshness indicator
- Make sure site is useable on mobile
- Implemented Stripe payment
- Filter by number of analysts (>1, >5, >10, >25, ...)
- Deployed onto Digial Ocean
- Bought stockupside.io domain
- Analyst accuracy page
- P/E, PEG, and momentum filters
- Fixed pro token and xss security vulerabilities
- Filter based email sending for pro users
- Created pages (100 stocks per page)
- Added login features for pro users
- Changed generate.py to update the cache per 50 stocks. Also saves data if process times out.
- Added blog posts
- Added watchlist
- Enabled email sending features
- Added market cap filters
- Improved SEO
- Similar stocks in /stocks/TICKER page
- 

## TO DO LIST
- Post on Reddit (User 9:1 rule)
- Post on Twitter/X (Be active in fintwt)
- Get first paying users

## TO DO LIST (After $100 - $1k MRR)
- Affiliate revenue
- Historical tracking: "Was this analyst right?"
- CSV/Excel export
- API access tier: Charge $99/mo to target quant hobbyist and small funds
- Email alerts
- Sector-specific landing pages (i.e. /sectors/technology, ...)
- Sector rotation signals
- Plain English summaries on the stock pages

## Growth roadmap
- $0 MRR | Current; site is not yet deployed
- $100+ MRR | 4-6 users paying for the monthly subscription
  Expected: 3-6 months
- $1k MRR | 35-60 paying users
  Expected: ~1 yr
- $10k MRR (Target) | 345-603 paying users -- eqivalent to a high-paying salary
  Expected: 3-5 years
- $100k MRR (Reach) | 3449-6030 paying users, unlikely to reach this point
  Expected: never