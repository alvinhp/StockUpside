// ─── StockUpside.io ── main.ts ─────────────────────────────────────────────────
// Bloomberg-terminal-inspired financial data dashboard
// Compiled with: tsc  (outputs to /public/main.js)

const API = "/api";

// ── Types ──────────────────────────────────────────────────────────────────────
interface Stock {
  rank: number; ticker: string; name: string; sector: string;
  current_price: number; target_price: number; upside_pct: number;
  high_target: number; low_target: number; analyst_count: number;
  consensus: string; strong_buy: number; buy: number; hold: number;
  sell: number; market_cap: string; pe_ratio: number; ytd_change: number;
  week52_low: number; week52_high: number; avg_volume: number;
  last_updated: string; locked?: boolean;
  momentum_trend: "up" | "down" | "neutral";
  momentum_detail: string;
  momentum_streak: number;
  peg_ratio: number;
  forward_pe: number;
}
interface ApiResp  { stocks: Stock[]; total: number; tier: string; last_updated: string; next_update: string; }
interface StatsResp { total_stocks: number; avg_upside: number; top_upside: number;
                      strong_buy_count: number;
                      sectors: Record<string,{count:number;avg_upside:number}>;
                      last_updated: string;
                      days_old: number;
                      freshness: "fresh" | "aging" | "stale";
                      generating: boolean; }

// ── State ──────────────────────────────────────────────────────────────────────
let all:      Stock[]     = [];
let filtered: Stock[]     = [];
let stats:    StatsResp | null = null;
let tier      = "free";
let proToken  = localStorage.getItem("su_token") || "";
let sortKey   = "rank";
let sortAsc   = true;
let secFilter    = "All";
let conFilter    = "All";
let minAnalysts  = 0;
let maxPE        = 0;   // 0 = no filter
let maxPEG       = 0;   // 0 = no filter
let query     = "";
let detail:   Stock | null = null;
let tickTimer: ReturnType<typeof setInterval> | null = null;
let currentPage  = 1;
const PAGE_SIZE  = 100;

// ── Format helpers ─────────────────────────────────────────────────────────────
const f2   = (n: number) => n.toFixed(2);
const pct  = (n: number) => `${n >= 0 ? "+" : ""}${n.toFixed(1)}%`;
const price= (n: number) => `$${f2(n)}`;
const vol  = (n: number) => n >= 1e9 ? `${(n/1e9).toFixed(1)}B` : n >= 1e6 ? `${(n/1e6).toFixed(1)}M` : `${(n/1e3).toFixed(0)}K`;
const ttm  = () => { const n=new Date(), m=new Date(n); m.setHours(24,0,0,0);
                     const d=m.getTime()-n.getTime(); const h=Math.floor(d/3600000),
                     mi=Math.floor((d%3600000)/60000), s=Math.floor((d%60000)/1000);
                     return `${String(h).padStart(2,"0")}:${String(mi).padStart(2,"0")}:${String(s).padStart(2,"0")}`; };
const cRating = (c: string) => ({ "Strong Buy":"#00e676","Buy":"#69f0ae","Hold":"#ffd740","Underperform":"#ff5252","Sell":"#d50000" }[c] || "#aaa");
const sIcon   = (s: string) => ({ "Technology":"⬡","Healthcare":"⊕","Financial Services":"◈","Consumer Cyclical":"◎",
                                   "Consumer Defensive":"▣","Energy":"◉","Industrials":"⬢","Communication Services":"◫",
                                   "Utilities":"◑","Real Estate":"▦","Basic Materials":"◆" }[s] || "◌");
function uClass(p: number): string {
  if (p >= 40) return "u-xl"; if (p >= 20) return "u-lg";
  if (p >= 10) return "u-md"; if (p >= 0)  return "u-sm"; return "u-dn";
}

// ── API ────────────────────────────────────────────────────────────────────────
async function load() {
  setLoader(true);
  try {
    const params = new URLSearchParams(window.location.search);
    const urlProToken = params.get("pro_token");
    if (urlProToken) {
      proToken = urlProToken;
      localStorage.setItem("su_token", proToken);
      window.history.replaceState({}, "", "/");  // clean the URL
    }
    if (proToken) {
      const v = await fetch(API+"/verify-token",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({token:proToken})});
      const vd = await v.json();
      tier = vd.valid ? "pro" : "free";
      if (!vd.valid) { proToken=""; localStorage.removeItem("su_token"); }
    }
    const [sr, str] = await Promise.all([
      fetch(`${API}/stocks?tier=${tier}`),
      fetch(`${API}/stats`)
    ]);
    const sd: ApiResp   = await sr.json();
    const st: StatsResp = await str.json();
    all = sd.stocks; tier = sd.tier; stats = st;
    applyFilters(); setLoader(false); renderAll();
    fixStickyOffset();
    startTicker();
  } catch(e) {
    setLoader(false);
    document.getElementById("app")!.innerHTML = errScreen(String(e));
  }
}

async function doSubscribe(email: string, plan: string = "monthly") {
  const r = await fetch(API+"/subscribe",{
    method:"POST",
    headers:{"Content-Type":"application/json"},
    body:JSON.stringify({email, plan})
  });
  const d = await r.json();
  
  // Real Stripe: redirect to checkout
  if (d.checkout_url) {
    window.location.href = d.checkout_url;
    return;
  }
  
  // Fallback error handling
  toast(d.error || "Subscription failed","err");
}

async function doFreeSubscribe() {
    const input = document.getElementById("free-email-input") as HTMLInputElement;
    const btn   = document.getElementById("free-email-btn")   as HTMLButtonElement;
    const email = input?.value?.trim();
    if (!email || !email.includes("@")) {
        toast("Enter a valid email", "err");
        return;
    }
    btn.textContent = "Saving…";
    btn.disabled = true;
    try {
        const r = await fetch(API + "/subscribe-free", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ email })
        });
        const d = await r.json();
        if (d.success) {
            localStorage.setItem("su_free_email", email);  // suppress bar on return visits
            const bar = document.getElementById("email-bar");
            if (bar) bar.innerHTML = `<div class="email-bar-confirm">
                ✓ You're on the list! We'll send the top 10 picks every Monday.
            </div>`;
        } else {
            toast(d.error || "Something went wrong", "err");
            btn.textContent = "Notify Me →";
            btn.disabled = false;
        }
    } catch(e) {
        toast("Could not connect", "err");
        btn.textContent = "Notify Me →";
        btn.disabled = false;
    }
}

// ── Filter / Sort ──────────────────────────────────────────────────────────────
function applyFilters() {
  let s = [...all];
  if (query) { const q = query.toLowerCase();
    s = s.filter(x => !x.locked && (x.ticker.toLowerCase().includes(q) || x.name.toLowerCase().includes(q) || x.sector.toLowerCase().includes(q))); }
  if (secFilter !== "All") s = s.filter(x => x.locked || x.sector === secFilter);
  if (conFilter !== "All") s = s.filter(x => x.locked || x.consensus === conFilter);
  if (minAnalysts > 0)    s = s.filter(x => x.locked || x.analyst_count >= minAnalysts);
  if (maxPE > 0)          s = s.filter(x => x.locked || (x.pe_ratio > 0 && x.pe_ratio <= maxPE));
  if (maxPEG > 0)         s = s.filter(x => x.locked || (x.peg_ratio > 0 && x.peg_ratio <= maxPEG));
  const locked = s.filter(x => x.locked), free = s.filter(x => !x.locked);
  free.sort((a,b) => {
    const av=(a as any)[sortKey]??0, bv=(b as any)[sortKey]??0;
    return typeof av === "string" ? av.localeCompare(bv)*(sortAsc?1:-1) : (av-bv)*(sortAsc?1:-1);
  });
  filtered = [...free, ...locked];
}

function doSort(key: string) {
  sortAsc = sortKey === key ? !sortAsc : (key === "rank");
  sortKey = key;
  currentPage = 1;
  applyFilters(); renderRows();
}

// ── Render helpers ─────────────────────────────────────────────────────────────
function setLoader(on: boolean) {
  const el = document.getElementById("app")!;
  if (on) el.innerHTML = `
    <div class="loader-wrap"><div class="loader-box">
      <div class="ld-logo"><span class="ld-mark">▲</span><span class="ld-name">STOCKUPSIDE</span></div>
      <div class="ld-bar"><div class="ld-fill"></div></div>
      <div class="ld-txt">Fetching analyst consensus data…</div>
    </div></div>`;
}
function errScreen(e: string) {
  return `<div class="err-wrap"><div class="err-icon">⚠</div>
    <div class="err-msg">Could not connect to server.<br><small>${e}</small></div>
    <button class="btn-retry" onclick="location.reload()">Retry</button></div>`;
}

// ── Main render ────────────────────────────────────────────────────────────────
function renderAll() {
  const sectors = stats ? Object.keys(stats.sectors).sort() : [];
  const countShow = filtered.filter(x => !x.locked).length;
  document.getElementById("app")!.innerHTML = `
    ${header()}
    ${tier === "free" ? banner() : ""}
    ${tier === "free" ? emailBar() : ""}
    ${stats?.generating ? generatingBanner() : ""}
    <div class="sbar-desktop">${statsBar()}</div>
    ${controls(sectors, countShow)}
    <div class="sbar-mobile">${statsBar()}</div>
    <div class="tbl-wrap">${table()}</div>
    <div id="pgn-container">${pagination()}</div>
    ${momentumNote()}
    ${footer()}
    ${paywallModal()}
    
    <div id="toast-el" class="toast hidden"></div>`;
  bindGlobals();
  fixStickyOffset();
}

function header() {
  return `<header class="hdr">
    <div class="hdr-l">
      <div class="brand">
        <span class="brand-mark">▲</span>
        <div><div class="brand-name">STOCKUPSIDE<span class="brand-io">.IO</span></div>
          <div class="brand-tag">Analyst Price Target Intelligence</div></div>
      </div>
    </div>
    <div class="hdr-r">
      <a href="/changes" class="hdr-link">Rating Changes</a>
      <a href="/accuracy" class="hdr-link">Accuracy</a>
      <a href="/analyst-track-record" class="hdr-link">Track Record</a>
      <a href="/stocks" class="hdr-link">All Stocks</a>
      <div class="live-chip"><span class="live-dot"></span>LIVE</div>
      <div class="refresh-chip">
        <span class="rc-lbl">UPDATES IN</span>
        <span id="cd" class="cd">--:--:--</span>
      </div>
      ${tier==="pro"
        ? `<div class="pro-chip">✦ PRO</div>`
        : `<button class="btn-pro" id="btn-paywall">Unlock Pro →</button>`}
    </div>
  </header>`;
}

function banner() {
  return `<div class="banner">
    <div class="banner-l">🔒 <strong>Viewing 10 of 1000+ stocks.</strong>
      Upgrade to reveal all analyst picks ranked by upside.</div>
    <button class="btn-upg" id="btn-banner">Upgrade — $29/mo →</button>
  </div>`;
}

function generatingBanner() {
    if (!stats?.generating) return "";
    return `<div class="gen-banner">
        <span class="gen-spinner">⟳</span>
        Data is refreshing in the background — current prices may be up to 24h old.
        This typically takes 3–6 hours.
    </div>`;
}

function statsBar() {
  if (!stats) return "";

  // Freshness indicator
  const freshnessColor = stats.freshness === "fresh"  ? "var(--green-b)"
                       : stats.freshness === "aging"  ? "var(--amber)"
                       :                                "var(--red)";
  const freshnessLabel = stats.freshness === "fresh"  ? "● Live"
                       : stats.freshness === "aging"  ? "● 1 day old"
                       :                                `● ${stats.days_old}d old`;

  return `<div class="sbar">
    <div class="scard"><div class="sc-lbl">STOCKS TRACKED</div><div class="sc-val">${stats.total_stocks}</div></div>
    <div class="scard"><div class="sc-lbl">AVG ANALYST UPSIDE</div><div class="sc-val pos">${pct(stats.avg_upside)}</div></div>
    <div class="scard"><div class="sc-lbl">TOP UPSIDE PICK</div><div class="sc-val pos">${pct(stats.top_upside)}</div></div>
    <div class="scard"><div class="sc-lbl">BUY / STRONG BUY</div><div class="sc-val pos">${stats.strong_buy_count} <span class="sc-unit">stocks</span></div></div>
    <div class="scard">
      <div class="sc-lbl">LAST UPDATED</div>
      <div class="sc-val">${stats.last_updated}</div>
      <div class="sc-fresh" style="color:${freshnessColor}">${freshnessLabel}</div>
    </div>
  </div>`;
}

function emailBar() {
    // Don't show if already subscribed (free or pro) this session
    if (localStorage.getItem("su_free_email")) return "";
    return `<div class="email-bar" id="email-bar">
        <div class="email-bar-l">
            📬 <strong>Get the top 10 picks in your inbox every week.</strong>
            Free. No credit card required.
        </div>
        <div class="email-bar-r">
            <input type="email" id="free-email-input" class="free-email-input"
                placeholder="your@email.com" />
            <button class="btn-free-sub" id="free-email-btn">Notify Me →</button>
        </div>
    </div>`;
}

function controls(sectors: string[], count: number) {
  const cons = ["All","Strong Buy","Buy","Hold","Underperform"];
  return `<div class="ctrl">
    <div class="srch-wrap">
      <span class="srch-icon">⌕</span>
      <input id="srch" class="srch" type="text" placeholder="Search ticker, company, sector…" value="${query}" />
      ${query ? `<button class="srch-clr" id="srch-clr">✕</button>` : ""}
    </div>
    <div class="flts">
      <div class="flt-g"><label class="flt-lbl">SECTOR</label>
        <select class="flt-sel" id="flt-sec">
          <option value="All">All Sectors</option>
          ${sectors.map(s=>`<option value="${s}"${secFilter===s?" selected":""}>${s}</option>`).join("")}
        </select>
      </div>
      <div class="flt-g"><label class="flt-lbl">CONSENSUS</label>
        <select class="flt-sel" id="flt-con">
          ${cons.map(c=>`<option value="${c}"${conFilter===c?" selected":""}>${c}</option>`).join("")}
        </select>
      </div>
      <div class="flt-g"><label class="flt-lbl">MIN ANALYSTS</label>
        <select class="flt-sel" id="flt-analysts">
          <option value="0"${minAnalysts===0?" selected":""}>Any</option>
          <option value="2"${minAnalysts===2?" selected":""}>2+</option>
          <option value="5"${minAnalysts===5?" selected":""}>5+</option>
          <option value="10"${minAnalysts===10?" selected":""}>10+</option>
          <option value="15"${minAnalysts===15?" selected":""}>15+</option>
          <option value="25"${minAnalysts===25?" selected":""}>25+</option>
        </select>
      </div>
      <div class="flt-g"><label class="flt-lbl">MAX P/E</label>
        <select class="flt-sel" id="flt-pe">
          <option value="0"${maxPE===0?" selected":""}>Any</option>
          <option value="10"${maxPE===10?" selected":""}>≤10</option>
          <option value="15"${maxPE===15?" selected":""}>≤15</option>
          <option value="20"${maxPE===20?" selected":""}>≤20</option>
          <option value="35"${maxPE===35?" selected":""}>≤35</option>
          <option value="50"${maxPE===50?" selected":""}>≤50</option>
        </select>
      </div>
      <div class="flt-g"><label class="flt-lbl">MAX PEG</label>
        <select class="flt-sel" id="flt-peg">
          <option value="0"${maxPEG===0?" selected":""}>Any</option>
          <option value="0.5"${maxPEG===0.5?" selected":""}>≤0.5</option>
          <option value="1"${maxPEG===1?" selected":""}>≤1.0</option>
          <option value="1.5"${maxPEG===1.5?" selected":""}>≤1.5</option>
          <option value="2"${maxPEG===2?" selected":""}>≤2.0</option>
        </select>
      </div>
      <div class="res-cnt">Showing <strong id="cnt">${count}</strong> stocks</div>
    </div>
  </div>`;
}

const COLS = [
  {k:"rank",      l:"#",         sort:true},
  {k:"ticker",    l:"TICKER",    sort:true},
  {k:"name",      l:"COMPANY",   sort:false},
  {k:"sector",    l:"SECTOR",    sort:true},
  {k:"current_price", l:"PRICE", sort:true},
  {k:"target_price",  l:"TARGET",sort:true},
  {k:"upside_pct",l:"UPSIDE %",  sort:true},
  {k:"analyst_count",l:"ANALYSTS",sort:true},
  {k:"consensus", l:"CONSENSUS", sort:true},
  {k:"momentum_trend", l:"MOMENTUM ⓘ", sort:true, title:"Momentum tracks how analyst ratings and targets have changed over time. Builds up over 7–30 days of data collection." },
  {k:"ytd_change",l:"YTD",       sort:true},
];

function table() {
  const heads = COLS.map(c => {
        const act = sortKey === c.k, arrow = act ? (sortAsc ? " ↑" : " ↓") : "";
        const titleAttr = (c as any).title ? ` title="${(c as any).title}"` : "";
        return `<th class="th${c.sort ? " sort" : ""}${act ? " act" : ""}" 
                    data-k="${c.k}"${titleAttr}>${c.l}${arrow}</th>`;
    }).join("");
  return `<table class="tbl">
    <thead><tr>${heads}</tr></thead>
    <tbody id="tbody">${rows()}</tbody>
  </table>`;
}

function pagination(): string {
  const free = filtered.filter(x => !x.locked);
  const locked = filtered.filter(x => x.locked);
  const totalFree = free.length;
  const totalPages = Math.max(1, Math.ceil((totalFree + locked.length) / PAGE_SIZE));
  if (totalPages <= 1) return "";

  const prev = currentPage > 1 ? `<button class="pg-btn" id="pg-prev">← Prev</button>` : `<button class="pg-btn pg-dis" disabled>← Prev</button>`;
  const next = currentPage < totalPages ? `<button class="pg-btn" id="pg-next">Next →</button>` : `<button class="pg-btn pg-dis" disabled>Next →</button>`;

  // Page number buttons (show up to 7 around current)
  const pages: string[] = [];
  const range = 3;
  for (let p = 1; p <= totalPages; p++) {
    if (p === 1 || p === totalPages || (p >= currentPage - range && p <= currentPage + range)) {
      pages.push(`<button class="pg-btn${p === currentPage ? " pg-act" : ""}" data-p="${p}">${p}</button>`);
    } else if (pages[pages.length-1] !== "…") {
      pages.push("…");
    }
  }

  const start = (currentPage - 1) * PAGE_SIZE + 1;
  const end = Math.min(currentPage * PAGE_SIZE, totalFree + locked.length);

  return `<div class="pgn-wrap">
    <div class="pgn-info">Showing ${start}–${end} of ${filtered.length} stocks</div>
    <div class="pgn-btns">
      ${prev}
      ${pages.map(p => p === "…" ? `<span class="pg-ellipsis">…</span>` : p).join("")}
      ${next}
    </div>
  </div>`;
}

function momentumNote(): string {
    const allNeutral = all.length > 0 && all.every(s => !s.locked && s.momentum_trend === "neutral");
    if (!allNeutral) return "";
    return `<div class="momentum-note">
        ⓘ Momentum data is still building up — it populates automatically
        as daily snapshots are collected over 7–30 days.
    </div>`;
}

function momentumBadge(s: Stock): string {
  const { momentum_trend: t, momentum_detail: d, momentum_streak: streak } = s;
  if (!t || t === "neutral") {
    return `<td><span class="mom-badge mom-neutral" title="${d || 'No change'}">→ Neutral</span></td>`;
  }
  const arrow   = t === "up" ? "↑" : "↓";
  const cls     = t === "up" ? "mom-up" : "mom-down";
  const streakTxt = streak > 0 ? ` · ${streak}d` : "";
  return `<td>
    <span class="mom-badge ${cls}" title="${d}">
      ${arrow} ${t === "up" ? "Improving" : "Weakening"}${streakTxt}
    </span>
  </td>`;
}

function showLockedFeedback(tr: HTMLElement) {
    tr.classList.add("tr-locked-active");
    setTimeout(() => tr.classList.remove("tr-locked-active"), 600);
}

function rows() {
  const start = (currentPage - 1) * PAGE_SIZE;
  const end = start + PAGE_SIZE;
  const pageItems = filtered.slice(start, end);
  return pageItems.map(s => row(s)).join("");
}

function row(s: Stock): string {
  if (s.locked) return `
    <tr class="tr-locked" data-locked="1">
      <td class="td-rank">${s.rank}</td>
      <td><span class="tk-lock">???</span></td>
      <td><span class="nm-lock">Unlock Pro to reveal</span></td>
      <td><span class="sec-dim">${s.sector||"—"}</span></td>
      <td class="dim">—</td>
      <td class="dim">—</td>
      <td class="u-lock">${pct(s.upside_pct)}</td>
      <td class="dim">—</td>
      <td><span class="con-dim">${s.consensus}</span></td>
      <td class="dim">—</td>
      <td class="dim">—</td>
    </tr>
    <tr class="tr-mobile tr-mobile-locked" data-locked="1">
      <td colspan="11">
        <div class="mobile-card mobile-card-locked" onclick="showPW()">
          <div class="mc-top-row">
            <div class="mc-rank">#${s.rank}</div>
            <div class="mc-ticker-wrap">
              <div class="mc-ticker" style="color:var(--text3)">???</div>
              <div class="mc-name" style="filter:blur(4px);user-select:none">Pro Stock Locked</div>
            </div>
            <div class="mc-upside-hero u-lg">${pct(s.upside_pct)}</div>
          </div>
          <div class="mc-lock-overlay">🔒 Unlock Pro to reveal</div>
        </div>
      </td>
    </tr>`;

  const medal = s.rank===1?"🥇":s.rank===2?"🥈":s.rank===3?"🥉":"";
  const ytdCls = s.ytd_change >= 0 ? "pos" : "neg";
  
  return `
    <tr class="tr-stock" data-ticker="${s.ticker}">
      <td class="td-rank">${medal||s.rank}</td>
      <td><a href="/stocks/${s.ticker}" class="tk" style="text-decoration:none" onclick="event.stopPropagation()">${s.ticker}</a></td>
      <td class="td-name">${s.name}</td>
      <td><span class="sec-tag">${sIcon(s.sector)} ${s.sector}</span></td>
      <td class="td-price">${price(s.current_price)}</td>
      <td class="td-price">${price(s.target_price)}</td>
      <td>
        <div class="upside-cell ${uClass(s.upside_pct)}">
          <span class="up-val">${pct(s.upside_pct)}</span>
          <div class="up-bar-bg"><div class="up-bar" style="width:${Math.min(100,Math.max(0,s.upside_pct))}%"></div></div>
        </div>
      </td>
      <td class="td-an">${s.analyst_count}</td>
      <td><span class="con-badge" style="color:${cRating(s.consensus)};border-color:${cRating(s.consensus)}33">${s.consensus}</span></td>
      ${momentumBadge(s)}
      <td class="${ytdCls}">${pct(s.ytd_change)}</td>
    </tr>
    <!-- Mobile card view (hidden on desktop) -->
    <tr class="tr-mobile" data-ticker="${s.ticker}">
      <td colspan="11">
        <div class="mobile-card" onclick="window.location.href='/stocks/${s.ticker}'">
          <div class="mc-header">
            <div class="mc-top-row">
              <div class="mc-rank">#${s.rank}</div>
              <div class="mc-ticker-wrap">
                <div class="mc-ticker">${s.ticker}</div>
                <div class="mc-name">${s.name}</div>
              </div>
              <div class="mc-upside-hero ${uClass(s.upside_pct)}">${pct(s.upside_pct)}</div>
            </div>
          </div>
          <div class="mc-grid">
            <div class="mc-stat">
              <span class="mc-label">Price</span>
              <span class="mc-value">${price(s.current_price)}</span>
            </div>
            <div class="mc-stat">
              <span class="mc-label">Target</span>
              <span class="mc-value pos">${price(s.target_price)}</span>
            </div>
            <div class="mc-stat">
              <span class="mc-label">Consensus</span>
              <span class="mc-value" style="color:${cRating(s.consensus)}">${s.consensus}</span>
            </div>
            <div class="mc-stat">
              <span class="mc-label">Analysts</span>
              <span class="mc-value">${s.analyst_count}</span>
            </div>
            <div class="mc-stat">
              <span class="mc-label">YTD</span>
              <span class="mc-value ${ytdCls}">${pct(s.ytd_change)}</span>
            </div>
            <div class="mc-stat">
              <span class="mc-label">Sector</span>
              <span class="mc-value">${s.sector}</span>
            </div>
          </div>
        </div>
      </td>
    </tr>`;
}

function renderRows() {
  const tb = document.getElementById("tbody");
  if (!tb) { renderAll(); return; }
  tb.innerHTML = rows();
  const cnt = document.getElementById("cnt");
  if (cnt) cnt.textContent = String(filtered.filter(x=>!x.locked).length);
  // Re-render pagination
  const pgnEl = document.getElementById("pgn-container");
  if (pgnEl) pgnEl.innerHTML = pagination();
  // Re-bind row clicks
  bindRows();
  bindPagination();
}

function footer() {
  return `<footer class="ftr">
    <nav class="ftr-mobile-nav">
      <a href="/changes" class="ftr-nav-link">Rating Changes</a>
      <a href="/accuracy" class="ftr-nav-link">Accuracy</a>
      <a href="/analyst-track-record" class="ftr-nav-link">Track Record</a>
      <a href="/stocks"   class="ftr-nav-link">All Stocks</a>
      <a href="/privacy"  class="ftr-nav-link">Privacy</a>
      <a href="/disclaimer" class="ftr-nav-link">Disclaimer</a>
    </nav>
    <div class="ftr-bottom">
      <div>© ${new Date().getFullYear()} StockUpside.io · Updated daily at midnight EST · <a href="/disclaimer" style="color:var(--text3)">Not financial advice</a></div>
      <div class="ftr-r"><a href="/privacy">Privacy</a> · <a href="/disclaimer">Disclaimer</a> · <a href="mailto:hello@stockupside.io">Contact</a></div>
    </div>
  </footer>`;
}

// ── Paywall modal ──────────────────────────────────────────────────────────────
function paywallModal() {
  return `<div id="pw" class="modal-bg hidden">
    <div class="modal" role="dialog">
      <button class="modal-x" id="pw-close">✕</button>
      <div class="pw-head">
        <div class="pw-mark">▲</div>
        <h2>Unlock Full Access</h2>
        <p>All analyst-ranked stocks, updated every day at midnight.</p>
      </div>
      <div class="plans">
        <div class="plan featured">
          <div class="plan-badge">MOST POPULAR</div>
          <div class="plan-name">Pro Monthly</div>
          <div class="plan-price">$29<span>/mo</span></div>
          <ul>
            <li>✓ Full top 5000+ ranked list</li>
            <li>✓ Everything in free tier</li>
            <li>✓ Priority support</li>
            <li>✓ CSV export (Coming soon)</li>
          </ul>
          <input type="email" id="pw-email" class="pw-email" placeholder="your@email.com" />
          <button class="btn-sub" id="pw-sub">Get Pro Access →</button>
          
        </div>
        <div class="plan">
          <div class="plan-name">Pro Annual</div>
          <div class="plan-price">$199<span>/yr</span></div>
          <div class="plan-save">Save $149 vs monthly</div>
          <ul>
            <li>✓ Everything in Monthly</li>
            <li>✓ 90-day historical data</li>
            <li>✓ REST API access (Coming soon)</li>
          </ul>
          <button class="btn-sub btn-sub-sec" id="pw-ann">Get Annual →</button>
        </div>
      </div>
      <div class="pw-foot">🔒 Stripe · Cancel anytime · 7-day money-back</div>
      <div class="pw-recover">
        Already a Pro subscriber? <button class="pw-recover-btn" id="pw-recover-btn">Restore access →</button>
      </div>
    </div>
  </div>`;
}



// ── Detail modal ───────────────────────────────────────────────────────────────
// function renderDetail(s: Stock) {
//   const tot = s.strong_buy + s.buy + s.hold + s.sell || 1;
//   const sbP = s.strong_buy/tot*100, bP = s.buy/tot*100, hP = s.hold/tot*100, sP = s.sell/tot*100;
//   const rng = s.high_target - s.low_target || 1;
//   const curPos = Math.min(98,Math.max(2,(s.current_price - s.low_target)/rng*100));
//   const tgtPos = Math.min(98,Math.max(2,(s.target_price  - s.low_target)/rng*100));

//   return `<div id="dt" class="modal-bg" role="dialog">
//     <div class="modal modal-lg">
//       <button class="modal-x" id="dt-close">✕</button>
//       <div class="dt-head">
//         <div class="dt-tl-group">
//           <span class="dt-tk">${s.ticker}</span>
//           <span class="dt-con" style="color:${cRating(s.consensus)}">${s.consensus}</span>
//         </div>
//         <div class="dt-nm">${s.name}</div>
//         <div class="dt-meta">${sIcon(s.sector)} ${s.sector} · Rank #${s.rank} · Market Cap ${s.market_cap}</div>
//       </div>
//       <div class="dt-grid">
//         <div class="dt-sec">
//           <div class="sec-title">PRICE ANALYSIS</div>
//           <div class="pg">
//             ${[["Current Price",price(s.current_price),""],["Analyst Target",price(s.target_price),"pos"],
//                ["Upside",pct(s.upside_pct),"pos"],["Bull Target",price(s.high_target),""],
//                ["Bear Target",price(s.low_target),""],["P/E Ratio",`${s.pe_ratio}x`,""]]
//               .map(([l,v,c])=>`<div class="pi"><div class="pi-l">${l}</div><div class="pi-v ${c}">${v}</div></div>`).join("")}
//           </div>
//           <div class="rng-lbl"><span>Price Target Range</span><span>${price(s.low_target)} — ${price(s.high_target)}</span></div>
//           <div class="rng-track">
//             <div class="rng-dot rng-cur" style="left:${curPos}%"><div class="rng-flag">Now</div></div>
//             <div class="rng-dot rng-tgt" style="left:${tgtPos}%"><div class="rng-flag rng-flag-t">Target</div></div>
//           </div>
//         </div>
//         <div class="dt-sec">
//           <div class="sec-title">ANALYST CONSENSUS (${tot} analysts)</div>
//           <div class="ratings">
//             ${[["Strong Buy",s.strong_buy,sbP,"#00e676"],["Buy",s.buy,bP,"#69f0ae"],
//                ["Hold",s.hold,hP,"#ffd740"],["Sell",s.sell,sP,"#ff5252"]]
//               .map(([l,n,p,c])=>`<div class="rb">
//                 <span class="rb-l" style="color:${c}">${l}</span>
//                 <div class="rb-bg"><div class="rb-fill" style="width:${p}%;background:${c}"></div></div>
//                 <span class="rb-n">${n}</span><span class="rb-p">${(p as number).toFixed(0)}%</span>
//               </div>`).join("")}
//           </div>
//           <div class="ds-grid">
//             ${[["52W LOW",price(s.week52_low),""],["52W HIGH",price(s.week52_high),""],
//                ["YTD",pct(s.ytd_change),s.ytd_change>=0?"pos":"neg"],["AVG VOL",vol(s.avg_volume),""]]
//               .map(([l,v,c])=>`<div class="dsi"><div class="dsi-l">${l}</div><div class="dsi-v ${c}">${v}</div></div>`).join("")}
//           </div>
//         </div>
//       </div>
//       <div class="dt-disc">⚠ Analyst targets are consensus estimates and do not guarantee future performance. Not financial advice.</div>
//     </div>
//   </div>`;
// }

// ── UI helpers ─────────────────────────────────────────────────────────────────
function closeModal(id: string) {
  const el = document.getElementById(id);
  if (el) el.classList.add("hidden");
}
function showPW() {
  const el = document.getElementById("pw");
  if (el) el.classList.remove("hidden");
}
function toast(msg: string, type: "ok"|"err") {
  const el = document.getElementById("toast-el");
  if (!el) return;
  el.textContent = msg; el.className = `toast ${type}`;
  setTimeout(()=>{ el.className = "toast hidden"; }, 4000);
}
function fixStickyOffset() {
  requestAnimationFrame(() => {
    // On mobile, the table scrolls with the page — no fixed height needed
    if (window.innerWidth <= 768) {
      const wrap = document.querySelector(".tbl-wrap") as HTMLElement;
      if (wrap) { wrap.style.height = "auto"; wrap.style.overflowX = "visible"; }
      return;
    }
    let offset = 0;
    const hdr  = document.querySelector(".hdr") as HTMLElement;
    const ban  = document.querySelector(".banner") as HTMLElement;
    const sbar = document.querySelector(".sbar-desktop .sbar") as HTMLElement;
    const ctrl = document.querySelector(".ctrl") as HTMLElement;
    
    if (hdr)  offset += hdr.offsetHeight;
    if (ban)  offset += ban.offsetHeight;
    if (sbar) offset += sbar.offsetHeight;

    if (ctrl) {
      ctrl.style.top = `${offset}px`;
      offset += ctrl.offsetHeight;
    }

    const wrap = document.querySelector(".tbl-wrap") as HTMLElement;
    if (wrap) {
      const footerHeight = 60;
      wrap.style.height = `calc(100vh - ${offset}px - ${footerHeight}px)`;
    }

    document.documentElement.style.setProperty("--bars-height", `${offset}px`);
  });
}
function startTicker() {
  if (tickTimer) clearInterval(tickTimer);
  tickTimer = setInterval(()=>{
    const el = document.getElementById("cd");
    if (el) el.textContent = ttm();
  }, 1000);
  const el = document.getElementById("cd");
  if (el) el.textContent = ttm();
}

// ── Event binding ──────────────────────────────────────────────────────────────
function bindPagination() {
  document.getElementById("pg-prev")?.addEventListener("click", () => {
    if (currentPage > 1) { currentPage--; renderRows(); window.scrollTo({top: 0, behavior: "smooth"}); }
  });
  document.getElementById("pg-next")?.addEventListener("click", () => {
    const totalPages = Math.ceil(filtered.length / PAGE_SIZE);
    if (currentPage < totalPages) { currentPage++; renderRows(); window.scrollTo({top: 0, behavior: "smooth"}); }
  });
  document.querySelectorAll<HTMLElement>(".pg-btn[data-p]").forEach(btn => {
    btn.addEventListener("click", () => {
      currentPage = parseInt(btn.dataset.p!);
      renderRows();
      window.scrollTo({top: 0, behavior: "smooth"});
    });
  });
}

function bindRows() {
  document.querySelectorAll<HTMLElement>(".tr-stock").forEach(tr => {
    tr.onclick = (e) => {
      // Don't intercept direct clicks on the ticker <a> tag — browser handles those
      if ((e.target as HTMLElement).tagName === "A") return;
      const tk = tr.dataset.ticker;
      if (tk) window.location.href = `/stocks/${tk}`;
    };
    // Cursor cue
    tr.style.cursor = "pointer";
  });
  document.querySelectorAll<HTMLElement>(".tr-locked").forEach(tr => {
    tr.onclick = () => {
      showLockedFeedback(tr);
      showPW();
    };
    tr.style.cursor = "pointer";
  });
}

function bindGlobals() {
  // Pro token recovery
  const recoverBtn = document.getElementById("pw-recover-btn");
  if (recoverBtn) recoverBtn.onclick = async () => {
    const email = prompt("Enter your Pro subscriber email:");
    if (!email || !email.includes("@")) return;
    recoverBtn.textContent = "Looking up…";
    try {
      const r = await fetch(API + "/get-token", {
        method: "POST", headers: {"Content-Type":"application/json"},
        body: JSON.stringify({email})
      });
      const d = await r.json();
      if (d.token) {
        proToken = d.token;
        localStorage.setItem("su_token", proToken);
        window.location.href = d.redirect;
      } else {
        toast(d.error || "Email not found", "err");
        recoverBtn.textContent = "Restore access →";
      }
    } catch {
      toast("Could not connect", "err");
      recoverBtn.textContent = "Restore access →";
    }
  };

  // Paywall buttons
  const bPW = document.getElementById("btn-paywall");
  if (bPW) bPW.onclick = showPW;
  const bBn = document.getElementById("btn-banner");
  if (bBn) bBn.onclick = showPW;
  const bPWClose = document.getElementById("pw-close");
  if (bPWClose) bPWClose.onclick = () => closeModal("pw");
  const freeSubBtn = document.getElementById("free-email-btn");
  if (freeSubBtn) freeSubBtn.onclick = doFreeSubscribe;
  document.getElementById("pw")?.addEventListener("click", e => {
    if ((e.target as HTMLElement).id === "pw") closeModal("pw");
  });

  // Subscribe
  const subBtn = document.getElementById("pw-sub");
if (subBtn) subBtn.onclick = async () => {
  const em = (document.getElementById("pw-email") as HTMLInputElement)?.value?.trim();
  if (!em||!em.includes("@")) { toast("Enter a valid email","err"); return; }
  subBtn.textContent = "Processing…"; 
  (subBtn as HTMLButtonElement).disabled = true;
  await doSubscribe(em, "monthly");  // explicit plan
  // If success, user is redirected; if error, button re-enables above
  subBtn.textContent = "Get Pro Access →"; 
  (subBtn as HTMLButtonElement).disabled = false;
};
  const annBtn = document.getElementById("pw-ann");
  if (annBtn) annBtn.onclick = async () => {
    const em = (document.getElementById("pw-email") as HTMLInputElement)?.value?.trim();
    if (!em||!em.includes("@")) { toast("Enter your email first","err"); document.getElementById("pw-email")?.focus(); return; }
    annBtn.textContent = "Processing…";
    (annBtn as HTMLButtonElement).disabled = true;
    await doSubscribe(em, "annual");
    // Note: if it succeeds, user gets redirected to Stripe so we never reach here
    annBtn.textContent = "Get Annual →";
    (annBtn as HTMLButtonElement).disabled = false;
  };

  // Sort
  document.querySelectorAll<HTMLElement>(".th.sort").forEach(th => {
    th.onclick = () => { const k = th.dataset.k; if (k) doSort(k); };
  });

  // Search
  const srch = document.getElementById("srch") as HTMLInputElement;
  if (srch) srch.oninput = () => { query = srch.value; currentPage=1; applyFilters(); renderRows(); };
  const srchClr = document.getElementById("srch-clr");
  if (srchClr) srchClr.onclick = () => { query=""; currentPage=1; if(srch) srch.value=""; applyFilters(); renderAll(); };

  // Sector filter
  const fSec = document.getElementById("flt-sec") as HTMLSelectElement;
  if (fSec) fSec.onchange = () => { secFilter=fSec.value; currentPage=1; applyFilters(); renderRows(); };

  // Consensus filter
  const fCon = document.getElementById("flt-con") as HTMLSelectElement;
  if (fCon) fCon.onchange = () => { conFilter=fCon.value; currentPage=1; applyFilters(); renderRows(); };

  // Analyst count filter
  const fAnalysts = document.getElementById("flt-analysts") as HTMLSelectElement;
  if (fAnalysts) fAnalysts.onchange = () => { minAnalysts=parseInt(fAnalysts.value); currentPage=1; applyFilters(); renderRows(); };

  // P/E filter
  const fPE = document.getElementById("flt-pe") as HTMLSelectElement;
  if (fPE) fPE.onchange = () => { maxPE=parseFloat(fPE.value); currentPage=1; applyFilters(); renderRows(); };

  // PEG filter
  const fPEG = document.getElementById("flt-peg") as HTMLSelectElement;
  if (fPEG) fPEG.onchange = () => { maxPEG=parseFloat(fPEG.value); currentPage=1; applyFilters(); renderRows(); };

  // Row clicks
  bindRows();

  // Pagination
  bindPagination();

  // Ticker
  startTicker();
}

// ── Boot ───────────────────────────────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", load);