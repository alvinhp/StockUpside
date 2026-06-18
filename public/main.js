"use strict";
// ─── StockUpside.io ── main.ts ─────────────────────────────────────────────────
// Bloomberg-terminal-inspired financial data dashboard
// Compiled with: tsc  (outputs to /public/main.js)
const API = "/api";
// ── XSS helper ─────────────────────────────────────────────────────────────────
// Any user-controlled string (search query, etc.) interpolated into a
// template that gets assigned via innerHTML must be escaped first —
// otherwise a query like `"><img src=x onerror=alert(1)>` breaks out of
// the search input's value="..." attribute and executes arbitrary script.
function escapeHtml(str) {
    return str
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
}
// ── State ──────────────────────────────────────────────────────────────────────
let all = [];
let filtered = [];
let stats = null;
let tier = "free";
let proToken = localStorage.getItem("su_token") || "";
let watchlist = new Set();
const isWatchlistPage = window.location.pathname === "/watchlist";
let emailPrefs = null;
let sortKey = "rank";
let sortAsc = true;
let secFilter = "All";
let conFilter = "All";
let minAnalysts = 0;
let minMarketCap = 0; // raw USD; 0 = no filter
let maxPE = 0; // 0 = no filter
let maxPEG = 0; // 0 = no filter
let momentumFilter = "All"; // All | up | down | neutral
let query = "";
let detail = null;
let tickTimer = null;
let currentPage = 1;
const PAGE_SIZE = 100;
// ── Format helpers ─────────────────────────────────────────────────────────────
const f2 = (n) => n.toFixed(2);
const pct = (n) => `${n >= 0 ? "+" : ""}${n.toFixed(1)}%`;
const price = (n) => `$${f2(n)}`;
const vol = (n) => n >= 1e9 ? `${(n / 1e9).toFixed(1)}B` : n >= 1e6 ? `${(n / 1e6).toFixed(1)}M` : `${(n / 1e3).toFixed(0)}K`;
const ttm = () => {
    const n = new Date(), m = new Date(n);
    m.setHours(24, 0, 0, 0);
    const d = m.getTime() - n.getTime();
    const h = Math.floor(d / 3600000), mi = Math.floor((d % 3600000) / 60000), s = Math.floor((d % 60000) / 1000);
    return `${String(h).padStart(2, "0")}:${String(mi).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
};
const cRating = (c) => ({ "Strong Buy": "#00e676", "Buy": "#69f0ae", "Hold": "#ffd740", "Underperform": "#ff5252", "Sell": "#d50000" }[c] || "#aaa");
const sIcon = (s) => ({ "Technology": "⬡", "Healthcare": "⊕", "Financial Services": "◈", "Consumer Cyclical": "◎",
    "Consumer Defensive": "▣", "Energy": "◉", "Industrials": "⬢", "Communication Services": "◫",
    "Utilities": "◑", "Real Estate": "▦", "Basic Materials": "◆" }[s] || "◌");
function uClass(p) {
    if (p >= 40)
        return "u-xl";
    if (p >= 20)
        return "u-lg";
    if (p >= 10)
        return "u-md";
    if (p >= 0)
        return "u-sm";
    return "u-dn";
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
            window.history.replaceState({}, "", "/"); // clean the URL
        }
        if (proToken) {
            const v = await fetch(API + "/verify-token", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ token: proToken }) });
            const vd = await v.json();
            tier = vd.valid ? "pro" : "free";
            if (!vd.valid) {
                proToken = "";
                localStorage.removeItem("su_token");
            }
        }
        const [sr, str] = await Promise.all([
            fetch(`${API}/stocks${proToken ? `?token=${encodeURIComponent(proToken)}` : ""}`),
            fetch(`${API}/stats`)
        ]);
        const sd = await sr.json();
        const st = await str.json();
        all = sd.stocks;
        tier = sd.tier;
        stats = st;
        // Reflect the server-applied default filters on the free tier so the
        // dropdowns show what's actually being filtered (locked/disabled).
        if (tier !== "pro" && sd.free_filters) {
            minMarketCap = sd.free_filters.min_market_cap;
            minAnalysts = sd.free_filters.min_analysts;
        }
        if (tier === "pro") {
            try {
                const wr = await fetch(`${API}/watchlist?token=${encodeURIComponent(proToken)}`);
                const wd = await wr.json();
                watchlist = new Set(wd.tickers || []);
            }
            catch {
                // Non-fatal — watchlist star state just won't be pre-populated
            }
        }
        if (isWatchlistPage) {
            applyFilters();
            setLoader(false);
            renderWatchlistPage();
            fixStickyOffset();
            startTicker();
            return;
        }
        applyFilters();
        setLoader(false);
        renderAll();
        fixStickyOffset();
        startTicker();
    }
    catch (e) {
        setLoader(false);
        document.getElementById("app").innerHTML = errScreen(String(e));
    }
}
async function toggleWatchlist(ticker, starEl) {
    const adding = !watchlist.has(ticker);
    // Optimistic UI update
    if (adding) {
        watchlist.add(ticker);
        starEl.textContent = "★";
        starEl.classList.add("wl-active");
        starEl.title = "Remove from watchlist";
    }
    else {
        watchlist.delete(ticker);
        starEl.textContent = "☆";
        starEl.classList.remove("wl-active");
        starEl.title = "Add to watchlist";
    }
    try {
        const r = await fetch(`${API}/watchlist`, {
            method: adding ? "POST" : "DELETE",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ token: proToken, ticker }),
        });
        if (!r.ok)
            throw new Error(await r.text());
        toast(adding ? `${ticker} added to your watchlist` : `${ticker} removed from your watchlist`, "ok");
        if (isWatchlistPage && !adding) {
            // Removing from the dedicated watchlist page — drop the row immediately
            applyFilters();
            renderWatchlistRows();
        }
    }
    catch {
        // Revert on failure
        if (adding) {
            watchlist.delete(ticker);
            starEl.textContent = "☆";
            starEl.classList.remove("wl-active");
            starEl.title = "Add to watchlist";
        }
        else {
            watchlist.add(ticker);
            starEl.textContent = "★";
            starEl.classList.add("wl-active");
            starEl.title = "Remove from watchlist";
        }
        toast("Couldn't update watchlist — please try again.", "err");
    }
}
async function doSubscribe(email, plan = "monthly") {
    const r = await fetch(API + "/subscribe", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email, plan })
    });
    const d = await r.json();
    // Real Stripe: redirect to checkout
    if (d.checkout_url) {
        window.location.href = d.checkout_url;
        return;
    }
    // Fallback error handling
    toast(d.error || "Subscription failed", "err");
}
async function doFreeSubscribe() {
    const input = document.getElementById("free-email-input");
    const btn = document.getElementById("free-email-btn");
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
            plausible?.("Free Signup");
            localStorage.setItem("su_free_email", email); // suppress bar on return visits
            const bar = document.getElementById("email-bar");
            if (bar)
                bar.innerHTML = `<div class="email-bar-confirm">
                ✓ You're on the list! We'll send the top 10 picks every Monday.
            </div>`;
        }
        else {
            toast(d.error || "Something went wrong", "err");
            btn.textContent = "Notify Me →";
            btn.disabled = false;
        }
    }
    catch (e) {
        toast("Could not connect", "err");
        btn.textContent = "Notify Me →";
        btn.disabled = false;
    }
}
// ── Filter / Sort ──────────────────────────────────────────────────────────────
function applyFilters() {
    let s = [...all];
    if (isWatchlistPage) {
        s = s.filter(x => !x.locked && watchlist.has(x.ticker));
    }
    if (query) {
        const q = query.toLowerCase();
        s = s.filter(x => !x.locked && (x.ticker.toLowerCase().includes(q) || x.name.toLowerCase().includes(q) || x.sector.toLowerCase().includes(q)));
    }
    if (secFilter !== "All")
        s = s.filter(x => x.locked || x.sector === secFilter);
    if (conFilter !== "All")
        s = s.filter(x => x.locked || x.consensus === conFilter);
    if (minAnalysts > 0)
        s = s.filter(x => x.locked || x.analyst_count >= minAnalysts);
    if (minMarketCap > 0)
        s = s.filter(x => x.locked || (x.market_cap_raw ?? 0) >= minMarketCap);
    if (maxPE > 0)
        s = s.filter(x => x.locked || (x.pe_ratio > 0 && x.pe_ratio <= maxPE));
    if (maxPEG > 0)
        s = s.filter(x => x.locked || (x.peg_ratio > 0 && x.peg_ratio <= maxPEG));
    if (momentumFilter !== "All")
        s = s.filter(x => x.locked || x.momentum_trend === momentumFilter);
    const locked = s.filter(x => x.locked), free = s.filter(x => !x.locked);
    free.sort((a, b) => {
        const av = a[sortKey] ?? 0, bv = b[sortKey] ?? 0;
        return typeof av === "string" ? av.localeCompare(bv) * (sortAsc ? 1 : -1) : (av - bv) * (sortAsc ? 1 : -1);
    });
    filtered = [...free, ...locked];
}
function doSort(key) {
    sortAsc = sortKey === key ? !sortAsc : (key === "rank");
    sortKey = key;
    currentPage = 1;
    if (isWatchlistPage) {
        applyFilters();
        renderWatchlistRows();
        return;
    }
    applyFilters();
    renderRows();
}
// ── Watchlist page ───────────────────────────────────────────────────────────
function watchlistTable(items) {
    if (tier !== "pro") {
        return `<div class="wl-locked-wrap">
      <div class="wl-locked-icon">🔒</div>
      <h2>Watchlists are a Pro feature</h2>
      <p>Track unlimited stocks across the whole market. Upgrade to Pro to
         build your own watchlist and see it here.</p>
      <button class="btn-pro" id="wl-upgrade-btn">Unlock Pro →</button>
    </div>`;
    }
    if (items.length === 0) {
        return `<div class="wl-empty-wrap">
      <div class="wl-empty-icon">☆</div>
      <h2>Your watchlist is empty</h2>
      <p>Click the ☆ next to any stock on <a href="/stocks">All Stocks</a> to add it here.</p>
    </div>`;
    }
    const heads = COLS.map(c => {
        const act = sortKey === c.k, arrow = act ? (sortAsc ? " ↑" : " ↓") : "";
        const titleAttr = c.title ? ` title="${c.title}"` : "";
        return `<th class="th${c.sort ? " sort" : ""}${act ? " act" : ""}"
                data-k="${c.k}"${titleAttr}>${c.l}${arrow}</th>`;
    }).join("");
    return `<table class="tbl">
    <thead><tr>${heads}</tr></thead>
    <tbody id="tbody">${items.map(s => row(s)).join("")}</tbody>
  </table>`;
}
function renderWatchlistPage() {
    const items = filtered.filter(x => !x.locked);
    document.getElementById("app").innerHTML = `
    ${header()}
    <div class="wl-wrap">
      <div class="wl-title-row">
        <h1>★ My Watchlist</h1>
        ${tier === "pro" ? `<div class="wl-count">${items.length} stock${items.length === 1 ? "" : "s"} tracked</div>` : ""}
      </div>
      <div class="tbl-wrap">${watchlistTable(items)}</div>
    </div>
    ${footer()}
    ${paywallModal()}
    <div id="toast-el" class="toast hidden"></div>`;
    bindGlobals();
    bindRows();
    const upg = document.getElementById("wl-upgrade-btn");
    if (upg)
        upg.onclick = () => showPW();
}
function renderWatchlistRows() {
    const items = filtered.filter(x => !x.locked);
    const tb = document.getElementById("tbody");
    if (!tb) {
        renderWatchlistPage();
        return;
    }
    tb.innerHTML = items.map(s => row(s)).join("");
    const cnt = document.querySelector(".wl-count");
    if (cnt)
        cnt.textContent = `${items.length} stock${items.length === 1 ? "" : "s"} tracked`;
    bindRows();
    if (items.length === 0)
        renderWatchlistPage();
}
// ── Render helpers ─────────────────────────────────────────────────────────────
function setLoader(on) {
    const el = document.getElementById("app");
    if (on)
        el.innerHTML = `
    <div class="loader-wrap"><div class="loader-box">
      <div class="ld-logo"><span class="ld-mark">▲</span><span class="ld-name">STOCKUPSIDE</span></div>
      <div class="ld-bar"><div class="ld-fill"></div></div>
      <div class="ld-txt">Fetching analyst consensus data…</div>
    </div></div>`;
}
function errScreen(e) {
    return `<div class="err-wrap"><div class="err-icon">⚠</div>
    <div class="err-msg">Could not connect to server.<br><small>${escapeHtml(e)}</small></div>
    <button class="btn-retry" onclick="location.reload()">Retry</button></div>`;
}
// ── Main render ────────────────────────────────────────────────────────────────
function renderAll() {
    const sectors = stats ? Object.keys(stats.sectors).sort() : [];
    const countShow = filtered.filter(x => !x.locked).length;
    document.getElementById("app").innerHTML = `
    ${header()}
    ${tier === "free" ? banner() : ""}
    ${tier === "free" ? emailBar() : ""}
    ${stats?.generating ? generatingBanner() : ""}
    <div class="sbar-desktop">${statsBar()}</div>
    <div class="ctrl-and-nav">
      <div class="mobile-nav-tabs">${mobileNavTabs()}</div>
      ${controls(sectors, countShow)}
    </div>
    <div class="sbar-mobile">${statsBarMobile()}</div>
    <div class="tbl-wrap">${table()}</div>
    <div id="pgn-container">${pagination()}</div>
    ${momentumNote()}
    ${footer()}
    ${paywallModal()}
    ${tier === "pro" ? emailPrefsModal() : ""}
    
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
      <a href="/sectors" class="hdr-link">Sectors</a>
      <a href="/blog" class="hdr-link">Blog</a>
      <a href="/watchlist" class="hdr-link">My Watchlist</a>
      <div class="live-chip"><span class="live-dot"></span>LIVE</div>
      <div class="refresh-chip">
        <span class="rc-lbl">UPDATES IN</span>
        <span id="cd" class="cd">--:--:--</span>
      </div>
      ${tier === "pro"
        ? `<button class="btn-emailprefs" id="btn-emailprefs">✉ Digest Settings</button><div class="pro-chip">✦ PRO</div><button class="btn-login" id="btn-logout">Log Out</button>`
        : `<button class="btn-login" id="btn-login">Log In</button><button class="btn-pro" id="btn-paywall">Unlock Pro →</button>`}
    </div>
  </header>`;
}
function banner() {
    return `<div class="banner">
    <div class="banner-l">🔒 <strong>Viewing 10 of 3,500+ stocks.</strong>
      Upgrade to reveal all analyst picks ranked by upside.</div>
    <button class="btn-upg" id="btn-banner">Upgrade - $29/mo →</button>
  </div>`;
}
function generatingBanner() {
    if (!stats?.generating)
        return "";
    return `<div class="gen-banner">
        <span class="gen-spinner">⟳</span>
        Data is refreshing in the background. Current prices may be up to 24h old.
        This typically takes 3–6 hours.
    </div>`;
}
function statsBar() {
    if (!stats)
        return "";
    // Freshness indicator
    const freshnessColor = stats.freshness === "fresh" ? "var(--green-b)"
        : stats.freshness === "aging" ? "var(--amber)"
            : "var(--red)";
    const freshnessLabel = stats.freshness === "fresh" ? "● Live"
        : stats.freshness === "aging" ? "● 1 day old"
            : `● ${stats.days_old}d old`;
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
    if (localStorage.getItem("su_free_email"))
        return "";
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
function statsBarMobile() {
    if (!stats)
        return "";
    const freshnessColor = stats.freshness === "fresh" ? "var(--green-b)"
        : stats.freshness === "aging" ? "var(--amber)"
            : "var(--red)";
    const freshnessLabel = stats.freshness === "fresh" ? "● Live"
        : stats.freshness === "aging" ? "● 1 day old"
            : `● ${stats.days_old}d old`;
    return `<div class="sbar sbar-mobile-only">
    <div class="scard">
      <div class="sc-lbl">LAST UPDATED</div>
      <div class="sc-val">${stats.last_updated}</div>
      <div class="sc-fresh" style="color:${freshnessColor}">${freshnessLabel}</div>
    </div>
  </div>`;
}
function controls(sectors, count) {
    const cons = ["All", "Strong Buy", "Buy", "Hold", "Underperform"];
    return `<div class="ctrl">
    <div class="srch-wrap">
      <span class="srch-icon">⌕</span>
      <input id="srch" class="srch" type="text" placeholder="Search ticker, company, sector…" value="${escapeHtml(query)}" />
      ${query ? `<button class="srch-clr" id="srch-clr">✕</button>` : ""}
    </div>
    <div class="flts">
      <div class="flt-g"><label class="flt-lbl">SECTOR</label>
        <select class="flt-sel" id="flt-sec">
          <option value="All">All Sectors</option>
          ${sectors.map(s => `<option value="${s}"${secFilter === s ? " selected" : ""}>${s}</option>`).join("")}
        </select>
      </div>
      <div class="flt-g"><label class="flt-lbl">CONSENSUS</label>
        <select class="flt-sel" id="flt-con">
          ${cons.map(c => `<option value="${c}"${conFilter === c ? " selected" : ""}>${c}</option>`).join("")}
        </select>
      </div>
      <div class="flt-g"><label class="flt-lbl">MARKET CAP${tier !== "pro" ? ` <span class="flt-lock" title="Free tier default. Upgrade to Pro to change">🔒</span>` : ""}</label>
        <select class="flt-sel" id="flt-mcap"${tier !== "pro" ? " disabled" : ""}>
          <option value="0"${minMarketCap === 0 ? " selected" : ""}>Any (Nano+)</option>
          <option value="50000000"${minMarketCap === 50000000 ? " selected" : ""}>Micro+ (&gt;$50M)</option>
          <option value="250000000"${minMarketCap === 250000000 ? " selected" : ""}>Small+ (&gt;$250M)</option>
          <option value="2000000000"${minMarketCap === 2000000000 ? " selected" : ""}>Mid+ (&gt;$2B)</option>
          <option value="10000000000"${minMarketCap === 10000000000 ? " selected" : ""}>Large+ (&gt;$10B)</option>
        </select>
      </div>
      <div class="flt-g"><label class="flt-lbl">MIN ANALYSTS${tier !== "pro" ? ` <span class="flt-lock" title="Free tier default. Upgrade to Pro to change">🔒</span>` : ""}</label>
        <select class="flt-sel" id="flt-analysts"${tier !== "pro" ? " disabled" : ""}>
          <option value="0"${minAnalysts === 0 ? " selected" : ""}>Any</option>
          <option value="2"${minAnalysts === 2 ? " selected" : ""}>2+</option>
          <option value="5"${minAnalysts === 5 ? " selected" : ""}>5+</option>
          <option value="10"${minAnalysts === 10 ? " selected" : ""}>10+</option>
          <option value="15"${minAnalysts === 15 ? " selected" : ""}>15+</option>
          <option value="25"${minAnalysts === 25 ? " selected" : ""}>25+</option>
        </select>
      </div>
      <div class="flt-g"><label class="flt-lbl">MAX P/E</label>
        <select class="flt-sel" id="flt-pe">
          <option value="0"${maxPE === 0 ? " selected" : ""}>Any</option>
          <option value="10"${maxPE === 10 ? " selected" : ""}>≤10</option>
          <option value="15"${maxPE === 15 ? " selected" : ""}>≤15</option>
          <option value="20"${maxPE === 20 ? " selected" : ""}>≤20</option>
          <option value="35"${maxPE === 35 ? " selected" : ""}>≤35</option>
          <option value="50"${maxPE === 50 ? " selected" : ""}>≤50</option>
        </select>
      </div>
      <div class="flt-g"><label class="flt-lbl">MAX PEG</label>
        <select class="flt-sel" id="flt-peg">
          <option value="0"${maxPEG === 0 ? " selected" : ""}>Any</option>
          <option value="0.5"${maxPEG === 0.5 ? " selected" : ""}>≤0.5</option>
          <option value="1"${maxPEG === 1 ? " selected" : ""}>≤1.0</option>
          <option value="1.5"${maxPEG === 1.5 ? " selected" : ""}>≤1.5</option>
          <option value="2"${maxPEG === 2 ? " selected" : ""}>≤2.0</option>
        </select>
      </div>
      <div class="flt-g"><label class="flt-lbl">MOMENTUM</label>
        <select class="flt-sel" id="flt-momentum">
          <option value="All"${momentumFilter === "All" ? " selected" : ""}>Any</option>
          <option value="up"${momentumFilter === "up" ? " selected" : ""}>↑ Improving</option>
          <option value="neutral"${momentumFilter === "neutral" ? " selected" : ""}>→ Neutral</option>
          <option value="down"${momentumFilter === "down" ? " selected" : ""}>↓ Weakening</option>
        </select>
      </div>
      <div class="res-cnt">Showing <strong id="cnt">${count}</strong> stocks</div>
    </div>
  </div>`;
}
const COLS = [
    { k: "rank", l: "#", sort: true },
    { k: "watchlist", l: "★", sort: false, title: "Add to your watchlist (Pro)" },
    { k: "ticker", l: "TICKER", sort: true },
    { k: "name", l: "COMPANY", sort: false },
    { k: "sector", l: "SECTOR", sort: true },
    { k: "current_price", l: "PRICE", sort: true },
    { k: "target_price", l: "TARGET", sort: true },
    { k: "upside_pct", l: "UPSIDE %", sort: true },
    { k: "analyst_count", l: "ANALYSTS", sort: true },
    { k: "consensus", l: "CONSENSUS", sort: true },
    { k: "momentum_trend", l: "MOMENTUM ⓘ", sort: true, title: "Momentum tracks how analyst ratings and targets have changed over time. Builds up over 7–30 days of data collection." },
    { k: "ytd_change", l: "YTD", sort: true },
];
function table() {
    const heads = COLS.map(c => {
        const act = sortKey === c.k, arrow = act ? (sortAsc ? " ↑" : " ↓") : "";
        const titleAttr = c.title ? ` title="${c.title}"` : "";
        return `<th class="th${c.sort ? " sort" : ""}${act ? " act" : ""}" 
                    data-k="${c.k}"${titleAttr}>${c.l}${arrow}</th>`;
    }).join("");
    return `<table class="tbl">
    <thead><tr>${heads}</tr></thead>
    <tbody id="tbody">${rows()}</tbody>
  </table>`;
}
function pagination() {
    const free = filtered.filter(x => !x.locked);
    const locked = filtered.filter(x => x.locked);
    const totalFree = free.length;
    const totalPages = Math.max(1, Math.ceil((totalFree + locked.length) / PAGE_SIZE));
    if (totalPages <= 1)
        return "";
    const prev = currentPage > 1 ? `<button class="pg-btn" id="pg-prev">← Prev</button>` : `<button class="pg-btn pg-dis" disabled>← Prev</button>`;
    const next = currentPage < totalPages ? `<button class="pg-btn" id="pg-next">Next →</button>` : `<button class="pg-btn pg-dis" disabled>Next →</button>`;
    // Page number buttons (show up to 7 around current)
    const pages = [];
    const range = 3;
    for (let p = 1; p <= totalPages; p++) {
        if (p === 1 || p === totalPages || (p >= currentPage - range && p <= currentPage + range)) {
            pages.push(`<button class="pg-btn${p === currentPage ? " pg-act" : ""}" data-p="${p}">${p}</button>`);
        }
        else if (pages[pages.length - 1] !== "…") {
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
function momentumNote() {
    const allNeutral = all.length > 0 && all.every(s => !s.locked && s.momentum_trend === "neutral");
    if (!allNeutral)
        return "";
    return `<div class="momentum-note">
        ⓘ Momentum data is still building up, it populates automatically
        as daily snapshots are collected over 7–30 days.
    </div>`;
}
function momentumBadge(s) {
    const { momentum_trend: t, momentum_detail: d, momentum_streak: streak } = s;
    if (!t || t === "neutral") {
        return `<td><span class="mom-badge mom-neutral" title="${d || 'No change'}">→ Neutral</span></td>`;
    }
    const arrow = t === "up" ? "↑" : "↓";
    const cls = t === "up" ? "mom-up" : "mom-down";
    const streakTxt = streak > 0 ? ` · ${streak}d` : "";
    return `<td>
    <span class="mom-badge ${cls}" title="${d}">
      ${arrow} ${t === "up" ? "Improving" : "Weakening"}${streakTxt}
    </span>
  </td>`;
}
function showLockedFeedback(tr) {
    tr.classList.add("tr-locked-active");
    setTimeout(() => tr.classList.remove("tr-locked-active"), 600);
}
function rows() {
    const start = (currentPage - 1) * PAGE_SIZE;
    const end = start + PAGE_SIZE;
    const pageItems = filtered.slice(start, end);
    return pageItems.map(s => row(s)).join("");
}
function row(s) {
    if (s.locked)
        return `
    <tr class="tr-locked" data-locked="1">
      <td class="td-rank">${s.rank}</td>
      <td class="td-watch"><span class="wl-star wl-dim" title="Unlock Pro to use watchlists">☆</span></td>
      <td><span class="tk-lock">???</span></td>
      <td><span class="nm-lock">Unlock Pro to reveal</span></td>
      <td><span class="sec-dim">${s.sector || "—"}</span></td>
      <td class="dim">—</td>
      <td class="dim">—</td>
      <td class="u-lock">${pct(s.upside_pct)}</td>
      <td class="dim">—</td>
      <td><span class="con-dim">${s.consensus}</span></td>
      <td class="dim">—</td>
      <td class="dim">—</td>
    </tr>
    <tr class="tr-mobile tr-mobile-locked" data-locked="1">
      <td colspan="12">
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
    const medal = s.rank === 1 ? "🥇" : s.rank === 2 ? "🥈" : s.rank === 3 ? "🥉" : "";
    const ytdCls = s.ytd_change >= 0 ? "pos" : "neg";
    const inWatchlist = watchlist.has(s.ticker);
    const starCls = inWatchlist ? "wl-star wl-active" : "wl-star";
    const starChar = inWatchlist ? "★" : "☆";
    return `
    <tr class="tr-stock" data-ticker="${s.ticker}">
      <td class="td-rank">${medal || s.rank}</td>
      <td class="td-watch"><span class="${starCls}" data-watch-ticker="${s.ticker}" title="${inWatchlist ? "Remove from watchlist" : "Add to watchlist"}">${starChar}</span></td>
      <td><a href="/stocks/${s.ticker}" class="tk" style="text-decoration:none" onclick="event.stopPropagation()">${s.ticker}</a></td>
      <td class="td-name">${s.name}</td>
      <td><span class="sec-tag">${sIcon(s.sector)} ${s.sector}</span></td>
      <td class="td-price">${price(s.current_price)}</td>
      <td class="td-price">${price(s.target_price)}</td>
      <td>
        <div class="upside-cell ${uClass(s.upside_pct)}">
          <span class="up-val">${pct(s.upside_pct)}</span>
          <div class="up-bar-bg"><div class="up-bar" style="width:${Math.min(100, Math.max(0, s.upside_pct))}%"></div></div>
        </div>
      </td>
      <td class="td-an">${s.analyst_count}</td>
      <td><span class="con-badge" style="color:${cRating(s.consensus)};border-color:${cRating(s.consensus)}33">${s.consensus}</span></td>
      ${momentumBadge(s)}
      <td class="${ytdCls}">${pct(s.ytd_change)}</td>
    </tr>
    <!-- Mobile card view (hidden on desktop) -->
    <tr class="tr-mobile" data-ticker="${s.ticker}">
      <td colspan="12">
        <div class="mobile-card" onclick="window.location.href='/stocks/${s.ticker}'">
          <div class="mc-header">
            <div class="mc-top-row">
              <div class="mc-rank">#${s.rank}</div>
              <div class="mc-ticker-wrap">
                <div class="mc-ticker">${s.ticker}
                  <span class="${starCls}" data-watch-ticker="${s.ticker}" title="${inWatchlist ? "Remove from watchlist" : "Add to watchlist"}" onclick="event.stopPropagation()">${starChar}</span>
                </div>
                <div class="mc-name">${s.name}</div>
              </div>
              <div class="mc-upside-wrap">
                <div class="mc-upside-lbl">UPSIDE %</div>
                <div class="mc-upside-hero ${uClass(s.upside_pct)}">${pct(s.upside_pct)}</div>
              </div>
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
              <span class="mc-label">Momentum</span>
              <span class="mc-value">${s.momentum_trend === "up" ? "↑ Improving" : s.momentum_trend === "down" ? "↓ Weakening" : "→ Neutral"}</span>
            </div>
          </div>
        </div>
      </td>
    </tr>`;
}
function renderRows() {
    const tb = document.getElementById("tbody");
    if (!tb) {
        renderAll();
        return;
    }
    tb.innerHTML = rows();
    const cnt = document.getElementById("cnt");
    if (cnt)
        cnt.textContent = String(filtered.filter(x => !x.locked).length);
    // Re-render pagination
    const pgnEl = document.getElementById("pgn-container");
    if (pgnEl)
        pgnEl.innerHTML = pagination();
    // Re-bind row clicks
    bindRows();
    bindPagination();
}
function mobileNavTabs() {
    return `
    <a href="/changes" class="mob-tab">Rating Changes</a>
    <a href="/accuracy" class="mob-tab">Accuracy</a>
    <a href="/analyst-track-record" class="mob-tab">Track Record</a>
    <a href="/stocks" class="mob-tab">All Stocks</a>
    <a href="/sectors" class="mob-tab">Sectors</a>
    <a href="/blog" class="mob-tab">Blog</a>
    <a href="/watchlist" class="mob-tab">My Watchlist</a>
    <a href="/terms" class="mob-tab">Terms</a>
    <a href="/privacy" class="mob-tab">Privacy</a>`;
}
function footer() {
    return `<footer class="ftr">
    <nav class="ftr-mobile-nav">
      <a href="/changes" class="ftr-nav-link">Rating Changes</a>
      <a href="/accuracy" class="ftr-nav-link">Accuracy</a>
      <a href="/analyst-track-record" class="ftr-nav-link">Track Record</a>
      <a href="/stocks"   class="ftr-nav-link">All Stocks</a>
      <a href="/sectors"  class="ftr-nav-link">Sectors</a>
      <a href="/blog"     class="ftr-nav-link">Blog</a>
      <a href="/terms"    class="ftr-nav-link">Terms</a>
      <a href="/privacy"  class="ftr-nav-link">Privacy</a>
      <a href="/disclaimer" class="ftr-nav-link">Disclaimer</a>
    </nav>
    <div class="ftr-bottom">
      <div>© ${new Date().getFullYear()} StockUpside.io · Updated daily at midnight EST · <a href="/disclaimer" style="color:var(--text3)">Not financial advice</a></div>
      <div class="ftr-r"><a href="/terms">Terms</a> · <a href="/privacy">Privacy</a> · <a href="/disclaimer">Disclaimer</a> · <a href="mailto:hello@stockupside.io">Contact</a></div>
    </div>
  </footer>`;
}
// ── Paywall modal ──────────────────────────────────────────────────────────────
function emailPrefsModal() {
    const sectors = stats ? Object.keys(stats.sectors).sort() : [];
    const p = emailPrefs || { sector: "All", consensus: "All", min_analysts: 0, max_pe: 0, max_peg: 0, momentum: "All" };
    const cons = ["All", "Strong Buy", "Buy", "Hold", "Underperform"];
    return `<div id="ep" class="modal-bg hidden">
    <div class="modal modal-narrow" role="dialog">
      <button class="modal-x" id="ep-close">✕</button>
      <div class="pw-head">
        <div class="pw-mark">✉</div>
        <h2>Weekly Digest Settings</h2>
        <p>Every Monday we'll email your Top 10 picks. Set filters below to
           get picks tailored to your strategy. Leave as "Any" for the
           overall Top 10.</p>
      </div>
      <div class="ep-form">
        <div class="ep-row">
          <div class="flt-g"><label class="flt-lbl">SECTOR</label>
            <select class="flt-sel" id="ep-sector">
              <option value="All"${p.sector === "All" ? " selected" : ""}>All Sectors</option>
              ${sectors.map(s => `<option value="${s}"${p.sector === s ? " selected" : ""}>${s}</option>`).join("")}
            </select>
          </div>
          <div class="flt-g"><label class="flt-lbl">CONSENSUS</label>
            <select class="flt-sel" id="ep-consensus">
              ${cons.map(c => `<option value="${c}"${p.consensus === c ? " selected" : ""}>${c}</option>`).join("")}
            </select>
          </div>
        </div>
        <div class="ep-row">
          <div class="flt-g"><label class="flt-lbl">MIN ANALYSTS</label>
            <select class="flt-sel" id="ep-analysts">
              <option value="0"${p.min_analysts === 0 ? " selected" : ""}>Any</option>
              <option value="2"${p.min_analysts === 2 ? " selected" : ""}>2+</option>
              <option value="5"${p.min_analysts === 5 ? " selected" : ""}>5+</option>
              <option value="10"${p.min_analysts === 10 ? " selected" : ""}>10+</option>
              <option value="15"${p.min_analysts === 15 ? " selected" : ""}>15+</option>
              <option value="25"${p.min_analysts === 25 ? " selected" : ""}>25+</option>
            </select>
          </div>
          <div class="flt-g"><label class="flt-lbl">MAX P/E</label>
            <select class="flt-sel" id="ep-pe">
              <option value="0"${p.max_pe === 0 ? " selected" : ""}>Any</option>
              <option value="10"${p.max_pe === 10 ? " selected" : ""}>≤10</option>
              <option value="15"${p.max_pe === 15 ? " selected" : ""}>≤15</option>
              <option value="20"${p.max_pe === 20 ? " selected" : ""}>≤20</option>
              <option value="35"${p.max_pe === 35 ? " selected" : ""}>≤35</option>
              <option value="50"${p.max_pe === 50 ? " selected" : ""}>≤50</option>
            </select>
          </div>
        </div>
        <div class="ep-row">
          <div class="flt-g"><label class="flt-lbl">MAX PEG</label>
            <select class="flt-sel" id="ep-peg">
              <option value="0"${p.max_peg === 0 ? " selected" : ""}>Any</option>
              <option value="0.5"${p.max_peg === 0.5 ? " selected" : ""}>≤0.5</option>
              <option value="1"${p.max_peg === 1 ? " selected" : ""}>≤1.0</option>
              <option value="1.5"${p.max_peg === 1.5 ? " selected" : ""}>≤1.5</option>
              <option value="2"${p.max_peg === 2 ? " selected" : ""}>≤2.0</option>
            </select>
          </div>
          <div class="flt-g"><label class="flt-lbl">MOMENTUM</label>
            <select class="flt-sel" id="ep-momentum">
              <option value="All"${p.momentum === "All" ? " selected" : ""}>Any</option>
              <option value="up"${p.momentum === "up" ? " selected" : ""}>↑ Improving</option>
              <option value="neutral"${p.momentum === "neutral" ? " selected" : ""}>→ Neutral</option>
              <option value="down"${p.momentum === "down" ? " selected" : ""}>↓ Weakening</option>
            </select>
          </div>
        </div>
      </div>
      <div id="ep-match-info" class="ep-match-info"></div>
      <button class="btn-sub" id="ep-save">Save Preferences</button>
      <div class="pw-foot">Applies to next Monday's digest. If no stocks
        match your filters that week, we'll send the overall Top 10 instead.</div>
    </div>
  </div>`;
}
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
            <li>✓ Full top 3500+ ranked list</li>
            <li>✓ Filter by sector, consensus, momentum, and more</li>
            <li>✓ Unlimited watchlist</li>
            <li>✓ Weekly stock digest based on filters</li>
            <li>✓ Priority support</li>
            <li>✓ Everything in free tier</li>
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
function closeModal(id) {
    const el = document.getElementById(id);
    if (el)
        el.classList.add("hidden");
}
function showPW() {
    const el = document.getElementById("pw");
    if (el)
        el.classList.remove("hidden");
}
async function showEP() {
    const el = document.getElementById("ep");
    if (!el)
        return;
    el.classList.remove("hidden");
    // Load current prefs from server (in case they were set on another device)
    try {
        const r = await fetch(`${API}/email-prefs?token=${encodeURIComponent(proToken)}`);
        const d = await r.json();
        if (d.prefs) {
            emailPrefs = d.prefs;
            renderEPModal();
        }
    }
    catch {
        // Non-fatal — modal still shows defaults/cached values
    }
}
function renderEPModal() {
    const el = document.getElementById("ep");
    if (!el)
        return;
    // emailPrefsModal() returns the full `<div id="ep" ...>...</div>` wrapper —
    // replace just the inner content so the #ep element itself stays in the DOM
    const html = emailPrefsModal();
    const start = html.indexOf(">") + 1;
    const end = html.lastIndexOf("</div>");
    el.innerHTML = html.slice(start, end);
    bindEmailPrefs();
}
function toast(msg, type) {
    const el = document.getElementById("toast-el");
    if (!el)
        return;
    el.textContent = msg;
    el.className = `toast ${type}`;
    setTimeout(() => { el.className = "toast hidden"; }, 4000);
}
function fixStickyOffset() {
    requestAnimationFrame(() => {
        // On mobile, the table scrolls with the page — no fixed height needed
        if (window.innerWidth <= 768) {
            const wrap = document.querySelector(".tbl-wrap");
            if (wrap) {
                wrap.style.height = "auto";
                wrap.style.overflowX = "visible";
            }
            return;
        }
        let offset = 0;
        const hdr = document.querySelector(".hdr");
        const ban = document.querySelector(".banner");
        const sbar = document.querySelector(".sbar-desktop .sbar");
        const ctrl = document.querySelector(".ctrl");
        if (hdr)
            offset += hdr.offsetHeight;
        if (ban)
            offset += ban.offsetHeight;
        if (sbar)
            offset += sbar.offsetHeight;
        if (ctrl) {
            ctrl.style.top = `${offset}px`;
            offset += ctrl.offsetHeight;
        }
        const wrap = document.querySelector(".tbl-wrap");
        if (wrap) {
            const footerHeight = 60;
            wrap.style.height = `calc(100vh - ${offset}px - ${footerHeight}px)`;
        }
        document.documentElement.style.setProperty("--bars-height", `${offset}px`);
    });
}
function startTicker() {
    if (tickTimer)
        clearInterval(tickTimer);
    tickTimer = setInterval(() => {
        const el = document.getElementById("cd");
        if (el)
            el.textContent = ttm();
    }, 1000);
    const el = document.getElementById("cd");
    if (el)
        el.textContent = ttm();
}
// ── Event binding ──────────────────────────────────────────────────────────────
function bindPagination() {
    document.getElementById("pg-prev")?.addEventListener("click", () => {
        if (currentPage > 1) {
            currentPage--;
            renderRows();
            window.scrollTo({ top: 0, behavior: "smooth" });
        }
    });
    document.getElementById("pg-next")?.addEventListener("click", () => {
        const totalPages = Math.ceil(filtered.length / PAGE_SIZE);
        if (currentPage < totalPages) {
            currentPage++;
            renderRows();
            window.scrollTo({ top: 0, behavior: "smooth" });
        }
    });
    document.querySelectorAll(".pg-btn[data-p]").forEach(btn => {
        btn.addEventListener("click", () => {
            currentPage = parseInt(btn.dataset.p);
            renderRows();
            window.scrollTo({ top: 0, behavior: "smooth" });
        });
    });
}
function bindEmailPrefs() {
    const closeBtn = document.getElementById("ep-close");
    if (closeBtn)
        closeBtn.onclick = () => closeModal("ep");
    document.getElementById("ep")?.addEventListener("click", e => {
        if (e.target.id === "ep")
            closeModal("ep");
    });
    const saveBtn = document.getElementById("ep-save");
    if (!saveBtn)
        return;
    saveBtn.onclick = async () => {
        const sector = document.getElementById("ep-sector")?.value || "All";
        const consensus = document.getElementById("ep-consensus")?.value || "All";
        const minA = parseInt(document.getElementById("ep-analysts")?.value || "0");
        const maxPe = parseFloat(document.getElementById("ep-pe")?.value || "0");
        const maxPeg = parseFloat(document.getElementById("ep-peg")?.value || "0");
        const momentum = document.getElementById("ep-momentum")?.value || "All";
        const prefs = {
            sector, consensus, min_analysts: minA,
            max_pe: maxPe, max_peg: maxPeg, momentum,
        };
        saveBtn.disabled = true;
        saveBtn.textContent = "Saving…";
        const infoEl = document.getElementById("ep-match-info");
        try {
            const r = await fetch(`${API}/email-prefs`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ token: proToken, prefs }),
            });
            const d = await r.json();
            if (d.success) {
                emailPrefs = d.prefs;
                const n = d.matching_stocks ?? 0;
                if (infoEl) {
                    if (n === 0) {
                        infoEl.innerHTML = `⚠ No stocks currently match these filters. We'll send the overall Top 10 instead until something matches.`;
                        infoEl.className = "ep-match-info ep-match-warn";
                    }
                    else {
                        infoEl.innerHTML = `✓ ${n} stock${n === 1 ? "" : "s"} currently match. Your Monday digest will pick the top ${Math.min(10, n)} of these.`;
                        infoEl.className = "ep-match-info ep-match-ok";
                    }
                }
                toast("Digest preferences saved", "ok");
            }
            else {
                toast(d.error || "Could not save preferences", "err");
            }
        }
        catch {
            toast("Could not connect", "err");
        }
        finally {
            saveBtn.disabled = false;
            saveBtn.textContent = "Save Preferences";
        }
    };
}
function bindRows() {
    document.querySelectorAll(".wl-star").forEach(star => {
        star.onclick = (e) => {
            e.stopPropagation();
            const ticker = star.dataset.watchTicker;
            if (!ticker)
                return; // locked row's star has no ticker
            if (tier !== "pro") {
                toast("Watchlists are a Pro feature — upgrade to track your own stocks.", "err");
                showPW();
                return;
            }
            toggleWatchlist(ticker, star);
        };
    });
    document.querySelectorAll(".tr-stock").forEach(tr => {
        tr.onclick = (e) => {
            // Don't intercept direct clicks on the ticker <a> tag — browser handles those
            if (e.target.tagName === "A")
                return;
            const tk = tr.dataset.ticker;
            if (tk)
                window.location.href = `/stocks/${tk}`;
        };
        // Cursor cue
        tr.style.cursor = "pointer";
    });
    document.querySelectorAll(".tr-locked").forEach(tr => {
        tr.onclick = () => {
            showLockedFeedback(tr);
            plausible?.("Paywall Opened", { props: { source: "locked_row" } });
            showPW();
        };
        tr.style.cursor = "pointer";
    });
}
function bindGlobals() {
    // Pro login link request
    const recoverBtn = document.getElementById("pw-recover-btn");
    if (recoverBtn)
        recoverBtn.onclick = async () => {
            const email = prompt("Enter the email you subscribed with. We'll send you a login link:");
            if (!email || !email.includes("@"))
                return;
            recoverBtn.textContent = "Sending…";
            try {
                const r = await fetch(API + "/get-token", {
                    method: "POST", headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ email })
                });
                const d = await r.json();
                if (d.success) {
                    toast("If that email has a Pro subscription, a login link is on its way.", "ok");
                }
                else {
                    toast(d.error || "Something went wrong", "err");
                }
            }
            catch {
                toast("Could not connect", "err");
            }
            finally {
                recoverBtn.textContent = "Restore access →";
            }
        };
    // Paywall buttons
    const bPW = document.getElementById("btn-paywall");
    if (bPW)
        bPW.onclick = () => { plausible?.("Paywall Opened", { props: { source: "header_button" } }); showPW(); };
    const bBn = document.getElementById("btn-banner");
    if (bBn)
        bBn.onclick = () => { plausible?.("Paywall Opened", { props: { source: "banner" } }); showPW(); };
    const bPWClose = document.getElementById("pw-close");
    if (bPWClose)
        bPWClose.onclick = () => closeModal("pw");
    const freeSubBtn = document.getElementById("free-email-btn");
    if (freeSubBtn)
        freeSubBtn.onclick = doFreeSubscribe;
    document.getElementById("pw")?.addEventListener("click", e => {
        if (e.target.id === "pw")
            closeModal("pw");
    });
    // Email digest preferences (Pro only)
    const bEP = document.getElementById("btn-emailprefs");
    if (bEP)
        bEP.onclick = showEP;
    if (document.getElementById("ep"))
        bindEmailPrefs();
    // Subscribe
    const subBtn = document.getElementById("pw-sub");
    if (subBtn)
        subBtn.onclick = async () => {
            const em = document.getElementById("pw-email")?.value?.trim();
            if (!em || !em.includes("@")) {
                toast("Enter a valid email", "err");
                return;
            }
            subBtn.textContent = "Processing…";
            subBtn.disabled = true;
            plausible?.("Checkout Started", { props: { plan: "monthly" } });
            await doSubscribe(em, "monthly"); // explicit plan
            // If success, user is redirected; if error, button re-enables above
            subBtn.textContent = "Get Pro Access →";
            subBtn.disabled = false;
        };
    const annBtn = document.getElementById("pw-ann");
    if (annBtn)
        annBtn.onclick = async () => {
            const em = document.getElementById("pw-email")?.value?.trim();
            if (!em || !em.includes("@")) {
                toast("Enter your email first", "err");
                document.getElementById("pw-email")?.focus();
                return;
            }
            annBtn.textContent = "Processing…";
            annBtn.disabled = true;
            plausible?.("Checkout Started", { props: { plan: "annual" } });
            await doSubscribe(em, "annual");
            // Note: if it succeeds, user gets redirected to Stripe so we never reach here
            annBtn.textContent = "Get Annual →";
            annBtn.disabled = false;
        };
    // Sort
    document.querySelectorAll(".th.sort").forEach(th => {
        th.onclick = () => { const k = th.dataset.k; if (k)
            doSort(k); };
    });
    // Search
    const srch = document.getElementById("srch");
    if (srch)
        srch.oninput = () => { query = srch.value; currentPage = 1; applyFilters(); renderRows(); };
    const srchClr = document.getElementById("srch-clr");
    if (srchClr)
        srchClr.onclick = () => { query = ""; currentPage = 1; if (srch)
            srch.value = ""; applyFilters(); renderAll(); };
    // Sector filter
    const fSec = document.getElementById("flt-sec");
    if (fSec)
        fSec.onchange = () => { secFilter = fSec.value; currentPage = 1; applyFilters(); renderRows(); };
    // Consensus filter
    const fCon = document.getElementById("flt-con");
    if (fCon)
        fCon.onchange = () => { conFilter = fCon.value; currentPage = 1; applyFilters(); renderRows(); };
    // Market cap filter (Pro only — disabled for free tier)
    const fMcap = document.getElementById("flt-mcap");
    if (fMcap)
        fMcap.onchange = () => { minMarketCap = parseFloat(fMcap.value); currentPage = 1; applyFilters(); renderRows(); };
    // Analyst count filter
    const fAnalysts = document.getElementById("flt-analysts");
    if (fAnalysts)
        fAnalysts.onchange = () => { minAnalysts = parseInt(fAnalysts.value); currentPage = 1; applyFilters(); renderRows(); };
    // P/E filter
    const fPE = document.getElementById("flt-pe");
    if (fPE)
        fPE.onchange = () => { maxPE = parseFloat(fPE.value); currentPage = 1; applyFilters(); renderRows(); };
    // PEG filter
    const fPEG = document.getElementById("flt-peg");
    if (fPEG)
        fPEG.onchange = () => { maxPEG = parseFloat(fPEG.value); currentPage = 1; applyFilters(); renderRows(); };
    // Momentum filter
    const fMom = document.getElementById("flt-momentum");
    if (fMom)
        fMom.onchange = () => { momentumFilter = fMom.value; currentPage = 1; applyFilters(); renderRows(); };
    // Log Out button (Pro users)
    const btnLogout = document.getElementById("btn-logout");
    if (btnLogout)
        btnLogout.onclick = async () => {
            try {
                await fetch(API + "/logout", {
                    method: "POST", headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ token: proToken })
                });
            }
            catch {
                // Even if the request fails, clear local state so this device
                // stops sending the token.
            }
            proToken = "";
            localStorage.removeItem("su_token");
            window.location.href = "/";
        };
    // Log In button — sends a one-time login link by email (no token is
    // ever returned directly from this endpoint; see /api/get-token).
    const btnLogin = document.getElementById("btn-login");
    if (btnLogin)
        btnLogin.onclick = async () => {
            const email = prompt("Enter the email you subscribed with. We'll send you a login link:");
            if (!email || !email.includes("@"))
                return;
            btnLogin.textContent = "Sending…";
            try {
                const r = await fetch(API + "/get-token", {
                    method: "POST", headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ email })
                });
                const d = await r.json();
                if (d.success) {
                    toast("If that email has a Pro subscription, a login link is on its way.", "ok");
                }
                else {
                    toast(d.error || "Something went wrong", "err");
                }
            }
            catch {
                toast("Could not connect", "err");
            }
            finally {
                btnLogin.textContent = "Log In";
            }
        };
    // Row clicks
    bindRows();
    // Pagination
    bindPagination();
    // Ticker
    startTicker();
}
// ── Boot ───────────────────────────────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", load);
