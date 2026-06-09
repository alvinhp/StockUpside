"use strict";
// ─── StockUpside.io ── main.ts ─────────────────────────────────────────────────
// Bloomberg-terminal-inspired financial data dashboard
// Compiled with: tsc  (outputs to /public/main.js)
const API = "/api";
// ── State ──────────────────────────────────────────────────────────────────────
let all = [];
let filtered = [];
let stats = null;
let tier = "free";
let proToken = localStorage.getItem("su_token") || "";
let sortKey = "rank";
let sortAsc = true;
let secFilter = "All";
let conFilter = "All";
let minAnalysts = 0;
let query = "";
let detail = null;
let tickTimer = null;
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
            fetch(`${API}/stocks?tier=${tier}`),
            fetch(`${API}/stats`)
        ]);
        const sd = await sr.json();
        const st = await str.json();
        all = sd.stocks;
        tier = sd.tier;
        stats = st;
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
async function doSubscribe(email) {
    const r = await fetch(API + "/subscribe", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ email, plan: "pro" }) });
    const d = await r.json();
    if (d.success && d.token) {
        proToken = d.token;
        localStorage.setItem("su_token", proToken);
        tier = "pro";
        closeModal("pw");
        toast("🎉 Pro unlocked! (Demo mode — no payment taken)", "ok");
        await load();
    }
    else {
        toast(d.error || "Subscription failed", "err");
    }
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
    applyFilters();
    renderRows();
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
    <div class="err-msg">Could not connect to server.<br><small>${e}</small></div>
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
    ${statsBar()}
    ${controls(sectors, countShow)}
    <div class="tbl-wrap">${table()}</div>
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
      <a href="/stocks" class="hdr-link">All Stocks</a>
      <div class="live-chip"><span class="live-dot"></span>LIVE</div>
      <div class="refresh-chip">
        <span class="rc-lbl">UPDATES IN</span>
        <span id="cd" class="cd">--:--:--</span>
      </div>
      ${tier === "pro"
        ? `<div class="pro-chip">✦ PRO</div>`
        : `<button class="btn-pro" id="btn-paywall">Unlock Pro →</button>`}
    </div>
  </header>`;
}
function banner() {
    return `<div class="banner">
    <div class="banner-l">🔒 <strong>Viewing 10 of 100 stocks.</strong>
      Upgrade to reveal all analyst picks ranked by upside.</div>
    <button class="btn-upg" id="btn-banner">Upgrade — $29/mo →</button>
  </div>`;
}
function generatingBanner() {
    if (!stats?.generating)
        return "";
    return `<div class="gen-banner">
        <span class="gen-spinner">⟳</span>
        Data is refreshing in the background — current prices may be up to 24h old.
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
            Free — no credit card required.
        </div>
        <div class="email-bar-r">
            <input type="email" id="free-email-input" class="free-email-input"
                placeholder="your@email.com" />
            <button class="btn-free-sub" id="free-email-btn">Notify Me →</button>
        </div>
    </div>`;
}
function controls(sectors, count) {
    const cons = ["All", "Strong Buy", "Buy", "Hold", "Underperform"];
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
          ${sectors.map(s => `<option value="${s}"${secFilter === s ? " selected" : ""}>${s}</option>`).join("")}
        </select>
      </div>
      <div class="flt-g"><label class="flt-lbl">CONSENSUS</label>
        <select class="flt-sel" id="flt-con">
          ${cons.map(c => `<option value="${c}"${conFilter === c ? " selected" : ""}>${c}</option>`).join("")}
        </select>
      </div>
      <div class="flt-g"><label class="flt-lbl">MIN ANALYSTS</label>
        <select class="flt-sel" id="flt-analysts">
          <option value="0"${minAnalysts === 0 ? " selected" : ""}>Any</option>
          <option value="2"${minAnalysts === 2 ? " selected" : ""}>2+</option>
          <option value="5"${minAnalysts === 5 ? " selected" : ""}>5+</option>
          <option value="10"${minAnalysts === 10 ? " selected" : ""}>10+</option>
          <option value="15"${minAnalysts === 15 ? " selected" : ""}>15+</option>
          <option value="25"${minAnalysts === 25 ? " selected" : ""}>25+</option>
        </select>
      </div>
      <div class="res-cnt">Showing <strong id="cnt">${count}</strong> stocks</div>
    </div>
  </div>`;
}
const COLS = [
    { k: "rank", l: "#", sort: true },
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
function momentumNote() {
    const allNeutral = all.length > 0 && all.every(s => !s.locked && s.momentum_trend === "neutral");
    if (!allNeutral)
        return "";
    return `<div class="momentum-note">
        ⓘ Momentum data is still building up — it populates automatically
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
    return filtered.map(s => row(s)).join("");
}
function row(s) {
    if (s.locked)
        return `
    <tr class="tr-locked" data-locked="1">
      <td class="td-rank">${s.rank}</td>
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
    </tr>`;
    const medal = s.rank === 1 ? "🥇" : s.rank === 2 ? "🥈" : s.rank === 3 ? "🥉" : "";
    const ytdCls = s.ytd_change >= 0 ? "pos" : "neg";
    return `
    <tr class="tr-stock" data-ticker="${s.ticker}">
      <td class="td-rank">${medal || s.rank}</td>
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
    // Re-bind row clicks
    bindRows();
}
function footer() {
    return `<footer class="ftr">
    <nav class="ftr-mobile-nav">
      <a href="/changes" class="ftr-nav-link">Rating Changes</a>
      <a href="/accuracy" class="ftr-nav-link">Accuracy</a>
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
        <p>All 100 analyst-ranked stocks, updated every day at midnight.</p>
      </div>
      <div class="plans">
        <div class="plan featured">
          <div class="plan-badge">MOST POPULAR</div>
          <div class="plan-name">Pro Monthly</div>
          <div class="plan-price">$29<span>/mo</span></div>
          <ul>
            <li>✓ Full top-100 ranked list</li>
            <li>✓ Consensus breakdown per stock</li>
            <li>✓ Bull / bear price target range</li>
            <li>✓ Sector + consensus filters</li>
            <li>✓ Daily data refresh</li>
            <li>✓ CSV export</li>
          </ul>
          <input type="email" id="pw-email" class="pw-email" placeholder="your@email.com" />
          <button class="btn-sub" id="pw-sub">Get Pro Access →</button>
          <div class="pw-demo">💡 Demo mode — no real payment taken</div>
        </div>
        <div class="plan">
          <div class="plan-name">Pro Annual</div>
          <div class="plan-price">$199<span>/yr</span></div>
          <div class="plan-save">Save $149 vs monthly</div>
          <ul>
            <li>✓ Everything in Monthly</li>
            <li>✓ 90-day historical data</li>
            <li>✓ Email alerts</li>
            <li>✓ REST API access</li>
          </ul>
          <button class="btn-sub btn-sub-sec" id="pw-ann">Get Annual →</button>
        </div>
      </div>
      <div class="pw-foot">🔒 Stripe · Cancel anytime · 7-day money-back</div>
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
        let offset = 0;
        const hdr = document.querySelector(".hdr");
        const ban = document.querySelector(".banner");
        const sbar = document.querySelector(".sbar");
        const ctrl = document.querySelector(".ctrl");
        if (hdr)
            offset += hdr.offsetHeight;
        if (ban)
            offset += ban.offsetHeight;
        if (sbar)
            offset += sbar.offsetHeight;
        // Set controls top (it stacks after sbar)
        if (ctrl) {
            ctrl.style.top = `${offset}px`;
            offset += ctrl.offsetHeight;
        }
        // Set the wrapper height so the table fills remaining viewport
        const wrap = document.querySelector(".tbl-wrap");
        if (wrap)
            wrap.style.height = `calc(100vh - ${offset}px)`;
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
function bindRows() {
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
            showPW();
        };
        tr.style.cursor = "pointer";
    });
}
function bindGlobals() {
    // Paywall buttons
    const bPW = document.getElementById("btn-paywall");
    if (bPW)
        bPW.onclick = showPW;
    const bBn = document.getElementById("btn-banner");
    if (bBn)
        bBn.onclick = showPW;
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
            await doSubscribe(em);
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
            await doSubscribe(em);
        };
    // Sort
    document.querySelectorAll(".th.sort").forEach(th => {
        th.onclick = () => { const k = th.dataset.k; if (k)
            doSort(k); };
    });
    // Search
    const srch = document.getElementById("srch");
    if (srch)
        srch.oninput = () => { query = srch.value; applyFilters(); renderRows(); };
    const srchClr = document.getElementById("srch-clr");
    if (srchClr)
        srchClr.onclick = () => { query = ""; if (srch)
            srch.value = ""; applyFilters(); renderAll(); };
    // Sector filter
    const fSec = document.getElementById("flt-sec");
    if (fSec)
        fSec.onchange = () => { secFilter = fSec.value; applyFilters(); renderRows(); };
    // Consensus filter
    const fCon = document.getElementById("flt-con");
    if (fCon)
        fCon.onchange = () => { conFilter = fCon.value; applyFilters(); renderRows(); };
    // Analyst count filter
    const fAnalysts = document.getElementById("flt-analysts");
    if (fAnalysts)
        fAnalysts.onchange = () => { minAnalysts = parseInt(fAnalysts.value); applyFilters(); renderRows(); };
    // Row clicks
    bindRows();
    // Ticker
    startTicker();
}
// ── Boot ───────────────────────────────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", load);
