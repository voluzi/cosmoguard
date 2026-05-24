// Uses RELATIVE URLs so the same HTML works whether mounted at
// "/" (standalone dashboard listener) or "/admin/" (legacy
// co-mount on the metrics port).
const $ = s => document.querySelector(s);
const $$ = s => document.querySelectorAll(s);

function escapeHTML(s) {
  return String(s).replace(/[&<>"']/g,
    c => ({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[c]));
}
function fmt(v) {
  if (v == null || v === "") return '<span class="empty">—</span>';
  if (typeof v === "boolean") return v ? "yes" : "no";
  if (typeof v === "object") return `<code>${escapeHTML(JSON.stringify(v))}</code>`;
  return escapeHTML(String(v));
}
function badge(kind, label) {
  return `<span class="badge ${kind}"><span class="dot"></span>${escapeHTML(label)}</span>`;
}
function humanSecs(s) {
  if (s == null) return "?";
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s/60)}m ${s%60}s`;
  if (s < 86400) return `${Math.floor(s/3600)}h ${Math.floor((s%3600)/60)}m`;
  return `${Math.floor(s/86400)}d ${Math.floor((s%86400)/3600)}h`;
}

// charts.js publishes its helpers on window.dashCharts. We grab a
// reference once at script load — the file is loaded before app.js
// via <script> ordering in index.html, so this is always populated.
const charts = window.dashCharts;
// Single shared MetricsBuffer for every chart on every tab.
const mbuf = new charts.MetricsBuffer({ maxSamples: 60 });

// scopeState — "local" (this pod) or "cluster" (fan-out across every
// olric peer). Driven from /api/v1/info.cluster.enabled; no operator
// toggle. Seeded "local" so the first hydrate hits the right base
// before /info arrives.
let scopeState = "local";
function apiBase() {
  return scopeState === "cluster" ? "./api/v1/cluster" : "./api/v1";
}
// Peer roster from /api/v1/info (or unwrapped from a cluster envelope),
// consumed by the Cluster tab.
let lastPeers = [];

// Tab switching
function activateTab(name) {
  $$(".nav-link").forEach(a => a.classList.toggle("active", a.dataset.tab === name));
  $$("main section").forEach(s => s.classList.toggle("active", s.id === "tab-" + name));
  $("#title").textContent = name.charAt(0).toUpperCase() + name.slice(1);
  try { localStorage.setItem("cg.tab", name); } catch (_) {}
}
$$(".nav-link").forEach(a => {
  a.addEventListener("click", e => { e.preventDefault(); activateTab(a.dataset.tab); });
});
try {
  const saved = localStorage.getItem("cg.tab");
  if (saved) activateTab(saved);
} catch (_) {}

// API paths are relative so this HTML works at any mount prefix.
async function fetchJSON(rel) {
  const r = await fetch(rel, { credentials: "same-origin" });
  if (!r.ok) throw new Error(`${rel}: HTTP ${r.status}`);
  return r.json();
}

// unwrapEnvelope(payload) — cluster endpoints return {data, peers}; the
// local endpoints they correspond to return data directly. We detect
// structurally rather than by reading scopeState so a refresh in-flight
// during an info-driven scope flip doesn't mis-interpret its still-
// arriving cluster payload as a local one (or vice-versa). No local
// endpoint emits a top-level shape with both `data` and `peers`, so
// the structural check has no false positives.
function unwrapEnvelope(payload) {
  if (!payload || typeof payload !== "object") return payload;
  if (Object.prototype.hasOwnProperty.call(payload, "data") &&
      Object.prototype.hasOwnProperty.call(payload, "peers")) {
    if (Array.isArray(payload.peers)) lastPeers = payload.peers;
    return payload.data;
  }
  return payload;
}

let lastOK = 0;
// safeFetch wraps fetchJSON so a single failing endpoint degrades to
// `fallback` instead of failing the whole Promise.all batch and
// blanking every panel. Failures are collected for the topbar to
// surface "N endpoints unreachable" without nuking the rest of the view.
const fetchFailures = [];
async function safeFetch(rel, fallback) {
  try {
    return await fetchJSON(rel);
  } catch (e) {
    fetchFailures.push({ rel, error: e && e.message ? e.message : String(e) });
    return fallback;
  }
}
async function refresh() {
  fetchFailures.length = 0;
  try {
    // Resolve scope first so the scope-dependent batch below hits the
    // right endpoint base. /info uses fetchJSON (not safeFetch): when
    // even the most basic local endpoint is unreachable, the pod is
    // genuinely down and the full error banner is the right signal.
    const info = await fetchJSON("./api/v1/info");
    updateScopeFromInfo(info);
    // /upstreams, /rules, /identities stay local: they describe this
    // pod's process state, and the cluster aggregator doesn't mount
    // them (calling them under cluster scope would 404 the batch).
    const base = apiBase();
    const [upstreams, rules, identities, metrics,
           unmatched, denied, cardinality, reload, discovery,
           peers, recent, websocket] = await Promise.all([
      safeFetch("./api/v1/upstreams",  { upstreams: [] }),
      safeFetch("./api/v1/rules",      { rules: [] }),
      safeFetch("./api/v1/identities", { identities: [] }),
      // Metrics fallback is null: mbuf.push() already null-guards on
      // a missing timestamp_ms (charts.js MetricsBuffer.push), so a
      // null here is a silent no-op and the chart series stays on its
      // previous tail until the next successful poll.
      safeFetch(base + "/metrics",     null),
      // Diagnostics endpoints — same gate, same shape contract. Each
      // falls back to the matching empty-container shape its renderer
      // expects, so the panel keeps its prior contents (or shows "no
      // data") instead of joining the rest of the dashboard in a
      // blanket error state.
      safeFetch(base + "/unmatched",         { sections: {} }),
      safeFetch(base + "/denied",            { denied: [] }),
      safeFetch(base + "/cache-cardinality", { sections: {} }),
      // Reload fallback uses `{}` so normalizeReloadPayload's "no
      // reloads yet" branch lights up — preferable to an array form
      // that would mislead renderClusterReload into rendering zero
      // rows as a positive empty state.
      safeFetch(base + "/reload-status",     { reload: {} }),
      safeFetch(base + "/discovery-log",     { events: [] }),
      // Peers endpoint is only meaningful in cluster mode; we still
      // include it in the same Promise.all to keep the latency budget
      // capped at the slowest leg. Returns an empty list when the
      // cluster runtime isn't networked.
      scopeState === "cluster"
        ? safeFetch("./api/v1/cluster/peers", { peers: [] })
        : Promise.resolve({ peers: [] }),
      safeFetch(base + "/requests/recent",   { requests: [], enabled: false }),
      safeFetch(base + "/websocket",         { sections: [], enabled: false }),
    ]);
    lastOK = Date.now();
    if (fetchFailures.length === 0) {
      $("#error").classList.remove("show");
    } else {
      // Partial degradation: keep rendering what we got and surface a
      // discreet count + the first failed endpoint so the operator can
      // start at the right place. Full error banner is reserved for
      // the "/info itself failed" path below — that's the signal that
      // the whole pod is unreachable, distinct from a flaky single
      // endpoint.
      showPartialError(fetchFailures);
    }
    // /metrics, /unmatched, /denied, etc. are envelope-wrapped under
    // cluster scope. /rules and /identities are always local so they
    // skip the unwrap.
    const metricsRaw      = unwrapEnvelope(metrics);
    const rulesRaw        = rules;
    const identitiesRaw   = identities;
    const unmatchedRaw    = unwrapEnvelope(unmatched);
    const deniedRaw       = unwrapEnvelope(denied);
    const cardinalityRaw  = unwrapEnvelope(cardinality);
    const reloadRaw       = unwrapEnvelope(reload);
    const discoveryRaw    = unwrapEnvelope(discovery);
    mbuf.push(metricsRaw);
    renderHeader(info);
    renderReloadPill(reloadRaw);
    renderOverview(info, upstreams);
    renderTraffic();
    renderUpstreams(upstreams);
    renderRules(rulesRaw);
    renderIdentities(identitiesRaw);
    renderDiagnostics({ unmatched: unmatchedRaw, denied: deniedRaw, cardinality: cardinalityRaw, reload: reloadRaw, discovery: discoveryRaw });
    renderLiveTraffic(unwrapEnvelope(recent));
    renderWebSockets(unwrapEnvelope(websocket));
    if (scopeState === "cluster") {
      renderClusterPeers(peers && peers.peers);
      renderClusterReload(reloadRaw);
    }
  } catch (e) {
    showError(e.message);
  }
}

// updateScopeFromInfo sets scope from /info.cluster.enabled and
// reveals/hides the Cluster nav + topbar badge. On a runtime
// transition (hot-reload that toggled cluster mode) it also resets
// + rehydrates mbuf so the next poll has a coherent baseline.
// silent=true suppresses the rehydrate side effect — used by the
// boot path where the caller drives hydrate explicitly.
function updateScopeFromInfo(info, silent) {
  const cluster = (info && info.cluster) || {};
  const enabled = !!cluster.enabled;
  const count = cluster.peer_count | 0;

  const badge = $("#scope-badge");
  const navCluster = $("#nav-cluster");
  if (badge) badge.style.display = enabled ? "" : "none";
  if (navCluster) navCluster.style.display = enabled ? "" : "none";
  const countEl = $("#scope-badge-count");
  if (countEl) countEl.textContent = enabled && count > 0 ? `(${count})` : "";

  const want = enabled ? "cluster" : "local";
  if (want !== scopeState) {
    scopeState = want;
    if (!silent) {
      mbuf.reset && mbuf.reset();
      hydrateHistory().finally(() => { refresh().catch(() => {}); });
    }
  }
}

function showError(msg) {
  $("#error").textContent = "fetch error: " + msg;
  $("#error").classList.add("show");
  $("#status-text").textContent = "disconnected";
  $("#status-dot").classList.add("bad");
}

// showPartialError(failures) — at least one endpoint failed but /info
// (the liveness signal) succeeded, so the pod is up and the rest of
// the dashboard rendered fine. Show a softer one-line summary instead
// of the full "disconnected" banner — the connection dot stays green
// because we DO have a working channel to the pod. failures is
// [{rel, error}] from safeFetch; we name the first one so the
// operator has a concrete starting point and tally the rest.
function showPartialError(failures) {
  const first = failures[0];
  const extra = failures.length > 1 ? ` (+${failures.length - 1} more)` : "";
  const path = first.rel.replace(/^\.\//, "");
  $("#error").textContent = `partial fetch error: ${path} — ${first.error}${extra}`;
  $("#error").classList.add("show");
  // Keep status as connected — we DID reach the pod, just not every
  // endpoint. Operators triaging from the status dot should see green
  // because the pod itself is reachable; the banner conveys the
  // per-endpoint detail.
}

function renderHeader(info) {
  $("#status-text").textContent = "connected";
  $("#status-dot").classList.remove("bad");
  $("#uptime").textContent = humanSecs(info.uptime_seconds);
  $("#ver").textContent = info.version || "dev";
}

function renderOverview(info, ups) {
  const u = info.upstreams || {};
  const healthy = u.healthy ?? 0;
  const total = u.count ?? 0;
  const allOK = total > 0 && healthy === total;

  // Headline tiles aggregate over a 60 s trailing window so they're
  // statistically meaningful on low-traffic deployments. The line
  // charts below still render the per-5s series so spikes stay visible.
  const TRAILING = 60;
  const totalRate = mbuf.meanRateTrailing("*", TRAILING);
  const avgHit = mbuf.hitRateTrailing("*", TRAILING);
  const avgErr = mbuf.errorRateTrailing("*", TRAILING);
  const p99 = mbuf.latencyQuantileTrailing("*", 0.99, TRAILING);

  // Spark series for each tile — total-rate spark uses totalRate;
  // hit/err/p99 use the averaged-across-protocols series.
  const sparkTotal = mbuf.totalRate("*");
  const sparkHit = avgSeriesAcrossProtocols(p => mbuf.hitRate(p));
  const sparkErr = avgSeriesAcrossProtocols(p => mbuf.errorRate(p));
  const sparkP99 = avgSeriesAcrossProtocols(p => mbuf.latencyQuantile(p, 0.99));

  $("#overview-stats").innerHTML = [
    statTileSpark("req/s", charts.fmtRate(totalRate || 0), "req-spark", "across every protocol"),
    statTileSpark("cache hit", charts.fmtPercent(avgHit), "hit-spark", "avg across protocols"),
    statTileSpark("error rate", charts.fmtPercent(avgErr || 0), "err-spark",
                  (avgErr || 0) > 0.05 ? "elevated" : "nominal"),
    statTileSpark("p99 latency", charts.fmtSeconds(p99 || 0), "p99-spark", "across protocols"),
    statTile("upstreams", `${healthy} / ${total}`, allOK ? "ok" : (healthy === 0 ? "bad" : ""),
             allOK ? "all healthy" : (total === 0 ? "no upstreams" : `${total - healthy} unhealthy`)),
    statTile("uptime", humanSecs(info.uptime_seconds), "", info.started_at || ""),
    statTile("version", info.version || "dev", "", info.commit ? `commit ${(info.commit || '').slice(0,12)}` : ""),
    statTile("rules", String(((window.__lastRules || {}).rules || []).length || "—"), "", "compiled & sorted"),
  ].join("");

  // Draw the sparklines AFTER innerHTML replaces the DOM nodes.
  drawSparkSafe("req-spark", sparkTotal, "var(--accent)");
  drawSparkSafe("hit-spark", sparkHit, "var(--ok)");
  drawSparkSafe("err-spark", sparkErr, "var(--bad)");
  drawSparkSafe("p99-spark", sparkP99, "var(--warn)");

  const tbody = $("#overview-upstreams tbody");
  const rows = (ups.upstreams || []).slice(0, 8).map(u => upstreamRow(u, /*short=*/true));
  tbody.innerHTML = rows.length ? rows.join("") : `<tr><td colspan="4" class="empty">no upstreams</td></tr>`;
  const more = (ups.upstreams || []).length - 8;
  $("#upstream-hint").textContent = more > 0 ? `showing 8 of ${(ups.upstreams||[]).length}` : "live state across LCD/RPC pools";
}

function statTile(label, value, modifier, sub) {
  const cls = "stat" + (modifier ? " " + modifier : "");
  return `<div class="${cls}">
    <div class="label">${escapeHTML(label)}</div>
    <div class="value">${escapeHTML(String(value))}</div>
    ${sub ? `<div class="sub">${escapeHTML(sub)}</div>` : ""}
  </div>`;
}

// statTileSpark — variant of statTile with a small SVG sparkline next
// to the number. sparkId is the id of the <svg> the caller will draw
// into after the innerHTML swap (drawSparkSafe handles the lookup).
function statTileSpark(label, value, sparkId, sub) {
  return `<div class="stat stat-spark">
    <div class="label">${escapeHTML(label)}</div>
    <div class="value-row">
      <div class="value">${escapeHTML(String(value))}</div>
      <svg id="${sparkId}" class="spark" viewBox="0 0 120 32" preserveAspectRatio="none" aria-hidden="true"></svg>
    </div>
    ${sub ? `<div class="sub">${escapeHTML(sub)}</div>` : ""}
  </div>`;
}

function drawSparkSafe(id, series, color) {
  const el = document.getElementById(id);
  if (!el) return;
  charts.sparkline(el, series, { color });
}

// lastValue(series) → the last sample's v, or 0 when empty.
function lastValue(series) {
  return series && series.length > 0 ? series[series.length - 1].v : 0;
}

// avgRateAcrossProtocols(fn) — for snapshot-style helpers that return
// a single scalar per protocol (e.g. hitRateSnapshot). Returns the
// arithmetic mean, ignoring protocols with no traffic.
function avgRateAcrossProtocols(fn) {
  const protos = mbuf.protocolList();
  if (protos.length === 0) return 0;
  let sum = 0, n = 0;
  for (const p of protos) {
    const v = fn(p);
    if (isFinite(v) && v > 0) { sum += v; n++; }
  }
  return n > 0 ? sum / n : 0;
}

// avgSeriesAcrossProtocols(fn) — for helpers that return a [{ts,v},…]
// series. Averages by aligning on shared timestamps; falls back to
// whichever protocol has the most samples when alignment is sparse.
function avgSeriesAcrossProtocols(fn) {
  const protos = mbuf.protocolList();
  if (protos.length === 0) return [];
  const byTs = new Map();
  for (const p of protos) {
    for (const pt of fn(p) || []) {
      if (!byTs.has(pt.ts)) byTs.set(pt.ts, []);
      byTs.get(pt.ts).push(pt.v);
    }
  }
  const out = [];
  for (const [ts, arr] of [...byTs.entries()].sort((a, b) => a[0] - b[0])) {
    const sum = arr.reduce((s, v) => s + (isFinite(v) ? v : 0), 0);
    out.push({ ts, v: arr.length > 0 ? sum / arr.length : 0 });
  }
  return out;
}

function upstreamRow(u, short) {
  const health = u.healthy
    ? badge("ok", "healthy")
    : badge("bad", "unhealthy");
  const circuit = u.circuit_open
    ? badge("bad", "open")
    : badge("muted", "closed");
  if (short) {
    return `<tr>
      <td><code>${fmt(u.name)}</code></td>
      <td><code>${fmt(u.target)}</code></td>
      <td>${health}</td>
      <td>${circuit}</td>
    </tr>`;
  }
  return `<tr>
    <td><code>${fmt(u.name)}</code></td>
    <td><code>${fmt(u.target)}</code></td>
    <td>${fmt(u.weight)}</td>
    <td>${health}</td>
    <td>${circuit}</td>
  </tr>`;
}

function renderTraffic() {
  // Per-protocol request rate — one series per protocol the buffer
  // has observed traffic from.
  const protos = mbuf.protocolList();
  const rateSeries = protos.map((p, i) => ({
    label: p,
    color: charts.SERIES_PALETTE[i % charts.SERIES_PALETTE.length],
    points: mbuf.rate(p),
  }));
  charts.lineChart(document.getElementById("chart-rate"), {
    series: rateSeries,
    yFormat: charts.fmtRate,
  });

  // Cache outcomes — pick the FIRST protocol with any cache labels
  // for the stacked chart. Showing every protocol stacked would
  // double-count traffic that's already in the rate chart above; the
  // panel hint pins it to the "primary" protocol the operator's
  // looking at.
  let cacheProto = null;
  for (const p of protos) {
    const cb = mbuf.cacheBreakdown(p);
    if ((cb.hit.length + cb.miss.length + cb.error.length + cb.na.length) > 0) {
      cacheProto = p;
      break;
    }
  }
  if (cacheProto) {
    const cb = mbuf.cacheBreakdown(cacheProto);
    // Stacked needs aligned timestamps — use the longest series as
    // the truth and align the others.
    const aligned = alignSeriesByTs([cb.hit, cb.miss, cb.error, cb.na]);
    charts.stackedAreaChart(document.getElementById("chart-cache"), {
      series: [
        { label: `${cacheProto} hit`, color: "var(--ok)", points: aligned[0] },
        { label: `${cacheProto} miss`, color: "var(--muted-soft)", points: aligned[1] },
        { label: `${cacheProto} error`, color: "var(--bad)", points: aligned[2] },
        { label: `${cacheProto} n/a`, color: "var(--warn)", points: aligned[3] },
      ],
    });
  } else {
    charts.stackedAreaChart(document.getElementById("chart-cache"), { series: [] });
  }

  // Latency percentiles — average across protocols so a global p99
  // is meaningful.
  const p50 = avgSeriesAcrossProtocols(p => mbuf.latencyQuantile(p, 0.50));
  const p95 = avgSeriesAcrossProtocols(p => mbuf.latencyQuantile(p, 0.95));
  const p99 = avgSeriesAcrossProtocols(p => mbuf.latencyQuantile(p, 0.99));
  charts.lineChart(document.getElementById("chart-latency"), {
    series: [
      { label: "p50", color: "var(--accent)", points: p50 },
      { label: "p95", color: "var(--warn)", points: p95 },
      { label: "p99", color: "var(--bad)", points: p99 },
    ],
    yFormat: charts.fmtSeconds,
  });

  // Upstream health timeline — one strip per pool/upstream pair.
  const health = mbuf.upstreamHealth();
  const stripsHost = document.getElementById("timeline-strips");
  const keys = Object.keys(health).sort();
  if (keys.length === 0) {
    stripsHost.innerHTML = `<div class="empty">no upstreams reporting yet</div>`;
  } else {
    stripsHost.innerHTML = keys.map(k =>
      `<div class="timeline-row">
         <div class="timeline-label"><code>${escapeHTML(k)}</code></div>
         <svg id="ts-${cssId(k)}" class="timeline" viewBox="0 0 240 16" preserveAspectRatio="none"></svg>
       </div>`).join("");
    for (const k of keys) {
      charts.timelineStrip(document.getElementById(`ts-${cssId(k)}`), health[k]);
    }
  }
}

// alignSeriesByTs(arr) — given N point-series sharing the same poll
// cadence, returns N series of equal length aligned on the union of
// timestamps. Missing samples become {ts, v:0}. The stacked chart
// requires this alignment because layers are summed positionally.
function alignSeriesByTs(seriesArr) {
  const ts = new Set();
  for (const s of seriesArr) for (const p of s) ts.add(p.ts);
  const tsSorted = [...ts].sort((a, b) => a - b);
  return seriesArr.map(s => {
    const m = new Map(s.map(p => [p.ts, p.v]));
    return tsSorted.map(t => ({ ts: t, v: m.get(t) || 0 }));
  });
}

// cssId(s) — turn a pool/upstream key into a safe SVG id by
// replacing every non-id character with a dash. SVG ids are
// case-sensitive so we keep the original case.
function cssId(s) {
  return String(s).replace(/[^a-zA-Z0-9_-]/g, "-");
}

function renderUpstreams(d) {
  const rows = (d.upstreams || []).map(u => upstreamRow(u, false));
  $("#upstreams-table tbody").innerHTML =
    rows.length ? rows.join("") : `<tr><td colspan="5" class="empty">no upstreams</td></tr>`;
}

function renderRules(d) {
  window.__lastRules = d;
  // Per-rule metric annotations. perRuleDelta gives requests in the
  // last 5 minutes (capped by buffer age); perRuleHitRate gives the
  // owning protocol's hit-rate weighted to this rule. Default to —
  // for rules that haven't seen any traffic since the dashboard
  // booted, so a fresh-deploy operator sees the table populated
  // without misleading 0% noise everywhere.
  const reqDelta = mbuf.perRuleDelta(300);
  const hitRate = mbuf.perRuleHitRate();
  const rows = (d.rules || []).map(r => {
    const rid = r.rule_id || "";
    const reqs = reqDelta[rid];
    const hit = hitRate[rid];
    const reqCell = (typeof reqs === "number" && reqs > 0)
      ? `<code>${charts.fmtNumber(reqs)}</code>`
      : '<span class="empty">—</span>';
    // Cache hit-rate only makes sense for rules with cache enabled
    // — otherwise every observation lands as cache=n/a and "hit
    // rate" is misleading. Suppress the column for non-cache rules.
    const hitCell = (r.cache && typeof hit === "number")
      ? `<code>${charts.fmtPercent(hit)}</code>`
      : '<span class="empty">—</span>';
    return `
    <tr>
      <td><code>${fmt(r.section)}</code></td>
      <td>${fmt(r.priority)}</td>
      <td>${r.action === "allow" ? badge("ok", "allow") : badge("bad", "deny")}</td>
      <td class="summary-cell">${fmt(r.match_summary)}</td>
      <td>${r.cache ? `<code>${escapeHTML(r.cache)}</code>` : '<span class="empty">—</span>'}</td>
      <td>${r.rate_limit ? `<code>${escapeHTML(r.rate_limit)}</code>` : '<span class="empty">—</span>'}</td>
      <td>${r.auth ? `<code>${escapeHTML(r.auth)}</code>` : '<span class="empty">—</span>'}</td>
      <td class="num-cell">${reqCell}</td>
      <td class="num-cell">${hitCell}</td>
    </tr>`;
  });
  $("#rules-table tbody").innerHTML =
    rows.length ? rows.join("") : `<tr><td colspan="9" class="empty">no rules — default action applies</td></tr>`;
  // Refresh overview tile count (if overview is currently visible it will redraw).
}

function renderIdentities(d) {
  const rows = (d.identities || []).map(i => `
    <tr>
      <td><code>${fmt(i.name)}</code></td>
      <td>${fmt((i.scopes || []).join(", "))}</td>
    </tr>`);
  $("#identities-table tbody").innerHTML =
    rows.length ? rows.join("") : `<tr><td colspan="2" class="empty">no identities — auth disabled or anonymous-only</td></tr>`;
}

function renderLiveTraffic(payload) {
  const hint = $("#live-traffic-hint");
  const tbody = $("#live-traffic-table tbody");
  if (!payload || !payload.enabled) {
    if (hint) hint.textContent = "off — enable via dashboard.requestLog.enable: true";
    tbody.innerHTML = `<tr><td colspan="11" class="empty">request log is disabled</td></tr>`;
    return;
  }
  const rows = (payload.requests || []).map(e => {
    const t = e.timestamp_ms ? new Date(e.timestamp_ms).toISOString().slice(11, 23) : "—";
    const status = e.status || 0;
    const statusCls = status >= 500 ? "bad-text" : status >= 400 ? "warn-text" : status >= 200 ? "ok-text" : "";
    const lat = e.latency_ms != null ? `${e.latency_ms}ms` : "—";
    const path = e.path + (e.query ? `?${e.query}` : "");
    return `<tr>
      <td class="mono">${fmt(t)}</td>
      <td>${fmt(e.section)}</td>
      <td><code>${fmt(e.method)}</code></td>
      <td class="mono">${fmt(path)}</td>
      <td class="${statusCls} mono">${status || "—"}</td>
      <td class="mono">${lat}</td>
      <td>${fmt(e.cache_state)}</td>
      <td>${fmt(e.rule_tag)}</td>
      <td>${fmt(e.identity)}</td>
      <td class="mono">${fmt(e.source_ip)}</td>
      <td>${fmt(e.upstream)}</td>
    </tr>`;
  });
  if (hint) hint.textContent = `${rows.length} most-recent across enabled sections`;
  tbody.innerHTML = rows.length ? rows.join("") : `<tr><td colspan="11" class="empty">no requests captured yet</td></tr>`;
}

// renderWebSockets(payload) — the WebSockets tab. Headline stat cards
// aggregate across both WS sections (Tendermint /websocket + EVM WS);
// the three tables list per-connection, per-subscription, and per-
// upstream-connection detail. The dedup ratio (client subs ÷ upstream
// subs) is the panel's signature number: it shows how much fan-out the
// broker is saving the upstream nodes.
function renderWebSockets(payload) {
  const sections = (payload && payload.sections) || [];
  const statsHost = $("#ws-stats");
  const connsBody = $("#ws-conns-table tbody");
  const subsBody = $("#ws-subs-table tbody");
  const upsBody = $("#ws-ups-table tbody");
  // Defensive: if the WebSockets tab markup isn't present (older cached
  // bundle), bail rather than throwing on a null .innerHTML and blanking
  // the whole dashboard via refresh()'s catch.
  if (!statsHost || !connsBody || !subsBody || !upsBody) return;

  let conns = 0, clientSubs = 0, upstreamSubs = 0, upHealthy = 0, upTotal = 0;
  for (const s of sections) {
    conns += s.connections | 0;
    clientSubs += s.client_subscriptions | 0;
    upstreamSubs += s.upstream_subscriptions | 0;
    upHealthy += s.upstream_conns_healthy | 0;
    upTotal += s.upstream_conns_total | 0;
  }
  const ratio = upstreamSubs > 0 ? (clientSubs / upstreamSubs) : 0;
  const poolMod = upTotal === 0 ? "" : (upHealthy === upTotal ? "ok" : (upHealthy === 0 ? "bad" : "warn"));
  if (statsHost) {
    statsHost.innerHTML = [
      statTile("Connections", conns, "", "active client sockets"),
      statTile("Client subscriptions", clientSubs, "", "across all clients"),
      statTile("Upstream subscriptions", upstreamSubs, "",
        ratio > 0 ? `${ratio.toFixed(1)}× fan-out` : "deduplicated to upstream"),
      statTile("Upstream pool", `${upHealthy}/${upTotal}`, poolMod, "healthy backend conns"),
    ].join("");
  }

  // Connections table — newest first.
  const connRows = [];
  for (const s of sections) {
    for (const c of (s.conns || [])) {
      connRows.push({ section: s.section, c });
    }
  }
  connRows.sort((a, b) => (b.c.connected_ms || 0) - (a.c.connected_ms || 0));
  connsBody.innerHTML = connRows.length
    ? connRows.map(({ section, c }) => `<tr>
        <td>${fmt(section)}</td>
        <td class="mono">${fmt(c.source_ip)}</td>
        <td>${fmt(c.identity)}</td>
        <td class="mono" title="${c.connected_ms ? new Date(c.connected_ms).toISOString() : ""}">${c.connected_ms ? timeAgo(c.connected_ms) : "—"}</td>
        <td class="mono">${c.subscriptions | 0}</td>
      </tr>`).join("")
    : `<tr><td colspan="5" class="empty">no active connections</td></tr>`;

  // Subscriptions table — busiest first.
  const subRows = [];
  for (const s of sections) {
    for (const sub of (s.subs || [])) {
      subRows.push({ section: s.section, sub });
    }
  }
  subRows.sort((a, b) => (b.sub.subscribers || 0) - (a.sub.subscribers || 0));
  subsBody.innerHTML = subRows.length
    ? subRows.map(({ section, sub }) => `<tr>
        <td>${fmt(section)}</td>
        <td class="mono">${fmt(sub.param)}</td>
        <td class="mono">${sub.subscribers | 0}</td>
        <td class="mono">${fmt(sub.upstream)}</td>
      </tr>`).join("")
    : `<tr><td colspan="4" class="empty">no active subscriptions</td></tr>`;

  // Upstream connections table.
  const upRows = [];
  for (const s of sections) {
    for (const u of (s.upstreams || [])) {
      upRows.push({ section: s.section, u });
    }
  }
  upsBody.innerHTML = upRows.length
    ? upRows.map(({ section, u }) => `<tr>
        <td>${fmt(section)}</td>
        <td class="mono">${fmt(u.target)}</td>
        <td>${u.healthy ? badge("ok", "healthy") : badge("bad", "down")}</td>
        <td class="mono">${u.subscriptions | 0}</td>
      </tr>`).join("")
    : `<tr><td colspan="4" class="empty">no upstream connections</td></tr>`;
}

// normalizeReloadPayload(payload) — the local /reload-status returns
// `{reload: ReloadStatus}`; the cluster /reload-status returns
// `{reload: [{pod_id, reload: ReloadStatus}, …]}` because every pod
// reloads on its own schedule. To keep the existing pill + table
// renderers unaware of scope, we collapse the array form into a
// "cluster summary": pick the LATEST timestamp_ms across pods, and
// treat the whole cluster as successful only when every reported pod
// succeeded. The per-pod detail is rendered separately on the Cluster
// tab.
function normalizeReloadPayload(payload) {
  const r = (payload && payload.reload);
  if (Array.isArray(r)) {
    let latest = null;
    let allOK = true;
    let firstErr = "";
    for (const row of r) {
      const rl = row && row.reload;
      if (!rl || !rl.timestamp_ms) continue;
      if (!latest || rl.timestamp_ms > latest.timestamp_ms) latest = rl;
      if (rl.success === false) {
        allOK = false;
        if (!firstErr && rl.error) firstErr = `${row.pod_id || "?"}: ${rl.error}`;
      }
    }
    if (!latest) return { reload: {} };
    return { reload: { ...latest, success: allOK, error: firstErr || latest.error || "" } };
  }
  return payload || { reload: {} };
}

// renderReloadPill(payload) — colours the topbar pill from the most
// recent reload outcome. Green for a recent success, amber once the
// success is stale (>1h old) and the operator might no longer trust
// it, red when the last attempt errored out. The pill stays "no
// reloads yet" until the backend has stamped a first reload.
function renderReloadPill(payload) {
  const pill = $("#reload-pill");
  const text = $("#reload-pill-text");
  if (!pill || !text) return;
  payload = normalizeReloadPayload(payload);
  const r = (payload && payload.reload) || {};
  pill.classList.remove("ok", "bad", "warn", "muted");
  if (!r.timestamp_ms) {
    pill.classList.add("muted");
    text.textContent = "no reloads yet";
    pill.title = "configuration has not been hot-reloaded this run";
    return;
  }
  const ageMs = Date.now() - r.timestamp_ms;
  const ageStr = humanSecs(Math.max(0, Math.floor(ageMs / 1000))) + " ago";
  if (!r.success) {
    pill.classList.add("bad");
    text.textContent = "reload failed";
    pill.title = (r.error || "reload failed") + " · " + ageStr;
    return;
  }
  if (ageMs > 3600 * 1000) {
    pill.classList.add("warn");
    text.textContent = "reload stale";
    pill.title = "last reload " + ageStr + " — verify it still matches expectations";
    return;
  }
  pill.classList.add("ok");
  text.textContent = "reload ok · " + ageStr;
  pill.title = "last hot-reload succeeded " + ageStr;
}

// renderDiagnostics(payloads) — fan-out into the 5 panels on the
// Diagnostics tab. Each sub-renderer is nil-safe so a missing endpoint
// (older backend, transient 5xx that the gate decided to mask) just
// leaves its panel empty instead of bubbling a render error.
function renderDiagnostics(payloads) {
  renderUnmatched(payloads.unmatched);
  renderTopRules();
  renderCardinality(payloads.cardinality);
  renderUpstreamLatency();
  renderUpstreamVolume();
  renderDenied(payloads.denied);
  renderDiscovery(payloads.discovery);
  renderReloadTable(payloads.reload);
}

// renderUnmatched(payload) — per-section accordion. Each section
// header carries the row count, body is a table of (method, path,
// count, last seen). Sections with no entries collapse to a single
// "empty" stub so the operator knows the section was checked.
function renderUnmatched(payload) {
  const host = document.getElementById("unmatched-accordion");
  if (!host) return;
  const sections = (payload && payload.sections) || {};
  const keys = Object.keys(sections).sort();
  if (keys.length === 0) {
    host.innerHTML = `<div class="empty" style="padding:1rem 1.15rem;">no unmatched requests recorded yet</div>`;
    return;
  }
  // Preserve which sections were open across re-renders so the operator
  // doesn't have to re-expand a panel every 5 seconds.
  const wasOpen = new Set(
    [...host.querySelectorAll("details[open]")].map(d => d.dataset.section)
  );
  host.innerHTML = keys.map(section => {
    const rows = sections[section] || [];
    const total = rows.reduce((s, r) => s + (r.count || 0), 0);
    const openAttr = wasOpen.has(section) ? " open" : "";
    const body = rows.length === 0
      ? `<div class="empty" style="padding:0.85rem 1.15rem;">no unmatched requests</div>`
      : `<div class="table-wrap"><table class="accordion-table">
          <thead><tr><th>method</th><th>path</th><th>count</th><th>last seen</th></tr></thead>
          <tbody>${rows.slice(0, 20).map(r => `<tr>
            <td><code>${escapeHTML(r.method || "—")}</code></td>
            <td class="summary-cell">${r.path ? `<code>${escapeHTML(r.path)}</code>` : '<span class="empty">—</span>'}</td>
            <td class="num-cell"><code>${charts.fmtNumber(r.count || 0)}</code></td>
            <td>${escapeHTML(timeAgo(r.last_seen_ms))}</td>
          </tr>`).join("")}</tbody>
        </table></div>`;
    return `<details data-section="${escapeHTML(section)}"${openAttr}>
      <summary>
        <span class="acc-title"><code>${escapeHTML(section)}</code></span>
        <span class="acc-meta">${rows.length} entries · ${charts.fmtNumber(total)} hits</span>
      </summary>
      ${body}
    </details>`;
  }).join("");
}

// renderTopRules() — top-10 rules by 5-minute Δcount. Reuses the
// per-rule rate already aggregated in MetricsBuffer; the panel exists
// to answer "which rule is taking the bulk of the traffic" without
// the operator scrolling the full Rules tab.
function renderTopRules() {
  const host = document.getElementById("top-rules");
  if (!host) return;
  const delta = mbuf.perRuleDelta(300);
  const entries = Object.entries(delta).filter(([, v]) => v > 0);
  if (entries.length === 0) {
    host.innerHTML = `<div class="empty">collecting…</div>`;
    return;
  }
  entries.sort((a, b) => b[1] - a[1]);
  const top = entries.slice(0, 10).map(([rid, v]) => ({
    label: rid,
    value: v,
    sub: "in last 5m",
  }));
  charts.barListByValue(host, top, {
    valueFormat: charts.fmtNumber,
    emptyText: "no rule traffic in the last 5 minutes",
  });
}

// renderCardinality(payload) — flatten (section, rule_tag) rows into a
// single table sorted by distinct_keys desc, highlighting rows >=1000
// keys in red. Hot samples render as up to three small code chips.
function renderCardinality(payload) {
  const tbody = $("#cardinality-table tbody");
  if (!tbody) return;
  const sections = (payload && payload.sections) || {};
  const flat = [];
  for (const [section, rules] of Object.entries(sections)) {
    for (const r of (rules || [])) {
      flat.push({ section, ...r });
    }
  }
  flat.sort((a, b) => (b.distinct_keys || 0) - (a.distinct_keys || 0));
  if (flat.length === 0) {
    tbody.innerHTML = `<tr><td colspan="4" class="empty">no cacheable traffic observed yet</td></tr>`;
    return;
  }
  tbody.innerHTML = flat.map(r => {
    const hot = (r.hot_samples || []).slice(0, 3)
      .map(s => `<code>${escapeHTML(s)}</code>`).join(" ");
    const cls = (r.distinct_keys >= 1000) ? "card-hot" : "";
    return `<tr class="${cls}">
      <td><code>${escapeHTML(r.section)}</code></td>
      <td><code>${escapeHTML(r.rule_tag || "—")}</code></td>
      <td class="num-cell"><code>${charts.fmtNumber(r.distinct_keys || 0)}</code></td>
      <td class="summary-cell">${hot || '<span class="empty">—</span>'}</td>
    </tr>`;
  }).join("");
}

// renderUpstreamLatency() — small-multiples per (protocol, upstream)
// of P50 + P95 from the per-upstream histogram. Iterates over every
// protocol that exposes a by_upstream_histogram in the latest
// snapshot. Empty when no per-upstream traffic has flowed.
function renderUpstreamLatency() {
  const host = document.getElementById("upstream-latency");
  if (!host) return;
  const protos = mbuf.protocolList();
  const panels = [];
  for (const proto of protos) {
    const upstreams = mbuf.upstreamsForProto(proto);
    if (upstreams.length === 0) continue;
    const p50 = {};
    const p95 = {};
    for (const u of upstreams) {
      p50[u] = mbuf.upstreamLatencyQuantile(proto, u, 0.50);
      p95[u] = mbuf.upstreamLatencyQuantile(proto, u, 0.95);
    }
    const id50 = `upl-50-${cssId(proto)}`;
    const id95 = `upl-95-${cssId(proto)}`;
    panels.push(`<div class="upstream-latency-cell">
      <div class="upstream-latency-head"><code>${escapeHTML(proto)}</code> · P50</div>
      <svg id="${id50}" class="chart" viewBox="0 0 600 160" preserveAspectRatio="none"></svg>
    </div>
    <div class="upstream-latency-cell">
      <div class="upstream-latency-head"><code>${escapeHTML(proto)}</code> · P95</div>
      <svg id="${id95}" class="chart" viewBox="0 0 600 160" preserveAspectRatio="none"></svg>
    </div>`);
    // Defer SVG draws until after the innerHTML swap below.
    host.__pending = host.__pending || [];
    host.__pending.push({ id: id50, series: p50 });
    host.__pending.push({ id: id95, series: p95 });
  }
  if (panels.length === 0) {
    host.innerHTML = `<div class="empty" style="padding:1rem 1.15rem;">no per-upstream traffic yet</div>`;
    return;
  }
  const pending = host.__pending || [];
  host.__pending = null;
  host.innerHTML = panels.join("");
  for (const job of pending) {
    const svg = document.getElementById(job.id);
    if (!svg) continue;
    charts.multiPercentileChart(svg, { seriesByUpstream: job.series, yFormat: charts.fmtSeconds });
  }
}

// renderUpstreamVolume() — one stacked-bar row per protocol breaking
// down where each request landed over the last 5 minutes: served from
// local cache, served by upstream X, or dropped at the rule layer
// (deny / rate-limit / auth fail). Answers the operator's question
// "I sent 1000 requests against a cacheable rule — how many actually
// hit my node?" directly: cache segment width = cache_hit count,
// each upstream's segment = requests that reached it, dropped
// segment = total − cache − upstreams. Cumulative counters from
// MetricsSnapshot are diffed over 5 minutes so the bar reflects
// current-state distribution, not all-since-boot drift.
function renderUpstreamVolume() {
  const host = document.getElementById("upstream-volume");
  if (!host) return;
  const deltas = mbuf.perRequestRoutingDelta(300); // last 5 minutes
  const protos = Object.keys(deltas).sort();
  if (protos.length === 0) {
    host.innerHTML = `<div class="empty" style="padding:1rem 1.15rem;">no traffic in the last 5 min</div>`;
    return;
  }
  // Stable colour-per-upstream-name across protocols so node-a is
  // the same colour on every row.
  const allUpstreams = new Set();
  for (const proto of protos) {
    for (const u of Object.keys(deltas[proto].upstreams || {})) allUpstreams.add(u);
  }
  const upstreamOrder = Array.from(allUpstreams).sort();
  const palette = charts.SERIES_PALETTE || [];
  const colourFor = (u) => {
    const i = upstreamOrder.indexOf(u);
    return palette[i % palette.length] || "#888";
  };
  const CACHE_COLOUR   = "#9aa3af"; // neutral grey — never confused with an upstream
  const DROPPED_COLOUR = "#d9534f"; // red — denied/rate-limited surfaces as a warning slice

  const rows = protos.map(proto => {
    const r = deltas[proto];
    const ups = Object.keys(r.upstreams || {}).sort((a, b) => r.upstreams[b] - r.upstreams[a]);
    const upstreamSum = ups.reduce((acc, u) => acc + r.upstreams[u], 0);
    const dropped = Math.max(0, r.total - r.cache - upstreamSum);
    // Order: cache first, then upstreams (high → low), then dropped.
    const segs = [];
    if (r.cache > 0) {
      segs.push({ label: "cache hit", count: r.cache, colour: CACHE_COLOUR });
    }
    for (const u of ups) {
      segs.push({ label: `→ ${u}`, count: r.upstreams[u], colour: colourFor(u) });
    }
    if (dropped > 0) {
      segs.push({ label: "denied / rate-limited", count: dropped, colour: DROPPED_COLOUR });
    }
    const total = segs.reduce((a, s) => a + s.count, 0) || 1;
    const segments = segs.map(s => {
      const pct = (s.count / total) * 100;
      const title = `${s.label}: ${charts.fmtNumber(s.count)} req · ${charts.fmtPercent(s.count / total)}`;
      return `<span class="vol-seg" style="width:${pct.toFixed(2)}%;background:${s.colour};" title="${escapeHTML(title)}"></span>`;
    }).join("");
    const legend = segs.map(s => {
      return `<span class="vol-legend-item">
        <span class="vol-swatch" style="background:${s.colour};"></span>
        ${escapeHTML(s.label)}
        <span class="empty">${charts.fmtNumber(s.count)}</span>
      </span>`;
    }).join("");
    // Headline numbers: total served + the breakdown the operator cares about.
    const upstreamPct = r.total > 0 ? (upstreamSum / r.total) : 0;
    return `<div class="vol-row">
      <div class="vol-row-head">
        <code>${escapeHTML(proto)}</code>
        <span class="hint">${charts.fmtNumber(r.total)} req · ${charts.fmtNumber(upstreamSum)} to upstream (${charts.fmtPercent(upstreamPct)}) · 5m</span>
      </div>
      <div class="vol-bar">${segments}</div>
      <div class="vol-legend">${legend}</div>
    </div>`;
  }).join("");
  host.innerHTML = rows;
}

// renderDenied(payload) — newest-first table of deny records from the
// ring buffer. Reason badged by colour; path/rule/source rendered as
// code so an operator can copy-paste them into a config or grep.
function renderDenied(payload) {
  const tbody = $("#denied-table tbody");
  if (!tbody) return;
  const records = (payload && payload.denied) || [];
  if (records.length === 0) {
    tbody.innerHTML = `<tr><td colspan="7" class="empty">no denials recorded yet</td></tr>`;
    return;
  }
  tbody.innerHTML = records.map(d => {
    const reasonBadge = denyReasonBadge(d.reason);
    return `<tr>
      <td><span class="mono">${escapeHTML(fmtTimeShort(d.timestamp_ms))}</span></td>
      <td><code>${escapeHTML(d.section || "—")}</code></td>
      <td>${reasonBadge}</td>
      <td><code>${escapeHTML(d.method || "—")}</code></td>
      <td class="summary-cell">${d.path ? `<code>${escapeHTML(d.path)}</code>` : '<span class="empty">—</span>'}</td>
      <td><code>${escapeHTML(d.source_ip || "—")}</code></td>
      <td>${d.rule_tag ? `<code>${escapeHTML(d.rule_tag)}</code>` : '<span class="empty">—</span>'}</td>
    </tr>`;
  }).join("");
}

function denyReasonBadge(reason) {
  switch (reason) {
    case "rule":       return badge("bad", "rule");
    case "default":    return badge("warn", "default");
    case "rate_limit": return badge("warn", "rate-limit");
    case "auth":       return badge("bad", "auth");
    default:           return badge("muted", reason || "—");
  }
}

// renderDiscovery(payload) — newest-first table of DNS discovery
// events. Detail column collapses (resolved=[...]) for ticks and
// (upstream / ip / error) for the per-target events. Type is badged
// so add/remove/error are visually distinguishable.
function renderDiscovery(payload) {
  const tbody = $("#discovery-table tbody");
  if (!tbody) return;
  const events = (payload && payload.events) || [];
  if (events.length === 0) {
    tbody.innerHTML = `<tr><td colspan="4" class="empty">no discovery events — k8s discovery may be disabled</td></tr>`;
    return;
  }
  tbody.innerHTML = events.map(e => {
    let detail = "";
    if (e.type === "tick") {
      const r = (e.resolved || []).join(", ");
      detail = r ? `<code>${escapeHTML(r)}</code>` : '<span class="empty">empty resolve</span>';
    } else if (e.type === "error") {
      detail = `<span class="bad-text">${escapeHTML(e.error || "unknown")}</span>`;
    } else {
      // add / remove
      const parts = [];
      if (e.upstream) parts.push(`upstream=<code>${escapeHTML(e.upstream)}</code>`);
      if (e.ip)       parts.push(`ip=<code>${escapeHTML(e.ip)}</code>`);
      detail = parts.join(" · ") || '<span class="empty">—</span>';
    }
    return `<tr>
      <td><span class="mono">${escapeHTML(fmtTimeShort(e.timestamp_ms))}</span></td>
      <td><code>${escapeHTML(e.template || "—")}</code></td>
      <td>${discoveryTypeBadge(e.type)}</td>
      <td class="summary-cell">${detail}</td>
    </tr>`;
  }).join("");
}

function discoveryTypeBadge(t) {
  switch (t) {
    case "add":    return badge("ok", "add");
    case "remove": return badge("warn", "remove");
    case "error":  return badge("bad", "error");
    case "tick":   return badge("muted", "tick");
    default:       return badge("muted", t || "—");
  }
}

// renderReloadTable(payload) — section-by-section before/after rule
// counts from the last reload. The pill in the topbar carries the
// overall status; this table is the detail the operator opens when
// they want to confirm "yes, the new rule landed in the right
// section."
function renderReloadTable(payload) {
  const tbody = $("#reload-table tbody");
  const when = $("#reload-when");
  if (!tbody) return;
  payload = normalizeReloadPayload(payload);
  const r = (payload && payload.reload) || {};
  if (!r.timestamp_ms) {
    tbody.innerHTML = `<tr><td colspan="4" class="empty">awaiting first reload…</td></tr>`;
    if (when) when.textContent = "no reloads yet";
    return;
  }
  if (when) {
    const ageStr = humanSecs(Math.max(0, Math.floor((Date.now() - r.timestamp_ms) / 1000))) + " ago";
    when.textContent = r.success ? `succeeded · ${ageStr}` : `failed · ${ageStr}`;
  }
  const sections = r.sections || {};
  const keys = Object.keys(sections).sort();
  if (keys.length === 0) {
    tbody.innerHTML = `<tr><td colspan="4" class="empty">reload had no rule sections</td></tr>`;
    return;
  }
  tbody.innerHTML = keys.map(k => {
    const s = sections[k] || {};
    return `<tr>
      <td><code>${escapeHTML(k)}</code></td>
      <td class="num-cell"><code>${s.before || 0}</code></td>
      <td class="num-cell"><code>${s.after || 0}</code></td>
      <td>${reloadDeltaCell(s)}</td>
    </tr>`;
  }).join("");
}

// reloadDeltaCell renders the section-delta column with separate
// add/remove/modify chips. The earlier "+N / -N" shorthand collapsed
// an in-place edit (where Before == After) into "no change" — that's
// the exact confusion the new ReloadSection fields fix. We now show
// every non-zero component as its own coloured chip and fall back to
// "no change" only when all three are zero.
function reloadDeltaCell(s) {
  const chips = [];
  if (s.added)    chips.push(`<span class="ok-text">+${s.added} added</span>`);
  if (s.modified) chips.push(`<span class="warn-text">~${s.modified} modified</span>`);
  if (s.removed)  chips.push(`<span class="bad-text">-${s.removed} removed</span>`);
  if (chips.length === 0) return '<span class="empty">no change</span>';
  return chips.join(" · ");
}

// timeAgo(ms) — humanises a wall-clock timestamp relative to "now"
// for the unmatched panel's "last seen" column. Falls back to an
// empty string for missing values so the table cell renders blank
// instead of "NaNs ago".
function timeAgo(ms) {
  if (!ms || !isFinite(ms)) return "—";
  const sec = Math.max(0, Math.floor((Date.now() - ms) / 1000));
  return humanSecs(sec) + " ago";
}

// fmtTimeShort(ms) — wall-clock HH:MM:SS for the denied + discovery
// tables. Same format as charts.js' internal helper, repeated here
// to avoid exposing it from charts.js just for two callers.
function fmtTimeShort(ms) {
  if (!ms || !isFinite(ms)) return "—";
  const d = new Date(ms);
  const pad = n => String(n).padStart(2, "0");
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

// renderClusterPeers(rows) — populates the Cluster tab's peer table
// using /api/v1/cluster/peers (one row per olric member, with partition
// ownership counts from the routing table) augmented with the per-peer
// envelope from the most-recent fan-out (so a peer that has rejoined
// but is failing to respond to peer-API calls shows up as "unreachable"
// instead of silently green). The "reachable" cell merges by pod id:
// rows that came back as Err in the envelope flip to red; rows that
// don't appear in lastPeers at all (cluster/peers but no fan-out yet)
// stay neutral.
function renderClusterPeers(rows) {
  const tbody = $("#cluster-peers-table tbody");
  if (!tbody) return;
  rows = Array.isArray(rows) ? rows : [];
  const reachableByPod = new Map();
  for (const p of lastPeers || []) {
    reachableByPod.set(p.pod_id, p);
  }
  if (rows.length === 0) {
    tbody.innerHTML = `<tr><td colspan="7" class="empty">no cluster peers — running embedded-only or this is the only pod</td></tr>`;
    return;
  }
  tbody.innerHTML = rows.map(p => {
    const rch = reachableByPod.get(p.pod_id);
    let reachCell;
    if (!rch) {
      reachCell = badge("muted", "—");
    } else if (rch.ok) {
      reachCell = badge("ok", "reachable");
    } else {
      const errAttr = rch.error ? ` title="${escapeHTML(rch.error)}"` : "";
      reachCell = `<span class="badge bad"${errAttr}><span class="dot"></span>unreachable</span>`;
    }
    const role = p.coordinator
      ? badge("ok", "coordinator")
      : badge("muted", "member");
    return `<tr>
      <td><code>${escapeHTML(p.pod_id || "?")}</code></td>
      <td><code>${escapeHTML(p.addr || "?")}</code></td>
      <td>${role}</td>
      <td>${escapeHTML(humanSecs(p.age_seconds || 0))}</td>
      <td class="num-cell"><code>${charts.fmtNumber(p.partitions_owned || 0)}</code></td>
      <td class="num-cell"><code>${charts.fmtNumber(p.partitions_backed || 0)}</code></td>
      <td>${reachCell}</td>
    </tr>`;
  }).join("");
  const hint = $("#cluster-hint");
  if (hint) hint.textContent = `${rows.length} member${rows.length === 1 ? "" : "s"} · partition counts from olric routing table`;
}

// renderClusterReload(payload) — per-pod reload status, surfaced as
// one row per known peer so the operator can answer "did every pod
// pick up the new config and on what timeline." Sorts by pod id so
// the table is stable across polls. payload is the unwrapped
// {reload: [...]} for cluster scope or {reload: ReloadStatus} for
// local — in the local case we synthesize a single-row table.
function renderClusterReload(payload) {
  const tbody = $("#cluster-reload-table tbody");
  if (!tbody) return;
  const r = payload && payload.reload;
  const rows = Array.isArray(r) ? r : (r && r.timestamp_ms ? [{ pod_id: "this pod", reload: r }] : []);
  if (rows.length === 0) {
    tbody.innerHTML = `<tr><td colspan="5" class="empty">no reloads yet</td></tr>`;
    return;
  }
  tbody.innerHTML = rows.map(row => {
    const rl = row.reload || {};
    let status, when;
    if (!rl.timestamp_ms) {
      status = badge("muted", "no reload");
      when = '<span class="empty">—</span>';
    } else {
      status = rl.success ? badge("ok", "ok") : badge("bad", "failed");
      when = escapeHTML(humanSecs(Math.max(0, Math.floor((Date.now() - rl.timestamp_ms) / 1000))) + " ago");
    }
    const sectionList = Object.entries(rl.sections || {})
      .filter(([, s]) => (s.added || s.removed || s.modified))
      .map(([k, s]) => {
        const parts = [];
        if (s.added)    parts.push(`+${s.added}`);
        if (s.modified) parts.push(`~${s.modified}`);
        if (s.removed)  parts.push(`-${s.removed}`);
        return `<code>${escapeHTML(k)}</code> ${parts.join(" ")}`;
      }).join(" · ") || '<span class="empty">no changes</span>';
    const err = rl.error ? `<span class="bad-text">${escapeHTML(rl.error)}</span>` : '<span class="empty">—</span>';
    return `<tr>
      <td><code>${escapeHTML(row.pod_id || "?")}</code></td>
      <td>${status}</td>
      <td>${when}</td>
      <td class="summary-cell">${sectionList}</td>
      <td>${err}</td>
    </tr>`;
  }).join("");
}

// hydrateHistory() — pre-populate mbuf from the server-side ring buffer
// so charts render with up to 5 minutes of context the moment the
// dashboard opens (instead of building up tick-by-tick after a pod
// restart or a fresh page load). Best-effort: a 4xx/5xx here just
// leaves the buffer empty and the regular refresh polls take over.
async function hydrateHistory() {
  try {
    const raw = await fetchJSON(apiBase() + "/metrics/history");
    const payload = unwrapEnvelope(raw);
    const snaps = (payload && payload.history) || [];
    // Snapshots arrive oldest-first; push() preserves order and caps
    // at maxSamples so over-cap restores are trimmed naturally.
    for (const s of snaps) mbuf.push(s);
  } catch (_) {
    // Silent: the live /api/v1/metrics poll will repopulate the buffer
    // on the next tick, and the panel renderers already handle empty
    // series gracefully.
  }
}

// Self-rescheduling chain instead of setInterval: a slow /api/v1/* poll
// (server stalled, browser tab throttled, network blip) would otherwise
// stack up calls — setInterval fires every 5s regardless of whether
// the previous fetch has returned, so a 30s server hiccup queues 6
// pending refreshes that all hit on recovery. The setTimeout chain
// only re-arms after the prior refresh resolves, so polls never
// overlap and a slow backend can't cascade.
async function refreshLoop() {
  try {
    await refresh();
  } finally {
    setTimeout(refreshLoop, 5000);
  }
}
// Resolve scope from /info BEFORE the initial hydrate: a cluster
// deployment's per-pod /api/v1/metrics/history reflects only that
// pod's slice of traffic, so hydrating against the local endpoint
// would seed the buffer with whichever fraction the load balancer
// picked. Fetching /info first lets us seed mbuf from
// /api/v1/cluster/metrics/history (cluster-aggregated, identical
// across pods).
(async () => {
  try {
    const info = await fetchJSON("./api/v1/info");
    updateScopeFromInfo(info, /*silent=*/true);
  } catch (_) {
    // /info unreachable means the pod is down; refreshLoop's first
    // iteration will surface the full error banner.
  }
  await hydrateHistory();
  refreshLoop();
})();
