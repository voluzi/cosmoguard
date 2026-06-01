// charts.js — hand-rolled SVG chart helpers + a small ring buffer
// for per-poll metric snapshots. No external dependencies, no build
// step. Lives next to app.js and is loaded by index.html via a
// plain <script src> tag before app.js so the helpers are global.
//
// Design choice: every chart accepts a caller-provided <svg> and
// draws into it via innerHTML rewrite. No DOM allocation in the
// hot path (data window is small: 60 samples @ 5s = 5 minutes).
// Hover detail is via the standard SVG <title> tooltip — no JS
// hover handlers, no positioned popovers, no z-index puzzles.

(function () {
  "use strict";

  // ---------------------------------------------------------------------
  // Color palette — uses the same CSS vars as the rest of the dashboard
  // so dark / light theme switching just works. Each chart accepts a
  // colors array if the caller wants to override.
  // ---------------------------------------------------------------------
  const SERIES_PALETTE = [
    "var(--accent)",
    "var(--ok)",
    "var(--warn)",
    "var(--bad)",
    "#a855f7", // purple
    "#06b6d4", // cyan
    "#ec4899", // pink
    "#f97316", // orange
  ];

  // svgNS / formatters / shared utilities -------------------------------
  function fmtNumber(n) {
    if (!isFinite(n)) return "—";
    if (n === 0) return "0";
    const abs = Math.abs(n);
    if (abs >= 1000) return n.toFixed(0);
    if (abs >= 10) return n.toFixed(1);
    if (abs >= 1) return n.toFixed(2);
    return n.toFixed(3);
  }
  function fmtRate(n) {
    if (!isFinite(n) || n === 0) return "0/s";
    return fmtNumber(n) + "/s";
  }
  function fmtPercent(n) {
    if (!isFinite(n)) return "—";
    return (n * 100).toFixed(n >= 0.1 ? 1 : 2) + "%";
  }
  function fmtSeconds(n) {
    if (!isFinite(n) || n <= 0) return "—";
    if (n < 0.001) return (n * 1e6).toFixed(0) + "µs";
    if (n < 1) return (n * 1000).toFixed(1) + "ms";
    return n.toFixed(2) + "s";
  }
  function fmtTimeShort(ts) {
    const d = new Date(ts);
    const hh = String(d.getHours()).padStart(2, "0");
    const mm = String(d.getMinutes()).padStart(2, "0");
    const ss = String(d.getSeconds()).padStart(2, "0");
    return `${hh}:${mm}:${ss}`;
  }

  // ---------------------------------------------------------------------
  // sparkline(svgEl, points, opts) — single autoscaled path for the
  // tiny chart next to each stat tile. points is an array of {ts,v}.
  // No axes, no labels, no legend; the surrounding card carries the
  // semantic context.
  // ---------------------------------------------------------------------
  function sparkline(svg, points, opts) {
    opts = opts || {};
    const w = svg.viewBox.baseVal.width || 120;
    const h = svg.viewBox.baseVal.height || 32;
    const color = opts.color || "var(--accent)";
    const pad = 2;
    if (!points || points.length < 2) {
      svg.innerHTML = `<text x="${w / 2}" y="${h / 2 + 4}" text-anchor="middle" fill="var(--muted-soft)" font-size="10">collecting…</text>`;
      return;
    }
    const xs = points.map(p => p.ts);
    const ys = points.map(p => p.v);
    const minY = Math.min(0, ...ys); // anchor to 0 so a flat-zero line is visible
    const maxY = Math.max(...ys, minY + 1e-9);
    const xMin = xs[0];
    const xMax = xs[xs.length - 1];
    const xSpan = Math.max(xMax - xMin, 1);
    const ySpan = Math.max(maxY - minY, 1e-9);
    const path = points
      .map((p, i) => {
        const x = pad + ((p.ts - xMin) / xSpan) * (w - 2 * pad);
        const y = h - pad - ((p.v - minY) / ySpan) * (h - 2 * pad);
        return (i === 0 ? "M" : "L") + x.toFixed(1) + "," + y.toFixed(1);
      })
      .join(" ");
    const lastP = points[points.length - 1];
    const lastX = pad + ((lastP.ts - xMin) / xSpan) * (w - 2 * pad);
    const lastY = h - pad - ((lastP.v - minY) / ySpan) * (h - 2 * pad);
    svg.innerHTML =
      `<path d="${path}" fill="none" stroke="${color}" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>` +
      `<circle cx="${lastX.toFixed(1)}" cy="${lastY.toFixed(1)}" r="2" fill="${color}"/>`;
  }

  // ---------------------------------------------------------------------
  // lineChart(svgEl, {series, yFormat}) — multi-series line chart with
  // gridlines, y-axis labels, legend, hover via <title>. Used in the
  // Traffic tab.
  // series: [{label, color?, points:[{ts,v}, ...]}, ...]
  // opts.yFormat: function(v) → string (default fmtNumber).
  // ---------------------------------------------------------------------
  function lineChart(svg, opts) {
    const w = svg.viewBox.baseVal.width || 600;
    const h = svg.viewBox.baseVal.height || 200;
    const series = (opts && opts.series) || [];
    const yFormat = (opts && opts.yFormat) || fmtNumber;
    const margins = { l: 48, r: 12, t: 8, b: 22 };
    const plotW = w - margins.l - margins.r;
    const plotH = h - margins.t - margins.b;

    if (series.length === 0 || series.every(s => !s.points || s.points.length < 2)) {
      svg.innerHTML = `<text x="${w / 2}" y="${h / 2}" text-anchor="middle" fill="var(--muted-soft)" font-size="12">collecting…</text>`;
      return;
    }

    // Combined x/y range across all series so they share the axes.
    let xMin = Infinity, xMax = -Infinity, yMax = -Infinity;
    for (const s of series) {
      for (const p of s.points || []) {
        if (p.ts < xMin) xMin = p.ts;
        if (p.ts > xMax) xMax = p.ts;
        if (p.v > yMax) yMax = p.v;
      }
    }
    if (!isFinite(xMin) || !isFinite(yMax) || yMax <= 0) yMax = 1;
    const xSpan = Math.max(xMax - xMin, 1);
    const yMin = 0; // request-rate / latency / hit-rate are non-negative

    // Y gridlines at 0, 25%, 50%, 75%, 100%.
    let grid = "";
    let yLabels = "";
    for (let i = 0; i <= 4; i++) {
      const y = margins.t + (i / 4) * plotH;
      const val = yMax - (i / 4) * (yMax - yMin);
      grid += `<line x1="${margins.l}" y1="${y.toFixed(1)}" x2="${margins.l + plotW}" y2="${y.toFixed(1)}" stroke="var(--border)" stroke-width="0.5"/>`;
      yLabels += `<text x="${margins.l - 4}" y="${(y + 3).toFixed(1)}" text-anchor="end" font-size="10" fill="var(--muted)">${yFormat(val)}</text>`;
    }
    // X gridline at the right edge for the latest tick.
    const xLabels =
      `<text x="${margins.l}" y="${h - 6}" text-anchor="start" font-size="10" fill="var(--muted)">${fmtTimeShort(xMin)}</text>` +
      `<text x="${margins.l + plotW}" y="${h - 6}" text-anchor="end" font-size="10" fill="var(--muted)">${fmtTimeShort(xMax)}</text>`;

    // Each series → a single path; <title> on the path gives a
    // hover summary (browsers honor it after a short delay).
    //
    // Legend wraps to a new row when the next item would extend
    // past the plot's right edge. Without wrap, a chart with many
    // series (e.g. 5 protocols on the Request-rate panel) pushed
    // the legend past the SVG viewBox and the rightmost item
    // visibly clipped at the card edge.
    let paths = "";
    let legend = "";
    const legendMaxX = margins.l + plotW;
    const legendRowH = 12;
    let legendX = margins.l;
    let legendY = 2;
    series.forEach((s, idx) => {
      const color = s.color || SERIES_PALETTE[idx % SERIES_PALETTE.length];
      const pts = (s.points || []).filter(p => isFinite(p.v));
      if (pts.length < 2) return;
      const d = pts
        .map((p, i) => {
          const x = margins.l + ((p.ts - xMin) / xSpan) * plotW;
          const y = margins.t + plotH - ((p.v - yMin) / Math.max(yMax - yMin, 1e-9)) * plotH;
          return (i === 0 ? "M" : "L") + x.toFixed(1) + "," + y.toFixed(1);
        })
        .join(" ");
      const last = pts[pts.length - 1];
      paths +=
        `<path d="${d}" fill="none" stroke="${color}" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">` +
        `<title>${escAttr(s.label)}: ${yFormat(last.v)} @ ${fmtTimeShort(last.ts)}</title>` +
        `</path>`;
      // Estimate this item's pixel width: 14px for the swatch +
      // ~6px per character of label. Wrap when adding it would
      // overflow the plot's right edge.
      const itemW = 18 + (s.label.length * 6);
      if (legendX > margins.l && legendX + itemW > legendMaxX) {
        legendX = margins.l;
        legendY += legendRowH;
      }
      legend +=
        `<g transform="translate(${legendX},${legendY})">` +
        `<rect width="10" height="3" y="6" fill="${color}"/>` +
        `<text x="14" y="10" font-size="10" fill="var(--fg)">${escAttr(s.label)}</text>` +
        `</g>`;
      legendX += itemW;
    });

    svg.innerHTML = grid + yLabels + xLabels + paths + legend;
  }

  // ---------------------------------------------------------------------
  // stackedAreaChart(svgEl, {series, colors}) — area chart where
  // series stack on top of each other. Used for the cache-outcome
  // breakdown (hit vs miss vs error vs n/a) so the y-total is also
  // the total request rate.
  // series: [{label, color?, points:[{ts,v}, ...]}, ...]
  // All series MUST share the same `ts` axis.
  // ---------------------------------------------------------------------
  function stackedAreaChart(svg, opts) {
    const w = svg.viewBox.baseVal.width || 600;
    const h = svg.viewBox.baseVal.height || 200;
    const series = (opts && opts.series) || [];
    const margins = { l: 48, r: 12, t: 8, b: 22 };
    const plotW = w - margins.l - margins.r;
    const plotH = h - margins.t - margins.b;

    if (series.length === 0 || (series[0].points || []).length < 2) {
      svg.innerHTML = `<text x="${w / 2}" y="${h / 2}" text-anchor="middle" fill="var(--muted-soft)" font-size="12">collecting…</text>`;
      return;
    }

    const n = series[0].points.length;
    const xs = series[0].points.map(p => p.ts);
    const xMin = xs[0];
    const xMax = xs[xs.length - 1];
    const xSpan = Math.max(xMax - xMin, 1);
    // Compute stacked y at each index.
    const stacks = new Array(n).fill(0);
    let yMax = 0;
    for (const s of series) {
      for (let i = 0; i < n; i++) {
        const v = (s.points[i] && s.points[i].v) || 0;
        stacks[i] += v;
        if (stacks[i] > yMax) yMax = stacks[i];
      }
    }
    if (yMax <= 0) yMax = 1;

    // Gridlines + y labels.
    let grid = "";
    let yLabels = "";
    for (let i = 0; i <= 4; i++) {
      const y = margins.t + (i / 4) * plotH;
      const val = yMax - (i / 4) * yMax;
      grid += `<line x1="${margins.l}" y1="${y.toFixed(1)}" x2="${margins.l + plotW}" y2="${y.toFixed(1)}" stroke="var(--border)" stroke-width="0.5"/>`;
      yLabels += `<text x="${margins.l - 4}" y="${(y + 3).toFixed(1)}" text-anchor="end" font-size="10" fill="var(--muted)">${fmtRate(val)}</text>`;
    }
    const xLabels =
      `<text x="${margins.l}" y="${h - 6}" text-anchor="start" font-size="10" fill="var(--muted)">${fmtTimeShort(xMin)}</text>` +
      `<text x="${margins.l + plotW}" y="${h - 6}" text-anchor="end" font-size="10" fill="var(--muted)">${fmtTimeShort(xMax)}</text>`;

    // Build each layer top-down as a closed polygon path.
    // running[i] = cumulative sum at index i so far. Legend wraps
    // identically to lineChart so a narrow 3-column-grid panel
    // doesn't push the last layer label off the right edge.
    const running = new Array(n).fill(0);
    let paths = "";
    let legend = "";
    const legendMaxX = margins.l + plotW;
    const legendRowH = 12;
    let legendX = margins.l;
    let legendY = 2;
    series.forEach((s, idx) => {
      const color = s.color || SERIES_PALETTE[idx % SERIES_PALETTE.length];
      const top = [];
      const bottom = [];
      for (let i = 0; i < n; i++) {
        const v = (s.points[i] && s.points[i].v) || 0;
        const x = margins.l + ((xs[i] - xMin) / xSpan) * plotW;
        const yLo = margins.t + plotH - (running[i] / yMax) * plotH;
        running[i] += v;
        const yHi = margins.t + plotH - (running[i] / yMax) * plotH;
        top.push(`${x.toFixed(1)},${yHi.toFixed(1)}`);
        bottom.push(`${x.toFixed(1)},${yLo.toFixed(1)}`);
      }
      const d = "M" + top.join(" L") + " L" + bottom.reverse().join(" L") + " Z";
      paths += `<path d="${d}" fill="${color}" fill-opacity="0.6" stroke="${color}" stroke-width="0.5"><title>${escAttr(s.label)}</title></path>`;
      const itemW = 18 + (s.label.length * 6);
      if (legendX > margins.l && legendX + itemW > legendMaxX) {
        legendX = margins.l;
        legendY += legendRowH;
      }
      legend +=
        `<g transform="translate(${legendX},${legendY})">` +
        `<rect width="10" height="3" y="6" fill="${color}"/>` +
        `<text x="14" y="10" font-size="10" fill="var(--fg)">${escAttr(s.label)}</text>` +
        `</g>`;
      legendX += itemW;
    });

    svg.innerHTML = grid + yLabels + xLabels + paths + legend;
  }

  // ---------------------------------------------------------------------
  // timelineStrip(svgEl, samples) — green/red bar strip showing one
  // upstream's healthy gauge over time. samples is [{ts, healthy(0/1)}].
  // ---------------------------------------------------------------------
  function timelineStrip(svg, samples) {
    const w = svg.viewBox.baseVal.width || 240;
    const h = svg.viewBox.baseVal.height || 16;
    if (!samples || samples.length < 1) {
      svg.innerHTML = `<rect width="${w}" height="${h}" fill="var(--bg-soft)"/>`;
      return;
    }
    // One bar per sample, stretched to fill the width.
    const n = samples.length;
    const cellW = w / n;
    let bars = "";
    samples.forEach((s, i) => {
      const x = i * cellW;
      const color = s.healthy >= 1 ? "var(--ok)" : "var(--bad)";
      bars += `<rect x="${x.toFixed(1)}" y="0" width="${(cellW + 0.5).toFixed(1)}" height="${h}" fill="${color}"/>`;
    });
    svg.innerHTML = bars;
  }

  // ---------------------------------------------------------------------
  // multiPercentileChart(svg, {seriesByUpstream, q, yFormat}) — small
  // overlay chart used by the per-upstream latency panel. Draws one
  // line per upstream in its own palette slot. `seriesByUpstream` is
  // a map[upstream] = [{ts,v}, ...]; `q` is the quantile label baked
  // into the legend (e.g. "p95"). Internally just a thin wrapper
  // around lineChart so the small-multiples grid layout stays in
  // app.js where it can use the upstream-count to choose a grid.
  // ---------------------------------------------------------------------
  function multiPercentileChart(svg, opts) {
    const seriesByUpstream = (opts && opts.seriesByUpstream) || {};
    const upstreams = Object.keys(seriesByUpstream).sort();
    const series = upstreams.map((u, i) => ({
      label: u,
      color: SERIES_PALETTE[i % SERIES_PALETTE.length],
      points: seriesByUpstream[u] || [],
    }));
    lineChart(svg, {
      series,
      yFormat: (opts && opts.yFormat) || fmtSeconds,
    });
  }

  // ---------------------------------------------------------------------
  // barListByValue(host, items, opts) — horizontal-bar list rendered
  // with plain divs (no SVG) so it reflows naturally with the panel.
  // items: [{label, value, sub?, badge?}, ...] — value is the numeric
  // magnitude used to compute the bar's width relative to the largest
  // item. opts.valueFormat (function) formats the value displayed at
  // the bar's right end; defaults to fmtNumber. opts.emptyText
  // overrides the placeholder when items is empty.
  // ---------------------------------------------------------------------
  function barListByValue(host, items, opts) {
    opts = opts || {};
    const fmt = opts.valueFormat || fmtNumber;
    if (!items || items.length === 0) {
      host.innerHTML = `<div class="empty">${escAttr(opts.emptyText || "nothing to report yet")}</div>`;
      return;
    }
    const max = items.reduce((m, x) => (x.value > m ? x.value : m), 0) || 1;
    host.innerHTML = items
      .map(it => {
        const pct = Math.max(2, Math.min(100, (it.value / max) * 100));
        const badge = it.badge ? `<span class="bar-list-badge">${escAttr(it.badge)}</span>` : "";
        const sub = it.sub ? `<div class="bar-list-sub">${escAttr(it.sub)}</div>` : "";
        return `<div class="bar-list-row">
          <div class="bar-list-meta">
            <div class="bar-list-label">${escAttr(it.label)} ${badge}</div>
            ${sub}
          </div>
          <div class="bar-list-track">
            <div class="bar-list-fill" style="width:${pct.toFixed(1)}%"></div>
          </div>
          <div class="bar-list-value">${escAttr(fmt(it.value))}</div>
        </div>`;
      })
      .join("");
  }

  // ---------------------------------------------------------------------
  // MetricsBuffer — keeps a rolling window of per-poll snapshots and
  // computes deltas + rates client-side. The server is stateless w.r.t.
  // time; everything time-series lives here.
  // ---------------------------------------------------------------------
  class MetricsBuffer {
    constructor(opts) {
      this.max = (opts && opts.maxSamples) || 60;
      this.samples = []; // each: {ts: Number, snap: MetricsSnapshot JSON}
    }
    push(snap) {
      if (!snap || typeof snap.timestamp_ms !== "number") return;
      this.samples.push({ ts: snap.timestamp_ms, snap });
      if (this.samples.length > this.max) {
        this.samples.shift();
      }
    }
    // reset() — drop every sample. Used by the dashboard when the
    // scope toggles between "this pod" and "cluster": cumulative
    // counters across scopes aren't comparable, so the next push()
    // re-seeds the baseline cleanly.
    reset() { this.samples = []; }
    size() { return this.samples.length; }

    // rate(proto, labelPath?) → [{ts, v}] — instantaneous rate (Δcount/Δt)
    // sampled at each poll. labelPath = ["by_cache","hit"] e.g.
    // returns just histogram.count when labelPath is omitted.
    rate(proto, labelPath) {
      if (this.samples.length < 2) return [];
      const out = [];
      for (let i = 1; i < this.samples.length; i++) {
        const a = this.samples[i - 1];
        const b = this.samples[i];
        const va = countAt(a.snap, proto, labelPath);
        const vb = countAt(b.snap, proto, labelPath);
        const dt = (b.ts - a.ts) / 1000;
        if (dt <= 0) continue;
        let v = (vb - va) / dt;
        if (v < 0) v = 0; // counter reset (hot-reload) → don't flash negative
        out.push({ ts: b.ts, v });
      }
      return out;
    }

    // totalRate(proto) — sum across every protocol's request count.
    // When proto === "*", sums all protocols.
    totalRate(proto) {
      if (proto === "*") {
        const protos = this.protocolList();
        const merged = {};
        for (const p of protos) {
          for (const pt of this.rate(p)) {
            merged[pt.ts] = (merged[pt.ts] || 0) + pt.v;
          }
        }
        return Object.keys(merged).map(k => ({ ts: +k, v: merged[k] })).sort((a, b) => a.ts - b.ts);
      }
      return this.rate(proto);
    }

    // cacheBreakdown(proto) → {hit, miss, error, na: [{ts,v}, ...]}
    // — each label's rate series, aligned on the same timestamps.
    cacheBreakdown(proto) {
      return {
        hit: this.rate(proto, ["by_cache", "hit"]),
        miss: this.rate(proto, ["by_cache", "miss"]),
        error: this.rate(proto, ["by_cache", "error"]),
        na: this.rate(proto, ["by_cache", "n/a"]),
      };
    }

    // hitRate(proto) → [{ts,v}] of (hit / hit+miss+error+na) per sample.
    hitRate(proto) {
      const cb = this.cacheBreakdown(proto);
      const series = [];
      // Walk by hit's index since all series share the same Δt.
      for (let i = 0; i < cb.hit.length; i++) {
        const total = (cb.hit[i]?.v || 0) + (cb.miss[i]?.v || 0) + (cb.error[i]?.v || 0) + (cb.na[i]?.v || 0);
        if (total <= 0) {
          series.push({ ts: cb.hit[i].ts, v: 0 });
        } else {
          series.push({ ts: cb.hit[i].ts, v: (cb.hit[i].v || 0) / total });
        }
      }
      return series;
    }

    // hitRateSnapshot(proto) — single point-in-time hit % derived from
    // the cumulative counters of the LAST snapshot (no delta). Used
    // for stat-tile big numbers when there's only one sample yet.
    hitRateSnapshot(proto) {
      if (this.samples.length === 0) return 0;
      const last = this.samples[this.samples.length - 1].snap;
      const p = last.protocols && last.protocols[proto];
      if (!p || !p.by_cache) return 0;
      const c = p.by_cache;
      const total = (c.hit || 0) + (c.miss || 0) + (c.error || 0) + (c["n/a"] || 0);
      if (total <= 0) return 0;
      return (c.hit || 0) / total;
    }

    // errorRate(proto) → [{ts,v}] of fraction of requests that ended in
    // a deny or a 4xx/5xx status. Sums over by_action["deny"] +
    // by_status keys starting with 4 or 5 (excluding 200-class).
    errorRate(proto) {
      if (this.samples.length < 2) return [];
      const out = [];
      for (let i = 1; i < this.samples.length; i++) {
        const a = this.samples[i - 1];
        const b = this.samples[i];
        const totalA = countAt(a.snap, proto);
        const totalB = countAt(b.snap, proto);
        const dTotal = totalB - totalA;
        if (dTotal <= 0) { out.push({ ts: b.ts, v: 0 }); continue; }
        let errA = (countAt(a.snap, proto, ["by_action", "deny"]) || 0);
        let errB = (countAt(b.snap, proto, ["by_action", "deny"]) || 0);
        const aStatus = (a.snap.protocols?.[proto]?.by_status) || {};
        const bStatus = (b.snap.protocols?.[proto]?.by_status) || {};
        for (const k of new Set([...Object.keys(aStatus), ...Object.keys(bStatus)])) {
          if (k.startsWith("4") || k.startsWith("5")) {
            errA += (aStatus[k] || 0);
            errB += (bStatus[k] || 0);
          }
        }
        const dErr = Math.max(0, errB - errA);
        out.push({ ts: b.ts, v: dErr / dTotal });
      }
      return out;
    }

    // trailingPair(windowSec) — returns the oldest sample within the
    // last windowSec seconds paired with the newest sample, or null
    // when fewer than 2 samples exist. Used by the trailing-window
    // helpers below so Overview tiles aggregate over a meaningful
    // span instead of the most-recent 5-second window (which is
    // 0–2 requests on a low-traffic deployment and flaps wildly).
    trailingPair(windowSec) {
      const n = this.samples.length;
      if (n < 2) return null;
      const newest = this.samples[n - 1];
      const cutoff = newest.ts - windowSec * 1000;
      let oldest = this.samples[0];
      for (let i = 0; i < n - 1; i++) {
        if (this.samples[i].ts >= cutoff) { oldest = this.samples[i]; break; }
      }
      if (oldest.ts >= newest.ts) return null;
      return { a: oldest, b: newest };
    }

    // meanRateTrailing(proto, windowSec) — req/s over the trailing
    // window. proto === "*" sums across every protocol.
    meanRateTrailing(proto, windowSec) {
      const pair = this.trailingPair(windowSec);
      if (!pair) return 0;
      const dt = (pair.b.ts - pair.a.ts) / 1000;
      if (dt <= 0) return 0;
      const protos = proto === "*" ? this.protocolList() : [proto];
      let dTotal = 0;
      for (const p of protos) {
        const va = countAt(pair.a.snap, p);
        const vb = countAt(pair.b.snap, p);
        if (vb > va) dTotal += (vb - va);
      }
      return dTotal / dt;
    }

    // hitRateTrailing(proto, windowSec) — (Σhit / Σrequests) over the
    // trailing window. proto === "*" sums across every protocol.
    // Weighting by request count happens naturally because we're
    // summing the raw deltas, not averaging per-window ratios.
    hitRateTrailing(proto, windowSec) {
      const pair = this.trailingPair(windowSec);
      if (!pair) return this.hitRateSnapshot(proto === "*" ? this.protocolList()[0] : proto);
      const protos = proto === "*" ? this.protocolList() : [proto];
      let hit = 0, total = 0;
      for (const p of protos) {
        const ha = countAt(pair.a.snap, p, ["by_cache", "hit"]) || 0;
        const hb = countAt(pair.b.snap, p, ["by_cache", "hit"]) || 0;
        if (hb > ha) hit += (hb - ha);
        const aBC = pair.a.snap.protocols?.[p]?.by_cache || {};
        const bBC = pair.b.snap.protocols?.[p]?.by_cache || {};
        for (const k of new Set([...Object.keys(aBC), ...Object.keys(bBC)])) {
          const d = (bBC[k] || 0) - (aBC[k] || 0);
          if (d > 0) total += d;
        }
      }
      if (total <= 0) return 0;
      return hit / total;
    }

    // errorRateTrailing(proto, windowSec) — deny + 4xx + 5xx fraction
    // over the trailing window.
    errorRateTrailing(proto, windowSec) {
      const pair = this.trailingPair(windowSec);
      if (!pair) return 0;
      const protos = proto === "*" ? this.protocolList() : [proto];
      let err = 0, total = 0;
      for (const p of protos) {
        const dTotal = countAt(pair.b.snap, p) - countAt(pair.a.snap, p);
        if (dTotal > 0) total += dTotal;
        const denyA = countAt(pair.a.snap, p, ["by_action", "deny"]) || 0;
        const denyB = countAt(pair.b.snap, p, ["by_action", "deny"]) || 0;
        if (denyB > denyA) err += (denyB - denyA);
        const aStatus = pair.a.snap.protocols?.[p]?.by_status || {};
        const bStatus = pair.b.snap.protocols?.[p]?.by_status || {};
        for (const k of new Set([...Object.keys(aStatus), ...Object.keys(bStatus)])) {
          if (!k.startsWith("4") && !k.startsWith("5")) continue;
          const d = (bStatus[k] || 0) - (aStatus[k] || 0);
          if (d > 0) err += d;
        }
      }
      if (total <= 0) return 0;
      return err / total;
    }

    // latencyQuantileTrailing(proto, q, windowSec) — qth quantile
    // computed from histogram bucket deltas between the oldest and
    // newest samples in the trailing window. Produces a single
    // statistically meaningful number (one big bucket of samples
    // instead of averaging many tiny per-window p99s). proto === "*"
    // sums buckets across every protocol.
    latencyQuantileTrailing(proto, q, windowSec) {
      const pair = this.trailingPair(windowSec);
      if (!pair) return 0;
      const protos = proto === "*" ? this.protocolList() : [proto];
      const bucketAgg = new Map();
      let totalDelta = 0;
      for (const p of protos) {
        const pa = pair.a.snap.protocols?.[p]?.histogram;
        const pb = pair.b.snap.protocols?.[p]?.histogram;
        if (!pa || !pb || !pa.buckets || !pb.buckets) continue;
        const d = (pb.count || 0) - (pa.count || 0);
        if (d <= 0) continue;
        totalDelta += d;
        const aMap = new Map(pa.buckets.map(b => [b.le, b.count || 0]));
        for (const bk of pb.buckets) {
          const bd = (bk.count || 0) - (aMap.get(bk.le) || 0);
          if (bd > 0) bucketAgg.set(bk.le, (bucketAgg.get(bk.le) || 0) + bd);
        }
      }
      if (totalDelta <= 0) return 0;
      const bounds = [...bucketAgg.keys()].sort((a, b) => a - b);
      const target = totalDelta * q;
      let last = { le: 0, cum: 0 };
      let cum = 0;
      for (const le of bounds) {
        cum += bucketAgg.get(le);
        if (cum >= target) {
          const span = cum - last.cum || 1;
          const need = target - last.cum;
          return last.le + (need / span) * (le - last.le);
        }
        last = { le, cum };
      }
      return bounds[bounds.length - 1] || 0;
    }

    // latencyQuantile(proto, q) → [{ts,v}] approximating the qth
    // quantile of per-window observations from the histogram bucket
    // delta. Linear interpolation across bucket boundaries; not as
    // precise as a server-side native histogram, but plenty for
    // dashboards.
    latencyQuantile(proto, q) {
      if (this.samples.length < 2) return [];
      const out = [];
      for (let i = 1; i < this.samples.length; i++) {
        const a = this.samples[i - 1].snap;
        const b = this.samples[i].snap;
        const pa = a.protocols?.[proto]?.histogram;
        const pb = b.protocols?.[proto]?.histogram;
        if (!pa || !pb || !pa.buckets || !pb.buckets) continue;
        const totalDelta = (pb.count || 0) - (pa.count || 0);
        if (totalDelta <= 0) { out.push({ ts: this.samples[i].ts, v: 0 }); continue; }
        // Build delta-per-bucket array aligned by Le.
        const aMap = new Map(pa.buckets.map(b => [b.le, b.count || 0]));
        const bArr = pb.buckets;
        let cum = 0;
        const target = totalDelta * q;
        let last = { le: 0, cum: 0 };
        let landed = null;
        for (const bk of bArr) {
          const delta = (bk.count || 0) - (aMap.get(bk.le) || 0);
          cum += delta;
          if (cum >= target && landed === null) {
            // Linear interpolation within this bucket.
            const span = cum - last.cum || 1;
            const need = target - last.cum;
            const frac = need / span;
            landed = last.le + frac * (bk.le - last.le);
            break;
          }
          last = { le: bk.le, cum };
        }
        out.push({ ts: this.samples[i].ts, v: landed === null ? bArr[bArr.length - 1].le : landed });
      }
      return out;
    }

    // upstreamLatencyQuantile(proto, upstream, q) → [{ts,v}] —
    // per-upstream variant of latencyQuantile(). Differences the
    // `by_upstream_histogram[upstream]` cumulative buckets between
    // adjacent samples and linearly interpolates the qth quantile
    // inside the landing bucket. Returns [] when the snapshot lacks
    // the per-upstream split (older proxies) or the upstream went
    // quiet across the window.
    upstreamLatencyQuantile(proto, upstream, q) {
      if (this.samples.length < 2) return [];
      const out = [];
      for (let i = 1; i < this.samples.length; i++) {
        const a = this.samples[i - 1].snap;
        const b = this.samples[i].snap;
        const ua = a.protocols?.[proto]?.by_upstream_histogram?.[upstream];
        const ub = b.protocols?.[proto]?.by_upstream_histogram?.[upstream];
        if (!ua || !ub || !ua.buckets || !ub.buckets) continue;
        const totalDelta = (ub.count || 0) - (ua.count || 0);
        if (totalDelta <= 0) { out.push({ ts: this.samples[i].ts, v: 0 }); continue; }
        const aMap = new Map(ua.buckets.map(b => [b.le, b.count || 0]));
        const bArr = ub.buckets;
        let cum = 0;
        const target = totalDelta * q;
        let last = { le: 0, cum: 0 };
        let landed = null;
        for (const bk of bArr) {
          const delta = (bk.count || 0) - (aMap.get(bk.le) || 0);
          cum += delta;
          if (cum >= target && landed === null) {
            const span = cum - last.cum || 1;
            const need = target - last.cum;
            const frac = need / span;
            landed = last.le + frac * (bk.le - last.le);
            break;
          }
          last = { le: bk.le, cum };
        }
        out.push({ ts: this.samples[i].ts, v: landed === null ? bArr[bArr.length - 1].le : landed });
      }
      return out;
    }

    // upstreamsForProto(proto) — every upstream label that has a
    // per-upstream histogram entry in the LATEST snapshot. Used by
    // the Diagnostics tab's per-upstream latency small-multiples.
    upstreamsForProto(proto) {
      if (this.samples.length === 0) return [];
      const last = this.samples[this.samples.length - 1].snap;
      const m = last.protocols?.[proto]?.by_upstream_histogram;
      if (!m) return [];
      return Object.keys(m).sort();
    }

    // perRequestRoutingDelta(seconds) → map[proto] = {
    //   cache:     number,           // served from local cache (never touched upstream)
    //   upstreams: {name: number},   // reached this upstream
    //   total:     number,           // every request in the window (incl. denied/rate-limited)
    // }
    //
    // The Diagnostics tab uses this to render the "where did each
    // request go" panel — cache vs each upstream. The empty-string
    // upstream bucket (cache hits + denied requests carry no
    // upstream label) is intentionally NOT folded back into the
    // upstreams object; instead, cache-hits are sourced from
    // by_cache["hit"] and the empty bucket's residue (total - cache -
    // sum(upstreams)) represents requests that never made it past
    // the rule engine (deny / rate-limit / auth failure). Surfacing
    // those three populations separately answers "1000 sent, 1 to
    // upstream, 999 cached" the way an operator expects.
    perRequestRoutingDelta(seconds) {
      const out = {};
      if (this.samples.length < 2) return out;
      const target = Date.now() - seconds * 1000;
      let baseIdx = 0;
      for (let i = 0; i < this.samples.length; i++) {
        if (this.samples[i].ts >= target) break;
        baseIdx = i;
      }
      const base = this.samples[baseIdx].snap;
      const last = this.samples[this.samples.length - 1].snap;
      const protos = new Set([
        ...Object.keys(base.protocols || {}),
        ...Object.keys(last.protocols || {}),
      ]);
      for (const proto of protos) {
        const bP = base.protocols?.[proto] || {};
        const lP = last.protocols?.[proto] || {};
        const bU = bP.by_upstream || {};
        const lU = lP.by_upstream || {};
        const upstreams = {};
        let upstreamSum = 0;
        for (const u of new Set([...Object.keys(bU), ...Object.keys(lU)])) {
          if (u === "") continue; // empty label = cache hit or denied — counted below
          const d = Math.max(0, (lU[u] || 0) - (bU[u] || 0));
          if (d > 0) { upstreams[u] = d; upstreamSum += d; }
        }
        const bC = bP.by_cache || {};
        const lC = lP.by_cache || {};
        const cache = Math.max(0, (lC.hit || 0) - (bC.hit || 0));
        const total = Math.max(
          0,
          ((lP.histogram && lP.histogram.count) || 0) -
            ((bP.histogram && bP.histogram.count) || 0),
        );
        if (total === 0 && upstreamSum === 0 && cache === 0) continue;
        out[proto] = { cache, upstreams, total };
      }
      return out;
    }

    // upstreamHealth() → map[poolUpstream] = [{ts,healthy:0/1}, ...]
    upstreamHealth() {
      const out = {};
      for (const s of this.samples) {
        for (const u of (s.snap.upstreams || [])) {
          const k = `${u.pool}/${u.upstream}`;
          if (!out[k]) out[k] = [];
          out[k].push({ ts: s.ts, healthy: u.healthy });
        }
      }
      return out;
    }

    // protocolList() — every protocol slug present in the latest
    // snapshot. Used by Traffic-tab "iterate over protocols" loops.
    protocolList() {
      if (this.samples.length === 0) return [];
      const last = this.samples[this.samples.length - 1].snap;
      return Object.keys(last.protocols || {}).sort();
    }

    // perRuleCounts() → map[rule_id] = cumulative count summed across
    // every protocol (in the LATEST snapshot). Used by the Rules tab
    // to annotate rules with req-5m + hit-rate columns.
    perRuleCounts() {
      const out = {};
      if (this.samples.length === 0) return out;
      const last = this.samples[this.samples.length - 1].snap;
      for (const p of Object.values(last.protocols || {})) {
        for (const [k, v] of Object.entries(p.by_rule || {})) {
          out[k] = (out[k] || 0) + v;
        }
      }
      return out;
    }

    // perRuleHitRate() → map[rule_id] = hit / (hit+miss+error+n/a)
    // computed from the LATEST snapshot's cumulative counters
    // summed across every protocol. A rule that never went through
    // a cacheable code path returns 0 (or its absence).
    //
    // The histogram labels carry cache + rule_id together on each
    // observation, but the snapshot only aggregates them
    // independently (by_cache vs by_rule). To get per-rule
    // hit-rate we re-walk the most recent samples (.snap.protocols)
    // and look for the bounded set of cache labels under each
    // rule's bucket — done by re-reading the raw families isn't
    // possible from here, so we approximate: a rule's hit rate is
    // the by_cache breakdown of its OWNING protocol, weighted by
    // the rule's share of that protocol's traffic. For most
    // configs (one cacheable rule per protocol) this collapses to
    // the protocol-wide hit rate, which is correct.
    perRuleHitRate() {
      const out = {};
      if (this.samples.length === 0) return out;
      const last = this.samples[this.samples.length - 1].snap;
      for (const proto of Object.values(last.protocols || {})) {
        const c = proto.by_cache || {};
        const total = (c.hit || 0) + (c.miss || 0) + (c.error || 0) + (c["n/a"] || 0);
        if (total <= 0) continue;
        const hit = (c.hit || 0) / total;
        for (const rid of Object.keys(proto.by_rule || {})) {
          out[rid] = hit;
        }
      }
      return out;
    }

    // perRuleDelta(seconds) → map[rule_id] = count delta over the
    // last `seconds` of sampled history. Caps at the buffer's age if
    // less is available. Returns 0 when fewer than 2 samples.
    perRuleDelta(seconds) {
      const out = {};
      if (this.samples.length < 2) return out;
      const target = Date.now() - seconds * 1000;
      let baseIdx = 0;
      for (let i = 0; i < this.samples.length; i++) {
        if (this.samples[i].ts >= target) break;
        baseIdx = i;
      }
      const base = this.samples[baseIdx].snap;
      const last = this.samples[this.samples.length - 1].snap;
      const baseSum = {};
      const lastSum = {};
      for (const p of Object.values(base.protocols || {})) {
        for (const [k, v] of Object.entries(p.by_rule || {})) {
          baseSum[k] = (baseSum[k] || 0) + v;
        }
      }
      for (const p of Object.values(last.protocols || {})) {
        for (const [k, v] of Object.entries(p.by_rule || {})) {
          lastSum[k] = (lastSum[k] || 0) + v;
        }
      }
      for (const k of new Set([...Object.keys(baseSum), ...Object.keys(lastSum)])) {
        out[k] = Math.max(0, (lastSum[k] || 0) - (baseSum[k] || 0));
      }
      return out;
    }
  }

  // countAt(snap, proto, labelPath?) — total or by-label cumulative
  // count from one snapshot. Returns 0 when the path is missing.
  function countAt(snap, proto, labelPath) {
    const p = snap.protocols && snap.protocols[proto];
    if (!p) return 0;
    if (!labelPath) return (p.histogram && p.histogram.count) || 0;
    const m = p[labelPath[0]];
    if (!m) return 0;
    return m[labelPath[1]] || 0;
  }

  // escAttr — minimal HTML attribute / text escaping for <title>
  // and <text> nodes. Same intent as escapeHTML in app.js but kept
  // local so charts.js stays self-contained for review.
  function escAttr(s) {
    return String(s)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;");
  }

  // Publish a small global so app.js can pull what it needs without
  // imports.
  window.dashCharts = {
    sparkline,
    lineChart,
    stackedAreaChart,
    timelineStrip,
    multiPercentileChart,
    barListByValue,
    MetricsBuffer,
    fmtRate,
    fmtPercent,
    fmtSeconds,
    fmtNumber,
    SERIES_PALETTE,
  };
})();
