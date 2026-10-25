/* global Plotly */
// Thin Plotly wrappers with app-themed layouts.

// Spanish locale for every chart: month names, date order and "," decimals. Registered
// inline so it needs no extra CDN script (guarded: the UI-test Plotly stub has no register).
if (typeof Plotly !== "undefined" && typeof Plotly.register === "function") {
  Plotly.register({
    moduleType: "locale",
    name: "es",
    dictionary: {},
    format: {
      days: ["domingo", "lunes", "martes", "miércoles", "jueves", "viernes", "sábado"],
      shortDays: ["dom", "lun", "mar", "mié", "jue", "vie", "sáb"],
      months: ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"],
      shortMonths: ["ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic"],
      date: "%d/%m/%Y",
      decimal: ",",
      thousands: ".",
    },
  });
}

const COMMON_LAYOUT = {
  separators: ",.",
  font: { family: "Inter, sans-serif", size: 12, color: "#191c20" },
  paper_bgcolor: "rgba(0,0,0,0)",
  plot_bgcolor: "rgba(0,0,0,0)",
  margin: { l: 48, r: 16, t: 24, b: 44 },
  showlegend: false,
  xaxis: { gridcolor: "#e1e2e8", linecolor: "#c4c6d2", tickfont: { size: 11 } },
  yaxis: { gridcolor: "#e1e2e8", linecolor: "#c4c6d2", tickfont: { size: 11 } },
};
const CONFIG = { responsive: true, displaylogo: false, displayModeBar: false, locale: "es" };
// Date x-axes read day-first in Spanish ("13 abr") instead of Plotly's US "Apr 13, 2026".
const DAY_MS = 86400000;
const DATE_XAXIS = {
  ...COMMON_LAYOUT.xaxis,
  hoverformat: "%-d %b %Y",
  // Coarse → fine: Plotly takes the first stop whose (inclusive) range matches, so a tick
  // spacing of exactly 1 day must hit the day format before the hour one (same for 1 month).
  tickformatstops: [
    { dtickrange: ["M12", null], value: "%Y" },
    { dtickrange: ["M1", "M12"], value: "%b %Y" },
    { dtickrange: [DAY_MS, "M1"], value: "%-d %b" },
    { dtickrange: [null, DAY_MS], value: "%-d %b, %H:%M" },
  ],
};

// Every chart renders through here so the reveal transition (and any future
// motion tweak) stays in one place instead of five separate call sites.
function _renderChart(el, traces, layout, config) {
  el.classList.remove("fade-in");
  void el.offsetHeight; // force reflow so the animation restarts
  el.classList.add("fade-in");
  // Plotly writes zoom/autorange state back into the layout it is given; clone so the shared
  // axis objects (COMMON_LAYOUT.xaxis, DATE_XAXIS) never carry one chart's zoom into the next.
  Plotly.newPlot(el, traces, structuredClone(layout), config);
}

export function loadingSkeleton() {
  return '<div class="skeleton h-full w-full"></div>';
}

// overlay: optional secondary-axis series (e.g. Brent crude), [{date, value}] — same
// yaxis2 idiom as multiLineWithDiff, but always visible (no toggle-off state of its own;
// the caller only calls lineTrend with an overlay when it wants one shown).
export function lineTrend(el, points, { label = "Precio", overlay = null, overlayLabel = "Brent (USD/bbl)" } = {}) {
  if (!points || !points.length) { el.innerHTML = emptyMsg("Sin datos"); return; }
  el.innerHTML = "";
  const x = points.map((p) => p.date);
  const avg = points.map((p) => p.avg_price);
  const min = points.map((p) => p.min_price);
  const max = points.map((p) => p.max_price);
  const traces = [
    { x, y: max, name: "Max", mode: "lines", line: { color: "#b2c5ff", width: 1 }, hoverinfo: "skip" },
    { x, y: min, name: "Min", mode: "lines", line: { color: "#b2c5ff", width: 1 }, fill: "tonexty", fillcolor: "rgba(178,197,255,0.25)", hoverinfo: "skip" },
    { x, y: avg, name: label, mode: "lines+markers", line: { color: "#001642", width: 2.5 }, marker: { size: 5 } },
  ];
  const layout = { ...COMMON_LAYOUT, xaxis: DATE_XAXIS, showlegend: true, legend: { orientation: "h", y: -0.2 } };
  if (overlay && overlay.length) {
    traces.push({
      x: overlay.map((p) => p.date),
      y: overlay.map((p) => p.value),
      mode: "lines",
      name: overlayLabel,
      yaxis: "y2",
      line: { color: "#b3923a", width: 1.5, dash: "dot" },
    });
    layout.yaxis2 = { overlaying: "y", side: "right", gridcolor: "transparent", automargin: true };
  }
  _renderChart(el, traces, layout, CONFIG);
}

export function multiLine(el, seriesMap, { labels = {} } = {}) {
  const keys = Object.keys(seriesMap || {});
  if (!keys.length) { el.innerHTML = emptyMsg("Sin datos"); return; }
  el.innerHTML = "";
  const palette = ["#001642", "#0453cd", "#b3923a", "#0e7b52", "#a33d3d", "#6f42c1", "#ba1a1a"];
  const traces = keys.map((k, i) => {
    const pts = seriesMap[k] || [];
    return {
      x: pts.map((p) => p.date),
      y: pts.map((p) => p.avg_price),
      mode: "lines",
      name: labels[k] || k,
      line: { color: palette[i % palette.length], width: 2 },
    };
  });
  _renderChart(el, traces, { ...COMMON_LAYOUT, xaxis: DATE_XAXIS, showlegend: true, legend: { orientation: "h", y: -0.2 } }, CONFIG);
}

// Same as multiLine, plus one extra trace plotted on a secondary right-hand y-axis (%).
// Used for a computed %-diff line alongside price/cost lines that already share their own axis.
// diffSeries: [{date, value}]. diffVisible sets the trace's initial toggle state.
export function multiLineWithDiff(el, seriesMap, diffSeries, { labels = {}, diffLabel = "% dif.", diffVisible = true } = {}) {
  const keys = Object.keys(seriesMap || {});
  if (!keys.length) { el.innerHTML = emptyMsg("Sin datos"); return; }
  el.innerHTML = "";
  const palette = ["#001642", "#0453cd", "#b3923a", "#0e7b52", "#a33d3d", "#6f42c1", "#ba1a1a"];
  const traces = keys.map((k, i) => {
    const pts = seriesMap[k] || [];
    return {
      x: pts.map((p) => p.date),
      y: pts.map((p) => p.avg_price),
      mode: "lines",
      name: labels[k] || k,
      line: { color: palette[i % palette.length], width: 2 },
    };
  });
  const diffTraceIndex = traces.length;
  traces.push({
    x: (diffSeries || []).map((p) => p.date),
    y: (diffSeries || []).map((p) => p.value),
    mode: "lines",
    name: diffLabel,
    yaxis: "y2",
    line: { color: "#6f42c1", width: 1.5, dash: "dot" },
    visible: diffVisible ? true : "legendonly",
  });
  _renderChart(
    el,
    traces,
    {
      ...COMMON_LAYOUT,
      xaxis: DATE_XAXIS,
      showlegend: true,
      legend: { orientation: "h", y: -0.2 },
      // Hidden (not just the trace) when the diff line is toggled off — an unused axis with no
      // visible trace on it is still a right-hand scale a reader has to make sense of. automargin
      // grows the fixed right margin to fit the actual tick text (e.g. "-106.4%" on a narrow
      // card) instead of a guessed pixel width that a wide value would still clip.
      yaxis2: {
        overlaying: "y",
        side: "right",
        ticksuffix: "%",
        gridcolor: "transparent",
        visible: diffVisible,
        automargin: true,
      },
    },
    CONFIG,
  );
  el.dataset.diffTraceIndex = String(diffTraceIndex);
}

// Merges two {date, avg_price} series on date and computes the %-diff of b vs a.
// Drops dates missing from either side, and a zero-price date (division by zero), rather than
// pretending a value it doesn't have.
export function mergePctDiff(seriesA, seriesB) {
  const byDate = new Map((seriesA || []).map((p) => [p.date, p.avg_price]));
  return (seriesB || [])
    .filter((p) => byDate.get(p.date))
    .map((p) => {
      const a = byDate.get(p.date);
      return { date: p.date, value: ((p.avg_price - a) / a) * 100 };
    });
}

export function horizontalBar(el, rows, { labelKey, valueKey, color = "#001642", maxRows = 15, colorFn = null, tickSuffix = "", labelFn = null } = {}) {
  if (!rows || !rows.length) { el.innerHTML = emptyMsg("Sin datos"); return; }
  el.innerHTML = "";
  const slice = rows.slice(0, maxRows);
  const y = slice.map((r) => (labelFn ? labelFn(r[labelKey]) : r[labelKey]));
  const x = slice.map((r) => r[valueKey]);
  const markerColor = colorFn ? slice.map(colorFn) : color;
  _renderChart(
    el,
    [{ x, y, type: "bar", orientation: "h", marker: { color: markerColor } }],
    {
      ...COMMON_LAYOUT,
      margin: { l: 5, r: 16, t: 16, b: 32 },
      xaxis: { ...COMMON_LAYOUT.xaxis, type: "linear", ticksuffix: tickSuffix },
      yaxis: { ...COMMON_LAYOUT.yaxis, type: "category", autorange: "reversed", automargin: true },
    },
    CONFIG,
  );
}

// Tank level across the trip, iPhone-battery style: one bar per distance bucket, height
// is the tank fill % at that km. Fuel drains as km add up and jumps back up at each
// refuel stop. Bars are colored by fuel status (same thresholds as the trip KPIs) and a
// dashed line marks the requested reserve floor.
// Tank-level thresholds (%) shared with the trip KPIs so the chart colors and the
// "Combustible al llegar" KPI stay locked to the same low/mid boundaries.
export const FUEL_LOW_PCT = 15;
export const FUEL_MID_PCT = 30;
const FUEL_COLORS = { low: "#ba1a1a", mid: "#b3923a", ok: "#0e7b52" };
function fuelColor(pct) {
  return pct < FUEL_LOW_PCT ? FUEL_COLORS.low : pct < FUEL_MID_PCT ? FUEL_COLORS.mid : FUEL_COLORS.ok;
}

export function fuelByDistance(el, points, { floorPct = null, turnaroundKm = null } = {}) {
  if (!points || !points.length) { el.innerHTML = emptyMsg("Sin datos"); return; }
  el.innerHTML = "";
  const x = points.map((p) => p.km);
  const y = points.map((p) => p.pct);
  const colors = points.map((p) => fuelColor(p.pct));

  const shapes = [];
  const annotations = [];
  if (floorPct != null && floorPct > 0) {
    shapes.push({
      type: "line", xref: "paper", x0: 0, x1: 1, yref: "y", y0: floorPct, y1: floorPct,
      line: { color: "#ba1a1a", width: 1.5, dash: "dash" },
    });
    annotations.push({
      xref: "paper", x: 1, yref: "y", y: floorPct, yanchor: "bottom", xanchor: "right",
      text: "Reserva mín.", showarrow: false, font: { size: 10, color: "#ba1a1a" },
    });
  }
  // Round trip: mark where the outbound leg ends and the return leg begins.
  if (Number.isFinite(turnaroundKm) && turnaroundKm > 0) {
    shapes.push({
      type: "line", xref: "x", x0: turnaroundKm, x1: turnaroundKm, yref: "paper", y0: 0, y1: 1,
      line: { color: "#747782", width: 1.5, dash: "dot" },
    });
    annotations.push({
      xref: "x", x: turnaroundKm, yref: "paper", y: 1, yanchor: "bottom", xanchor: "center",
      text: "Destino", showarrow: false, font: { size: 10, color: "#747782" },
    });
  }

  _renderChart(
    el,
    [{
      x, y, type: "bar", marker: { color: colors },
      hovertemplate: "%{x:.0f} km<br>%{y:.0f}%<extra></extra>",
    }],
    {
      ...COMMON_LAYOUT,
      margin: { l: 48, r: 16, t: 24, b: 48 },
      bargap: 0.15,
      shapes,
      annotations,
      xaxis: { ...COMMON_LAYOUT.xaxis, type: "linear", ticksuffix: " km", rangemode: "tozero" },
      yaxis: { ...COMMON_LAYOUT.yaxis, ticksuffix: "%", range: [0, 100] },
    },
    CONFIG,
  );
}

// Mirrors --fp-series-* in tokens.css; Plotly needs literal colours.
export const SERIES_COLORS = { gasoline_95_e5_price: "#0453cd", diesel_a_price: "#c26a1b" };
// Cheaper / pricier than a reference (tokens --fp-support / --fp-error).
export const DELTA_COLORS = { cheaper: "#0e7b52", pricier: "#ba1a1a" };

// Several fuels' daily averages on one axis. The selected fuel is drawn heavier; every line ends in a
// direct label (name + latest price) so identity never depends on colour alone.
// series: [{ key, label, points: [{date, avg_price}], color?, dash?, endLabel? }]; color defaults to the
// fuel's SERIES_COLORS entry, endLabel (HTML) to label.
// range: optional [startISO, endISO] the x axis always spans (the selected period), even when the
// series start later; without it the axis autoranges to the data.
export function fuelPair(el, series, { selected = null, range = null } = {}) {
  const drawn = (series || []).filter((s) => s.points && s.points.length);
  if (!drawn.length) { el.innerHTML = emptyMsg("Sin datos para este periodo"); return; }
  el.innerHTML = "";
  const lasts = drawn.map((s) => s.points[s.points.length - 1].avg_price);
  const all = drawn.flatMap((s) => s.points.map((p) => p.avg_price));
  const span = Math.max(...all) - Math.min(...all) || 0.01;
  const traces = drawn.map((s) => {
    const on = !selected || s.key === selected;
    return {
      x: s.points.map((p) => p.date),
      y: s.points.map((p) => p.avg_price),
      name: s.label,
      mode: "lines",
      line: { color: s.color || SERIES_COLORS[s.key] || "#001642", width: on ? 3 : 1.75, dash: s.dash || "solid" },
      opacity: on ? 1 : 0.55,
      hovertemplate: "%{y:.3f} €/l<extra>%{fullData.name}</extra>",
    };
  });
  // Nudge the two end labels apart when the lines finish close together.
  const close = drawn.length === 2 && Math.abs(lasts[0] - lasts[1]) < span * 0.08;
  const annotations = drawn.map((s, i) => ({
    // Paper x keeps the label in the right margin and out of the x autorange.
    xref: "paper",
    x: 1,
    y: lasts[i],
    xanchor: "left",
    xshift: 8,
    yshift: close ? (lasts[i] >= lasts[1 - i] ? 16 : -16) : 0,
    showarrow: false,
    align: "left",
    text: `<b>${s.endLabel || s.label}</b><br>${lasts[i].toFixed(3).replace(".", ",")} €/l`,
    font: { size: 12, color: "#191c20" },
  }));
  _renderChart(
    el,
    traces,
    {
      ...COMMON_LAYOUT,
      margin: { l: 48, r: 100, t: 16, b: 40 },
      xaxis: {
        ...DATE_XAXIS,
        ...(range ? { range, autorange: false } : {}),
        showspikes: true,
        spikemode: "across",
        spikecolor: "#747782",
        spikethickness: 1,
        spikedash: "solid",
      },
      yaxis: { ...COMMON_LAYOUT.yaxis, tickformat: ".2f" },
      hovermode: "x unified",
      hoverlabel: { bgcolor: "#2e3135", bordercolor: "#2e3135", font: { color: "#eff0f6", family: "Inter, sans-serif" } },
      annotations,
    },
    CONFIG,
  );
}

// Within-week price gap per weekday (€/l): green bars below zero are cheaper days, red above are pricier.
// days: [{day_of_week, deviation}] (0 = Monday); labels: one short name per weekday.
export function weekdayBars(el, days, labels) {
  if (!days || !days.length) { el.innerHTML = emptyMsg("Sin datos suficientes"); return; }
  el.innerHTML = "";
  const y = days.map((d) => d.deviation);
  const fmt = (v) => {
    const r = Math.round(v * 1000) / 1000; // no sign on values that round to 0,000
    return `${r < 0 ? "−" : r > 0 ? "+" : ""}${Math.abs(r).toFixed(3).replace(".", ",")}`;
  };
  // Symmetric range with headroom so the value labels outside the bars never hit the day names.
  const reach = Math.max(...y.map(Math.abs), 0.001) * 1.45;
  _renderChart(
    el,
    [{
      type: "bar",
      x: days.map((d) => labels[d.day_of_week]),
      y,
      marker: { color: y.map((v) => (v < 0 ? DELTA_COLORS.cheaper : DELTA_COLORS.pricier)) },
      text: y.map(fmt),
      textposition: "outside",
      textfont: { size: 11 },
      constraintext: "none",
      cliponaxis: false,
      hovertemplate: "%{x}: %{text} €/l<extra></extra>",
    }],
    {
      ...COMMON_LAYOUT,
      margin: { l: 52, r: 8, t: 24, b: 32 },
      bargap: 0.35,
      yaxis: {
        ...COMMON_LAYOUT.yaxis,
        range: [-reach, reach],
        tickformat: ".3f",
        zeroline: true,
        zerolinecolor: "#747782",
        title: { text: "€/l", font: { size: 11 } },
      },
    },
    CONFIG,
  );
}

export function heatmap(el, x, y, z) {
  if (!z || !z.length) { el.innerHTML = emptyMsg("Sin datos"); return; }
  el.innerHTML = "";
  _renderChart(
    el,
    [{ x, y, z, type: "heatmap", colorscale: [[0, "#dae2ff"], [1, "#001642"]], showscale: false }],
    { ...COMMON_LAYOUT, margin: { l: 80, r: 16, t: 16, b: 40 } },
    CONFIG,
  );
}

export function emptyMsg(text) {
  return `<div class="flex items-center justify-center h-full py-12 text-outline text-sm">${text}</div>`;
}
