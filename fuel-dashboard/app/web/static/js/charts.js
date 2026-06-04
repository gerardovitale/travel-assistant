/* global Plotly */
// Thin Plotly wrappers with app-themed layouts.

const COMMON_LAYOUT = {
  font: { family: "Inter, sans-serif", size: 12, color: "#191c20" },
  paper_bgcolor: "rgba(0,0,0,0)",
  plot_bgcolor: "rgba(0,0,0,0)",
  margin: { l: 48, r: 16, t: 24, b: 44 },
  showlegend: false,
  xaxis: { gridcolor: "#e1e2e8", linecolor: "#c4c6d2", tickfont: { size: 11 } },
  yaxis: { gridcolor: "#e1e2e8", linecolor: "#c4c6d2", tickfont: { size: 11 } },
};
const CONFIG = { responsive: true, displaylogo: false, displayModeBar: false };

export function lineTrend(el, points, { label = "Precio" } = {}) {
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
  Plotly.newPlot(el, traces, { ...COMMON_LAYOUT, showlegend: true, legend: { orientation: "h", y: -0.2 } }, CONFIG);
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
  Plotly.newPlot(el, traces, { ...COMMON_LAYOUT, showlegend: true, legend: { orientation: "h", y: -0.2 } }, CONFIG);
}

export function horizontalBar(el, rows, { labelKey, valueKey, color = "#001642", maxRows = 15, colorFn = null, tickSuffix = "" } = {}) {
  if (!rows || !rows.length) { el.innerHTML = emptyMsg("Sin datos"); return; }
  el.innerHTML = "";
  const slice = rows.slice(0, maxRows);
  const y = slice.map((r) => r[labelKey]);
  const x = slice.map((r) => r[valueKey]);
  const markerColor = colorFn ? slice.map(colorFn) : color;
  Plotly.newPlot(
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

export function fuelByDistance(el, points, { floorPct = null } = {}) {
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

  Plotly.newPlot(
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

export function heatmap(el, x, y, z) {
  if (!z || !z.length) { el.innerHTML = emptyMsg("Sin datos"); return; }
  el.innerHTML = "";
  Plotly.newPlot(
    el,
    [{ x, y, z, type: "heatmap", colorscale: [[0, "#dae2ff"], [1, "#001642"]], showscale: false }],
    { ...COMMON_LAYOUT, margin: { l: 80, r: 16, t: 16, b: 40 } },
    CONFIG,
  );
}

export function emptyMsg(text) {
  return `<div class="flex items-center justify-center h-full py-12 text-outline text-sm">${text}</div>`;
}
