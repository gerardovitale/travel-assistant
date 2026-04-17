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

export function horizontalBar(el, rows, { labelKey, valueKey, color = "#001642", maxRows = 15 } = {}) {
  if (!rows || !rows.length) { el.innerHTML = emptyMsg("Sin datos"); return; }
  el.innerHTML = "";
  const slice = rows.slice(0, maxRows);
  const y = slice.map((r) => r[labelKey]);
  const x = slice.map((r) => r[valueKey]);
  Plotly.newPlot(
    el,
    [{ x, y, type: "bar", orientation: "h", marker: { color } }],
    { ...COMMON_LAYOUT, margin: { l: 140, r: 16, t: 16, b: 32 }, yaxis: { ...COMMON_LAYOUT.yaxis, autorange: "reversed" } },
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
