// Lightweight formatters (es-ES locale).
const euro = new Intl.NumberFormat("es-ES", { style: "currency", currency: "EUR", minimumFractionDigits: 3, maximumFractionDigits: 3 });
const euro2 = new Intl.NumberFormat("es-ES", { style: "currency", currency: "EUR", minimumFractionDigits: 2, maximumFractionDigits: 2 });
const num1 = new Intl.NumberFormat("es-ES", { minimumFractionDigits: 1, maximumFractionDigits: 1 });
const pct1 = new Intl.NumberFormat("es-ES", { style: "percent", minimumFractionDigits: 1, maximumFractionDigits: 1 });

export const formatPrice = (v) => (v == null || isNaN(v)) ? "—" : euro.format(v);
export const formatEur = (v) => (v == null || isNaN(v)) ? "—" : euro2.format(v);
export const formatKm = (v) => (v == null || isNaN(v)) ? "—" : `${num1.format(v)} km`;
export const formatMin = (v) => (v == null || isNaN(v)) ? "—" : `${Math.round(v)} min`;
export const formatPct = (v) => (v == null || isNaN(v)) ? "—" : pct1.format(v / 100);
export const formatDelta = (v) => {
  if (v == null || isNaN(v)) return "—";
  const sign = v > 0 ? "+" : v < 0 ? "−" : "";
  return `${sign}${Math.abs(v).toFixed(2)}%`;
};
export const formatDate = (iso) => {
  if (!iso) return "—";
  const d = new Date(iso);
  return d.toLocaleDateString("es-ES", { day: "2-digit", month: "short" });
};
export const escapeHtml = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
export const titleCase = (s) => String(s ?? "").toLowerCase().replace(/\b\w/g, (c) => c.toUpperCase());
