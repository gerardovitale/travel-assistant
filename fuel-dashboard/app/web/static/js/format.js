// Lightweight formatters (es-ES locale).
const euro = new Intl.NumberFormat("es-ES", { style: "currency", currency: "EUR", minimumFractionDigits: 3, maximumFractionDigits: 3 });
const euro2 = new Intl.NumberFormat("es-ES", { style: "currency", currency: "EUR", minimumFractionDigits: 2, maximumFractionDigits: 2 });
const pct1 = new Intl.NumberFormat("es-ES", { style: "percent", minimumFractionDigits: 1, maximumFractionDigits: 1 });

const numFormats = new Map();
const numFormat = (digits) => {
  if (!numFormats.has(digits)) {
    numFormats.set(digits, new Intl.NumberFormat("es-ES", { minimumFractionDigits: digits, maximumFractionDigits: digits }));
  }
  return numFormats.get(digits);
};
const isNum = (v) => typeof v === "number" && !isNaN(v);

// Unit price per litre — every 3-decimal price in the app is €/l.
export const formatPrice = (v) => (v == null || isNaN(v)) ? "—" : `${euro.format(v)}/l`;
// Plain es-ES number (comma decimal), e.g. formatNum(48, 1) → "48,0".
export const formatNum = (v, digits = 1) => isNum(v) ? numFormat(digits).format(v) : "—";
// -1 / 0 / 1 for v as displayed with `digits` decimals: a value that rounds to zero is 0, so
// sign-driven styling (green/red, trend icons) agrees with the rendered number.
// Rounds the magnitude so halves go away from zero on both sides (-0.25 → -0.3, like 0.25 → 0.3).
const roundAbs = (v, digits) => Math.round(Math.abs(v) * 10 ** digits) / 10 ** digits;
export const displaySign = (v, digits = 1) => (!isNum(v) || roundAbs(v, digits) === 0 ? 0 : Math.sign(v));
// Signed percentage with a true minus sign, e.g. formatSignedPct(-2.4) → "−2,4%".
// The sign follows the rounded value, so -0.04 reads "0,0%" rather than "−0,0%".
export const formatSignedPct = (v, digits = 1) => {
  if (!isNum(v)) return "—";
  const s = displaySign(v, digits);
  const sign = s > 0 ? "+" : s < 0 ? "−" : "";
  return `${sign}${numFormat(digits).format(roundAbs(v, digits))}%`;
};
export const formatEur = (v) => (v == null || isNaN(v)) ? "—" : euro2.format(v);
export const formatKm = (v) => (v == null || isNaN(v)) ? "—" : `${numFormat(1).format(v)} km`;
export const formatMin = (v) => {
  if (v == null || typeof v !== "number" || isNaN(v)) return "—";
  const totalMin = Math.round(v);
  if (totalMin < 60) return `${totalMin} min`;
  const h = Math.floor(totalMin / 60);
  const m = totalMin % 60;
  return m === 0 ? `${h}h` : `${h}h ${m}min`;
};
export const formatPct = (v) => (v == null || isNaN(v)) ? "—" : pct1.format(v / 100);
export const formatDelta = (v) => formatSignedPct(v, 2);
export const formatDate = (iso) => {
  if (!iso) return "—";
  const d = new Date(iso);
  return d.toLocaleDateString("es-ES", { day: "2-digit", month: "short" });
};
// Date-only strings ("2026-04-17") are calendar days, not UTC instants: parse them as local
// midnight so they never shift to the previous day west of UTC.
const parseDataDate = (raw) => {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(raw);
  return m ? new Date(+m[1], +m[2] - 1, +m[3]) : new Date(raw);
};
const dayLabel = (d, now) => {
  const startOf = (x) => new Date(x.getFullYear(), x.getMonth(), x.getDate()).getTime();
  const diffDays = Math.round((startOf(now) - startOf(d)) / 86400000);
  // -1 too: a timestamp up to a day ahead of the browser clock (skew, timezone) is still "today".
  if (diffDays === 0 || diffDays === -1) return "hoy";
  if (diffDays === 1) return "ayer";
  const opts = { day: "numeric", month: "short" };
  if (d.getFullYear() !== now.getFullYear()) opts.year = "numeric";
  return d.toLocaleDateString("es-ES", opts).replace(".", "");
};
const withDe = (day) => (day === "hoy" || day === "ayer" ? `de ${day}` : `del ${day}`);

// Top-bar data freshness text, e.g. "Tiempo real · hoy 09:30", "Datos de ayer",
// "Datos del 14 abr (sin actualizar)". Returns null when a healthy response carries no usable
// date (the API sends "unknown"), so the caller can show it as unavailable rather than OK.
export const formatFreshness = (raw, { realtime = false, stale = false, now = new Date() } = {}) => {
  const d = raw ? parseDataDate(raw) : null;
  if (!d || isNaN(d)) return stale ? "Datos no actualizados" : null;
  const day = dayLabel(d, now);
  if (stale) return `Datos ${withDe(day)} (sin actualizar)`;
  if (realtime && raw.includes("T")) {
    const time = d.toLocaleTimeString("es-ES", { hour: "2-digit", minute: "2-digit" });
    return `Tiempo real · ${day} ${time}`;
  }
  return `Datos ${withDe(day)}`;
};
export const escapeHtml = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
// Unicode-aware (\b\w would capitalise after "ñ"/"ó": "león" → "LeóN"). Matches Python's
// str.title(), which the API uses for province display names.
export const titleCase = (s) =>
  String(s ?? "").toLowerCase().replace(/(^|[^\p{L}])(\p{L})/gu, (_, pre, c) => pre + c.toUpperCase());
