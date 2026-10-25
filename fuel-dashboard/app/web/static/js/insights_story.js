// Pure helpers that turn trend data into the sentences shown on /insights (no DOM, unit-tested).
import { displaySign, formatEur, formatNum } from "./format.js";

// Prices are always in euros (€/l, 3 decimals), never cents.
const PRICE_DIGITS = 3;

const FUEL_SUBJECT = { gasoline_95_e5_price: "La gasolina 95", diesel_a_price: "El diésel" };
const PERIOD_PHRASE = {
  week: "la última semana",
  month: "el último mes",
  quarter: "los últimos 3 meses",
  half_year: "los últimos 6 meses",
  year: "el último año",
};

// Last minus first average price (€/l) of a trend series, or null with fewer than two points.
export function priceChange(points) {
  if (!points || points.length < 2) return null;
  return points[points.length - 1].avg_price - points[0].avg_price;
}

// Latest price minus the price `days` calendar days earlier (closest point on or before that day).
export function changeOverDays(points, days) {
  if (!points || points.length < 2) return null;
  const last = points[points.length - 1];
  const cutoff = new Date(last.date).getTime() - days * 86400000;
  const earlier = [...points].reverse().find((p) => new Date(p.date).getTime() <= cutoff);
  return earlier ? last.avg_price - earlier.avg_price : null;
}

const FUEL_NOUN = { gasoline_95_e5_price: "la gasolina 95", diesel_a_price: "el diésel" };

// gap = islands-and-Ceuta/Melilla price minus peninsula price (€/l).
// "En las islas, Ceuta y Melilla la gasolina 95 cuesta 0,175 €/l menos que en la península".
export function regionGapHeadline(fuelType, gap) {
  const noun = FUEL_NOUN[fuelType] || "el combustible";
  if (gap == null) return `Península vs. islas, Ceuta y Melilla: sin datos suficientes para ${noun}`;
  const sign = displaySign(gap, PRICE_DIGITS);
  if (sign === 0) return `En las islas, Ceuta y Melilla ${noun} cuesta lo mismo que en la península`;
  const word = sign < 0 ? "menos" : "más";
  return `En las islas, Ceuta y Melilla ${noun} cuesta ${formatNum(Math.abs(gap), PRICE_DIGITS)} €/l ${word} que en la península`;
}

// First data day (ISO date) when a series starts after the period's start day, i.e. history is
// shorter than the period; else null. Point dates may carry a time ("2026-04-17 00:00:00").
export function coverageStart(points, startDate) {
  if (!points?.length || !startDate) return null;
  const first = points[0].date.slice(0, 10);
  return first > startDate ? first : null;
}

// "La gasolina 95 ha bajado 0,068 €/l en los últimos 3 meses". Changes that round to 0,000 €/l read as stable.
// since: a day label ("17 feb 2026") when the data covers only part of the period, so the sentence
// never claims a longer span than it measured ("… desde el 17 feb 2026").
export function trendHeadline(fuelType, period, change, { since = null } = {}) {
  const subject = FUEL_SUBJECT[fuelType] || "El precio";
  const when = since ? `desde el ${since}` : `en ${PERIOD_PHRASE[period] || "el periodo"}`;
  if (change == null) return `${subject}: sin datos suficientes ${when}`;
  const sign = displaySign(change, PRICE_DIGITS);
  if (sign === 0) return `${subject} se ha mantenido estable ${when}`;
  const verb = sign < 0 ? "ha bajado" : "ha subido";
  return `${subject} ${verb} ${formatNum(Math.abs(change), PRICE_DIGITS)} €/l ${when}`;
}

// day_of_week 0 = Monday, as the API sends it.
export const DAY_NAMES = ["lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo"];
export const DAY_SHORT = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"];

// Cheapest and priciest weekday from [{day_of_week, deviation}], with the gap between them (€/l).
export function bestAndWorstDay(days) {
  if (!days?.length) return null;
  const sorted = [...days].sort((a, b) => a.deviation - b.deviation);
  const best = sorted[0], worst = sorted[sorted.length - 1];
  return { best: best.day_of_week, worst: worst.day_of_week, gap: worst.deviation - best.deviation };
}

// "El lunes es el día más barato; el viernes, el más caro". A gap that rounds to 0,000 €/l reads as no difference.
export function weekdayHeadline(days) {
  const bw = bestAndWorstDay(days);
  if (!bw) return "Cuándo repostar: sin datos suficientes";
  if (displaySign(bw.gap, PRICE_DIGITS) === 0) return "El día de la semana apenas cambia el precio";
  return `El ${DAY_NAMES[bw.best]} es el día más barato; el ${DAY_NAMES[bw.worst]}, el más caro`;
}

// What the weekday gap means for one tank, in euros: "Con un depósito de 50 l, repostar el lunes en vez
// del viernes te ahorra 0,39 €".
export function tankSavingText(days, litres = 50) {
  const bw = bestAndWorstDay(days);
  if (!bw || displaySign(bw.gap, PRICE_DIGITS) === 0) return null;
  return `Con un depósito de ${litres} l, repostar el ${DAY_NAMES[bw.best]} en vez del ${DAY_NAMES[bw.worst]} te ahorra ${formatEur(bw.gap * litres)}`;
}

const REGIME_WORD = { cheap: "barato", normal: "en su nivel habitual", expensive: "caro" };

// Most likely price level tomorrow from the forecast's next_day_probabilities ({cheap, normal, expensive}):
// "Lo más probable mañana: que siga barato (63 %)".
export function tomorrowHeadline(currentRegime, probabilities) {
  const entries = Object.entries(probabilities || {}).filter(([k]) => k in REGIME_WORD);
  if (!entries.length) return null;
  const [regime, p] = entries.sort((a, b) => b[1] - a[1])[0];
  const verb = regime === currentRegime ? "que siga" : "que pase a estar";
  return `Lo más probable mañana: ${verb} ${REGIME_WORD[regime]} (${formatNum(p * 100, 0)} %)`;
}

// Brands from /brands/scorecard: [{brand, name, kind, stations, price_gap, below_market_pct, win_pct}],
// price_gap in €/l against the other stations of the brand's zip codes (negative = cheaper).

// The `limit` biggest chains of one kind ("all", "low_cost" or "traditional"), cheapest first. The
// cheapest brand and the cheapest low-cost brand are always kept, so the headline's brand is listed.
export function visibleBrands(brands, kind = "all", limit = 12) {
  const ofKind = (brands || []).filter((b) => kind === "all" || b.kind === kind);
  const biggest = [...ofKind].sort((a, b) => b.stations - a.stations).slice(0, limit);
  const pinned = [cheapestBrand(ofKind), cheapestBrand(ofKind.filter((b) => b.kind === "low_cost"))];
  const shown = new Set([...biggest, ...pinned.filter(Boolean)]);
  return [...shown].sort((a, b) => a.price_gap - b.price_gap);
}

// Brand with the lowest price gap, if it is actually cheaper than its zip codes.
export function cheapestBrand(brands) {
  const best = [...(brands || [])].sort((a, b) => a.price_gap - b.price_gap)[0];
  return best && displaySign(best.price_gap, PRICE_DIGITS) < 0 ? best : null;
}

// "Las low-cost cuestan hasta 0,190 €/l menos que la competencia de su código postal"; without a cheaper
// low-cost brand, the cheapest brand of any kind.
export function brandsHeadline(brands) {
  const lowCost = cheapestBrand((brands || []).filter((b) => b.kind === "low_cost"));
  if (lowCost) return `Las low-cost cuestan hasta ${formatNum(-lowCost.price_gap, PRICE_DIGITS)} €/l menos que la competencia de su código postal`;
  const best = cheapestBrand(brands);
  if (best) return `${best.name} es la marca más barata: ${formatNum(-best.price_gap, PRICE_DIGITS)} €/l menos que la competencia de su código postal`;
  return brands?.length ? "Ninguna marca es más barata que la competencia de su código postal" : "Marcas: sin datos suficientes en esta zona";
}

// What a brand's gap means in euros, for one 50 l tank and a year of 1.200 l.
export function brandSavings(priceGap, { tank = 50, year = 1200 } = {}) {
  const sign = displaySign(priceGap, PRICE_DIGITS);
  return { sign, tank: Math.abs(priceGap) * tank, year: Math.abs(priceGap) * year };
}
