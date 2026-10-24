// Pure helpers that turn trend data into the sentences shown on /insights (no DOM, unit-tested).
import { displaySign, formatNum } from "./format.js";

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
