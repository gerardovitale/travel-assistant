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

// gap = Canarias, Ceuta and Melilla price minus the peninsula-and-Baleares price (€/l).
// "En Canarias, Ceuta y Melilla la gasolina 95 cuesta 0,175 €/l menos que en la península y Baleares".
export function regionGapHeadline(fuelType, gap) {
  const noun = FUEL_NOUN[fuelType] || "el combustible";
  if (gap == null) return `Península y Baleares vs. Canarias, Ceuta y Melilla: sin datos suficientes para ${noun}`;
  const sign = displaySign(gap, PRICE_DIGITS);
  if (sign === 0) return `En Canarias, Ceuta y Melilla ${noun} cuesta lo mismo que en la península y Baleares`;
  const word = sign < 0 ? "menos" : "más";
  return `En Canarias, Ceuta y Melilla ${noun} cuesta ${formatNum(Math.abs(gap), PRICE_DIGITS)} €/l ${word} que en la península y Baleares`;
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

// Gasoline or diesel over the years the car is kept, fuel only. gasoline100 / diesel100: € per 100 km at
// today's prices; premium: how much more the diesel version costs to buy (€); km: per year.
// tieMarginPct is the server's TIE_MARGIN_PCT (sent with the catalog): a gap smaller than that share of
// the gasoline fuel bill is a tie, which with no premium is exactly the server's per-100 km tie band.
export function fuelChoice({ gasoline100, diesel100, km, premium, years, tieMarginPct = 0 }) {
  const perKm = (gasoline100 - diesel100) / 100; // € diesel saves per km; negative when it costs more
  const band = (tieMarginPct / 100) * (gasoline100 / 100); // € per km still inside the tie band
  const yearly = perKm * km;
  const total = yearly * years - premium;
  const tie = Math.abs(total) < band * km * years;
  // Diesel wins once km × years × (perKm − band) ≥ premium; it never does if that edge is not positive.
  const edge = perKm - band;
  return {
    perKm,
    yearly,
    total,
    winner: tie ? "tie" : total > 0 ? "diesel" : "gasoline",
    // Rounded up to 500 km, so it agrees with the verdict at every slider step (multiples of 1000 km).
    breakevenKm: edge > 0 ? Math.ceil(premium / (years * edge) / 500) * 500 : null,
    paybackYears: yearly > 0 ? premium / yearly : null,
  };
}

// Verdict sentences for a fuelChoice result: "Con un diésel ahorras 157,60 € en 8 años".
export function fuelChoiceText(choice, { premium, years }) {
  const { winner, total, perKm, breakevenKm, paybackYears } = choice;
  const headline = {
    diesel: `Con un diésel ahorras ${formatEur(total)} en ${years} años`,
    gasoline: `La gasolina te sale ${formatEur(-total)} más barata en ${years} años`,
    tie: `Gasolina y diésel te cuestan prácticamente lo mismo en ${years} años`,
  }[winner];
  let detail;
  if (breakevenKm == null) {
    detail = perKm > 0
      ? "Con los precios de hoy, el diésel gasta casi lo mismo en combustible por kilómetro, así que no compensa con ningún kilometraje."
      : "Con los precios de hoy, el diésel no gasta menos en combustible por kilómetro, así que no compensa con ningún kilometraje.";
  } else if (premium <= 0) {
    detail = "Sin sobrecoste al comprarlo, el diésel ahorra desde el primer kilómetro.";
  } else {
    // The verdict nets two things the cost bars show separately: spell out both.
    const tradeOff = `En ${years} años el diésel gasta ${formatEur(choice.yearly * years)} menos en combustible, pero cuesta ${formatEur(premium)} más al comprarlo.`;
    // Rounded up, so a payback just past the years kept never reads as "8,0 años, más de los 8".
    const payback = Math.ceil(paybackYears * 10) / 10;
    const recover = payback <= years
      ? `Recuperas el sobrecoste en ${formatNum(payback, 1)} años`
      : `Recuperarías el sobrecoste en ${formatNum(payback, 1)} años, más de los ${years} que tendrás el coche`;
    detail = `${tradeOff} ${recover}; compensa a partir de unos ${formatNum(breakevenKm, 0)} km al año.`;
  }
  return { headline, detail };
}
