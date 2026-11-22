// Redesigned /insights: one scrolling story driven by the hero's fuel + scope filters.
import { api, qs, toast } from "./app.js";
import { fuelPair, loadingSkeleton, SERIES_COLORS, weekdayBars } from "./charts.js";
import { displaySign, escapeHtml, formatDay, formatEur, formatNum, formatSignedNum, formatSignedPriceDelta } from "./format.js";
import { getProvinces } from "./fuel.js";
import {
  bestAndWorstDay,
  brandSavings,
  brandsHeadline,
  cheapestBrand,
  changeOverDays,
  coverageStart,
  DAY_NAMES,
  DAY_SHORT,
  fuelChoice,
  fuelChoiceText,
  priceChange,
  regionGapHeadline,
  tankSavingText,
  tomorrowHeadline,
  trendHeadline,
  visibleBrands,
  weekdayHeadline,
} from "./insights_story.js";

const FUELS = [
  { key: "gasoline_95_e5_price", label: "Gasolina 95" },
  { key: "diesel_a_price", label: "Diésel" },
];
// Scope select values that are not provinces. Province values are the raw /provinces keys.
// Scopes follow the fuel-tax regimes; Baleares pays the same taxes as the peninsula.
const REGION_SCOPES = {
  peninsula: "Península y Baleares",
  special: "Canarias, Ceuta y Melilla",
  compare: "Península y Baleares vs. Canarias, Ceuta y Melilla",
};

const filters = document.getElementById("insights-filters");
const scopeSelect = filters.querySelector('select[name="scope"]');
const params = new URLSearchParams(location.search);
const state = {
  fuel: params.get("fuel") || filters.dataset.defaultFuel,
  period: params.get("period") || filters.dataset.defaultPeriod,
  scope: params.get("scope") || filters.dataset.defaultScope,
  // main: { [fuelKey]: points } for the scope (the peninsula when comparing); special: the same for
  // Canarias, Ceuta and Melilla, only loaded when comparing. window: the period's {start, end} days.
  trend: { main: {}, special: null, window: null },
  // "Cuándo repostar" for the main scope, per fuel: weekday gaps and the 7-day forecast. While a new
  // scope loads, nothing from the previous one is shown.
  when: { days: {}, forecast: {}, loading: true },
  // "Marcas" for the main scope: { [fuelKey]: brands } from /brands/scorecard, the kind filter and
  // the brand whose card is open (null = the cheapest one shown).
  brands: { fuels: {}, kind: "all", selected: null, expanded: false, loading: true, unavailable: false },
  // "Gasolina o diésel": the catalog's pairs sold new today, today's prices for the main scope and the
  // calculator's inputs. The premium starts at the pair's list-price gap. Inputs only re-render; a new
  // scope refetches the prices.
  fuelChoice: { pairs: [], pair: null, tieMarginPct: 0, prices: null, loading: true, km: 15000, premium: 0, years: 8 },
};
let trendRequest = 0;
let whenRequest = 0;
let brandsRequest = 0;
let fuelChoiceRequest = 0;
let fuelChoicePairsRequest = null;
// Absent when the fuel-type report is switched off.
const fuelChoiceSection = document.getElementById("combustible");

const isCompare = () => state.scope === "compare";

function syncToggle(name, value) {
  for (const btn of document.querySelectorAll(`[data-toggle-group="${name}"] [data-value]`)) {
    btn.setAttribute("aria-pressed", String(btn.dataset.value === value));
  }
}

// Shareable state: ?fuel=&period=&scope= (defaults are left out to keep URLs clean).
function syncUrl() {
  const q = qs({
    fuel: state.fuel === filters.dataset.defaultFuel ? null : state.fuel,
    period: state.period === filters.dataset.defaultPeriod ? null : state.period,
    scope: state.scope === filters.dataset.defaultScope ? null : state.scope,
  });
  history.replaceState(null, "", q ? `${location.pathname}?${q}${location.hash}` : `${location.pathname}${location.hash}`);
}

function scopeLabel() {
  return REGION_SCOPES[state.scope] || scopeSelect.selectedOptions[0]?.textContent || state.scope;
}

// Query params for the main series: a region, or a single province.
function mainScopeParams() {
  if (state.scope === "compare") return { region: "peninsula" };
  if (state.scope in REGION_SCOPES) return { region: state.scope };
  return { province: state.scope };
}

const lastPrice = (points) => (points?.length ? points[points.length - 1].avg_price : null);

function renderKpis() {
  const kpiScope = isCompare() ? REGION_SCOPES.peninsula : scopeLabel();
  for (const tile of document.querySelectorAll("[data-kpi-price]")) {
    const fuel = tile.dataset.kpiPrice;
    tile.classList.toggle("hidden", fuel !== state.fuel);
    const points = state.trend.main[fuel];
    if (!points?.length) continue;
    tile.querySelector("[data-kpi-value]").textContent = formatNum(lastPrice(points), 3);
    const week = changeOverDays(points, 7);
    const delta = tile.querySelector("[data-kpi-delta]");
    const sign = displaySign(week, 3);
    delta.textContent = week == null ? kpiScope : `${formatSignedPriceDelta(week)} vs hace 7 días · ${kpiScope}`;
    delta.className = sign < 0 ? "text-xs fp-delta--down" : sign > 0 ? "text-xs fp-delta--up" : "text-xs text-on-surface-variant";
  }
}

// Fuel view: both fuels for one scope. Compare view: the selected fuel, peninsula and Baleares vs. Canarias, Ceuta and Melilla.
function trendSeries() {
  const { main, special } = state.trend;
  if (!isCompare()) return FUELS.map((f) => ({ ...f, points: main[f.key] || [] }));
  const color = SERIES_COLORS[state.fuel];
  return [
    { key: "peninsula", label: REGION_SCOPES.peninsula, color, points: main[state.fuel] || [] },
    { key: "special", label: REGION_SCOPES.special, endLabel: "Canarias, Ceuta<br>y Melilla", color, dash: "dash", points: special?.[state.fuel] || [] },
  ];
}

function renderLegend(series) {
  document.getElementById("trend-legend").innerHTML = series
    .map((s) => {
      const color = s.color || SERIES_COLORS[s.key];
      const line = s.dash ? `border-top: 3px dashed ${color}` : `background: ${color}`;
      return `<li class="inline-flex items-center gap-2"><span class="inline-block w-5 h-[3px] rounded" style="${line}" aria-hidden="true"></span>${escapeHtml(s.label)}</li>`;
    })
    .join("");
}

// Day the shown data starts when history is shorter than the selected period, else null.
function trendCoverageStart() {
  const { main, special, window } = state.trend;
  const firsts = [main[state.fuel], isCompare() ? special?.[state.fuel] : null]
    .map((points) => coverageStart(points, window?.start))
    .filter(Boolean);
  return firsts.length ? firsts.sort().at(-1) : null;
}

function renderCoverage(since) {
  const note = document.getElementById("trend-coverage");
  note.classList.toggle("hidden", !since);
  if (since) note.querySelector("[data-coverage-text]").textContent = `Solo hay datos desde el ${formatDay(since)}; el periodo empieza el ${formatDay(state.trend.window.start)}.`;
}

function renderTrend() {
  const series = trendSeries();
  const { window } = state.trend;
  fuelPair(document.getElementById("trend-pair-chart"), series, {
    selected: isCompare() ? null : state.fuel,
    range: window ? [window.start, window.end] : null,
  });
  const since = trendCoverageStart();
  renderCoverage(since);
  renderLegend(series);
  document.getElementById("trend-scope").textContent = scopeLabel();
  const headline = document.getElementById("trend-headline");
  const spread = document.getElementById("trend-spread");
  const spreadLabel = document.getElementById("trend-spread-label");
  const { main, special } = state.trend;
  if (isCompare()) {
    const peninsula = lastPrice(main[state.fuel]), islands = lastPrice(special?.[state.fuel]);
    const gap = peninsula != null && islands != null ? islands - peninsula : null;
    headline.textContent = regionGapHeadline(state.fuel, gap);
    spreadLabel.textContent = "Diferencia con Canarias, Ceuta y Melilla:";
    spread.textContent = gap == null ? "—" : formatSignedPriceDelta(-gap).replace("+", "");
  } else {
    headline.textContent = trendHeadline(state.fuel, state.period, priceChange(main[state.fuel]), { since: since && formatDay(since) });
    spreadLabel.textContent = "Diferencia gasolina − diésel:";
    const gas = lastPrice(main.gasoline_95_e5_price), diesel = lastPrice(main.diesel_a_price);
    spread.textContent = gas != null && diesel != null ? formatSignedPriceDelta(gas - diesel).replace("+", "") : "—";
  }
}

const capitalize = (s) => s.charAt(0).toUpperCase() + s.slice(1);

const REGIME_NOW = { cheap: "Precios bajos ahora", normal: "Precios habituales", expensive: "Precios altos" };

// "Precios altos · 60 % de que bajen en 7 días"; when prices are already low there is nothing cheaper to wait for.
function forecastNote(forecast) {
  if (!forecast || forecast.insufficient_data) return "Previsión a 7 días";
  const now = REGIME_NOW[forecast.current_regime] || "Previsión a 7 días";
  if (forecast.current_regime === "cheap") return now;
  return `${now} · ${formatNum(forecast.cheaper_within_7d * 100, 0)} % de que bajen en 7 días`;
}

function renderWhenKpis() {
  if (state.when.loading) {
    for (const sel of ["[data-kpi-best-day]", "[data-kpi-forecast]"]) document.querySelector(sel).textContent = "—";
    document.querySelector("[data-kpi-best-day-note]").textContent = "Cargando…";
    document.querySelector("[data-kpi-forecast-note]").textContent = "Cargando…";
    return;
  }
  const bw = bestAndWorstDay(state.when.days[state.fuel]);
  const clear = bw && displaySign(bw.gap, 3) !== 0;
  document.querySelector("[data-kpi-best-day]").textContent = clear ? capitalize(DAY_NAMES[bw.best]) : "—";
  document.querySelector("[data-kpi-best-day-note]").textContent = !bw
    ? "Sin datos suficientes"
    : clear
      ? `${formatNum(bw.gap, 3)} €/l menos que el ${DAY_NAMES[bw.worst]}`
      : "Sin un día claramente más barato";
  const forecast = state.when.forecast[state.fuel];
  document.querySelector("[data-kpi-forecast]").textContent = forecast?.recommendation || "—";
  document.querySelector("[data-kpi-forecast-note]").textContent = forecastNote(forecast);
}

function renderWhen() {
  document.getElementById("when-scope").textContent = isCompare() ? REGION_SCOPES.peninsula : scopeLabel();
  if (state.when.loading) {
    document.getElementById("weekday-chart").innerHTML = loadingSkeleton();
    document.getElementById("when-headline").textContent = "¿Qué día sale más barato?";
    document.getElementById("when-saving").classList.add("hidden");
    document.getElementById("forecast-recommendation").textContent = "—";
    document.getElementById("forecast-explanation").textContent = "";
    document.getElementById("forecast-tomorrow").classList.add("hidden");
    return;
  }
  const days = state.when.days[state.fuel];
  weekdayBars(document.getElementById("weekday-chart"), days, DAY_SHORT);
  document.getElementById("when-headline").textContent = weekdayHeadline(days);
  const saving = tankSavingText(days);
  const savingEl = document.getElementById("when-saving");
  savingEl.classList.toggle("hidden", !saving);
  savingEl.querySelector("[data-saving-text]").textContent = saving || "";

  const forecast = state.when.forecast[state.fuel];
  document.getElementById("forecast-recommendation").textContent = forecast?.recommendation || "—";
  document.getElementById("forecast-explanation").textContent = forecast?.explanation || "";
  const tomorrow = document.getElementById("forecast-tomorrow");
  const probs = forecast && !forecast.insufficient_data ? forecast.next_day_probabilities : null;
  const headline = probs && tomorrowHeadline(forecast.current_regime, probs);
  tomorrow.classList.toggle("hidden", !headline);
  if (!headline) return;
  tomorrow.querySelector("[data-tomorrow-headline]").textContent = headline;
  for (const regime of ["cheap", "normal", "expensive"]) {
    const p = probs[regime] || 0;
    tomorrow.querySelector(`[data-tomorrow-bar="${regime}"]`).style.width = `${p * 100}%`;
    tomorrow.querySelector(`[data-tomorrow-pct="${regime}"]`).textContent = `${formatNum(p * 100, 0)} %`;
  }
}

const BRAND_KIND = { low_cost: "Low-cost", traditional: "Tradicional" };
const BRANDS_LISTED = 12;

function renderBrandKpi() {
  const value = document.querySelector("[data-kpi-brand]");
  const note = document.querySelector("[data-kpi-brand-note]");
  if (state.brands.loading) {
    value.textContent = "—";
    note.textContent = "Cargando…";
    return;
  }
  const best = cheapestBrand(state.brands.fuels[state.fuel]);
  value.textContent = best ? best.name : "—";
  note.textContent = best
    ? `${formatNum(-best.price_gap, 3)} €/l menos que su competencia · gana el ${formatNum(best.win_pct, 0)} % de los días`
    : "Sin datos suficientes";
}

function brandRow(b, reach, selected) {
  const width = (Math.abs(b.price_gap) / reach) * 50;
  const sign = displaySign(b.price_gap, 3);
  const cheaper = sign < 0;
  const bar = sign === 0 ? "" : `<span class="fp-brand-row__bar fp-brand-row__bar--${cheaper ? "cheaper" : "pricier"}" style="left: ${cheaper ? 50 - width : 50}%; width: ${width}%"></span>`;
  return `<button type="button" class="fp-brand-row" data-brand="${escapeHtml(b.brand)}" aria-pressed="${b.brand === selected}"
      aria-label="${escapeHtml(`${b.name}: ${formatSignedPriceDelta(b.price_gap)} frente a su competencia, la más barata el ${formatNum(b.win_pct, 0)} % de los días`)}">
    <span class="fp-brand-row__name">${escapeHtml(b.name)}</span>
    <span class="fp-brand-row__track">${bar}</span>
    <span class="fp-brand-row__num">${formatSignedNum(b.price_gap, 3)}</span>
    <span class="fp-brand-row__num fp-brand-row__win"><span class="fp-brand-row__meter"><span style="width: ${b.win_pct}%"></span></span>${formatNum(b.win_pct, 0)} %</span>
    <span class="fp-brand-row__stations">${formatNum(b.stations, 0)}</span>
  </button>`;
}

function renderBrandCard(b) {
  const card = document.getElementById("brand-card");
  const set = (sel, text) => (card.querySelector(sel).textContent = text);
  if (!b) {
    for (const sel of ["[data-brand-name]", "[data-brand-gap]", "[data-brand-below]", "[data-brand-tank]"]) set(sel, "—");
    set("[data-brand-meta]", state.brands.loading ? "Cargando…" : "");
    set("[data-brand-tank-label]", "Por depósito de 50 l");
    set("[data-brand-year]", "");
    return;
  }
  const savings = brandSavings(b.price_gap);
  set("[data-brand-name]", b.name);
  set("[data-brand-meta]", `${BRAND_KIND[b.kind]} · ${formatNum(b.stations, 0)} estaciones`);
  set("[data-brand-gap]", formatSignedPriceDelta(b.price_gap));
  set("[data-brand-below]", `${formatNum(b.below_market_pct, 0)} %`);
  set("[data-brand-tank-label]", savings.sign < 0 ? "Ahorro por depósito de 50 l" : savings.sign > 0 ? "Sobrecoste por depósito de 50 l" : "Por depósito de 50 l");
  set("[data-brand-tank]", savings.sign === 0 ? formatEur(0) : formatEur(savings.tank));
  set("[data-brand-year]", savings.sign === 0 ? "Cuesta lo mismo que su competencia" : `Al año, con 1.200 l: ${formatEur(savings.year)}`);
}

function renderBrands() {
  document.getElementById("brands-scope").textContent = isCompare() ? REGION_SCOPES.peninsula : scopeLabel();
  const rows = document.getElementById("brand-rows");
  if (state.brands.loading) {
    document.getElementById("brands-more").classList.add("hidden");
    document.getElementById("brands-headline").textContent = "¿Qué marcas salen más baratas?";
    rows.innerHTML = `<div class="h-96">${loadingSkeleton()}</div>`;
    renderBrandCard(null);
    return;
  }
  const all = state.brands.fuels[state.fuel] || [];
  document.getElementById("brands-headline").textContent = state.brands.unavailable
    ? "Las marcas no están disponibles ahora mismo"
    : brandsHeadline(all);
  const { kind, expanded } = state.brands;
  const short = visibleBrands(all, kind, BRANDS_LISTED);
  const shown = expanded ? visibleBrands(all, kind, Infinity) : short;
  const total = all.filter((b) => kind === "all" || b.kind === kind).length;
  const more = document.getElementById("brands-more");
  // Pinned cheapest brands can already make the short list the whole list.
  more.classList.toggle("hidden", short.length >= total);
  more.setAttribute("aria-expanded", String(expanded));
  more.textContent = expanded ? "Ver solo las principales" : `Ver las ${total} marcas`;
  const selected = shown.find((b) => b.brand === state.brands.selected) || shown[0] || null;
  // Symmetric scale: the longest bar fills its half of the track.
  const reach = Math.max(...shown.map((b) => Math.abs(b.price_gap)), 0.001) * 1.05;
  rows.innerHTML = shown.length
    ? shown.map((b) => brandRow(b, reach, selected?.brand)).join("")
    : `<p class="py-6 text-sm text-on-surface-variant">${state.brands.unavailable ? "Vuelve a intentarlo en unos minutos." : "No hay marcas con suficientes estaciones en esta zona."}</p>`;
  renderBrandCard(selected);
}

const FUEL_CHOICE_TAG = {
  diesel: ["check_circle", "Compensa el diésel"],
  gasoline: ["info", "Compensa la gasolina"],
  tie: ["balance", "Empate"],
};

const fuelChoicePair = () => state.fuelChoice.pairs.find((p) => p.pair_id === state.fuelChoice.pair) || null;
const pairVehicle = (pair, energy) => pair?.vehicles.find((v) => v.energy_type === energy) || null;

function renderFuelChoiceInputs() {
  const fc = state.fuelChoice;
  const labels = { km: `${formatNum(fc.km, 0)} km`, premium: `${formatNum(fc.premium, 0)} €`, years: `${fc.years} años` };
  for (const [name, text] of Object.entries(labels)) {
    fuelChoiceSection.querySelector(`[data-range-value="${name}"]`).textContent = text;
    fuelChoiceSection.querySelector(`input[name="${name}"]`).value = String(fc[name]);
  }
  const pair = fuelChoicePair();
  const gasoline = pairVehicle(pair, "gasoline"), diesel = pairVehicle(pair, "diesel");
  document.getElementById("fuel-choice-consumption").textContent = pair
    ? `Consumo homologado: gasolina ${formatNum(gasoline.consumption, 1)} · diésel ${formatNum(diesel.consumption, 1)} l/100 km`
    : "Consumo homologado: —";
  const premium = pair?.diesel_premium;
  const source = document.getElementById("fuel-choice-premium-source");
  source.classList.toggle("hidden", !premium);
  if (!premium) return;
  source.querySelector("[data-premium-text]").textContent =
    `Precio oficial consultado el ${formatDay(premium.checked_on)}: diésel ${formatNum(premium.diesel_list_price, 0)} €, gasolina ${formatNum(premium.gasoline_list_price, 0)} €. Cámbialo si tu oferta es distinta.`;
  const links = premium.source_urls.filter((url) => url.startsWith("https://"));
  source.querySelector("[data-premium-links]").replaceChildren(
    ...links.map((url, i) =>
      Object.assign(document.createElement("a"), {
        href: url,
        target: "_blank",
        rel: "noopener",
        className: "ml-1 underline font-semibold",
        textContent: links.length > 1 ? `Fuente ${i + 1}` : "Fuente",
      }),
    ),
  );
}

// A value as its slider can show it: inside the slider's range, on one of its steps.
function onSlider(name, value) {
  const input = fuelChoiceSection.querySelector(`input[name="${name}"]`);
  const min = Number(input.min), max = Number(input.max), step = Number(input.step);
  return Math.min(max, Math.max(min, min + Math.round((value - min) / step) * step));
}

// A new model brings its own list-price premium.
function selectFuelChoicePair(pairId) {
  const pair = state.fuelChoice.pairs.find((p) => p.pair_id === pairId);
  state.fuelChoice.pair = pair?.pair_id ?? null;
  state.fuelChoice.premium = onSlider("premium", pair ? pair.diesel_premium.eur : 0);
}

function renderFuelChoice() {
  if (!fuelChoiceSection) return;
  const fc = state.fuelChoice;
  document.getElementById("fuel-choice-scope").textContent = isCompare() ? REGION_SCOPES.peninsula : scopeLabel();
  renderFuelChoiceInputs();
  const tag = document.getElementById("fuel-choice-tag");
  const verdict = document.getElementById("fuel-choice-verdict");
  const detail = document.getElementById("fuel-choice-detail");
  const pair = fuelChoicePair();
  const gasoline = pairVehicle(pair, "gasoline"), diesel = pairVehicle(pair, "diesel");
  const ready = !fc.loading && fc.prices && gasoline && diesel;
  const costs = ready
    ? { gasoline: gasoline.consumption * fc.prices.gasoline, diesel: diesel.consumption * fc.prices.diesel }
    : null;
  const top = costs ? Math.max(costs.gasoline, costs.diesel) : 0;
  for (const key of ["gasoline", "diesel"]) {
    fuelChoiceSection.querySelector(`[data-cost-bar="${key}"]`).style.width = costs ? `${(costs[key] / top) * 100}%` : "0";
    fuelChoiceSection.querySelector(`[data-cost-value="${key}"]`).textContent = costs ? formatEur(costs[key]) : "—";
  }
  const prices = document.getElementById("fuel-choice-prices");
  const yearly = fuelChoiceSection.querySelector('[data-testid="fuel-choice-yearly"]');
  const yearlyLabel = fuelChoiceSection.querySelector("[data-yearly-label]");
  const breakeven = fuelChoiceSection.querySelector('[data-testid="fuel-choice-breakeven"]');
  if (!costs) {
    tag.className = "fp-verdict-tag self-start";
    tag.textContent = "—";
    verdict.textContent = fc.loading ? "—" : "La calculadora no está disponible ahora mismo";
    detail.textContent = fc.loading ? "Cargando…" : "Vuelve a intentarlo en unos minutos.";
    prices.textContent = "";
    yearlyLabel.textContent = "Ahorro en combustible / año";
    yearly.textContent = "—";
    breakeven.textContent = "—";
    return;
  }
  const inputs = { km: fc.km, premium: fc.premium, years: fc.years };
  const choice = fuelChoice({ gasoline100: costs.gasoline, diesel100: costs.diesel, tieMarginPct: fc.tieMarginPct, ...inputs });
  const text = fuelChoiceText(choice, inputs);
  const [icon, label] = FUEL_CHOICE_TAG[choice.winner];
  tag.className = `fp-verdict-tag fp-verdict-tag--${choice.winner} self-start`;
  tag.innerHTML = `<span class="material-symbols-outlined text-[16px]" aria-hidden="true">${icon}</span>${label}`;
  verdict.textContent = text.headline;
  detail.textContent = text.detail;
  prices.textContent = `Precios del ${formatDay(fc.prices.date)}: gasolina 95 a ${formatNum(fc.prices.gasoline, 3)} €/l y diésel a ${formatNum(fc.prices.diesel, 3)} €/l.`;
  yearlyLabel.textContent = choice.yearly < 0 ? "Sobrecoste en combustible / año" : "Ahorro en combustible / año";
  yearly.textContent = formatEur(Math.abs(choice.yearly));
  breakeven.textContent = choice.breakevenKm == null ? "Nunca" : choice.breakevenKm === 0 ? "Siempre" : `${formatNum(choice.breakevenKm, 0)} km/año`;
}

function render() {
  renderKpis();
  renderTrend();
  renderWhenKpis();
  renderWhen();
  renderBrandKpi();
  renderBrands();
  renderFuelChoice();
}

const fetchTrend = (fuel, scopeParams) =>
  api(`/trends/price?${qs({ fuel_type: fuel, period: state.period, ...scopeParams })}`, { signal: AbortSignal.timeout(15000) });

// { series: { [fuelKey]: points }, window: { start, end } | null } for one scope.
async function fetchScope(scopeParams) {
  const responses = await Promise.all(FUELS.map((f) => fetchTrend(f.key, scopeParams)));
  const { start_date: start, end_date: end } = responses[0];
  return { series: Object.fromEntries(FUELS.map((f, i) => [f.key, responses[i].trend])), window: start && end ? { start, end } : null };
}

async function loadTrend() {
  const req = ++trendRequest;
  document.getElementById("trend-pair-chart").innerHTML = loadingSkeleton();
  try {
    const [main, special] = await Promise.all([fetchScope(mainScopeParams()), isCompare() ? fetchScope({ region: "special" }) : null]);
    if (req !== trendRequest) return; // a newer filter change already superseded this load
    state.trend = { main: main.series, special: special?.series ?? null, window: main.window };
  } catch (err) {
    if (req !== trendRequest) return;
    state.trend = { main: {}, special: null, window: null };
    toast(`No se pudo cargar la tendencia: ${err.message}`, "error");
  }
  renderKpis();
  renderTrend();
}

// One request for both fuels, so the fuel toggle never waits on the network.
async function loadWhen() {
  const req = ++whenRequest;
  state.when = { days: {}, forecast: {}, loading: true };
  renderWhenKpis();
  renderWhen();
  let fuels = {};
  try {
    const query = qs({ fuel_type: FUELS.map((f) => f.key), ...mainScopeParams() });
    ({ fuels } = await api(`/trends/refuel-timing?${query}`, { signal: AbortSignal.timeout(15000) }));
  } catch (err) {
    if (req !== whenRequest) return;
    toast(`No se pudo cargar cuándo repostar: ${err.message}`, "error");
  }
  if (req !== whenRequest) return; // a newer filter change already superseded this load
  state.when = {
    days: Object.fromEntries(FUELS.map((f) => [f.key, fuels[f.key]?.days])),
    forecast: Object.fromEntries(FUELS.map((f) => [f.key, fuels[f.key]?.forecast])),
    loading: false,
  };
  renderWhenKpis();
  renderWhen();
}

// One request for both fuels, like loadWhen.
async function loadBrands() {
  const req = ++brandsRequest;
  state.brands = { ...state.brands, fuels: {}, loading: true, unavailable: false };
  renderBrandKpi();
  renderBrands();
  let fuels = {};
  let unavailable = false;
  try {
    const query = qs({ fuel_type: FUELS.map((f) => f.key), ...mainScopeParams() });
    ({ fuels } = await api(`/brands/scorecard?${query}`, { signal: AbortSignal.timeout(15000) }));
  } catch (err) {
    if (req !== brandsRequest) return;
    unavailable = true;
    // 404: the report is not published yet (first deploy) — the headline says so, no error toast.
    if (err.status !== 404) toast(`No se pudieron cargar las marcas: ${err.message}`, "error");
  }
  if (req !== brandsRequest) return; // a newer filter change already superseded this load
  state.brands = { ...state.brands, fuels, loading: false, unavailable };
  renderBrandKpi();
  renderBrands();
}

// The model picker: gasoline/diesel pairs grouped by segment. It does not depend on the scope.
// Only pairs with a list-price premium: the question is which one to buy, so both must be sold new.
async function loadFuelChoicePairs() {
  const { pairs, tie_margin_pct: tieMarginPct } = await api("/reportes/fuel-type/vehicles", { signal: AbortSignal.timeout(15000) });
  state.fuelChoice.tieMarginPct = tieMarginPct;
  const comparable = pairs.filter((p) => pairVehicle(p, "gasoline") && pairVehicle(p, "diesel") && p.diesel_premium);
  const select = document.getElementById("fuel-choice-pair");
  const segments = new Map();
  for (const p of comparable) {
    if (!segments.has(p.segment_label)) segments.set(p.segment_label, select.appendChild(Object.assign(document.createElement("optgroup"), { label: p.segment_label })));
    segments.get(p.segment_label).append(new Option(p.model, p.pair_id));
  }
  const fallback = fuelChoiceSection.dataset.defaultPair;
  state.fuelChoice.pairs = comparable;
  selectFuelChoicePair(comparable.some((p) => p.pair_id === fallback) ? fallback : comparable[0]?.pair_id);
  select.value = state.fuelChoice.pair ?? "";
}

// Loaded once; a failed load is tried again on the next zone change instead of being kept.
function fuelChoicePairs() {
  fuelChoicePairsRequest ??= loadFuelChoicePairs().catch((err) => {
    fuelChoicePairsRequest = null;
    throw err;
  });
  return fuelChoicePairsRequest;
}

// Today's gasoline and diesel prices for the main scope, both from the same day. Every model is priced
// with them, so switching models never refetches.
async function loadFuelChoice() {
  if (!fuelChoiceSection) return;
  const req = ++fuelChoiceRequest;
  state.fuelChoice = { ...state.fuelChoice, prices: null, loading: true };
  renderFuelChoice();
  let prices = null;
  try {
    await fuelChoicePairs();
    if (!state.fuelChoice.pair) throw Object.assign(new Error("sin coches en el catálogo"), { status: 404 });
    const query = qs({ pair_id: state.fuelChoice.pair, ...mainScopeParams() });
    const r = await api(`/reportes/fuel-type/breakeven?${query}`, { signal: AbortSignal.timeout(15000) });
    prices = { gasoline: r.gasoline.price_per_unit, diesel: r.diesel.price_per_unit, date: r.price_date };
  } catch (err) {
    if (req !== fuelChoiceRequest) return;
    // 404: no prices for this scope yet — the card says so, no error toast.
    if (err.status !== 404) toast(`No se pudo cargar la calculadora: ${err.message}`, "error");
  }
  if (req !== fuelChoiceRequest) return; // a newer filter change already superseded this load
  state.fuelChoice = { ...state.fuelChoice, prices, loading: false };
  renderFuelChoice();
}

function onToggle(name, apply) {
  document.querySelector(`[data-toggle-group="${name}"]`)?.addEventListener("click", (e) => {
    const btn = e.target.closest("[data-value]");
    if (!btn) return;
    apply(btn.dataset.value);
    syncToggle(name, btn.dataset.value);
    syncUrl();
  });
}

async function init() {
  if (fuelChoiceSection) {
    state.fuelChoice.km = onSlider("km", Number(fuelChoiceSection.dataset.defaultKm) || state.fuelChoice.km);
    fuelChoiceSection.addEventListener("input", (e) => {
      const { name, value } = e.target;
      if (name === "pair") selectFuelChoicePair(value);
      else if (["km", "premium", "years"].includes(name)) state.fuelChoice[name] = Number(value);
      else return;
      renderFuelChoice();
    });
  }
  syncToggle("fuel", state.fuel);
  syncToggle("period", state.period);
  onToggle("fuel", (v) => { state.fuel = v; render(); }); // both fuels are already loaded
  onToggle("period", (v) => { state.period = v; loadTrend(); });
  onToggle("brand-kind", (v) => { state.brands.kind = v; renderBrands(); });
  document.getElementById("brands-more").addEventListener("click", () => {
    state.brands.expanded = !state.brands.expanded;
    renderBrands();
  });
  document.getElementById("brand-rows").addEventListener("click", (e) => {
    const row = e.target.closest("[data-brand]");
    if (!row) return;
    state.brands.selected = row.dataset.brand;
    renderBrands();
  });
  scopeSelect.addEventListener("change", () => {
    state.scope = scopeSelect.value;
    syncUrl();
    loadTrend();
    loadWhen();
    loadBrands();
    loadFuelChoice();
  });
  try {
    const group = scopeSelect.querySelector("[data-province-options]");
    for (const [raw, pretty] of Object.entries(await getProvinces())) group.append(new Option(pretty, raw));
  } catch {
    // Regions still work without the province list.
  }
  if (![...scopeSelect.options].some((o) => o.value === state.scope)) state.scope = filters.dataset.defaultScope;
  scopeSelect.value = state.scope;
  await Promise.all([loadTrend(), loadWhen(), loadBrands(), loadFuelChoice()]);
}

init();
