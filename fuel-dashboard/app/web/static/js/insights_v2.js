// Redesigned /insights: one scrolling story driven by the hero's fuel + scope filters.
import { api, qs, toast } from "./app.js";
import { fuelPair, loadingSkeleton, SERIES_COLORS, weekdayBars } from "./charts.js";
import { displaySign, escapeHtml, formatDay, formatNum, formatSignedPriceDelta } from "./format.js";
import { getProvinces } from "./fuel.js";
import {
  bestAndWorstDay,
  changeOverDays,
  coverageStart,
  DAY_NAMES,
  DAY_SHORT,
  priceChange,
  regionGapHeadline,
  tankSavingText,
  tomorrowHeadline,
  trendHeadline,
  weekdayHeadline,
} from "./insights_story.js";

const FUELS = [
  { key: "gasoline_95_e5_price", label: "Gasolina 95" },
  { key: "diesel_a_price", label: "Diésel" },
];
// Scope select values that are not provinces. Province values are the raw /provinces keys.
const REGION_SCOPES = { peninsula: "Península", special: "Islas, Ceuta y Melilla", compare: "Península vs. islas" };

const filters = document.getElementById("insights-filters");
const scopeSelect = filters.querySelector('select[name="scope"]');
const params = new URLSearchParams(location.search);
const state = {
  fuel: params.get("fuel") || filters.dataset.defaultFuel,
  period: params.get("period") || filters.dataset.defaultPeriod,
  scope: params.get("scope") || filters.dataset.defaultScope,
  // main: { [fuelKey]: points } for the scope (the peninsula when comparing); special: the same for
  // islands, Ceuta and Melilla, only loaded when comparing. window: the period's {start, end} days.
  trend: { main: {}, special: null, window: null },
  // "Cuándo repostar" for the main scope, per fuel: weekday gaps and the 7-day forecast. While a new
  // scope loads, nothing from the previous one is shown.
  when: { days: {}, forecast: {}, loading: true },
};
let trendRequest = 0;
let whenRequest = 0;

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

// Fuel view: both fuels for one scope. Compare view: the selected fuel, peninsula vs. islands.
function trendSeries() {
  const { main, special } = state.trend;
  if (!isCompare()) return FUELS.map((f) => ({ ...f, points: main[f.key] || [] }));
  const color = SERIES_COLORS[state.fuel];
  return [
    { key: "peninsula", label: REGION_SCOPES.peninsula, color, points: main[state.fuel] || [] },
    { key: "special", label: REGION_SCOPES.special, endLabel: "Islas, Ceuta<br>y Melilla", color, dash: "dash", points: special?.[state.fuel] || [] },
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
    spreadLabel.textContent = "Diferencia península − islas:";
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

function render() {
  renderKpis();
  renderTrend();
  renderWhenKpis();
  renderWhen();
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
  syncToggle("fuel", state.fuel);
  syncToggle("period", state.period);
  onToggle("fuel", (v) => { state.fuel = v; render(); }); // both fuels are already loaded
  onToggle("period", (v) => { state.period = v; loadTrend(); });
  scopeSelect.addEventListener("change", () => {
    state.scope = scopeSelect.value;
    syncUrl();
    loadTrend();
    loadWhen();
  });
  try {
    const group = scopeSelect.querySelector("[data-province-options]");
    for (const [raw, pretty] of Object.entries(await getProvinces())) group.append(new Option(pretty, raw));
  } catch {
    // Regions still work without the province list.
  }
  if (![...scopeSelect.options].some((o) => o.value === state.scope)) state.scope = filters.dataset.defaultScope;
  scopeSelect.value = state.scope;
  await Promise.all([loadTrend(), loadWhen()]);
}

init();
