import { api, qs } from "./app.js";
import { registerShareBuilder, initShare } from "./share.js";
import { populateFuelSelect, populateGroupSelect, getProvinces, getCatalog, FUEL_LABELS } from "./fuel.js";
import { lineTrend, multiLine, horizontalBar, heatmap, emptyMsg, loadingSkeleton } from "./charts.js";
import { createMap, drawGeoJSON } from "./maps.js";
import { formatPrice, escapeHtml } from "./format.js";

// ----------------------------- URL STATE + SHARE -----------------------------
// Deep-linkable views: the active tab lives in the path (/insights/{tab}) and only filters that
// differ from their defaults are appended as query params, so common views stay short. Each section
// registers a share builder that reproduces its exact view (tab + filters + #anchor).

let activeTab = "trends";
let initialTab = "trends";
let pendingParams = null; // URLSearchParams parsed once from the initial URL
const tabDefaults = {}; // baseline (pristine) filter values captured per tab after populate

// Reportes brand picker state (up to 4 brands; see initReportes / loadBrandOptions below).
let reportesBrandUniverse = []; // selectable brands for the current fuel type, coverage-ordered
let selectedBrands = []; // currently selected brands (<=4)
let reportesPickerReady = false; // true once the picker has loaded its first option set
const MAX_REPORT_BRANDS = 4;

function ctrl(sel) {
  return document.querySelector(sel);
}

function optText(elOrSel) {
  const el = typeof elOrSel === "string" ? ctrl(elOrSel) : elOrSel;
  if (!el || el.selectedIndex < 0) return "";
  return (el.options[el.selectedIndex]?.text || "").trim();
}

// Current raw filter values per tab, keyed by the short query-param name.
const PARAMS = {
  trends: () => ({
    fuel: ctrl('#trends-filter select[name="fuel_group"]')?.value || "",
    prov: ctrl('#trends-filter select[name="province"]')?.value || "",
    zip: (ctrl('#trends-filter input[name="zip_code"]')?.value || "").trim(),
    period: ctrl('#trends-filter select[name="period"]')?.value || "",
  }),
  historical: () => ({
    fuel: ctrl('#historical-form select[name="fuel_type"]')?.value || "",
    period: ctrl('#historical-form select[name="period"]')?.value || "",
    prov: ctrl('#historical-form select[name="province"]')?.value || "",
  }),
  // The reportes tab hosts several reports; `report` selects which one, and each report's own
  // filters ride along in the same query string.
  reportes: () => ({
    report: activeReport,
    fuel: ctrl('#reportes-filter select[name="fuel_type"]')?.value || "",
    dir: ctrl("#reportes-direction-select")?.value || "",
    // Sorted CSV so URL state is order-independent; empty until the picker has loaded.
    brands: reportesPickerReady ? selectedBrands.slice().sort().join(",") : "",
    pair: ctrl('#fuel-type-filter select[name="pair"]')?.value || "",
    prov: ctrl('#fuel-type-filter select[name="province"]')?.value || "",
    km: ctrl("#fuel-type-annual-km")?.value || "",
    period: ctrl("#fuel-type-period-select")?.value || "",
  }),
  zones: () => ({
    fuel: ctrl("#zones-fuel")?.value || "",
    mainland: ctrl("#zones-mainland") ? (ctrl("#zones-mainland").checked ? "1" : "0") : "1",
  }),
  quality: () => ({}),
};

function restoreFilters(tab) {
  if (!pendingParams) return;
  const p = pendingParams;
  const set = (sel, key) => {
    const el = ctrl(sel);
    if (el && p.has(key)) el.value = p.get(key);
  };
  if (tab === "trends") {
    set('#trends-filter select[name="fuel_group"]', "fuel");
    set('#trends-filter select[name="province"]', "prov");
    set('#trends-filter input[name="zip_code"]', "zip");
    set('#trends-filter select[name="period"]', "period");
  } else if (tab === "historical") {
    set('#historical-form select[name="fuel_type"]', "fuel");
    set('#historical-form select[name="period"]', "period");
    set('#historical-form select[name="province"]', "prov");
  } else if (tab === "reportes") {
    set('#reportes-filter select[name="fuel_type"]', "fuel");
    set("#reportes-direction-select", "dir");
    set('#fuel-type-filter select[name="pair"]', "pair");
    set('#fuel-type-filter select[name="province"]', "prov");
    set("#fuel-type-annual-km", "km");
    set("#fuel-type-period-select", "period");
  } else if (tab === "zones") {
    set("#zones-fuel", "fuel");
    const m = ctrl("#zones-mainland");
    if (m && p.has("mainland")) m.checked = p.get("mainland") !== "0";
  }
}

// Capture pristine defaults (after selects are populated) then apply any URL filters for the tab the
// link targeted. Call inside each tab's init, before its first load.
function captureAndRestore(tab) {
  tabDefaults[tab] = PARAMS[tab]();
  if (tab === initialTab) restoreFilters(tab);
}

// Only emit params that differ from the captured defaults → short URLs for the common case.
function serializeFilters(tab) {
  const def = tabDefaults[tab];
  if (!def) return {};
  const cur = PARAMS[tab]();
  const out = {};
  for (const k of Object.keys(cur)) {
    if (cur[k] !== "" && cur[k] !== def[k]) out[k] = cur[k];
  }
  return out;
}

function tabPath(tab) {
  return tab === "trends" ? "/insights" : `/insights/${tab}`;
}

function syncUrl(tab, keepHash = true) {
  if (!tab) return;
  const query = qs(serializeFilters(tab));
  const url = tabPath(tab) + (query ? `?${query}` : "") + (keepHash ? location.hash : "");
  history.replaceState(null, "", url);
}

const SECTION_TAB = {
  "sec-trends-price": "trends",
  "sec-trends-variants": "trends",
  "sec-trends-forecast": "trends",
  "sec-zones-map": "zones",
  "sec-hist-provinces": "historical",
  "sec-hist-dow": "historical",
  "sec-hist-brands": "historical",
  "sec-hist-brand-trend": "historical",
  "sec-hist-volatility": "historical",
  "sec-reportes-coverage": "reportes",
  "sec-reportes-win-rate": "reportes",
  "sec-reportes-price-delta": "reportes",
  "sec-reportes-days-below": "reportes",
  "sec-reportes-fuel-verdict": "reportes",
  "sec-reportes-fuel-cost": "reportes",
  "sec-reportes-fuel-provinces": "reportes",
  "sec-reportes-fuel-history": "reportes",
  "sec-quality": "quality",
};
const SECTION_TITLE = {
  "sec-trends-price": "Tendencia de precios",
  "sec-trends-variants": "Comparativa de variantes",
  "sec-trends-forecast": "Pronóstico de repostaje",
  "sec-zones-map": "Zonas más baratas",
  "sec-hist-provinces": "Ranking por provincia",
  "sec-hist-dow": "Patrón por día de la semana",
  "sec-hist-brands": "Ranking de marcas",
  "sec-hist-brand-trend": "Evolución por marcas",
  "sec-hist-volatility": "Volatilidad por zona",
  "sec-reportes-coverage": "Cobertura geográfica",
  "sec-reportes-win-rate": "Tasa de éxito por marca",
  "sec-reportes-price-delta": "Diferencial de precio vs. mercado",
  "sec-reportes-days-below": "Días por debajo del precio de mercado",
  "sec-reportes-fuel-verdict": "¿Gasolina o diésel?",
  "sec-reportes-fuel-cost": "Coste por 100 km",
  "sec-reportes-fuel-provinces": "Dónde compensa más",
  "sec-reportes-fuel-history": "¿Ha cambiado la respuesta?",
  "sec-quality": "Calidad de datos",
};

// Human-readable filter context per tab, read straight from the selected option labels.
function tabContext(tab) {
  const parts = [];
  if (tab === "trends") {
    parts.push(optText('#trends-filter select[name="fuel_group"]'));
    const zip = (ctrl('#trends-filter input[name="zip_code"]')?.value || "").trim();
    const prov = ctrl('#trends-filter select[name="province"]');
    if (zip) parts.push(`CP ${zip}`);
    else if (prov?.value) parts.push(optText(prov));
    parts.push(optText('#trends-filter select[name="period"]'));
  } else if (tab === "historical") {
    parts.push(optText('#historical-form select[name="fuel_type"]'));
    const prov = ctrl('#historical-form select[name="province"]');
    if (prov?.value) parts.push(optText(prov));
    parts.push(optText('#historical-form select[name="period"]'));
  } else if (tab === "reportes" && activeReport === "combustible") {
    parts.push(optText('#fuel-type-filter select[name="pair"]'));
    const prov = ctrl('#fuel-type-filter select[name="province"]');
    parts.push(prov?.value ? optText(prov) : "Nacional");
    const km = (ctrl("#fuel-type-annual-km")?.value || "").trim();
    if (km) parts.push(`${km} km/año`);
  } else if (tab === "reportes") {
    parts.push(optText('#reportes-filter select[name="fuel_type"]'));
    parts.push(optText("#reportes-direction-select"));
  } else if (tab === "zones") {
    parts.push(optText("#zones-fuel"));
    if (ctrl("#zones-mainland") && !ctrl("#zones-mainland").checked) parts.push("España completa");
  }
  return parts.filter(Boolean);
}

function buildShareText(key) {
  const tab = SECTION_TAB[key];
  return [SECTION_TITLE[key], ...tabContext(tab)].join(" · ") + " — Fuel Precision";
}

function buildShareUrl(tab, hash) {
  const query = qs(serializeFilters(tab));
  return location.origin + tabPath(tab) + (query ? `?${query}` : "") + (hash || "");
}

function registerShareBuilders() {
  for (const key of Object.keys(SECTION_TAB)) {
    registerShareBuilder(key, () => ({
      title: "Fuel Precision",
      text: buildShareText(key),
      url: buildShareUrl(SECTION_TAB[key], `#${key}`),
    }));
  }
}

// Delegated listeners keep the URL in sync without touching each tab's existing handlers.
// Selects/checkboxes (`change`) sync immediately; text inputs (`input`) are debounced so typing a
// zip doesn't replaceState on every keystroke.
const TRACKED_FILTERS =
  '#trends-filter [name], #historical-form [name], #reportes-filter [name], #reportes-direction-select, ' +
  '#fuel-type-filter [name], #fuel-type-period-select, #zones-fuel, #zones-mainland';
const debouncedUrlSync = debounce(() => syncUrl(activeTab, true), 600);
function onFilterChange(e) {
  if (e.target.matches?.(TRACKED_FILTERS)) syncUrl(activeTab, true);
}
function onFilterInput(e) {
  if (e.target.matches?.(TRACKED_FILTERS)) debouncedUrlSync();
}

// Scroll a shared #section into view. Charts render asynchronously and resize the section after the
// first scroll, so re-align on each reflow for a short window, then stop. scroll-margin-top (CSS)
// keeps the title clear of the fixed app bar.
function scrollToHashTarget(target) {
  const el = document.querySelector(target);
  if (!el) return;
  const align = () => el.scrollIntoView({ behavior: "smooth", block: "start" });
  align();
  if (typeof ResizeObserver === "undefined") return;
  const ro = new ResizeObserver(() => align());
  ro.observe(el);
  setTimeout(() => ro.disconnect(), 2500);
}

function applyStateFromUrl() {
  const tabsEl = document.getElementById("insight-tabs");
  const fromPath = tabsEl?.dataset.activeTab || "trends";
  initialTab = loaders[fromPath] ? fromPath : "trends";
  pendingParams = new URLSearchParams(location.search);
  switchTab(initialTab, { sync: false });
  if (location.hash) scrollToHashTarget(location.hash);
}

function debounce(fn, ms) {
  let t;
  return (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), ms); };
}

function kpi(label, value, icon) {
  return `<div class="bg-surface-container-lowest border border-outline-variant/40 rounded-2xl p-4 shadow-sm">
    <div class="flex items-center gap-2 text-outline"><span class="material-symbols-outlined text-[18px]">${icon}</span>
    <span class="text-[11px] font-label font-bold tracking-wider uppercase">${label}</span></div>
    <p class="mt-2 font-headline font-extrabold text-2xl text-on-surface">${value}</p></div>`;
}

function percent(value) {
  return `${((value || 0) * 100).toFixed(0)} %`;
}

// Forecast API requires ge=60; Markov chain needs ~90 days for reliable transitions.
const FORECAST_MIN_WINDOW_DAYS = 90;
const HISTORICAL_PERIOD_DAYS = {
  week: FORECAST_MIN_WINDOW_DAYS,
  month: FORECAST_MIN_WINDOW_DAYS,
  quarter: FORECAST_MIN_WINDOW_DAYS,
  half_year: 180,
  year: 365,
};

// ------------------------------- TRENDS --------------------------------------

async function loadTrends() {
  const form = document.getElementById("trends-filter");
  const data = new FormData(form);
  const zip = (data.get("zip_code") || "").trim();
  const province = (data.get("province") || "").trim() || null;
  const fuelGroup = data.get("fuel_group");
  const cat = await getCatalog();
  const fuelType = cat.primary[fuelGroup];
  if (!fuelType) return;
  const params = { fuel_type: fuelType, period: data.get("period") };
  if (zip) params.zip_code = zip;
  else if (province) params.province = province;
  const kpisEl = document.getElementById("trend-kpis");
  const chartEl = document.getElementById("trend-chart");
  chartEl.innerHTML = loadingSkeleton();
  try {
    const resp = await api(`/trends/price?${qs(params)}`, { signal: AbortSignal.timeout(15000) });
    const pts = resp.trend || [];
    const location = resp.zip_code || "Nacional";
    if (!pts.length) { kpisEl.innerHTML = ""; chartEl.innerHTML = emptyMsg(`Sin datos para ${location}`); return; }
    const avgs = pts.map((p) => p.avg_price);
    const min = Math.min(...avgs), max = Math.max(...avgs), curr = avgs[avgs.length - 1];
    const first = avgs[0];
    const pct = first ? ((curr - first) / first) * 100 : 0;
    kpisEl.innerHTML = [
      kpi("Actual", formatPrice(curr), "local_offer"),
      kpi("Mínimo", formatPrice(min), "south"),
      kpi("Máximo", formatPrice(max), "north"),
      kpi("Variación", `${pct.toFixed(2)} %`, pct >= 0 ? "trending_up" : "trending_down"),
    ].join("");
    lineTrend(chartEl, pts, { label: FUEL_LABELS[fuelType] || "Precio" });
  } catch (err) { chartEl.innerHTML = emptyMsg(err.message); }
}

async function loadGroupTrends() {
  const form = document.getElementById("trends-filter");
  const data = new FormData(form);
  const zip = (data.get("zip_code") || "").trim();
  const province = (data.get("province") || "").trim() || null;
  const params = { fuel_group: data.get("fuel_group"), period: data.get("period") };
  if (zip) params.zip_code = zip;
  else if (province) params.province = province;
  const el = document.getElementById("group-trend-chart");
  el.innerHTML = loadingSkeleton();
  try {
    const resp = await api(`/trends/group?${qs(params)}`, { signal: AbortSignal.timeout(15000) });
    multiLine(el, resp.series || {}, { labels: FUEL_LABELS });
  } catch (err) { el.innerHTML = emptyMsg(err.message); }
}

// ------------------------------- ZONES ---------------------------------------

let zonesMap, zonesLayer;
const zonesState = {
  currentProvince: null,
  currentDistrict: null,
  currentMunicipality: null,
  provinceItems: [],
  provinceGeojson: null,
  baseGeojson: null,
  detailType: null,
  detailItems: [],
};

function zonesParams() {
  return {
    fuel_type: document.getElementById("zones-fuel").value,
    mainland_only: document.getElementById("zones-mainland").checked,
  };
}

function isMadridProvince(province) {
  return (province || "").trim().toLowerCase() === "madrid";
}

function zoneButtonClass(active = false) {
  return [
    "w-full",
    "flex",
    "items-center",
    "justify-between",
    "px-3",
    "py-2",
    "rounded-lg",
    "transition",
    "text-left",
    active ? "bg-primary-container/10 ring-1 ring-primary-container/30" : "hover:bg-surface-container",
  ].join(" ");
}

function showZonesStatus(kind, text) {
  const el = document.getElementById("zones-status");
  el.className = `mt-4 rounded-xl px-4 py-3 text-sm ${kind === "error" ? "bg-error-container text-on-error-container" : "bg-primary-container/10 text-primary-container"}`;
  el.textContent = text;
  el.classList.remove("hidden");
}

function hideZonesStatus() {
  document.getElementById("zones-status").classList.add("hidden");
}

function setZonesLayer(geojson) {
  if (zonesLayer) {
    zonesMap.removeLayer(zonesLayer);
    zonesLayer = null;
  }
  if (geojson?.features?.length) {
    zonesLayer = drawGeoJSON(zonesMap, geojson);
  }
}

function renderProvinceList(items) {
  const list = document.getElementById("zones-provinces");
  list.innerHTML = items.map((it) => `
    <button type="button" data-province="${escapeHtml(it.province)}" class="${zoneButtonClass(zonesState.currentProvince === it.province)}">
      <span class="font-medium">${escapeHtml(it.province)}</span>
      <span class="flex items-center gap-3 text-sm">
        <span class="text-outline">${it.station_count} est.</span>
        <span class="font-headline font-bold text-primary-container">${formatPrice(it.avg_price)}</span>
      </span>
    </button>`).join("") || emptyMsg("Sin datos");
  list.querySelectorAll("button[data-province]").forEach((button) => {
    button.addEventListener("click", () => openProvinceDetail(button.dataset.province));
  });
}

function setDetailPanel({ title, caption, html }) {
  document.getElementById("zones-detail-title").textContent = title;
  document.getElementById("zones-detail-caption").textContent = caption || "";
  document.getElementById("zones-districts").innerHTML = html;
  document.getElementById("zones-districts-wrap").classList.remove("hidden");
}

function hideDetailPanel() {
  zonesState.currentProvince = null;
  zonesState.currentDistrict = null;
  zonesState.currentMunicipality = null;
  zonesState.detailType = null;
  zonesState.detailItems = [];
  document.getElementById("zones-districts-wrap").classList.add("hidden");
}

function renderDistrictList(items) {
  const activeDistrict = zonesState.currentDistrict;
  setDetailPanel({
    title: `Distritos · ${zonesState.currentProvince}`,
    caption: "Selecciona un distrito para pintar en el mapa sus códigos postales.",
    html: items.map((it) => `
      <button type="button" data-district="${escapeHtml(it.district)}" class="${zoneButtonClass(activeDistrict === it.district)}">
        <span class="font-medium">${escapeHtml(it.district)}</span>
        <span class="flex gap-3 text-sm">
          <span class="text-outline">${it.station_count} est.</span>
          <span class="font-headline font-bold text-primary-container">${formatPrice(it.avg_price)}</span>
        </span>
      </button>`).join("") || emptyMsg("Sin distritos"),
  });
  document.querySelectorAll("#zones-districts button[data-district]").forEach((button) => {
    button.addEventListener("click", () => loadDistrictZipOverlay(zonesState.currentProvince, button.dataset.district));
  });
}

function renderMunicipalityList(items) {
  const activeMunicipality = zonesState.currentMunicipality;
  setDetailPanel({
    title: `Municipios · ${zonesState.currentProvince}`,
    caption: "Selecciona un municipio para pintar en el mapa sus códigos postales.",
    html: items.map((municipality) => `
      <button type="button" data-municipality="${escapeHtml(municipality)}" class="${zoneButtonClass(activeMunicipality === municipality)}">
        <span class="font-medium">${escapeHtml(municipality)}</span>
        <span class="text-sm text-outline">Ver códigos postales</span>
      </button>`).join("") || emptyMsg("Sin municipios"),
  });
  document.querySelectorAll("#zones-districts button[data-municipality]").forEach((button) => {
    button.addEventListener("click", () => loadMunicipalityZipOverlay(zonesState.currentProvince, button.dataset.municipality));
  });
}

async function drawZipOverlay(zones, scopeLabel) {
  if (!zones.length) {
    showZonesStatus("info", `No hay códigos postales con datos para ${scopeLabel}.`);
    setZonesLayer(zonesState.baseGeojson);
    return;
  }
  const zipCodes = zones.map((zone) => zone.zip_code);
  const { geojson } = await api(`/zones/postal-geojson?${qs({ zip_codes: zipCodes })}`);
  const priceByZip = new Map(zones.map((zone) => [zone.zip_code, zone]));
  for (const feature of geojson.features || []) {
    const zipCode = String(feature.properties?.COD_POSTAL || "").trim();
    const zone = priceByZip.get(zipCode);
    feature.properties = {
      ...feature.properties,
      zip_code: zipCode,
      avg_price: zone?.avg_price ?? null,
      station_count: zone?.station_count ?? 0,
    };
  }
  if (!geojson.features?.length) {
    showZonesStatus("info", `No hay geometrías postales disponibles para ${scopeLabel}.`);
    setZonesLayer(zonesState.baseGeojson);
    return;
  }
  setZonesLayer(geojson);
  hideZonesStatus();
}

async function loadProvinceMap() {
  hideZonesStatus();
  hideDetailPanel();
  const params = zonesParams();
  document.getElementById("zones-provinces").innerHTML = emptyMsg("Cargando provincias…");
  try {
    const [resp, geoResp] = await Promise.all([
      api(`/zones/province-map?${qs(params)}`),
      api(`/zones/province-geojson?${qs(params)}`),
    ]);
    const items = resp.items || [];
    items.sort((a, b) => a.avg_price - b.avg_price);
    zonesState.provinceItems = items;
    zonesState.provinceGeojson = geoResp.geojson || { type: "FeatureCollection", features: [] };
    zonesState.baseGeojson = zonesState.provinceGeojson;
    renderProvinceList(items);
    setZonesLayer(zonesState.provinceGeojson);
    showZonesStatus("info", "Selecciona una provincia para ver el detalle geográfico.");
  } catch (err) {
    zonesState.provinceItems = [];
    document.getElementById("zones-provinces").innerHTML = emptyMsg(err.message);
    setZonesLayer({ type: "FeatureCollection", features: [] });
    showZonesStatus("error", err.message);
  }
}

async function openProvinceDetail(province) {
  zonesState.currentProvince = province;
  zonesState.currentDistrict = null;
  zonesState.currentMunicipality = null;
  renderProvinceList(zonesState.provinceItems);
  if (isMadridProvince(province)) {
    await loadDistricts(province);
    return;
  }
  await loadMunicipalities(province);
}

async function loadDistricts(province) {
  const fuel = document.getElementById("zones-fuel").value;
  setDetailPanel({ title: `Distritos · ${province}`, caption: "Cargando distritos…", html: emptyMsg("Cargando distritos…") });
  try {
    const [resp, geoResp] = await Promise.all([
      api(`/zones/districts?${qs({ province, fuel_type: fuel })}`),
      api(`/zones/district-geojson?${qs({ province, fuel_type: fuel })}`),
    ]);
    const items = resp.items || [];
    items.sort((a, b) => a.avg_price - b.avg_price);
    zonesState.detailType = "district";
    zonesState.detailItems = items;
    zonesState.baseGeojson = geoResp.geojson || zonesState.provinceGeojson;
    setZonesLayer(zonesState.baseGeojson);
    renderDistrictList(items);
    showZonesStatus("info", "Madrid admite un segundo nivel de detalle por distritos.");
  } catch (err) {
    setZonesLayer(zonesState.provinceGeojson);
    setDetailPanel({ title: `Distritos · ${province}`, caption: err.message, html: emptyMsg(err.message) });
    showZonesStatus("error", err.message);
  }
}

async function loadDistrictZipOverlay(province, district) {
  zonesState.currentDistrict = district;
  renderDistrictList(zonesState.detailItems);
  try {
    const resp = await api(`/zones/district-zips?${qs({ province, district, fuel_type: document.getElementById("zones-fuel").value })}`);
    await drawZipOverlay(resp.zones || [], `${district}, ${province}`);
  } catch (err) {
    setZonesLayer(zonesState.baseGeojson);
    showZonesStatus("error", err.message);
  }
}

async function loadMunicipalities(province) {
  setDetailPanel({ title: `Municipios · ${province}`, caption: "Cargando municipios…", html: emptyMsg("Cargando municipios…") });
  try {
    const resp = await api(`/zones/municipalities?${qs({ province })}`);
    const items = resp.municipalities || [];
    zonesState.detailType = "municipality";
    zonesState.detailItems = items;
    zonesState.baseGeojson = zonesState.provinceGeojson;
    setZonesLayer(zonesState.baseGeojson);
    renderMunicipalityList(items);
    showZonesStatus("info", "Selecciona un municipio para ver sus códigos postales en el mapa.");
  } catch (err) {
    setZonesLayer(zonesState.provinceGeojson);
    setDetailPanel({ title: `Municipios · ${province}`, caption: err.message, html: emptyMsg(err.message) });
    showZonesStatus("error", err.message);
  }
}

async function loadMunicipalityZipOverlay(province, municipality) {
  zonesState.currentMunicipality = municipality;
  renderMunicipalityList(zonesState.detailItems);
  try {
    const resp = await api(`/zones/municipality-zips?${qs({ province, municipality, fuel_type: document.getElementById("zones-fuel").value })}`);
    await drawZipOverlay(resp.zones || [], `${municipality}, ${province}`);
  } catch (err) {
    setZonesLayer(zonesState.baseGeojson);
    showZonesStatus("error", err.message);
  }
}

// ---------------------------- HISTORICAL -------------------------------------

const REGIME_LABELS = { cheap: "Barato", normal: "Normal", expensive: "Caro" };
const REGIME_COLORS = { cheap: "bg-tertiary-container", normal: "bg-secondary-container", expensive: "bg-error-container" };
const GEOGRAPHY_LABELS = { zip_code: "Código postal", province: "Provincia" };

function renderForecastProbabilities(probabilities) {
  const el = document.getElementById("forecast-probabilities");
  const rows = Object.entries(probabilities || {});
  if (!rows.length) {
    el.innerHTML = emptyMsg("Sin datos");
    return;
  }
  el.innerHTML = rows.map(([regime, value]) => {
    const pct = Math.max(0, Math.min(100, (value || 0) * 100));
    const label = REGIME_LABELS[regime] || escapeHtml(regime);
    const barColor = REGIME_COLORS[regime] || "bg-primary-container";
    return `
      <div>
        <div class="flex items-baseline justify-between mb-1.5">
          <span class="text-sm font-semibold">${label}</span>
          <span class="font-headline font-bold text-2xl">${pct.toFixed(0)} %</span>
        </div>
        <div class="h-3 rounded-full bg-surface-container-high overflow-hidden">
          <div class="h-full rounded-full ${barColor}" style="width:${pct}%"></div>
        </div>
      </div>`;
  }).join("");
}

function renderForecast(resp) {
  const bannerEl = document.getElementById("forecast-banner");
  const kpisEl = document.getElementById("forecast-kpis");
  const scopeEl = document.getElementById("forecast-scope");
  const geoLabel = GEOGRAPHY_LABELS[resp.geography_type] || resp.geography_type;
  scopeEl.textContent = `${geoLabel}: ${resp.geography_value}`;

  if (resp.insufficient_data) {
    bannerEl.className = "rounded-xl px-4 py-3 text-sm mb-4 bg-surface-container text-on-surface";
    bannerEl.textContent = resp.explanation || resp.recommendation || "Sin suficiente histórico";
    bannerEl.classList.remove("hidden");
    kpisEl.innerHTML = "";
    renderForecastProbabilities({});
    return;
  }

  bannerEl.className = `rounded-xl px-4 py-3 text-sm mb-4 ${resp.recommendation === "Puedes esperar" ? "bg-tertiary-container/20 text-tertiary-container" : "bg-primary-container/10 text-primary-container"}`;
  bannerEl.innerHTML = `<span class="font-bold">${escapeHtml(resp.recommendation)}</span> · ${escapeHtml(resp.explanation || "")}`;
  bannerEl.classList.remove("hidden");
  kpisEl.innerHTML = [
    kpi("Régimen", escapeHtml(REGIME_LABELS[resp.current_regime] || resp.current_regime || "—"), "local_offer"),
    kpi("Precio actual", resp.current_avg_price != null ? formatPrice(resp.current_avg_price) : "—", "euro"),
    kpi("Más barato en 3d", resp.cheaper_within_3d != null ? percent(resp.cheaper_within_3d) : "—", "schedule"),
    kpi("Confianza", percent(resp.confidence), "analytics"),
  ].join("");
  renderForecastProbabilities(resp.next_day_probabilities);
}

function normalizeForecastZip(zip) {
  return /^\d{5}$/.test(zip || "") ? zip : null;
}

async function loadForecast() {
  const form = document.getElementById("trends-filter");
  const data = new FormData(form);
  const zip = (data.get("zip_code") || "").trim();
  const fuelGroup = data.get("fuel_group");
  const period = data.get("period");
  const province = (data.get("province") || "").trim() || null;
  const cat = await getCatalog();
  const fuelType = cat.primary[fuelGroup];
  if (!fuelType) return;

  const scopeEl = document.getElementById("forecast-scope");
  const bannerEl = document.getElementById("forecast-banner");
  const kpisEl = document.getElementById("forecast-kpis");
  const probsEl = document.getElementById("forecast-probabilities");
  const normalizedZip = normalizeForecastZip(zip);
  const windowDays = HISTORICAL_PERIOD_DAYS[period] || HISTORICAL_PERIOD_DAYS.half_year;

  scopeEl.textContent = "";
  bannerEl.className = "rounded-xl px-4 py-3 text-sm mb-4 bg-surface-container text-on-surface";
  kpisEl.innerHTML = "";
  probsEl.innerHTML = loadingSkeleton();

  if (!normalizedZip && !province) {
    bannerEl.textContent = "Introduce un código postal o selecciona una provincia para activar el pronóstico.";
    bannerEl.classList.remove("hidden");
    probsEl.innerHTML = emptyMsg("Falta contexto geográfico");
    return;
  }

  try {
    const resp = await api(`/historical/forecast?${qs({
      fuel_type: fuelType,
      zip_code: normalizedZip,
      province,
      window_days: windowDays,
    })}`);
    renderForecast(resp);
  } catch (err) {
    bannerEl.textContent = err.message;
    bannerEl.classList.remove("hidden");
    probsEl.innerHTML = emptyMsg(err.message);
  }
}

async function loadHistorical() {
  const form = document.getElementById("historical-form");
  const data = new FormData(form);
  const fuel = data.get("fuel_type"), period = data.get("period"), province = data.get("province");

  const provEl = document.getElementById("hist-provinces");
  provEl.innerHTML = loadingSkeleton();
  try {
    const r = await api(`/zones/provinces?${qs({ fuel_type: fuel, period })}`);
    const rows = r.rows || [];
    horizontalBar(provEl, rows, { labelKey: "province", valueKey: "avg_price" });
  } catch (err) { provEl.innerHTML = emptyMsg(err.message); }

  const dowEl = document.getElementById("hist-dow");
  dowEl.innerHTML = loadingSkeleton();
  try {
    const r = await api(`/historical/day-of-week?${qs({ fuel_type: fuel, province: province || null })}`);
    const rows = r.rows || [];
    if (!rows.length) { dowEl.innerHTML = emptyMsg("Sin datos"); }
    else {
      const days = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"];
      const values = days.map((_, i) => {
        const row = rows.find((r) => r.day_of_week === i);
        return row ? (row.avg_price ?? null) : null;
      });
      horizontalBar(dowEl, days.map((d, i) => ({ label: d, val: values[i] })), { labelKey: "label", valueKey: "val", color: "#0453cd" });
    }
  } catch (err) { dowEl.innerHTML = emptyMsg(err.message); }

  const brandsEl = document.getElementById("hist-brands");
  const brandTrendEl = document.getElementById("hist-brand-trend");
  brandsEl.innerHTML = loadingSkeleton(); brandTrendEl.innerHTML = loadingSkeleton();
  try {
    const r = await api(`/historical/brands?${qs({ fuel_type: fuel, period })}`);
    horizontalBar(brandsEl, r.ranking || [], { labelKey: "brand", valueKey: "avg_price" });
    const bySeries = {};
    for (const row of r.trend || []) {
      const brand = row.brand;
      bySeries[brand] = bySeries[brand] || [];
      bySeries[brand].push({ date: row.date, avg_price: row.avg_price });
    }
    multiLine(brandTrendEl, bySeries, {});
  } catch (err) { brandsEl.innerHTML = emptyMsg(err.message); brandTrendEl.innerHTML = emptyMsg(err.message); }

  const volEl = document.getElementById("hist-volatility");
  volEl.innerHTML = loadingSkeleton();
  try {
    const r = await api(`/historical/volatility?${qs({ fuel_type: fuel, period, mainland_only: true })}`);
    horizontalBar(volEl, r.rows || [], { labelKey: "zip_code", valueKey: "volatility_pct", color: "#a33d3d", maxRows: 20 });
  } catch (err) { volEl.innerHTML = emptyMsg(err.message); }
}

// ----------------------------- QUALITY ---------------------------------------

async function loadQuality() {
  const kpisEl = document.getElementById("quality-kpis");
  const summaryEl = document.getElementById("quality-summary");
  const missingEl = document.getElementById("quality-missing");
  kpisEl.innerHTML = ""; summaryEl.textContent = "Cargando…"; missingEl.innerHTML = "";
  try {
    const r = await api("/quality/inventory");
    const inv = r.inventory || {}; const latest = r.latest_day || {}; const rt = r.realtime || {};
    const sizeMb = inv.total_size_bytes ? (inv.total_size_bytes / (1024 * 1024)).toFixed(1) + " MB" : "—";
    const daysAgo = inv.max_date
      ? Math.floor((Date.now() - new Date(inv.max_date)) / 86_400_000)
      : null;
    const freshnessVal = daysAgo === null ? "—" : daysAgo === 0 ? "Hoy" : `${daysAgo}d atrás`;
    const freshnessIcon = daysAgo !== null && daysAgo > 2 ? "warning" : "check_circle";
    kpisEl.innerHTML = [
      kpi("Días con datos", String(inv.num_days || 0), "calendar_month"),
      kpi("Estaciones (último día)", String(latest.unique_stations || 0), "local_gas_station"),
      kpi("Tamaño en GCS", sizeMb, "storage"),
      kpi("Actualización", freshnessVal, freshnessIcon),
    ].join("");
    const rtStatus = !rt.realtime_enabled ? "desactivado" : rt.realtime_active ? "activo" : "inactivo (error)";
    summaryEl.textContent = `Rango: ${inv.min_date || "—"} → ${inv.max_date || "—"}. Realtime ${rtStatus}.`;
    const missing = r.missing_days || [];
    if (!missing.length) { missingEl.innerHTML = `<span class="text-tertiary-container">Sin días faltantes ✓</span>`; }
    else {
      missingEl.innerHTML = missing.map((d) => `<span class="px-2 py-1 rounded-full bg-error-container text-on-error-container">${escapeHtml(d)}</span>`).join("");
    }
  } catch (err) { summaryEl.textContent = err.message; }
}

// ------------------------------- TABS ----------------------------------------

const loaders = { trends: null, zones: null, historical: null, reportes: null, quality: null };
const loaded = new Set();
// The very first switchTab() call (page load / deep link) establishes the active tab -- it isn't
// a user-perceived "switch" away from anything, so it must never crossfade. Without this guard, a
// deep link into a non-default tab (e.g. /insights/reportes) would fade the server-rendered
// default "trends" panel out before fading the requested tab in, flashing the wrong content first.
let hasActivatedTab = false;

function switchTab(name, opts = {}) {
  document.querySelectorAll("#insight-tabs button").forEach((b) => {
    const active = b.dataset.tab === name;
    b.classList.toggle("bg-white", active); b.classList.toggle("text-primary-container", active);
    b.classList.toggle("bg-white/10", !active); b.classList.toggle("text-white", !active);
    b.querySelector(".material-symbols-outlined").classList.toggle("filled-icon", active);
  });

  const panels = document.querySelectorAll("[data-panel]");
  const current = [...panels].find((p) => !p.classList.contains("hidden"));
  const target = [...panels].find((p) => p.dataset.panel === name);

  const reveal = () => {
    panels.forEach((p) => p.classList.toggle("hidden", p.dataset.panel !== name));
    if (target) {
      target.classList.remove("fade-in");
      void target.offsetHeight; // force reflow so the animation restarts
      target.classList.add("fade-in");
    }
    if (!loaded.has(name) && loaders[name]) { loaders[name](); loaded.add(name); }
    activeTab = name;
    hasActivatedTab = true;
    // Switching tabs drops the previous section anchor and the other tab's filters.
    if (opts.sync !== false) syncUrl(name, false);
  };

  if (hasActivatedTab && current && current !== target) {
    // Cross-fade: fade the old panel out, then swap `hidden` and fade the new one in.
    // A timeout fallback covers `prefers-reduced-motion` zeroing the animation duration,
    // where `animationend` never fires.
    let done = false;
    const finish = () => {
      if (done) return;
      done = true;
      current.classList.remove("panel-fade-out");
      reveal();
    };
    current.classList.add("panel-fade-out");
    current.addEventListener("animationend", finish, { once: true });
    setTimeout(finish, 200);
  } else {
    reveal();
  }
}

async function initTrends() {
  const formEl = document.getElementById("trends-filter");
  const provSel = document.querySelector('#trends-filter select[name="province"]');

  // Fuel-group options and the province list are independent — fetch together
  // so the filter form reveals as one unit instead of field-by-field.
  const [, provincesResult] = await Promise.allSettled([
    populateGroupSelect(document.querySelector('#trends-filter select[name="fuel_group"]')),
    getProvinces(),
  ]);
  if (provincesResult.status === "fulfilled") {
    const provs = provincesResult.value;
    for (const [raw, pretty] of Object.entries(provs)) {
      const opt = document.createElement("option"); opt.value = raw; opt.textContent = pretty; provSel.appendChild(opt);
    }
    // Server-configured default province (DASHBOARD_TRENDS_DEFAULT_PROVINCE); falls back to "Todas"
    // if unset or absent from the option list.
    const defaultProvince = (formEl.dataset.defaultProvince || "").trim().toLowerCase();
    const provOpt = defaultProvince && [...provSel.options].find((o) => o.value.toLowerCase() === defaultProvince);
    if (provOpt) provSel.value = provOpt.value;
  }

  // Server-configured default period (DASHBOARD_TRENDS_DEFAULT_PERIOD); ignored if it doesn't match
  // one of the static period options.
  const periodSel = document.querySelector('#trends-filter select[name="period"]');
  const defaultPeriod = (formEl.dataset.defaultPeriod || "").trim();
  if (defaultPeriod && [...periodSel.options].some((o) => o.value === defaultPeriod)) {
    periodSel.value = defaultPeriod;
  }

  document.getElementById("trends-filter").addEventListener("submit", (e) => e.preventDefault());

  const reloadAll = () => { loadTrends(); loadGroupTrends(); loadForecast(); };
  const dAll = debounce(reloadAll, 600);

  document.querySelector('#trends-filter select[name="fuel_group"]').addEventListener("change", reloadAll);
  document.querySelector('#trends-filter select[name="period"]').addEventListener("change", reloadAll);
  document.querySelector('#trends-filter input[name="zip_code"]').addEventListener("input", dAll);
  provSel.addEventListener("change", reloadAll);

  captureAndRestore("trends");
  loadTrends();
  loadGroupTrends();
  loadForecast();
}
async function initZones() {
  await populateFuelSelect(document.getElementById("zones-fuel"));
  zonesMap = createMap(document.getElementById("zones-map"));
  document.getElementById("zones-fuel").addEventListener("change", loadProvinceMap);
  document.getElementById("zones-mainland").addEventListener("change", loadProvinceMap);
  document.getElementById("zones-detail-reset").addEventListener("click", loadProvinceMap);
  captureAndRestore("zones");
  loadProvinceMap();
}
async function initHistorical() {
  const provSel = document.querySelector('#historical-form select[name="province"]');
  const zipInput = document.querySelector('#historical-form input[name="zip_code"]');

  // Fuel-type options and the province list are independent — fetch together
  // so the filter form reveals as one unit instead of field-by-field.
  const [, provincesResult] = await Promise.allSettled([
    populateFuelSelect(document.querySelector('#historical-form select[name="fuel_type"]')),
    getProvinces(),
  ]);
  if (provincesResult.status === "fulfilled") {
    for (const [raw, pretty] of Object.entries(provincesResult.value)) {
      const opt = document.createElement("option"); opt.value = raw; opt.textContent = pretty; provSel.appendChild(opt);
    }
  }
  const debouncedHistorical = debounce(loadHistorical, 600);
  document.querySelector('#historical-form select[name="fuel_type"]').addEventListener("change", loadHistorical);
  document.querySelector('#historical-form select[name="period"]').addEventListener("change", loadHistorical);
  provSel.addEventListener("change", loadHistorical);
  zipInput.addEventListener("input", debouncedHistorical);
  captureAndRestore("historical");
  loadHistorical();
}
async function initQuality() { captureAndRestore("quality"); loadQuality(); }

// ----------------------------- REPORTES: confidence + savings -----------------------------
// Sample-size confidence bands. Mirror the backend (station_service.py / ingestor reports/config.py):
// 384 ~= n for +/-5% margin at 95% confidence; 100 = rough floor. Coverage rows carry no
// `confidence` field, so we band their `total_observations` (same unit: summed appearances).
const CONFIDENCE_OBS_HIGH = 384;
const CONFIDENCE_OBS_MEDIUM = 100;
const CONFIDENCE_META = {
  high: { label: "Alta", cls: "bg-tertiary-container text-on-tertiary-container" },
  medium: { label: "Media", cls: "bg-secondary-container text-on-secondary-container" },
  low: { label: "Baja", cls: "bg-surface-container-high text-on-surface-variant" },
};

function confidenceFromCount(n) {
  if (n >= CONFIDENCE_OBS_HIGH) return "high";
  if (n >= CONFIDENCE_OBS_MEDIUM) return "medium";
  return "low";
}

function confidenceBadge(level) {
  const m = CONFIDENCE_META[level] || CONFIDENCE_META.low;
  return `<span class="inline-block rounded-full px-2 py-0.5 text-[11px] font-label font-bold ${m.cls}">${m.label}</span>`;
}

// Render a one-line confidence legend (brand -> badge) as a sibling right after a chart container,
// keeping the shared horizontalBar() util untouched. Re-render replaces the prior legend.
function renderConfidenceLegend(chartEl, rows) {
  const legendId = `${chartEl.id}-confidence`;
  let legend = document.getElementById(legendId);
  if (!legend) {
    legend = document.createElement("div");
    legend.id = legendId;
    legend.className = "mt-3 flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-on-surface-variant";
    chartEl.insertAdjacentElement("afterend", legend);
  }
  if (!rows || !rows.length) { legend.innerHTML = ""; return; }
  legend.innerHTML = `<span class="font-label font-bold uppercase tracking-wide">Confianza</span>` +
    rows.map((r) => `<span class="inline-flex items-center gap-1"><span>${escapeHtml(brandLabel(r.brand))}</span>${confidenceBadge(r.confidence)}</span>`).join("");
}

let lastComparisonRows = [];

function savingsInputs() {
  const liters = parseFloat(document.getElementById("reportes-tank-liters")?.value) || 0;
  const fills = parseFloat(document.getElementById("reportes-fills-month")?.value) || 0;
  return { liters, fills };
}

// €/year vs market: weighted-mean prices x tank size x fills. An estimate under explicit,
// user-editable assumptions — never presented as exact.
function renderSavings(rows) {
  const el = document.getElementById("reportes-savings-kpis");
  if (!el) return;
  if (!rows || !rows.length) { el.innerHTML = emptyMsg("Sin datos"); return; }
  const { liters, fills } = savingsInputs();
  const cards = rows
    .map((r) => {
      const savingsPerL = (r.market_avg_price ?? 0) - (r.brand_avg_price ?? 0);
      const eurPerYear = savingsPerL * liters * fills * 12;
      return { brand: r.brand, savingsPerL, eurPerYear };
    })
    .sort((a, b) => b.eurPerYear - a.eurPerYear);
  el.innerHTML = cards
    .map((c) => {
      const positive = c.savingsPerL > 0;
      const value = positive
        ? `~${eurFmt(c.eurPerYear)}/año`
        : "Sin ahorro vs. mercado";
      const valueCls = positive ? "text-on-surface" : "text-on-surface-variant";
      return `<div class="bg-surface-container-lowest border border-outline-variant/40 rounded-2xl p-4 shadow-sm">
        <p class="text-[11px] font-label font-bold tracking-wider uppercase text-outline">${escapeHtml(brandLabel(c.brand))}</p>
        <p class="mt-2 font-headline font-extrabold text-2xl ${valueCls}">${value}</p>
        <p class="mt-1 text-xs text-on-surface-variant">${positive ? `${eurFmt(c.savingsPerL, 3)}/L vs. mercado` : ""}</p></div>`;
    })
    .join("");
}

function eurFmt(n, decimals = 2) {
  return `${(n || 0).toLocaleString("es-ES", { minimumFractionDigits: decimals, maximumFractionDigits: decimals })} €`;
}

// Cepsa rebranded to Moeve; display the new name (with the old one for recognition) everywhere a
// brand label is shown. Data key stays "cepsa". Other brands are simply capitalized.
const BRAND_DISPLAY = { cepsa: "Moeve (Cepsa)" };
function brandLabel(brand) {
  if (!brand) return "";
  return BRAND_DISPLAY[brand] || brand.charAt(0).toUpperCase() + brand.slice(1);
}

// Selected brands as repeated query params: "&brands=a&brands=b".
function brandsQuery() {
  return selectedBrands.map((b) => `&brands=${encodeURIComponent(b)}`).join("");
}

// Build the checkbox list from scratch (universe changed). Toggling a single brand uses
// updateBrandOptionStates() instead, so the just-clicked input is not detached mid-action.
function renderBrandOptions() {
  const panel = document.querySelector('[data-testid="reportes-brand-options"]');
  if (!panel) { updateBrandOptionStates(); return; }
  if (!reportesBrandUniverse.length) { panel.innerHTML = emptyMsg("Sin marcas"); updateBrandOptionStates(); return; }
  panel.innerHTML = reportesBrandUniverse
    .map((b) => {
      const checked = selectedBrands.includes(b);
      return `<label class="flex items-center gap-2 px-2 py-1 rounded" data-brand-row="${escapeHtml(b)}">
        <input type="checkbox" value="${escapeHtml(b)}" ${checked ? "checked" : ""}
          data-brand-option class="rounded border-outline-variant">
        <span class="text-sm">${escapeHtml(brandLabel(b))}</span>
      </label>`;
    })
    .join("");
  updateBrandOptionStates();
}

// Reflect the current selection without rebuilding the DOM: disable unchecked boxes at the cap,
// dim their rows, and refresh the count in the summary.
function updateBrandOptionStates() {
  const summary = document.querySelector('[data-testid="reportes-brand-summary"]');
  if (summary) summary.textContent = `Marcas (${selectedBrands.length}/${MAX_REPORT_BRANDS})`;
  const atMax = selectedBrands.length >= MAX_REPORT_BRANDS;
  document.querySelectorAll('[data-testid="reportes-brand-options"] input[data-brand-option]').forEach((cb) => {
    const disabled = !cb.checked && atMax;
    cb.disabled = disabled;
    const row = cb.closest("[data-brand-row]");
    if (row) row.classList.toggle("opacity-40", disabled);
  });
}

function onBrandChange(e) {
  const cb = e.target.closest("input[data-brand-option]");
  if (!cb) return;
  if (cb.checked && !selectedBrands.includes(cb.value)) selectedBrands.push(cb.value);
  else if (!cb.checked) selectedBrands = selectedBrands.filter((b) => b !== cb.value);
  if (selectedBrands.length > MAX_REPORT_BRANDS) selectedBrands = selectedBrands.slice(0, MAX_REPORT_BRANDS);
  updateBrandOptionStates(); // refresh the cap without detaching the clicked input
  reportesReload();
  syncUrl("reportes");
}

// Fetch the selectable brand universe for the current fuel type and reconcile the selection.
// First load honors ?brands= from the URL, else the server-provided defaults; on fuel change we
// keep whatever still exists and fall back to defaults if nothing remains.
async function loadBrandOptions() {
  const fuelType = document.querySelector('#reportes-filter select[name="fuel_type"]').value;
  let data = { brands: [], default: [] };
  try {
    data = await api(`/reportes/brands?fuel_type=${encodeURIComponent(fuelType)}`);
  } catch (err) {
    /* leave universe empty; charts will show "Sin datos" */
  }
  reportesBrandUniverse = data.brands || [];
  const defaults = (data.default || []).filter((b) => reportesBrandUniverse.includes(b)).slice(0, MAX_REPORT_BRANDS);
  // Record the default set so the URL omits ?brands= while the selection equals it.
  if (tabDefaults.reportes) tabDefaults.reportes.brands = defaults.slice().sort().join(",");

  if (!reportesPickerReady) {
    const fromUrl =
      pendingParams && pendingParams.has("brands")
        ? pendingParams
            .get("brands")
            .split(",")
            .map((b) => b.trim().toLowerCase())
            .filter(Boolean)
            .filter((b) => reportesBrandUniverse.includes(b))
        : null;
    selectedBrands = ((fromUrl && fromUrl.length ? fromUrl : defaults) || []).slice(0, MAX_REPORT_BRANDS);
    reportesPickerReady = true;
  } else {
    selectedBrands = selectedBrands.filter((b) => reportesBrandUniverse.includes(b));
    if (!selectedBrands.length) selectedBrands = defaults;
  }
  renderBrandOptions();
}

function reportesReload() {
  loadWinRate();
  loadPriceComparison();
  loadCoverage();
}

async function loadWinRate() {
  const el = document.getElementById("reportes-win-rate-chart");
  if (!el) return;
  const fuelType = document.querySelector('#reportes-filter select[name="fuel_type"]').value;
  const direction = document.getElementById("reportes-direction-select").value;
  el.innerHTML = loadingSkeleton();
  try {
    const url = `/reportes/win-rate?fuel_type=${encodeURIComponent(fuelType)}&direction=${encodeURIComponent(direction)}${brandsQuery()}`;
    const rows = await api(url);
    horizontalBar(el, rows, { labelKey: "brand", valueKey: "win_rate_pct", color: "#0453cd", tickSuffix: " %", labelFn: brandLabel });
    renderConfidenceLegend(el, rows);
  } catch (err) { el.innerHTML = emptyMsg(err.message); renderConfidenceLegend(el, []); }
}

async function loadPriceComparison() {
  const deltaEl = document.getElementById("reportes-price-delta-chart");
  const daysEl = document.getElementById("reportes-days-below-chart");
  if (!deltaEl || !daysEl) return;
  const fuelType = document.querySelector('#reportes-filter select[name="fuel_type"]').value;
  deltaEl.innerHTML = loadingSkeleton();
  daysEl.innerHTML = loadingSkeleton();
  try {
    const rows = await api(`/reportes/price-comparison?fuel_type=${encodeURIComponent(fuelType)}${brandsQuery()}`);
    horizontalBar(deltaEl, rows, {
      labelKey: "brand", valueKey: "price_delta_pct",
      colorFn: (r) => r.price_delta_pct >= 0 ? "#a33d3d" : "#1b5e20",
      tickSuffix: " %", labelFn: brandLabel,
    });
    renderConfidenceLegend(deltaEl, rows);
    const byDays = rows.slice().sort((a, b) => b.days_below_market_pct - a.days_below_market_pct);
    horizontalBar(daysEl, byDays, { labelKey: "brand", valueKey: "days_below_market_pct", color: "#0453cd", tickSuffix: " %", labelFn: brandLabel });
    renderConfidenceLegend(daysEl, byDays);
    lastComparisonRows = rows;
    renderSavings(rows);
  } catch (err) {
    deltaEl.innerHTML = emptyMsg(err.message);
    daysEl.innerHTML = emptyMsg(err.message);
    renderConfidenceLegend(deltaEl, []);
    renderConfidenceLegend(daysEl, []);
  }
}

async function loadCoverage() {
  const el = document.getElementById("reportes-coverage-table");
  if (!el) return;
  const fuelType = document.querySelector('#reportes-filter select[name="fuel_type"]').value;
  el.innerHTML = loadingSkeleton();
  try {
    const rows = await api(`/reportes/coverage?fuel_type=${encodeURIComponent(fuelType)}${brandsQuery()}`);
    if (!rows.length) { el.innerHTML = emptyMsg("Sin datos"); return; }
    el.innerHTML = `<table class="min-w-full text-sm">
      <thead><tr class="text-left text-xs font-label font-bold text-on-surface-variant uppercase tracking-wide">
        <th class="pb-2 pr-6 whitespace-nowrap">Marca</th>
        <th class="pb-2 pr-6 text-right whitespace-nowrap">C.P.</th>
        <th class="pb-2 pr-6 text-right whitespace-nowrap">Municipios</th>
        <th class="pb-2 pr-6 text-right whitespace-nowrap">Localidades</th>
        <th class="pb-2 pr-6 text-right whitespace-nowrap">Observaciones</th>
        <th class="pb-2 text-right whitespace-nowrap">Confianza</th>
      </tr></thead>
      <tbody class="divide-y divide-outline-variant/30">${rows.map((r) => `
        <tr>
          <td class="py-2 pr-6 font-medium whitespace-nowrap">${escapeHtml(brandLabel(r.brand))}</td>
          <td class="py-2 pr-6 text-right whitespace-nowrap">${r.zip_codes.toLocaleString("es-ES")}</td>
          <td class="py-2 pr-6 text-right whitespace-nowrap">${r.municipalities.toLocaleString("es-ES")}</td>
          <td class="py-2 pr-6 text-right whitespace-nowrap">${r.localities.toLocaleString("es-ES")}</td>
          <td class="py-2 pr-6 text-right whitespace-nowrap text-on-surface-variant">${r.total_observations.toLocaleString("es-ES")}</td>
          <td class="py-2 text-right whitespace-nowrap">${confidenceBadge(confidenceFromCount(r.total_observations))}</td>
        </tr>`).join("")}
      </tbody></table>`;
  } catch (err) { el.innerHTML = emptyMsg(err.message); }
}

// Reports available inside the Reportes tab. Each is lazily initialised the first time it is shown,
// so opening the tab only fetches the report actually being read.
let activeReport = "marcas";
const reportInitialised = {};
const reportInit = { combustible: () => initFuelTypeReport() };

function showReport(report) {
  activeReport = report;
  for (const panel of document.querySelectorAll("#report-picker ~ [data-report], [data-report]")) {
    panel.classList.toggle("hidden", panel.dataset.report !== report);
  }
  for (const option of document.querySelectorAll("[data-report-option]")) {
    const selected = option.dataset.reportOption === report;
    option.classList.toggle("ring-2", selected);
    option.classList.toggle("ring-primary-container", selected);
    option.setAttribute("aria-current", selected ? "true" : "false");
  }
  if (!reportInitialised[report] && reportInit[report]) {
    reportInitialised[report] = true;
    reportInit[report]();
  }
}

async function initReportes() {
  document.querySelectorAll("[data-report-option]").forEach((option) => {
    option.addEventListener("click", () => {
      showReport(option.dataset.reportOption);
      syncUrl("reportes");
    });
  });
  // Fuel change reloads the brand universe (brands can differ per fuel) then refreshes all charts.
  document.querySelector('#reportes-filter select[name="fuel_type"]').addEventListener("change", async () => {
    await loadBrandOptions();
    reportesReload();
    syncUrl("reportes");
  });
  document.getElementById("reportes-direction-select").addEventListener("change", loadWinRate);
  document.querySelector('[data-testid="reportes-brand-options"]')?.addEventListener("change", onBrandChange);
  // Recompute the savings estimate from cached rows on input change — no refetch needed.
  const recomputeSavings = () => renderSavings(lastComparisonRows);
  document.getElementById("reportes-tank-liters")?.addEventListener("input", recomputeSavings);
  document.getElementById("reportes-fills-month")?.addEventListener("input", recomputeSavings);
  captureAndRestore("reportes");
  // The deep link may target a report that the flags left out of the page; fall back to marcas.
  const requested = pendingParams?.get("report");
  const available = [...document.querySelectorAll("[data-report-option]")].map((o) => o.dataset.reportOption);
  showReport(available.includes(requested) ? requested : "marcas");
  await loadBrandOptions(); // resolves selectedBrands before the first chart fetch
  reportesReload();
}

// ----------------------------- COMPARADOR -----------------------------
// Fuel-only running cost: consumption (catalog) x EUR/L (our own prices). Annual km is applied
// client-side from the cached rows — it scales the euro gap and never changes the winner, because
// fuel-only cost is linear in km. Same cache-and-recompute pattern as the reportes savings block.

let fuelTypeCostRows = [];
let fuelTypePairs = {}; // pair_id -> catalog pair, so the cost call knows which vehicles to ask for
const ENERGY_LABELS = { gasoline: "Gasolina 95", diesel: "Diésel A", lpg: "GLP", electric: "Eléctrico" };
const ENERGY_COLORS = { gasoline: "#0453cd", diesel: "#b3923a", lpg: "#0e7b52", electric: "#6f42c1" };

function fuelTypeFilters() {
  return {
    pair: ctrl('#fuel-type-filter select[name="pair"]')?.value || "",
    province: ctrl('#fuel-type-filter select[name="province"]')?.value || "",
    km: parseFloat(ctrl("#fuel-type-annual-km")?.value) || 0,
    period: ctrl("#fuel-type-period-select")?.value || "year",
  };
}

async function loadFuelTypeOptions() {
  const pairSel = ctrl('#fuel-type-filter select[name="pair"]');
  const provSel = ctrl('#fuel-type-filter select[name="province"]');
  const catalog = await api("/reportes/fuel-type/vehicles");
  fuelTypePairs = Object.fromEntries(catalog.pairs.map((p) => [p.pair_id, p]));

  const bySegment = {};
  for (const pair of catalog.pairs) (bySegment[pair.segment_label] ||= []).push(pair);
  for (const [segmentLabel, pairs] of Object.entries(bySegment)) {
    const group = document.createElement("optgroup");
    group.label = segmentLabel;
    for (const pair of pairs) {
      const opt = document.createElement("option");
      opt.value = pair.pair_id;
      opt.textContent = `${pair.model} (${pair.model_year})`;
      group.appendChild(opt);
    }
    pairSel.appendChild(group);
  }
  const defaultPair = (ctrl("#fuel-type-filter")?.dataset.defaultPair || "").trim();
  if (defaultPair && [...pairSel.options].some((o) => o.value === defaultPair)) pairSel.value = defaultPair;

  const versionEl = document.querySelector('[data-testid="fuel-type-catalog-version"]');
  if (versionEl) versionEl.textContent = catalog.version;
  // Render the source as a link only when the catalog actually carries one. A citation that leads
  // nowhere is worse than no citation.
  const sourceEl = document.querySelector('[data-testid="fuel-type-catalog-source"]');
  if (sourceEl) {
    if (catalog.source_url) {
      sourceEl.innerHTML = `<a class="underline" target="_blank" rel="noopener" href="${escapeHtml(catalog.source_url)}">${escapeHtml(catalog.source)}</a>`;
    } else {
      sourceEl.textContent = catalog.source;
    }
  }

  try {
    const provs = await getProvinces();
    for (const [raw, pretty] of Object.entries(provs)) {
      const opt = document.createElement("option"); opt.value = raw; opt.textContent = pretty; provSel.appendChild(opt);
    }
  } catch {}
}

function renderFuelTypeAnnual() {
  const el = document.getElementById("fuel-type-annual-kpis");
  if (!el) return;
  const priced = fuelTypeCostRows.filter((r) => r.cost_per_100km != null);
  if (!priced.length) { el.innerHTML = emptyMsg("Sin datos"); return; }
  const { km } = fuelTypeFilters();
  const cards = priced
    .map((r) => ({ label: r.label, energy: r.energy_type, eurPerYear: (r.cost_per_100km * km) / 100 }))
    .sort((a, b) => a.eurPerYear - b.eurPerYear);
  const cheapest = cards[0];
  const gapCards = cards.map((c) => ({ ...c, gap: c.eurPerYear - cheapest.eurPerYear }));
  el.innerHTML = gapCards
    .map(
      (c) => `<div class="bg-surface-container-lowest border border-outline-variant/40 rounded-2xl p-4 shadow-sm">
        <p class="text-[11px] font-label font-bold tracking-wider uppercase text-outline">${escapeHtml(ENERGY_LABELS[c.energy] || c.energy)}</p>
        <p class="mt-2 font-headline font-extrabold text-2xl">${eurFmt(c.eurPerYear, 0)}/año</p>
        <p class="mt-1 text-xs text-on-surface-variant">${c.gap > 0 ? `+${eurFmt(c.gap, 0)} frente a la opción más barata` : "La más barata"}</p></div>`,
    )
    .join("");
}

// Every fuel-report card carries prose (a verdict sentence, a note, the input figures) alongside its
// chart. On a failed reload the chart is replaced but the prose is not, leaving the previous model's
// conclusion attributed to the newly selected one. Clearing is easy to forget per-card, so each
// loader's catch routes through here.
function clearFuelTypeText(...testIds) {
  for (const id of testIds) {
    const el = document.querySelector(`[data-testid="${id}"]`);
    if (el) el.textContent = "";
  }
}

// Spell out the arithmetic behind the verdict. The consumption figures are the weakest input in the
// whole report (manufacturer WLTP, see the disclosure block), so they are shown rather than implied.
function renderFuelTypeInputs(data) {
  const el = document.getElementById("fuel-type-inputs");
  if (!el) return;
  const numFmt = (n, d) => n.toLocaleString("es-ES", { minimumFractionDigits: d, maximumFractionDigits: d });
  const rows = [data.gasoline, data.diesel].map((v) => {
    const unit = v.consumption_unit.replace("/100km", "/100 km");
    return `<tr>
      <td class="py-1.5 pr-4">${escapeHtml(v.label)}</td>
      <td class="py-1.5 pr-4 text-right whitespace-nowrap">${numFmt(v.consumption, 1)} ${escapeHtml(unit)}</td>
      <td class="py-1.5 pr-4 text-right whitespace-nowrap text-on-surface-variant">×</td>
      <td class="py-1.5 pr-4 text-right whitespace-nowrap">${eurFmt(v.price_per_unit, 3)}/l</td>
      <td class="py-1.5 pr-4 text-right whitespace-nowrap text-on-surface-variant">=</td>
      <td class="py-1.5 text-right whitespace-nowrap font-medium">${eurFmt(v.cost_per_100km)}/100 km</td>
    </tr>`;
  });
  // Prefer the select's display label ("Madrid") over the raw data key ("madrid").
  const provSel = ctrl('#fuel-type-filter select[name="province"]');
  const where = data.province ? optText(provSel) || data.province : "media nacional";
  el.innerHTML = `
    <p class="text-xs font-label font-bold text-on-surface-variant uppercase tracking-wide mb-2">Datos usados en el cálculo</p>
    <div class="overflow-x-auto"><table class="min-w-full text-sm"><tbody>${rows.join("")}</tbody></table></div>
    <p class="mt-2 text-xs text-on-surface-variant">
      Consumo WLTP del fabricante · Precios de ${escapeHtml(where)} a ${escapeHtml(data.price_date)} ·
      Punto de equilibrio ${numFmt(data.breakeven_ratio, 2)} (el diésel gana mientras cueste menos de
      ${eurFmt(data.breakeven_diesel_price, 3)}/l)
    </p>`;
}

function renderFuelTypeVerdict(data) {
  const summaryEl = document.getElementById("fuel-type-verdict-summary");
  const kpiEl = document.getElementById("fuel-type-verdict-kpis");
  if (!summaryEl || !kpiEl) return;
  renderFuelTypeInputs(data);
  const winnerText = {
    diesel: `El diésel sale más barato: ${eurFmt(data.cost_gap_per_100km)} menos cada 100 km.`,
    gasoline: `La gasolina sale más barata: ${eurFmt(data.cost_gap_per_100km)} menos cada 100 km.`,
    tie: "Empate técnico: la diferencia es demasiado pequeña para declarar un ganador.",
  }[data.winner];
  summaryEl.textContent = `${data.model} · ${winnerText}`;
  const kpis = [
    { label: "Diésel hasta", value: `${eurFmt(data.breakeven_diesel_price, 3)}/L`, note: `Por encima gana la gasolina (hoy ${eurFmt(data.diesel.price_per_unit, 3)}/L)` },
    { label: "Margen", value: `${eurFmt(data.diesel_headroom_eur_l, 3)}/L`, note: "Lo que puede subir el diésel antes de perder" },
    { label: "Consumo de equilibrio", value: `${data.breakeven_diesel_consumption} l/100 km`, note: `El diésel gana mientras consuma menos en real (WLTP: ${data.diesel.consumption})` },
    { label: "Datos de", value: data.price_date, note: "Último día con precios de ambos combustibles" },
  ];
  kpiEl.innerHTML = kpis
    .map(
      (k) => `<div class="bg-surface-container-lowest border border-outline-variant/40 rounded-2xl p-4 shadow-sm">
        <p class="text-[11px] font-label font-bold tracking-wider uppercase text-outline">${escapeHtml(k.label)}</p>
        <p class="mt-2 font-headline font-extrabold text-xl">${escapeHtml(k.value)}</p>
        <p class="mt-1 text-xs text-on-surface-variant">${escapeHtml(k.note)}</p></div>`,
    )
    .join("");
}

async function loadFuelTypeVerdict() {
  const { pair, province } = fuelTypeFilters();
  if (!pair) return;
  const query = qs({ pair_id: pair, province: province || undefined });
  try {
    renderFuelTypeVerdict(await api(`/reportes/fuel-type/breakeven?${query}`));
  } catch {
    clearFuelTypeText("fuel-type-verdict-summary", "fuel-type-inputs");
    document.getElementById("fuel-type-verdict-kpis").innerHTML = emptyMsg("Sin datos");
  }
}

async function loadFuelTypeCost() {
  const el = document.getElementById("fuel-type-cost-chart");
  const { pair, province } = fuelTypeFilters();
  if (!pair || !el) return;
  const params = new URLSearchParams();
  for (const v of (fuelTypePairs[pair] || {}).vehicles || []) params.append("vehicle_ids", v.vehicle_id);
  if (province) params.set("province", province);
  el.innerHTML = loadingSkeleton();
  try {
    fuelTypeCostRows = await api(`/reportes/fuel-type/cost?${params.toString()}`);
  } catch {
    fuelTypeCostRows = [];
  }
  const priced = fuelTypeCostRows.filter((r) => r.cost_per_100km != null);
  horizontalBar(el, priced, {
    labelKey: "label",
    valueKey: "cost_per_100km", // gitleaks:allow — generic-api-key entropy match on a chart field name
    tickSuffix: " €",
    colorFn: (r) => ENERGY_COLORS[r.energy_type] || "#001642",
  });
  renderFuelTypeAnnual();
}

// A 50-bar chart carried no signal once diesel wins nearly everywhere — the useful question is
// "how much, and on how many stations". A table shows the per-province arithmetic and the sample
// size the verdict rests on, which bars cannot.
let provinceRows = [];
let provinceSort = { key: "signed_gap", dir: "desc" };

const PROVINCE_COLUMNS = [
  { key: "province", label: "Provincia", align: "left" },
  { key: "gasoline_price", label: "Gasolina", fmt: (r) => `${eurFmt(r.gasoline_price, 3)}/l` },
  { key: "diesel_price", label: "Diésel", fmt: (r) => `${eurFmt(r.diesel_price, 3)}/l` },
  { key: "cost_gasoline_per_100km", label: "Gasolina €/100 km", fmt: (r) => eurFmt(r.cost_gasoline_per_100km) },
  { key: "cost_diesel_per_100km", label: "Diésel €/100 km", fmt: (r) => eurFmt(r.cost_diesel_per_100km) },
  { key: "signed_gap", label: "Dif.", fmt: (r) => eurFmt(r.signed_gap) },
  { key: "station_count", label: "Est.", fmt: (r) => r.station_count.toLocaleString("es-ES") },
];

function provinceName(raw) {
  return raw.charAt(0).toUpperCase() + raw.slice(1);
}

function renderProvinceTable() {
  const el = document.getElementById("fuel-type-provinces-table");
  if (!el) return;
  if (!provinceRows.length) { el.innerHTML = emptyMsg("Sin datos"); return; }

  const { key, dir } = provinceSort;
  const sorted = [...provinceRows].sort((a, b) => {
    const [x, y] = [a[key], b[key]];
    const cmp = typeof x === "string" ? x.localeCompare(y, "es") : x - y;
    return dir === "asc" ? cmp : -cmp;
  });

  const head = PROVINCE_COLUMNS.map((c) => {
    const active = c.key === key;
    const arrow = active ? (dir === "asc" ? " ▲" : " ▼") : "";
    const align = c.align === "left" ? "text-left" : "text-right";
    return `<th class="pb-2 pr-4 ${align} whitespace-nowrap">
      <button type="button" data-sort-key="${c.key}" data-testid="province-sort-${c.key}"
              class="font-label font-bold uppercase tracking-wide ${active ? "text-on-surface" : ""}"
              aria-sort="${active ? (dir === "asc" ? "ascending" : "descending") : "none"}"
      >${escapeHtml(c.label)}${arrow}</button></th>`;
  }).join("");

  const body = sorted.map((r) => {
    const cells = PROVINCE_COLUMNS.map((c) => {
      const align = c.align === "left" ? "" : "text-right";
      const value = c.key === "province" ? escapeHtml(provinceName(r.province)) : c.fmt(r);
      // The gap column carries the verdict, so colour it by which fuel won.
      const tint = c.key === "signed_gap" ? (r.signed_gap > 0 ? "text-primary-container font-medium" : "font-medium") : "";
      return `<td class="py-2 pr-4 ${align} whitespace-nowrap ${tint}">${value}</td>`;
    }).join("");
    return `<tr>${cells}</tr>`;
  }).join("");

  el.innerHTML = `<table class="min-w-full text-sm">
    <thead><tr class="text-xs text-on-surface-variant">${head}</tr></thead>
    <tbody class="divide-y divide-outline-variant/30">${body}</tbody></table>`;

  el.querySelectorAll("[data-sort-key]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const nextKey = btn.dataset.sortKey;
      // Re-clicking the active column flips direction; a new column starts on its natural order
      // (names A→Z, numbers high→low).
      provinceSort = provinceSort.key === nextKey
        ? { key: nextKey, dir: provinceSort.dir === "asc" ? "desc" : "asc" }
        : { key: nextKey, dir: nextKey === "province" ? "asc" : "desc" };
      renderProvinceTable();
    });
  });
}

async function loadFuelTypeProvinces() {
  const el = document.getElementById("fuel-type-provinces-table");
  const noteEl = document.getElementById("fuel-type-provinces-note");
  const { pair } = fuelTypeFilters();
  if (!pair || !el) return;
  try {
    const data = await api(`/reportes/fuel-type/provinces?${qs({ pair_id: pair })}`);
    // Signed gap: positive = diesel cheaper.
    provinceRows = data.rows.map((r) => ({
      ...r,
      signed_gap: +(r.cost_gasoline_per_100km - r.cost_diesel_per_100km).toFixed(2),
    }));
    renderProvinceTable();
    const dieselWins = provinceRows.filter((r) => r.winner === "diesel").length;
    const dropped = data.provinces_dropped
      ? ` ${data.provinces_dropped} provincia(s) sin datos de uno de los dos combustibles quedan fuera.`
      : "";
    if (noteEl) noteEl.textContent = `El diésel gana en ${dieselWins} de ${provinceRows.length} provincias.${dropped}`;
  } catch {
    provinceRows = [];
    clearFuelTypeText("fuel-type-provinces-note");
    el.innerHTML = emptyMsg("Sin datos");
  }
}

async function loadFuelTypeHistory() {
  const el = document.getElementById("fuel-type-history-chart");
  const noteEl = document.getElementById("fuel-type-history-note");
  const { pair, province, period } = fuelTypeFilters();
  if (!pair || !el) return;
  el.innerHTML = loadingSkeleton();
  try {
    const data = await api(`/reportes/fuel-type/history?${qs({ pair_id: pair, province: province || undefined, period })}`);
    // multiLine keys off {date, avg_price}, so map each cost series onto that shape and reuse it.
    multiLine(
      el,
      {
        gasoline: data.series.map((p) => ({ date: p.date, avg_price: p.cost_gasoline_per_100km })),
        diesel: data.series.map((p) => ({ date: p.date, avg_price: p.cost_diesel_per_100km })),
      },
      { labels: { gasoline: "Gasolina €/100 km", diesel: "Diésel €/100 km" } },
    );
    if (noteEl) {
      // Days inside the tie band belong to neither fuel; naming them keeps the percentages adding up.
      const tie = data.pct_days_tie ? ` Un ${data.pct_days_tie}% de los días quedaron en empate técnico.` : "";
      noteEl.textContent = data.flips
        ? `El ganador cambió ${data.flips} vez/veces en el periodo. El diésel salió más barato el ${data.pct_days_diesel_wins}% de los días.${tie}`
        : `El resultado no cambió ni un solo día del periodo: el diésel salió más barato el ${data.pct_days_diesel_wins}% de los días.${tie}`;
    }
  } catch {
    clearFuelTypeText("fuel-type-history-note");
    el.innerHTML = emptyMsg("Sin datos");
  }
}

function fuelTypeReload() {
  loadFuelTypeVerdict();
  loadFuelTypeCost();
  loadFuelTypeProvinces();
  loadFuelTypeHistory();
}

async function initFuelTypeReport() {
  document.getElementById("fuel-type-filter").addEventListener("submit", (e) => e.preventDefault());
  // Options must load before the first fetch: the pair select is API-populated, so a deep-linked
  // pair would otherwise be applied to an empty select and silently fall back to "".
  await loadFuelTypeOptions();
  // initReportes captured this tab's defaults and applied the deep link while these controls were
  // still empty, so re-do both now: first re-baseline the defaults (otherwise every fuel-report
  // field looks non-default and lands in the URL), then re-apply the deep link against real options.
  if (tabDefaults.reportes) {
    Object.assign(tabDefaults.reportes, {
      pair: ctrl('#fuel-type-filter select[name="pair"]')?.value || "",
      prov: ctrl('#fuel-type-filter select[name="province"]')?.value || "",
      km: ctrl("#fuel-type-annual-km")?.value || "",
      period: ctrl("#fuel-type-period-select")?.value || "",
    });
  }
  if (initialTab === "reportes") restoreFilters("reportes");

  ctrl('#fuel-type-filter select[name="pair"]').addEventListener("change", fuelTypeReload);
  ctrl('#fuel-type-filter select[name="province"]').addEventListener("change", () => {
    loadFuelTypeVerdict();
    loadFuelTypeCost();
    loadFuelTypeHistory();
  });
  ctrl("#fuel-type-period-select").addEventListener("change", loadFuelTypeHistory);
  // Annual km only rescales cached rows — no refetch.
  ctrl("#fuel-type-annual-km").addEventListener("input", renderFuelTypeAnnual);

  fuelTypeReload();
}

loaders.trends = initTrends;
loaders.zones = initZones;
loaders.historical = initHistorical;
loaders.reportes = initReportes;
loaders.quality = initQuality;

document.addEventListener("DOMContentLoaded", () => {
  document.querySelectorAll("#insight-tabs button").forEach((b) => {
    b.addEventListener("click", () => switchTab(b.dataset.tab));
  });
  registerShareBuilders();
  initShare(document);
  document.addEventListener("change", onFilterChange);
  document.addEventListener("input", onFilterInput);
  applyStateFromUrl();
});
