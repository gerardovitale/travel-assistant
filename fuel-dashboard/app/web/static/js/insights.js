import { api, qs } from "./app.js";
import { populateFuelSelect, populateGroupSelect, getProvinces, getCatalog, FUEL_LABELS } from "./fuel.js";
import { lineTrend, multiLine, horizontalBar, heatmap, emptyMsg } from "./charts.js";
import { createMap, drawGeoJSON } from "./maps.js";
import { formatPrice, escapeHtml } from "./format.js";

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
  chartEl.innerHTML = emptyMsg("Cargando…");
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
  el.innerHTML = emptyMsg("Cargando…");
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
  probsEl.innerHTML = emptyMsg("Cargando…");

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
  provEl.innerHTML = emptyMsg("Cargando…");
  try {
    const r = await api(`/zones/provinces?${qs({ fuel_type: fuel, period })}`);
    const rows = r.rows || [];
    horizontalBar(provEl, rows, { labelKey: "province", valueKey: "avg_price" });
  } catch (err) { provEl.innerHTML = emptyMsg(err.message); }

  const dowEl = document.getElementById("hist-dow");
  dowEl.innerHTML = emptyMsg("Cargando…");
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
  brandsEl.innerHTML = emptyMsg("Cargando…"); brandTrendEl.innerHTML = emptyMsg("Cargando…");
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
  volEl.innerHTML = emptyMsg("Cargando…");
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

const loaders = { trends: null, zones: null, historical: null, quality: null };
const loaded = new Set();

function switchTab(name) {
  document.querySelectorAll("#insight-tabs button").forEach((b) => {
    const active = b.dataset.tab === name;
    b.classList.toggle("bg-white", active); b.classList.toggle("text-primary-container", active);
    b.classList.toggle("bg-white/10", !active); b.classList.toggle("text-white", !active);
    b.querySelector(".material-symbols-outlined").classList.toggle("filled-icon", active);
  });
  document.querySelectorAll("[data-panel]").forEach((p) => p.classList.toggle("hidden", p.dataset.panel !== name));
  if (!loaded.has(name) && loaders[name]) { loaders[name](); loaded.add(name); }
}

async function initTrends() {
  await populateGroupSelect(document.querySelector('#trends-filter select[name="fuel_group"]'));
  const provSel = document.querySelector('#trends-filter select[name="province"]');
  try {
    const provs = await getProvinces();
    for (const [raw, pretty] of Object.entries(provs)) {
      const opt = document.createElement("option"); opt.value = raw; opt.textContent = pretty; provSel.appendChild(opt);
    }
  } catch {}

  document.getElementById("trends-filter").addEventListener("submit", (e) => e.preventDefault());

  const reloadAll = () => { loadTrends(); loadGroupTrends(); loadForecast(); };
  const dAll = debounce(reloadAll, 600);

  document.querySelector('#trends-filter select[name="fuel_group"]').addEventListener("change", reloadAll);
  document.querySelector('#trends-filter select[name="period"]').addEventListener("change", reloadAll);
  document.querySelector('#trends-filter input[name="zip_code"]').addEventListener("input", dAll);
  provSel.addEventListener("change", reloadAll);

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
  loadProvinceMap();
}
async function initHistorical() {
  await populateFuelSelect(document.querySelector('#historical-form select[name="fuel_type"]'));
  const provSel = document.querySelector('#historical-form select[name="province"]');
  const zipInput = document.querySelector('#historical-form input[name="zip_code"]');
  try {
    const provs = await getProvinces();
    for (const [raw, pretty] of Object.entries(provs)) {
      const opt = document.createElement("option"); opt.value = raw; opt.textContent = pretty; provSel.appendChild(opt);
    }
  } catch {}
  const debouncedHistorical = debounce(loadHistorical, 600);
  document.querySelector('#historical-form select[name="fuel_type"]').addEventListener("change", loadHistorical);
  document.querySelector('#historical-form select[name="period"]').addEventListener("change", loadHistorical);
  provSel.addEventListener("change", loadHistorical);
  zipInput.addEventListener("input", debouncedHistorical);
  loadHistorical();
}
async function initQuality() { loadQuality(); }

loaders.trends = initTrends;
loaders.zones = initZones;
loaders.historical = initHistorical;
loaders.quality = initQuality;

document.addEventListener("DOMContentLoaded", () => {
  document.querySelectorAll("#insight-tabs button").forEach((b) => {
    b.addEventListener("click", () => switchTab(b.dataset.tab));
  });
  switchTab("trends");
});
