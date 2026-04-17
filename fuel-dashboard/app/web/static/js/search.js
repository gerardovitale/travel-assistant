import { api, qs, toast } from "./app.js";
import { populateFuelSelect, getLabels } from "./fuel.js";
import { createMap, drawStationsTracked, drawSearchPin, drawZipBoundary, drawRoute, priceColor } from "./maps.js";
import { formatPrice, formatKm, formatEur, escapeHtml } from "./format.js";
import { initBrandsDropdown, populateBrandsList, getSelectedLabels } from "./brands.js";

const APP_CONFIG = window.__APP_CONFIG__ || {};

const ADVANCED_FIELDS_BY_MODE = {
  nearest_by_address: [],
  cheapest_by_address: ["radius"],
  best_by_address: ["radius", "consumption", "tank"],
};

const state = {
  mode: "best_by_address",
  results: [],
  selectedLabels: new Set(),
};

let map, markersGroup, locationLayer;
let markerRefs = [];
let routeLayer = null;
let searchOriginCoords = null;
const routeCache = new Map();
let hoverTimer = null;

function isPostalCodeQuery(location) {
  return /^\d{5}$/.test(location);
}

// ── Mode switching ──────────────────────────────────────────────────

function switchMode(mode) {
  state.mode = mode;
  document.querySelectorAll("#mode-tabs button").forEach((b) => {
    const active = b.dataset.mode === mode;
    b.classList.toggle("seg-active", active);
    b.classList.toggle("seg-inactive", !active);
  });

  const advFields = new Set(ADVANCED_FIELDS_BY_MODE[mode]);
  const advSection = document.getElementById("advanced-section");
  document.querySelectorAll("[data-field]").forEach((el) => {
    el.hidden = !advFields.has(el.dataset.field);
  });

  if (advFields.size === 0) {
    advSection.removeAttribute("open");
    advSection.hidden = true;
  } else {
    advSection.hidden = false;
  }
}

// ── Brands dropdown (shared via brands.js) ─────────────────────────

// ── KPIs + rendering ────────────────────────────────────────────────

function kpiCard(label, value, icon = "insights") {
  return `
    <div class="bg-surface-container-lowest border border-outline-variant/40 rounded-2xl p-4 shadow-sm">
      <div class="flex items-center gap-2 text-outline">
        <span class="material-symbols-outlined text-[18px]">${icon}</span>
        <span class="text-[11px] font-label font-bold tracking-wider uppercase">${label}</span>
      </div>
      <p class="mt-2 font-headline font-extrabold text-2xl text-on-surface">${value}</p>
    </div>`;
}

function renderKpis(results) {
  if (!results.length) { document.getElementById("kpis").classList.add("hidden"); return; }
  const best = results[0];
  const prices = results.map((r) => r.price).filter((p) => p != null);
  const avg = prices.length ? prices.reduce((a, b) => a + b, 0) / prices.length : null;
  const cheapest = prices.length ? Math.min(...prices) : null;
  const nearest = results.reduce((min, r) => (r.distance_km != null && (min == null || r.distance_km < min.distance_km) ? r : min), null);

  const cards = [
    kpiCard("Mejor precio", formatPrice(cheapest), "savings"),
    kpiCard("Media del listado", formatPrice(avg), "bar_chart"),
    kpiCard(nearest ? "Más cercana" : "Resultados", nearest ? formatKm(nearest.distance_km) : String(results.length), "near_me"),
    kpiCard("Estación", escapeHtml(best.label || "—"), "local_gas_station"),
  ];
  const el = document.getElementById("kpis");
  el.innerHTML = cards.join("");
  el.classList.remove("hidden");
}

function renderList(results) {
  const listEl = document.getElementById("results-list");
  if (!results.length) {
    listEl.innerHTML = `<div class="bg-surface-container-low rounded-2xl p-8 text-center text-outline text-sm">Sin resultados. Prueba con otra búsqueda.</div>`;
    return;
  }
  const rows = results.map((s, i) => {
    const dist = s.distance_km != null ? formatKm(s.distance_km) : "";
    const cost = s.estimated_total_cost != null ? `· ${formatEur(s.estimated_total_cost)} total` : "";
    const pct = s.pct_vs_avg != null ? `<span class="${s.pct_vs_avg < 0 ? 'text-tertiary-container' : 'text-error'} text-[11px] font-bold">${s.pct_vs_avg > 0 ? '+' : ''}${s.pct_vs_avg.toFixed(1)}% vs media</span>` : "";
    const mapsUrl = `https://www.google.com/maps/dir/?api=1&destination=${s.latitude},${s.longitude}`;
    return `
      <article data-index="${i}" data-testid="search-result-card" class="bg-surface-container-lowest rounded-2xl shadow-sm border border-outline-variant/40 p-4 flex gap-3 items-start fade-in">
        <div class="h-10 w-10 rounded-lg bg-surface-container-high flex items-center justify-center text-primary-container shrink-0 font-headline font-bold">${i + 1}</div>
        <div class="flex-1 min-w-0">
          <div class="flex items-center justify-between gap-2">
            <h3 class="font-headline font-bold truncate">${escapeHtml(s.label || "Estación")}</h3>
            <span class="font-headline font-extrabold text-lg text-primary-container">${formatPrice(s.price)}</span>
          </div>
          <p class="text-[12px] text-on-surface-variant truncate mt-0.5">${escapeHtml([s.address, s.municipality].filter(Boolean).join(", "))}</p>
          <div class="flex items-center gap-3 mt-2 text-[12px] text-outline">
            <span>${dist}</span>
            <span>${cost}</span>
            ${pct}
          </div>
        </div>
        <a href="${mapsUrl}" target="_blank" rel="noopener" class="p-2 rounded-lg hover:bg-surface-container-high text-primary-container" title="Cómo llegar">
          <span class="material-symbols-outlined">directions</span>
        </a>
      </article>`;
  }).join("");
  listEl.innerHTML = rows;
}

function renderRecommendation(results) {
  const rec = document.getElementById("recommendation");
  if (!results.length) { rec.classList.add("hidden"); return; }
  const best = results[0];
  const dist = best.distance_km != null ? ` a ${best.distance_km.toFixed(1)} km` : "";
  const cost = best.estimated_total_cost != null ? `, con un coste estimado de ${formatEur(best.estimated_total_cost)}` : "";
  const msg = `La mejor opción es <b>${escapeHtml(best.label)}</b>${dist} al precio de <b>${formatPrice(best.price)}</b>${cost}.`;
  document.getElementById("recommendation-text").innerHTML = msg;
  rec.classList.remove("hidden");
}

function showBanner(kind, text) {
  const el = document.getElementById("status-banner");
  el.className = `rounded-xl px-4 py-3 text-sm ${kind === "error" ? "bg-error-container text-on-error-container" : "bg-primary-container/10 text-primary-container"}`;
  el.textContent = text;
  el.classList.remove("hidden");
}
function hideBanner() { document.getElementById("status-banner").classList.add("hidden"); }

function resetResults({ emptyState = null } = {}) {
  state.results = [];
  renderKpis([]);
  renderRecommendation([]);
  if (emptyState) {
    document.getElementById("results-list").innerHTML = emptyState;
  } else {
    renderList([]);
  }
  if (routeLayer) { map.removeLayer(routeLayer); routeLayer = null; }
  markerRefs = [];
  routeCache.clear();
  searchOriginCoords = null;
  const { group } = drawStationsTracked(map, markersGroup, []);
  markersGroup = group;
  if (locationLayer) { locationLayer.clearLayers(); }
}

// ── Search execution ────────────────────────────────────────────────

async function runSearch(form) {
  const data = new FormData(form);
  const fuelType = data.get("fuel_type");
  const labels = getSelectedLabels(state.selectedLabels);
  const common = { fuel_type: fuelType, labels };
  const location = (data.get("location") || "").trim();

  if (!location) {
    showBanner("error", "Introduce un código postal o dirección");
    return;
  }

  let path, params;
  try {
    const zipSearch = state.mode === "cheapest_by_address" && isPostalCodeQuery(location);
    if (zipSearch) {
      path = "/stations/cheapest-by-zip";
      params = { zip_code: location, ...common };
    } else {
      const endpoint = state.mode.replace(/_/g, "-");
      path = `/stations/${endpoint}`;
      params = { address: location, ...common };
      if (state.mode === "cheapest_by_address" || state.mode === "best_by_address") {
        params.radius_km = data.get("radius_km");
      }
      if (state.mode === "best_by_address") {
        params.consumption_lper100km = data.get("consumption_lper100km");
        params.tank_liters = data.get("tank_liters");
      }
    }

    showBanner("info", "Buscando estaciones…");
    const resp = await api(`${path}?${qs(params)}`);
    state.results = resp.stations || [];
    hideBanner();
    renderKpis(state.results);
    renderRecommendation(state.results);
    renderList(state.results);

    routeCache.clear();
    const isZip = isPostalCodeQuery(location);
    searchOriginCoords = isZip ? null : (resp.search_location || null);
    if (isZip) {
      try {
        const { geojson } = await api(`/zones/zip-boundary?zip_code=${location}`);
        locationLayer = drawZipBoundary(map, locationLayer, geojson);
      } catch { /* boundary unavailable — skip */ }
    }
    const tracked = drawStationsTracked(map, markersGroup, state.results);
    markersGroup = tracked.group;
    markerRefs = tracked.markers.map((m, i) => ({ marker: m, station: state.results[i] }));
    if (!isZip && resp.search_location) {
      locationLayer = drawSearchPin(map, locationLayer, resp.search_location.latitude, resp.search_location.longitude, location);
    }
  } catch (err) {
    showBanner("error", err.message || "Error buscando estaciones");
    resetResults();
  }
}

// ── Hover: highlight marker + draw route ────────────────────────────

function attachHoverHandlers() {
  const list = document.getElementById("results-list");

  list.addEventListener("mouseover", (e) => {
    const article = e.target.closest("article[data-index]");
    if (!article || !searchOriginCoords) return;
    const idx = parseInt(article.dataset.index, 10);
    if (isNaN(idx) || !markerRefs[idx]) return;

    clearTimeout(hoverTimer);
    // Restore all markers immediately before setting the new highlight
    markerRefs.forEach(({ marker: m, station: s }) => {
      const c = priceColor(s.price);
      m.setStyle({ color: c, fillColor: c, weight: 1.5 });
      m.setRadius(10);
    });
    if (routeLayer) { map.removeLayer(routeLayer); routeLayer = null; }

    const capturedOrigin = searchOriginCoords;
    hoverTimer = setTimeout(async () => {
      if (!capturedOrigin || capturedOrigin !== searchOriginCoords) return;
      const { marker, station } = markerRefs[idx];
      marker.setStyle({ color: "#001642", fillColor: "#001642", weight: 3 });
      marker.setRadius(14);
      const latlng = [station.latitude, station.longitude];
      if (!map.getBounds().contains(latlng)) map.panTo(latlng, { animate: true });

      if (routeLayer) { map.removeLayer(routeLayer); routeLayer = null; }
      try {
        let coords = routeCache.get(idx);
        if (!coords) {
          const resp = await api(
            `/route?origin_lat=${capturedOrigin.latitude}&origin_lon=${capturedOrigin.longitude}&dest_lat=${station.latitude}&dest_lon=${station.longitude}`,
          );
          coords = resp.coordinates;
          routeCache.set(idx, coords);
        }
        routeLayer = drawRoute(map, coords, { color: "#0453cd" });
      } catch { /* skip route if OSRM unavailable */ }
    }, 150);
  });

  list.addEventListener("mouseleave", () => {
    clearTimeout(hoverTimer);
    markerRefs.forEach(({ marker, station }) => {
      const c = priceColor(station.price);
      marker.setStyle({ color: c, fillColor: c, weight: 1.5 });
      marker.setRadius(10);
    });
    if (routeLayer) { map.removeLayer(routeLayer); routeLayer = null; }
  });
}

// ── Geolocation ─────────────────────────────────────────────────────

function initGeolocation() {
  const btn = document.getElementById("geo-btn");
  if (!btn || APP_CONFIG.disable_geolocation_lookup || !navigator.geolocation) return;
  btn.addEventListener("click", () => {
    btn.disabled = true;
    navigator.geolocation.getCurrentPosition(
      async (pos) => {
        const { latitude, longitude } = pos.coords;
        try {
          const r = await fetch(`https://nominatim.openstreetmap.org/reverse?format=json&lat=${latitude}&lon=${longitude}`);
          const body = await r.json();
          const addr = body.display_name || `${latitude},${longitude}`;
          document.querySelector('input[name="location"]').value = addr;
          toast("Ubicación detectada", "success");
        } catch { toast("No se pudo detectar la dirección", "error"); }
        btn.disabled = false;
      },
      () => { toast("Permiso de ubicación denegado", "error"); btn.disabled = false; },
      { timeout: 8000 },
    );
  });
}

// ── Init ────────────────────────────────────────────────────────────

async function init() {
  map = createMap(document.getElementById("map"));
  await populateFuelSelect(document.querySelector('select[name="fuel_type"]'));

  initBrandsDropdown("brands-toggle", "brands-list");
  try {
    const labels = await getLabels();
    populateBrandsList("brands-list", "brands-label", state.selectedLabels, labels);
  } catch (err) {
    console.warn("Failed to load brand labels:", err);
  }

  initGeolocation();
  attachHoverHandlers();

  document.querySelectorAll("#mode-tabs button").forEach((b) => {
    b.addEventListener("click", () => switchMode(b.dataset.mode));
  });
  switchMode(state.mode);

  document.getElementById("search-form").addEventListener("submit", (e) => {
    e.preventDefault();
    runSearch(e.target);
  });
}

document.addEventListener("DOMContentLoaded", init);
