import { api } from "./app.js";
import { populateFuelSelect, getLabels } from "./fuel.js";
import { createMap, drawStations, drawRoute, clearLayer } from "./maps.js";
import { formatPrice, formatKm, formatEur, formatMin, escapeHtml } from "./format.js";
import { initBrandsDropdown, populateBrandsList, getSelectedLabels } from "./brands.js";

let map, stationsLayer, routeLayer, markersLayer;
const selectedLabels = new Set();

function banner(kind, text) {
  const el = document.getElementById("trip-banner");
  el.className = `rounded-xl px-4 py-3 text-sm ${kind === "error" ? "bg-error-container text-on-error-container" : "bg-primary-container/10 text-primary-container"}`;
  el.textContent = text; el.classList.remove("hidden");
}
function hideBanner() { document.getElementById("trip-banner").classList.add("hidden"); }

function kpi(label, value, icon) {
  return `
    <div class="bg-surface-container-lowest border border-outline-variant/40 rounded-2xl p-4 shadow-sm">
      <div class="flex items-center gap-2 text-outline">
        <span class="material-symbols-outlined text-[18px]">${icon}</span>
        <span class="text-[11px] font-label font-bold tracking-wider uppercase">${label}</span>
      </div>
      <p class="mt-2 font-headline font-extrabold text-2xl text-on-surface">${value}</p>
    </div>`;
}

function renderKpis(plan) {
  const el = document.getElementById("trip-kpis");
  el.innerHTML = [
    kpi("Distancia total", formatKm(plan.total_distance_km), "straighten"),
    kpi("Duración", formatMin(plan.duration_minutes), "schedule"),
    kpi("Coste combustible", formatEur(plan.total_fuel_cost), "payments"),
    kpi("Ahorro estimado", formatEur(plan.savings_eur), "savings"),
  ].join("");
  el.classList.remove("hidden");
}

function stopCard(s, i) {
  const st = s.station;
  return `
    <article class="bg-surface-container-lowest rounded-2xl shadow-sm border border-outline-variant/40 p-4 flex gap-3 items-start">
      <div class="h-10 w-10 rounded-full bg-primary-container text-white flex items-center justify-center font-headline font-bold">${i + 1}</div>
      <div class="flex-1 min-w-0">
        <div class="flex items-center justify-between gap-2">
          <h3 class="font-headline font-bold truncate">${escapeHtml(st.label)}</h3>
          <span class="font-headline font-extrabold text-lg text-primary-container">${formatPrice(st.price)}</span>
        </div>
        <p class="text-[12px] text-on-surface-variant truncate mt-0.5">${escapeHtml([st.address, st.municipality].filter(Boolean).join(", "))}</p>
        <div class="flex flex-wrap items-center gap-x-3 gap-y-1 mt-2 text-[12px] text-outline">
          <span>Km ${st.route_km?.toFixed?.(1) ?? "—"}</span>
          <span>Desvío ${formatMin(s.detour_minutes)}</span>
          <span>Repostar ${s.liters_to_fill?.toFixed?.(1) ?? "—"} l</span>
          <span class="font-semibold text-on-surface">${formatEur(s.cost_eur)}</span>
        </div>
      </div>
    </article>`;
}

function renderStops(plan) {
  const el = document.getElementById("trip-stops");
  if (!plan.stops.length) {
    el.innerHTML = `<div class="bg-surface-container-low rounded-2xl p-8 text-center text-outline text-sm">No hacen falta paradas para este viaje.</div>`;
    return;
  }
  el.innerHTML = plan.stops.map(stopCard).join("");
}

function altCard(alt, bestCost) {
  const delta = alt.total_fuel_cost - bestCost;
  const deltaLabel = delta === 0 ? "igual que la mejor" : `${delta > 0 ? "+" : ""}${formatEur(delta)} vs mejor`;
  const color = delta > 0 ? "text-error" : "text-tertiary-container";
  return `
    <article class="snap-start shrink-0 w-72 bg-surface-container-lowest rounded-2xl border border-outline-variant/40 p-4 shadow-sm">
      <h3 class="font-headline font-bold">${escapeHtml(alt.strategy_name)}</h3>
      <p class="text-[12px] text-on-surface-variant mt-1">${escapeHtml(alt.strategy_description)}</p>
      <div class="mt-3 grid grid-cols-2 gap-2 text-sm">
        <div><span class="text-outline text-[11px] block">Paradas</span><b>${alt.stops.length}</b></div>
        <div><span class="text-outline text-[11px] block">Coste</span><b>${formatEur(alt.total_fuel_cost)}</b></div>
        <div><span class="text-outline text-[11px] block">Desvío</span><b>${formatMin(alt.total_detour_minutes)}</b></div>
        <div><span class="text-outline text-[11px] block">Δ</span><b class="${color}">${deltaLabel}</b></div>
      </div>
    </article>`;
}

function renderAlternatives(plan) {
  const wrap = document.getElementById("alt-plans-wrap");
  const el = document.getElementById("alt-plans");
  if (!plan.alternative_plans?.length) { wrap.classList.add("hidden"); return; }
  el.innerHTML = plan.alternative_plans.map((a) => altCard(a, plan.total_fuel_cost)).join("");
  wrap.classList.remove("hidden");
}

function resetPlanView(message = "Introduce origen y destino para planificar.") {
  document.getElementById("trip-kpis").classList.add("hidden");
  document.getElementById("trip-stops").innerHTML = `
    <div class="bg-surface-container-low rounded-2xl p-8 text-center text-outline text-sm">${escapeHtml(message)}</div>`;
  document.getElementById("alt-plans-wrap").classList.add("hidden");
  document.getElementById("alt-plans").innerHTML = "";
  routeLayer && map.removeLayer(routeLayer);
  markersLayer && map.removeLayer(markersLayer);
  stationsLayer = clearLayer(map, stationsLayer);
  routeLayer = null;
  markersLayer = null;
}

function fitPlanBounds(plan) {
  const points = [];

  for (const coord of plan.route_coordinates || []) {
    if (coord?.length >= 2) points.push([coord[1], coord[0]]);
  }
  for (const station of plan.candidate_stations || []) {
    points.push([station.latitude, station.longitude]);
  }
  for (const stop of plan.stops || []) {
    points.push([stop.station.latitude, stop.station.longitude]);
  }
  if (plan.origin_coords?.length >= 2) points.push(plan.origin_coords);
  if (plan.destination_coords?.length >= 2) points.push(plan.destination_coords);

  if (!points.length) return;
  if (points.length === 1) {
    map.setView(points[0], 12);
    return;
  }

  map.fitBounds(L.latLngBounds(points), { padding: [32, 32], maxZoom: 14 });
}

function renderMap(plan) {
  routeLayer && map.removeLayer(routeLayer);
  markersLayer && map.removeLayer(markersLayer);
  stationsLayer = clearLayer(map, stationsLayer);

  if (plan.route_coordinates?.length) {
    routeLayer = drawRoute(map, plan.route_coordinates);
  }
  if (plan.candidate_stations?.length) {
    stationsLayer = drawStations(map, stationsLayer, plan.candidate_stations);
  }
  const stops = plan.stops.map((s) => s.station);
  if (stops.length || plan.origin_coords) {
    // Highlight stops
    const hl = [];
    for (const st of stops) {
      hl.push(L.circleMarker([st.latitude, st.longitude], {
        radius: 10, color: "#001642", fillColor: "#001642", fillOpacity: 1, weight: 2,
      }).bindTooltip(st.label));
    }
    if (plan.origin_coords) hl.push(L.marker(plan.origin_coords).bindTooltip("Origen"));
    if (plan.destination_coords) hl.push(L.marker(plan.destination_coords).bindTooltip("Destino"));
    markersLayer = L.layerGroup(hl).addTo(map);
  }

  fitPlanBounds(plan);
}

async function runPlan(form) {
  const data = new FormData(form);
  const body = {
    origin: data.get("origin"),
    destination: data.get("destination"),
    fuel_type: data.get("fuel_type"),
    consumption_lper100km: parseFloat(data.get("consumption_lper100km")),
    tank_liters: parseFloat(data.get("tank_liters")),
    fuel_level_pct: parseFloat(data.get("fuel_level_pct")),
    max_detour_minutes: parseFloat(data.get("max_detour_minutes")),
    labels: getSelectedLabels(selectedLabels),
  };
  try {
    banner("info", "Calculando ruta…");
    const resp = await api("/trip/plan", { method: "POST", body: JSON.stringify(body) });
    hideBanner();
    const plan = resp.plan;
    renderKpis(plan);
    renderStops(plan);
    renderAlternatives(plan);
    renderMap(plan);
  } catch (err) {
    resetPlanView(err.message || "No se pudo generar una nueva ruta.");
    banner("error", err.message || "Error calculando el viaje");
  }
}

async function init() {
  map = createMap(document.getElementById("trip-map"));
  await populateFuelSelect(document.querySelector('select[name="fuel_type"]'));

  initBrandsDropdown("brands-toggle", "brands-list");
  try {
    const labels = await getLabels();
    populateBrandsList("brands-list", "brands-label", selectedLabels, labels);
  } catch (err) {
    console.warn("Failed to load brand labels:", err);
  }

  const levelInput = document.querySelector('input[name="fuel_level_pct"]');
  const levelLabel = document.getElementById("fuel-level-val");
  levelInput.addEventListener("input", () => { levelLabel.textContent = `${levelInput.value}%`; });

  const swap = document.getElementById("swap-btn");
  swap?.addEventListener("click", () => {
    const o = document.querySelector('input[name="origin"]');
    const d = document.querySelector('input[name="destination"]');
    const t = o.value; o.value = d.value; d.value = t;
  });

  document.getElementById("trip-form").addEventListener("submit", (e) => {
    e.preventDefault();
    runPlan(e.target);
  });
}

document.addEventListener("DOMContentLoaded", init);
