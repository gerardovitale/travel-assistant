import { api, toast } from "./app.js";
import { populateFuelSelect, getLabels } from "./fuel.js";
import { createMap, drawStations, drawRoute, clearLayer } from "./maps.js";
import { formatPrice, formatKm, formatEur, formatMin, escapeHtml } from "./format.js";
import { initBrandsDropdown, populateBrandsList, getSelectedLabels, updateBrandsLabel } from "./brands.js";
import { attachAutocomplete } from "./addressAutocomplete.js";

let map, stationsLayer, routeLayer, markersLayer;
let stopMarkerRefs = [];
let activeStopIdx = -1;
const selectedLabels = new Set();
const APP_CONFIG = window.__APP_CONFIG__ || {};

function buildShareUrl(formData, publicUrl) {
  const base = publicUrl ? publicUrl.replace(/\/$/, "") + "/trip" : window.location.origin + window.location.pathname;
  const params = new URLSearchParams();
  params.set("origin", formData.origin);
  params.set("destination", formData.destination);
  params.set("fuel_type", formData.fuel_type);
  params.set("consumption_lper100km", formData.consumption_lper100km);
  params.set("tank_liters", formData.tank_liters);
  params.set("fuel_level_pct", formData.fuel_level_pct);
  params.set("max_detour_minutes", formData.max_detour_minutes);
  (formData.labels || []).forEach((l) => params.append("labels[]", l));
  return `${base}?${params.toString()}`;
}

function readShareParams() {
  const p = new URLSearchParams(window.location.search);
  const origin = p.get("origin");
  const destination = p.get("destination");
  if (!origin || !destination) return null;
  return {
    origin,
    destination,
    fuel_type: p.get("fuel_type") || null,
    consumption_lper100km: p.get("consumption_lper100km") ? parseFloat(p.get("consumption_lper100km")) : null,
    tank_liters: p.get("tank_liters") ? parseFloat(p.get("tank_liters")) : null,
    fuel_level_pct: p.get("fuel_level_pct") ? parseFloat(p.get("fuel_level_pct")) : null,
    max_detour_minutes: p.get("max_detour_minutes") ? parseFloat(p.get("max_detour_minutes")) : null,
    labels: p.getAll("labels[]"),
  };
}

function banner(kind, text) {
  const el = document.getElementById("trip-banner");
  el.className = `rounded-xl px-4 py-3 text-sm ${kind === "error" ? "bg-error-container text-on-error-container" : "bg-primary-container/10 text-primary-container"}`;
  el.textContent = text; el.classList.remove("hidden");
}
function hideBanner() { document.getElementById("trip-banner").classList.add("hidden"); }

function kpi(label, value, icon, delay = 0, valueClass = "text-on-surface") {
  return `
    <div class="bg-surface-container-lowest border border-outline-variant/40 rounded-2xl p-4 shadow-sm fade-in" style="animation-delay:${delay}ms">
      <div class="flex items-center gap-2 text-outline">
        <span class="material-symbols-outlined text-[18px]">${icon}</span>
        <span class="text-[11px] font-label font-bold tracking-wider uppercase">${label}</span>
      </div>
      <p class="mt-2 font-headline font-extrabold text-2xl ${valueClass}">${value}</p>
    </div>`;
}

function renderKpis(plan) {
  const fuelPct = plan.fuel_at_destination_pct;
  const fuelClass = fuelPct == null ? "text-on-surface"
    : fuelPct < 15             ? "text-error"
    : fuelPct < 30             ? "text-amber-600"
    :                            "text-tertiary-container";
  const el = document.getElementById("trip-kpis");
  el.innerHTML = [
    kpi("Distancia total", formatKm(plan.total_distance_km), "straighten", 0),
    kpi("Duración", formatMin(plan.duration_minutes), "schedule", 60),
    kpi("Coste combustible", formatEur(plan.total_fuel_cost), "payments", 120),
    kpi("Ahorro estimado", formatEur(plan.savings_eur), "savings", 180),
    kpi("Combustible al llegar", `${fuelPct?.toFixed(0) ?? "—"}%`, "local_gas_station", 240, fuelClass),
  ].join("");
  el.classList.remove("hidden");
}

function stopCard(s, i) {
  const st = s.station;
  const mapsUrl = `https://www.google.com/maps/dir/?api=1&destination=${st.latitude},${st.longitude}`;
  return `
    <article data-testid="trip-stop-card" data-index="${i}" class="bg-surface-container-lowest rounded-2xl shadow-sm border border-outline-variant/40 p-4 flex gap-3 items-start fade-in" style="animation-delay:${Math.min(i * 60, 360)}ms">
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
      <a href="${mapsUrl}" target="_blank" rel="noopener" class="p-2 rounded-lg hover:bg-surface-container-high text-primary-container" title="Cómo llegar">
        <span class="material-symbols-outlined">directions</span>
      </a>
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
    <article data-testid="trip-alt-plan-card" class="snap-start shrink-0 w-72 bg-surface-container-lowest rounded-2xl border border-outline-variant/40 p-4 shadow-sm">
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

function tryOpenNativeApp(appUrl, webFallbackUrl) {
  let fallbackTimer;
  const onVisibility = () => {
    if (document.hidden) clearTimeout(fallbackTimer);
    document.removeEventListener("visibilitychange", onVisibility);
  };
  document.addEventListener("visibilitychange", onVisibility);
  fallbackTimer = setTimeout(() => {
    document.removeEventListener("visibilitychange", onVisibility);
    window.open(webFallbackUrl, "_blank");
  }, 1500);
  window.location.href = appUrl;
}

function buildNavUrls(plan) {
  const [oLat, oLon] = plan.origin_coords;
  const [dLat, dLon] = plan.destination_coords;

  // Google Maps web URL works via Universal Links on iOS and Android's intent system.
  const segments = [
    `${oLat},${oLon}`,
    ...plan.stops.map((s) => `${s.station.latitude},${s.station.longitude}`),
    `${dLat},${dLon}`,
  ];
  const googleUrl = `https://www.google.com/maps/dir/${segments.join("/")}`;

  // Waze URL scheme has no multi-waypoint support — navigate to first stop if available.
  // waze:// custom scheme opens the native app; web URL is the fallback if not installed.
  const wazeTarget = plan.stops.length
    ? [plan.stops[0].station.latitude, plan.stops[0].station.longitude]
    : [dLat, dLon];
  const wazeAppUrl = `waze://ul?ll=${wazeTarget[0]},${wazeTarget[1]}&navigate=yes`;
  const wazeWebUrl = `https://waze.com/ul?ll=${wazeTarget[0]},${wazeTarget[1]}&navigate=yes`;
  const wazeLabel = plan.stops.length ? "solo 1.ª parada" : "destino";

  // Apple Maps: https URL opens the native Maps app on iOS/macOS automatically.
  const appleUrl = `https://maps.apple.com/?saddr=${oLat},${oLon}&daddr=${dLat},${dLon}`;

  return { googleUrl, wazeAppUrl, wazeWebUrl, wazeLabel, appleUrl };
}

function showShare(formData, plan) {
  const publicUrl = window.__APP_CONFIG__?.public_url || "";
  const shareUrl = buildShareUrl(formData, publicUrl);
  const text = `¡He planificado un viaje de ${formData.origin} a ${formData.destination} con paradas para repostar al mejor precio! ⛽`;

  const copyBtn = document.getElementById("trip-share-copy");
  const waBtn = document.getElementById("trip-share-whatsapp");
  const tgBtn = document.getElementById("trip-share-telegram");

  copyBtn.dataset.url = shareUrl;
  waBtn.href = `https://wa.me/?text=${encodeURIComponent(text + " " + shareUrl)}`;
  tgBtn.href = `https://t.me/share/url?url=${encodeURIComponent(shareUrl)}&text=${encodeURIComponent(text)}`;

  const { googleUrl, wazeAppUrl, wazeWebUrl, wazeLabel, appleUrl } = buildNavUrls(plan);
  document.getElementById("nav-google").href = googleUrl;
  const wazeEl = document.getElementById("nav-waze");
  wazeEl.href = wazeWebUrl;
  wazeEl.dataset.appUrl = wazeAppUrl;
  document.getElementById("nav-waze-label").textContent = `(${wazeLabel})`;
  document.getElementById("nav-apple").href = appleUrl;
  document.getElementById("trip-nav").classList.remove("hidden");

  // Always push a relative URL — using the canonical publicUrl here would throw
  // a SecurityError when public_url differs from the current origin (e.g. staging).
  history.pushState({}, "", window.location.pathname + shareUrl.slice(shareUrl.indexOf("?")));
  document.getElementById("trip-share").classList.remove("hidden");
}

function hideShare() {
  document.getElementById("trip-share").classList.add("hidden");
  document.getElementById("trip-nav").classList.add("hidden");
}

function resetPlanView(message = "Introduce origen y destino para planificar.") {
  document.getElementById("trip-kpis").classList.add("hidden");
  document.getElementById("trip-stops").innerHTML = `
    <div class="bg-surface-container-low rounded-2xl p-8 text-center text-outline text-sm">${escapeHtml(message)}</div>`;
  document.getElementById("alt-plans-wrap").classList.add("hidden");
  document.getElementById("alt-plans").innerHTML = "";
  hideShare();
  routeLayer && map.removeLayer(routeLayer);
  markersLayer && map.removeLayer(markersLayer);
  stationsLayer = clearLayer(map, stationsLayer);
  routeLayer = null;
  markersLayer = null;
  stopMarkerRefs = [];
  activeStopIdx = -1;
}

const STOP_BASE_STYLE = { color: "#001642", fillColor: "#001642", fillOpacity: 1, weight: 2 };
const STOP_ACTIVE_STYLE = { color: "#FFB300", fillColor: "#FFB300", fillOpacity: 1, weight: 2 };

function resetStopHighlights() {
  activeStopIdx = -1;
  stopMarkerRefs.forEach((m) => { m.setStyle(STOP_BASE_STYLE); m.setRadius(10); });
}

function highlightStop(idx) {
  if (idx === activeStopIdx) return;
  resetStopHighlights();
  const marker = stopMarkerRefs[idx];
  if (!marker) return;
  activeStopIdx = idx;
  marker.setStyle(STOP_ACTIVE_STYLE);
  marker.setRadius(14);
  const latlng = marker.getLatLng();
  if (!map.getBounds().contains(latlng)) map.panTo(latlng, { animate: true });
}

function attachStopInteraction() {
  const container = document.getElementById("trip-stops");

  container.addEventListener("mouseover", (e) => {
    const article = e.target.closest("article[data-index]");
    if (!article) return;
    highlightStop(parseInt(article.dataset.index, 10));
  });

  container.addEventListener("mouseleave", () => resetStopHighlights());

  container.addEventListener("click", (e) => {
    if (e.target.closest("a")) return;
    const article = e.target.closest("article[data-index]");
    if (!article) return;
    highlightStop(parseInt(article.dataset.index, 10));
  });
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
  stopMarkerRefs = [];
  activeStopIdx = -1;

  if (plan.route_coordinates?.length) {
    routeLayer = drawRoute(map, plan.route_coordinates);
  }
  if (plan.candidate_stations?.length) {
    stationsLayer = drawStations(map, stationsLayer, plan.candidate_stations);
  }
  const stops = plan.stops.map((s) => s.station);
  if (stops.length || plan.origin_coords) {
    const hl = [];
    for (const st of stops) {
      const marker = L.circleMarker([st.latitude, st.longitude], { radius: 10, ...STOP_BASE_STYLE }).bindTooltip(st.label);
      stopMarkerRefs.push(marker);
      hl.push(marker);
    }
    if (plan.origin_coords) hl.push(L.marker(plan.origin_coords).bindTooltip("Origen"));
    if (plan.destination_coords) hl.push(L.marker(plan.destination_coords).bindTooltip("Destino"));
    markersLayer = L.layerGroup(hl).addTo(map);
  }

  fitPlanBounds(plan);
}

async function runPlan(form) {
  const submitBtn = form.querySelector('[type="submit"]');
  if (!submitBtn) return;
  const originalHTML = submitBtn.innerHTML;
  submitBtn.disabled = true;
  submitBtn.innerHTML = '<span class="material-symbols-outlined animate-spin align-middle">refresh</span><span class="ml-2">Calculando…</span>';

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
    showShare(body, plan);
  } catch (err) {
    resetPlanView(err.message || "No se pudo generar una nueva ruta.");
    banner("error", err.message || "Error calculando el viaje");
  } finally {
    submitBtn.disabled = false;
    submitBtn.innerHTML = originalHTML;
  }
}

// ── Geolocation ─────────────────────────────────────────────────────

function initTripGeolocation(originAC) {
  const btn = document.getElementById("geo-btn-origin");
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
          originAC.setValue(addr);
          toast("Ubicación detectada", "success");
        } catch { toast("No se pudo detectar la dirección", "error"); }
        btn.disabled = false;
      },
      (err) => {
        const msg =
          err.code === err.PERMISSION_DENIED  ? "Permiso de ubicación denegado" :
          err.code === err.TIMEOUT            ? "Tiempo agotado al obtener ubicación" :
                                               "No se pudo obtener la ubicación";
        toast(msg, "error");
        btn.disabled = false;
      },
      { timeout: 10000, maximumAge: 60000 },
    );
  });
}

async function init() {
  map = createMap(document.getElementById("trip-map"));
  // Must run before any await so inputs are wrapped before the page becomes interactive.
  const originAC = attachAutocomplete(document.querySelector('[name="origin"]'));
  attachAutocomplete(document.querySelector('[name="destination"]'));  // no geo button; controller unused
  initTripGeolocation(originAC);
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

  const form = document.getElementById("trip-form");
  form.addEventListener("submit", (e) => {
    e.preventDefault();
    runPlan(e.target);
  });

  attachStopInteraction();

  document.getElementById("nav-waze")?.addEventListener("click", (e) => {
    const appUrl = e.currentTarget.dataset.appUrl;
    if (!appUrl) return;
    e.preventDefault();
    tryOpenNativeApp(appUrl, e.currentTarget.href);
  });

  const copyBtn = document.getElementById("trip-share-copy");
  const copyIcon = copyBtn?.querySelector(".material-symbols-outlined");
  const copyLabel = document.getElementById("trip-share-copy-label");
  copyBtn?.addEventListener("click", () => {
    const url = copyBtn.dataset.url;
    if (!url) return;
    navigator.clipboard.writeText(url).then(() => {
      copyIcon.textContent = "check";
      copyLabel.textContent = "¡Copiado!";
      setTimeout(() => {
        copyIcon.textContent = "content_copy";
        copyLabel.textContent = "Copiar enlace";
      }, 2000);
    }).catch(() => {});
  });

  const params = readShareParams();
  if (params) {
    if (params.origin) document.querySelector('[name="origin"]').value = params.origin;
    if (params.destination) document.querySelector('[name="destination"]').value = params.destination;
    if (params.fuel_type) {
      const sel = document.querySelector('select[name="fuel_type"]');
      if (sel) sel.value = params.fuel_type;
    }
    if (params.consumption_lper100km != null) document.querySelector('[name="consumption_lper100km"]').value = params.consumption_lper100km;
    if (params.tank_liters != null) document.querySelector('[name="tank_liters"]').value = params.tank_liters;
    if (params.fuel_level_pct != null) {
      levelInput.value = params.fuel_level_pct;
      levelLabel.textContent = `${params.fuel_level_pct}%`;
    }
    if (params.max_detour_minutes != null) document.querySelector('[name="max_detour_minutes"]').value = params.max_detour_minutes;
    if (params.labels?.length) {
      params.labels.forEach((l) => {
        selectedLabels.add(l);
        const cb = document.querySelector(`input[data-testid="brand-checkbox-${CSS.escape(l)}"]`);
        if (cb) cb.checked = true;
      });
      updateBrandsLabel(document.getElementById("brands-label"), selectedLabels);
    }
    runPlan(form);
  }
}

document.addEventListener("DOMContentLoaded", init);
