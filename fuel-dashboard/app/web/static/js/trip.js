import { api, toast } from "./app.js";
import { populateFuelSelect, getLabels } from "./fuel.js";
import { createMap, drawStations, drawRoute, clearLayer } from "./maps.js";
import { formatPrice, formatKm, formatEur, formatMin, escapeHtml } from "./format.js";
import { initBrandsDropdown, populateBrandsList, getSelectedLabels, updateBrandsLabel } from "./brands.js";
import { attachAutocomplete } from "./addressAutocomplete.js";
import {
  buildNavUrls,
  getProviderOrder,
  openInMaps,
  openSmartNav,
} from "./openInMaps.js";

let map, stationsLayer, routeLayer, markersLayer;
let stopMarkerRefs = [];
let activeStopIdx = -1;
let currentNavUrls = null;  // Latest buildNavUrls() result; read by nav click handlers.
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
  params.set("min_fuel_at_destination_pct", formData.min_fuel_at_destination_pct);
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
    min_fuel_at_destination_pct: p.get("min_fuel_at_destination_pct") ? parseFloat(p.get("min_fuel_at_destination_pct")) : null,
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

// Render an inline disclosure card next to the results that lists every
// parameter used in the estimate. Defaults render as plain chips; any value
// the user changed from the APP_CONFIG default gets a "personalizado" suffix.
function renderAssumptions(body) {
  const entries = [
    { label: `Consumo ${body.consumption_lper100km} L/100 km`, value: body.consumption_lper100km, def: APP_CONFIG.default_consumption_lper100km },
    { label: `Depósito ${body.tank_liters} L`, value: body.tank_liters, def: APP_CONFIG.default_tank_liters },
    { label: `Nivel inicial ${body.fuel_level_pct}%`, value: body.fuel_level_pct, def: APP_CONFIG.default_fuel_level_pct },
    { label: `Desvío máx. ${body.max_detour_minutes} min`, value: body.max_detour_minutes, def: APP_CONFIG.default_max_detour_minutes },
    { label: `Mín. al llegar ${body.min_fuel_at_destination_pct}%`, value: body.min_fuel_at_destination_pct, def: APP_CONFIG.default_min_fuel_at_destination_pct },
  ];
  const list = document.getElementById("trip-assumptions-list");
  list.innerHTML = entries.map((e) => {
    const customized = e.def == null || Number(e.value) !== Number(e.def);
    const suffix = customized
      ? ` <span class="text-[10px] uppercase tracking-wide text-primary-container font-label font-bold">· personalizado</span>`
      : "";
    return `<li class="inline-flex items-center gap-1 rounded-full bg-surface-container px-3 py-1 text-[12px] text-on-surface-variant">${escapeHtml(e.label)}${suffix}</li>`;
  }).join("");
  document.getElementById("trip-assumptions").classList.remove("hidden");
}

function hideAssumptions() {
  document.getElementById("trip-assumptions").classList.add("hidden");
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

// Build the "Cómo llegar" anchor markup, or an empty string if coords are
// non-finite. Renders inline inside the stop/station card templates and is
// resilient to a bad row — one invalid station won't abort the whole list.
function directionsAnchorHtml(lat, lon) {
  try {
    const { google } = buildNavUrls({ destination: [lat, lon] });
    return `<a href="${google}" data-nav-smart data-lat="${lat}" data-lon="${lon}" rel="noopener noreferrer" class="p-2 rounded-lg hover:bg-surface-container-high text-primary-container" title="Cómo llegar"><span class="material-symbols-outlined">directions</span></a>`;
  } catch (err) {
    console.warn("trip stop: skipping directions link for invalid coords", { lat, lon, err: err.message });
    return "";
  }
}

function stopCard(s, i) {
  const st = s.station;
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
      ${directionsAnchorHtml(st.latitude, st.longitude)}
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

function showShare(formData, plan) {
  const publicUrl = window.__APP_CONFIG__?.public_url || "";
  const shareUrl = buildShareUrl(formData, publicUrl);
  const text = `¡He planificado un viaje de ${formData.origin} a ${formData.destination} con paradas para repostar al mejor precio! ⛽`;

  const copyBtn = document.getElementById("trip-share-copy");
  const waBtn = document.getElementById("trip-share-whatsapp");
  const tgBtn = document.getElementById("trip-share-telegram");
  const xBtn = document.getElementById("trip-share-x");
  const nativeBtn = document.getElementById("trip-share-native");

  copyBtn.dataset.url = shareUrl;
  waBtn.href = `https://wa.me/?text=${encodeURIComponent(text + " " + shareUrl)}`;
  tgBtn.href = `https://t.me/share/url?url=${encodeURIComponent(shareUrl)}&text=${encodeURIComponent(text)}`;
  xBtn.href = `https://twitter.com/intent/tweet?text=${encodeURIComponent(text)}&url=${encodeURIComponent(shareUrl)}`;
  nativeBtn.dataset.url = shareUrl;
  nativeBtn.dataset.text = text;

  currentNavUrls = buildNavUrls({
    origin: plan.origin_coords,
    destination: plan.destination_coords,
    waypoints: plan.stops.map((s) => [s.station.latitude, s.station.longitude]),
  });
  document.getElementById("nav-google").href = currentNavUrls.google;
  document.getElementById("nav-waze").href = currentNavUrls.waze;
  document.getElementById("nav-waze-label").textContent = `(${currentNavUrls.wazeLabel})`;
  document.getElementById("nav-apple").href = currentNavUrls.apple;
  reorderNavTiles();

  // Always push a relative URL — using the canonical publicUrl here would throw
  // a SecurityError when public_url differs from the current origin (e.g. staging).
  history.pushState({}, "", window.location.pathname + shareUrl.slice(shareUrl.indexOf("?")));
  document.getElementById("trip-actions").classList.remove("hidden");
}

function hideShare() {
  document.getElementById("trip-actions").classList.add("hidden");
  closeDialog("trip-share-dialog");
  closeDialog("trip-nav-dialog");
  currentNavUrls = null;
}

// Reorder + filter the picker tiles per platform. Tiles whose provider isn't
// in the platform's visible list (e.g., Apple Maps on Android) get hidden;
// the rest are reordered so the most-likely-installed app sits first. The
// Navegar dialog markup defines a static superset; this prunes/reorders at
// runtime when URLs are assigned.
function reorderNavTiles() {
  const grid = document.querySelector("#trip-nav-dialog .trip-action-sheet__grid");
  if (!grid) return;
  const tiles = {
    google: document.getElementById("nav-google"),
    apple:  document.getElementById("nav-apple"),
    waze:   document.getElementById("nav-waze"),
  };
  const visible = new Set(getProviderOrder());
  Object.entries(tiles).forEach(([provider, tile]) => {
    if (!tile) return;
    tile.classList.toggle("hidden", !visible.has(provider));
  });
  visible.forEach((provider) => {
    const tile = tiles[provider];
    if (tile) grid.appendChild(tile);  // appendChild on existing node moves it
  });
}

function openDialog(id) {
  const dlg = document.getElementById(id);
  if (!dlg || dlg.open) return;
  if (typeof dlg.showModal === "function") dlg.showModal();
  else dlg.setAttribute("open", "");
}

function closeDialog(id) {
  const dlg = document.getElementById(id);
  if (!dlg || !dlg.open) return;
  if (typeof dlg.close === "function") dlg.close();
  else dlg.removeAttribute("open");
}

function resetPlanView(message = "Introduce origen y destino para planificar.") {
  hideAssumptions();
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
    // Directions icon: intercept and route through the platform-aware opener
    // instead of letting the bare <a href> open Google Maps everywhere.
    // Modifier-clicks (Cmd/Ctrl/Shift/middle) bypass the interception so the
    // browser's native "open in new tab" still works for desktop users.
    const navLink = e.target.closest("a[data-nav-smart]");
    if (navLink) {
      if (e.metaKey || e.ctrlKey || e.shiftKey || e.button !== 0) return;
      e.preventDefault();
      const lat = parseFloat(navLink.dataset.lat);
      const lon = parseFloat(navLink.dataset.lon);
      if (Number.isFinite(lat) && Number.isFinite(lon)) {
        openSmartNav(buildNavUrls({ destination: [lat, lon] }));
      }
      return;
    }
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
    min_fuel_at_destination_pct: parseFloat(data.get("min_fuel_at_destination_pct")),
    labels: getSelectedLabels(selectedLabels),
  };
  try {
    banner("info", "Calculando ruta…");
    const resp = await api("/trip/plan", { method: "POST", body: JSON.stringify(body) });
    hideBanner();
    const plan = resp.plan;
    renderAssumptions(body);
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

  const minFuelDestInput = document.querySelector('input[name="min_fuel_at_destination_pct"]');
  const minFuelDestLabel = document.getElementById("min-fuel-dest-val");
  minFuelDestInput.addEventListener("input", () => {
    if (parseInt(minFuelDestInput.value, 10) > 80) minFuelDestInput.value = 80;
    minFuelDestLabel.textContent = `${minFuelDestInput.value}%`;
  });

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

  // "Ajustar parámetros" in the assumptions disclosure opens the form's
  // Opciones avanzadas section so users can refine the inputs that shaped
  // the estimate. The anchor href handles the scroll for free.
  document.getElementById("trip-assumptions-edit")?.addEventListener("click", () => {
    const advanced = document.getElementById("advanced-section");
    if (advanced) advanced.open = true;
  });

  // Dialog open/close wiring
  document.getElementById("trip-share-button")?.addEventListener("click", () => openDialog("trip-share-dialog"));
  // Primary "Navegar" button opens the provider picker. Tiles inside are
  // platform-aware (see reorderNavTiles): order changes per platform and
  // Apple Maps is hidden on Android since the app isn't available there.
  document.getElementById("trip-nav-button")?.addEventListener("click", () => openDialog("trip-nav-dialog"));
  document.querySelectorAll("[data-close-dialog]").forEach((btn) => {
    btn.addEventListener("click", () => closeDialog(btn.dataset.closeDialog));
  });
  // Click on backdrop (outside the inner panel) closes the dialog
  document.querySelectorAll("dialog.trip-action-sheet").forEach((dlg) => {
    dlg.addEventListener("click", (e) => { if (e.target === dlg) closeDialog(dlg.id); });
  });

  // Close dialog after picking an external link target (visual feedback / clean state)
  document.querySelectorAll(
    "#trip-share-whatsapp, #trip-share-telegram, #trip-share-x",
  ).forEach((el) => el.addEventListener("click", () => closeDialog("trip-share-dialog")));

  // Nav tiles: route through openInMaps so behavior is platform-correct
  // (same-window on mobile to keep Universal Links smooth, new tab on desktop).
  [
    ["nav-google", "google"],
    ["nav-apple",  "apple"],
    ["nav-waze",   "waze"],
  ].forEach(([id, provider]) => {
    document.getElementById(id)?.addEventListener("click", (e) => {
      e.preventDefault();
      closeDialog("trip-nav-dialog");
      if (currentNavUrls) openInMaps(provider, currentNavUrls);
    });
  });

  // Native Web Share API — only surface the tile if supported
  const nativeBtn = document.getElementById("trip-share-native");
  if (nativeBtn && typeof navigator.share === "function") {
    nativeBtn.classList.remove("hidden");
    nativeBtn.addEventListener("click", async () => {
      const url = nativeBtn.dataset.url;
      const text = nativeBtn.dataset.text;
      if (!url) return;
      try {
        await navigator.share({ title: "Plan de viaje", text, url });
        closeDialog("trip-share-dialog");
      } catch (_) { /* user cancelled or share failed silently */ }
    });
  }

  const copyBtn = document.getElementById("trip-share-copy");
  const copyLabel = document.getElementById("trip-share-copy-label");
  const copyIconIdle = copyBtn?.querySelector('[data-copy-state="idle"]');
  const copyIconDone = copyBtn?.querySelector('[data-copy-state="done"]');
  copyBtn?.addEventListener("click", () => {
    const url = copyBtn.dataset.url;
    if (!url) return;
    navigator.clipboard.writeText(url).then(() => {
      copyIconIdle.classList.add("hidden");
      copyIconDone.classList.remove("hidden");
      copyLabel.textContent = "¡Copiado!";
      setTimeout(() => {
        copyIconIdle.classList.remove("hidden");
        copyIconDone.classList.add("hidden");
        copyLabel.textContent = "Copiar enlace";
        closeDialog("trip-share-dialog");
      }, 1200);
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
    if (params.min_fuel_at_destination_pct != null) {
      minFuelDestInput.value = params.min_fuel_at_destination_pct;
      minFuelDestLabel.textContent = `${params.min_fuel_at_destination_pct}%`;
    }
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
