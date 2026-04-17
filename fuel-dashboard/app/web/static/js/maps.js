/* global L */
// Leaflet helpers. Uses OpenStreetMap tiles.
import { escapeHtml } from "./format.js";

const TILE_URL = "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png";
const TILE_ATTRIB = '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>';
const SPAIN_CENTER = [40.4168, -3.7038];

export function createMap(el, { center = SPAIN_CENTER, zoom = 6 } = {}) {
  const map = L.map(el, { zoomControl: true, attributionControl: true }).setView(center, zoom);
  L.tileLayer(TILE_URL, { attribution: TILE_ATTRIB, maxZoom: 19 }).addTo(map);
  return map;
}

export function clearLayer(map, group) {
  if (group) { group.clearLayers(); return group; }
  return L.layerGroup().addTo(map);
}

const PRICE_BINS = [
  { max: 1.4, color: "#004e33" },
  { max: 1.5, color: "#0e7b52" },
  { max: 1.6, color: "#b3923a" },
  { max: 1.75, color: "#a33d3d" },
  { max: Infinity, color: "#6e1414" },
];
export function priceColor(price) {
  for (const bin of PRICE_BINS) if (price <= bin.max) return bin.color;
  return "#333";
}

export function stationMarker(station, { highlight = false } = {}) {
  const color = highlight ? "#001642" : priceColor(station.price);
  const size = highlight ? 14 : 10;
  return L.circleMarker([station.latitude, station.longitude], {
    radius: size,
    color: color,
    fillColor: color,
    fillOpacity: 0.85,
    weight: highlight ? 3 : 1.5,
  }).bindPopup(renderPopup(station));
}

function renderPopup(s) {
  const price = s.price != null ? `${s.price.toFixed(3)} €/l` : "—";
  const addr = [s.address, s.municipality].filter(Boolean).join(", ");
  const dist = s.distance_km != null ? `${s.distance_km.toFixed(1)} km` : "";
  const mapsUrl = `https://www.google.com/maps/dir/?api=1&destination=${s.latitude},${s.longitude}`;
  return `
    <div style="font-family: Inter, sans-serif; min-width: 200px;">
      <div style="font-weight: 700; font-family: Manrope, sans-serif; font-size: 14px;">${escapeHtml(s.label)}</div>
      <div style="color: #444650; font-size: 12px; margin: 4px 0;">${escapeHtml(addr)}</div>
      <div style="display:flex; justify-content: space-between; margin-top: 6px;">
        <span style="font-weight: 700; color: ${priceColor(s.price)};">${price}</span>
        <span style="color: #747782; font-size: 12px;">${dist}</span>
      </div>
      <a href="${mapsUrl}" target="_blank" rel="noopener" style="display: inline-block; margin-top: 8px; color: #0453cd; font-size: 12px; font-weight: 600;">Cómo llegar →</a>
    </div>`;
}

export function drawStationsTracked(map, group, stations) {
  group = clearLayer(map, group);
  const markers = stations.map((s) => stationMarker(s).addTo(group));
  if (markers.length) {
    const bounds = L.latLngBounds(stations.map((s) => [s.latitude, s.longitude]));
    map.invalidateSize();
    map.fitBounds(bounds, { padding: [32, 32], maxZoom: 14 });
  }
  return { group, markers };
}

export function drawStations(map, group, stations) {
  return drawStationsTracked(map, group, stations).group;
}

export function drawSearchPin(map, group, lat, lon, label) {
  group = clearLayer(map, group);
  const icon = L.divIcon({
    className: "",
    html: `<span class="material-symbols-outlined" style="font-size:32px;color:#0453cd;filter:drop-shadow(0 1px 2px rgba(0,0,0,.4));">location_on</span>`,
    iconSize: [32, 32],
    iconAnchor: [16, 32],
    popupAnchor: [0, -32],
  });
  L.marker([lat, lon], { icon })
    .bindPopup(`<b style="font-family:Manrope,sans-serif;">${escapeHtml(label)}</b>`)
    .addTo(group);
  return group;
}

export function drawZipBoundary(map, group, geojson) {
  group = clearLayer(map, group);
  if (!geojson || !geojson.type) return group;
  L.geoJSON(geojson, {
    style: {
      color: "#0453cd",
      weight: 2,
      fillColor: "#0453cd",
      fillOpacity: 0.08,
      dashArray: "6 4",
    },
  }).addTo(group);
  return group;
}

export function drawRoute(map, coords, { color = "#001642" } = {}) {
  if (!coords || coords.length < 2) return null;
  // Expect [lon, lat] pairs from OSRM; Leaflet expects [lat, lon]
  const latlngs = coords.map((c) => [c[1], c[0]]);
  return L.polyline(latlngs, { color, weight: 5, opacity: 0.8 }).addTo(map);
}

export function drawGeoJSON(map, geojson, { valueKey = "avg_price", onEachFeature = null, maxZoom = 12 } = {}) {
  if (!geojson || !geojson.features?.length) return null;
  const layer = L.geoJSON(geojson, {
    style: (feature) => {
      const v = feature.properties?.[valueKey];
      return {
        color: "#00296d",
        weight: 1,
        fillColor: v != null ? priceColor(v) : "#c4c6d2",
        fillOpacity: 0.6,
      };
    },
    onEachFeature: (feature, layer) => {
      const props = feature.properties || {};
      const label = props.zip_code || props.province || props.district || props.municipality || props.name || "—";
      const avg = props.avg_price != null ? `${props.avg_price.toFixed(3)} €/l` : "—";
      layer.bindPopup(`<b>${escapeHtml(label)}</b><br>${avg}`);
      onEachFeature && onEachFeature(feature, layer);
    },
  }).addTo(map);
  const bounds = layer.getBounds?.();
  if (bounds?.isValid?.()) {
    map.fitBounds(bounds, { padding: [24, 24], maxZoom });
  }
  return layer;
}
