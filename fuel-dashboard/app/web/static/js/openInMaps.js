// Reusable "open in maps" utility.
//
// Builds Universal-Link URLs for Google Maps, Apple Maps and Waze, picks a
// per-platform default, and routes clicks so iOS hands off to the native app
// without going through a blank Safari tab. Consumed by trip.js (full route
// with waypoints) and search.js (single-destination "Cómo llegar").
//
// Coordinates are always [latitude, longitude] tuples — matches the TripPlan
// API shape so callers don't have to remap.

const PROVIDERS = ["google", "apple", "waze"];

function isValidCoord(coord) {
  return Array.isArray(coord)
    && coord.length >= 2
    && Number.isFinite(coord[0])
    && Number.isFinite(coord[1]);
}

function fmt(coord) {
  const [lat, lon] = coord;
  return `${lat},${lon}`;
}

// Build provider URLs from a coordinate set. `destination` is required;
// `origin` and `waypoints` are optional. Returns the three URLs plus a Waze
// label describing what the single-destination Waze link points at, since
// Waze doesn't support multi-stop routes.
export function buildNavUrls({ origin, destination, waypoints = [] }) {
  if (!isValidCoord(destination)) {
    throw new Error("buildNavUrls: destination must be a finite [lat, lon] pair");
  }
  if (origin !== undefined && !isValidCoord(origin)) {
    throw new Error("buildNavUrls: origin, when provided, must be a finite [lat, lon] pair");
  }

  const dest = fmt(destination);
  const stops = waypoints.filter(isValidCoord);

  const googleParams = new URLSearchParams({ api: "1", destination: dest, travelmode: "driving" });
  if (origin) googleParams.set("origin", fmt(origin));
  if (stops.length) googleParams.set("waypoints", stops.map(fmt).join("|"));
  const google = `https://www.google.com/maps/dir/?${googleParams.toString()}`;

  // Apple Maps multi-stop encodes additional stops with "+to:" inside daddr.
  // The final destination goes last so the route ends where the user expects.
  const daddrSegments = [...stops.map(fmt), dest].join("+to:");
  const appleParams = new URLSearchParams({ daddr: daddrSegments, dirflg: "d" });
  if (origin) appleParams.set("saddr", fmt(origin));
  // URLSearchParams encodes "+" as "%2B" which Apple Maps still accepts, but
  // the "+to:" form is the documented one — emit it verbatim for readability.
  const apple = `https://maps.apple.com/?${appleParams.toString().replace(/%2Bto%3A/g, "+to:")}`;

  // Waze: no multi-stop. Aim at the first waypoint if present (next refuel
  // is what the driver wants next), otherwise at the final destination.
  const wazeTarget = stops[0] || destination;
  const waze = `https://waze.com/ul?ll=${fmt(wazeTarget)}&navigate=yes`;
  const wazeLabel = stops.length ? "solo 1.ª parada" : "destino";

  return { google, apple, waze, wazeLabel };
}

// User-agent based platform detection. Narrow and pragmatic — used only to
// pick a smart default and reorder the picker, never for security checks.
// iPadOS 13+ defaults to a Macintosh UA on Safari; we disambiguate via
// maxTouchPoints so iPad users get the iOS smart default.
export function detectPlatform(
  ua = (typeof navigator !== "undefined" ? navigator.userAgent : ""),
  touchPoints = (typeof navigator !== "undefined" ? navigator.maxTouchPoints : 0),
) {
  if (/iPhone|iPad|iPod/i.test(ua)) return "ios";
  if (/Android/i.test(ua)) return "android";
  if (/Macintosh/i.test(ua) && touchPoints > 1) return "ios";
  return "desktop";
}

// Visible providers per platform, in preferred order. Android omits Apple
// Maps because the app isn't available there. The first entry is also what
// openSmartNav() picks as the platform default.
export function getProviderOrder(platform = detectPlatform()) {
  switch (platform) {
    case "ios":     return ["apple", "google", "waze"];
    case "android": return ["google", "waze"];
    default:        return ["google", "apple", "waze"];
  }
}

// Open a specific provider. On mobile we navigate same-window so iOS hands
// off the HTTPS URL to the registered Universal Link handler (Apple Maps,
// Google Maps app, Waze) without showing an interstitial blank tab. On
// desktop we open in a new tab so the trip planner stays visible.
export function openInMaps(provider, urls, { platform = detectPlatform(), sameWindow } = {}) {
  if (!PROVIDERS.includes(provider)) throw new Error(`openInMaps: unknown provider "${provider}"`);
  const url = urls[provider];
  if (!url) return;
  const useSameWindow = sameWindow ?? (platform !== "desktop");
  if (useSameWindow) {
    window.location.assign(url);
  } else {
    window.open(url, "_blank", "noopener,noreferrer");
  }
}

// One-tap "open in the best app for this device". Used by the primary
// Navegar button and the station-card directions icon.
export function openSmartNav(urls, opts = {}) {
  const platform = opts.platform ?? detectPlatform();
  const [provider] = getProviderOrder(platform);
  openInMaps(provider, urls, { ...opts, platform });
}
