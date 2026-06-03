import { describe, it, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import {
  buildNavUrls,
  detectPlatform,
  getProviderOrder,
  openInMaps,
  openSmartNav,
} from "../../app/web/static/js/openInMaps.js";

const MAD = [40.4168, -3.7038];
const SEV = [37.3886, -5.9823];
const CHICLANA = [36.4188, -6.1444];
const STOP_A = [38.5, -4.5];
const STOP_B = [37.5, -5.5];

describe("buildNavUrls — destination only (station card use case)", () => {
  const { google, apple, waze, wazeLabel } = buildNavUrls({ destination: SEV });

  it("Google uses api=1 with destination only", () => {
    assert.match(google, /^https:\/\/www\.google\.com\/maps\/dir\/\?/);
    assert.match(google, /api=1/);
    assert.match(google, /destination=37\.3886%2C-5\.9823/);
    assert.match(google, /travelmode=driving/);
    assert.doesNotMatch(google, /origin=/);
    assert.doesNotMatch(google, /waypoints=/);
  });

  it("Apple uses daddr only (no saddr, no waypoints)", () => {
    assert.match(apple, /^https:\/\/maps\.apple\.com\/\?/);
    assert.match(apple, /daddr=37\.3886%2C-5\.9823/);
    assert.match(apple, /dirflg=d/);
    assert.doesNotMatch(apple, /saddr=/);
    assert.doesNotMatch(apple, /\+to:/);
  });

  it("Waze targets the destination and labels it as such", () => {
    assert.equal(waze, "https://waze.com/ul?ll=37.3886,-5.9823&navigate=yes");
    assert.equal(wazeLabel, "destino");
  });
});

describe("buildNavUrls — origin + destination (no stops)", () => {
  const { google, apple, waze, wazeLabel } = buildNavUrls({ origin: MAD, destination: SEV });

  it("Google includes origin and destination", () => {
    assert.match(google, /origin=40\.4168%2C-3\.7038/);
    assert.match(google, /destination=37\.3886%2C-5\.9823/);
    assert.doesNotMatch(google, /waypoints=/);
  });

  it("Apple includes saddr + daddr", () => {
    assert.match(apple, /saddr=40\.4168%2C-3\.7038/);
    assert.match(apple, /daddr=37\.3886%2C-5\.9823/);
    assert.doesNotMatch(apple, /\+to:/);
  });

  it("Waze still targets the final destination when no stops", () => {
    assert.equal(waze, "https://waze.com/ul?ll=37.3886,-5.9823&navigate=yes");
    assert.equal(wazeLabel, "destino");
  });
});

describe("buildNavUrls — full route with multiple waypoints (trip planner use case)", () => {
  const { google, apple, waze, wazeLabel } = buildNavUrls({
    origin: MAD,
    destination: CHICLANA,
    waypoints: [STOP_A, STOP_B],
  });

  it("Google encodes waypoints with | separator", () => {
    assert.match(google, /waypoints=38\.5%2C-4\.5%7C37\.5%2C-5\.5/);
    assert.match(google, /origin=40\.4168%2C-3\.7038/);
    assert.match(google, /destination=36\.4188%2C-6\.1444/);
  });

  it("Apple chains stops via +to: inside daddr, with final destination last", () => {
    assert.match(apple, /saddr=40\.4168%2C-3\.7038/);
    assert.match(apple, /daddr=38\.5%2C-4\.5\+to:37\.5%2C-5\.5\+to:36\.4188%2C-6\.1444/);
  });

  it("Waze falls back to the first stop and labels it accordingly", () => {
    assert.equal(waze, "https://waze.com/ul?ll=38.5,-4.5&navigate=yes");
    assert.equal(wazeLabel, "solo 1.ª parada");
  });
});

describe("buildNavUrls — invalid input", () => {
  it("throws when destination is missing", () => {
    assert.throws(() => buildNavUrls({}), /destination/);
  });

  it("throws when destination is malformed", () => {
    assert.throws(() => buildNavUrls({ destination: [40] }), /destination/);
  });

  it("throws when destination contains NaN", () => {
    assert.throws(() => buildNavUrls({ destination: [NaN, -3.7] }), /destination/);
  });

  it("throws when destination contains Infinity", () => {
    assert.throws(() => buildNavUrls({ destination: [40.4, Infinity] }), /destination/);
  });

  it("throws when origin is provided but malformed", () => {
    assert.throws(() => buildNavUrls({ origin: [NaN, 0], destination: SEV }), /origin/);
  });

  it("silently ignores malformed waypoints", () => {
    const { google } = buildNavUrls({
      destination: SEV,
      waypoints: [null, [38.5], [NaN, 0], STOP_A],
    });
    assert.match(google, /waypoints=38\.5%2C-4\.5$/);
  });
});

describe("detectPlatform", () => {
  const MAC_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15";

  it("detects iPhone", () => {
    const ua = "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15";
    assert.equal(detectPlatform(ua), "ios");
  });

  it("detects iPad (legacy UA)", () => {
    assert.equal(detectPlatform("Mozilla/5.0 (iPad; CPU OS 17_0)"), "ios");
  });

  it("detects iPadOS 13+ masquerading as Macintosh (touch points > 1)", () => {
    assert.equal(detectPlatform(MAC_UA, 5), "ios");
  });

  it("treats true Macintosh (no touch) as desktop", () => {
    assert.equal(detectPlatform(MAC_UA, 0), "desktop");
    assert.equal(detectPlatform(MAC_UA, 1), "desktop");
  });

  it("detects Android", () => {
    const ua = "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36";
    assert.equal(detectPlatform(ua, 5), "android");
  });

  it("falls back to desktop for everything else", () => {
    assert.equal(detectPlatform(""), "desktop");
  });
});

describe("getProviderOrder", () => {
  it("iOS shows all three with Apple Maps first", () => {
    assert.deepEqual(getProviderOrder("ios"), ["apple", "google", "waze"]);
  });

  it("Android hides Apple Maps and prefers Google first", () => {
    assert.deepEqual(getProviderOrder("android"), ["google", "waze"]);
  });

  it("Desktop shows all three with Google Maps first", () => {
    assert.deepEqual(getProviderOrder("desktop"), ["google", "apple", "waze"]);
  });
});

describe("openInMaps — navigation behavior", () => {
  let assignCalls;
  let openCalls;
  const URLS = {
    google: "https://www.google.com/maps/dir/?api=1&destination=37.4,-5.9",
    apple:  "https://maps.apple.com/?daddr=37.4,-5.9&dirflg=d",
    waze:   "https://waze.com/ul?ll=37.4,-5.9&navigate=yes",
  };

  beforeEach(() => {
    assignCalls = [];
    openCalls = [];
    globalThis.window = {
      location: { assign: (url) => assignCalls.push(url) },
      open: (...args) => openCalls.push(args),
    };
  });

  afterEach(() => {
    delete globalThis.window;
  });

  it("on iOS navigates same-window so Universal Links route to the native app", () => {
    openInMaps("apple", URLS, { platform: "ios" });
    assert.deepEqual(assignCalls, [URLS.apple]);
    assert.equal(openCalls.length, 0);
  });

  it("on Android navigates same-window so the intent system routes to the native app", () => {
    openInMaps("google", URLS, { platform: "android" });
    assert.deepEqual(assignCalls, [URLS.google]);
    assert.equal(openCalls.length, 0);
  });

  it("on desktop opens in a new tab with noopener so the trip view stays put", () => {
    openInMaps("google", URLS, { platform: "desktop" });
    assert.equal(assignCalls.length, 0);
    assert.equal(openCalls.length, 1);
    assert.equal(openCalls[0][0], URLS.google);
    assert.equal(openCalls[0][1], "_blank");
    assert.match(openCalls[0][2], /noopener/);
  });

  it("respects explicit sameWindow:false even on mobile", () => {
    openInMaps("google", URLS, { platform: "ios", sameWindow: false });
    assert.equal(assignCalls.length, 0);
    assert.equal(openCalls.length, 1);
  });

  it("respects explicit sameWindow:true even on desktop", () => {
    openInMaps("google", URLS, { platform: "desktop", sameWindow: true });
    assert.equal(assignCalls.length, 1);
    assert.equal(openCalls.length, 0);
  });

  it("throws on unknown provider", () => {
    assert.throws(() => openInMaps("yahoo", URLS, { platform: "ios" }), /unknown provider/);
  });

  it("no-ops when the URL for the chosen provider is missing", () => {
    openInMaps("google", { google: undefined, apple: URLS.apple, waze: URLS.waze }, { platform: "ios" });
    assert.equal(assignCalls.length, 0);
    assert.equal(openCalls.length, 0);
  });
});

describe("openSmartNav — picks platform default", () => {
  let assignCalls;
  let openCalls;
  const URLS = {
    google: "https://www.google.com/maps/dir/?api=1&destination=37.4,-5.9",
    apple:  "https://maps.apple.com/?daddr=37.4,-5.9&dirflg=d",
    waze:   "https://waze.com/ul?ll=37.4,-5.9&navigate=yes",
  };

  beforeEach(() => {
    assignCalls = [];
    openCalls = [];
    globalThis.window = {
      location: { assign: (url) => assignCalls.push(url) },
      open: (...args) => openCalls.push(args),
    };
  });

  afterEach(() => {
    delete globalThis.window;
  });

  it("iOS → Apple Maps (same-window)", () => {
    openSmartNav(URLS, { platform: "ios" });
    assert.deepEqual(assignCalls, [URLS.apple]);
  });

  it("Android → Google Maps (same-window)", () => {
    openSmartNav(URLS, { platform: "android" });
    assert.deepEqual(assignCalls, [URLS.google]);
  });

  it("Desktop → Google Maps (new tab)", () => {
    openSmartNav(URLS, { platform: "desktop" });
    assert.equal(assignCalls.length, 0);
    assert.equal(openCalls[0][0], URLS.google);
  });
});
