import { expect, test } from "@playwright/test";

import { TripPage } from "./pages/trip-page";
import { blockExternalNetwork, setFixture } from "./support";

test.beforeEach(async ({ page }) => {
  await blockExternalNetwork(page);
  await setFixture(page, "happy_path");
});

test("trip planner renders KPIs, stops, and alternatives from the happy path fixture", async ({ page }) => {
  const tripPage = new TripPage(page);
  await tripPage.goto();

  const requestPromise = page.waitForRequest((request) => request.url().includes("/api/v1/trip/plan"));
  await tripPage.plan("Madrid", "Sevilla");

  const request = await requestPromise;
  expect(request.postDataJSON()).toMatchObject({
    destination: "Sevilla",
    fuel_type: "gasoline_95_e5_price",
    origin: "Madrid",
  });

  await expect(page.getByTestId("trip-kpis")).toBeVisible();
  await expect(page.getByTestId("trip-kpis")).toContainText("%");
  await expect(page.getByTestId("trip-stops").getByTestId("trip-stop-card")).toHaveCount(2);
  await expect(page.getByTestId("trip-alt-plans").getByTestId("trip-alt-plan-card")).toHaveCount(2);
  await expect(page.getByTestId("trip-map")).toBeVisible();
  await expect(page.getByTestId("trip-floor-warning")).toBeHidden();

  // Tank-level chart renders at the bottom: one Plotly bar trace sampled across
  // even distance buckets for the happy-path fixture.
  await expect(page.getByTestId("trip-fuel-chart-wrap")).toBeVisible();
  const chart = page.getByTestId("trip-fuel-chart");
  await expect(chart).toHaveAttribute("data-plot-ready", "true");
  await expect(chart).toHaveAttribute("data-plot-traces", "1");

  const stopCard = page.getByTestId("trip-stop-card").first();
  const mapsLink = stopCard.locator('a[title="Cómo llegar"]');
  // Href stays Google Maps (desktop / right-click affordance); the click is
  // intercepted at runtime and routed through the platform-aware opener.
  await expect(mapsLink).toHaveAttribute("href", /google\.com\/maps.*destination=/);
  await expect(mapsLink).toHaveAttribute("data-nav-smart", "");
  await expect(mapsLink).not.toHaveAttribute("target", /.+/);
});

test("swap button and fuel-level slider update the form state", async ({ page }) => {
  const tripPage = new TripPage(page);
  await tripPage.goto();

  await tripPage.originInput.fill("Madrid");
  await tripPage.destinationInput.fill("Valencia");
  await page.getByTestId("trip-swap").click();

  await expect(tripPage.originInput).toHaveValue("Valencia");
  await expect(tripPage.destinationInput).toHaveValue("Madrid");

  await page.getByTestId("trip-fuel-level").evaluate((element, value) => {
    const input = element as HTMLInputElement;
    input.value = String(value);
    input.dispatchEvent(new Event("input", { bubbles: true }));
  }, 55);
  await expect(page.locator("#fuel-level-val")).toHaveText("55%");
});

test("assumptions disclosure shows every parameter after a successful plan", async ({ page }) => {
  const tripPage = new TripPage(page);
  await tripPage.goto();
  await tripPage.plan("Madrid", "Sevilla");

  const card = page.getByTestId("trip-assumptions");
  await expect(card).toBeVisible();
  await expect(card).toContainText("Resultado aproximado basado en:");

  const chips = page.getByTestId("trip-assumptions-list").locator("li");
  await expect(chips).toHaveCount(5);
  await expect(card).not.toContainText("personalizado");

  await page.getByTestId("trip-assumptions-edit").click();
  await expect(page.locator("#advanced-section")).toHaveAttribute("open", "");
});

test("customizing a parameter tags its chip as personalizado", async ({ page }) => {
  const tripPage = new TripPage(page);
  await tripPage.goto();

  // Open Opciones avanzadas to expose the consumption input, then change it from 7 to 8.
  await page.locator("#advanced-section").evaluate((d) => ((d as HTMLDetailsElement).open = true));
  await page.locator('input[name="consumption_lper100km"]').fill("8");
  await tripPage.plan("Madrid", "Sevilla");

  const chips = page.getByTestId("trip-assumptions-list").locator("li");
  const consumo = chips.filter({ hasText: "Consumo 8" });
  await expect(consumo).toContainText("personalizado");
  const deposito = chips.filter({ hasText: "Depósito" });
  await expect(deposito).not.toContainText("personalizado");
});

test("no-stop journeys keep the success state but collapse alternatives", async ({ page }) => {
  await setFixture(page, "trip_no_stops");
  const tripPage = new TripPage(page);
  await tripPage.goto();

  await tripPage.plan("Madrid", "Sevilla");

  await expect(page.getByTestId("trip-stops")).toContainText("No hacen falta paradas");
  await expect(page.getByTestId("trip-alt-plans-wrap")).toBeHidden();
  // A stop-free trip still charts the single origin→destination leg.
  await expect(page.getByTestId("trip-fuel-chart-wrap")).toBeVisible();
  await expect(page.getByTestId("trip-fuel-chart")).toHaveAttribute("data-plot-ready", "true");
});

test("arrival-fuel floor that cannot be met surfaces a visible warning", async ({ page }) => {
  await setFixture(page, "trip_floor_unmet");
  const tripPage = new TripPage(page);
  await tripPage.goto();

  await tripPage.plan("Madrid", "Sevilla");

  await expect(page.getByTestId("trip-kpis")).toBeVisible();
  await expect(page.getByTestId("trip-floor-warning")).toBeVisible();
  await expect(page.getByTestId("trip-floor-warning")).toContainText("No se garantiza");
});

test("trip planner errors reset the previous plan", async ({ page }) => {
  const tripPage = new TripPage(page);
  await tripPage.goto();

  const firstPlanDone = page.waitForResponse((r) => r.url().includes("/api/v1/trip/plan"));
  await tripPage.plan("Madrid", "Sevilla");
  await firstPlanDone;
  await expect(page.getByTestId("trip-stops").getByTestId("trip-stop-card")).toHaveCount(2);

  await setFixture(page, "trip_error");
  await tripPage.plan("Madrid", "Sevilla");

  await expect(page.getByTestId("trip-banner")).toContainText("Route unavailable for selected itinerary");
  await expect(page.getByTestId("trip-stops")).toContainText("Route unavailable for selected itinerary");
  await expect(page.getByTestId("trip-alt-plans-wrap")).toBeHidden();
  await expect(page.getByTestId("trip-fuel-chart-wrap")).toBeHidden();
});

test("share dialog exposes copy, WhatsApp, and Telegram tiles after a successful plan", async ({ page }) => {
  const tripPage = new TripPage(page);
  await tripPage.goto();

  const firstDone = page.waitForResponse((r) => r.url().includes("/api/v1/trip/plan"));
  await tripPage.plan("Madrid", "Sevilla");
  await firstDone;

  await expect(page.getByTestId("trip-actions")).toBeVisible();
  await page.getByTestId("trip-share-button").click();
  const shareDialog = page.getByTestId("trip-share-dialog");
  await expect(shareDialog).toBeVisible();
  await expect(shareDialog.getByTestId("trip-share-copy")).toBeVisible();
  await expect(shareDialog.getByTestId("trip-share-whatsapp")).toHaveAttribute("href", /wa\.me/);
  await expect(shareDialog.getByTestId("trip-share-telegram")).toHaveAttribute("href", /t\.me\/share/);
  await expect(shareDialog.getByTestId("trip-share-x")).toHaveAttribute("href", /twitter\.com\/intent\/tweet/);

  // Close dialog before next assertion
  await page.keyboard.press("Escape");

  await setFixture(page, "trip_error");
  await tripPage.plan("Madrid", "Sevilla");

  await expect(page.getByTestId("trip-actions")).toBeHidden();
});

test("Navegar opens picker with Google Maps, Waze, and Apple Maps tiles after a successful plan", async ({ page }) => {
  const tripPage = new TripPage(page);
  await tripPage.goto();

  await tripPage.plan("Madrid", "Sevilla");

  await expect(page.getByTestId("trip-actions")).toBeVisible();
  await page.getByTestId("trip-nav-button").click();
  const navDialog = page.getByTestId("trip-nav-dialog");
  await expect(navDialog).toBeVisible();
  // Modern api=1 Google Maps URL (Universal-Link friendly, multi-waypoint).
  await expect(navDialog.getByTestId("trip-nav-google")).toHaveAttribute("href", /google\.com\/maps\/dir\/\?api=1/);
  await expect(navDialog.getByTestId("trip-nav-waze")).toHaveAttribute("href", /^https:\/\/waze\.com\/ul\?/);
  await expect(navDialog.getByTestId("trip-nav-apple")).toHaveAttribute("href", /maps\.apple\.com.*daddr=/);
  await expect(navDialog.getByTestId("trip-nav-apple")).toContainText("ruta completa");
});

test.describe("iOS emulation", () => {
  test.use({
    userAgent:
      "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
  });

  test("picker reorders Apple Maps to the first tile and keeps all three visible", async ({ page }) => {
    const tripPage = new TripPage(page);
    await tripPage.goto();
    await tripPage.plan("Madrid", "Sevilla");

    await page.getByTestId("trip-nav-button").click();
    // All three tiles visible on iOS, Apple-first order.
    await expect(page.getByTestId("trip-nav-apple")).toBeVisible();
    await expect(page.getByTestId("trip-nav-google")).toBeVisible();
    await expect(page.getByTestId("trip-nav-waze")).toBeVisible();
    const visibleTiles = page.locator("#trip-nav-dialog .trip-action-sheet__grid > a:not(.hidden)");
    await expect(visibleTiles.nth(0)).toHaveAttribute("data-testid", "trip-nav-apple");
    await expect(visibleTiles.nth(1)).toHaveAttribute("data-testid", "trip-nav-google");
    await expect(visibleTiles.nth(2)).toHaveAttribute("data-testid", "trip-nav-waze");
  });

  test("round trip with more than 3 waypoints sends Google only the first leg of the route", async ({ page }) => {
    const tripPage = new TripPage(page);
    await tripPage.goto();
    await page.getByTestId("trip-round-trip").check();
    await tripPage.plan("Madrid", "Sevilla");

    // 2 outbound stops + Sevilla + 1 return stop = 4 waypoints > 3 allowed on mobile:
    // Google keeps the first 3 and ends at the return stop instead of skipping points.
    await page.getByTestId("trip-nav-button").click();
    await expect(page.getByTestId("trip-nav-google")).toContainText("primer tramo");
    const params = new URL((await page.getByTestId("trip-nav-google").getAttribute("href"))!).searchParams;
    expect(params.get("waypoints")).toBe("40.03,-3.6|38.34,-3.52|37.3891,-5.9845");
    expect(params.get("destination")).toBe("37.88,-4.77");
    // Apple Maps has no documented cap and keeps the full route back to Madrid.
    await expect(page.getByTestId("trip-nav-apple")).toHaveAttribute("href", /\+to:40\.4168%2C-3\.7038/);
  });
});

test.describe("Android emulation", () => {
  test.use({
    userAgent:
      "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Mobile Safari/537.36",
  });

  test("picker hides Apple Maps on Android and surfaces Google + Waze only", async ({ page }) => {
    const tripPage = new TripPage(page);
    await tripPage.goto();
    await tripPage.plan("Madrid", "Sevilla");

    await page.getByTestId("trip-nav-button").click();
    await expect(page.getByTestId("trip-nav-google")).toBeVisible();
    await expect(page.getByTestId("trip-nav-waze")).toBeVisible();
    // Apple Maps tile is hidden — the app isn't available on Android.
    await expect(page.getByTestId("trip-nav-apple")).toBeHidden();
    const visibleTiles = page.locator("#trip-nav-dialog .trip-action-sheet__grid > a:not(.hidden)");
    await expect(visibleTiles).toHaveCount(2);
    await expect(visibleTiles.nth(0)).toHaveAttribute("data-testid", "trip-nav-google");
    await expect(visibleTiles.nth(1)).toHaveAttribute("data-testid", "trip-nav-waze");
  });
});

test("action row hides after a plan error", async ({ page }) => {
  const tripPage = new TripPage(page);
  await tripPage.goto();

  const firstDone = page.waitForResponse((r) => r.url().includes("/api/v1/trip/plan"));
  await tripPage.plan("Madrid", "Sevilla");
  await firstDone;
  await expect(page.getByTestId("trip-actions")).toBeVisible();

  await setFixture(page, "trip_error");
  await tripPage.plan("Madrid", "Sevilla");
  await expect(page.getByTestId("trip-actions")).toBeHidden();
});

test("share URL pre-populates form and auto-runs plan on load", async ({ page }) => {
  const planRequest = page.waitForRequest((r) => r.url().includes("/api/v1/trip/plan"));
  await page.goto(
    "/trip?origin=Madrid&destination=Sevilla&fuel_type=gasoline_95_e5_price" +
    "&consumption_lper100km=7&tank_liters=40&fuel_level_pct=25&max_detour_minutes=5" +
    "&min_fuel_at_destination_pct=30"
  );

  await expect(page.getByTestId("trip-origin-input")).toHaveValue("Madrid");
  await expect(page.getByTestId("trip-destination-input")).toHaveValue("Sevilla");
  await expect(page.getByTestId("trip-min-fuel-dest")).toHaveValue("30");

  const req = await planRequest;
  expect(req.postDataJSON()).toMatchObject({ origin: "Madrid", destination: "Sevilla", fuel_type: "gasoline_95_e5_price" });

  await expect(page.getByTestId("trip-actions")).toBeVisible();
});

test("round trip toggle plans both legs and labels stops and KPIs", async ({ page }) => {
  const tripPage = new TripPage(page);
  await tripPage.goto();

  await page.getByTestId("trip-round-trip").check();
  await expect(page.locator("#min-fuel-dest-title")).toHaveText("Combustible mínimo al volver al origen");

  const requestPromise = page.waitForRequest((request) => request.url().includes("/api/v1/trip/plan"));
  await tripPage.plan("Madrid", "Sevilla");
  expect((await requestPromise).postDataJSON()).toMatchObject({ round_trip: true });

  const kpis = page.getByTestId("trip-kpis");
  await expect(kpis).toContainText("Combustible en destino");
  await expect(kpis).toContainText("Combustible al volver");
  await expect(kpis).not.toContainText("Combustible al llegar");

  const legs = page.getByTestId("trip-stops").getByTestId("trip-stop-leg");
  await expect(legs).toHaveCount(3);
  await expect(legs.last()).toHaveText("Vuelta");
  await expect(page.getByTestId("trip-stop-card").last()).toContainText("BP Cordoba Norte");
  // Totals include the return-leg stop: 62.70 € outbound + 30 l × 1.474 € = 106.92 €.
  await expect(kpis).toContainText("106,92");
  await expect(page.getByTestId("trip-assumptions-list")).toContainText("Ida y vuelta");
  await expect(page.getByTestId("trip-fuel-chart")).toHaveAttribute("data-plot-ready", "true");

  await expect(page).toHaveURL(/round_trip=1/);

  // Navigation: A → outbound stops → B → return stop → A. Desktop allows 9 Google
  // waypoints, so these 4 fit and the route ends back at the origin.
  await page.getByTestId("trip-nav-button").click();
  const google = await page.getByTestId("trip-nav-google").getAttribute("href");
  const params = new URL(google!).searchParams;
  expect(params.get("destination")).toBe("40.4168,-3.7038");
  expect(params.get("waypoints")).toBe("40.03,-3.6|38.34,-3.52|37.3891,-5.9845|37.88,-4.77");
  await expect(page.getByTestId("trip-nav-google")).toContainText("ruta completa");
});

test("round trip without outbound stops labels Waze as the destination", async ({ page }) => {
  const tripPage = new TripPage(page);
  await tripPage.goto();
  await page.getByTestId("trip-round-trip").check();

  // BP only exists on the return leg, so the first waypoint is the turnaround (B).
  await page.getByTestId("trip-brands-toggle").click();
  await page.getByTestId("brand-checkbox-bp").check();
  await tripPage.plan("Madrid", "Sevilla");

  await expect(page.getByTestId("trip-stop-card")).toHaveCount(1);
  await page.getByTestId("trip-nav-button").click();
  await expect(page.locator("#nav-waze-label")).toHaveText("(destino)");
  await expect(page.getByTestId("trip-nav-waze")).toHaveAttribute("href", /ll=37\.3891,-5\.9845/);
});

test("round trip toggle restored by the browser keeps the floor label in sync", async ({ page }) => {
  // Simulate browser form restoration (reload / back navigation): the box is
  // already checked when trip.js init runs, without any change event firing.
  await page.addInitScript(() => {
    document.addEventListener("DOMContentLoaded", () => {
      (document.querySelector('input[name="round_trip"]') as HTMLInputElement).checked = true;
    });
  });
  const tripPage = new TripPage(page);
  await tripPage.goto();

  await expect(page.getByTestId("trip-round-trip")).toBeChecked();
  await expect(page.locator("#min-fuel-dest-title")).toHaveText("Combustible mínimo al volver al origen");
});

test("one-way plans show no leg badges", async ({ page }) => {
  const tripPage = new TripPage(page);
  await tripPage.goto();
  await tripPage.plan("Madrid", "Sevilla");

  await expect(page.getByTestId("trip-stop-card")).toHaveCount(2);
  await expect(page.getByTestId("trip-stop-leg")).toHaveCount(0);
  await expect(page.getByTestId("trip-kpis")).toContainText("Combustible al llegar");
});

test("share URL with round_trip=1 pre-checks the toggle and auto-runs", async ({ page }) => {
  const planRequest = page.waitForRequest((r) => r.url().includes("/api/v1/trip/plan"));
  await page.goto("/trip?origin=Madrid&destination=Sevilla&fuel_type=gasoline_95_e5_price&round_trip=1");

  await expect(page.getByTestId("trip-round-trip")).toBeChecked();
  expect((await planRequest).postDataJSON()).toMatchObject({ round_trip: true });
  await expect(page.getByTestId("trip-kpis")).toContainText("Combustible al volver");
});

test("one-way share link unchecks a round-trip box restored by the browser", async ({ page }) => {
  await page.addInitScript(() => {
    document.addEventListener("DOMContentLoaded", () => {
      (document.querySelector('input[name="round_trip"]') as HTMLInputElement).checked = true;
    });
  });
  const planRequest = page.waitForRequest((r) => r.url().includes("/api/v1/trip/plan"));
  await page.goto("/trip?origin=Madrid&destination=Sevilla&fuel_type=gasoline_95_e5_price");

  expect((await planRequest).postDataJSON()).toMatchObject({ round_trip: false });
  await expect(page.getByTestId("trip-round-trip")).not.toBeChecked();
  await expect(page.locator("#min-fuel-dest-title")).toHaveText("Combustible mínimo al llegar");
});
