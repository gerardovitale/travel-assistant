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
