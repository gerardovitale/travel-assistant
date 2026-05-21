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

  const stopCard = page.getByTestId("trip-stop-card").first();
  const mapsLink = stopCard.locator('a[title="Cómo llegar"]');
  await expect(mapsLink).toHaveAttribute("href", /google\.com\/maps.*destination=/);
  await expect(mapsLink).toHaveAttribute("target", "_blank");
  await expect(mapsLink).toHaveAttribute("rel", "noopener");
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

test("no-stop journeys keep the success state but collapse alternatives", async ({ page }) => {
  await setFixture(page, "trip_no_stops");
  const tripPage = new TripPage(page);
  await tripPage.goto();

  await tripPage.plan("Madrid", "Sevilla");

  await expect(page.getByTestId("trip-stops")).toContainText("No hacen falta paradas");
  await expect(page.getByTestId("trip-alt-plans-wrap")).toBeHidden();
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

test("share section appears after successful plan and hides after an error", async ({ page }) => {
  const tripPage = new TripPage(page);
  await tripPage.goto();

  const firstDone = page.waitForResponse((r) => r.url().includes("/api/v1/trip/plan"));
  await tripPage.plan("Madrid", "Sevilla");
  await firstDone;

  await expect(page.getByTestId("trip-share")).toBeVisible();
  await expect(page.getByTestId("trip-share-copy")).toBeVisible();
  await expect(page.getByTestId("trip-share-whatsapp")).toBeVisible();
  await expect(page.getByTestId("trip-share-telegram")).toBeVisible();

  await setFixture(page, "trip_error");
  await tripPage.plan("Madrid", "Sevilla");

  await expect(page.getByTestId("trip-share")).toBeHidden();
});

test("nav section appears after successful plan with correct navigation links", async ({ page }) => {
  const tripPage = new TripPage(page);
  await tripPage.goto();

  await tripPage.plan("Madrid", "Sevilla");

  await expect(page.getByTestId("trip-nav")).toBeVisible();
  await expect(page.getByTestId("trip-nav-google")).toHaveAttribute("href", /google\.com\/maps/);
  await expect(page.getByTestId("trip-nav-waze")).toBeVisible();
  await expect(page.getByTestId("trip-nav-apple")).toContainText("solo ruta directa");
});

test("nav section hides after a plan error", async ({ page }) => {
  const tripPage = new TripPage(page);
  await tripPage.goto();

  const firstDone = page.waitForResponse((r) => r.url().includes("/api/v1/trip/plan"));
  await tripPage.plan("Madrid", "Sevilla");
  await firstDone;
  await expect(page.getByTestId("trip-nav")).toBeVisible();

  await setFixture(page, "trip_error");
  await tripPage.plan("Madrid", "Sevilla");
  await expect(page.getByTestId("trip-nav")).toBeHidden();
});

test("share URL pre-populates form and auto-runs plan on load", async ({ page }) => {
  const planRequest = page.waitForRequest((r) => r.url().includes("/api/v1/trip/plan"));
  await page.goto(
    "/trip?origin=Madrid&destination=Sevilla&fuel_type=gasoline_95_e5_price" +
    "&consumption_lper100km=7&tank_liters=40&fuel_level_pct=25&max_detour_minutes=5"
  );

  await expect(page.getByTestId("trip-origin-input")).toHaveValue("Madrid");
  await expect(page.getByTestId("trip-destination-input")).toHaveValue("Sevilla");

  const req = await planRequest;
  expect(req.postDataJSON()).toMatchObject({ origin: "Madrid", destination: "Sevilla", fuel_type: "gasoline_95_e5_price" });

  await expect(page.getByTestId("trip-share")).toBeVisible();
});
