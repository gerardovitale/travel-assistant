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
    fuel_type: "diesel_a_price",
    origin: "Madrid",
  });

  await expect(page.getByTestId("trip-kpis")).toBeVisible();
  await expect(page.getByTestId("trip-stops").getByTestId("trip-stop-card")).toHaveCount(2);
  await expect(page.getByTestId("trip-alt-plans").getByTestId("trip-alt-plan-card")).toHaveCount(2);
  await expect(page.getByTestId("trip-map")).toBeVisible();
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

  await tripPage.plan("Madrid", "Sevilla");
  await expect(page.getByTestId("trip-stops").getByTestId("trip-stop-card")).toHaveCount(2);

  await setFixture(page, "trip_error");
  await tripPage.plan("Madrid", "Sevilla");

  await expect(page.getByTestId("trip-banner")).toContainText("Route unavailable for selected itinerary");
  await expect(page.getByTestId("trip-stops")).toContainText("Route unavailable for selected itinerary");
  await expect(page.getByTestId("trip-alt-plans-wrap")).toBeHidden();
});
