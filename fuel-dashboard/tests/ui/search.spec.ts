import { expect, test } from "@playwright/test";

import { SearchPage } from "./pages/search-page";
import { blockExternalNetwork, setFixture } from "./support";

test.beforeEach(async ({ page }) => {
  await blockExternalNetwork(page);
  await setFixture(page, "happy_path");
});

test("best-option search renders KPIs, recommendation, and results", async ({ page }) => {
  const searchPage = new SearchPage(page);
  await searchPage.goto();

  const requestPromise = page.waitForRequest((request) => request.url().includes("/api/v1/stations/best-by-address"));
  await searchPage.search("Madrid");

  const request = await requestPromise;
  const url = new URL(request.url());
  expect(url.searchParams.get("address")).toBe("Madrid");
  expect(url.searchParams.get("fuel_type")).toBe("gasoline_95_e5_price");

  await expect(page.getByTestId("search-kpis")).toBeVisible();
  await expect(page.getByTestId("search-recommendation")).toContainText("Plenoil Atocha");
  await expect(searchPage.results).toHaveCount(4);
  await expect(page.getByTestId("search-map")).toBeVisible();
});

test("mode switching updates advanced fields and nearest search hits the nearest endpoint", async ({ page }) => {
  const searchPage = new SearchPage(page);
  await searchPage.goto();

  await searchPage.selectMode("nearest_by_address");
  await expect(page.locator('[data-field="radius"]')).toBeHidden();
  await expect(page.locator('[data-field="consumption"]')).toBeHidden();
  await expect(page.locator('[data-field="tank"]')).toBeHidden();

  const requestPromise = page.waitForRequest((request) => request.url().includes("/api/v1/stations/nearest-by-address"));
  await searchPage.search("Madrid");
  await requestPromise;

  await expect(searchPage.results.first()).toContainText("Repsol Chamberi");
});

test("zip shortcut uses the cheapest-by-zip flow and keeps the empty-state UX", async ({ page }) => {
  await setFixture(page, "search_empty");
  const searchPage = new SearchPage(page);
  await searchPage.goto();

  await searchPage.selectMode("cheapest_by_address");
  const requestPromise = page.waitForRequest((request) => request.url().includes("/api/v1/stations/cheapest-by-zip"));
  await searchPage.search("28001");

  const request = await requestPromise;
  expect(new URL(request.url()).searchParams.get("zip_code")).toBe("28001");

  await expect(searchPage.results).toHaveCount(0);
  await expect(page.getByTestId("search-results-list")).toContainText("Sin resultados");
  await expect(page.getByTestId("search-status-banner")).toBeHidden();
});

test("brand filters are sent to the backend and narrow the rendered stations", async ({ page }) => {
  const searchPage = new SearchPage(page);
  await searchPage.goto();

  await searchPage.selectBrand("repsol");

  const requestPromise = page.waitForRequest((request) => request.url().includes("/api/v1/stations/best-by-address"));
  await searchPage.search("Madrid");
  const request = await requestPromise;

  expect(new URL(request.url()).searchParams.getAll("labels")).toEqual(["repsol"]);
  await expect(searchPage.results).toHaveCount(1);
  await expect(searchPage.results.first()).toContainText("Repsol Chamberi");
});

test("errors clear stale results and show the search banner", async ({ page }) => {
  const searchPage = new SearchPage(page);
  await searchPage.goto();

  await searchPage.search("Madrid");
  await expect(searchPage.results).toHaveCount(4);

  await setFixture(page, "search_error");
  const requestPromise = page.waitForRequest((request) => request.url().includes("/api/v1/stations/best-by-address"));
  await searchPage.search("Madrid");
  await requestPromise;

  await expect(page.getByTestId("search-status-banner")).toContainText("No stations found within radius");
  await expect(searchPage.results).toHaveCount(0);
  await expect(page.getByTestId("search-recommendation")).toBeHidden();
});
