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

  // Station-card "Cómo llegar" uses the shared openInMaps builder and is
  // intercepted on click to hand off to the platform-best app.
  const directionsLink = searchPage.results.first().locator('a[title="Cómo llegar"]');
  await expect(directionsLink).toHaveAttribute("href", /google\.com\/maps\/dir\/\?api=1.*destination=/);
  await expect(directionsLink).toHaveAttribute("data-nav-smart", "");
  await expect(directionsLink).not.toHaveAttribute("target", /.+/);
});

test("best-option search shows the support nudge with a savings figure and Ko-fi link", async ({ page }) => {
  const searchPage = new SearchPage(page);
  await searchPage.goto();

  await searchPage.search("Madrid");
  await expect(searchPage.results).toHaveCount(4);

  const nudge = page.getByTestId("search-support-nudge");
  await expect(nudge).toBeVisible();
  await expect(nudge).toContainText("Te ahorras");
  const kofiLink = nudge.locator("a");
  await expect(kofiLink).toHaveAttribute("href", "https://ko-fi.com/fuelprecision");
  await expect(kofiLink).toHaveAttribute("target", "_blank");
});

test("empty results hide the support nudge", async ({ page }) => {
  await setFixture(page, "search_empty");
  const searchPage = new SearchPage(page);
  await searchPage.goto();

  await searchPage.selectMode("cheapest_by_address");
  await searchPage.search("28001");

  await expect(page.getByTestId("search-support-nudge")).toBeHidden();
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

  // Non-best modes have no tank_liters field in play, so the nudge falls back
  // to APP_CONFIG.default_refill_liters and must say so explicitly (no
  // misleading "this tank" claim when the figure is an assumption).
  const nudge = page.getByTestId("search-support-nudge");
  await expect(nudge).toBeVisible();
  await expect(nudge).toContainText("un depósito de 30 L");
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

  // A single result has no spread to save against — the nudge must stay hidden
  // rather than show a meaningless or zero saving.
  await expect(page.getByTestId("search-support-nudge")).toBeHidden();
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
