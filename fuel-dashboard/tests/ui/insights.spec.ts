import { expect, test } from "@playwright/test";

import { InsightsPage } from "./pages/insights-page";
import { blockExternalNetwork, setFixture } from "./support";

test.beforeEach(async ({ page }) => {
  await blockExternalNetwork(page);
  await setFixture(page, "happy_path");
});

test("trends tab loads charts and debounced zip updates only send the final request", async ({ page }) => {
  const insightsPage = new InsightsPage(page);
  const zipRequests: string[] = [];

  page.on("request", (request) => {
    if (request.url().includes("/api/v1/trends/price") && request.url().includes("zip_code=")) {
      zipRequests.push(request.url());
    }
  });

  await insightsPage.goto();

  await expect(page.getByTestId("trend-chart")).toHaveAttribute("data-plot-ready", "true");
  await expect(page.getByTestId("group-trend-chart")).toHaveAttribute("data-plot-ready", "true");

  await insightsPage.zipInput.fill("2");
  await insightsPage.zipInput.fill("28");
  await insightsPage.zipInput.fill("280");
  await insightsPage.zipInput.fill("2800");
  await insightsPage.zipInput.fill("28001");

  await page.waitForTimeout(900);

  expect(zipRequests).toHaveLength(1);
  expect(zipRequests[0]).toContain("zip_code=28001");
  await expect(page.getByTestId("trend-kpis")).toContainText("Actual");
});

test("quality tab renders KPI and summary content", async ({ page }) => {
  const insightsPage = new InsightsPage(page);
  await insightsPage.goto();

  await page.getByTestId("insight-tab-quality").click();

  await expect(page.getByTestId("quality-kpis")).toContainText("Días con datos");
  await expect(page.getByTestId("quality-summary")).toContainText("Rango: 2026-01-01");
});

test("trends tab renders markov forecast once a zip code is provided", async ({ page }) => {
  await setFixture(page, "insights_all");
  const insightsPage = new InsightsPage(page);
  await insightsPage.goto();

  const responsePromise = page.waitForResponse((r) => r.url().includes("/api/v1/historical/forecast"));
  await insightsPage.zipInput.fill("28001");
  await responsePromise;

  await expect(insightsPage.forecastBanner).toContainText("Reposta hoy");
  await expect(page.getByTestId("forecast-kpis")).toContainText("Régimen");
  await expect(page.getByTestId("forecast-probabilities")).toContainText("Barato");
});

test("trends tab shows an insufficient-data state when the forecast is unavailable", async ({ page }) => {
  await setFixture(page, "historical_sparse");
  const insightsPage = new InsightsPage(page);
  await insightsPage.goto();

  const responsePromise = page.waitForResponse((r) => r.url().includes("/api/v1/historical/forecast"));
  await insightsPage.zipInput.fill("28001");
  await responsePromise;

  await expect(insightsPage.forecastBanner).toContainText("No hay suficiente historico");
  await expect(page.getByTestId("forecast-probabilities")).toContainText("Sin datos");
});

test("forecast ignores partial zip codes and falls back to province", async ({ page }) => {
  await setFixture(page, "insights_all");
  const insightsPage = new InsightsPage(page);
  const forecastRequests: string[] = [];

  page.on("request", (request) => {
    if (request.url().includes("/api/v1/historical/forecast")) {
      forecastRequests.push(request.url());
    }
  });

  await insightsPage.goto();
  await insightsPage.trendsProvinceSelect.selectOption("madrid");
  const responsePromise = page.waitForResponse((r) => r.url().includes("/api/v1/historical/forecast"));
  await insightsPage.zipInput.fill("28");
  await responsePromise;

  const lastRequest = forecastRequests.at(-1) || "";
  expect(lastRequest).toContain("province=madrid");
  expect(lastRequest).not.toContain("zip_code=28");
  await expect(insightsPage.forecastBanner).toContainText("Reposta hoy");
});

test("forecast sends the selected period as the forecast window", async ({ page }) => {
  await setFixture(page, "insights_all");
  const insightsPage = new InsightsPage(page);

  await insightsPage.goto();
  await insightsPage.trendsProvinceSelect.selectOption("madrid");
  // Wait for the province-triggered forecast to settle before measuring the period-triggered one
  await page.waitForResponse((r) => r.url().includes("/api/v1/historical/forecast"));

  const responsePromise = page.waitForResponse(
    (r) => r.url().includes("/api/v1/historical/forecast") && r.url().includes("window_days=365"),
  );
  await insightsPage.trendsPeriodSelect.selectOption("year");
  await responsePromise;
});

test("section share button opens a share menu and copies a deep link", async ({ page, context }) => {
  await context.grantPermissions(["clipboard-read", "clipboard-write"]);
  // Force the popover path (no native share sheet) deterministically across browsers.
  await page.addInitScript(() => {
    try {
      // @ts-expect-error remove native share so the popover renders
      delete window.navigator.share;
    } catch {
      /* ignore */
    }
  });

  const insightsPage = new InsightsPage(page);
  await insightsPage.goto();
  await expect(page.getByTestId("trend-chart")).toHaveAttribute("data-plot-ready", "true");

  const shareBtn = page.locator('[data-share="sec-trends-price"]');
  await expect(shareBtn).toBeVisible();
  await expect(shareBtn).toHaveAttribute("aria-expanded", "false");
  await shareBtn.click();
  await expect(shareBtn).toHaveAttribute("aria-expanded", "true");

  await expect(page.getByText("Copiar enlace")).toBeVisible();
  await expect(page.getByText("WhatsApp")).toBeVisible();
  await expect(page.getByText("Telegram")).toBeVisible();

  await page.getByText("Copiar enlace").click();
  await expect(page.getByTestId("toast")).toContainText("Enlace copiado");

  const clip = await page.evaluate(() => navigator.clipboard.readText());
  expect(clip).toContain("/insights");
  expect(clip).toContain("#sec-trends-price");
});

test("filter changes sync to the URL and restore on reload", async ({ page }) => {
  const insightsPage = new InsightsPage(page);
  await insightsPage.goto();
  await expect(page.getByTestId("trend-chart")).toHaveAttribute("data-plot-ready", "true");

  await insightsPage.trendsPeriodSelect.selectOption("year");
  await expect(page).toHaveURL(/[?&]period=year/);

  await page.reload();
  await expect(insightsPage.trendsPeriodSelect).toHaveValue("year");
});

test("switching tabs updates the path", async ({ page }) => {
  const insightsPage = new InsightsPage(page);
  await insightsPage.goto();

  await page.getByTestId("insight-tab-quality").click();
  await expect(page).toHaveURL(/\/insights\/quality$/);
});

test("reportes deep link restores the active tab and its filters", async ({ page }) => {
  await setFixture(page, "insights_all");
  await page.goto("/insights/reportes?fuel=diesel_a_price&dir=priciest");

  // Server activates the reportes tab; the client restores both filters from the query.
  await expect(page.getByTestId("insight-tabs")).toHaveAttribute("data-active-tab", "reportes");
  await expect(page.getByTestId("reportes-fuel-select")).toHaveValue("diesel_a_price");
  await expect(page.getByTestId("reportes-direction-select")).toHaveValue("priciest");
  await expect(page.locator('[data-share="sec-reportes-win-rate"]')).toBeVisible();
});

test("reportes shows the savings estimate and recomputes on input change", async ({ page }) => {
  await setFixture(page, "insights_all");
  await page.goto("/insights/reportes");

  const kpis = page.getByTestId("reportes-savings-kpis");
  await expect(kpis).toContainText("/año");

  // Cheapest brand (largest €/año) headlines first; pricier-than-market brand is clamped.
  await expect(kpis.locator("> div").first()).toContainText("plenoil");
  await expect(kpis).toContainText("Sin ahorro vs. mercado");

  const before = (await kpis.innerText()).trim();

  // Doubling litres/repostaje must change the estimate without a refetch.
  await page.getByTestId("reportes-tank-liters").fill("80");
  await page.getByTestId("reportes-tank-liters").dispatchEvent("input");
  await expect(kpis).not.toHaveText(before);
});

test("reportes savings inputs stay inside the card on a narrow iphone viewport", async ({ page }) => {
  await page.setViewportSize({ width: 375, height: 667 }); // iPhone SE / 8 width
  await setFixture(page, "insights_all");
  await page.goto("/insights/reportes");

  const card = page.locator("#sec-reportes-savings");
  await expect(card).toBeVisible();

  // Card itself must not overflow horizontally, and both inputs must sit within its right edge.
  const cardBox = await card.boundingBox();
  const overflow = await card.evaluate((el) => el.scrollWidth - el.clientWidth);
  expect(overflow).toBeLessThanOrEqual(1); // allow sub-pixel rounding

  for (const id of ["reportes-tank-liters", "reportes-fills-month"]) {
    const inputBox = await page.getByTestId(id).boundingBox();
    expect(inputBox!.x + inputBox!.width).toBeLessThanOrEqual(cardBox!.x + cardBox!.width + 1);
  }
});

test("reportes coverage table exposes a confidence column", async ({ page }) => {
  await setFixture(page, "insights_all");
  await page.goto("/insights/reportes");

  const table = page.getByTestId("reportes-coverage-table");
  await expect(table).toContainText("Confianza");
});

test("forecast clamps short trend periods to the minimum forecast window of 90 days", async ({ page }) => {
  await setFixture(page, "insights_all");
  const insightsPage = new InsightsPage(page);
  const forecastRequests: string[] = [];

  page.on("request", (request) => {
    if (request.url().includes("/api/v1/historical/forecast")) {
      forecastRequests.push(request.url());
    }
  });

  await insightsPage.goto();
  await insightsPage.trendsProvinceSelect.selectOption("madrid");
  const responsePromise = page.waitForResponse((r) => r.url().includes("/api/v1/historical/forecast"));
  await insightsPage.trendsPeriodSelect.selectOption("month");
  await responsePromise;

  const lastRequest = forecastRequests.at(-1) || "";
  expect(lastRequest).toContain("window_days=90");
});
