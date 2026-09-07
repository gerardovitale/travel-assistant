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

test("comparativa de variantes populates the default variant pair and recomputes the diff without refetching", async ({ page }) => {
  await setFixture(page, "insights_all");
  const insightsPage = new InsightsPage(page);
  await insightsPage.goto();

  // The UI-test fixture always models the diesel group regardless of the selected one, so switch
  // to it explicitly rather than relying on the form's own default (gasoline_95).
  await page.getByTestId("trends-group-select").selectOption("diesel");
  const chart = page.getByTestId("group-trend-chart");
  await expect(chart).toHaveAttribute("data-plot-ready", "true");
  await expect(insightsPage.trendsVariantASelect).toHaveValue("diesel_a_price");
  await expect(insightsPage.trendsVariantBSelect).toHaveValue("diesel_premium_price");
  await expect(insightsPage.trendsVariantDiffToggle).toBeChecked();
  await expect(insightsPage.trendsVariantPicker).toBeVisible();
  await expect(chart).toHaveAttribute("data-yaxis2-visible", "true");

  const groupRequests: string[] = [];
  page.on("request", (request) => {
    if (request.url().includes("/api/v1/trends/group")) groupRequests.push(request.url());
  });

  await insightsPage.trendsVariantASelect.selectOption("diesel_b_price");

  // Switching off the diff line hides both the %-axis and the now-pointless variant pickers.
  await insightsPage.trendsVariantDiffToggle.uncheck();
  await expect(insightsPage.trendsVariantPicker).toBeHidden();
  await expect(chart).toHaveAttribute("data-yaxis2-visible", "false");

  // All of the above are pure client-side recomputes over the already-fetched group series.
  expect(groupRequests).toHaveLength(0);
});

test("comparativa de variantes ignores a stale group response that resolves after a newer one", async ({ page }) => {
  await setFixture(page, "insights_all");
  const insightsPage = new InsightsPage(page);

  // Delay only the page's initial request (fuel_group=gasoline_95, the form's default) so it
  // resolves after the fast follow-up request for "diesel" that the test triggers below.
  await page.route("**/api/v1/trends/group*", async (route) => {
    const url = new URL(route.request().url());
    if (url.searchParams.get("fuel_group") === "gasoline_95") await new Promise((r) => setTimeout(r, 600));
    await route.continue();
  });

  await insightsPage.goto();
  await page.getByTestId("trends-group-select").selectOption("diesel");
  await expect(page.getByTestId("group-trend-chart")).toHaveAttribute("data-plot-ready", "true");
  await expect(insightsPage.trendsVariantASelect).toHaveValue("diesel_a_price");

  // Give the delayed gasoline_95 response time to land. Without the request-sequence guard it
  // would overwrite the variant selects back to the gasoline_95 default pair after the fact.
  await page.waitForTimeout(800);
  await expect(insightsPage.trendsVariantASelect).toHaveValue("diesel_a_price");
  await expect(insightsPage.trendsVariantBSelect).toHaveValue("diesel_premium_price");
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

  // Cheapest default brand (largest €/año) headlines first; pricier-than-market brand is clamped.
  await expect(kpis.locator("> div").first()).toContainText("Costco");
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

test("reportes brand picker defaults to four brands, refreshes charts, and caps at four", async ({ page }) => {
  await setFixture(page, "insights_all");

  const coverageRequests: string[] = [];
  page.on("request", (request) => {
    if (request.url().includes("/api/v1/reportes/coverage")) coverageRequests.push(request.url());
  });

  await page.goto("/insights/reportes");

  // Picker opens with the four default brands pre-selected.
  await page.getByTestId("reportes-brand-picker").locator("summary").click();
  const summary = page.getByTestId("reportes-brand-summary");
  await expect(summary).toHaveText("Marcas (4/4)");
  const options = page.getByTestId("reportes-brand-options");
  await expect(options.locator("input[data-brand-option]:checked")).toHaveCount(4);

  // Cepsa is shown under its Moeve rebrand.
  await expect(options).toContainText("Moeve (Cepsa)");

  // With four selected, an unchecked brand is disabled (the max-4 cap).
  await expect(options.locator("input[data-brand-option]:not(:checked)").first()).toBeDisabled();

  // Deselecting a brand re-fetches the charts and frees up the cap.
  const before = coverageRequests.length;
  await options.locator("input[data-brand-option]:checked").first().uncheck();
  await expect(summary).toHaveText("Marcas (3/4)");
  await expect.poll(() => coverageRequests.length).toBeGreaterThan(before);
  await expect(options.locator("input[data-brand-option]:not(:checked)").first()).toBeEnabled();

  // Selection is reflected in the shareable URL.
  await expect(page).toHaveURL(/brands=/);
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

test("fuel-type report tab renders the verdict, both charts and the methodology disclosure", async ({ page }) => {
  await setFixture(page, "insights_all");
  await page.goto("/insights/reportes?report=combustible");

  await expect(page.getByTestId("insight-tabs")).toHaveAttribute("data-active-tab", "reportes");
  await expect(page.getByTestId("fuel-type-cost-chart")).toHaveAttribute("data-plot-ready", "true");
  await expect(page.getByTestId("fuel-type-history-chart")).toHaveAttribute("data-plot-ready", "true");
  await expect(page.getByTestId("fuel-type-provinces-table")).toContainText("Provincia");

  await expect(page.getByTestId("fuel-type-verdict-summary")).toContainText("100 km");
  await expect(page.getByTestId("fuel-type-verdict-kpis")).toContainText("Consumo de equilibrio");
  await expect(page.getByTestId("fuel-type-provinces-note")).toContainText("provincias");

  // The figures behind the verdict must be on screen, not implied.
  const inputs = page.getByTestId("fuel-type-inputs");
  await expect(inputs).toContainText("l/100 km");
  await expect(inputs).toContainText("/100 km");
  await expect(inputs).toContainText("Consumo WLTP del fabricante");

  // The WLTP caveat and the buy-vs-run limit must be visible, not hidden behind a tooltip.
  const disclosure = page.getByTestId("fuel-type-disclosure");
  await expect(disclosure).toContainText("WLTP");
  await expect(disclosure).toContainText("coste de uso");
  await expect(page.getByTestId("fuel-type-catalog-version")).not.toBeEmpty();
});

test("fuel-type history chart shows the diff line and its axis by default, and hides both when toggled off", async ({ page }) => {
  const insightsPage = new InsightsPage(page);
  await setFixture(page, "insights_all");
  await page.goto("/insights/reportes?report=combustible");
  const chart = page.getByTestId("fuel-type-history-chart");
  await expect(chart).toHaveAttribute("data-plot-ready", "true");
  await expect(insightsPage.fuelTypeDiffToggle).toBeChecked();
  // Three traces: gasoline, diesel, and the diff line — visible by default (index 2).
  await expect(chart).toHaveAttribute("data-trace2-visible", "true");
  await expect(chart).toHaveAttribute("data-yaxis2-visible", "true");

  const historyRequests: string[] = [];
  page.on("request", (request) => {
    if (request.url().includes("/api/v1/reportes/fuel-type/history")) historyRequests.push(request.url());
  });

  await insightsPage.fuelTypeDiffToggle.uncheck();
  await expect(chart).toHaveAttribute("data-trace2-visible", "legendonly");
  await expect(chart).toHaveAttribute("data-yaxis2-visible", "false");
  // A pure re-render from the cached response — no refetch.
  expect(historyRequests).toHaveLength(0);
});

test("fuel-type report annual km recomputes the yearly cost without refetching", async ({ page }) => {
  await setFixture(page, "insights_all");
  await page.goto("/insights/reportes?report=combustible");
  await expect(page.getByTestId("fuel-type-cost-chart")).toHaveAttribute("data-plot-ready", "true");

  const before = await page.getByTestId("fuel-type-annual-kpis").innerText();

  const costRequests: string[] = [];
  page.on("request", (request) => {
    if (request.url().includes("/api/v1/reportes/fuel-type/")) costRequests.push(request.url());
  });

  await page.getByTestId("fuel-type-annual-km").fill("30000");
  await expect(page.getByTestId("fuel-type-annual-kpis")).not.toHaveText(before);

  // Annual km is pure client-side rescaling of cached rows.
  expect(costRequests).toHaveLength(0);
});

test("fuel-type report deep link restores the selected pair and province", async ({ page }) => {
  await setFixture(page, "insights_all");
  await page.goto("/insights/reportes?report=combustible&pair=nissan-qashqai&prov=madrid");

  await expect(page.getByTestId("fuel-type-pair-select")).toHaveValue("nissan-qashqai");
  await expect(page.getByTestId("fuel-type-province-select")).toHaveValue("madrid");
  await expect(page.getByTestId("fuel-type-cost-chart")).toHaveAttribute("data-plot-ready", "true");
});

test("fuel-type report cards fit a 375px viewport and the wide table scrolls inside its own box", async ({ page }) => {
  await page.setViewportSize({ width: 375, height: 800 });
  await setFixture(page, "insights_all");
  await page.goto("/insights/reportes?report=combustible");
  await expect(page.getByTestId("fuel-type-cost-chart")).toHaveAttribute("data-plot-ready", "true");

  // Cards with no intentionally-wide content must not overflow at all.
  for (const id of ["sec-reportes-fuel-verdict", "sec-reportes-fuel-cost"]) {
    const overflow = await page.locator(`#${id}`).evaluate((el) => el.scrollWidth - el.clientWidth);
    expect(overflow, id).toBeLessThanOrEqual(1);
  }

  // The province table is wider than a phone by design, so it must sit in a scroll container
  // rather than push the page. Asserted structurally: Tailwind is CDN-loaded and blocked here, so
  // overflow-x-auto has no effect on measured layout in this environment.
  const wrapperClass = await page
    .getByTestId("fuel-type-provinces-table")
    .evaluate((el) => (el.parentElement as HTMLElement).className);
  expect(wrapperClass).toContain("overflow-x-auto");
});

test("fuel-type report clears the previous verdict when a pair has no data", async ({ page }) => {
  await setFixture(page, "insights_all");
  await page.goto("/insights/reportes?report=combustible");
  await expect(page.getByTestId("fuel-type-verdict-summary")).not.toBeEmpty();

  // Next breakeven call 404s, as it does for a pair or province with no overlapping day.
  await page.route("**/api/v1/reportes/fuel-type/breakeven*", (route) =>
    route.fulfill({ status: 404, contentType: "application/json", body: '{"detail":"Aggregate report not available"}' }),
  );
  await page.getByTestId("fuel-type-pair-select").selectOption("nissan-qashqai");

  // The old model's verdict must not survive next to the empty state.
  await expect(page.getByTestId("fuel-type-verdict-summary")).toBeEmpty();
  await expect(page.getByTestId("fuel-type-verdict-kpis")).toContainText("Sin datos");
});

test("reportes picker switches between reports and syncs the choice to the URL", async ({ page }) => {
  await setFixture(page, "insights_all");
  await page.goto("/insights/reportes");

  // Assert on the report wrappers, not the chart divs: Tailwind is CDN-loaded and blocked in UI
  // test mode, so `h-48` never applies and a chart div has no height of its own to be "visible".
  const marcas = page.locator('[data-report="marcas"]');
  const combustible = page.locator('[data-report="combustible"]');

  // Brand report is the default; the fuel-type one is listed but not shown.
  await expect(page.getByTestId("reportes-win-rate-chart")).toHaveAttribute("data-plot-ready", "true");
  await expect(marcas).toBeVisible();
  await expect(combustible).toBeHidden();

  await page.getByTestId("report-option-combustible").click();
  await expect(page).toHaveURL(/[?&]report=combustible/);
  await expect(page.getByTestId("fuel-type-cost-chart")).toHaveAttribute("data-plot-ready", "true");
  await expect(combustible).toBeVisible();
  await expect(marcas).toBeHidden();

  // And back, without a reload. marcas is the default, so it drops out of the URL again.
  await page.getByTestId("report-option-marcas").click();
  await expect(marcas).toBeVisible();
  await expect(combustible).toBeHidden();
  await expect(page).not.toHaveURL(/[?&]report=combustible/);
});

test("reportes does not fetch the fuel-type report until it is opened", async ({ page }) => {
  await setFixture(page, "insights_all");
  const fuelRequests: string[] = [];
  page.on("request", (r) => {
    if (r.url().includes("/api/v1/reportes/fuel-type/")) fuelRequests.push(r.url());
  });

  await page.goto("/insights/reportes");
  await expect(page.getByTestId("reportes-win-rate-chart")).toHaveAttribute("data-plot-ready", "true");
  expect(fuelRequests).toHaveLength(0);

  await page.getByTestId("report-option-combustible").click();
  await expect(page.getByTestId("fuel-type-cost-chart")).toHaveAttribute("data-plot-ready", "true");
  expect(fuelRequests.length).toBeGreaterThan(0);
});

test("fuel-type province table sorts by column and flips direction on re-click", async ({ page }) => {
  await setFixture(page, "insights_all");
  await page.goto("/insights/reportes?report=combustible");

  const table = page.getByTestId("fuel-type-provinces-table");
  await expect(table).toContainText("Provincia");
  const firstCell = () => table.locator("tbody tr").first().locator("td").first();

  // Default sort is by the gap, widest first.
  const byGapDesc = await firstCell().innerText();

  await page.getByTestId("province-sort-province").click();
  const byNameAsc = await firstCell().innerText();
  expect(byNameAsc).not.toBe(byGapDesc);

  // Re-clicking the active column reverses it.
  await page.getByTestId("province-sort-province").click();
  const byNameDesc = await firstCell().innerText();
  expect(byNameDesc).not.toBe(byNameAsc);
  expect(byNameAsc.localeCompare(byNameDesc, "es")).toBeLessThan(0);
});

test("fuel-type report clears every stale note when a reload fails", async ({ page }) => {
  await setFixture(page, "insights_all");
  await page.goto("/insights/reportes?report=combustible");
  await expect(page.getByTestId("fuel-type-provinces-note")).toContainText("provincias");
  await expect(page.getByTestId("fuel-type-history-note")).not.toBeEmpty();
  await expect(page.getByTestId("fuel-type-inputs")).toContainText("l/100 km");

  // Every fuel-type call now fails, as it does for a pair with no overlapping price day.
  await page.route("**/api/v1/reportes/fuel-type/{breakeven,provinces,history}*", (route) =>
    route.fulfill({ status: 404, contentType: "application/json", body: '{"detail":"not available"}' }),
  );
  await page.getByTestId("fuel-type-pair-select").selectOption("nissan-qashqai");

  // No prose from the previous model may survive next to the empty states.
  await expect(page.getByTestId("fuel-type-verdict-summary")).toBeEmpty();
  await expect(page.getByTestId("fuel-type-inputs")).toBeEmpty();
  await expect(page.getByTestId("fuel-type-provinces-note")).toBeEmpty();
  await expect(page.getByTestId("fuel-type-history-note")).toBeEmpty();
});
