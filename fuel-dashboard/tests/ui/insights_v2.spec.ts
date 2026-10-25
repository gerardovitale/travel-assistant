import { expect, test } from "@playwright/test";

import { blockExternalNetwork, setFixture } from "./support";

test.beforeEach(async ({ page }) => {
  await blockExternalNetwork(page);
  await setFixture(page, "insights_v2");
});

test("story page shows today's price before the trend loads, then a data-driven headline", async ({ page }) => {
  await page.goto("/insights");

  await expect(page.getByTestId("insights-v2-page")).toBeVisible();
  await expect(page.getByTestId("insights-construction-note")).toContainText("Sección en construcción.");
  await expect(page.getByTestId("kpi-price-gasoline_95_e5_price")).toContainText("1,529");
  await expect(page.getByTestId("trend-pair-chart")).toHaveAttribute("data-plot-ready", "true");
  // Fixture gasoline falls 0,032 €/l over the 90 days to 1,529.
  await expect(page.getByTestId("trend-headline")).toHaveText("La gasolina 95 ha bajado 0,032 €/l en los últimos 3 meses");
  await expect(page.getByTestId("trend-spread")).toHaveText("0,077 €/l");
});

test("fuel toggle switches headline and KPI without refetching; period refetches and is kept in the URL", async ({ page }) => {
  const trendRequests: string[] = [];
  page.on("request", (r) => {
    if (r.url().includes("/api/v1/trends/price")) trendRequests.push(r.url());
  });
  await page.goto("/insights");
  await expect(page.getByTestId("trend-pair-chart")).toHaveAttribute("data-plot-ready", "true");
  expect(trendRequests).toHaveLength(2);

  const fuel = page.getByTestId("insights-fuel-toggle");
  await fuel.getByRole("button", { name: "Diésel" }).click();
  await expect(fuel.getByRole("button", { name: "Diésel" })).toHaveAttribute("aria-pressed", "true");
  await expect(page.getByTestId("trend-headline")).toHaveText("El diésel ha bajado 0,032 €/l en los últimos 3 meses");
  await expect(page.getByTestId("kpi-price-diesel_a_price")).toBeVisible();
  await expect(page.getByTestId("kpi-price-gasoline_95_e5_price")).toBeHidden();
  expect(trendRequests).toHaveLength(2);

  await page.getByTestId("trend-period-toggle").getByRole("button", { name: "1A" }).click();
  await expect(page.getByTestId("trend-headline")).toContainText("en el último año");
  expect(trendRequests.slice(2).every((u) => u.includes("period=year"))).toBe(true);
  await expect(page).toHaveURL(/fuel=diesel_a_price/);
  await expect(page).toHaveURL(/period=year/);
});

// The fixture's latest data day is 2026-04-17; each period ends there and spans its full length.
const PERIOD_WINDOWS = [
  { label: "1M", start: "2026-03-19", days: 30 },
  { label: "3M", start: "2026-01-18", days: 90 },
  { label: "6M", start: "2025-10-20", days: 180 },
  { label: "1A", start: "2025-04-18", days: 365 },
];

test("each period sets the x axis and the data to exactly that window", async ({ page }) => {
  await page.goto("/insights");
  const chart = page.getByTestId("trend-pair-chart");
  for (const { label, start, days } of PERIOD_WINDOWS) {
    await page.getByTestId("trend-period-toggle").getByRole("button", { name: label }).click();
    await expect(chart).toHaveAttribute("data-x-range", `${start}..2026-04-17`);
    for (const trace of [0, 1]) {
      await expect(chart).toHaveAttribute(`data-trace${trace}-x`, `${start}..2026-04-17`);
      await expect(chart).toHaveAttribute(`data-trace${trace}-points`, String(days));
    }
    await expect(page.getByTestId("trend-coverage")).toBeHidden();
  }
});

test("a period longer than the stored history keeps its axis and says where the data starts", async ({ page }) => {
  await setFixture(page, "insights_v2_short_history");
  await page.goto("/insights?period=year");
  const chart = page.getByTestId("trend-pair-chart");
  await expect(chart).toHaveAttribute("data-x-range", "2025-04-18..2026-04-17");
  await expect(chart).toHaveAttribute("data-trace0-x", "2026-02-17..2026-04-17");
  await expect(page.getByTestId("trend-coverage")).toContainText("Solo hay datos desde el 17 feb 2026; el periodo empieza el 18 abr 2025.");
  // The headline measures only the span it has data for.
  await expect(page.getByTestId("trend-headline")).toHaveText("La gasolina 95 ha bajado 0,025 €/l desde el 17 feb 2026");

  await page.getByTestId("trend-period-toggle").getByRole("button", { name: "1M" }).click();
  await expect(chart).toHaveAttribute("data-x-range", "2026-03-19..2026-04-17");
  await expect(page.getByTestId("trend-coverage")).toBeHidden();
  await expect(page.getByTestId("trend-headline")).toContainText("en el último mes");
});

test("defaults to the peninsula and offers islands, Ceuta and Melilla as their own scope", async ({ page }) => {
  const requests: string[] = [];
  page.on("request", (r) => {
    if (r.url().includes("/api/v1/trends/price")) requests.push(r.url());
  });
  await page.goto("/insights");
  await expect(page.getByTestId("trend-pair-chart")).toHaveAttribute("data-plot-ready", "true");
  expect(requests.every((u) => u.includes("region=peninsula"))).toBe(true);
  await expect(page.getByTestId("trend-scope")).toHaveText("Península");

  await page.getByTestId("insights-scope-select").selectOption("special");
  await expect(page.getByTestId("trend-scope")).toHaveText("Islas, Ceuta y Melilla");
  await expect(page.getByTestId("kpi-price-gasoline_95_e5_price")).toContainText("1,354");
  await expect(page).toHaveURL(/scope=special/);
});

test("compares the peninsula with islands, Ceuta and Melilla for the selected fuel", async ({ page }) => {
  await page.goto("/insights?scope=compare");
  await expect(page.getByTestId("trend-pair-chart")).toHaveAttribute("data-plot-ready", "true");
  await expect(page.getByTestId("trend-headline")).toHaveText(
    "En las islas, Ceuta y Melilla la gasolina 95 cuesta 0,175 €/l menos que en la península",
  );
  await expect(page.getByTestId("trend-spread")).toHaveText("0,175 €/l");
  // The KPI keeps showing the peninsula price while comparing.
  await expect(page.getByTestId("kpi-price-gasoline_95_e5_price")).toContainText("1,529");
});

test("a province scopes the trend request", async ({ page }) => {
  await page.goto("/insights");
  const request = page.waitForRequest((r) => r.url().includes("/api/v1/trends/price") && r.url().includes("province=madrid"));
  await page.getByTestId("insights-scope-select").selectOption("madrid");
  await request;
  await expect(page.getByTestId("trend-scope")).toHaveText("Madrid");
});

test("story page fits a phone screen", async ({ page }) => {
  await page.setViewportSize({ width: 360, height: 780 });
  await page.goto("/insights");
  await expect(page.getByTestId("trend-pair-chart")).toHaveAttribute("data-plot-ready", "true");
  const overflow = await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
  expect(overflow).toBeLessThanOrEqual(0);
});

test("the under-construction note links to the previous insights page", async ({ page }) => {
  await page.goto("/insights");
  await page.getByTestId("insights-previous-link").click();
  await expect(page).toHaveURL(/\/insights\/trends$/);
  await expect(page.getByTestId("insight-tab-trends")).toBeVisible();
});

test("Cuándo repostar names the cheapest weekday, the saving per tank and the 7-day forecast", async ({ page }) => {
  await page.goto("/insights#cuando");
  await expect(page.getByTestId("weekday-chart")).toHaveAttribute("data-trace0-points", "7");
  await expect(page.getByTestId("when-headline")).toHaveText("El lunes es el día más barato; el viernes, el más caro");
  await expect(page.getByTestId("when-saving")).toContainText(
    "Con un depósito de 50 l, repostar el lunes en vez del viernes te ahorra 0,39 €",
  );
  await expect(page.getByTestId("kpi-best-day")).toContainText("Lunes");
  await expect(page.getByTestId("kpi-best-day")).toContainText("0,008 €/l menos que el viernes");
  await expect(page.getByTestId("forecast-recommendation")).toHaveText("Reposta hoy");
  await expect(page.getByTestId("forecast-explanation")).toContainText("en la España peninsular están bajos");
  await expect(page.getByTestId("forecast-tomorrow")).toContainText("Lo más probable mañana: que siga barato (63 %)");
  await expect(page.getByTestId("kpi-forecast")).toContainText("Precios bajos ahora");

  // Both fuels come in one request: switching fuel updates the section without a new one.
  const requests: string[] = [];
  page.on("request", (r) => {
    if (r.url().includes("/api/v1/trends/refuel-timing")) requests.push(r.url());
  });
  await page.getByTestId("insights-fuel-toggle").getByRole("button", { name: "Diésel" }).click();
  await expect(page.getByTestId("when-saving")).toContainText("te ahorra 0,31 €");
  await expect(page.getByTestId("kpi-best-day")).toContainText("0,006 €/l menos que el viernes");
  expect(requests).toHaveLength(0);
});

test("Cuándo repostar follows the zone selector", async ({ page }) => {
  await page.goto("/insights");
  await expect(page.getByTestId("when-headline")).toContainText("lunes");
  const request = page.waitForRequest((r) => r.url().includes("/api/v1/trends/refuel-timing") && r.url().includes("region=special"));
  await page.getByTestId("insights-scope-select").selectOption("special");
  const url = new URL((await request).url());
  expect(url.searchParams.getAll("fuel_type")).toEqual(["gasoline_95_e5_price", "diesel_a_price"]);
  await expect(page.getByTestId("when-scope")).toHaveText("Islas, Ceuta y Melilla");
  await expect(page.getByTestId("forecast-explanation")).toContainText("en las islas, Ceuta y Melilla");
});

test("Cuándo repostar never shows the previous zone's answer while the new one loads", async ({ page }) => {
  await page.goto("/insights");
  await expect(page.getByTestId("forecast-recommendation")).toHaveText("Reposta hoy");
  let release: () => void = () => {};
  const held = new Promise<void>((resolve) => (release = resolve));
  await page.route("**/api/v1/trends/refuel-timing?**", async (route) => {
    await held;
    await route.continue();
  });
  await page.getByTestId("insights-scope-select").selectOption("special");
  await expect(page.getByTestId("forecast-recommendation")).toHaveText("—");
  await expect(page.getByTestId("kpi-best-day")).toContainText("Cargando…");
  await expect(page.getByTestId("when-saving")).toBeHidden();
  release();
  await expect(page.getByTestId("forecast-explanation")).toContainText("en las islas, Ceuta y Melilla");
});
