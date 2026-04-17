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
