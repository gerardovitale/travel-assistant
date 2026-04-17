import { expect, test } from "@playwright/test";

import { blockExternalNetwork, setFixture } from "./support";

test.beforeEach(async ({ page }) => {
  await blockExternalNetwork(page);
});

test("loading page is rendered while data is warming", async ({ page }) => {
  await setFixture(page, "loading");

  for (const path of ["/", "/trip", "/insights"]) {
    const response = await page.goto(path);
    expect(response?.status()).toBe(503);
    await expect(page.getByTestId("loading-page")).toBeVisible();
    await expect(page.getByTestId("loading-retry")).toBeVisible();
  }
});

test("stale health status is surfaced in the shell", async ({ page }) => {
  await setFixture(page, "quality_stale");

  const response = await page.goto("/");
  expect(response?.ok()).toBeTruthy();

  await expect(page.getByTestId("data-status")).toContainText("datos 2026-04-14");
});
