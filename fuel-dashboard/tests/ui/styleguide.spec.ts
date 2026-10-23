import { expect, test } from "@playwright/test";

import { blockExternalNetwork } from "./support";

test.beforeEach(async ({ page }) => {
  await blockExternalNetwork(page);
});

test("styleguide renders tokens and every component group", async ({ page }) => {
  const response = await page.goto("/styleguide");
  expect(response?.ok()).toBeTruthy();

  await expect(page.getByTestId("styleguide-page")).toBeVisible();
  for (const section of ["colors", "buttons", "forms", "feedback"]) {
    await expect(page.getByTestId(`styleguide-${section}`)).toBeVisible();
  }

  // Tokens resolve to real colors (would be transparent if a --fp-* variable were missing).
  const primary = await page
    .locator(".fp-btn--primary")
    .first()
    .evaluate((el) => getComputedStyle(el).backgroundImage);
  expect(primary).toContain("rgb(0, 22, 66)");

  // The active segment keeps its white pill; inactive ones stay transparent.
  const activeBg = await page.locator(".fp-seg__item.seg-active").evaluate((el) => getComputedStyle(el).backgroundColor);
  expect(activeBg).toBe("rgb(255, 255, 255)");

  await expect(page.getByRole("button", { name: "Usar mi ubicación" })).toBeVisible();
  await expect(page.getByLabel("Radio (km)")).toHaveAttribute("aria-invalid", "true");
  await expect(page.getByRole("alert")).toContainText("No encontramos esa dirección.");
});
