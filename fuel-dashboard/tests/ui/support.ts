import { Page } from "@playwright/test";

const BASE_URL = process.env.PLAYWRIGHT_TEST_BASE_URL || "http://127.0.0.1:18080";

export async function blockExternalNetwork(page: Page): Promise<void> {
  await page.route("**/*", async (route) => {
    const url = route.request().url();
    if (url.startsWith(BASE_URL) || url.startsWith("data:") || url.startsWith("blob:")) {
      await route.continue();
      return;
    }
    await route.abort();
  });
}

export async function setFixture(page: Page, fixtureSet: string): Promise<void> {
  await page.context().clearCookies();
  await page.context().addCookies([
    {
      name: "ui_fixture_set",
      value: fixtureSet,
      url: BASE_URL,
    },
  ]);
}
