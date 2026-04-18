import { Locator, Page } from "@playwright/test";

export class InsightsPage {
  readonly page: Page;
  readonly zipInput: Locator;
  readonly trendsPeriodSelect: Locator;
  readonly trendsProvinceSelect: Locator;
  readonly forecastBanner: Locator;

  constructor(page: Page) {
    this.page = page;
    this.zipInput = page.getByTestId("trends-zip-input");
    this.trendsPeriodSelect = page.locator('#trends-filter select[name="period"]');
    this.trendsProvinceSelect = page.getByTestId("trends-province-select");
    this.forecastBanner = page.getByTestId("forecast-banner");
  }

  async goto() {
    return this.page.goto("/insights");
  }
}
