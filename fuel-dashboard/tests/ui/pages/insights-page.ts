import { Locator, Page } from "@playwright/test";

export class InsightsPage {
  readonly page: Page;
  readonly zipInput: Locator;
  readonly historicalTab: Locator;
  readonly historicalZipInput: Locator;
  readonly historicalPeriodSelect: Locator;
  readonly historicalProvinceSelect: Locator;
  readonly forecastBanner: Locator;

  constructor(page: Page) {
    this.page = page;
    this.zipInput = page.getByTestId("trends-zip-input");
    this.historicalTab = page.getByTestId("insight-tab-historical");
    this.historicalZipInput = page.getByTestId("historical-zip-input");
    this.historicalPeriodSelect = page.locator('#historical-form select[name="period"]');
    this.historicalProvinceSelect = page.locator('#historical-form select[name="province"]');
    this.forecastBanner = page.getByTestId("forecast-banner");
  }

  async goto() {
    return this.page.goto("/insights");
  }
}
