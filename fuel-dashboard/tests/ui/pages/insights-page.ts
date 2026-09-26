import { Locator, Page } from "@playwright/test";

export class InsightsPage {
  readonly page: Page;
  readonly zipInput: Locator;
  readonly trendsPeriodSelect: Locator;
  readonly trendsProvinceSelect: Locator;
  readonly forecastBanner: Locator;
  readonly trendsVariantASelect: Locator;
  readonly trendsVariantBSelect: Locator;
  readonly trendsVariantDiffToggle: Locator;
  readonly trendsVariantPicker: Locator;
  readonly fuelTypeDiffToggle: Locator;
  readonly trendsBrentToggle: Locator;
  readonly commodityCorrelationKpi: Locator;

  constructor(page: Page) {
    this.page = page;
    this.zipInput = page.getByTestId("trends-zip-input");
    this.trendsPeriodSelect = page.locator('#trends-filter select[name="period"]');
    this.trendsProvinceSelect = page.getByTestId("trends-province-select");
    this.forecastBanner = page.getByTestId("forecast-banner");
    this.trendsVariantASelect = page.getByTestId("trends-variant-a");
    this.trendsVariantBSelect = page.getByTestId("trends-variant-b");
    this.trendsVariantDiffToggle = page.locator("#trends-variant-diff-toggle");
    this.trendsVariantPicker = page.locator("#trends-variant-picker");
    this.fuelTypeDiffToggle = page.locator("#fuel-type-diff-toggle");
    this.trendsBrentToggle = page.locator("#trends-brent-toggle");
    this.commodityCorrelationKpi = page.getByTestId("commodity-correlation-kpi");
  }

  async goto() {
    return this.page.goto("/insights");
  }
}
