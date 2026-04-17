import { Locator, Page } from "@playwright/test";

export class SearchPage {
  readonly page: Page;
  readonly locationInput: Locator;
  readonly submitButton: Locator;
  readonly results: Locator;

  constructor(page: Page) {
    this.page = page;
    this.locationInput = page.getByTestId("search-location-input");
    this.submitButton = page.getByTestId("search-submit");
    this.results = page.getByTestId("search-result-card");
  }

  async goto() {
    return this.page.goto("/");
  }

  async selectMode(mode: "best_by_address" | "cheapest_by_address" | "nearest_by_address") {
    await this.page.getByTestId(`search-mode-${mode}`).click();
  }

  async selectBrand(rawLabel: string) {
    await this.page.getByTestId("search-brands-toggle").click();
    await this.page.locator(`[data-testid="brand-checkbox-${rawLabel}"]`).check();
    await this.page.getByTestId("search-brands-toggle").click();
  }

  async search(location: string) {
    await this.locationInput.fill(location);
    await this.submitButton.click();
  }
}
