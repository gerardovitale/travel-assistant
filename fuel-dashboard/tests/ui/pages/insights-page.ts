import { Locator, Page } from "@playwright/test";

export class InsightsPage {
  readonly page: Page;
  readonly zipInput: Locator;

  constructor(page: Page) {
    this.page = page;
    this.zipInput = page.getByTestId("trends-zip-input");
  }

  async goto() {
    return this.page.goto("/insights");
  }
}
