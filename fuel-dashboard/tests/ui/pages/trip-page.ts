import { Locator, Page } from "@playwright/test";

export class TripPage {
  readonly page: Page;
  readonly originInput: Locator;
  readonly destinationInput: Locator;
  readonly submitButton: Locator;

  constructor(page: Page) {
    this.page = page;
    this.originInput = page.getByTestId("trip-origin-input");
    this.destinationInput = page.getByTestId("trip-destination-input");
    this.submitButton = page.getByTestId("trip-submit");
  }

  async goto() {
    return this.page.goto("/trip");
  }

  async plan(origin: string, destination: string) {
    await this.originInput.fill(origin);
    await this.destinationInput.fill(destination);
    await this.submitButton.click();
  }
}
