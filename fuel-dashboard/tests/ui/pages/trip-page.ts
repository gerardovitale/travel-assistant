import { expect, Locator, Page } from "@playwright/test";

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
    const response = await this.page.goto("/trip");
    await this.waitForReady();
    return response;
  }

  // trip.js init() attaches the form/swap listeners in the same synchronous block
  // that renders the brand checkboxes (after the fuel + labels fetches settle).
  // Interacting earlier races init and is silently ignored (e.g. swap does nothing).
  async waitForReady() {
    await expect(this.page.getByTestId("trip-fuel-select")).toBeEnabled();
    await expect(this.page.locator('[data-testid^="brand-checkbox-"]').first()).toBeAttached();
  }

  async plan(origin: string, destination: string) {
    await this.originInput.fill(origin);
    await this.destinationInput.fill(destination);
    await expect(this.submitButton).toBeEnabled();
    await this.submitButton.click();
  }
}
