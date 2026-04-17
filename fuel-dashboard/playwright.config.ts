import { defineConfig } from "@playwright/test";

const PORT = 18080;
const BASE_URL = `http://127.0.0.1:${PORT}`;

export default defineConfig({
  testDir: "./tests/ui",
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  reporter: process.env.CI ? [["html", { open: "never" }], ["list"]] : "list",
  use: {
    baseURL: BASE_URL,
    screenshot: "only-on-failure",
    trace: "retain-on-failure",
    video: "retain-on-failure",
  },
  webServer: {
    command: `uv run uvicorn main:app --app-dir app --host 127.0.0.1 --port ${PORT}`,
    cwd: __dirname,
    env: {
      DASHBOARD_DISABLE_EXTERNAL_ASSETS: "true",
      DASHBOARD_UI_FIXTURE_SET: "happy_path",
      DASHBOARD_UI_TEST_MODE: "true",
    },
    reuseExistingServer: false,
    timeout: 60_000,
    url: BASE_URL,
  },
  projects: [
    {
      name: "chromium",
      use: {
        browserName: "chromium",
        viewport: { width: 1440, height: 1100 },
      },
    },
  ],
});
