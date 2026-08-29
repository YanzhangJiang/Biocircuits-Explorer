import { defineConfig } from '@playwright/test';

const isCI = !!process.env.CI;

export default defineConfig({
  testDir: './e2e',
  outputDir: 'e2e/.artifacts/test-results',
  snapshotPathTemplate: '{testDir}/__screenshots__/{arg}{ext}',
  fullyParallel: false,
  forbidOnly: isCI,
  retries: isCI ? 1 : 0,
  workers: isCI ? 1 : undefined,
  timeout: 30_000,
  expect: {
    timeout: 5_000,
  },
  reporter: isCI
    ? [
        ['line'],
        ['html', { outputFolder: 'e2e/.artifacts/playwright-report', open: 'never' }],
      ]
    : [['list'], ['html', { outputFolder: 'e2e/.artifacts/playwright-report', open: 'never' }]],
  use: {
    baseURL: 'http://127.0.0.1:4173',
    browserName: 'chromium',
    colorScheme: 'light',
    deviceScaleFactor: 1,
    locale: 'en-US',
    reducedMotion: 'reduce',
    screenshot: 'only-on-failure',
    timezoneId: 'UTC',
    trace: 'retain-on-failure',
    video: 'retain-on-failure',
    viewport: { width: 1600, height: 900 },
  },
  webServer: {
    command: 'python3 -m http.server 4173 --directory public --bind 127.0.0.1 --protocol HTTP/1.1',
    url: 'http://127.0.0.1:4173/index-node.html',
    reuseExistingServer: !isCI,
    stdout: 'ignore',
    stderr: 'ignore',
    timeout: 10_000,
  },
});
