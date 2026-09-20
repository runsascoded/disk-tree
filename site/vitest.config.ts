import { defineConfig } from 'vitest/config'

// Unit specs for the pure server folds (`functions/_lib`) and client helpers;
// the Playwright suite under e2e/ has its own runner (`pnpm test:e2e`).
export default defineConfig({
  test: { include: ['functions/**/*.test.ts', 'src/**/*.test.ts'] },
})
