import { defineConfig } from 'vitest/config'

// Unit tests for the app's pure helpers (`src/**/*.test.ts`); the React app
// itself is covered by Playwright (`test:e2e`).
export default defineConfig({
  test: {
    include: ['src/**/*.test.ts'],
    environment: 'node',
  },
})
