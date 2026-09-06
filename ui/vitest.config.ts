import { defineConfig } from 'vitest/config'

// Unit tests for the Pages Functions (`cfn/`, `functions/`); the React app is
// covered by Playwright (`test:e2e`).
export default defineConfig({
  test: {
    include: ['cfn/**/*.test.ts'],
    environment: 'node',
  },
})
