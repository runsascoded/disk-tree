import { test, expect } from '@playwright/test'

test.describe('Navigation', () => {
  test('homepage shows scan list or empty state', async ({ page }) => {
    await page.goto('/')

    // Should show the header with tabs (with longer timeout for cold start)
    await expect(page.getByRole('link', { name: 'Scans' })).toBeVisible({ timeout: 15000 })
    await expect(page.getByRole('link', { name: 'S3' })).toBeVisible()
    await expect(page.getByRole('link', { name: 'Recent' })).toBeVisible()

    // Wait for initial load - either scans table or content appears
    await expect(page.locator('table, main')).toBeVisible({ timeout: 10000 })
  })

  test('can navigate to S3 buckets page', async ({ page }) => {
    await page.goto('/')
    await page.getByRole('link', { name: 'S3' }).click()

    await expect(page).toHaveURL(/\/s3\/?$/)
    // Wait for content to appear
    await expect(page.locator('body')).toContainText(/(S3|Bucket|No bucket)/i, { timeout: 10000 })
  })

  test('can navigate to Recent page', async ({ page }) => {
    await page.goto('/')
    await page.getByRole('link', { name: 'Recent' }).click()

    await expect(page).toHaveURL('/recent')
    // Page should have loaded
    await expect(page.locator('body')).toBeVisible()
  })
})

test.describe('Directory listing', () => {
  test('can view root directory if scanned', async ({ page }) => {
    // Navigate to root - will either show directory listing or ancestor scan
    await page.goto('/file/')

    // Should have content visible
    await expect(page.locator('body')).toBeVisible()
  })
})
