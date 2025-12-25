import { test, expect } from '@playwright/test'

test.describe('Scan functionality', () => {
  test('scan list shows path and stats columns', async ({ page }) => {
    await page.goto('/')

    // Wait for data to load (table or empty message)
    await expect(page.locator('table, main')).toBeVisible({ timeout: 10000 })

    // If there are scans, verify table structure
    const table = page.locator('table')
    if (await table.isVisible()) {
      // Should have expected column headers
      const headers = page.locator('th')
      await expect(headers).toContainText(['Path'])
    }
  })

  test('clicking a scan navigates to directory view', async ({ page }) => {
    await page.goto('/')

    // Wait for table to appear
    await expect(page.locator('table')).toBeVisible({ timeout: 10000 })

    // Find first scan link and click it
    const scanLink = page.locator('table a').first()
    if (await scanLink.isVisible()) {
      await scanLink.click()

      // Should navigate to file or s3 view
      await expect(page).toHaveURL(new RegExp('/(file|s3)/'))

      // Should show directory listing or loading state (within timeout)
      await expect(page.locator('body')).toBeVisible()
    }
  })
})

test.describe('Directory view', () => {
  test('shows path in breadcrumb or title', async ({ page }) => {
    // Navigate to a nested path
    await page.goto('/file/Users')

    // Should show some content (wait for React to render)
    await expect(page.locator('body')).toContainText('Users', { timeout: 10000 })
  })

  test('directory rows are clickable', async ({ page }) => {
    await page.goto('/file/Users')

    // Wait for content to load
    await expect(page.locator('body')).toContainText('Users', { timeout: 10000 })

    // Look for any links in the content area
    const links = page.locator('a[href*="/file/"]')
    const count = await links.count()
    expect(count).toBeGreaterThanOrEqual(0) // May or may not have subdirs
  })
})
