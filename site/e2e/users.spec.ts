import { expect, test } from '@playwright/test'
import { collectErrors, SIZE_RE } from './helpers'

test.describe('/users', () => {
  test('renders keep/sweep/unmarked columns (the 2026-08-31 regression)', async ({ page }) => {
    const errors = collectErrors(page)
    await page.goto('/users')

    // The estate table loads: at least one user row shows an attributed size.
    await expect(page.getByText(SIZE_RE).first()).toBeVisible()

    // The bug: /api/marks/totals 1102'd, so every keep/sweep/unmarked cell was
    // stuck at the '…' placeholder (UserPage `cell()` renders '…' until `fates`
    // resolves). A healthy page resolves them to bytes or '—' — none remain '…'.
    await expect(page.locator('td.num', { hasText: '…' })).toHaveCount(0)

    expect(errors).toEqual([])
  })
})
