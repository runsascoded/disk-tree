import { describe, expect, it } from 'vitest'
import { planAge, variantForPlan } from './agePyramid'

describe('planAge — budget picks the finest tier that fits', () => {
  // A 30-day window: 1d≈30 bins, 1mo=1, 1y=1 (tiers are 1d/1mo/1y).
  const FROM = new Date('2026-06-01T00:00:00Z')
  const TO = new Date('2026-07-01T00:00:00Z')
  const cases: [number, string][] = [
    [100, 'age-pyramid-1d'],   // 1d (~30) fits
    [10, 'age-pyramid-1mo'],   // 1d too many, 1mo (1) fits
  ]
  for (const [budget, variant] of cases) {
    it(`30d, budget ${budget} → ${variant}`, () => {
      expect(variantForPlan(planAge(FROM, TO, budget))).toBe(variant)
    })
  }

  it('a 6-year window under budget 3 falls back to the coarsest tier (1y)', () => {
    const from = new Date('2020-01-01T00:00:00Z')
    const to = new Date('2026-01-01T00:00:00Z')
    // 1d≈2190, 1mo≈72, 1y≈6 — none ≤ 3, so coarsest.
    expect(variantForPlan(planAge(from, to, 3))).toBe('age-pyramid-1y')
  })
})
