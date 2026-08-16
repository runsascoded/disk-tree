import { describe, expect, it } from 'vitest'
import { formatTbYears, SEC_PER_YEAR, sumTbYears, TB } from '../src/stats'

const NOW = 1_700_000_000

describe('sumTbYears', () => {
  it('1 TB aged exactly 1 year scores 1', () => {
    expect(sumTbYears(TB, NOW - SEC_PER_YEAR, NOW)).toBe(1)
  })

  it('scales linearly in both size and age', () => {
    expect(sumTbYears(2 * TB, NOW - SEC_PER_YEAR, NOW)).toBe(2)
    expect(sumTbYears(TB, NOW - 2 * SEC_PER_YEAR, NOW)).toBe(2)
    expect(sumTbYears(0.5 * TB, NOW - 4 * SEC_PER_YEAR, NOW)).toBe(2)
  })

  it('zero size or zero age scores 0', () => {
    expect(sumTbYears(0, NOW - SEC_PER_YEAR, NOW)).toBe(0)
    expect(sumTbYears(TB, NOW, NOW)).toBe(0)
  })

  it('missing mean → null (not 0 — unknown, not fresh)', () => {
    expect(sumTbYears(TB, null, NOW)).toBeNull()
    expect(sumTbYears(TB, undefined, NOW)).toBeNull()
  })

  it('additivity: parent score = Σ children scores (exact for these inputs)', () => {
    // Children: (4 TB, mean now−1y) + (2 TB, mean now−4y).
    // Parent: size 6 TB, mean = (4·(now−1y) + 2·(now−4y)) / 6 = now − 2y.
    const meanParent = (4 * (NOW - SEC_PER_YEAR) + 2 * (NOW - 4 * SEC_PER_YEAR)) / 6
    expect(sumTbYears(4 * TB, NOW - SEC_PER_YEAR, NOW)! + sumTbYears(2 * TB, NOW - 4 * SEC_PER_YEAR, NOW)!).toBe(12)
    expect(sumTbYears(6 * TB, meanParent, NOW)).toBe(12)
  })
})

describe('formatTbYears', () => {
  it('3 sig-figs by default, trailing zeros stripped', () => {
    expect(formatTbYears(123.456)).toBe('123 TB·yr')
    expect(formatTbYears(1.5)).toBe('1.5 TB·yr')
    expect(formatTbYears(0)).toBe('0 TB·yr')
  })

  it('steps the byte unit down so small scores stay readable', () => {
    expect(formatTbYears(0.123456)).toBe('123 GB·yr')
    expect(formatTbYears(1e-5)).toBe('10 MB·yr')
    expect(formatTbYears(1e-9)).toBe('1 KB·yr')
    expect(formatTbYears(1e-12)).toBe('1 B·yr')
  })

  it('clamps at PB·yr for huge scores rather than inventing a unit', () => {
    expect(formatTbYears(1e6)).toBe('1000 PB·yr')
  })

  it('respects the sigFigs override', () => {
    expect(formatTbYears(0.123456, 5)).toBe('123.46 GB·yr')
    expect(formatTbYears(987, 2)).toBe('990 TB·yr')
  })

  it('non-positive scores render as a plain zero', () => {
    expect(formatTbYears(-1)).toBe('0 TB·yr')
  })
})
