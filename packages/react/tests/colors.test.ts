import { describe, expect, it } from 'vitest'
import { age01, ageDomain, ageFade } from '../src/colors'

const PANEL = 'var(--dt-treemap-fade-panel, #1a1a1f)'

describe('ageFade', () => {
  it('mixes bg toward the panel color in oklch, ink at half rate', () => {
    expect(ageFade({ bg: 'hsl(210 70% 55%)', ink: '#fff' }, 1)).toEqual({
      bg: `color-mix(in oklch, hsl(210 70% 55%), ${PANEL} 72%)`,
      ink: `color-mix(in oklch, #fff, ${PANEL} 36%)`,
    })
  })

  it('age01=0 is a no-op (identity, not a 0% mix)', () => {
    const style = { bg: 'red', ink: '#fff' }
    expect(ageFade(style, 0)).toBe(style)
  })

  it('clamps age01 above 1 to the floor', () => {
    expect(ageFade({ bg: 'red' }, 5)).toEqual({
      bg: `color-mix(in oklch, red, ${PANEL} 72%)`,
    })
  })

  it('respects floor and panel overrides', () => {
    expect(ageFade({ bg: 'red', ink: 'blue' }, 0.5, { floor: 0.5, panel: '#000' })).toEqual({
      bg: 'color-mix(in oklch, red, #000 25%)',
      ink: 'color-mix(in oklch, blue, #000 13%)',
    })
  })

  it('leaves styles without a bg untouched', () => {
    const style = { ink: '#fff' }
    expect(ageFade(style, 1)).toBe(style)
  })

  it('preserves hatch and opacity fields', () => {
    expect(ageFade({ bg: 'red', hatch: 'repeating-linear-gradient(x)', opacity: 0.5 }, 1)).toEqual({
      bg: `color-mix(in oklch, red, ${PANEL} 72%)`,
      hatch: 'repeating-linear-gradient(x)',
      opacity: 0.5,
    })
  })
})

describe('ageDomain', () => {
  it('returns [oldest, newest] ignoring null/undefined ages', () => {
    const nodes = [{ a: 30 }, { a: null }, { a: 10 }, { a: undefined }, { a: 20 }]
    expect(ageDomain(nodes, n => n.a)).toEqual([10, 30])
  })

  it('returns null when no node carries an age', () => {
    expect(ageDomain([{ a: null }, { a: undefined }], n => n.a)).toBeNull()
  })

  it('single-valued domain collapses to [v, v]', () => {
    expect(ageDomain([{ a: 7 }], n => n.a)).toEqual([7, 7])
  })
})

describe('age01', () => {
  it('newest → 0, oldest → 1, midpoint → 0.5', () => {
    expect(age01(100, [0, 100])).toBe(0)
    expect(age01(0, [0, 100])).toBe(1)
    expect(age01(50, [0, 100])).toBe(0.5)
  })

  it('clamps outside the domain', () => {
    expect(age01(200, [0, 100])).toBe(0)
    expect(age01(-50, [0, 100])).toBe(1)
  })

  it('degenerate domain maps to 0 (no fade)', () => {
    expect(age01(7, [7, 7])).toBe(0)
  })
})
