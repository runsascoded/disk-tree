import { describe, expect, it } from 'vitest'
import { geneses, stackSeries, youngestGenesis } from './series'

const P = { key: 'p', points: [{ x: 1, y: 90 }, { x: 2, y: 100 }, { x: 3, y: 101 }] }
const H = { key: 'h', points: [{ x: 3, y: 40 }] }

describe('geneses', () => {
  it('first x per trace; empty traces have none', () => {
    expect(geneses([P, H, { key: 'e', points: [] }])).toEqual(new Map([['p', 1], ['h', 3]]))
    expect(youngestGenesis([P, H])).toBe(3)
    expect(youngestGenesis([P])).toBeNull()
    expect(youngestGenesis([])).toBeNull()
  })
})

describe('stackSeries', () => {
  it('bands run between running sums; a late root starts at its genesis', () => {
    expect(stackSeries([P, H])).toEqual([
      { key: 'p', points: [{ x: 1, y0: 0, y: 90 }, { x: 2, y0: 0, y: 100 }, { x: 3, y0: 0, y: 101 }] },
      { key: 'h', points: [{ x: 3, y0: 101, y: 141 }] },
    ])
  })
  it('a gap inside a trace counts as 0 for the bands above it', () => {
    const a = { key: 'a', points: [{ x: 1, y: 10 }, { x: 3, y: 10 }] }
    const b = { key: 'b', points: [{ x: 1, y: 5 }, { x: 2, y: 5 }, { x: 3, y: 5 }] }
    expect(stackSeries([a, b])).toEqual([
      { key: 'a', points: [{ x: 1, y0: 0, y: 10 }, { x: 2, y0: 0, y: 0 }, { x: 3, y0: 0, y: 10 }] },
      { key: 'b', points: [{ x: 1, y0: 10, y: 15 }, { x: 2, y0: 0, y: 5 }, { x: 3, y0: 10, y: 15 }] },
    ])
  })
})
