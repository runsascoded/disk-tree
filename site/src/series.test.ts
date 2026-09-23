import { describe, expect, it } from 'vitest'
import { geneses, pickAnnotations, prominentExtrema, stackSeries, youngestGenesis } from './series'

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

describe('pickAnnotations', () => {
  // first, min, max, last distinct; x=2 is a mid-series dip a window can select.
  const line = [
    { x: 0, y: 80 }, // first
    { x: 1, y: 62 }, // global min
    { x: 2, y: 70 }, // a dip (below mid=78.5)
    { x: 3, y: 95 }, // global max
    { x: 4, y: 88 },
    { x: 5, y: 85 }, // last
  ]

  it('picks max, min, first, last (in that order) with no selection', () => {
    expect(pickAnnotations(line)).toEqual([
      { x: 3, y: 95, below: false },
      { x: 1, y: 62, below: true },
      { x: 0, y: 80, below: false },
      { x: 5, y: 85, below: false },
    ])
  })

  it('adds the point nearest each window edge (the size at the selection ends)', () => {
    expect(pickAnnotations(line, [2, 4])).toEqual([
      { x: 3, y: 95, below: false },
      { x: 1, y: 62, below: true },
      { x: 0, y: 80, below: false },
      { x: 5, y: 85, below: false },
      { x: 2, y: 70, below: true }, // window start: a low → below
      { x: 4, y: 88, below: false }, // window end: a high → above
    ])
  })

  it('snaps a window edge to the nearest point, and collapses when it lands on an existing role', () => {
    // edges 1.6→x2 (dip) and 4.9→x5 (already `last`, so no duplicate)
    expect(pickAnnotations(line, [1.6, 4.9])).toEqual([
      { x: 3, y: 95, below: false },
      { x: 1, y: 62, below: true },
      { x: 0, y: 80, below: false },
      { x: 5, y: 85, below: false },
      { x: 2, y: 70, below: true },
    ])
  })

  it('needs at least two points', () => {
    expect(pickAnnotations([])).toEqual([])
    expect(pickAnnotations([{ x: 0, y: 5 }])).toEqual([])
    expect(pickAnnotations([{ x: 0, y: 5 }], [0, 0])).toEqual([])
  })

  // A W-shape with interior peaks/troughs that are NOT the globals.
  const wave = [
    { x: 0, y: 50 }, // first
    { x: 1, y: 90 }, // interior peak
    { x: 2, y: 40 }, // interior trough
    { x: 3, y: 70 }, // interior peak
    { x: 4, y: 30 }, // global min
    { x: 5, y: 100 }, // global max = last
  ]

  it('radius=1 adds every prominent interior peak/valley not already a global/end', () => {
    // globals/ends first, then interior peaks (x1, x3), then interior valley (x2).
    expect(pickAnnotations(wave, undefined, 1)).toEqual([
      { x: 5, y: 100, below: false }, // global max (= last)
      { x: 4, y: 30, below: true }, // global min
      { x: 0, y: 50, below: true }, // first
      { x: 1, y: 90, below: false }, // interior peak
      { x: 3, y: 70, below: false }, // interior peak
      { x: 2, y: 40, below: true }, // interior valley
    ])
  })

  it('a wider suppression radius keeps only the more prominent of nearby peaks/valleys', () => {
    // r=2.5: peak x1 outranks x3 (|1-3|=2 < 2.5); valley x4 outranks x2.
    expect(pickAnnotations(wave, undefined, 2.5)).toEqual([
      { x: 5, y: 100, below: false },
      { x: 4, y: 30, below: true },
      { x: 0, y: 50, below: true },
      { x: 1, y: 90, below: false }, // the more prominent interior peak
    ])
  })

  // A sharp spike (x=3) shorter than a distant endpoint (x=5): "tallest within
  // ±r" would suppress it, but prominence keeps it (you descend into the x=4
  // trough to reach the taller x=5). This is the case the eye wants labeled.
  const spike = [
    { x: 0, y: 10 }, { x: 1, y: 30 }, { x: 2, y: 15 }, { x: 3, y: 95 }, { x: 4, y: 40 }, { x: 5, y: 100 },
  ]
  it('keeps a prominent spike beside a taller-but-distant endpoint', () => {
    expect(pickAnnotations(spike, undefined, 2.5)).toEqual([
      { x: 5, y: 100, below: false }, // global max (= last)
      { x: 0, y: 10, below: true }, // global min (= first)
      { x: 3, y: 95, below: false }, // the spike — survives despite x=5 being taller
      { x: 4, y: 40, below: true }, // the trough before the endpoint climb
    ])
  })
})

describe('prominentExtrema', () => {
  const wave = [
    { x: 0, y: 50 }, { x: 1, y: 90 }, { x: 2, y: 40 }, { x: 3, y: 70 }, { x: 4, y: 30 }, { x: 5, y: 100 },
  ]
  it('all interior peaks/valleys survive when the suppression radius is small', () => {
    expect(prominentExtrema(wave, 1)).toEqual({
      maxes: [{ x: 1, y: 90 }, { x: 3, y: 70 }],
      mins: [{ x: 2, y: 40 }, { x: 4, y: 30 }],
    })
  })
  it('within the radius, the more prominent of two peaks/valleys wins', () => {
    expect(prominentExtrema(wave, 2.5)).toEqual({
      maxes: [{ x: 1, y: 90 }], // beats x3
      mins: [{ x: 4, y: 30 }], // beats x2
    })
  })
  it('ranks a spike above a gentle bump by prominence, not raw height', () => {
    const spike = [
      { x: 0, y: 10 }, { x: 1, y: 30 }, { x: 2, y: 15 }, { x: 3, y: 95 }, { x: 4, y: 40 }, { x: 5, y: 100 },
    ]
    // x3 (95) outranks x1 (30) and isn't suppressed by the taller endpoint x5.
    expect(prominentExtrema(spike, 2.5)).toEqual({ maxes: [{ x: 3, y: 95 }], mins: [{ x: 4, y: 40 }] })
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
