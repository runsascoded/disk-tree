import { describe, expect, it } from 'vitest'
import { expandIntervals, type Interval } from './overTime.js'

describe('expandIntervals', () => {
  const scans = ['2026-08-19T1201', '2026-08-19T2359', '2026-08-20T1201', '2026-08-20T2359']

  it('paints each interval onto its covered scans', () => {
    const rows: Interval[] = [
      { b: 100, o: 10, lo: 0, hi: 1 },
      { b: 200, o: 20, lo: 2, hi: 3 },
    ]
    expect([...expandIntervals(rows, scans)]).toEqual([
      ['2026-08-19T1201', { b: 100, o: 10 }],
      ['2026-08-19T2359', { b: 100, o: 10 }],
      ['2026-08-20T1201', { b: 200, o: 20 }],
      ['2026-08-20T2359', { b: 200, o: 20 }],
    ])
  })

  it('leaves a gap where a path is absent (interval does not cover it)', () => {
    // present only scans 1–2 → 0 and 3 get no point
    const m = expandIntervals([{ b: 7, o: 1, lo: 1, hi: 2 }], scans)
    expect([...m.keys()]).toEqual(['2026-08-19T2359', '2026-08-20T1201'])
  })

  it('ignores interval bounds past the scan list', () => {
    const m = expandIntervals([{ b: 5, o: 1, lo: 2, hi: 9 }], scans)
    expect([...m.keys()]).toEqual(['2026-08-20T1201', '2026-08-20T2359'])
  })
})
