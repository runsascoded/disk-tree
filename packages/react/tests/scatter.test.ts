import { describe, expect, it } from 'vitest'
import {
  decadesBetween,
  isoScoreDecades,
  isoScoreSegment,
  isoScoresForData,
  logDomain,
  logPos,
  logTicks,
  radiusFor,
} from '../src/scatter'

describe('logDomain', () => {
  it('snaps out to the enclosing decades', () => {
    expect(logDomain([3, 250])).toEqual([1, 1000])
    expect(logDomain([0.05, 7])).toEqual([0.01, 10])
  })

  it('keeps exact decade endpoints instead of padding them', () => {
    expect(logDomain([0.01, 1000])).toEqual([0.01, 1000])
  })

  it('skips non-positive, non-finite and nullish values', () => {
    expect(logDomain([0, -5, null, undefined, NaN, Infinity, 3, 40])).toEqual([1, 100])
  })

  it('gives a value that is itself a decade a decade of headroom either side', () => {
    expect(logDomain([1])).toEqual([0.1, 10])
  })

  it('leaves a repeated non-decade value in its own enclosing decade', () => {
    expect(logDomain([5, 5])).toEqual([1, 10])
  })

  it('returns null when nothing is plottable', () => {
    expect(logDomain([])).toBeNull()
    expect(logDomain([0, -1, null])).toBeNull()
  })
})

describe('logPos', () => {
  it('maps endpoints to 0 and 1, and the geometric midpoint to 0.5', () => {
    expect(logPos(1, [1, 100])).toBe(0)
    expect(logPos(100, [1, 100])).toBe(1)
    expect(logPos(10, [1, 100])).toBe(0.5)
  })

  it('clamps outside the domain', () => {
    expect(logPos(0.01, [1, 100])).toBe(0)
    expect(logPos(1e6, [1, 100])).toBe(1)
  })

  it('degenerate domain or non-positive value maps to 0', () => {
    expect(logPos(5, [10, 10])).toBe(0)
    expect(logPos(0, [1, 100])).toBe(0)
  })
})

describe('logTicks', () => {
  it('returns every decade when they fit under maxTicks', () => {
    expect(logTicks([0.01, 1000])).toEqual([0.01, 0.1, 1, 10, 100, 1000])
  })

  it('thins by whole decades when they do not', () => {
    expect(logTicks([1, 1e9], 6)).toEqual([1, 100, 10_000, 1e6, 1e8])
  })

  it('only returns decades inside a non-snapped domain', () => {
    expect(logTicks([3, 250])).toEqual([10, 100])
  })

  it('returns [] when no decade falls inside', () => {
    expect(logTicks([2, 9])).toEqual([])
  })
})

describe('isoScoreSegment', () => {
  it('spans corner to corner when the contour crosses the box diagonally', () => {
    expect(isoScoreSegment(1, [0.1, 10], [0.1, 10])).toEqual([[0.1, 10], [10, 0.1]])
  })

  it('clips to the box edges the contour actually meets', () => {
    expect(isoScoreSegment(10, [0.1, 10], [0.1, 10])).toEqual([[1, 10], [10, 1]])
  })

  it('returns null for a corner-grazing contour', () => {
    expect(isoScoreSegment(100, [0.1, 10], [0.1, 10])).toBeNull()
    expect(isoScoreSegment(0.01, [0.1, 10], [0.1, 10])).toBeNull()
  })

  it('returns null for contours outside the box, and for non-positive scores', () => {
    expect(isoScoreSegment(1e6, [0.1, 10], [0.1, 10])).toBeNull()
    expect(isoScoreSegment(0, [0.1, 10], [0.1, 10])).toBeNull()
    expect(isoScoreSegment(-1, [0.1, 10], [0.1, 10])).toBeNull()
  })

  it('every returned endpoint satisfies x·y = score', () => {
    const seg = isoScoreSegment(2, [0.1, 10], [0.01, 100])!
    expect(seg.map(([x, y]) => Number((x * y).toPrecision(12)))).toEqual([2, 2])
  })
})

describe('isoScoreDecades', () => {
  it('excludes the corner-only decades, keeping the interior ones', () => {
    // Box corners score 0.01 (bottom-left) and 100 (top-right).
    expect(isoScoreDecades([0.1, 10], [0.1, 10])).toEqual([0.1, 1, 10])
  })

  it('thins to at most maxLines, stepping up from the lowest interior decade', () => {
    // Corners score 1e-6 … 1e6, so interior decades are 1e-5 … 1e5 (11 of
    // them); 3 lines means every 4th, from the bottom.
    expect(isoScoreDecades([1e-3, 1e3], [1e-3, 1e3], 3)).toEqual([1e-5, 0.1, 1000])
  })

  it('returns [] when no decade crosses the interior', () => {
    expect(isoScoreDecades([1, 2], [1, 2])).toEqual([])
  })

  it('every returned decade has a real segment in the box', () => {
    const x: [number, number] = [0.01, 100]
    const y: [number, number] = [0.001, 10]
    expect(isoScoreDecades(x, y).every(s => isoScoreSegment(s, x, y) !== null)).toBe(true)
  })
})

describe('decadesBetween', () => {
  it('returns the strictly-interior decades', () => {
    expect(decadesBetween(0.5, 500)).toEqual([1, 10, 100])
    expect(decadesBetween(1, 100)).toEqual([10])
  })

  it('thins from the bottom to at most maxCount', () => {
    expect(decadesBetween(0.5, 1e6, 3)).toEqual([1, 100, 10_000])
  })

  it('returns [] for sub-decade or non-positive ranges', () => {
    expect(decadesBetween(2, 9)).toEqual([])
    expect(decadesBetween(0, 100)).toEqual([])
    expect(decadesBetween(-1, 100)).toEqual([])
  })
})

describe('isoScoresForData', () => {
  const x: [number, number] = [1e-3, 10]
  const y: [number, number] = [1e-6, 1]

  it('follows the data, not the empty box corner', () => {
    // Box corners span 1e-9 … 10, but the points all score ~1e-3 … ~1e-1.
    const scores = [1.2e-3, 4e-2, 9e-2]
    expect(isoScoresForData(scores, x, y)).toEqual([0.01])
    expect(isoScoreDecades(x, y)).toEqual([1e-8, 1e-6, 1e-4, 0.01, 1])
  })

  it('labels the nearest decade when the data spans less than one', () => {
    expect(isoScoresForData([2e-4, 5e-4], x, y)).toEqual([1e-3])
  })

  it('falls back to the box decades when there are no scores', () => {
    expect(isoScoresForData([], x, y)).toEqual(isoScoreDecades(x, y))
  })

  it('never returns a decade whose contour misses the box', () => {
    const scores = [1e6, 1e7]
    expect(isoScoresForData(scores, x, y).every(s => isoScoreSegment(s, x, y) !== null)).toBe(true)
  })
})

describe('radiusFor', () => {
  it('scales area, not radius: quarter weight → half radius', () => {
    expect(radiusFor(100, 100, 2.5, 14)).toBe(14)
    expect(radiusFor(25, 100, 2.5, 14)).toBe(7)
  })

  it('floors at rMin for tiny, zero, and missing weights', () => {
    expect(radiusFor(1, 1e6, 2.5, 14)).toBe(2.5)
    expect(radiusFor(0, 100, 2.5, 14)).toBe(2.5)
    expect(radiusFor(null, 100, 2.5, 14)).toBe(2.5)
    expect(radiusFor(undefined, 100, 2.5, 14)).toBe(2.5)
  })

  it('clamps weights above the max and handles a zero max', () => {
    expect(radiusFor(500, 100, 2.5, 14)).toBe(14)
    expect(radiusFor(5, 0, 2.5, 14)).toBe(2.5)
  })
})
