import { describe, expect, it } from 'vitest'
import { bytesOlderThan, peakBin, timeTicks, totalBytes } from '../src/histogram'

const EDGES = [0, 100, 200, 300]
const BINS = [10, 20, 30]

describe('bytesOlderThan', () => {
  it('counts whole bins that lie below the threshold', () => {
    expect(bytesOlderThan(EDGES, BINS, 100)).toBe(10)
    expect(bytesOlderThan(EDGES, BINS, 200)).toBe(30)
    expect(bytesOlderThan(EDGES, BINS, 300)).toBe(60)
  })

  it('splits the straddling bin by the fraction of its span below the threshold', () => {
    expect(bytesOlderThan(EDGES, BINS, 150)).toBe(20)   // 10 + half of 20
    expect(bytesOlderThan(EDGES, BINS, 250)).toBe(45)   // 10 + 20 + half of 30
    expect(bytesOlderThan(EDGES, BINS, 275)).toBe(52.5) // 10 + 20 + 3/4 of 30
  })

  it('is 0 at or below the oldest edge, and the full total at or above the newest', () => {
    expect(bytesOlderThan(EDGES, BINS, 0)).toBe(0)
    expect(bytesOlderThan(EDGES, BINS, -50)).toBe(0)
    expect(bytesOlderThan(EDGES, BINS, 1e9)).toBe(60)
  })

  it('counts a zero-width bin whole once the threshold reaches it, never dividing by zero', () => {
    // A degenerate bin comes from a single distinct mtime; the general rule
    // ("count the bin when the threshold is at or past its upper edge")
    // applies to it unchanged.
    expect(bytesOlderThan([0, 0, 100], [7, 3], 0)).toBe(7)
    expect(bytesOlderThan([0, 0, 100], [7, 3], 50)).toBe(8.5) // 7 whole + half of 3
    expect(bytesOlderThan([0, 0, 100], [7, 3], -1)).toBe(0)
  })

  it('handles empty bins', () => {
    expect(bytesOlderThan([0, 100], [], 50)).toBe(0)
  })
})

describe('totalBytes', () => {
  it('sums the bins', () => {
    expect(totalBytes(BINS)).toBe(60)
    expect(totalBytes([])).toBe(0)
  })
})

describe('peakBin', () => {
  it('is the largest single bin across all items (the shared bar scale)', () => {
    expect(peakBin([[1, 5], [3, 2], [0, 0]])).toBe(5)
  })

  it('is 0 for no items or all-zero bins', () => {
    expect(peakBin([])).toBe(0)
    expect(peakBin([[0, 0], [0]])).toBe(0)
  })
})

describe('timeTicks', () => {
  it('spans the range inclusively with evenly-spaced values', () => {
    expect(timeTicks(0, 300, 4)).toEqual([0, 100, 200, 300])
    expect(timeTicks(0, 100, 2)).toEqual([0, 100])
  })

  it('collapses a degenerate range to a single tick', () => {
    expect(timeTicks(50, 50)).toEqual([50])
    expect(timeTicks(50, 10)).toEqual([50])
  })
})
