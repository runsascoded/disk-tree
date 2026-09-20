import { describe, expect, it } from 'vitest'
import { blobKey, groupMatches } from './index'

// The blob handle's in-memory span selection must be the predicate
// `selectSpans` sends D1 (index.ts), NULL semantics included.
const rect = { dLo: 2, dHi: 2, pLo: 'marin-b/x', pHi: 'marin-b/x0' }
const single = { dMin: 2, dMax: 2, pMin: 'marin-b/a', pMax: 'marin-b/m', bMax: 5_000 }
const boundary = { dMin: 1, dMax: 3, pMin: 'marin-a/z', pMax: 'marin-b/c', bMax: 5_000 }

describe('groupMatches', () => {
  it('a single-depth group needs a path overlap; a depth-boundary group only the depth', () => {
    expect(groupMatches(single, [rect])).toBe(false)
    expect(groupMatches({ ...single, pMin: 'marin-b/w', pMax: 'marin-b/z' }, [rect])).toBe(true)
    expect(groupMatches(boundary, [rect])).toBe(true)
    expect(groupMatches({ ...boundary, dMin: 3, dMax: 4 }, [rect])).toBe(false)
  })
  it('any rect may hold the group', () => {
    expect(groupMatches(single, [rect, { dLo: 1, dHi: 3, pLo: '', pHi: '\uffff' }])).toBe(true)
  })
  it('prunes by the floored byte threshold', () => {
    expect(groupMatches(boundary, [rect], 5_000.7)).toBe(true)
    expect(groupMatches(boundary, [rect], 5_001)).toBe(false)
  })
  it('a lens needs the usr range to cover the key; a single-user group also needs the rect', () => {
    const lens = { key: 'kim' }
    expect(groupMatches({ ...single, uMin: null, uMax: null }, [rect], 0, lens)).toBe(false)
    expect(groupMatches({ ...single, uMin: 'alice', uMax: 'zed' }, [rect], 0, lens)).toBe(true)
    expect(groupMatches({ ...single, uMin: 'lee', uMax: 'zed' }, [rect], 0, lens)).toBe(false)
    expect(groupMatches({ ...single, uMin: 'kim', uMax: 'kim' }, [rect], 0, lens)).toBe(false)
    expect(groupMatches({ ...single, uMin: 'kim', uMax: 'kim', pMin: 'marin-b/w', pMax: 'marin-b/z' }, [rect], 0, lens)).toBe(true)
  })
})

describe('blobKey', () => {
  it('sits beside the tier parquet', () => {
    expect(blobKey('listing/2026-09-07/index/20260907T070112Z', 'coarse24-user')).toBe('listing/2026-09-07/index/20260907T070112Z/path-index-coarse24-by-user.groups.json')
    expect(blobKey('listing/2026-07-30', 'path')).toBe('listing/2026-07-30/path-index.groups.json')
  })
})
