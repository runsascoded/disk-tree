import { describe, expect, it } from 'vitest'
import { plainRow } from './overTime.js'

describe('plainRow', () => {
  it('turns every BigInt field into a number and leaves the rest alone', () => {
    expect(plainRow({ depth: 1n, path: 'a/b', b: 12345678901234n, o: 3n, __scan_lo: 0n, __scan_hi: 15n })).toEqual(
      { depth: 1, path: 'a/b', b: 12345678901234, o: 3, __scan_lo: 0, __scan_hi: 15 },
    )
    expect(plainRow({ depth: 2, path: 'x', b: null })).toEqual({ depth: 2, path: 'x', b: null })
  })
})
