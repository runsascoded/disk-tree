import { describe, expect, it } from 'vitest'
import { canonId, shortUserKey } from './UserChip'

describe('shortUserKey', () => {
  it('is the shortest alias that prefixes the canonical id, never an unrelated handle', () => {
    expect(shortUserKey('kaiyue-wen')).toBe('kaiyue')
    expect(canonId('when')).toBe('kaiyue-wen') // old `?o=when` links still resolve
    expect(shortUserKey('nobody-here')).toBe('nobody-here')
  })
})
