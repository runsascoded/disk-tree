import { describe, expect, it } from 'vitest'
import { blockOf, viewOf, type BlockIndex } from './extras.js'

const idx: BlockIndex = { v: 1, n: 0, size: 0, keys: ['a/1', 'b/2', 'd/1'], offsets: [0, 100, 200] }

describe('blockOf', () => {
  it('finds the block whose first key is ≤ the key', () => {
    expect(blockOf(idx, 'a/1')).toBe(0)
    expect(blockOf(idx, 'a/9')).toBe(0)
    expect(blockOf(idx, 'b/2')).toBe(1)
    expect(blockOf(idx, 'c')).toBe(1)
    expect(blockOf(idx, 'd/1')).toBe(2)
    expect(blockOf(idx, 'z')).toBe(2)
  })
  it('is −1 before the first key', () => {
    expect(blockOf(idx, '0')).toBe(-1)
  })
})

describe('viewOf', () => {
  const v = viewOf(
    ['b/grug/run1', 'b/grug/run1/checkpoints'],
    ['b/grug\tcalvin\twandb-run\tent/proj/abc', 'b/data\tryan\tmanual\t', 'b/grug/run9\tkevin\t\t'],
  )
  it('holds the checkpoint dirs', () => {
    expect([...v.ck].sort()).toEqual(['b/grug/run1', 'b/grug/run1/checkpoints'])
  })
  it('resolves provenance from the deepest attributing ancestor with that user', () => {
    expect(v.provenance('b/grug/run1/checkpoints', 'calvin')).toEqual(['wandb-run', 'ent/proj/abc', 'b/grug'])
    expect(v.provenance('b/data/x', 'ryan')).toEqual(['manual', null, 'b/data'])
    expect(v.provenance('b/grug/run9/steps', 'kevin')).toEqual(['unknown', null, 'b/grug/run9'])
  })
  it('is null for another user, or off the map', () => {
    expect(v.provenance('b/grug/run1', 'ryan')).toBeNull()
    expect(v.provenance('c/x', 'calvin')).toBeNull()
  })
})
