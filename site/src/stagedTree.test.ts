import { describe, expect, it } from 'vitest'
import { sortStaged, stagedTree } from './stagedTree'

describe('stagedTree: staged prefixes as one treemap tree', () => {
  it('a trie on bucket + segments; interiors re-aggregate; missing or empty stats drop out', () => {
    const stats = {
      'gs://b1/ckpt/run-a/': { b: 300, o: 3, d: 100, a: 7, us: [['ann', 300]] as [string, number][] },
      'gs://b1/ckpt/run-b/': { b: 100, o: 1, d: 200, us: [['bob', 60], ['ann', 40]] as [string, number][] },
      'gs://b2/tmp/': { b: 50, o: 5 },
      'gs://b2/empty/': { b: 0, o: 0 },
    }
    const t = stagedTree(['gs://b2/tmp/', 'gs://b1/ckpt/run-a/', 'gs://b1/ckpt/run-b/', 'gs://b2/empty/', 'gs://b3/gone/'], stats, 'staged')
    expect(t).toEqual({
      n: 'staged', k: 'dir', b: 450, o: 9, d: 125, a: 7, us: [['ann', 340], ['bob', 60]],
      c: [
        {
          n: 'b1', k: 'dir', b: 400, o: 4, d: 125, a: 7, us: [['ann', 340], ['bob', 60]],
          c: [{
            n: 'ckpt', k: 'dir', b: 400, o: 4, d: 125, a: 7, us: [['ann', 340], ['bob', 60]],
            c: [
              { n: 'run-a', k: 'dir', b: 300, o: 3, d: 100, a: 7, us: [['ann', 300]] },
              { n: 'run-b', k: 'dir', b: 100, o: 1, d: 200, us: [['bob', 60], ['ann', 40]] },
            ],
          }],
        },
        { n: 'b2', k: 'dir', b: 50, o: 5, c: [{ n: 'tmp', k: 'dir', b: 50, o: 5 }] },
      ],
    })
  })
})

describe('sortStaged', () => {
  const rows = [
    { prefix: 'gs://b/a/', added_ts: 3, stat: { b: 10, o: 1 } },
    { prefix: 'gs://b/b/', added_ts: 1 },
    { prefix: 'gs://b/c/', added_ts: 2, stat: { b: 30, o: 1, d: 5 } },
    { prefix: 'gs://b/d/', added_ts: 2, stat: { b: 10, o: 2 } },
  ]
  const order = (k: Parameters<typeof sortStaged>[1], asc: boolean) => sortStaged(rows, k, asc).map(r => r.prefix.slice(7, 8))
  it('by bytes, created, staged time; rows without the value last in both directions; ties by prefix', () => {
    expect([order('b', false), order('b', true), order('d', false), order('staged', true), order('prefix', false)]).toEqual([
      ['c', 'a', 'd', 'b'],
      ['a', 'd', 'c', 'b'],
      ['c', 'a', 'b', 'd'],
      ['b', 'c', 'd', 'a'],
      ['d', 'c', 'b', 'a'],
    ])
  })
})
