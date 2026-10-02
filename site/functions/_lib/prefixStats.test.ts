import { describe, expect, it } from 'vitest'
import { foldPrefixStats } from './prefixStats'

const row = (path: string, usr: string | null, size: number, o: number, extra: { mm?: number; read?: number } = {}) => ({
  path, depth: path.split('/').length, usr, size, n_files: o,
  mtime_mean: extra.mm ?? null, mtime_w: extra.mm != null ? size : 0, last_read: extra.read ?? null,
})

describe('foldPrefixStats: index rows → one stat per staged prefix', () => {
  it('sums owner slices, weights the created day, keeps the latest read, drops rows nobody asked for', () => {
    const day = 86400
    const rows = [
      row('b/x', 'ann', 300, 3, { mm: 100 * day, read: 7 }),
      row('b/x', 'bob', 100, 1, { mm: 200 * day, read: 9 }),
      row('b/x', null, 600, 2),
      row('b/y', null, 5, 1),
      row('b/x/deeper', 'ann', 50, 1),
    ]
    expect(foldPrefixStats(['gs://b/x/', 'gs://b/y', 'gs://b/gone/'], rows)).toEqual({
      'gs://b/x/': { b: 1000, o: 6, d: 125, a: 9, us: [['ann', 300], ['bob', 100]] },
      'gs://b/y': { b: 5, o: 1 },
    })
  })

  it('the same path asked twice (with and without the slash) answers both', () => {
    expect(foldPrefixStats(['gs://b/x/', 'gs://b/x'], [row('b/x', null, 1, 1)])).toEqual({
      'gs://b/x/': { b: 1, o: 1 },
      'gs://b/x': { b: 1, o: 1 },
    })
  })
})
