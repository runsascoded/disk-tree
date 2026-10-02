// `/staged`'s data shaping: the staged prefixes as a treemap tree (a trie on
// bucket + path segments, the staged prefixes its leaves) and the item
// table's sort. Pure, so the page's numbers are tested here.
import type { TreeNode } from './types'

/** One staged prefix's numbers at the chosen scan (`/api/prefix-stats`). */
export interface PrefixStat { b: number; o: number; d?: number; a?: number; us?: [string, number][] }

/** `gs://bucket/a/b/` → `['bucket', 'a', 'b']`. */
export const prefixSegs = (prefix: string): string[] =>
  prefix.replace(/^[a-z0-9]+:\/\//, '').replace(/\/+$/, '').split('/')

/**
 * The staged prefixes as one tree under `rootName`: interior nodes are the
 * path segments between them, re-aggregated from the leaves (bytes and
 * objects summed, owners summed, created day byte-weighted, last read the
 * max). Prefixes without stats (gone by this scan, or not loaded) are left
 * out; children sort heaviest first.
 */
export function stagedTree(prefixes: string[], stats: Record<string, PrefixStat>, rootName: string): TreeNode {
  type Mut = { n: string; c: Map<string, Mut>; leaf?: PrefixStat }
  const root: Mut = { n: rootName, c: new Map() }
  for (const p of prefixes) {
    const s = stats[p]
    if (!s || s.b <= 0) continue
    let at = root
    for (const seg of prefixSegs(p)) {
      let next = at.c.get(seg)
      if (!next) at.c.set(seg, (next = { n: seg, c: new Map() }))
      at = next
    }
    at.leaf = s
  }
  const build = (m: Mut): TreeNode => {
    if (m.leaf) {
      const s = m.leaf
      return { n: m.n, k: 'dir', b: s.b, o: s.o, ...(s.d != null ? { d: s.d } : {}), ...(s.a != null ? { a: s.a } : {}), ...(s.us?.length ? { us: s.us } : {}) }
    }
    const c = [...m.c.values()].map(build).sort((x, y) => y.b - x.b)
    const b = c.reduce((t, k) => t + k.b, 0)
    const o = c.reduce((t, k) => t + k.o, 0)
    const dw = c.filter(k => k.d != null)
    const dB = dw.reduce((t, k) => t + k.b, 0)
    const as = c.filter(k => k.a != null).map(k => k.a!)
    const us = new Map<string, number>()
    for (const k of c) for (const [u, ub] of k.us ?? []) us.set(u, (us.get(u) ?? 0) + ub)
    return {
      n: m.n, k: 'dir', b, o,
      ...(dB > 0 ? { d: Math.round(dw.reduce((t, k) => t + k.d! * k.b, 0) / dB) } : {}),
      ...(as.length ? { a: Math.max(...as) } : {}),
      ...(us.size ? { us: [...us].sort((x, y) => y[1] - x[1]) } : {}),
      c,
    }
  }
  return build(root)
}

export type StagedSortKey = 'prefix' | 'b' | 'o' | 'd' | 'a' | 'staged'

/** The item table's order: by `key`, missing values last either way, ties by prefix. */
export function sortStaged<T extends { prefix: string; added_ts: number; stat?: PrefixStat }>(rows: T[], key: StagedSortKey, asc: boolean): T[] {
  const val = (r: T): number | string | undefined =>
    key === 'prefix' ? r.prefix
    : key === 'staged' ? r.added_ts
    : r.stat?.[key]
  return [...rows].sort((x, y) => {
    const a = val(x)
    const b = val(y)
    if (a === undefined || b === undefined) {
      if (a === b) return x.prefix.localeCompare(y.prefix)
      return a === undefined ? 1 : -1
    }
    const c = typeof a === 'string' ? a.localeCompare(b as string) : a - (b as number)
    return (asc ? c : -c) || x.prefix.localeCompare(y.prefix)
  })
}
