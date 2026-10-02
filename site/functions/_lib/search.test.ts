import { beforeAll, describe, expect, it, vi } from 'vitest'
import type { Env } from './auth'
import { matchRoots } from './filter'
import { type IndexHandle, openIndex, readRects, type Row } from './index'
import { type NamePred, parseQuery } from './scope'
import { type SearchLimits, SEARCH_LIMITS, searchKey, searchRoots } from './search'
import { planPositive } from './searchQuery'
import { sqliteD1 } from './testD1'
import { type D1Variant, fixture, FILES, GETS, readJson, seedGeneration } from './testStore'
import { buildDiff, buildView } from './view'

vi.mock('@rdub/file-tree/stores/s3', async () => ({ S3Store: (await import('./testStore')).S3Store }))

// The search index reader (specs/path-store-search.md §4) over
// `fixtures/v2-search/` (`gen.py` `write_v2_search`): buckets `bk` (6000
// filler objects `fill/f*` + `ttl` dirs nested in `ttl` dirs, case variants,
// `.safetensors` objects, `ckpt…final` paths, a Kelvin-sign `Key`) and `zz`,
// `path` + `bysize` sorts in 2048-row groups (3 each), the search sidecars at
// 2048-row data groups and 2 rows per directory group. Served twice: with the
// sidecars (SEARCH) and without them (PLAIN: the same generation, the
// pre-index read).

const SEARCH = '2026-10-01T0001'
const PLAIN = '2026-10-01T0002'
const SEARCH_PQ = '2026-10-01T0003'
const dirOf = (date: string) => `cw-l2/${date}/index/g`
const files = { path: { parquet: 'v2-search/path-index.parquet', groups: 'v2-search/path-index.groups.json' }, bysize: { parquet: 'v2-search/path-index-bysize.parquet', groups: 'v2-search/path-index-bysize.groups.json' } }
const MiB = 1 << 20
let env: Env

beforeAll(async () => {
  ;(globalThis as unknown as { caches: unknown }).caches = { default: { match: async () => undefined, put: async () => {} } }
  const { db, raw } = await sqliteD1('cw')
  const v = await readJson<Record<string, D1Variant>>('v2-search/d1.json')
  for (const date of [SEARCH, PLAIN]) seedGeneration(raw, { date, gen: 'g', dir: dirOf(date), variants: v, files })
  // Retired from D1: the `path` groups' metadata comes from the blob.
  seedGeneration(raw, { date: SEARCH_PQ, gen: 'g', dir: dirOf(SEARCH_PQ), variants: v, files, retired: ['path', 'bysize'] })
  for (const date of [SEARCH, SEARCH_PQ]) for (const role of ['names', 'trigrams', 'search'] as const) FILES.set(searchKey(dirOf(date), role), fixture(`v2-search/path-index.${role}.parquet`))
  env = { DB: db, ROOT_LABEL: 'root', GCS_HMAC_KEY_ID: 'k', GCS_HMAC_SECRET: 's' } as Env
})

const allRows = async (date: string): Promise<Row[]> => readRects(await openIndex(env, date), [{ dLo: 1, dHi: 1e9, pLo: '', pHi: '￿' }])
const under = (root: string) => (p: string) => root === '' || p.startsWith(root + '/')
/** `matchRoots` over every row of the store under `root` — what the index must reproduce. */
async function brute(q: string, root: string): Promise<string[]> {
  const rows = await allRows(PLAIN)
  return matchRoots(rows.map(r => r.path).filter(under(root)), parseQuery(q)!, root)
}
/** The positive search for `pred` (null: the index declines or has no sidecar). */
async function find(h: IndexHandle, pred: NamePred, root: string, limits?: SearchLimits) {
  const plan = planPositive(pred.query!)
  return plan ? searchRoots(env, h, pred, plan, root, limits) : null
}
const byPath = (rows: Row[]) => [...rows].sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : a.usr! < b.usr! ? -1 : 1))

describe('searchRoots: exactly the outermost matches', () => {
  it('a substring at the root: every `ttl` dir and object, nested ones collapsed, any case', async () => {
    const got = (await find(await openIndex(env, SEARCH), parseQuery('ttl')!, ''))!
    expect(got.roots).toEqual(['bk/fill/zz-TTL-b', 'bk/fill/zz-ttl-a', 'bk/iris/TTL-misc', 'bk/tmp/ttl=14d', 'bk/tmp/ttl=7d', 'zz/Checkpoints/ttl'])
    expect(got.truncated).toBe(false)
    // The roots' own rows, every one: phase 1's aggregates come from them.
    const all = await allRows(PLAIN)
    expect(byPath(got.rows)).toEqual(byPath(all.filter(r => got.roots.includes(r.path))))
    // 1 trigram in 1 postings group → 7 candidates (`ttl`, `TTL-misc`,
    // `inner-ttl`, `ttl=14d`, `ttl=7d`, `zz-ttl-a`, `zz-TTL-b`) in 2 names
    // groups, all verified, living in 1 `path` group; 2 directory groups read.
    expect(got.stats).toEqual({ mode: 'trigrams', trigrams: 1, postingsRgs: 1, candidates: 7, dirGroups: 2, namesRgs: 2, names: 7, pathRgs: 1, lifted: 0 })
  })

  const QUERIES = [
    'ttl', 'TTL', 'ttl=14d', 'ttl|safetensors', 'run-a/ckpt', 'tmp/ttl', 'swarm', '.safetensors', 'key', 'checkpoints', 'ckpt', 'gr', 'f0042', 'zz-',
    'ckpt final', 'grug swarm', 'ckpt*final', 'model-*-of-*.safetensors', 'tmp/*/ckpt', 'f00*1', '"ttl=7d"', 'TTL -14d', 'ttl -fill|safetensors', 'models -llama',
  ]
  for (const root of ['', 'bk', 'bk/tmp', 'bk/iris', 'zz']) {
    it(`= matchRoots over every row, under '${root}'`, async () => {
      const h = await openIndex(env, SEARCH)
      const got: unknown[] = []
      const want: unknown[] = []
      for (const q of QUERIES) {
        const pred = parseQuery(q)!
        if (pred(root)) continue // the whole view matches: no search runs
        const f = await find(h, pred, root)
        got.push([q, f?.roots, f?.truncated])
        want.push([q, await brute(q, root), false])
      }
      expect(got).toEqual(want)
    })
  }

  it('the cold-footer copy (row-group metadata from the blob) answers the same', async () => {
    const [d1, pq] = await Promise.all([openIndex(env, SEARCH), openIndex(env, SEARCH_PQ)])
    expect(pq.mode).toBe('blob')
    for (const q of ['ttl', 'safetensors', 'gr']) {
      expect((await find(pq, parseQuery(q)!, ''))!.roots).toEqual((await find(d1, parseQuery(q)!, ''))!.roots)
    }
  })

  it('serves what it can, declines the rest (null = the view reads as before)', async () => {
    const h = await openIndex(env, SEARCH)
    // A term ending in `/`, the regex fallback, a query of only negatives.
    for (const q of ['ckpt/', '/ckpt.*final/', '-fill']) expect([q, await find(h, parseQuery(q)!, '')]).toEqual([q, null])
    // No sidecar for the generation.
    expect(await find(await openIndex(env, PLAIN), parseQuery('ttl')!, '')).toBeNull()
  })

  it('a short needle has no trigram: the names scan, heaviest names first', async () => {
    const got = (await find(await openIndex(env, SEARCH), parseQuery('gr')!, ''))!
    expect([got.roots, got.truncated, got.stats.mode]).toEqual([['bk/runs/grug'], false, 'scan'])
  })

  it('no candidate: no `path` group is read', async () => {
    const h = await openIndex(env, SEARCH)
    const before = GETS.length
    const got = (await find(h, parseQuery('qqqzzz')!, ''))!
    expect([got.roots, got.truncated, got.stats.candidates, got.stats.pathRgs]).toEqual([[], false, 0, 0])
    expect(GETS.slice(before).filter(g => g.key.endsWith('/path-index.parquet'))).toEqual([])
  })
})

describe('budgets: a cut search is flagged, heaviest names kept, roots still outermost', () => {
  it('`path` groups: the heaviest names’ groups first', async () => {
    const h = await openIndex(env, SEARCH)
    // `0598`: `f05980`..`f05989` (path group 2) and `f00598` (group 0). By
    // bytes: `f05987` (2 KiB), then `f00598` and `f05986` (1 KiB, by name) —
    // `f00598` needs a second group, so the search stops there. Every match
    // in the group it read comes back, lighter names' included.
    const got = (await find(h, parseQuery('0598')!, '', { ...SEARCH_LIMITS, pathRgs: 1 }))!
    expect([got.roots, got.truncated, got.stats.pathRgs, got.stats.lifted]).toEqual([Array.from({ length: 10 }, (_, i) => `bk/fill/f0598${i}`), true, 1, 0])
    expect((await find(h, parseQuery('0598')!, ''))!.roots).toEqual(await brute('0598', ''))
  })
  it('a cut ancestor is lifted: its rows come from one point lookup', async () => {
    const h = await openIndex(env, SEARCH)
    // `zz-TTL-b`, `zz-ttl-a` (path group 2) outweigh the bucket `zz` (group
    // 0): only group 2 is read, where `zz/…` rows match too — their outermost
    // match, the bucket, is fetched by itself.
    const got = (await find(h, parseQuery('zz')!, '', { ...SEARCH_LIMITS, pathRgs: 1 }))!
    expect([got.roots, got.truncated, got.stats.pathRgs, got.stats.lifted]).toEqual([await brute('zz', ''), true, 1, 1])
    expect(got.roots).toEqual(['bk/fill/zz-TTL-b', 'bk/fill/zz-ttl-a', 'zz'])
    expect(byPath(got.rows)).toEqual(byPath((await allRows(PLAIN)).filter(r => got.roots.includes(r.path))))
  })
  it('names groups', async () => {
    const h = await openIndex(env, SEARCH)
    const got = (await find(h, parseQuery('gr')!, '', { ...SEARCH_LIMITS, namesRgs: 1 }))!
    // `grug` is a heavy name (first names group); the rest are not read.
    expect([got.roots, got.truncated, got.stats.namesRgs]).toEqual([['bk/runs/grug'], true, 1])
  })
  it('an unselective trigram is no constraint: same roots, more names verified', async () => {
    const h = await openIndex(env, SEARCH)
    const got = (await find(h, parseQuery('safetensors')!, '', { ...SEARCH_LIMITS, triRgs: 0 }))!
    expect([got.roots, got.truncated, got.stats.mode]).toEqual([await brute('safetensors', ''), false, 'scan'])
  })
})

describe('the filter view', () => {
  const base = { w: 1280, h: 800, minArea: 12, atten: 1 }
  it('= the pre-index view when that read sees every row (threshold 0), but the roots come from the index', async () => {
    for (const q of ['ttl', 'safetensors', 'ckpt|swarm', 'ckpt final', 'model-*', '/model-\\d+/', '/^model-\\d+/']) {
      for (const path of ['', 'bk']) {
        const o = { ...base, path, threshold: 0, query: parseQuery(q)! }
        const [a, b] = await Promise.all([buildView(env, { ...o, date: SEARCH }), buildView(env, { ...o, date: PLAIN })])
        // The regex fallback never uses the index (and `^` anchors to the
        // full path: no bucket starts with `model-`).
        const tier = q.startsWith('/') ? b.tier.split('+')[0] : 'search'
        expect([q, path, a.tier.split('+')[0], { ...a, tier: '' }]).toEqual([q, path, tier, { ...b, tier: '' }])
      }
    }
  })

  it('a search cut before any root falls back to the pre-index read', async () => {
    const o = { ...base, path: '', threshold: 0, query: parseQuery('ttl')! }
    const [a, b] = await Promise.all([buildView(env, { ...o, date: SEARCH, searchLimits: { ...SEARCH_LIMITS, pathRgs: 0 } }), buildView(env, { ...o, date: PLAIN })])
    expect(a).toEqual(b)
    expect(a.tier).toBe('path+path')
  })

  it('finds a match below the pixel budget the pre-index read can’t see', async () => {
    const o = { ...base, path: '', query: parseQuery('notes')! }
    const [a, b] = await Promise.all([buildView(env, { ...o, date: SEARCH }), buildView(env, { ...o, date: PLAIN })])
    expect([a.matches, a.matched, a.tree.b]).toEqual([['bk/iris/notes.txt'], [{ path: 'bk/iris/notes.txt', b: 50, o: 1 }], 50])
    expect([b.matches, b.tree.b]).toEqual([[], 0])
  })

  it('`matched` is heaviest first', async () => {
    const v = await buildView(env, { ...base, date: SEARCH, path: '', query: parseQuery('safetensors|ttl=')! })
    expect(v.matched).toEqual([
      { path: 'bk/models/llama/model-00001-of-00002.safetensors', b: 8 * MiB, o: 1 },
      { path: 'bk/models/llama/model-00002-of-00002.safetensors', b: 8 * MiB, o: 1 },
      { path: 'bk/tmp/ttl=14d', b: 5 * MiB, o: 2 },
      { path: 'bk/tmp/ttl=7d', b: 2 * MiB, o: 1 },
      { path: 'bk/models/tiny.safetensors', b: 10, o: 1 },
    ])
  })

  it('a filtered diff uses it on both sides', async () => {
    // The same generation on both sides: one finds its roots by index, the
    // other by the pre-index read; nothing differs.
    const d = await buildDiff(env, { ...base, from: PLAIN, to: SEARCH, top: 100, path: '', threshold: 0, query: parseQuery('ttl')! })
    expect([d.rows, d.tier, d.total_a, d.total_b]).toEqual([[], 'search+path', 7352542, 7352542])
    expect(d.matched?.map(m => m.path)).toEqual(['bk/fill/zz-TTL-b', 'bk/fill/zz-ttl-a', 'bk/iris/TTL-misc', 'bk/tmp/ttl=14d', 'bk/tmp/ttl=7d', 'zz/Checkpoints/ttl'])
  })
})

/** A view tree as `[path, bytes]` rows, depth first (folds as `…/(other)`). */
function flat(n: { n: string; b: number; c?: unknown[] }, at = ''): [string, number][] {
  const p = at === '' ? '' : at
  const out: [string, number][] = [[p, n.b]]
  for (const c of (n.c ?? []) as { n: string; b: number; c?: unknown[] }[]) out.push(...flat(c, p ? `${p}/${c.n}` : c.n))
  return out
}

describe('NOT: excluded descendants leave their roots’ totals and the tree', () => {
  const base = { w: 1280, h: 800, minArea: 12, atten: 1, threshold: 0 }
  it('subtracts the outermost excluded paths: `tmp -ckpt`', async () => {
    const v = await buildView(env, { ...base, date: SEARCH, path: '', query: parseQuery('tmp -ckpt')! })
    // bk/tmp holds 10 MiB; its `run-a/ckpt` (4 MiB, one object) is excluded.
    expect([v.matches, v.matched, v.excluded, v.tree.b, v.tier]).toEqual([['bk/tmp'], [{ path: 'bk/tmp', b: 6 * MiB, o: 3 }], ['bk/tmp/ttl=14d/run-a/ckpt'], 6 * MiB, 'search+path'])
    expect(flat(v.tree)).toEqual([
      ['', 6 * MiB], ['bk', 6 * MiB], ['bk/tmp', 6 * MiB],
      ['bk/tmp/scratch', 3 * MiB], ['bk/tmp/scratch/q.bin', 3 * MiB],
      ['bk/tmp/ttl=7d', 2 * MiB], ['bk/tmp/ttl=7d/z.bin', 2 * MiB],
      ['bk/tmp/ttl=14d', MiB], ['bk/tmp/ttl=14d/run-a', MiB], ['bk/tmp/ttl=14d/run-a/y.bin', MiB],
    ])
  })

  it('only negatives: the view root less the outermost excluded paths', async () => {
    const v = await buildView(env, { ...base, date: SEARCH, path: 'bk', query: parseQuery('-fill -models')! })
    const all = await allRows(PLAIN)
    const size = (p: string) => all.filter(r => r.path === p).reduce((n, r) => n + r.size, 0)
    expect([v.matches, v.excluded, v.tree.b]).toEqual([['bk'], ['bk/fill', 'bk/models'], size('bk') - size('bk/fill') - size('bk/models')])
  })

  it('the index and the pre-index read agree (threshold 0)', async () => {
    const got: unknown[] = []
    const want: unknown[] = []
    for (const q of ['tmp -ckpt', 'ttl -14d', 'ttl -"ttl=14d"', '-fill', 'bk -fill -tmp', 'ckpt -final|safetensors -00002', '-x*bin', 'iris -inner*ttl']) {
      for (const path of ['', 'bk', 'bk/tmp']) {
        const o = { ...base, path, query: parseQuery(q)! }
        const [a, b] = await Promise.all([buildView(env, { ...o, date: SEARCH }), buildView(env, { ...o, date: PLAIN })])
        got.push([q, path, { ...a, tier: '' }])
        want.push([q, path, { ...b, tier: '' }])
      }
    }
    expect(got).toEqual(want)
  })

  it('an excluded path below the pixel budget is still subtracted — only the index finds it', async () => {
    const all = await allRows(PLAIN)
    const size = (p: string) => all.filter(r => r.path === p).reduce((n, r) => n + r.size, 0)
    const o = { w: 1280, h: 800, minArea: 12, atten: 1, path: '', query: parseQuery('bk -notes')! }
    const [a, b] = await Promise.all([buildView(env, { ...o, date: SEARCH }), buildView(env, { ...o, date: PLAIN })])
    expect([a.excluded, a.tree.b]).toEqual([['bk/iris/notes.txt'], size('bk') - 50])
    expect([b.excluded, b.tree.b]).toEqual([undefined, size('bk')])
  })

  it('a filtered diff subtracts on both sides', async () => {
    const d = await buildDiff(env, { ...base, from: PLAIN, to: SEARCH, top: 100, path: '', query: parseQuery('tmp -ckpt')! })
    expect([d.rows, d.total_a, d.total_b, d.matched]).toEqual([[], 6 * MiB, 6 * MiB, [{ path: 'bk/tmp', b: 6 * MiB, o: 3 }]])
  })
})
