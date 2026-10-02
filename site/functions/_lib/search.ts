/**
 * The search index reader (specs/path-store-search.md §4): a filter query →
 * its match roots under a view root, from the generation's search sidecars
 * beside the `path` sort — `path-index.search.parquet` (the directory),
 * `.trigrams.parquet` (trigram → name-id postings), `.names.parquet` (the
 * vocabulary in impact order, each name's `path` row groups) — and then only
 * the `path` row groups those names live in. No D1 rows: every file is
 * range-read through the colo cache (a generation dir is immutable) and
 * decoded groups share the isolate's LRU with the tier reads.
 *
 * Exactness: the planner (`searchQuery.ts`) yields a superset of the names a
 * match root's last segment can have; every row read is tested with the
 * view's own predicate; each matching row is lifted to its shallowest
 * matching prefix. Unless a budget cut the search (`truncated`), every
 * root's rows were among those read — the roots equal `matchRoots` over
 * every row under the view root.
 */
import { type FileMetaData, parquetMetadata, parquetReadObjects, type RowGroup } from 'hyparquet'
import type { Env } from './auth.js'
import { cacheGet, cachePut, cachedRange, type FileSlice, GROUP_READS, type IndexHandle, mapLimit, num, readAsks, readFooterBytes, readGroupsAt, reviveRowGroup, type Row, str } from './index.js'
import type { NamePred } from './scope.js'
import { type Formula, or, type SearchPlan } from './searchQuery.js'
import { storeKey } from './stores.js'
import { compressors } from './zstd.js'

/** The sidecars' names beside the `path` sort (`disk_tree.find.search`). */
export const SEARCH_FILES = {
  names: 'path-index.names.parquet',
  trigrams: 'path-index.trigrams.parquet',
  search: 'path-index.search.parquet',
} as const
export const searchKey = (dir: string, role: keyof typeof SEARCH_FILES): string => `${dir}/${SEARCH_FILES[role]}`

/** Per-request read budgets (spec §4.2–4.3). */
export interface SearchLimits {
  /** Postings row groups a trigram may span and still be read; wider is
   * unselective and treated as no constraint (sound: only widens). */
  triRgs: number
  /** Names row groups read to verify candidates (heaviest ids first). */
  namesRgs: number
  /** `path`-sort row groups read for the verified names' rows. */
  pathRgs: number
  /** Row groups the lifted-root point lookup may touch (truncated only). */
  liftGroups: number
}
export const SEARCH_LIMITS: SearchLimits = { triRgs: 8, namesRgs: 32, pathRgs: 32, liftGroups: 60 }

interface SchemaElement { type: string; name: string; repetition_type: string; converted_type?: string }

/** One directory row: a row group of the names (`file` 0) or trigrams (1) file. */
interface DirRow { file: number; rg: number; rowStart: number; rowEnd: number; kMin: number; kMax: number; rgJson: string }

/** One row group of the directory itself, with the bounds its stats give. */
interface DirGroup {
  n: number
  byteStart: number
  byteEnd: number
  meta: RowGroup
  bounds: { fileMin: number; fileMax: number; rgMin: number; rgMax: number; kLo: number; kHi: number } | null
}

interface SearchIndex {
  key: string
  dir: string
  metadata: FileMetaData
  groups: DirGroup[]
  names: SchemaElement[]
  trigrams: SchemaElement[]
}

const DIR_COLS = ['file', 'rg', 'row_start', 'row_end', 'k_min', 'k_max', 'rg_json']
const MISS_TTL = 60_000
const opened = new Map<string, { at: number; idx: Promise<SearchIndex | null> }>()

/** A generation's search directory, or null when it has none (a generation
 * written without `-S`, every v1 index). Remembered per isolate (a miss for
 * `MISS_TTL`). */
export function openSearch(env: Env, dir: string): Promise<SearchIndex | null> {
  const key = searchKey(dir, 'search')
  const ck = `${storeKey(env)}|${key}`
  const hit = opened.get(ck)
  if (hit && Date.now() - hit.at < MISS_TTL) return hit.idx
  const idx = (async (): Promise<SearchIndex | null> => {
    let buf: ArrayBuffer
    try {
      buf = await readFooterBytes(env, key)
    } catch (e) {
      if ((e as Error).name === 'NotFoundError') return null
      throw e
    }
    const metadata = parquetMetadata(buf)
    const kv = new Map((metadata.key_value_metadata ?? []).map(e => [e.key, e.value]))
    if (kv.get('search_v') !== '1') throw new Error(`${key}: not a v1 search directory (search_v=${kv.get('search_v')})`)
    const groups = metadata.row_groups.map((meta, n): DirGroup => {
      const cols = new Map(meta.columns.map(c => [c.meta_data!.path_in_schema[0], c.meta_data!]))
      let byteStart = Infinity
      let byteEnd = 0
      for (const c of cols.values()) {
        const data = Number(c.data_page_offset)
        const dict = c.dictionary_page_offset == null ? data : Number(c.dictionary_page_offset)
        const start = dict > 0 ? Math.min(dict, data) : data
        byteStart = Math.min(byteStart, start)
        byteEnd = Math.max(byteEnd, start + Number(c.total_compressed_size))
      }
      const st = (col: string, side: 'min' | 'max') => { const s = cols.get(col)?.statistics; return side === 'min' ? (s?.min_value ?? s?.min) : (s?.max_value ?? s?.max) }
      const b = [st('file', 'min'), st('file', 'max'), st('rg', 'min'), st('rg', 'max'), st('k_min', 'min'), st('k_max', 'max')]
      const bounds = b.some(v => v == null) ? null : { fileMin: num(b[0]), fileMax: num(b[1]), rgMin: num(b[2]), rgMax: num(b[3]), kLo: num(b[4]), kHi: num(b[5]) }
      return { n, byteStart, byteEnd, meta, bounds }
    })
    return {
      key, dir, metadata, groups,
      names: JSON.parse(kv.get('names_schema') ?? '[]') as SchemaElement[],
      trigrams: JSON.parse(kv.get('trigrams_schema') ?? '[]') as SchemaElement[],
    }
  })()
  opened.set(ck, { at: Date.now(), idx })
  idx.catch(() => opened.delete(ck))
  return idx
}

/** Decode one directory group (cached per isolate). */
async function readDirGroup(env: Env, idx: SearchIndex, g: DirGroup): Promise<DirRow[]> {
  const k = `${storeKey(env)}|${idx.key}|d${g.n}`
  const hit = cacheGet<DirRow>(k)
  if (hit) return hit
  const buf = await cachedRange(env, idx.key, g.byteStart, g.byteEnd)
  const file: FileSlice = { byteLength: g.byteEnd, slice: async (s, e) => buf.slice(s - g.byteStart, (e ?? g.byteEnd) - g.byteStart) }
  const metadata = { ...idx.metadata, row_groups: [g.meta], num_rows: g.meta.num_rows }
  const rows = (await parquetReadObjects({ file, metadata, columns: DIR_COLS, compressors })) as Record<string, unknown>[]
  const out = rows.map((r): DirRow => ({ file: num(r.file), rg: num(r.rg), rowStart: num(r.row_start), rowEnd: num(r.row_end), kMin: num(r.k_min), kMax: num(r.k_max), rgJson: str(r.rg_json) }))
  cachePut(k, out, out.reduce((n, r) => n + 64 + 2 * r.rgJson.length, 0))
  return out
}

/** Directory rows `row` accepts, from the groups whose bounds `group` admits
 * (a group without stats always), in directory order; `stop` ends the scan
 * once enough rows are in (groups are read 8 at a time). */
async function dirRows(
  env: Env,
  idx: SearchIndex,
  group: (b: NonNullable<DirGroup['bounds']>) => boolean,
  row: (r: DirRow) => boolean,
  stop: (got: DirRow[]) => boolean = () => false,
): Promise<{ rows: DirRow[]; groups: number }> {
  const sel = idx.groups.filter(g => !g.bounds || group(g.bounds))
  const out: DirRow[] = []
  let read = 0
  for (let i = 0; i < sel.length && !stop(out); i += 8) {
    const batch = sel.slice(i, i + 8)
    read += batch.length
    for (const rs of await mapLimit(batch, GROUP_READS, g => readDirGroup(env, idx, g))) for (const r of rs) if (row(r)) out.push(r)
  }
  return { rows: out, groups: read }
}

/** Decode one data row group (names or trigrams) from its directory row:
 * the byte range its revived `RowGroup` spans, colo-cached. */
async function readData(env: Env, idx: SearchIndex, role: 'names' | 'trigrams', d: DirRow, columns: string[]): Promise<Record<string, unknown>[]> {
  const schema = role === 'names' ? idx.names : idx.trigrams
  const rg = reviveRowGroup(d.rgJson, schema) as unknown as RowGroup
  let start = Infinity
  let end = 0
  for (const c of rg.columns) {
    const m = c.meta_data!
    const data = Number(m.data_page_offset)
    const dict = m.dictionary_page_offset == null ? data : Number(m.dictionary_page_offset)
    const s = dict > 0 ? Math.min(dict, data) : data
    start = Math.min(start, s)
    end = Math.max(end, s + Number(m.total_compressed_size))
  }
  const key = searchKey(idx.dir, role)
  const buf = await cachedRange(env, key, start, end)
  const file: FileSlice = { byteLength: end, slice: async (s, e) => buf.slice(s - start, (e ?? end) - start) }
  const metadata = { version: 1, schema, num_rows: rg.num_rows, row_groups: [rg], metadata_length: 0 } as unknown as FileMetaData
  return (await parquetReadObjects({ file, metadata, columns, compressors })) as Record<string, unknown>[]
}

/** One postings group as parallel arrays (cached). */
async function readPostings(env: Env, idx: SearchIndex, d: DirRow): Promise<{ tri: Int32Array; id: Int32Array }> {
  const k = `${storeKey(env)}|${idx.key}|t${d.rg}`
  const hit = cacheGet<{ tri: Int32Array; id: Int32Array }>(k)
  if (hit) return hit[0]
  const rows = await readData(env, idx, 'trigrams', d, ['tri', 'id'])
  const tri = Int32Array.from(rows, r => num(r.tri))
  const id = Int32Array.from(rows, r => num(r.id))
  cachePut(k, [{ tri, id }], 8 * rows.length + 64)
  return { tri, id }
}

interface NameRow { id: number; name: string; nRgs: number; rgs: string | null }

/** One names group (cached). */
async function readNames(env: Env, idx: SearchIndex, d: DirRow): Promise<NameRow[]> {
  const k = `${storeKey(env)}|${idx.key}|n${d.rg}`
  const hit = cacheGet<NameRow>(k)
  if (hit) return hit
  const rows = (await readData(env, idx, 'names', d, ['id', 'name', 'n_rgs', 'rgs'])).map((r): NameRow => ({ id: num(r.id), name: str(r.name), nRgs: num(r.n_rgs), rgs: r.rgs == null ? null : str(r.rgs) }))
  cachePut(k, rows, rows.reduce((n, r) => n + 48 + 2 * (r.name.length + (r.rgs?.length ?? 0)), 0))
  return rows
}

/** The trigrams a formula mentions. */
function trisOf(f: Formula, out = new Set<number>()): Set<number> {
  if (f === true) return out
  if (typeof f === 'number') out.add(f)
  else for (const x of 'and' in f ? f.and : f.or) trisOf(x, out)
  return out
}

/** The formula with every trigram `keep` rejects relaxed to `true`. */
function relax(f: Formula, keep: (t: number) => boolean): Formula {
  if (f === true) return true
  if (typeof f === 'number') return keep(f) ? f : true
  const xs = ('and' in f ? f.and : f.or).map(x => relax(x, keep))
  if ('and' in f) {
    const ys = xs.filter(x => x !== true)
    return ys.length === 0 ? true : ys.length === 1 ? ys[0] : { and: ys }
  }
  return or(xs)
}

/** Ids satisfying a (non-`true`) formula, from each trigram's sorted ids. */
function evaluate(f: Formula, lists: Map<number, number[]>): Set<number> {
  if (f === true) throw new Error('evaluate: an unconstrained formula has no finite id set')
  if (typeof f === 'number') return new Set(lists.get(f) ?? [])
  const sets = ('and' in f ? f.and : f.or).map(x => evaluate(x, lists))
  if ('or' in f) {
    const u = new Set<number>()
    for (const s of sets) for (const x of s) u.add(x)
    return u
  }
  sets.sort((a, b) => a.size - b.size)
  let acc = sets[0]
  for (const s of sets.slice(1)) acc = new Set([...acc].filter(x => s.has(x)))
  return acc
}

/** The first index in sorted `xs` with `xs[i] >= v`. */
function lowerBound(xs: number[], v: number): number {
  let lo = 0
  let hi = xs.length
  while (lo < hi) {
    const mid = (lo + hi) >> 1
    if (xs[mid] < v) lo = mid + 1
    else hi = mid
  }
  return lo
}
const anyIn = (xs: number[], lo: number, hi: number): boolean => { const i = lowerBound(xs, lo); return i < xs.length && xs[i] <= hi }

export interface SearchFound {
  /** The match roots' own rows (every owner slice), lifted ones included. */
  rows: Row[]
  /** The outermost matches strictly under the view root, sorted. */
  roots: string[]
  /** A budget cut the search: the roots are those of the heaviest names. */
  truncated: boolean
  stats: {
    /** `trigrams`: candidates from postings; `scan`: the names in id order. */
    mode: 'trigrams' | 'scan'
    trigrams: number
    postingsRgs: number
    candidates: number
    dirGroups: number
    namesRgs: number
    names: number
    pathRgs: number
    lifted: number
  }
}

/** The outermost paths strictly under `root` that pass `query`, on a store
 * generation's `path` sort `h`, given `plan` — the planner's candidate names
 * for the last segment of every such path (`planPositive` for match roots,
 * `planNegative` for excluded paths). Null when the index can't answer (no
 * sidecar for the generation): the caller reads as before. `query` must be
 * an interval on every root-to-leaf chain (false, then true, then false —
 * `pos ∧ ¬neg`), so a matching path's outermost match is its shallowest
 * matching prefix. */
export async function searchRoots(env: Env, h: IndexHandle, query: NamePred, plan: SearchPlan, root: string, limits: SearchLimits = SEARCH_LIMITS): Promise<SearchFound | null> {
  const idx = await openSearch(env, h.dir)
  if (!idx) return null
  let dirGroups = 0

  // Candidates: the postings of the selective trigrams, per the formulas.
  const tris = [...new Set(plan.branches.flatMap(b => [...trisOf(b.formula)]))].sort((a, b) => a - b)
  const triDir = new Map<number, DirRow[]>()
  if (tris.length) {
    const got = await dirRows(env, idx,
      b => b.fileMin <= 1 && b.fileMax >= 1 && anyIn(tris, b.kLo, b.kHi),
      r => r.file === 1 && anyIn(tris, r.kMin, r.kMax))
    dirGroups += got.groups
    for (const t of tris) triDir.set(t, got.rows.filter(r => r.kMin <= t && t <= r.kMax))
  }
  const formula = or(plan.branches.map(b => relax(b.formula, t => triDir.get(t)!.length <= limits.triRgs)))
  let candidates: number[] | null = null // null = the names scan
  let postingsRgs = 0
  if (formula !== true) {
    const used = trisOf(formula)
    const groups = new Map<number, DirRow>()
    for (const t of used) for (const r of triDir.get(t)!) groups.set(r.rg, r)
    postingsRgs = groups.size
    const lists = new Map<number, number[]>([...used].map(t => [t, []]))
    for (const p of await mapLimit([...groups.values()], GROUP_READS, d => readPostings(env, idx, d))) {
      for (let i = 0; i < p.tri.length; i++) lists.get(p.tri[i])?.push(p.id[i])
    }
    candidates = [...evaluate(formula, lists)].sort((a, b) => a - b)
  }

  // Verification: the candidates' names groups, heaviest ids first.
  let truncated = false
  let nameGroups: DirRow[]
  if (candidates === null) {
    const got = await dirRows(env, idx,
      b => b.fileMin <= 0 && b.fileMax >= 0 && b.rgMin <= limits.namesRgs,
      r => r.file === 0 && r.rg <= limits.namesRgs,
      rs => rs.length > limits.namesRgs)
    dirGroups += got.groups
    nameGroups = got.rows.sort((a, b) => a.rg - b.rg)
  } else {
    const cs = candidates
    const got = cs.length
      ? await dirRows(env, idx,
        b => b.fileMin <= 0 && b.fileMax >= 0 && anyIn(cs, b.kLo, b.kHi),
        r => r.file === 0 && anyIn(cs, r.kMin, r.kMax),
        rs => rs.length > limits.namesRgs)
      : { rows: [], groups: 0 }
    dirGroups += got.groups
    nameGroups = got.rows.sort((a, b) => a.rg - b.rg)
  }
  if (nameGroups.length > limits.namesRgs) {
    truncated = true
    nameGroups = nameGroups.slice(0, limits.namesRgs)
  }
  const want = candidates && new Set(candidates)
  const names: NameRow[] = []
  for (const rs of await mapLimit(nameGroups, GROUP_READS, d => readNames(env, idx, d))) {
    for (const r of rs) if ((!want || want.has(r.id)) && plan.branches.some(b => b.test(r.name))) names.push(r)
  }
  names.sort((a, b) => a.id - b.id)

  // The verified names' `path` groups, heaviest first, within the budget.
  const pathRgs = new Set<number>()
  for (const n of names) {
    if (n.rgs == null) { truncated = true; break }
    const add = n.rgs.split(',').map(Number).filter(rg => !pathRgs.has(rg))
    if (pathRgs.size + add.length > limits.pathRgs) { truncated = true; break }
    for (const rg of add) pathRgs.add(rg)
  }
  const under = root === '' ? () => true : (p: string) => p.startsWith(root + '/')
  const read = await readGroupsAt(h, [...pathRgs].sort((a, b) => a - b), r => under(r.path) && query(r.path))

  // Roots: each matching row's shallowest matching prefix below the root.
  const dRoot = root === '' ? 0 : root.split('/').length
  const outermost = (p: string): string => {
    const segs = p.split('/')
    for (let k = dRoot + 1; k < segs.length; k++) {
      const q = segs.slice(0, k).join('/')
      if (query(q)) return q
    }
    return p
  }
  const roots = new Set<string>()
  for (const r of read) roots.add(outermost(r.path))
  const rows = read.filter(r => roots.has(r.path))
  const have = new Set(rows.map(r => r.path))
  const missing = [...roots].filter(p => !have.has(p))
  let lifted = 0
  if (missing.length) {
    // Only reachable when truncated (a root's name was cut) — or if the
    // DuckDB/JS lowercase tables ever disagree on a name; either way the
    // root is real and its rows are one point lookup away.
    const miss = new Set(missing)
    try {
      const got = await readAsks(h, missing.map(p => ({ depth: p.split('/').length, path: p })), r => miss.has(r.path), { maxGroups: limits.liftGroups })
      rows.push(...got.rows)
      for (const r of got.rows) if (miss.delete(r.path)) lifted++
    } catch (e) {
      if (!String((e as Error).message).startsWith('lookup too wide')) throw e
    }
    if (miss.size) {
      truncated = true
      for (const p of miss) roots.delete(p)
    }
  }
  return {
    rows,
    roots: [...roots].sort(),
    truncated,
    stats: { mode: candidates === null ? 'scan' : 'trigrams', trigrams: tris.length, postingsRgs, candidates: candidates?.length ?? 0, dirGroups, namesRgs: nameGroups.length, names: names.length, pathRgs: pathRgs.size, lifted },
  }
}
