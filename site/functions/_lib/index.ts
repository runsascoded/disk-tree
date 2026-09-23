/**
 * A scan's index tiers (`<dir>/path-index[-coarse<E>][-by-user].parquet`) as
 * a row-group-pruned range reader — shared by `/api/subtree` (pixel-budget
 * drill), `/api/diff`, `/api/series` and `/api/marks/totals` (exact keep /
 * sweep bytes per live mark).
 *
 * Each file is sorted (depth, path) (or (usr, depth, path)): the descendants
 * of P at each depth are one contiguous run, so any prefix query is a few
 * row-group selections on the footer stats plus ranged reads of just those
 * groups.
 *
 * The footer stats live in D1 (specs/path-agnostic-serving.md §2.1):
 * `index_schema` is the per-(date, variant) **pointer** — the generation
 * `gen` and bucket dir `dir` of the file set a run published — and
 * `index_row_groups` holds every group's stats + compact (~250 B) metadata,
 * keyed by that gen. Row-group selection is a SQL query scoped to the
 * handle's gen, and we fetch metadata only for the groups a query actually
 * reads, so the ~5 MB thrift footer is never parsed on a cold isolate. A run
 * never overwrites a file the pointer names: it lands a new generation and
 * flips the pointer last (specs/view-serving.md, "Index rewrite vs D1
 * footer"). A pointer whose row groups retention retired (`index-gc -r`)
 * opens the tier's group-manifest blob beside the parquet instead — the same
 * rows as one ~10 MB document, cached at the edge and per isolate — since
 * parsing the parquet footer itself exceeds the Worker's memory.
 */
import { S3Store } from '@rdub/file-tree/stores/s3'
import { parquetMetadataAsync, parquetReadObjects } from 'hyparquet'
import type { Env } from './auth.js'
import { shared } from './shared.js'

/** A leaf of the stored parquet schema (`index_schema.schema_json`). */
interface SchemaElement { type: string; name: string; repetition_type: string; converted_type?: string }

export const BUCKET = 'oa-gcs-usage-dvx'

export interface Row {
  path: string
  depth: number
  usr: string | null
  b: number
  o: number
  wts: number
  wb: number
  c2: number
  c3: number
  c4: number
  a: number | null
}

interface GroupSpan {
  rowStart: number
  rowEnd: number
  dMin: number
  dMax: number
  pMin: string
  pMax: string
  bMax: number
}

type FileSlice = { byteLength: number; slice: (s: number, e?: number) => Promise<ArrayBuffer> }

/** Per-request timing sink: `(phase, ms)` accumulates into a `Server-Timing`
 * header (`/api/subtree`, `/api/diff`), so DevTools shows where a cold view
 * went — D1 span queries, group-metadata fetch, range fetches, decode. Handles
 * are memoized across requests, so a trace rides on a per-request copy
 * (`withTrace`), never on the shared handle. */
export type Trace = (name: string, ms: number) => void
export const withTrace = <H extends IndexHandle>(h: H, trace?: Trace): H => (trace ? { ...h, trace } : h)
const now = () => performance.now()

// D1-backed: metadata comes from index_schema/index_row_groups per query.
interface D1Handle {
  mode: 'd1'
  file: FileSlice
  env: Env
  date: string
  variant: string
  /** The generation the schema row pointed at when this handle opened; every
   * row-group query is scoped to it, so a flip mid-handle is invisible. */
  gen: string
  schema: SchemaElement[]
  version: number
  trace?: Trace
  /** A coarse tier's absolute byte floor (every path with subtree bytes >= floor
   * is present); null for the floor-free tier. */
  floor: number | null
}

/** A user lens filter: `usr` column = `key`, applied on the by-user index
 * variant (the only sort besides path — ownership has no group facet). */
export type Lens = { key: string }
/** Blob-backed: the same stats + compact metadata D1 holds for a tier, as
 * one document beside its parquet (`<tier>.groups.json`, written by
 * `index-sync`), held in memory. What a scan whose row groups retention
 * retired from D1 (`index-gc -r`) opens: one ~10 MB fetch, cached at the
 * edge and per isolate, then the D1 handle's selection over an array. */
interface BlobHandle extends Omit<D1Handle, 'mode'> {
  mode: 'blob'
  groups: BlobGroup[]
}
interface BlobGroup extends Span {
  uMin: string | null
  uMax: string | null
  rgJson: string
}
export type IndexHandle = D1Handle | BlobHandle

export const num = (v: unknown): number => (typeof v === 'bigint' ? Number(v) : (v as number) ?? 0)
export const str = (v: unknown): string =>
  typeof v === 'string' ? v : v instanceof Uint8Array ? new TextDecoder().decode(v) : String(v ?? '')

/** The index-store creds: an r2/s3 deploy sets `STORE_*`; unset falls back to
 *  the GCS HMAC pair so gcs/cw are unchanged (specs/union-of-roots.md). */
export const storeCreds = (env: Env) => ({
  accessKeyId: env.STORE_ACCESS_KEY_ID ?? env.GCS_HMAC_KEY_ID,
  secretAccessKey: env.STORE_SECRET_ACCESS_KEY ?? env.GCS_HMAC_SECRET,
})

/** Whether the index store is configured — the readiness gate every
 *  index-serving endpoint checks before reading (replaces the direct
 *  `GCS_HMAC_*` check, so an r2 deploy passes on `STORE_*`). */
export const storeReady = (env: Env): boolean => {
  const { accessKeyId, secretAccessKey } = storeCreds(env)
  return !!(accessKeyId && secretAccessKey)
}

/** Where the index store lives: `STORE_ENDPOINT`/`STORE_BUCKET`/`STORE_REGION`
 *  point any S3-compatible store (R2: the account's S3 endpoint + `auto`);
 *  unset = the GCS defaults. One place, so every proxy resolves the same store. */
export const storeTarget = (env: Env) => ({
  endpoint: env.STORE_ENDPOINT ?? 'https://storage.googleapis.com',
  bucket: env.STORE_BUCKET ?? BUCKET,
  region: env.STORE_REGION ?? 'us-east1',
})

export function makeStore(env: Env) {
  return S3Store({
    ...storeTarget(env),
    prefixes: env.STORE_PREFIXES
      ? env.STORE_PREFIXES.split(',').map(s => s.trim()).filter(Boolean)
      : ['listing/', 'snapshots/'],
    ...storeCreds(env),
  })
}

/** Index variant → parquet key under the generation dir D1 points at
 * (`index_schema.dir`, e.g. `listing/<date>/index/<gen>`). Variants are
 * `<tier>[-<sort>]`: tier `''` (floor-free) or `coarse<E>`; sort `path`
 * (default) or `user`. Mirrors `INDEX_VARIANTS` in the dt-cloud CLI
 * (specs/view-serving.md §1). */
export function indexKey(dir: string, variant: string): string {
  // The age index is a standalone index (per-path created-day strata), not a
  // path-index tier, so it keeps its own base name (specs/age-index.md).
  if (variant === 'age') return `${dir}/age-index.parquet`
  // Phase B: one path-major pyramid tier per bin (`age-pyramid-<bin>`).
  const pm = /^age-pyramid-(\d+(?:min|h|d|mo|y))$/.exec(variant)
  if (pm) return `${dir}/age-pyramid-${pm[1]}.parquet`
  // The cross-scan over-time index — a standalone singleton, own base name
  // (specs/obs-axis-indexing.md Phase 1).
  if (variant === 'over-time') return `${dir}/over-time.parquet`
  const m = /^(?:(coarse\d+)(?:-(user))?|(path|user))$/.exec(variant)
  if (!m) throw new Error(`bad index variant '${variant}'`)
  const tier = m[1] ? `-${m[1]}` : ''
  const sort = m[2] ?? (m[3] === 'path' ? undefined : m[3])
  return `${dir}/path-index${tier}${sort ? `-by-${sort}` : ''}.parquet`
}

/** Where a scan's floor-free path index lives (the D1 pointer); null when
 * the scan was never synced. For the raw-parquet proxy and other readers
 * outside the D1 row-group path. */
export async function indexDir(env: Env, date: string, variant = 'path'): Promise<string | null> {
  if (!env.DB) return null
  const r = await env.DB.prepare('SELECT dir FROM index_schema WHERE date = ? AND variant = ?').bind(date, variant).first<{ dir: string | null }>()
  return r?.dir ?? null
}

function fileFor(env: Env, dir: string, variant: string): FileSlice {
  const store = makeStore(env)
  const key = indexKey(dir, variant)
  let size: Promise<number> | null = null
  const byteLengthP = () => (size ??= store.get(key, { offset: 0, length: 1 }).then(r => {
    if (!r.totalSize) throw new Error('index size unknown (no Content-Range)')
    return r.totalSize
  }))
  return {
    // byteLength is never needed (row-group offsets are absolute); hyparquet
    // still expects the field, so it stays a lazy zero.
    get byteLength() { return 0 },
    slice: async (s: number, e?: number) => {
      const end = e ?? (await byteLengthP())
      const r = await store.get(key, { offset: s, length: end - s })
      return r.bytes.buffer.slice(r.bytes.byteOffset, r.bytes.byteOffset + r.bytes.byteLength) as ArrayBuffer
    },
  }
}

// Per-isolate cache of the pointer read (one D1 row). A generation is
// immutable, but the pointer can flip (a REPROC), so entries expire: after
// HANDLE_TTL a handle re-reads the schema row and follows the new generation.
// `index-gc` runs at the end of the job, well past the TTL, so a live handle
// never outlives its generation's rows.
const HANDLE_TTL = 60_000
const handles = new Map<string, Promise<IndexHandle>>()
const handleAt = new Map<string, number>()

export async function openIndex(env: Env, date: string, variant = 'path'): Promise<IndexHandle> {
  const ck = `${date}:${variant}`
  const at = handleAt.get(ck)
  if (at == null || Date.now() - at >= HANDLE_TTL) {
    handles.delete(ck)
    handleAt.set(ck, Date.now())
  }
  return shared(handles, ck, async (): Promise<IndexHandle> => {
    if (!env.DB) throw new Error('index reader not configured (DB)')
    const s = await env.DB.prepare('SELECT version, schema_json, floor_bytes, gen, dir FROM index_schema WHERE date = ? AND variant = ?').bind(date, variant).first<{ version: number; schema_json: string; floor_bytes: number | null; gen: string | null; dir: string | null }>()
    if (!s || !s.gen || !s.dir) throw new Error(`index variant '${variant}' not synced for ${date}`)
    // A pointer whose row groups were retired (`index-gc -r`) still names
    // the generation dir: open the tier's group-manifest blob there instead.
    // (Parsing the parquet footer itself is not an option — a floor-free
    // tier's ~27k-group footer exceeds the Worker's memory.)
    const any = await env.DB.prepare('SELECT 1 AS x FROM index_row_groups WHERE date = ? AND variant = ? AND gen = ? LIMIT 1').bind(date, variant, s.gen).first<{ x: number }>()
    if (!any) return openBlob(env, date, variant, s.gen, s.dir)
    return { mode: 'd1', file: fileFor(env, s.dir, variant), env, date, variant, gen: s.gen, schema: JSON.parse(s.schema_json), version: s.version, floor: s.floor_bytes == null ? null : num(s.floor_bytes) }
  }, 30_000) // a blob open is a ~15 MB fetch + parse
}

/** The blob's on-disk shape (index_footer.py `groups_blob`): groups are
 * `index_row_groups` columns in order, `rg_json` as the string `reviveRowGroup`
 * takes. */
interface GroupsBlob {
  v: number
  version: number
  schema: SchemaElement[]
  floor_bytes: number | null
  groups: [number, number, number, string, string, number, string | null, string | null, number, number, string][]
}
const BLOB_CACHE_TTL = 86_400 // a generation dir is immutable

export const blobKey = (dir: string, variant: string): string => indexKey(dir, variant).replace(/\.parquet$/, '.groups.json')

async function openBlob(env: Env, date: string, variant: string, gen: string, dir: string): Promise<BlobHandle> {
  const key = blobKey(dir, variant)
  const cache = (caches as unknown as { default: Cache }).default
  const ck = new Request(`https://index-blob.cache/${key}`)
  let text: string
  const hit = await cache.match(ck)
  if (hit) text = await hit.text()
  else {
    let got: { bytes: Uint8Array }
    try {
      got = await makeStore(env).get(key)
    } catch (e) {
      // No rows and no blob: the tier is unreachable — the "not synced"
      // shape, which `tryOpen` folds and the floor-free open reports.
      throw new Error(`index variant '${variant}' not synced for ${date} (retired from D1; no ${key}: ${(e as Error).message})`)
    }
    text = new TextDecoder().decode(got.bytes)
    await cache.put(ck, new Response(text, { headers: { 'content-type': 'application/json', 'cache-control': `max-age=${BLOB_CACHE_TTL}` } }))
  }
  const blob = JSON.parse(text) as GroupsBlob
  if (blob.v !== 1) throw new Error(`${key}: unknown blob version ${blob.v}`)
  const groups: BlobGroup[] = blob.groups.map(([rg, dMin, dMax, pMin, pMax, bMax, uMin, uMax, rowStart, rowEnd, rgJson]) => ({ rg, dMin, dMax, pMin, pMax, bMax, uMin, uMax, rowStart, rowEnd, rgJson }))
  return { mode: 'blob', file: fileFor(env, dir, variant), env, date, variant, gen, schema: blob.schema, version: blob.version, floor: blob.floor_bytes == null ? null : num(blob.floor_bytes), groups }
}

// --- shared row shaping ------------------------------------------------------

const toRow = (r: Record<string, unknown>): Row => ({
  path: str(r.path),
  depth: num(r.depth),
  usr: r.usr == null ? null : str(r.usr),
  b: num(r.b),
  o: num(r.o),
  wts: num(r.wts),
  wb: num(r.wb),
  c2: num(r.c2),
  c3: num(r.c3),
  c4: num(r.c4),
  a: r.a == null ? null : num(r.a),
})

// --- D1 metadata: revive stored RowGroup JSON into hyparquet's shape ---------

/** The compact form `index-sync` stores (index_footer.py): `[num_rows, codec,
 * [[data_page_offset, total_compressed_size, dictionary_page_offset|0], …]]`,
 * one triple per leaf column in schema order — the only column-chunk fields
 * hyparquet reads (plus `type`/`path_in_schema`, taken from the schema). */
type CompactGroup = [number, string, [number, number, number][]]

export function reviveRowGroup(json: string, schema: SchemaElement[]): Record<string, unknown> {
  const [numRows, codec, cols] = JSON.parse(json) as CompactGroup
  const leaves = schema.slice(1) // [0] is the root element
  if (cols.length !== leaves.length) throw new Error(`row group has ${cols.length} columns, schema ${leaves.length}`)
  return {
    num_rows: BigInt(numRows),
    columns: cols.map(([dpo, size, dict], i) => ({
      meta_data: {
        type: leaves[i].type,
        path_in_schema: [leaves[i].name],
        codec,
        data_page_offset: BigInt(dpo),
        total_compressed_size: BigInt(size),
        ...(dict ? { dictionary_page_offset: BigInt(dict) } : {}),
      },
    })),
  }
}

/** Read one row group (given its stored metadata JSON) via a subset
 * FileMetaData, as raw column records (pre-`toRow`). The age index (variant
 * `age`) carries its own columns (`day,b,o`), not the shared `Row` shape. */
async function readGroupRaw(h: IndexHandle, rgJson: string, columns?: string[]): Promise<Record<string, unknown>[]> {
  const rg = reviveRowGroup(rgJson, h.schema)
  const metadata = { version: h.version, schema: h.schema, num_rows: rg.num_rows, row_groups: [rg], metadata_length: 0 } as unknown as Awaited<ReturnType<typeof parquetMetadataAsync>>
  const trace = h.trace
  const file: FileSlice = trace
    ? { byteLength: h.file.byteLength, slice: async (s, e) => { const t0 = now(); try { return await h.file.slice(s, e) } finally { trace('fetch', now() - t0) } } }
    : h.file
  const t0 = now()
  const rows = (await parquetReadObjects({ file, metadata, columns })) as Record<string, unknown>[]
  trace?.('group', now() - t0)
  return rows
}

/** Read one row group as shaped `Row`s. */
async function readGroup(h: IndexHandle, rgJson: string, columns?: string[]): Promise<Row[]> {
  return (await readGroupRaw(h, rgJson, columns)).map(toRow)
}

interface Span extends GroupSpan { rg: number }

/** Decoded row groups, per isolate (LRU by an estimated byte size).
 *
 * Decoding is the cold cost on the edge: ~100 ms of CPU per 8k-row group
 * (Workers Logs `cpuTime` 2026-09-15: a 25-group root subtree = 2.6 s CPU
 * against 0.3 s of I/O), and the same tier groups serve every view of a
 * scan — the root at each width, the bucket drills, both sides of every
 * diff. A generation dir is immutable, so `(date, variant, gen, rg)` is a
 * stable key. Rows are never mutated by readers (`classRow` copies). The cap
 * keeps well inside the isolate's 128 MB with concurrent requests. */
const GROUP_CACHE_CAP = 24 << 20
const ROW_BYTES = 160 // a shaped Row with a ~60-char path, roughly
const groupCache = new Map<string, Row[]>()
let groupCacheBytes = 0

async function readGroupCached(h: IndexHandle, rg: number, rgJson: string): Promise<Row[]> {
  const k = `${h.date}|${h.variant}|${h.gen}|${rg}`
  const hit = groupCache.get(k)
  if (hit) {
    groupCache.delete(k)
    groupCache.set(k, hit) // LRU: most recent last
    h.trace?.('gcache', 1)
    return hit
  }
  const rows = await readGroup(h, rgJson)
  const bytes = rows.length * ROW_BYTES
  if (bytes <= GROUP_CACHE_CAP) {
    while (groupCacheBytes + bytes > GROUP_CACHE_CAP && groupCache.size) {
      const [k0, v0] = groupCache.entries().next().value as [string, Row[]]
      groupCache.delete(k0)
      groupCacheBytes -= v0.length * ROW_BYTES
    }
    groupCache.set(k, rows)
    groupCacheBytes += bytes
  }
  return rows
}

/** Row groups in flight at once per read. Each group is its own range
 * fetch + decode; the fetch is ~200–240 ms of latency for ~280 KB, so the
 * reads are latency-bound, not bandwidth-bound: a root subtree's 24 groups
 * took three rounds at 8-wide (`Server-Timing` 2026-09-15: fetch 5.0 s
 * summed, 1.0 s wall) and a 7-day diff's 107 groups fourteen. 32 in flight
 * is ~9 MB of buffers at most — well inside the isolate's memory. */
const GROUP_READS = 32

/** `Promise.all(items.map(f))` with at most `limit` in flight; results in
 * input order. */
async function mapLimit<T, R>(items: T[], limit: number, f: (t: T) => Promise<R>): Promise<R[]> {
  const out: R[] = new Array(items.length)
  let next = 0
  const worker = async () => {
    for (;;) {
      const i = next++
      if (i >= items.length) return
      out[i] = await f(items[i])
    }
  }
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, worker))
  return out
}

/** The span query's predicate, for a blob handle's in-memory groups — the
 * same test `selectSpans` sends D1, SQL NULL semantics included (a group with
 * no usr stats never matches a lens). */
export function groupMatches(g: { dMin: number; dMax: number; pMin: string; pMax: string; bMax: number; uMin?: string | null; uMax?: string | null }, rects: Rect[], bMin = 0, lens?: Lens): boolean {
  if (bMin > 0 && g.bMax < Math.floor(bMin)) return false
  const rect = (r: Rect) => g.dMax >= r.dLo && g.dMin <= r.dHi && (g.dMin !== g.dMax || (g.pMax >= r.pLo && g.pMin <= r.pHi))
  if (!lens) return rects.some(rect)
  if (g.uMin == null || g.uMax == null || !(g.uMin <= lens.key && g.uMax >= lens.key)) return false
  return g.uMin !== g.uMax || rects.some(rect)
}

/** Candidate row groups for a set of (depth, path-range) rectangles — one SQL
 * pass (no rg_json yet), carrying each group's stats so the caller can apply a
 * finer per-ask test. A group spanning a depth boundary resets path order, so
 * the path test only applies within a single depth (`d_min = d_max`). */
async function selectSpans(h: IndexHandle, rects: Rect[], cap = 4000, bMin = 0, lens?: Lens): Promise<Span[]> {
  if (h.mode === 'blob') {
    const out = h.groups.filter(g => groupMatches(g, rects, bMin, lens))
    if (out.length > cap) throw new Error(`query too wide: >${cap} row groups (drill deeper or raise minArea)`)
    return out
  }
  const where: string[] = []
  const binds: unknown[] = []
  // The (depth, path) rect; valid within a single primary-key group only
  // (single-depth for the path index, single-user for the lens index).
  const rectSql = '(d_max >= ? AND d_min <= ? AND (d_min <> d_max OR (p_max >= ? AND p_min <= ?)))'
  for (const r of rects) {
    if (lens) {
      // Prune to groups whose usr range covers the lens key; the rect is a
      // secondary test that only holds inside a single-key group.
      where.push(`(u_min <= ? AND u_max >= ? AND (u_min <> u_max OR ${rectSql}))`)
      binds.push(lens.key, lens.key, r.dLo, r.dHi, r.pLo, r.pHi)
    } else {
      where.push(rectSql)
      binds.push(r.dLo, r.dHi, r.pLo, r.pHi)
    }
  }
  // Prune groups whose biggest row can't clear the shallowest threshold — the
  // footer path prunes by b_max during its scan, so without this a large
  // subtree returns far more candidate groups than it can draw and hits `cap`.
  const bFloor = bMin > 0 ? ' AND b_max >= ?' : ''
  const sql = `SELECT rg, d_min, d_max, p_min, p_max, b_max, row_start, row_end FROM index_row_groups WHERE date = ? AND variant = ? AND gen = ? AND (${where.join(' OR ')})${bFloor} ORDER BY rg LIMIT ${cap + 1}`
  if (bMin > 0) binds.push(Math.floor(bMin))
  const res = await h.env.DB!.prepare(sql).bind(h.date, h.variant, h.gen, ...binds).all<{ rg: number; d_min: number; d_max: number; p_min: string; p_max: string; b_max: number; row_start: number; row_end: number }>()
  if (res.results.length > cap) throw new Error(`query too wide: >${cap} row groups (drill deeper or raise minArea)`)
  return res.results.map(r => ({ rg: r.rg, dMin: num(r.d_min), dMax: num(r.d_max), pMin: r.p_min, pMax: r.p_max, bMax: num(r.b_max), rowStart: num(r.row_start), rowEnd: num(r.row_end) }))
}

/** Fetch stored metadata JSON for a set of row groups. */
async function fetchGroupJson(h: IndexHandle, rgs: number[]): Promise<Map<number, string>> {
  const out = new Map<number, string>()
  if (h.mode === 'blob') {
    const want = new Set(rgs)
    for (const g of h.groups) if (want.has(g.rg)) out.set(g.rg, g.rgJson)
    return out
  }
  for (let i = 0; i < rgs.length; i += 80) {
    const chunk = rgs.slice(i, i + 80)
    const sql = `SELECT rg, rg_json FROM index_row_groups WHERE date = ? AND variant = ? AND gen = ? AND rg IN (${chunk.map(() => '?').join(',')})`
    const res = await h.env.DB!.prepare(sql).bind(h.date, h.variant, h.gen, ...chunk).all<{ rg: number; rg_json: string }>()
    for (const r of res.results) out.set(r.rg, r.rg_json)
  }
  return out
}

// --- public read API ---------------------------------------------------------

/** Rows in the (depth, path) rectangle; `thrAt(depth)` prunes groups whose
 * biggest row can't clear the threshold. Same contract in both handle modes. */
export async function readRows(
  h: IndexHandle,
  dLo: number,
  dHi: number,
  pLo: string,
  pHi: string,
  thrAt?: (depth: number) => number,
  lens?: Lens,
): Promise<Row[]> {
  return readRects(h, [{ dLo, dHi, pLo, pHi }], thrAt, lens)
}

export interface Rect { dLo: number; dHi: number; pLo: string; pHi: string }

/** Rows in any of several (depth, path-range) rectangles — one read: the
 * candidate groups of all rects come from a few span queries (rects batched
 * per statement), each group is decoded once, and a row passes if some rect
 * holds it. What a lens's scattered claimed regions need. */
export async function readRects(
  h: IndexHandle,
  rects: Rect[],
  thrAt?: (depth: number) => number,
  lens?: Lens,
): Promise<Row[]> {
  if (!rects.length) return []
  // A row passes the lens iff its usr equals the key.
  const lensOk = (r: Row) => !lens || r.usr === lens.key
  const inRect = (r: Row) => rects.some(q => r.depth >= q.dLo && r.depth <= q.dHi && r.path >= q.pLo && r.path <= q.pHi)
  const dMin = Math.min(...rects.map(q => q.dLo))
  const RECTS_PER_QUERY = 20 // D1 binds: 4 per rect (+2 with a lens)
  const byRg = new Map<number, Span>()
  let t0 = now()
  for (let i = 0; i < rects.length; i += RECTS_PER_QUERY) {
    const spans = await selectSpans(h, rects.slice(i, i + RECTS_PER_QUERY), 4000, thrAt ? thrAt(dMin) : 0, lens)
    for (const s of spans) byRg.set(s.rg, s)
  }
  h.trace?.('spans', now() - t0)
  const kept = [...byRg.values()].sort((a, b) => a.rg - b.rg).filter(s => !thrAt || s.bMax >= thrAt(Math.max(s.dMin, dMin)))
  if (kept.length > 250) throw new Error('query too wide: drill deeper or raise minArea')
  // Bound the decode too, not just the group count — a broad lens (a big
  // user spread across the estate) can select few-enough groups but still
  // decode millions of rows and blow the Worker CPU. Error cleanly instead.
  const totalRows = kept.reduce((n, s) => n + (s.rowEnd - s.rowStart), 0)
  if (totalRows > 700_000) throw new Error('query too wide: drill deeper or raise minArea')
  t0 = now()
  const jsons = await fetchGroupJson(h, kept.map(s => s.rg))
  h.trace?.('rgjson', now() - t0)
  h.trace?.('ngroups', kept.length)
  t0 = now()
  const perGroup = await mapLimit(kept, GROUP_READS, async s => {
    const j = jsons.get(s.rg)
    if (!j) return []
    const out: Row[] = []
    for (const r of await readGroupCached(h, s.rg, j)) if (inRect(r) && lensOk(r)) out.push(r)
    return out
  })
  h.trace?.('groups', now() - t0)
  return perGroup.flat()
}

/** A point lookup `(depth, path)` or a one-level range under a prefix. */
export type Ask = { depth: number; path: string } | { depth: number; under: string }

const askRect = (a: Ask): Rect =>
  'path' in a
    ? { dLo: a.depth, dHi: a.depth, pLo: a.path, pHi: a.path }
    : { dLo: a.depth, dHi: a.depth, pLo: a.under + '/', pHi: a.under + '0' } // '0' sorts just past '/'

const groupMayHold = (g: Span, a: Ask): boolean => {
  if (g.dMax < a.depth || g.dMin > a.depth) return false
  if (g.dMin !== g.dMax) return true
  return 'path' in a ? !(g.pMax < a.path || g.pMin > a.path) : !(g.pMax < a.under + '/' || g.pMin > a.under + '0')
}

/** Point lookups: exact `(depth, path)` rows or `(depth, under-prefix)`
 * ranges, many at once (the totals manifest's ~8k prefixes) — `keep` is the
 * caller's exact test over the rows of the candidate groups. */
export async function readAsks(
  h: IndexHandle,
  asks: Ask[],
  keep: (r: Row) => boolean,
  { columns, maxGroups = 60 }: { columns?: string[]; maxGroups?: number } = {},
): Promise<{ rows: Row[]; groups: number }> {
  // Collapse asks to one rectangle per depth (min..max path) to keep the SQL
  // small; the exact ask set is enforced by `keep` after the read.
  const byDepth = new Map<number, { pLo: string; pHi: string }>()
  for (const a of asks) {
    const r = askRect(a)
    const cur = byDepth.get(a.depth)
    byDepth.set(a.depth, cur ? { pLo: cur.pLo < r.pLo ? cur.pLo : r.pLo, pHi: cur.pHi > r.pHi ? cur.pHi : r.pHi } : { pLo: r.pLo, pHi: r.pHi })
  }
  const rects = [...byDepth.entries()].map(([d, r]) => ({ dLo: d, dHi: d, pLo: r.pLo, pHi: r.pHi }))
  // A per-depth [min,max] rectangle over-selects the groups between the
  // lowest and highest ask; narrow to groups an actual ask falls in.
  let t0 = now()
  const cand = await selectSpans(h, rects)
  h.trace?.('spans', now() - t0)
  const spans = cand.filter(s => asks.some(a => groupMayHold(s, a)))
  if (spans.length > maxGroups) throw new Error(`lookup too wide: ${spans.length} row groups (cap ${maxGroups})`)
  t0 = now()
  const jsons = await fetchGroupJson(h, spans.map(s => s.rg))
  h.trace?.('rgjson', now() - t0)
  h.trace?.('ngroups', spans.length)
  t0 = now()
  const perGroup = await mapLimit(spans, GROUP_READS, async s => {
    const j = jsons.get(s.rg)
    if (!j) return []
    // a column subset (the totals manifest) bypasses the cache: cached rows are whole
    return (columns ? await readGroup(h, j, columns) : await readGroupCached(h, s.rg, j)).filter(keep)
  })
  h.trace?.('groups', now() - t0)
  return { rows: perGroup.flat(), groups: spans.length }
}

/** Raw rows (pre-`toRow` records) for a single `(depth, path)` point lookup —
 * the age index (variant `age`), whose columns (`day,b,o`) aren't the shared
 * `Row` shape but which sorts `(depth, path, day)` so a path's day rows are one
 * contiguous run. Same span-select + row-group-prune path as the readers above. */
export async function readPoint(h: IndexHandle, depth: number, path: string, columns?: string[]): Promise<Record<string, unknown>[]> {
  const ask: Ask = { depth, path }
  const cand = await selectSpans(h, [askRect(ask)])
  const spans = cand.filter(s => groupMayHold(s, ask))
  if (spans.length > 60) throw new Error(`age lookup too wide: ${spans.length} row groups`)
  const jsons = await fetchGroupJson(h, spans.map(s => s.rg))
  const perGroup = await mapLimit(spans, GROUP_READS, async s => {
    const j = jsons.get(s.rg)
    if (!j) return []
    return (await readGroupRaw(h, j, columns)).filter(r => str(r.path) === path)
  })
  return perGroup.flat()
}
