/** Bytes under a path per scan — the size-over-time chart, scoped like the
 * map (specs/view-serving.md §1 "series.json" → this).
 *
 *   GET /api/series?path=<P>[&lens=user:<id>][&o=claimed|unclaimed]
 *
 * One point per scan the index knows: P's own row in that scan's coarsest
 * tier that holds it (or the floor-free tier), scoped per row like a view's
 * root. Nothing is precomputed per prefix and nothing is floored: a path
 * below every tier falls through to the floor-free tier. Scans predating the
 * index (or the scope's variant) have no per-prefix point — but for the
 * unscoped whole-bucket series (empty path, no lens / owner / class filter)
 * a scan's `meta.json` total is as good as an index row, so those scans are
 * filled in from the snapshot dir: the chart keeps any history that never
 * got (or lost) its tiers, rather than showing a gap.
 */
import { type Ctx, json, requireScope, requireViewer } from '../_lib/auth.js'
import { snapshotsPrefix } from '../_lib/shared.js'
import { type Lens, makeStore } from '../_lib/index.js'
import { ledgerHead } from '../_lib/ledger.js'
import { classKey, parseClasses, parseOwner } from '../_lib/scope.js'
import { readRootAgg, readRootRows } from '../_lib/view.js'
import { parsePaths } from '../_lib/filter.js'
import { metaRoots, rootPoints, type RootRow } from '../_lib/series.js'

// The default store's snapshot dirs (`snapshots/<date>/`; other stores live in
// a named subdir that DATE_RE keeps out), and the scan-id shape they're named by.
const DATE_RE = /^\d{4}-\d{2}-\d{2}(T\d{4})?$/

/** Scans present as snapshot dirs but absent from the index (oldest first). */
async function unindexedScans(env: Ctx['env'], indexed: Set<string>): Promise<string[]> {
  const store = makeStore(env)
  const out: string[] = []
  let cursor: string | undefined
  do {
    const page = await store.list(snapshotsPrefix(env), { cursor })
    for (const e of page.entries) {
      const d = e.key.slice(snapshotsPrefix(env).length).replace(/\/$/, '')
      if (e.isDir && DATE_RE.test(d) && !indexed.has(d)) out.push(d)
    }
    cursor = page.cursor
  } while (cursor)
  return out.sort()
}

type Meta = { total_bytes?: number; total_objects?: number; buckets?: Record<string, { total_bytes: number; total_objects: number }> }

async function readMeta(env: Ctx['env'], date: string): Promise<Meta | null> {
  try {
    const { bytes } = await makeStore(env).get(`${snapshotsPrefix(env)}${date}/meta.json`)
    return JSON.parse(new TextDecoder().decode(bytes)) as Meta
  } catch (e) {
    console.log(`series /: no meta.json point for ${date}: ${(e as Error).message}`)
    return null
  }
}

/** A whole-bucket point from a scan's `meta.json` (no tiers needed). */
async function metaPoint(env: Ctx['env'], date: string): Promise<{ date: string; b: number; o: number } | null> {
  const m = await readMeta(env, date)
  return m && typeof m.total_bytes === 'number' ? { date, b: m.total_bytes, o: m.total_objects ?? 0 } : null
}

export const onRequestGet = async (ctx: Ctx): Promise<Response> => {
  const { env, request } = ctx
  if (!env.DB) return json({ error: 'index backend not configured (DB)' }, 503)
  if (!env.GCS_HMAC_KEY_ID || !env.GCS_HMAC_SECRET) return json({ error: 'index reader not configured' }, 503)
  const gated = await requireViewer(ctx)
  if (gated instanceof Response) return gated
  const url = new URL(request.url)
  const path = (url.searchParams.get('path') ?? '').replace(/\/+$/, '')
  if (path.includes('..') || path.startsWith('/')) return json({ error: 'bad path' }, 400)
  // `paths=` (specs/filter-views.md §4): the filter's match roots — the point
  // is the sum of one root read per scan. Bounded like a view's region reads.
  const paths = parsePaths(url.searchParams.getAll('paths'))
  if (paths.some(p => p.includes('..') || p.startsWith('/'))) return json({ error: 'bad paths' }, 400)
  if (paths.length > 24) return json({ error: 'too many paths (max 24)' }, 400)
  const lensRaw = url.searchParams.get('lens')
  let lens: Lens | undefined
  if (lensRaw) {
    const m = /^user:(.+)$/.exec(lensRaw)
    if (!m) return json({ error: 'bad lens (want user:<id>)' }, 400)
    lens = { key: m[1] }
  }
  const owner = parseOwner(url.searchParams.get('o'))
  const classes = parseClasses(url.searchParams.get('cl'))
  // `split=roots` (specs/root-geneses.md §1): the unscoped store root only —
  // one trace per depth-1 row (bucket) beside the total.
  const split = url.searchParams.get('split')
  if (split && split !== 'roots') return json({ error: 'bad split (want roots)' }, 400)
  if (split && (path || paths.length || lens || owner || classes)) return json({ error: 'split=roots is for the unscoped store root only' }, 400)

  // Every scan with a synced floor-free index, oldest first.
  const rows = await env.DB.prepare("SELECT DISTINCT date FROM index_schema WHERE variant = 'path' ORDER BY date").all<{ date: string }>()
  const dates = rows.results.map(r => r.date)
  // A user lens applies the live claims, so its key carries the ledger head.
  const head = lens ? await ledgerHead(env) : 0
  // Unscoped whole-bucket series only: scans without tiers still have a total in meta.json.
  const extra = path === '' && !paths.length && !lens && !owner && !classes ? await unindexedScans(env, new Set(dates)) : []
  const cacheKey = new Request(`https://series.cache/${encodeURIComponent(path)}?P=${encodeURIComponent(paths.join(','))}&l=${lensRaw ?? ''}&o=${owner ?? ''}&cl=${classKey(classes)}&s=${split ?? ''}&d=${dates.join(',')}&x=${extra.join(',')}&head=${head}`)
  const cache = (caches as unknown as { default: Cache }).default
  const hit = await cache.match(cacheKey)
  if (hit) return hit

  const points: { date: string; b: number; o: number }[] = []
  // split=roots: each scan's depth-1 rows (the total is their sum), and for
  // tier-less scans whatever meta.json says about its roots.
  const rootsByDate = new Map<string, RootRow[]>()
  // One scan's point; a D1 hiccup ("internal error") gets one more try, and
  // anything else unreadable is a missing point, not a failed chart — logged,
  // since a silently absent point looks like a gap in the data.
  const point = async (date: string, tries = 2): Promise<{ date: string; b: number; o: number } | null> => {
    try {
      if (split) {
        const rows = await readRootRows(env, date)
        if (!rows) return null
        rootsByDate.set(date, rows)
        return { date, b: rows.reduce((n, r) => n + r.b, 0), o: rows.reduce((n, r) => n + r.o, 0) }
      }
      if (paths.length) {
        // Σ over the match roots; a root absent from a scan contributes 0.
        const parts = await Promise.all(paths.map(p => readRootAgg(env, { date, path: p, lens, owner, classes })))
        const b = parts.reduce((n, a) => n + (a?.b ?? 0), 0)
        const o = parts.reduce((n, a) => n + (a?.o ?? 0), 0)
        return parts.some(Boolean) ? { date, b, o } : null
      }
      const a = await readRootAgg(env, { date, path, lens, owner, classes })
      return a ? { date, ...a } : null
    } catch (e) {
      const msg = (e as Error).message
      if (tries > 1 && /internal error/i.test(msg)) return point(date, tries - 1)
      console.log(`series ${path || '/'} ${lensRaw ?? owner ?? ''}: no point for ${date}: ${msg}`)
      return null
    }
  }
  // Scans are independent reads (each a few D1 round trips and one range
  // GET); a dozen at a time keeps a 40-scan history to ~4 rounds.
  for (let i = 0; i < dates.length; i += 12) {
    const got = await Promise.all(dates.slice(i, i + 12).map(d => point(d)))
    for (const g of got) if (g) points.push(g)
  }
  // A single-bucket store's tier-less history belongs to its sole root: the
  // earliest indexed scan having exactly one root says which.
  const first = [...rootsByDate.keys()].sort()[0]
  const soleRoot = first && rootsByDate.get(first)!.length === 1 ? rootsByDate.get(first)![0].path : null
  for (let i = 0; i < extra.length; i += 12) {
    const got = await Promise.all(extra.slice(i, i + 12).map(async d => {
      if (!split) return metaPoint(env, d)
      const m = await readMeta(env, d)
      if (!m || typeof m.total_bytes !== 'number') return null
      const roots = metaRoots(m, soleRoot)
      if (roots.length) rootsByDate.set(d, roots)
      return { date: d, b: m.total_bytes, o: m.total_objects ?? 0 }
    }))
    for (const g of got) if (g) points.push(g)
  }
  points.sort((a, b) => a.date.localeCompare(b.date))
  const res = json({ path, ...(paths.length ? { paths } : {}), ...(lensRaw ? { lens: lensRaw } : {}), ...(owner ? { owner } : {}), points, ...(split ? { roots: rootPoints(rootsByDate) } : {}) }, 200, { 'cache-control': 'private, max-age=300' })
  await cache.put(cacheKey, res.clone())
  return res
}
