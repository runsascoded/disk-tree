import type { Env } from '../../cfn/env'
import { scansPrefix } from '../../cfn/env'
import { error, json, normUri } from '../../cfn/http'
import { findCovering, getScans } from '../../cfn/manifests'
import type { Scan } from '../../cfn/manifests'
import { r2Buffer, readChunkPointers, readRows } from '../../cfn/parquet'
import type { TreeRow } from '../../cfn/parquet'

const DEFAULT_DEPTH = 2
const DEFAULT_MAX_ROWS = 2000  // keep in sync with `server.DEFAULT_MAX_ROWS` / `ui/src/api.ts`

/** A row as `/api/scan` returns it: paths relative to the requested uri. */
type ApiRow = Omit<TreeRow, 'parent'> & { parent: string | null; uri: string; scanned?: boolean; scan_time?: string }

const rowUri = (scan: Scan, path: string): string =>
  path === '.' ? scan.path : scan.path === '/' ? `/${path}` : `${scan.path}/${path}`

const depthOf = (rel: string): number => (rel === '.' || rel === '' ? 0 : rel.split('/').length)

interface Resolved {
  /** The blob that actually holds `rel` — a chunk blob if one was crossed. */
  blob: string
  /** `rel` rebased into that blob's coordinates (`.` at the chunk root). */
  rebased: string
  /** Scan-relative path of the resolved chunk's root (`''` when no chunk was
   *  crossed) — used to re-prefix chunk rows back into scan coordinates. */
  chunkRootRel: string
}

/** Resolve a scan-relative path that may sit at/inside a hybrid chunk to the
 *  blob holding it, walking each blob's chunk-pointer map along the path and
 *  recursing through nested chunks (`diff.resolve_chunk_for_path`). A missing
 *  chunk blob is treated as unchunked (best-effort). */
async function resolveChunk(env: Env, blob: string, rel: string, chunkRootRel = ''): Promise<Resolved> {
  if (!rel || rel === '.') return { blob, rebased: '.', chunkRootRel }
  const key = scansPrefix(env) + blob
  const head = await env.SCANS.head(key)
  if (!head) return { blob, rebased: rel, chunkRootRel }
  const pointers = await readChunkPointers(r2Buffer(env.SCANS, key, head.size))
  if (pointers.size === 0) return { blob, rebased: rel, chunkRootRel }
  const parts = rel.split('/')
  for (let i = 0; i < parts.length; i++) {
    const anc = parts.slice(0, i + 1).join('/')
    const ref = pointers.get(anc)
    if (ref && (await env.SCANS.head(scansPrefix(env) + ref))) {
      const remaining = i + 1 < parts.length ? parts.slice(i + 1).join('/') : '.'
      return resolveChunk(env, ref, remaining, chunkRootRel ? `${chunkRootRel}/${anc}` : anc)
    }
  }
  return { blob, rebased: rel, chunkRootRel }
}

/** Re-prefix a chunk-relative row back into scan coordinates: the chunk root
 *  sits at scan-relative `chunkRootRel` (depth `rootDepth`). Identity when the
 *  path wasn't chunked (`chunkRootRel === ''`). */
function toScanAbs(r: TreeRow, chunkRootRel: string, rootDepth: number): TreeRow {
  if (!chunkRootRel) return r
  const path = r.path === '.' ? chunkRootRel : `${chunkRootRel}/${r.path}`
  const parent =
    r.parent == null || r.parent === '' ? null
    : r.parent === '.' ? chunkRootRel
    : `${chunkRootRel}/${r.parent}`
  return { ...r, path, parent, depth: r.depth + rootDepth }
}

/** `GET /api/scan?uri=&depth=&max_rows=[&scan_id=]` — the Flask handler's
 *  scan-backed branch: the newest scan of `uri` or an ancestor, a depth- and
 *  prefix-pruned read of its blob, paths rebased to `uri`. Follows hybrid
 *  chunks (a `child_scan_id` subtree lives in its own blob), both when the
 *  viewed path is inside one and to fill in a chunk-stub child's breakdown. No
 *  filesystem fallback (nothing here can list one); no single-child
 *  auto-expand. */
export const onRequestGet: PagesFunction<Env> = async ({ request, env }) => {
  const params = new URL(request.url).searchParams
  const uri = normUri(params.get('uri'))
  const depth = Number(params.get('depth') ?? DEFAULT_DEPTH)
  const maxRows = Number(params.get('max_rows') ?? DEFAULT_MAX_ROWS)
  const scanId = params.get('scan_id')

  const scans = await getScans(env)
  let scan: Scan | null
  if (scanId) {
    scan = scans.find(s => s.id === Number(scanId)) ?? null
    if (scan && !(uri === scan.path || uri.startsWith(scan.path.replace(/\/$/, '') + '/'))) {
      return error(`Scan ${scanId} does not cover path ${uri}`, 400)
    }
  } else {
    scan = findCovering(scans, uri)
  }
  if (!scan) return error('No scan found for path', 404, { uri })

  // Scan-relative path of the viewed uri, and its depth in scan coordinates.
  const rel = scan.path === uri ? '.' : uri.slice(scan.path.replace(/\/$/, '').length + 1)
  const viewedDepth = depthOf(rel)

  // Follow hybrid chunks: read the blob that actually holds `rel`, then map its
  // rows back into scan coordinates so the rest of the handler is chunk-blind.
  const { blob, rebased, chunkRootRel } = await resolveChunk(env, scan.blob, rel)
  const rootDepth = depthOf(chunkRootRel)
  const key = scansPrefix(env) + blob
  const head = await env.SCANS.head(key)
  if (!head) return error(`blob missing: ${blob}`, 500)
  const rawRows = await readRows(r2Buffer(env.SCANS, key, head.size), {
    maxDepth: depthOf(rebased) + depth,
    prefix: rebased === '.' ? null : rebased,
  })
  const stored = rawRows.map(r => toScanAbs(r, chunkRootRel, rootDepth))

  const rootRow = stored.find(r => r.path === rel)
  if (!rootRow) return error('URI not found in scan', 404, { uri, scan_path: scan.path })

  // Rebase to the viewed dir: its row becomes `.`, descendants lose the prefix.
  const cut = rel === '.' ? 0 : rel.length + 1
  const toApi = (r: TreeRow): ApiRow => {
    const { parent, ...rest } = r
    const path = r.path === rel ? '.' : cut ? r.path.slice(cut) : r.path
    const relParent = path === '.' ? null : parent == null ? null : parent === rel ? '.' : cut && parent.startsWith(rel + '/') ? parent.slice(cut) : parent
    return { ...rest, path, parent: relParent, uri: rowUri(scan, r.path), depth: r.depth - viewedDepth }
  }
  const root = toApi(rootRow)
  const all = stored.filter(r => r !== rootRow).map(toApi).filter(r => r.depth >= 1 && r.depth <= depth)
  const children = all.filter(r => r.depth === 1).map(r => ({ ...r, scanned: true, scan_time: scan.time }))

  // Chunk completeness: a direct child that is itself a chunk stub carries only
  // a summary row here — splice in its top-level (depth-1) breakdown from its
  // chunk blob so the treemap shows real cells, not one opaque box
  // (`server.py` child-scan loading). Best-effort: a missing chunk is skipped.
  const grandchildren: ApiRow[] = []
  for (const c of children) {
    if (!c.child_scan_id || c.depth + 1 > depth) continue
    const cKey = scansPrefix(env) + c.child_scan_id
    const cHead = await env.SCANS.head(cKey)
    if (!cHead) continue
    const cRows = await readRows(r2Buffer(env.SCANS, cKey, cHead.size), { maxDepth: 1 })
    for (const cr of cRows) {
      if (cr.depth !== 1) continue
      grandchildren.push({ ...cr, path: `${c.path}/${cr.path}`, parent: c.path, depth: c.depth + 1, uri: `${c.uri}/${cr.path}` })
    }
  }
  const allRows = grandchildren.length ? [...all, ...grandchildren] : all

  let rows: ApiRow[] = allRows
  if (maxRows > 0 && allRows.length > maxRows) {
    // Top N by size, plus each kept row's ancestors so the treemap stays a tree.
    const byPath = new Map(allRows.map(r => [r.path, r]))
    const kept = [...allRows].sort((a, b) => (b.size ?? 0) - (a.size ?? 0)).slice(0, maxRows)
    const included = new Set(kept.map(r => r.path))
    for (const r of [...kept]) {
      for (let p = r.parent; p && p !== '.' && !included.has(p); ) {
        const pr = byPath.get(p)
        if (!pr) break
        kept.push(pr)
        included.add(p)
        p = pr.parent
      }
    }
    rows = kept
  }

  return json({
    root,
    children,
    rows,
    time: scan.time,
    scan_path: scan.path,
    scan_status: 'full',
    error_count: scan.error_count,
    error_paths: scan.error_paths,
    collapsed_rows: [],
  })
}
