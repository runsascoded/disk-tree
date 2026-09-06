import type { Env } from '../../cfn/env'
import { error, json, normUri } from '../../cfn/http'
import { blobKey, findCovering, getScans } from '../../cfn/manifests'
import type { Scan } from '../../cfn/manifests'
import { r2Buffer, readRows } from '../../cfn/parquet'
import type { TreeRow } from '../../cfn/parquet'

const DEFAULT_DEPTH = 2
const DEFAULT_MAX_ROWS = 2000  // keep in sync with `server.DEFAULT_MAX_ROWS` / `ui/src/api.ts`

/** A row as `/api/scan` returns it: paths relative to the requested uri. */
type ApiRow = Omit<TreeRow, 'parent'> & { parent: string | null; uri: string; scanned?: boolean; scan_time?: string }

const rowUri = (scan: Scan, path: string): string =>
  path === '.' ? scan.path : scan.path === '/' ? `/${path}` : `${scan.path}/${path}`

/** `GET /api/scan?uri=&depth=&max_rows=[&scan_id=]` — the Flask handler's
 *  scan-backed branch: the newest scan of `uri` or an ancestor, a depth- and
 *  prefix-pruned read of its blob, paths rebased to `uri`. No filesystem
 *  fallback (nothing here can list one), no chunk following yet (reduced
 *  blobs are single files), no single-child auto-expand. */
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

  const rel = scan.path === uri ? '.' : uri.slice(scan.path.replace(/\/$/, '').length + 1)
  const viewedDepth = rel === '.' ? 0 : rel.split('/').length
  const key = blobKey(env, scan)
  const head = await env.SCANS.head(key)
  if (!head) return error(`blob missing: ${scan.blob}`, 500)
  const stored = await readRows(r2Buffer(env.SCANS, key, head.size), {
    maxDepth: viewedDepth + depth,
    prefix: rel === '.' ? null : rel,
  })
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

  let rows = all
  if (maxRows > 0 && all.length > maxRows) {
    // Top N by size, plus each kept row's ancestors so the treemap stays a tree.
    const byPath = new Map(all.map(r => [r.path, r]))
    const kept = [...all].sort((a, b) => (b.size ?? 0) - (a.size ?? 0)).slice(0, maxRows)
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
