import type { Env } from '../../../cfn/env'
import { error, json, normUri } from '../../../cfn/http'
import { ancestors, blobKey, getScans } from '../../../cfn/manifests'
import { r2Buffer, readRows } from '../../../cfn/parquet'

/** `GET /api/scans/history?uri=` — every scan of `uri` or an ancestor, newest
 *  first, each with `scan_path` (the path actually scanned). An ancestor
 *  scan's stats are the subpath's row in its blob (a depth-bounded,
 *  prefix-pruned read). */
export const onRequestGet: PagesFunction<Env> = async ({ request, env }) => {
  const uri = normUri(new URL(request.url).searchParams.get('uri'))
  const paths = new Set(ancestors(uri))
  const scans = (await getScans(env))
    .filter(s => paths.has(s.path))
    .sort((a, b) => (a.time < b.time ? 1 : a.time > b.time ? -1 : 0))
  const results = []
  for (const s of scans) {
    const base = { id: s.id, path: s.path, time: s.time, size: s.size, n_children: s.n_children, n_desc: s.n_desc, scan_path: s.path }
    if (s.path === uri) {
      results.push(base)
      continue
    }
    const rel = uri.slice(s.path.length).replace(/^\//, '')
    const key = blobKey(env, s)
    const head = await env.SCANS.head(key)
    if (!head) continue
    try {
      const rows = await readRows(r2Buffer(env.SCANS, key, head.size), { maxDepth: rel.split('/').length, prefix: rel })
      const row = rows.find(r => r.path === rel)
      if (row) results.push({ ...base, size: row.size, n_children: row.n_children, n_desc: row.n_desc })
    } catch (e) {
      return error(`reading ${key}: ${(e as Error).message}`, 500)
    }
  }
  return json(results)
}
