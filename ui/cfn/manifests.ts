/** The scan list, from the `<blob>.scan.json` manifests `disk-tree reduce --to`
 *  / `index --to` leave beside remote blobs — the static stand-in for the
 *  SQLite `scan` table (`scan_manifest.py`). Ids are positional in
 *  `(time, blob)` order, so they're stable across requests without a DB. */
import type { Env } from './env'
import { scansPrefix } from './env'

export const SUFFIX = '.scan.json'

/** One row of the Flask `/api/scans` list (`server._fetch_scans_data`). */
export interface Scan {
  id: number
  path: string
  time: string
  blob: string
  error_count: number | null
  /** JSON-encoded array, as the DB row stores it. */
  error_paths: string | null
  size: number | null
  n_children: number | null
  n_desc: number | null
  mtime: number | null
}

interface Manifest {
  format: string
  version: number
  time: string
  path: string
  blob: string
  size?: number | null
  n_children?: number | null
  n_desc?: number | null
  mtime?: number | null
  error_count?: number | null
  error_paths?: string[] | null
}

/** What the manifest loader needs from a bucket — so tests can hand it a directory. */
export interface Store {
  keys(prefix: string): Promise<string[]>
  text(key: string): Promise<string>
}

export function r2Store(bucket: R2Bucket): Store {
  return {
    async keys(prefix) {
      const out: string[] = []
      let cursor: string | undefined
      do {
        const page = await bucket.list({ prefix, cursor })
        out.push(...page.objects.map(o => o.key))
        cursor = page.truncated ? page.cursor : undefined
      } while (cursor)
      return out
    },
    async text(key) {
      const obj = await bucket.get(key)
      if (!obj) throw new Error(`missing object ${key}`)
      return obj.text()
    },
  }
}

export async function loadScans(store: Store, prefix: string): Promise<Scan[]> {
  const keys = (await store.keys(prefix)).filter(k => k.endsWith(SUFFIX))
  const manifests = await Promise.all(keys.map(async k => JSON.parse(await store.text(k)) as Manifest))
  const valid = manifests.filter(m => m.format === 'disk-tree-scan')
  valid.sort((a, b) => (a.time < b.time ? -1 : a.time > b.time ? 1 : a.blob < b.blob ? -1 : a.blob > b.blob ? 1 : 0))
  return valid.map((m, i) => ({
    id: i + 1,
    path: m.path,
    time: m.time,
    blob: m.blob,
    error_count: m.error_count ?? null,
    error_paths: m.error_paths == null ? null : JSON.stringify(m.error_paths),
    size: m.size ?? null,
    n_children: m.n_children ?? null,
    n_desc: m.n_desc ?? null,
    mtime: m.mtime ?? null,
  }))
}

const TTL_MS = 60_000
let cached: { at: number; scans: Scan[] } | null = null

/** All scans in the bucket, memoized per isolate for a minute (the Flask
 *  server caches its list for 60 s too). */
export async function getScans(env: Env): Promise<Scan[]> {
  const now = Date.now()
  if (cached && now - cached.at < TTL_MS) return cached.scans
  const scans = await loadScans(r2Store(env.SCANS), scansPrefix(env))
  cached = { at: now, scans }
  return scans
}

export const resetScanCache = (): void => { cached = null }

/** Most recent scan per path, newest first (`_fetch_scans_data`). */
export function latestPerPath(scans: Scan[]): Scan[] {
  const by = new Map<string, Scan>()
  for (const s of scans) {
    const cur = by.get(s.path)
    if (!cur || s.time > cur.time) by.set(s.path, s)
  }
  return [...by.values()].sort((a, b) => (a.time < b.time ? 1 : a.time > b.time ? -1 : 0))
}

/** Parent of a scan path — `backends.url.url_parent`: `null` at a root
 *  (`/`, or a bucket root for `scheme://bucket`). */
export function parentOf(path: string): string | null {
  const i = path.indexOf('://')
  if (i >= 0) {
    const scheme = path.slice(0, i + 3)
    const rest = path.slice(i + 3)
    const j = rest.lastIndexOf('/')
    return j <= 0 ? null : scheme + rest.slice(0, j)
  }
  if (path === '/') return null
  const j = path.lastIndexOf('/')
  return j <= 0 ? '/' : path.slice(0, j)
}

/** `uri` and every ancestor, nearest first. */
export function ancestors(uri: string): string[] {
  const out = [uri]
  for (let p = parentOf(uri); p !== null; p = parentOf(p)) out.push(p)
  return out
}

/** The scan `/api/scan` serves for `uri`: the newest scan of `uri` or any
 *  ancestor (the freshest candidate wins, whatever its depth). */
export function findCovering(scans: Scan[], uri: string): Scan | null {
  const paths = new Set(ancestors(uri))
  let best: Scan | null = null
  for (const s of scans) {
    if (paths.has(s.path) && (!best || s.time > best.time)) best = s
  }
  return best
}

export const blobKey = (env: Env, scan: Scan): string => scansPrefix(env) + scan.blob
