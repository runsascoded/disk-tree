// The cross-scan over-time index reader (specs/obs-axis-indexing.md Phase 1):
// one contiguous `(depth, path)` read of the SCD-2 interval singleton, expanded
// into a `(b, o)` point per scan the index spans — replacing `/api/series`'s
// one point read per scan generation. A singleton over the *observation* axis,
// pointed to in D1 as variant `over-time` under the latest scan it was built
// for; the ordered scan list rides in an `over-time.scans.json` sidecar (the
// D1-footer read path rebuilds row-group stats but not KV metadata).
import type { Env } from './auth.js'
import { indexDir, indexKey, makeStore, num, openIndex, readPoint, str } from './index.js'

const COLS = ['depth', 'path', 'b', 'o', '__scan_lo', '__scan_hi']

/** One SCD-2 interval row: `(b, o)` held over member scans `[lo, hi]`. */
export interface Interval { b: number; o: number; lo: number; hi: number }

/** Expand interval rows against the ordered scan list into `date → {b,o}` —
 * each interval paints its constant value onto scans `[lo, hi]`. A path absent
 * from a scan simply has no interval covering it, so that date gets no point. */
export function expandIntervals(rows: Interval[], scans: string[]): Map<string, { b: number; o: number }> {
  const out = new Map<string, { b: number; o: number }>()
  for (const r of rows) {
    for (let i = r.lo; i <= r.hi; i++) {
      const date = scans[i]
      if (date !== undefined) out.set(date, { b: r.b, o: r.o })
    }
  }
  return out
}

/** The latest scan date carrying an `over-time` pointer (the singleton is
 * rebuilt+re-synced under the newest scan each run); null when none synced. */
export async function latestOverTimeDate(env: Env): Promise<string | null> {
  if (!env.DB) return null
  const r = await env.DB.prepare("SELECT date FROM index_schema WHERE variant = 'over-time' ORDER BY date DESC LIMIT 1").first<{ date: string }>()
  return r?.date ?? null
}

const scansCache = new Map<string, Promise<string[]>>()

/** The ordered scan list from the sidecar beside the index (cached per dir). */
async function overTimeScans(env: Env, dir: string): Promise<string[]> {
  let p = scansCache.get(dir)
  if (!p) {
    p = (async () => {
      const key = indexKey(dir, 'over-time').replace(/over-time\.parquet$/, 'over-time.scans.json')
      const { bytes } = await makeStore(env).get(key)
      return JSON.parse(new TextDecoder().decode(bytes)) as string[]
    })()
    scansCache.set(dir, p)
  }
  try {
    return await p
  } catch (e) {
    scansCache.delete(dir)
    throw e
  }
}

/** A path's `date → {b,o}` line from the over-time index, or null when the
 * index isn't synced or doesn't hold the path (caller falls back to per-scan
 * reads). One contiguous point read + a cached sidecar fetch. */
export async function readOverTime(env: Env, path: string): Promise<Map<string, { b: number; o: number }> | null> {
  const date = await latestOverTimeDate(env)
  if (!date) return null
  const dir = await indexDir(env, date, 'over-time')
  if (!dir) return null
  const depth = path === '' ? 0 : path.split('/').length
  let raw: Record<string, unknown>[]
  try {
    raw = await readPoint(await openIndex(env, date, 'over-time'), depth, path, COLS)
  } catch (e) {
    if (/not synced/.test((e as Error).message)) return null
    throw e
  }
  if (!raw.length) return null
  const scans = await overTimeScans(env, dir)
  const rows: Interval[] = raw
    .filter(r => str(r.path) === path)
    .map(r => ({ b: num(r.b), o: num(r.o), lo: num(r.__scan_lo), hi: num(r.__scan_hi) }))
  return expandIntervals(rows, scans)
}
