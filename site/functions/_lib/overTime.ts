// The cross-scan over-time index reader (specs/obs-axis-indexing.md Phase 1):
// a path's size-over-time line stitched across sealed capped-K multi-scan
// groups, replacing `/api/series`'s one point read per scan generation.
//
// Routing + expansion are pyrmts primitives; cw supplies only the range IO.
// `MultiScanD1Index.listMultiScans('over-time')` reads the group manifest
// (`pyramid_multiscans`); `seriesAcrossGroups` orders the groups by scan span
// and, per group, calls cw's `load` — a *footer-pruned* `readPoint` of just the
// queried `(depth,path)` row groups, returned as a key-filtered partial
// `MultiScan` (that key's interval rows + the group's full `scans` list, which
// is the load-bearing part). So a line is ⌈N/K⌉ pruned reads, not one giant
// file; the unsealed ≤K-scan tip is served by `/api/series`'s per-scan fallback.
import type { Env } from './auth.js'
import { openIndex, readPoint } from './index.js'
import { type Dim, type Metric, type MultiScan, type MultiScanIndexEntry, type Pyramid, seriesAcrossGroups } from 'pyrmts'
import { MultiScanD1Index } from 'pyrmts-cfw'

const COLS = ['depth', 'path', 'b', 'o', '__scan_lo', '__scan_hi']

// cw's over-time shape as a pyrmts logical schema: key `(depth, path)`, state
// `(b, o)` (mirrors `over_time_pyramid()` in the producer).
type Schema = Pick<Pyramid, 'binCol' | 'dims' | 'metrics'>
const SCHEMA: Schema = {
  binCol: 'depth',
  dims: [{ name: 'path', type: 'string' } as Dim],
  metrics: [{ name: 'b', monoid: 'count' } as Metric, { name: 'o', monoid: 'count' } as Metric],
}

/** A path's `scan → {b,o}` line from the over-time group manifest, or null when
 * no groups are synced or the path is absent everywhere (caller falls back to
 * per-scan reads). Each group is a footer-pruned point read; absent (monoid-
 * identity `0/0`) scans are dropped so the chart keeps gap semantics. */
export async function readOverTime(env: Env, path: string): Promise<Map<string, { b: number; o: number }> | null> {
  if (!env.DB) return null
  let entries: MultiScanIndexEntry[]
  try {
    entries = await new MultiScanD1Index(env.DB).listMultiScans('over-time')
  } catch {
    return null // manifest table absent (not migrated) → fallback
  }
  if (!entries.length) return null
  const depth = path === '' ? 0 : path.split('/').length
  // load(archiveKey): footer-pruned fetch of one group's rows for this key, as a
  // partial MultiScan. The group's dir/date is its archive key (how the producer
  // synced each group's footer); its scans list comes from the manifest entry.
  const byKey = new Map(entries.map(e => [e.key, e]))
  const load = async (archiveKey: string): Promise<MultiScan> => {
    const entry = byKey.get(archiveKey)
    if (!entry) return { rows: [], scans: [], encoder: 'interval' }
    const rows = await readPoint(await openIndex(env, archiveKey, 'over-time'), depth, path, COLS)
    return { rows: rows as MultiScan['rows'], scans: entry.scans, encoder: 'interval' }
  }
  const pts = await seriesAcrossGroups(entries, SCHEMA, { depth, path }, load)
  const out = new Map<string, { b: number; o: number }>()
  for (const p of pts) {
    const b = num(p.state.b)
    const o = num(p.state.o)
    if (b !== 0 || o !== 0) out.set(p.scan, { b, o })
  }
  return out.size ? out : null
}

const num = (v: unknown): number => (typeof v === 'bigint' ? Number(v) : (v as number) ?? 0)
