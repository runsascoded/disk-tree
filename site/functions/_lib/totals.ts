/** Exact keep / sweep / last-ckpt / undecided bytes for the estate (or a
 * drilled subtree) — the ledger folded server-side and priced against the
 * floor-free path index (specs/path-agnostic-serving.md §2.3), so every
 * consumer (map rollup, /users, digest, sweep executor, the per-node mark
 * axis of `/api/subtree`) reads one number.
 *
 * Cost model: one point lookup per live ledger prefix + a one/two-level
 * range under each keep_last_ckpt prefix. Marks cluster, so ~8k prefixes
 * touch ~25 row groups (~200 MB of parquet, read once per (scan, ledger
 * head) and cached in D1 — `mark_totals`, one head at a time). A new action invalidates by
 * changing the head; the recompute happens on the next request. */
import type { Env } from './auth.js'
import { openIndex, readAsks, type Ask, type Row } from './index.js'
import { loadLedger } from './ledger.js'
import { shared } from './shared.js'
import {
  addAgg, computeTotals, foldLatest, idxKey, newAgg, newer,
  type ClaimRow, type KeepRow, type OwnerRow, type PathAgg, type Totals,
} from './marks.js'

const COLUMNS = ['path', 'depth', 'usr', 'b', 'o', 'c2', 'c3', 'c4']
const MAX_GROUPS = 400

/** Bump when the manifest's shape changes: cached bodies with another
 * version are recomputed (2: rows carry `eff`/`net`, clears included; 3: `us`
 * per band + `claims`; 4: claims carry `us` + `action_id` for the owner lens). */
export const MANIFEST_VERSION = 5 // v5: claims carry `who` (the assigner) — /api/assignments

export interface TotalsBody extends Totals {
  v: number
  scan: string
  head: number
  path?: string
  computed: { at: number; ms: number; groups: number; prefixes: number; index: string }
}

// Per-isolate memo on top of D1 (the body is a few MB). A fresh estate
// manifest is a ~15 s compute; the watchdog gives it four times that.
const memo = new Map<string, Promise<TotalsBody>>()
const MEMO_WAIT = 60_000

/** Newest live keep/owner on a STRICT ancestor of `pfx` (what P inherits). */
function inheritedCovering<R extends { prefix: string; ts: number; action_id: number }>(map: Map<string, R>, pfx: string): R | null {
  let win: R | null = null
  for (const r of map.values()) {
    if (r.prefix.length < pfx.length && pfx.startsWith(r.prefix) && (!win || newer(r, win))) win = r
  }
  return win
}

async function compute(env: Env, date: string, keeps: Map<string, KeepRow>, owners: Map<string, OwnerRow>, head: number, scopePfx?: string): Promise<TotalsBody> {
  const t0 = Date.now()
  const idx = await openIndex(env, date)
  // Scope to a subtree P: only marks under-or-at P matter for its totals, and
  // P's residual inherits the newest mark/claim from above P.
  const scope = scopePfx
    ? { inheritedKeep: inheritedCovering(keeps, scopePfx), inheritedOwner: inheritedCovering(owners, scopePfx) }
    : undefined
  const scopeKey = scopePfx ? idxKey(scopePfx).path : null
  const inScope = (p: string) => !scopeKey || (() => { const k = idxKey(p).path; return k === scopeKey || k.startsWith(scopeKey + '/') })()
  if (scopePfx) {
    keeps = new Map([...keeps].filter(([p]) => inScope(p)))
    owners = new Map([...owners].filter(([p]) => inScope(p)))
  }
  const want = new Map<string, number>() // index path → depth, for point lookups
  for (const p of [...keeps.keys(), ...owners.keys()]) {
    const { path, depth } = idxKey(p)
    want.set(path, depth)
  }
  // Roots: the estate's depth-1 buckets, or the single scoped subtree P.
  const scopeDepth = scopePfx ? idxKey(scopePfx).depth : 0
  if (scopeKey) want.set(scopeKey, scopeDepth)
  const klcPaths = new Set<string>()
  for (const r of keeps.values()) if (r.keep === 'keep_last_ckpt') klcPaths.add(idxKey(r.prefix).path)
  const asks: Ask[] = scopeKey ? [{ depth: scopeDepth, path: scopeKey }] : [{ depth: 1, under: '' }]
  for (const [path, depth] of want) asks.push({ depth, path })
  for (const k of klcPaths) {
    const d = k.split('/').length
    asks.push({ depth: d + 1, under: k }, { depth: d + 2, under: k })
  }
  const parentOf = (p: string) => p.slice(0, Math.max(0, p.lastIndexOf('/')))
  const isKlcKid = (r: Row): string | null => {
    const p1 = parentOf(r.path)
    if (klcPaths.has(p1)) return p1
    const p2 = parentOf(p1)
    return p2 && klcPaths.has(p2) ? p2 : null
  }
  const isRoot = (r: Row) => (scopeKey ? r.path === scopeKey && r.depth === scopeDepth : r.depth === 1)
  const { rows, groups } = await readAsks(
    idx,
    asks,
    r => isRoot(r) || want.get(r.path) === r.depth || isKlcKid(r) != null,
    { columns: COLUMNS, maxGroups: MAX_GROUPS },
  )
  const aggs = new Map<string, PathAgg>()
  const buckets = new Set<string>()
  const klcKidAgg = new Map<string, Map<string, number>>() // klc path → child path → bytes
  for (const r of rows) {
    if (isRoot(r)) buckets.add(r.path)
    if (isRoot(r) || want.get(r.path) === r.depth) {
      let a = aggs.get(r.path)
      if (!a) aggs.set(r.path, (a = newAgg()))
      addAgg(a, r)
    }
    const k = isKlcKid(r)
    if (k) {
      let m = klcKidAgg.get(k)
      if (!m) klcKidAgg.set(k, (m = new Map()))
      m.set(r.path, (m.get(r.path) ?? 0) + r.b)
    }
  }
  const klcKids = new Map([...klcKidAgg].map(([k, m]) => [k, [...m].map(([path, b]) => ({ path, b }))]))
  const totals = computeTotals({ keeps, owners, aggs, buckets: [...buckets].sort(), klcKids, scope })
  return {
    v: MANIFEST_VERSION,
    scan: date,
    head,
    ...(scopePfx ? { path: scopePfx } : {}),
    ...totals,
    computed: { at: Math.floor(Date.now() / 1000), ms: Date.now() - t0, groups, prefixes: want.size, index: idx.mode },
  }
}

/** The totals for `date` at the current ledger head — estate-wide, or scoped
 * to `scopePfx` (`gs://marin-<bucket>/…/`). Estate bodies persist in D1
 * (`mark_totals`, keyed by scan + head); scoped ones use the isolate memo. */
export async function markTotals(env: Env, date: string, scopePfx?: string): Promise<TotalsBody> {
  const { keepRows, ownerRows, head } = await loadLedger(env)
  const key = `${date}:${head}:${scopePfx ?? ''}`
  return shared(memo, key, async () => {
      // Only the estate total is worth persisting in D1 (it's the ~200 MB
      // read); a drilled subtree is cheap to recompute per isolate.
      if (!scopePfx) {
        const cached = await env.DB!.prepare('SELECT body FROM mark_totals WHERE scan = ? AND head = ?').bind(date, head).first<{ body: string }>()
        if (cached) {
          const body = JSON.parse(cached.body) as TotalsBody
          if (body.v === MANIFEST_VERSION) return body
        }
      }
      const body = await compute(env, date, foldLatest(keepRows), foldLatest(ownerRows), head, scopePfx)
      if (!scopePfx) {
        // Every reader keys by the live head, so bodies of older heads are
        // dead weight (a few MB per scan, per head): drop them as this one
        // lands, or a lens series under each new head would grow D1 by the
        // whole history's worth of manifests.
        await env.DB!.batch([
          env.DB!.prepare('INSERT OR REPLACE INTO mark_totals (scan, head, body, claims, computed_ts, ms) VALUES (?, ?, ?, ?, ?, ?)')
            .bind(date, head, JSON.stringify(body), JSON.stringify(body.claims), body.computed.at, body.computed.ms),
          env.DB!.prepare('DELETE FROM mark_totals WHERE head < ?').bind(head),
        ])
      }
      return body
  }, MEMO_WAIT)
}

// Claims alone, per (scan, head): a tenth of the manifest.
const claimsMemo = new Map<string, Promise<ClaimRow[]>>()

/** The live claims priced against `date` — what a user lens folds. The
 * manifest carries them, but a lens series reads one per scan, so they are
 * stored beside the body (`mark_totals.claims`) and fetched alone: ~250 KB
 * instead of ~3 MB per scan. Falls back to the whole manifest for a row
 * written before the column existed (or none yet). */
export async function markClaims(env: Env, date: string): Promise<ClaimRow[]> {
  const { head } = await loadLedger(env)
  const full = memo.get(`${date}:${head}:`)
  if (full) return (await full).claims
  const key = `${date}:${head}`
  return shared(claimsMemo, key, async () => {
    const row = await env.DB!.prepare('SELECT claims FROM mark_totals WHERE scan = ? AND head = ?').bind(date, head).first<{ claims: string | null }>()
    if (row?.claims) return JSON.parse(row.claims) as ClaimRow[]
    return (await markTotals(env, date)).claims
  }, MEMO_WAIT)
}
