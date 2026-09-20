/**
 * Exact keep / sweep / undecided totals from the ledger + the floor-free path
 * index (specs/path-agnostic-serving.md §2.3; algorithm from
 * specs/exact-state-totals.md). Pure: callers supply the folded ledger and the
 * index aggregates for the prefixes involved; nothing here does I/O.
 *
 * Model. Every live ledger prefix (a mark or a claim) is a node of a trie
 * `U`. Its *band* is its own bytes minus the bytes of its nearest marked
 * descendants — the exclusive slice that no deeper prefix claims. The band
 * is painted with the prefix's *effective* keep and owner: the newest row on
 * an ancestor-or-self (recency beats specificity — a newer broad mark
 * repaints older deeper ones, a newer clear repaints them back). Totals are
 * sums of bands; whatever a bucket holds outside every band is undecided.
 */

export type MarkAction = 'keep' | 'sweep' | 'keep_last_ckpt'
export type MarkState = MarkAction | 'unmarked'
export const STATES: MarkState[] = ['keep', 'keep_last_ckpt', 'sweep', 'unmarked']

export interface LedgerRow { prefix: string; ts: number; action_id: number }
export interface KeepRow extends LedgerRow { keep: MarkAction | null; who?: string }
export interface OwnerRow extends LedgerRow { owner: string | null; who?: string }

/** Per-path aggregate from the index: bytes, objects, per-user bytes, and
 * non-STANDARD class bytes ("2" NL, "3" CL, "4" AR; STANDARD = b − Σ). */
export interface PathAgg { b: number; o: number; us: Record<string, number>; cb: Record<string, number> }
export const newAgg = (): PathAgg => ({ b: 0, o: 0, us: {}, cb: {} })
export const addAgg = (a: PathAgg, r: { b: number; o: number; usr: string | null; c2: number; c3: number; c4: number }): void => {
  a.b += r.b
  a.o += r.o
  if (r.usr) a.us[r.usr] = (a.us[r.usr] ?? 0) + r.b
  for (const [k, v] of [['2', r.c2], ['3', r.c3], ['4', r.c4]] as [string, number][]) if (v) a.cb[k] = (a.cb[k] ?? 0) + v
}
const subAgg = (a: PathAgg, kids: PathAgg[]): PathAgg => {
  const out: PathAgg = { b: a.b, o: a.o, us: { ...a.us }, cb: { ...a.cb } }
  for (const k of kids) {
    out.b -= k.b
    out.o -= k.o
    for (const [u, b] of Object.entries(k.us)) out.us[u] = (out.us[u] ?? 0) - b
    for (const [c, b] of Object.entries(k.cb)) out.cb[c] = (out.cb[c] ?? 0) - b
  }
  out.b = Math.max(0, out.b)
  out.o = Math.max(0, out.o)
  for (const m of [out.us, out.cb]) for (const k of Object.keys(m)) if (m[k] <= 0) delete m[k]
  return out
}
/** Full class mix (STANDARD implied) scaled to `b` of the aggregate's bytes. */
const mixOf = (a: PathAgg, b: number): Record<string, number> => {
  if (a.b <= 0 || b <= 0) return {}
  const cold = Object.values(a.cb).reduce((s, v) => s + v, 0)
  const f = b / a.b
  const out: Record<string, number> = {}
  if (a.b > cold) out['1'] = (a.b - cold) * f
  for (const [c, v] of Object.entries(a.cb)) out[c] = v * f
  return out
}

export const newer = (a: LedgerRow, b: LedgerRow): boolean => a.ts > b.ts || (a.ts === b.ts && a.action_id > b.action_id)
export const norm = (p: string): string => (p.endsWith('/') ? p : p + '/')

/** Latest live row per (normalized) prefix — the API returns history rows. */
export function foldLatest<R extends LedgerRow>(rows: R[]): Map<string, R> {
  const m = new Map<string, R>()
  for (const raw of rows) {
    const r = raw.prefix.endsWith('/') ? raw : { ...raw, prefix: raw.prefix + '/' }
    const cur = m.get(r.prefix)
    if (!cur || newer(r, cur)) m.set(r.prefix, r)
  }
  return m
}

/** `gs://marin-b/x/y/` → index key `marin-b/x/y` at depth 3. */
export const idxKey = (prefix: string): { path: string; depth: number } => {
  const path = prefix.replace(/^[a-z0-9]+:\/\//, '').replace(/\/+$/, '')
  return { path, depth: path.split('/').length }
}
const parentOf = (path: string): string => {
  const i = path.lastIndexOf('/')
  return i === -1 ? '' : path.slice(0, i)
}

export const CKPT_NUM_RE = /^(?:step|checkpoint|ckpt|iter|epoch|global_?step)[-_]?(\d+)/i

export interface StateTotals { keep: number; keep_last_ckpt: number; sweep: number; unmarked: number }
export interface UserTotals extends StateTotals { mix: Record<MarkState, Record<string, number>> }
export interface MarkRow {
  prefix: string
  /** null = a clear (an explicit "no decision" that repaints deeper marks
   * as unmarked when newer) — carried so per-node folds see it. */
  keep: MarkAction | null
  who?: string
  ts: number
  bytes: number
  objects: number
  /** Bytes/objects this mark alone decides (minus deeper live marks). */
  net_bytes: number
  net_objects: number
  /** Set when a newer ancestor mark repaints this one — it decides nothing. */
  repainted_by?: string
  /** The state this mark's band actually carries (its own keep, or the
   * repainter's) — what a per-node fold applies. */
  eff: MarkState
  /** The band's bytes by painted state: a decomposed `keep_last_ckpt` splits
   * into keep + sweep; anything else is all one state. Sums to the band. */
  net: Record<MarkState, number>
  /** The band's bytes per person — the claimant takes the whole band when a
   * live claim covers it, else the scan's per-user slices. */
  us: Record<string, number>
}
/** A live owner claim, sized from the index (bytes under its prefix). */
export interface ClaimRow {
  prefix: string
  owner: string | null
  /** The assigner (`actions.actor`) — always a person today. */
  who?: string
  ts: number
  action_id: number
  bytes: number
  objects: number
  /** The subtree's bytes per scan-attributed user (what the claim
   * repaints) — the owner lens subtracts and adds these per band. */
  us: Record<string, number>
  /** Set when a newer ancestor claim overrides this one. */
  repainted_by?: string
}
export interface Totals {
  bytes: number
  objects: number
  total: StateTotals
  users: Record<string, UserTotals>
  marks: MarkRow[]
  claims: ClaimRow[]
}

export interface TotalsInput {
  keeps: Map<string, KeepRow>
  owners: Map<string, OwnerRow>
  /** Aggregates by index path for every ledger prefix (missing = 0 bytes:
   * the prefix no longer exists in this scan) and for every bucket (depth 1). */
  aggs: Map<string, PathAgg>
  /** Bucket index paths (depth-1 rows present in the scan). */
  buckets: string[]
  /** For `keep_last_ckpt` prefixes: candidate children (index path → bytes)
   * one and two levels down, keyed by the KLC prefix's index path. */
  klcKids: Map<string, { path: string; b: number }[]>
  /** Scope the totals to one subtree P instead of the whole estate: `buckets`
   * is `[P]`, `keeps`/`owners` hold only the marks under-or-at P, and P's
   * residual (bytes under no deeper mark) takes P's *inherited* state — the
   * newest mark/claim on an ancestor-or-self of P. Absent = whole estate
   * (buckets are the depth-1 roots, residual is unmarked). */
  scope?: {
    inheritedKeep: KeepRow | null
    inheritedOwner: OwnerRow | null
  }
}

const stateOf = (r: KeepRow | null): MarkState => r?.keep ?? 'unmarked'

interface Node {
  prefix: string
  path: string
  depth: number
  parent: Node | null
  kids: Node[]
  keep: KeepRow | null
  owner: OwnerRow | null
  effKeep: KeepRow | null
  effOwner: OwnerRow | null
  agg: PathAgg
  band: PathAgg
}

/** Kept bytes inside a KLC prefix: at the first level (1 or 2 down) that has
 * step-numbered dirs, each ckpt-parent keeps its max-step child. */
export function klcKeptBytes(kPath: string, kids: { path: string; b: number }[]): number {
  const byParent = new Map<string, { path: string; b: number }[]>()
  for (const k of kids) {
    const par = parentOf(k.path)
    const arr = byParent.get(par)
    if (arr) arr.push(k)
    else byParent.set(par, [k])
  }
  const bestOf = (arr: { path: string; b: number }[]): number | null => {
    let best: { n: number; b: number } | null = null
    for (const k of arr) {
      const m = CKPT_NUM_RE.exec(k.path.slice(k.path.lastIndexOf('/') + 1))
      if (!m) continue
      const n = Number(m[1])
      if (!best || n > best.n) best = { n, b: k.b }
    }
    return best?.b ?? null
  }
  // Direct step children win (the client walk stops at the first step level).
  const direct = byParent.get(kPath)
  if (direct) {
    const b = bestOf(direct)
    if (b != null) return b
  }
  let kept = 0
  for (const [par, arr] of byParent) if (par !== kPath) kept += bestOf(arr) ?? 0
  return kept
}

export function computeTotals(input: TotalsInput): Totals {
  const { keeps, owners, aggs, buckets, klcKids } = input
  const nodes = new Map<string, Node>()
  const mk = (prefix: string): Node => {
    let n = nodes.get(prefix)
    if (n) return n
    const { path, depth } = idxKey(prefix)
    n = { prefix, path, depth, parent: null, kids: [], keep: null, owner: null, effKeep: null, effOwner: null, agg: aggs.get(path) ?? newAgg(), band: newAgg() }
    nodes.set(prefix, n)
    return n
  }
  for (const [p, r] of keeps) mk(p).keep = r
  for (const [p, r] of owners) mk(p).owner = r
  // Parent = nearest strict ancestor in the trie.
  const ancestors = (p: string): string[] => {
    const out: string[] = []
    let i = p.indexOf('/', 'gs://'.length)
    while (i !== -1) { out.push(p.slice(0, i + 1)); i = p.indexOf('/', i + 1) }
    return out.slice(0, -1) // strict
  }
  const ordered = [...nodes.values()].sort((a, b) => a.depth - b.depth || (a.prefix < b.prefix ? -1 : 1))
  for (const n of ordered) {
    for (const a of ancestors(n.prefix).reverse()) {
      const par = nodes.get(a)
      if (par) { n.parent = par; par.kids.push(n); break }
    }
    const inhK = n.parent?.effKeep ?? input.scope?.inheritedKeep ?? null
    n.effKeep = n.keep && (!inhK || newer(n.keep, inhK)) ? n.keep : inhK
    const inhO = n.parent?.effOwner ?? input.scope?.inheritedOwner ?? null
    n.effOwner = n.owner && (!inhO || newer(n.owner, inhO)) ? n.owner : inhO
  }
  for (const n of nodes.values()) n.band = subAgg(n.agg, n.kids.map(k => k.agg))

  const total: StateTotals = { keep: 0, keep_last_ckpt: 0, sweep: 0, unmarked: 0 }
  const users: Record<string, UserTotals> = {}
  const userRec = (u: string): UserTotals => (users[u] ??= { keep: 0, keep_last_ckpt: 0, sweep: 0, unmarked: 0, mix: { keep: {}, keep_last_ckpt: {}, sweep: {}, unmarked: {} } })
  const addMix = (into: Record<string, number>, mix: Record<string, number>, f: number) => {
    for (const [c, b] of Object.entries(mix)) if (b * f > 0) into[c] = (into[c] ?? 0) + b * f
  }
  // Paint `frac` of a band with state `f`: the band's bytes to the total, and
  // to its claimant (whole band) or its scan-attributed users (their slices).
  const paint = (band: PathAgg, f: MarkState, frac: number, claimant: string | null) => {
    if (frac <= 0 || band.b <= 0) return
    total[f] += band.b * frac
    const mix = mixOf(band, band.b)
    if (claimant) {
      const u = userRec(claimant)
      u[f] += band.b * frac
      addMix(u.mix[f], mix, frac)
    } else {
      for (const [usr, b] of Object.entries(band.us)) {
        const u = userRec(usr)
        u[f] += b * frac
        addMix(u.mix[f], mix, (b / band.b) * frac)
      }
    }
  }
  const painted = new Map<Node, Record<MarkState, number>>()
  const bandUs = new Map<Node, Record<string, number>>()
  for (const n of nodes.values()) {
    const f: MarkState = n.effKeep?.keep ?? 'unmarked'
    const claimant = n.effOwner?.owner ?? null
    bandUs.set(n, claimant ? (n.band.b > 0 ? { [claimant]: n.band.b } : {}) : Object.fromEntries(Object.entries(n.band.us).filter(([, b]) => b > 0)))
    const split: Record<MarkState, number> = { keep: 0, keep_last_ckpt: 0, sweep: 0, unmarked: 0 }
    if (f === 'keep_last_ckpt') {
      // Decompose where the step dirs are in view; the kept child's bytes are
      // keep, the rest of the band sweeps. Unresolvable → stays "last ckpt".
      const kPath = idxKey(n.effKeep!.prefix).path
      const kids = klcKids.get(kPath)
      const kept = kids?.length ? Math.min(n.band.b, klcKeptBytes(kPath, kids)) : null
      if (kept == null) { paint(n.band, 'keep_last_ckpt', 1, claimant); split.keep_last_ckpt = n.band.b }
      else {
        const r = n.band.b > 0 ? kept / n.band.b : 0
        paint(n.band, 'keep', r, claimant)
        paint(n.band, 'sweep', 1 - r, claimant)
        split.keep = n.band.b * r
        split.sweep = n.band.b * (1 - r)
      }
    } else { paint(n.band, f, 1, claimant); split[f] = n.band.b }
    painted.set(n, split)
  }
  // Undecided remainder: each bucket minus its top-level bands. A ledger row
  // exactly on the bucket is the sole top-level node and its band already
  // covers the whole remainder.
  let bytes = 0
  let objects = 0
  const residualState = stateOf(input.scope?.inheritedKeep ?? null)
  const residualClaimant = input.scope?.inheritedOwner?.owner ?? null
  for (const bp of buckets) {
    const agg = aggs.get(bp) ?? newAgg()
    bytes += agg.b
    objects += agg.o
    // A mark/claim exactly on the root covers its whole subtree — its own band
    // already painted the remainder; nothing left to attribute to inheritance.
    if (nodes.has(`gs://${bp}/`)) continue
    const top = [...nodes.values()].filter(n => !n.parent && n.path.startsWith(bp + '/'))
    paint(subAgg(agg, top.map(n => n.agg)), residualState, 1, residualClaimant)
  }
  const marks: MarkRow[] = []
  for (const n of nodes.values()) {
    if (!n.keep) continue
    const live = n.effKeep === n.keep
    marks.push({
      prefix: n.prefix,
      keep: n.keep.keep,
      who: n.keep.who,
      ts: n.keep.ts,
      bytes: n.agg.b,
      objects: n.agg.o,
      net_bytes: live ? n.band.b : 0,
      net_objects: live ? n.band.o : 0,
      ...(live ? {} : { repainted_by: n.effKeep!.prefix }),
      eff: n.effKeep?.keep ?? 'unmarked',
      net: painted.get(n)!,
      us: bandUs.get(n)!,
    })
  }
  marks.sort((a, b) => b.net_bytes - a.net_bytes)
  const claims: ClaimRow[] = []
  for (const n of nodes.values()) {
    if (!n.owner) continue
    claims.push({
      prefix: n.prefix,
      owner: n.owner.owner,
      who: n.owner.who,
      ts: n.owner.ts,
      action_id: n.owner.action_id,
      bytes: n.agg.b,
      objects: n.agg.o,
      us: Object.fromEntries(Object.entries(n.agg.us).filter(([, b]) => b > 0)),
      ...(n.effOwner === n.owner ? {} : { repainted_by: n.effOwner!.prefix }),
    })
  }
  claims.sort((a, b) => b.bytes - a.bytes)
  for (const k of STATES) total[k] = Math.round(total[k])
  for (const u of Object.values(users)) for (const k of STATES) u[k] = Math.round(u[k])
  return { bytes, objects, total, users, marks, claims }
}
