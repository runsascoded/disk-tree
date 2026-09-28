// Tree walks behind the /mark tabs: collect the maximal subtrees that a
// review lens cares about (mine / unclaimed), so each tab is a
// ranked worklist of prefixes rather than a hunt through the treemap.
import { useQuery } from '@tanstack/react-query'
import { reaggregate, type NodePred } from './filterTree'
import { newer, type KeepRow, type MarkAction, type MarkIndex, type OwnerRow } from './marks'
import { classMix, unclaimedBytes, type TreeNode } from './types'

export interface SweepRow {
  uri: string
  node: TreeNode
  /** Bytes the lens attributes here (mine / unclaimed). */
  b: number
  /** Share of the node the lens owns. */
  frac: number
}

/** Bytes the lens assigns to a node. */
export type Lens = (n: TreeNode) => number

export const userLens = (user: string): Lens => n => n.us?.find(([u]) => u === user)?.[1] ?? 0

/** Bytes no person owns under a node — the unclaimed pool's share (the
 * map's unclaimed highlight dims cells that aren't majority-unclaimed). */
export const unattrLens: Lens = unclaimedBytes

/**
 * Treemap-scoping predicate for a lens: keep the maximal subtrees the lens
 * owns (≥`minFrac` of the node), prune the rest, re-aggregate ancestors —
 * so a tab's map shows just that tab's data instead of dimming the estate.
 */
export const lensNodePred = (lens: Lens, minFrac = 0.6): NodePred => n => {
  const b = lens(n)
  return b > 0 && b >= minFrac * n.b
}

/**
 * Maximal nodes where the lens owns ≥`minFrac` of the node and ≥`minBytes`
 * absolute — don't descend into a collected node (its children are implied),
 * but do descend into mixed nodes to find the owned subtrees inside them.
 */
export function collectRows(
  root: TreeNode,
  lens: Lens,
  { minBytes = 100e9, minFrac = 0.6 }: { minBytes?: number; minFrac?: number } = {},
): SweepRow[] {
  const rows: SweepRow[] = []
  const walk = (n: TreeNode, uri: string) => {
    for (const c of n.c ?? []) {
      if (c.n.startsWith('(')) continue
      const cUri = `${uri}/${c.n}`
      const b = lens(c)
      if (b < minBytes) continue
      if (b >= minFrac * c.b) rows.push({ uri: cUri, node: c, b, frac: b / c.b })
      else walk(c, cUri)
    }
  }
  // root.c are buckets; bucket URIs are `gs://<bucket>`
  for (const bucket of root.c ?? []) walk(bucket, `gs://${bucket.n}`)
  return rows
}

/**
 * The keep-axis "to-do": the largest prefixes with no keep/sweep decision
 * anywhere in their subtree or ancestry (mirrors `functions/_lib/todo.ts`, so
 * the tab and `dt-cloud todo` agree). A keep decision inherits down, so a node
 * is a to-do item only when it's fully untouched; marking part of it drops it
 * and surfaces its still-clean siblings. Biggest first.
 */
export function collectTodo(root: TreeNode, idx: MarkIndex, minBytes = 20e9): SweepRow[] {
  const rows: SweepRow[] = []
  const walk = (n: TreeNode, uri: string) => {
    const { mark, under } = idx.resolve(uri)
    if (mark) return // decided on this prefix or an ancestor — whole subtree settled
    if (under === 0) {
      if (n.b >= minBytes) rows.push({ uri, node: n, b: n.b, frac: 1 })
      return // no decision anywhere below → a clean chunk; take it whole
    }
    for (const c of n.c ?? []) {
      if (!c.n.startsWith('(')) walk(c, `${uri}/${c.n}`)
    }
  }
  for (const bucket of root.c ?? []) walk(bucket, `gs://${bucket.n}`)
  return rows.sort((a, b) => b.b - a.b)
}

// ---- Fast state walks -------------------------------------------------------
// `MarkIndex.resolve` is O(marked prefixes) per call — fine per rendered cell,
// quadratic-feeling over a whole-tree walk. These walkers instead thread the
// winning row down the DFS (newest ancestor-or-equal row, same semantics as
// `resolve`) and answer "any live mark strictly below?" from a precomputed
// ancestor set, so each node costs O(1).

export type MarkState = MarkAction | 'unmarked'

interface StateWalkCtx {
  /** Latest live row exactly on this (trailing-`/`) prefix. */
  own: (uri: string) => KeepRow | undefined
  /** Latest live claim exactly on this prefix (when claims are threaded). */
  ownOwner: (uri: string) => OwnerRow | undefined
  /** Any live set-mark or claim strictly below this prefix? */
  below: (uri: string) => boolean
}

function stateWalkCtx(keeps: Map<string, KeepRow>, owners?: Map<string, OwnerRow>): StateWalkCtx {
  const anc = new Set<string>()
  const addAnc = (prefix: string) => {
    // gs://bucket/a/b/ → ancestors gs://bucket/, gs://bucket/a/
    const segs = prefix.replace(/\/+$/, '').split('/')
    for (let i = 3; i < segs.length; i++) anc.add(segs.slice(0, i).join('/') + '/')
  }
  for (const r of keeps.values()) if (r.keep != null) addAnc(r.prefix)
  // Claims count as "something below" too — the walk must descend far enough
  // to apply ownership overrides at their prefixes.
  if (owners) for (const r of owners.values()) if (r.owner != null) addAnc(r.prefix)
  const norm = (uri: string) => (uri.endsWith('/') ? uri : uri + '/')
  return {
    own: uri => keeps.get(norm(uri)),
    ownOwner: uri => owners?.get(norm(uri)),
    below: uri => anc.has(norm(uri)),
  }
}

/** Newest of the inherited claim and this prefix's own (a newer null-owner
 * row releases inherited claims). */
const winOwner = (ctx: StateWalkCtx, uri: string, inherited: OwnerRow | null): OwnerRow | null => {
  const own = ctx.ownOwner(uri)
  return own && (!inherited || newer(own, inherited)) ? own : inherited
}

/** Newest of the inherited winner and this prefix's own row (clears count:
 * a newer `keep: null` row repaints inherited marks back to unmarked). */
const winRow = (ctx: StateWalkCtx, uri: string, inherited: KeepRow | null): KeepRow | null => {
  const own = ctx.own(uri)
  return own && (!inherited || newer(own, inherited)) ? own : inherited
}

const stateOf = (win: KeepRow | null): MarkState => win?.keep ?? 'unmarked'

/** The mark-state axis the page filters on: `keep`/`sweep` are effective
 * decisions, `unmarked` is the review backlog. */
export type MarkAxis = 'keep' | 'sweep' | 'unmarked'
export const MARK_AXES: MarkAxis[] = ['keep', 'sweep', 'unmarked']

/** Does a prefix's effective state fall inside the page's mark-state axis? */
export const markAllowed = (state: MarkState, allowed: ReadonlySet<MarkAxis>): boolean => allowed.has(state)

/**
 * Scope the map to the mark states in `allowed` (the page's mark axis —
 * `{unmarked}` is the old To-do lens): prune any subtree whose effective
 * decision falls outside it, keep uniformly-decided subtrees whole, recurse into
 * mixed ones and re-aggregate ancestors. Folded `(other)` tiles inside mixed
 * nodes are dropped — the tree can't say what's inside them.
 */
/**
 * Per-user bytes by state across the whole tree, in one walk: descend only
 * while a subtree still holds deeper marks; at each settle point distribute
 * the node's `us` shares (minus what descended into recursed children — so
 * folded tiles and floor residue take the node's own state).
 */
/** One user's bytes by state, plus the storage-class mix behind each state
 * (class id → bytes; STANDARD = "1") so keep / sweep / undecided can be priced
 * like Attributed is. A user's share of a node is assumed to carry the node's
 * class mix (the tree has no per-user class split). */
export interface UserStates extends Record<MarkState, number> {
  mix: Record<MarkState, Record<string, number>>
}

export function allUserStates(
  root: TreeNode,
  idx: MarkIndex,
  /** Canonicalize claim `owner` values (emails → user ids); claims are the
   * ownership WAL — a claimed subtree attributes wholly to its claimant,
   * overriding scan attribution until the pipeline catches up. */
  canon: (who: string) => string = w => w,
): Map<string, UserStates> {
  const ctx = stateWalkCtx(idx.keeps, idx.owners)
  const out = new Map<string, UserStates>()
  // `mix` is the class mix of the bytes being settled at this point (the
  // node's, or the residue left after recursed children); the user's `b` is
  // spread over it pro rata.
  const add = (u: string, f: MarkState, b: number, mix: Record<string, number>, mixB: number) => {
    let rec = out.get(u)
    if (!rec) out.set(u, (rec = { keep: 0, sweep: 0, unmarked: 0, mix: { keep: {}, sweep: {}, unmarked: {} } }))
    rec[f] += b
    if (mixB > 0) for (const [c, cb] of Object.entries(mix)) if (cb > 0) rec.mix[f][c] = (rec.mix[f][c] ?? 0) + b * (cb / mixB)
  }
  const walk = (n: TreeNode, uri: string, inhKeep: KeepRow | null, inhOwn: OwnerRow | null) => {
    const win = winRow(ctx, uri, inhKeep)
    const ownRow = winOwner(ctx, uri, inhOwn)
    const claimant = ownRow?.owner != null ? canon(ownRow.owner) : null
    const f = stateOf(win)
    if (!ctx.below(uri)) {
      const mix = classMix(n)
      if (claimant) add(claimant, f, n.b, mix, n.b)
      else for (const [u, b] of n.us ?? []) if (b > 0) add(u, f, b, mix, n.b)
      return
    }
    const rest = new Map<string, number>(n.us ?? [])
    let restB = n.b
    const restMix = classMix(n)
    for (const c of n.c ?? []) {
      if (c.n.startsWith('(')) continue
      restB -= c.b
      walk(c, `${uri}/${c.n}`, win, ownRow)
      for (const [u, b] of c.us ?? []) rest.set(u, (rest.get(u) ?? 0) - b)
      for (const [cls, cb] of Object.entries(classMix(c))) restMix[cls] = Math.max(0, (restMix[cls] ?? 0) - cb)
    }
    const restMixB = Object.values(restMix).reduce((a, b) => a + b, 0)
    if (claimant) { if (restB > 0) add(claimant, f, restB, restMix, restMixB) }
    else for (const [u, b] of rest) if (b > 0) add(u, f, b, restMix, restMixB)
  }
  for (const b of root.c ?? []) walk(b, `gs://${b.n}`, null, null)
  return out
}

/**
 * MarkState totals (bytes) for one subtree — the "of the current view, how much is
 * keep / sweep / undecided" rollup. `uri` is the node's full URI (`''` for
 * the artifact root, whose children are buckets); marks inherited from
 * ancestors of `uri` are folded in.
 */
export function subtreeStateTotals(
  node: TreeNode,
  uri: string,
  idx: MarkIndex,
): Record<MarkState, number> {
  const ctx = stateWalkCtx(idx.keeps)
  const out: Record<MarkState, number> = { keep: 0, sweep: 0, unmarked: 0 }
  const settle = (b: number, win: KeepRow | null) => {
    if (b > 0) out[stateOf(win)] += b
  }
  const walk = (n: TreeNode, u: string, inherited: KeepRow | null) => {
    const win = winRow(ctx, u, inherited)
    if (!ctx.below(u)) return settle(n.b, win)
    let rest = n.b
    for (const c of n.c ?? []) {
      if (c.n.startsWith('(')) continue
      rest -= c.b
      walk(c, `${u}/${c.n}`, win)
    }
    settle(rest, win)
  }
  if (uri === '') {
    for (const b of node.c ?? []) walk(b, `gs://${b.n}`, null)
    return out
  }
  // Fold in marks on ancestors of `uri` (the drilled node inherits them).
  const clean = uri.replace(/^([a-z0-9]+:\/\/)/, '')
  const scheme = uri.slice(0, uri.length - clean.length) || 'gs://'
  const segs = clean.replace(/\/+$/, '').split('/')
  let inherited: KeepRow | null = null
  for (let i = 1; i < segs.length; i++) {
    inherited = winRow(ctx, `${scheme}${segs.slice(0, i).join('/')}`, inherited)
  }
  walk(node, `${scheme}${segs.join('/')}`, inherited)
  return out
}

/** Reviewed = covered by any mark (deepest-wins ancestor or own). */
export const reviewedBytes = (rows: SweepRow[], idx: MarkIndex): number =>
  rows.reduce((s, r) => s + (idx.resolve(r.uri).mark ? r.b : 0), 0)

/** D1 `user_emails` as an email → canonical-user map (signed-in readers only). */
export function useUserEmails(enabled: boolean): Record<string, string> | undefined {
  const { data } = useQuery<Record<string, string>, Error>({
    queryKey: ['user-emails'],
    enabled,
    staleTime: 10 * 60_000,
    retry: false,
    queryFn: async () => {
      const r = await fetch('/api/db/user_emails', { credentials: 'include' })
      if (!r.ok) throw new Error(`user_emails: ${r.status}`)
      const { rows } = (await r.json()) as { rows: { email: string; user: string }[] }
      return Object.fromEntries(rows.map(x => [x.email, x.user]))
    },
  })
  return data
}

/** The viewer's canonical attribution user id, from D1 `user_emails`. */
export function useMyUser(email: string | undefined, enabled: boolean): string | null {
  const data = useUserEmails(enabled && !!email)
  return (email && data?.[email.toLowerCase()]) || null
}
