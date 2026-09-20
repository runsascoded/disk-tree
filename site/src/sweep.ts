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

// ---- keep_last_ckpt decomposition -----------------------------------------
// A KLC mark means "within each checkpoint run under this prefix, keep the
// newest step dir; sweep the rest". Aggregations shouldn't show that as its
// own category — they should show the *actual* keep/sweep proportions.

const CKPT_NUM_RE = /^(?:step|checkpoint|ckpt|iter|epoch|global_?step)[-_]?(\d+)/i
const normUri = (uri: string): string => (uri.endsWith('/') ? uri : uri + '/')

export interface KlcSplit {
  /** Kept subtrees (each ckpt-parent's newest step child), with their bytes. */
  kept: { uri: string; b: number }[]
  keptB: number
  totalB: number
}

export type KlcIndex = Map<string, KlcSplit>

/**
 * For each live `keep_last_ckpt` mark, walk its subtree in the scan tree: at
 * every node with step-numbered children, the max-step child is kept (no
 * deeper recursion); everything else sweeps. Marks whose prefix the tree
 * can't resolve, or with no ckpt-shaped descendants in view, get no entry —
 * callers render those as first-class KLC (amber).
 */
export function klcSplits(root: TreeNode, keeps: Map<string, KeepRow>): KlcIndex {
  const out: KlcIndex = new Map()
  for (const r of keeps.values()) {
    if (r.keep !== 'keep_last_ckpt') continue
    const p = normUri(r.prefix)
    let node: TreeNode | undefined = root
    for (const s of p.replace(/^[a-z0-9]+:\/\//, '').replace(/\/+$/, '').split('/')) {
      node = node?.c?.find(c => c.n === s)
    }
    if (!node) continue
    const kept: { uri: string; b: number }[] = []
    const walk = (n: TreeNode, u: string) => {
      const steps = (n.c ?? [])
        .map(c => ({ c, m: CKPT_NUM_RE.exec(c.n) }))
        .filter((x): x is { c: TreeNode; m: RegExpExecArray } => x.m != null)
      if (steps.length) {
        let best = steps[0]
        for (const s of steps) if (Number(s.m[1]) > Number(best.m[1])) best = s
        kept.push({ uri: `${u}${best.c.n}/`, b: best.c.b })
        return
      }
      for (const c of n.c ?? []) if (!c.n.startsWith('(')) walk(c, `${u}${c.n}/`)
    }
    walk(node, p)
    if (kept.length) out.set(p, { kept, keptB: kept.reduce((s, k) => s + k.b, 0), totalB: node.b })
  }
  return out
}

/** A klc-governed uri's concrete state: inside a kept subtree → keep; contains
 * kept subtrees → mixed (caller splits by `klcKeptWithin`); else sweep. */
export const klcStateAt = (uri: string, split: KlcSplit): 'keep' | 'sweep' | 'mixed' => {
  const u = normUri(uri)
  if (split.kept.some(k => u.startsWith(k.uri))) return 'keep'
  if (split.kept.some(k => k.uri.startsWith(u))) return 'mixed'
  return 'sweep'
}

/** Kept bytes inside `uri` (for proportional splits at mixed nodes). */
export const klcKeptWithin = (uri: string, split: KlcSplit): number => {
  const u = normUri(uri)
  return split.kept.reduce((s, k) => s + (k.uri.startsWith(u) ? k.b : 0), 0)
}

/** The mark-state axis the page filters on: `keep`/`sweep` are effective
 * decisions (a `keep_last_ckpt` mark counts as both — it splits its subtree),
 * `unmarked` is the review backlog. */
export type MarkAxis = 'keep' | 'sweep' | 'unmarked'
export const MARK_AXES: MarkAxis[] = ['keep', 'sweep', 'unmarked']

/** Does a prefix's effective state fall inside the page's mark-state axis? */
export const markAllowed = (state: MarkState, allowed: ReadonlySet<MarkAxis>): boolean =>
  state === 'keep_last_ckpt' ? allowed.has('keep') || allowed.has('sweep') : allowed.has(state)

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
  klc?: KlcIndex,
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
    if (!rec) out.set(u, (rec = { keep: 0, keep_last_ckpt: 0, sweep: 0, unmarked: 0, mix: { keep: {}, keep_last_ckpt: {}, sweep: {}, unmarked: {} } }))
    rec[f] += b
    if (mixB > 0) for (const [c, cb] of Object.entries(mix)) if (cb > 0) rec.mix[f][c] = (rec.mix[f][c] ?? 0) + b * (cb / mixB)
  }
  // Settle `each(f, frac)` bytes at `uri` under `win` — KLC decomposes into
  // its real keep/sweep proportions when the split is resolvable.
  const settle = (uri: string, nodeB: number, win: KeepRow | null, each: (f: MarkState, frac: number) => void) => {
    const f = stateOf(win)
    if (f !== 'keep_last_ckpt' || !klc) return each(f, 1)
    const split = klc.get(win!.prefix.endsWith('/') ? win!.prefix : win!.prefix + '/')
    if (!split) return each(f, 1)
    const rel = klcStateAt(uri, split)
    if (rel !== 'mixed') return each(rel, 1)
    const ratio = nodeB > 0 ? Math.min(1, klcKeptWithin(uri, split) / nodeB) : 0
    each('keep', ratio)
    each('sweep', 1 - ratio)
  }
  const walk = (n: TreeNode, uri: string, inhKeep: KeepRow | null, inhOwn: OwnerRow | null) => {
    const win = winRow(ctx, uri, inhKeep)
    const ownRow = winOwner(ctx, uri, inhOwn)
    const claimant = ownRow?.owner != null ? canon(ownRow.owner) : null
    if (!ctx.below(uri)) {
      const mix = classMix(n)
      settle(uri, n.b, win, (f, frac) => {
        if (claimant) add(claimant, f, n.b * frac, mix, n.b)
        else for (const [u, b] of n.us ?? []) if (b > 0) add(u, f, b * frac, mix, n.b)
      })
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
    settle(uri, n.b, win, (f, frac) => {
      if (claimant) { if (restB > 0) add(claimant, f, restB * frac, restMix, restMixB) }
      else for (const [u, b] of rest) if (b > 0) add(u, f, b * frac, restMix, restMixB)
    })
  }
  for (const b of root.c ?? []) walk(b, `gs://${b.n}`, null, null)
  return out
}

/**
 * MarkState totals (bytes) for one subtree — the "of the current view, how much is
 * keep / sweep / undecided" rollup. `uri` is the node's full URI (`''` for
 * the artifact root, whose children are buckets); marks inherited from
 * ancestors of `uri` are folded in. KLC decomposes via `klc` when given —
 * bytes under an unresolvable KLC mark stay in `keep_last_ckpt`.
 */
export function subtreeStateTotals(
  node: TreeNode,
  uri: string,
  idx: MarkIndex,
  klc?: KlcIndex,
): Record<MarkState, number> {
  const ctx = stateWalkCtx(idx.keeps)
  const out: Record<MarkState, number> = { keep: 0, keep_last_ckpt: 0, sweep: 0, unmarked: 0 }
  const settle = (u: string, b: number, win: KeepRow | null) => {
    if (b <= 0) return
    const f = stateOf(win)
    if (f !== 'keep_last_ckpt' || !klc) { out[f] += b; return }
    const split = klc.get(win!.prefix.endsWith('/') ? win!.prefix : win!.prefix + '/')
    if (!split) { out[f] += b; return }
    const rel = klcStateAt(u, split)
    if (rel === 'keep') out.keep += b
    else if (rel === 'sweep') out.sweep += b
    else {
      const kept = Math.min(b, klcKeptWithin(u, split))
      out.keep += kept
      out.sweep += b - kept
    }
  }
  const walk = (n: TreeNode, u: string, inherited: KeepRow | null) => {
    const win = winRow(ctx, u, inherited)
    if (!ctx.below(u)) return settle(u, n.b, win)
    let rest = n.b
    for (const c of n.c ?? []) {
      if (c.n.startsWith('(')) continue
      rest -= c.b
      walk(c, `${u}/${c.n}`, win)
    }
    settle(u, rest, win)
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

const CKPT_SEG_RE = /^(step|checkpoint|ckpt|iter|epoch|global_?step)[-_]?\d+/i

/**
 * Checkpoint-shaped: under a checkpoints path segment, named like one, or
 * ≥2 step-numbered children. Gates the `keep_last_ckpt` button — offering
 * it on arbitrary dirs was confusing. (Scan post-proc will flag this
 * properly per specs/actions-ledger.md; these heuristics cover the interim.)
 */
// A dir is checkpoint-shaped when IT is the checkpoints dir (its own name),
// or it directly holds one (a run dir with `checkpoints/` under it), or it
// holds ≥ 2 step-numbered children. Being *somewhere under* a `checkpoints/`
// ancestor is not enough — that offered "keep last ckpt" on every leaf of a
// bucket's `checkpoints/` tree. Children below the pixel budget aren't
// loaded, so a dir whose shape is unknown is not offered (the CLI still
// accepts KLC anywhere). Better still would be an ahead-of-time flag on each
// index row (specs/children-table-selection.md § later).
/** A run directory: ≥ 2 step-numbered children (`step-100`, `step-200`, …).
 * `keep_last_ckpt` at such a dir keeps the highest step and sweeps the rest. */
const isRunDir = (n: TreeNode): boolean =>
  (n.c ?? []).filter(c => CKPT_SEG_RE.test(c.n)).length >= 2

/** Offer keep-last-ckpt only when checkpoints sit within ~2 levels below the
 * node: the run dir itself, or its parent (a `checkpoints/` dir or a group of
 * runs). Higher up (`marin/`, whose runs are 4–5 levels down) it is far too
 * broad — one step would be kept across every run — so it is not offered
 * there; the CLI/API still accepts KLC at any depth for a curated run list.
 * A node's own name is intentionally not enough: naming a dir `checkpoints`
 * doesn't put the steps within reach if they are deep below.
 * Folding can hide children, so an unknown shape is simply not offered.
 * (A per-row "checkpoints within N" flag from the index would be exact —
 * specs/children-table-selection.md § later.) */
export const looksCkpt = (n: TreeNode, _uri?: string): boolean =>
  n.k === 1 ||
  isRunDir(n) ||
  (n.c ?? []).some(isRunDir)

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
