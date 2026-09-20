/** The mark axis (`k=` ⊆ `ksu`) applied per view node, server-side
 * (specs/view-serving.md §2): of a path's subtree bytes, how many sit under
 * an allowed state.
 *
 * Inputs: the per-mark manifest the estate totals already compute per
 * `(scan, ledger head)` (`_lib/totals.ts`): every live mark's subtree bytes,
 * its *band* (bytes minus deeper marks) split by painted state, and whether a
 * newer ancestor repaints it. From that, any path N's allowed bytes are:
 *
 *   value(N) = [state(cover N) allowed] · (b(N) − Σ_{m ∈ top(N)} bytes(m))
 *            + Σ_{m strictly under N} Σ_{f allowed} net_f(m)
 *
 * where cover(N) is the newest ledger row at-or-above N — a mark or a clear
 * (its *effective* state is N's residual's state — recency beats specificity;
 * a newer clear above a mark repaints it unmarked), top(N) the outermost rows
 * strictly under N (their subtrees are not N's residual), and every mark
 * under N contributes its band under its *effective* state (a repainted mark's
 * band carries the repainter's). Bands partition the subtree, so this is
 * exact; `keep_last_ckpt` counts as keep ∧ sweep where it can't be split,
 * matching the client's `markAllowed`. */
import { canonId } from './identity.js'
import type { MarkState, MarkRow } from './marks.js'
import { idxKey } from './marks.js'

export type MarkAxis = 'keep' | 'sweep' | 'unmarked'
const LETTERS: Record<string, MarkAxis> = { k: 'keep', s: 'sweep', u: 'unmarked' }

/** `k=ksu` letters → the allowed set; all/none/absent → undefined (no scope). */
export function parseMarkAxes(raw: string | null): Set<MarkAxis> | undefined {
  if (!raw) return undefined
  const out = new Set<MarkAxis>()
  for (const ch of raw) if (LETTERS[ch]) out.add(LETTERS[ch])
  return out.size === 0 || out.size === 3 ? undefined : out
}

export const markAllowed = (state: MarkState, allowed: ReadonlySet<MarkAxis>): boolean =>
  state === 'keep_last_ckpt' ? allowed.has('keep') || allowed.has('sweep') : allowed.has(state as MarkAxis)

interface Mark {
  path: string // index key (`marin-b/x/y`)
  ts: number
  eff: MarkState // effective state of its band (own keep, a repainter's, or unmarked for a clear)
  bytes: number // subtree bytes (all owners)
  net: Record<MarkState, number> // band bytes by painted state
  /** The lens user's share of the band, and of the whole subtree (Σ over
   * marks at-or-under this one) — a lens view's rows are that user's slices,
   * so the fold has to subtract and add the user's bytes, not everyone's. */
  ub: number
  ubUnder: number
}

export interface MarkScope {
  /** Bytes of `path`'s subtree (`b` = the view's bytes there: everyone's, or
   * the lens user's) that sit under an allowed state. */
  value(path: string, b: number): number
}

export function markScope(marks: MarkRow[], allowed: ReadonlySet<MarkAxis>, user?: string): MarkScope {
  const userShare = (us: Record<string, number>): number => {
    if (!user) return 0
    let b = 0
    for (const [k, v] of Object.entries(us)) if (canonId(k) === user) b += v
    return b
  }
  const all: Mark[] = marks
    .map(m => ({ path: idxKey(m.prefix).path, ts: m.ts, eff: m.eff, bytes: m.bytes, net: m.net, ub: userShare(m.us), ubUnder: 0 }))
    .sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0))
  const byPath = new Map(all.map(m => [m.path, m]))
  const parentOf = (p: string): string => {
    const cut = p.lastIndexOf('/')
    return cut === -1 ? '' : p.slice(0, cut)
  }
  // Nearest strict ancestor mark per mark (null = none): a mark under N is
  // one of N's *top* marks iff its nearest ancestor mark is not under N.
  const anc = new Map<string, Mark | null>()
  for (const m of all) {
    let a: Mark | null = null
    for (let q = parentOf(m.path); ; q = parentOf(q)) {
      const hit = byPath.get(q)
      if (hit) { a = hit; break }
      if (q === '') break
    }
    anc.set(m.path, a)
  }
  // The user's bytes under each mark = its band share plus every nested
  // mark's (bands partition the subtree).
  if (user) {
    for (const m of all) {
      for (let a: Mark | null = m; a; a = anc.get(a.path) ?? null) a.ubUnder += m.ub
    }
  }
  const bandOf = (m: Mark): number => Object.values(m.net).reduce((s, v) => s + v, 0)
  // Allowed bytes of a band: all of it, or the lens user's share of it (the
  // share is assumed spread across a decomposed keep_last_ckpt's halves).
  const netAllowed = (m: Mark): number => {
    let s = 0
    for (const f of Object.keys(m.net) as MarkState[]) if (m.net[f] > 0 && markAllowed(f, allowed)) s += m.net[f]
    if (!user) return s
    const band = bandOf(m)
    return band > 0 ? (s * m.ub) / band : 0
  }
  const bytesUnder = (m: Mark): number => (user ? m.ubUnder : m.bytes)
  // First index in the sorted list whose path is >= `key`.
  const lowerBound = (key: string): number => {
    let lo = 0
    let hi = all.length
    while (lo < hi) {
      const mid = (lo + hi) >> 1
      if (all[mid].path < key) lo = mid + 1
      else hi = mid
    }
    return lo
  }
  return {
    value(path: string, b: number): number {
      // cover: the newest mark on N's ancestor-or-self chain (recency wins).
      let cover: Mark | null = null
      for (let q = path; ; q = parentOf(q)) {
        const m = byPath.get(q)
        if (m && (!cover || m.ts > cover.ts)) cover = m
        if (q === '') break
      }
      // marks strictly under N: one contiguous run of the sorted list.
      const pfx = path === '' ? '' : path + '/'
      let sumNet = 0
      let topBytes = 0
      for (let i = lowerBound(pfx); i < all.length; i++) {
        const m = all[i]
        if (pfx && !m.path.startsWith(pfx)) break
        if (m.path === path) continue
        sumNet += netAllowed(m)
        const a = anc.get(m.path) ?? null
        if (!a || a.path.length <= path.length) topBytes += bytesUnder(m)
      }
      const residual = Math.max(0, b - topBytes)
      const coverState: MarkState = cover?.eff ?? 'unmarked'
      return (markAllowed(coverState, allowed) ? residual : 0) + sumNet
    },
  }
}

/** "Does any live mark sit strictly under `path`?" over a sorted mark list —
 * the per-user worklist settles at the outermost subtrees with no marks
 * inside (the review backlog's unit). */
export function marksUnder(marks: MarkRow[]): (path: string) => boolean {
  const paths = marks.map(m => idxKey(m.prefix).path).sort()
  return (path: string) => {
    const pfx = path === '' ? '' : path + '/'
    let lo = 0
    let hi = paths.length
    while (lo < hi) {
      const mid = (lo + hi) >> 1
      if (paths[mid] < pfx) lo = mid + 1
      else hi = mid
    }
    return lo < paths.length && (pfx === '' || paths[lo].startsWith(pfx))
  }
}

