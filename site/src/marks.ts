// Actions-ledger data plumbing (specs/actions-ledger.md): TSQ bindings for
// /api/actions and the most-recent-wins resolver the treemap overlay uses.
// The ledger is an append-only WAL of actions; the API serves the live
// expanded prefix rows per axis (keep / owner), and this module folds them:
// for a path, the effective value per axis is the most recent live row on an
// ancestor-or-equal prefix (recency beats specificity — a newer broad mark
// repaints older deeper ones). `sweep` is the default state (absence of a
// mark) — explicit rows record affirmative decisions, incl. clears.
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useMemo } from 'react'

export type MarkAction = 'keep' | 'keep_last_ckpt' | 'sweep'

export interface KeepRow {
  prefix: string
  keep: MarkAction | null
  ts: number
  who: string
  memo: string | null
  action_id: number
}

export interface OwnerRow {
  prefix: string
  owner: string | null
  ts: number
  who: string
  memo: string | null
  action_id: number
}

/** The resolved (effective) keep-state of a prefix, with provenance. */
export interface Mark {
  prefix: string
  action: MarkAction
  who: string
  ts: number
  note: string | null
}

export interface Owner {
  prefix: string
  /** Canonical user id (or email, for pre-mapping claims). */
  who: string
  ts: number
  /** The assigner (`actions.actor`) and their memo — provenance. */
  by: string
  memo: string | null
}

export const ACTION_LABELS: Record<MarkAction, string> = {
  keep: 'keep',
  keep_last_ckpt: 'keep last ckpt',
  sweep: 'sweep',
}

// The sweep close date is TBD (it slipped past the original 2026-08-28
// target); UI copy says "date TBD" rather than committing to one.

// 30s poll: several people mark concurrently during the sprint, and the
// overlay should reflect their marks without a reload.
export function useMarks(enabled: boolean) {
  return useQuery<{ keeps: KeepRow[]; owners: OwnerRow[] }, Error>({
    queryKey: ['actions'],
    enabled,
    refetchInterval: 30_000,
    queryFn: async () => {
      const r = await fetch('/api/actions', { credentials: 'include' })
      if (!r.ok) throw new Error(`actions: ${r.status}`)
      return r.json()
    },
  })
}

/** One POSTable action; omitted axis = untouched, null value = clear. */
export interface ActionPost {
  pattern: string
  keep?: MarkAction | null
  /** `'@me'` = the server resolves the actor's canonical user id. */
  owner?: string | null
  memo?: string
  scan?: string
}

// The scan id the viewer is looking at, stamped onto posted actions. Set by
// App (module-level: mutations fire from deep components that don't
// otherwise care which scan is showing).
let currentScan: string | undefined
export const setCurrentScan = (s?: string) => { currentScan = s }

export function useMarkMutations() {
  const qc = useQueryClient()
  const done = () => {
    void qc.invalidateQueries({ queryKey: ['actions'] })
    void qc.invalidateQueries({ queryKey: ['mark-totals'] })
  }
  const post = useMutation({
    mutationFn: async (v: ActionPost | ActionPost[]) => {
      const items = (Array.isArray(v) ? v : [v]).map(a => ({ scan: currentScan, ...a }))
      const r = await fetch('/api/actions', {
        method: 'POST',
        credentials: 'include',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(items.length === 1 ? items[0] : items),
      })
      if (!r.ok) throw new Error(((await r.json()) as { error?: string }).error ?? `${r.status}`)
    },
    onSuccess: done,
  })
  // Compat surfaces for the mark/claim call sites.
  const put = {
    mutate: (v: { prefix: string; action: MarkAction | null; note?: string }, opts?: { onSuccess?: () => void }) =>
      post.mutate({ pattern: v.prefix, keep: v.action, memo: v.note }, opts),
    error: post.error,
  }
  const claim = {
    // `owner` omitted → claim for the actor (`'@me'`); a canonical user id →
    // assign it to that user; `release: true` → clear ownership.
    mutate: (v: { prefix: string; owner?: string | null; release?: boolean }) =>
      post.mutate({ pattern: v.prefix, owner: v.release ? null : (v.owner ?? '@me') }),
    error: post.error,
  }
  return { put, claim, post }
}

export interface ResolvedMark {
  /** Effective state: most recent live row on an ancestor-or-equal prefix. */
  mark: Mark | null
  /** The winning mark sits exactly on this prefix (vs inherited). */
  own: boolean
  /** Marked prefixes strictly inside this subtree (drill to see them). */
  under: number
}

export interface MarkIndex {
  resolve: (uri: string) => ResolvedMark
  claimOf: (uri: string) => Owner | null
  /** Live set-marks strictly under a prefix — what a broad mark would repaint. */
  overridesOf: (uri: string) => { n: number; keeps: number }
  /** Latest live row per prefix (normalized, trailing `/`) — for tree walks
   * that thread the winning mark down instead of calling `resolve` per node. */
  keeps: Map<string, KeepRow>
  /** Latest live claim per prefix — the ownership WAL the walkers apply on
   * top of scan attribution (a claim re-attributes its subtree instantly,
   * without waiting for the next scan's pipeline pass). */
  owners: Map<string, OwnerRow>
  count: number
}

export const newer = (a: { ts: number; action_id: number }, b: { ts: number; action_id: number }): boolean =>
  a.ts > b.ts || (a.ts === b.ts && a.action_id > b.action_id)

/** Latest live row per prefix (the API may return history rows per prefix). */
function foldLatest<R extends { prefix: string; ts: number; action_id: number }>(rows: R[]): Map<string, R> {
  const m = new Map<string, R>()
  for (const r of rows) {
    const cur = m.get(r.prefix)
    if (!cur || newer(r, cur)) m.set(r.prefix, r)
  }
  return m
}

/**
 * Lookups are O(depth): a prefix's state is decided by the newest live row on
 * one of its ancestors-or-self, so `resolve` walks the ~6 ancestor prefixes
 * and probes a Map — not a scan over every mark. (The original brute-force
 * scan was fine at hundreds of marks; at ~7k marks × ~1k rendered cells ×
 * a re-render per hover it made tooltips take seconds.) "Marks strictly
 * below" counts are precomputed once per index build (rows × depth).
 */
export function useMarkIndex(data: { keeps: KeepRow[]; owners: OwnerRow[] } | undefined): MarkIndex {
  return useMemo(() => {
    const norm = (uri: string) => (uri.endsWith('/') ? uri : uri + '/')
    const keeps = foldLatest((data?.keeps ?? []).map(r => (r.prefix.endsWith('/') ? r : { ...r, prefix: r.prefix + '/' })))
    const owners = foldLatest((data?.owners ?? []).map(r => (r.prefix.endsWith('/') ? r : { ...r, prefix: r.prefix + '/' })))
    // 'gs://b/x/y/' → ['gs://b/', 'gs://b/x/', 'gs://b/x/y/'] (self last).
    const ancestors = (p: string): string[] => {
      const out: string[] = []
      let i = p.indexOf('/', 'gs://'.length)
      while (i !== -1) {
        out.push(p.slice(0, i + 1))
        i = p.indexOf('/', i + 1)
      }
      return out
    }
    // Live set-marks strictly inside each prefix (n) and how many of those
    // are keeps (kept) — what a broad mark at that prefix would repaint.
    const below = new Map<string, { n: number; kept: number }>()
    for (const r of keeps.values()) {
      if (r.keep == null) continue
      const anc = ancestors(r.prefix)
      for (const a of anc.slice(0, -1)) {
        const b = below.get(a) ?? { n: 0, kept: 0 }
        b.n++
        if (r.keep !== 'sweep') b.kept++
        below.set(a, b)
      }
    }
    const resolve = (uri: string): ResolvedMark => {
      const p = norm(uri)
      let win: KeepRow | null = null
      for (const a of ancestors(p)) {
        const r = keeps.get(a)
        if (r && (!win || newer(r, win))) win = r
      }
      const mark = win?.keep != null
        ? { prefix: win.prefix, action: win.keep, who: win.who, ts: win.ts, note: win.memo }
        : null
      return { mark, own: mark != null && win!.prefix === p, under: below.get(p)?.n ?? 0 }
    }
    const claimOf = (uri: string): Owner | null => {
      const p = norm(uri)
      let win: OwnerRow | null = null
      for (const a of ancestors(p)) {
        const r = owners.get(a)
        if (r && (!win || newer(r, win))) win = r
      }
      return win?.owner != null ? { prefix: win.prefix, who: win.owner, ts: win.ts, by: win.who, memo: win.memo } : null
    }
    const overridesOf = (uri: string) => {
      const b = below.get(norm(uri))
      return { n: b?.n ?? 0, keeps: b?.kept ?? 0 }
    }
    let count = 0
    for (const r of keeps.values()) if (r.keep != null) count++
    return { resolve, claimOf, overridesOf, keeps, owners, count }
  }, [data])
}
