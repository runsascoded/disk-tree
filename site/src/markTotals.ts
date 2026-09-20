import { useQuery } from '@tanstack/react-query'
import type { MarkState, UserStates } from './sweep'

// Exact keep / sweep / undecided totals from `/api/marks/totals`: the ledger
// folded server-side and priced against the floor-free path index
// (specs/path-agnostic-serving.md §2.3). One number for the map's root
// rollup, /users, and the digest — no tree.json, no floor.
export interface MarkTotals {
  scan: string
  head: number
  bytes: number
  objects: number
  total: Record<MarkState, number>
  users: Record<string, UserStates>
  mark_count: number
  computed: { at: number; ms: number; groups: number; prefixes: number }
}

export function useMarkTotals(scan: string | null, path?: string, enabled = true) {
  return useQuery<MarkTotals, Error>({
    queryKey: ['mark-totals', scan, path ?? ''],
    enabled: enabled && !!scan,
    staleTime: 30_000,
    refetchInterval: 30_000,
    // A fresh ledger head recomputes server-side (~10s cold) — don't give up
    // on the first slow answer.
    retry: 2,
    queryFn: async () => {
      const q = `date=${scan}` + (path ? `&path=${encodeURIComponent(path)}` : '')
      const r = await fetch(`/api/marks/totals?${q}`, { credentials: 'include' })
      if (!r.ok) throw new Error(`marks/totals: ${r.status}`)
      return r.json()
    },
  })
}
