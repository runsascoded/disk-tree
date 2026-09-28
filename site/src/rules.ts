import { useQuery, type UseQueryResult } from '@tanstack/react-query'
import type { Rules } from './types'
import { DEFAULT_STORE } from './stores'

/** The published attribution rules (`/data/rules.json`) — one definition for
 * every page, so all read one cache entry with one policy.
 * Only the owners store publishes them; elsewhere the query never runs (a
 * plan-first deployment on R2 has no `rules.json` to 404 on). */
export function useRules(): UseQueryResult<Rules> {
  return useQuery<Rules>({
    queryKey: ['rules'],
    enabled: DEFAULT_STORE.owners,
    queryFn: async () => {
      const r = await fetch('/data/rules.json')
      if (!r.ok) throw new Error(`rules: ${r.status}`)
      return r.json()
    },
    retry: false,
    staleTime: 10 * 60_000,
  })
}
