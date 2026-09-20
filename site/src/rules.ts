import { useQuery, type UseQueryResult } from '@tanstack/react-query'
import type { Rules } from './types'

/** The published attribution rules (`/data/rules.json`) — one definition for
 * the map page and /marks, so both read one cache entry with one policy. */
export function useRules(): UseQueryResult<Rules> {
  return useQuery<Rules>({
    queryKey: ['rules'],
    queryFn: async () => {
      const r = await fetch('/data/rules.json')
      if (!r.ok) throw new Error(`rules: ${r.status}`)
      return r.json()
    },
    retry: false,
    staleTime: 10 * 60_000,
  })
}
