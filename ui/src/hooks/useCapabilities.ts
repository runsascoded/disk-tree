import { useQuery } from '@tanstack/react-query'
import { fetchCapabilities } from '../api'
import type { Capabilities } from '../api'

/** What the server behind `/api` can do (`GET /api/capabilities`). The static
 *  (Cloudflare Pages) deployment serves scans but can't walk, mutate, or run
 *  the heavier analyses; components hide those affordances. `undefined` while
 *  loading — treat as "not yet", so nothing flashes on and off. A server
 *  without the endpoint can do everything (see `fetchCapabilities`). */
export function useCapabilities(): Capabilities | undefined {
  const { data } = useQuery({ queryKey: ['capabilities'], queryFn: fetchCapabilities, staleTime: Infinity })
  return data
}
