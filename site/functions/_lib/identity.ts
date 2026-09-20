/** Canonical user ids server-side — the same rule the client applies
 * (`src/UserChip.tsx`): sanitize an actor string (email or raw id) to a
 * handle, then follow the bundled registry's alias keys to the canonical rec. */
import { IDENTITIES } from '../../src/identities.gen.js'

export const whoToHandle = (who: string): string =>
  who.split('@')[0].toLowerCase().replace(/[^a-z0-9_-]+/g, '-').replace(/^-+|-+$/g, '')

export const canonId = (who: string): string => {
  const h = whoToHandle(who)
  return IDENTITIES[h]?.u ?? h
}
