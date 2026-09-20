import { useEffect } from 'react'
import { DEFAULT_STORE } from './stores'

// The suffix every tab title carries; the page-specific crumbs sit in front of
// it, most-specific first (`Sweep · Marin GCS usage`, `runs · Files · Marin GCS
// usage`). Kept in sync with the `<title>` in index.html and the GCS store's
// `title` (stores.ts).
export const SITE = DEFAULT_STORE.title

/** Set `document.title` to `<crumb> · … · Marin GCS usage`. Pass the
 *  page-specific crumbs (most-specific first); falsy crumbs drop out, and no
 *  crumbs yields the bare site name (the home page). */
export function useDocTitle(...crumbs: (string | false | null | undefined)[]) {
  const title = [...crumbs.filter(Boolean), SITE].join(' · ')
  useEffect(() => {
    document.title = title
  }, [title])
}
