// "Copy link with preview" (specs/dogi.md): mint a per-view token for the
// current page and copy its URL, so a paste unfurls with the full card
// (labels, sizes, owners) instead of the anonymous one. The token covers this
// exact view only and is recorded (and revocable) server-side.
import { useCallback, useState } from 'react'

export type ShareStatus = { state: 'idle' } | { state: 'busy' } | { state: 'copied'; days: number } | { state: 'error'; error: string }

export const SHARE_DAYS = 30

/** Mint a full-card link for `href`; resolves to the URL to share. */
export async function mintPreviewLink(href: string, days = SHARE_DAYS): Promise<string> {
  const r = await fetch('/api/og/mint', {
    method: 'POST',
    credentials: 'include',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ url: href, days }),
  })
  const j = (await r.json().catch(() => ({}))) as { url?: string; error?: string }
  if (!r.ok || !j.url) throw new Error(j.error ?? `${r.status}`)
  return j.url
}

/** `share()` mints for the current location and copies the link. */
export function useSharePreview() {
  const [status, setStatus] = useState<ShareStatus>({ state: 'idle' })
  const share = useCallback(async () => {
    setStatus({ state: 'busy' })
    try {
      const url = await mintPreviewLink(window.location.href)
      await navigator.clipboard.writeText(url)
      setStatus({ state: 'copied', days: SHARE_DAYS })
    } catch (e) {
      setStatus({ state: 'error', error: (e as Error).message })
    }
    setTimeout(() => setStatus({ state: 'idle' }), 4000)
  }, [])
  return { share, status }
}

export const shareLabel = (s: ShareStatus): string =>
  s.state === 'busy' ? 'Copying link with preview…'
  : s.state === 'copied' ? `Copied: preview link, good for ${s.days} days`
  : s.state === 'error' ? `Couldn't copy: ${s.error}`
  : 'Copy link with preview'
