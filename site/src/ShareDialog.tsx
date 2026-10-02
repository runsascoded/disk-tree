// The "Share…" dialog (specs/dogi.md): copy this page's link, optionally with
// a detailed link-preview card (`og=`), or, for admins, as a link that grants
// read-only access (`key=`, whose preview is detailed too).
import { useEffect, useState } from 'react'
import { useCanAssign } from './auth'
import { SHARE_DAYS, useShare, type ShareMode } from './sharePreview'

export function ShareDialog({ onClose }: { onClose: () => void }) {
  const isAdmin = useCanAssign()
  const [details, setDetails] = useState(false)
  const [access, setAccess] = useState(false)
  const { share, status, reset } = useShare()
  const mode: ShareMode = access ? 'access' : details ? 'preview' : 'plain'
  useEffect(() => reset(), [mode]) // eslint-disable-line react-hooks/exhaustive-deps
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === 'Escape' && onClose()
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onClose])
  return (
    <div className="token-backdrop" onClick={onClose}>
      <div className="token-modal share-modal" onClick={e => e.stopPropagation()} role="dialog" aria-label="Share">
        <div className="token-head">
          <h2>Share this view</h2>
          <button className="token-x" type="button" onClick={onClose} aria-label="Close">×</button>
        </div>
        <label className="share-opt">
          <input type="checkbox" checked={details || access} disabled={access} onChange={e => setDetails(e.target.checked)} />
          <span>
            Preview shows details (labels, sizes, owners)
            <span className="hint">Only changes the card Slack, iMessage etc. show when the link is pasted. It gives no access: people still sign in to open the page. Good for this exact view, {SHARE_DAYS} days; admins can revoke it.</span>
          </span>
        </label>
        {isAdmin && (
          <label className="share-opt">
            <input type="checkbox" checked={access} onChange={e => setAccess(e.target.checked)} />
            <span>
              Grant access to anyone with the link
              <span className="hint">Mints a read-only share link ({SHARE_DAYS} days, revocable on /admin): whoever opens it can browse the site without signing in. Its preview shows details.</span>
            </span>
          </label>
        )}
        <div className="share-actions">
          <button type="button" className="btn primary" disabled={status.state === 'busy'} onClick={() => void share(mode)}>
            {status.state === 'busy' ? 'Copying…' : status.state === 'copied' ? 'Copied ✓' : 'Copy link'}
          </button>
          {status.state === 'error' && <span className="err">{status.error}</span>}
        </div>
        {status.state === 'copied' && <code className="share-url">{status.url}</code>}
      </div>
    </div>
  )
}
