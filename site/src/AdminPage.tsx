import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { Link } from 'react-router-dom'
import { useDocTitle } from './title'

// Share-link console (staff-only; the backend enforces the `admin` scope on
// every /api/auth/grants route — this page just renders the 403 politely).
// Mint a named link, copy it exactly once (the raw token is never shown
// again), and revoke it to kill every session it ever minted, instantly.

interface Grant {
  id: string
  name: string | null
  note: string | null
  email: string | null
  scopes: string[]
  maxRedeems: number | null
  redeems: number
  expiresAt: number | null
  createdAt: number
  createdBy: string
  revokedAt: number | null
  lastUsedAt: number | null
}

const fmtTs = (ts: number | null): string => (ts ? new Date(ts * 1000).toLocaleString() : '—')

const linkFor = (token: string): string => `${window.location.origin}/?key=${token}`

export function AdminPage() {
  useDocTitle('Admin')
  const qc = useQueryClient()
  const [memo, setMemo] = useState('')
  const [user, setUser] = useState('')
  const [days, setDays] = useState('30')
  const [minted, setMinted] = useState<{ label: string; url: string } | null>(null)

  const grantsQ = useQuery<{ grants: Grant[] }, Error>({
    queryKey: ['auth', 'grants'],
    retry: false,
    queryFn: async () => {
      const r = await fetch('/api/auth/grants', { credentials: 'include' })
      if (r.status === 401 || r.status === 403) throw new Error('staff only')
      if (!r.ok) throw new Error(`grants: ${r.status}`)
      return r.json()
    },
  })

  const mint = useMutation({
    mutationFn: async () => {
      const expiresInS = days.trim() ? Number(days) * 86400 : null
      // memo → `note` (the link's label, admin-side); user → `name` (also the
      // link-holder's display name, so only set when the link is person-bound)
      const r = await fetch('/api/auth/grants', {
        method: 'POST',
        credentials: 'include',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ note: memo.trim(), name: user.trim() || null, scopes: ['gcs'], expiresInS }),
      })
      if (!r.ok) throw new Error(`create failed: ${r.status}`)
      return r.json() as Promise<{ grant: Grant; token: string }>
    },
    onSuccess: ({ grant, token }) => {
      setMinted({ label: grant.note ?? grant.name ?? 'unnamed', url: linkFor(token) })
      setMemo('')
      setUser('')
      void qc.invalidateQueries({ queryKey: ['auth', 'grants'] })
    },
  })

  const revoke = useMutation({
    mutationFn: async (id: string) => {
      const r = await fetch(`/api/auth/grants/${id}/revoke`, { method: 'POST', credentials: 'include' })
      if (!r.ok) throw new Error(`revoke failed: ${r.status}`)
    },
    onSuccess: () => void qc.invalidateQueries({ queryKey: ['auth', 'grants'] }),
  })

  if (grantsQ.error) {
    return (
      <div className="admin-page">
        <h1>Share links</h1>
        <p>{grantsQ.error.message === 'staff only' ? 'This console is staff-only.' : grantsQ.error.message}</p>
        <p><Link to="/">← back</Link></p>
      </div>
    )
  }

  const grants = grantsQ.data?.grants ?? []
  return (
    <div className="admin-page">
      <h1>Share links</h1>
      <p>
        Named, revocable view links for people outside the SSO/whitelist set. The raw link is shown{' '}
        <strong>once</strong>, when the link is created; revoking a link signs out everyone using it, on their next request.{' '}
        Per-email access lives in the <Link to="/admin/db/allowed_emails">allowlist table</Link> (all tables:{' '}
        <Link to="/admin/db">/admin/db</Link>). <Link to="/">← back to the dashboard</Link>
      </p>
      <form
        className="mint"
        onSubmit={e => {
          e.preventDefault()
          mint.mutate()
        }}
      >
        <div className="field">
          <label htmlFor="mint-memo">Memo</label>
          <input id="mint-memo" value={memo} onChange={e => setMemo(e.target.value)} required />
          <span className="hint">label for this link — where it's shared or what it's for, e.g. <code>#internal-discuss</code></span>
        </div>
        <div className="field">
          <label htmlFor="mint-user">User</label>
          <input id="mint-user" value={user} onChange={e => setUser(e.target.value)} />
          <span className="hint">optional — set when the link is for one person; shown as their display name while they browse</span>
        </div>
        <div className="field">
          <label htmlFor="mint-days">Expiry</label>
          <input id="mint-days" className="days" value={days} onChange={e => setDays(e.target.value)} inputMode="numeric" size={4} />
          <span className="hint">days until the link stops working; blank = never</span>
        </div>
        <div className="field submit">
          <button type="submit" disabled={mint.isPending}>Create link</button>
          {mint.error && <span className="err">{mint.error.message}</span>}
        </div>
      </form>
      {minted && (
        <div className="minted">
          <p>
            Link <strong>{minted.label}</strong> — copy it now; it won't be shown again:
          </p>
          <div className="token-row">
            <code>{minted.url}</code>
            <button type="button" onClick={() => void navigator.clipboard.writeText(minted.url)}>copy</button>
          </div>
        </div>
      )}
      <table className="grants">
        <thead>
          <tr>
            <th>memo</th><th>user</th><th>scopes</th><th>redeems</th><th>last used</th><th>expires</th><th>created</th><th></th>
          </tr>
        </thead>
        <tbody>
          {grants.map(g => (
            <tr key={g.id} className={g.revokedAt ? 'revoked' : ''}>
              <td>{g.note ?? <em>—</em>}</td>
              <td>{g.name}</td>
              <td>{g.scopes.join(' ')}</td>
              <td>{g.redeems}{g.maxRedeems != null ? `/${g.maxRedeems}` : ''}</td>
              <td>{fmtTs(g.lastUsedAt)}</td>
              <td>{fmtTs(g.expiresAt)}</td>
              <td title={`by ${g.createdBy}`}>{fmtTs(g.createdAt)}</td>
              <td>
                {g.revokedAt
                  ? <span className="revoked-label">revoked {fmtTs(g.revokedAt)}</span>
                  : <button type="button" onClick={() => revoke.mutate(g.id)} disabled={revoke.isPending}>revoke</button>}
              </td>
            </tr>
          ))}
          {!grants.length && !grantsQ.isPending && (
            <tr><td colSpan={8}><em>no links created yet</em></td></tr>
          )}
        </tbody>
      </table>
    </div>
  )
}
