/** `/access` — mint, list and revoke share links, and read the access log.
 *  Admin (allowlisted SSO) only. Shapes are `@open-athena/auth`'s: string ids,
 *  epoch-second timestamps, `redeems` = sessions minted (not requests). */
import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Alert, Box, Button, TextField, Typography } from '@mui/material'
import type { AccessEvent, Grant } from '@open-athena/auth'
import { ApiError, api, isAdmin, ssoUrl, useWhoami } from '../auth'

interface Minted {
  grant: Grant
  token: string
}

const day = (s: number | null | undefined): string => (s == null ? '—' : new Date(s * 1000).toISOString().slice(0, 10))
const minute = (s: number | null | undefined): string =>
  s == null ? '—' : new Date(s * 1000).toISOString().slice(0, 16).replace('T', ' ') + 'Z'

/** A link signs the browser in on the scans page, then `?key=` is stripped. */
const linkFor = (token: string): string => `${location.origin}/?key=${token}`

function status(g: Grant): string {
  if (g.revokedAt) return 'revoked'
  if (g.disabledAt) return 'disabled'
  if (g.expiresAt && g.expiresAt * 1000 < Date.now()) return 'expired'
  if (g.maxRedeems != null && g.redeems >= g.maxRedeems) return 'exhausted'
  return 'active'
}

export function AccessPage() {
  const { whoami } = useWhoami()
  const qc = useQueryClient()
  const [name, setName] = useState('')
  const [days, setDays] = useState('')
  const [maxRedeems, setMaxRedeems] = useState('')
  const [minted, setMinted] = useState<Minted | null>(null)
  const [confirming, setConfirming] = useState<string | null>(null)

  const grants = useQuery({
    queryKey: ['auth', 'grants'],
    queryFn: () => api<{ grants: Grant[] }>('/api/auth/grants'),
    enabled: isAdmin(whoami),
    retry: false,
  })
  const log = useQuery({
    queryKey: ['auth', 'log'],
    queryFn: () => api<{ events: AccessEvent[] }>('/api/auth/log?limit=200'),
    enabled: isAdmin(whoami),
    retry: false,
  })
  const invalidate = () => qc.invalidateQueries({ queryKey: ['auth'] })
  const mint = useMutation({
    mutationFn: () =>
      api<Minted>('/api/auth/grants', {
        name: name.trim(),
        scopes: ['view'],
        ...(days.trim() ? { expiresInS: parseInt(days, 10) * 86_400 } : {}),
        ...(maxRedeems.trim() ? { maxRedeems: parseInt(maxRedeems, 10) } : {}),
      }),
    onSuccess: m => {
      setMinted(m)
      setName(''); setDays(''); setMaxRedeems('')
      invalidate()
    },
  })
  const revoke = useMutation({
    mutationFn: (id: string) => api(`/api/auth/grants/${id}/revoke`, {}),
    onSuccess: invalidate,
  })

  if (whoami === undefined) return <p className="dim">loading…</p>
  if (whoami === null) return <Alert severity="info">Admin only — <a href={ssoUrl('/access')}>sign in</a>.</Alert>
  if (!isAdmin(whoami)) return <Alert severity="warning">Admin only (this link isn't one).</Alert>
  if (grants.error) {
    const e = grants.error
    return <Alert severity="error">{e instanceof ApiError ? `error ${e.status}: ${e.message}` : String(e)}</Alert>
  }
  const names = new Map((grants.data?.grants ?? []).map(g => [g.id, g.name ?? g.id]))

  return (
    <Box className="access" sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
      <Typography variant="h6">Access</Typography>
      <Typography variant="body2">
        Share links sign a browser into this deployment as a <code>view</code>-only visitor. Name each one after
        the person you're sending it to; the token is shown once, at mint. Revoking kills its sessions on their
        next request.
      </Typography>
      <Box
        component="form"
        onSubmit={e => { e.preventDefault(); if (name.trim()) mint.mutate() }}
        sx={{ display: 'flex', gap: 1, alignItems: 'center', flexWrap: 'wrap' }}
      >
        <TextField size="small" label="name (who is this for?)" value={name} onChange={e => setName(e.target.value)} sx={{ minWidth: 260 }} />
        <TextField size="small" label="expires (days)" value={days} onChange={e => setDays(e.target.value)} sx={{ width: 130 }} />
        <TextField size="small" label="max opens" value={maxRedeems} onChange={e => setMaxRedeems(e.target.value)} sx={{ width: 110 }} />
        <Button type="submit" variant="contained" size="small" disabled={!name.trim() || mint.isPending}>mint</Button>
        {mint.error && <span className="error">{String(mint.error)}</span>}
      </Box>
      {minted && (
        <Alert severity="success" onClose={() => setMinted(null)}>
          <b>{minted.grant.name}</b> — copy this link now; it won't be shown again:
          <Box sx={{ display: 'flex', gap: 1, alignItems: 'center', mt: 1 }}>
            <code style={{ wordBreak: 'break-all' }}>{linkFor(minted.token)}</code>
            <Button size="small" onClick={() => navigator.clipboard.writeText(linkFor(minted.token))}>copy</Button>
          </Box>
        </Alert>
      )}

      <table className="access-table">
        <thead>
          <tr>
            <th>name</th><th>created</th><th>expires</th><th className="num">opens</th><th>last used</th><th>status</th><th></th>
          </tr>
        </thead>
        <tbody>
          {(grants.data?.grants ?? []).map(g => (
            <tr key={g.id} className={status(g) === 'active' ? '' : 'dim'}>
              <td><b>{g.name}</b></td>
              <td title={`by ${g.createdBy}`}>{day(g.createdAt)}</td>
              <td>{day(g.expiresAt)}</td>
              <td className="num">{g.redeems}{g.maxRedeems != null ? ` / ${g.maxRedeems}` : ''}</td>
              <td>{minute(g.lastUsedAt)}</td>
              <td>{status(g)}</td>
              <td>
                {status(g) === 'active' && (confirming === g.id ? (
                  <>
                    <Button size="small" color="error" onClick={() => { revoke.mutate(g.id); setConfirming(null) }}>confirm revoke</Button>
                    <Button size="small" onClick={() => setConfirming(null)}>keep</Button>
                  </>
                ) : (
                  <Button size="small" onClick={() => setConfirming(g.id)}>revoke</Button>
                ))}
              </td>
            </tr>
          ))}
        </tbody>
      </table>

      <Typography variant="subtitle2">Recent access</Typography>
      <table className="access-table">
        <thead>
          <tr><th>when</th><th>event</th><th>who</th><th>path</th><th>country</th><th>detail</th></tr>
        </thead>
        <tbody>
          {(log.data?.events ?? []).map((e, i) => (
            <tr key={i}>
              <td>{minute(e.ts)}</td>
              <td>{e.event}</td>
              <td>{e.grantId ? names.get(e.grantId) ?? e.grantId : e.sessionSub?.replace(/^e:/, '') ?? ''}</td>
              <td><code>{e.path ?? ''}</code></td>
              <td>{e.country ?? ''}</td>
              <td>{e.reason ?? ''}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </Box>
  )
}
