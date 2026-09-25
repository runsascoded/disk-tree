/** `/staged` — the staged-delete queue (spec `specs/staged-delete.md`, CP3;
 *  page UX per `specs/done/staged-page-ux.md`).
 *  Everyone who can see it sees the staged sets + the runs feed; on a gated
 *  deployment only an admin can dispatch (the local server has no gate, so its
 *  single user can). Dispatch enqueues on the cloud edge, deletes inline on the
 *  Flask peer — either way the plan closes and a run is recorded. The Flask
 *  peer also deletes one item at a time (`uris`) and previews (dry run). */
import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Alert, Box, Button, Tooltip, Typography } from '@mui/material'
import { dispatchPlan, fetchStaged, unstageUris } from '../api'
import type { DispatchResult, StagedItem, StagedPlan, StagedRun } from '../api'
import { useCapabilities } from '../hooks/useCapabilities'
import { isAdmin, ssoUrl, useWhoami } from '../auth'
import { formatSize } from '../utils/format'
import { commonDir, elideMiddle } from '../staged'

const minute = (s: number | null | undefined): string =>
  s == null ? '—' : new Date(s * 1000).toISOString().slice(0, 16).replace('T', ' ') + 'Z'

function runState(r: StagedRun): string {
  if (r.finished_ts == null) return 'enqueued'
  return r.mode === 'real' ? 'done' : 'dry'
}

const sum = (items: StagedItem[], k: 'bytes' | 'objects'): number | null =>
  items.some(i => i[k] != null) ? items.reduce((a, i) => a + (i[k] ?? 0), 0) : null

/** A row's path: relative to the plan's shared dir, middle-elided, full URI on
 *  hover / long-press, copied on click. */
function PathCell({ uri, base }: { uri: string; base: string }) {
  const [copied, setCopied] = useState(false)
  const rel = uri.startsWith(base) ? uri.slice(base.length) : uri
  const copy = () => {
    navigator.clipboard?.writeText(uri).then(() => { setCopied(true); setTimeout(() => setCopied(false), 1200) })
  }
  return (
    <Tooltip title={copied ? 'copied' : uri} placement="top" arrow enterTouchDelay={0} leaveTouchDelay={3000}>
      <code className="staged-path" onClick={copy}>{elideMiddle(rel)}</code>
    </Tooltip>
  )
}

const scope = (r: DispatchResult): string =>
  `${formatSize(r.bytes ?? r.deleted_bytes ?? 0)} across ${r.objects ?? r.deleted_objects ?? 0} object${(r.objects ?? r.deleted_objects) === 1 ? '' : 's'}`

export function StagedPage() {
  const caps = useCapabilities()
  const { enabled, whoami } = useWhoami()
  const qc = useQueryClient()
  const [confirming, setConfirming] = useState<number | null>(null)
  const [confirmingUri, setConfirmingUri] = useState<string | null>(null)
  const [preview, setPreview] = useState<{ plan: number; result: DispatchResult } | null>(null)

  // Gated deployments need admin to dispatch; the local server (no gate) lets
  // its single user dispatch.
  const canDispatch = enabled === false || isAdmin(whoami)
  // One-at-a-time deletes and dry runs exist on the Flask peer only: the edge
  // enqueues whole plans for a server-side executor (and a cloud function can't
  // reach a laptop's paths anyway).
  const perItem = canDispatch && caps?.static === false

  const staged = useQuery({ queryKey: ['staged'], queryFn: fetchStaged, enabled: caps?.stageDelete === true, retry: false })
  const invalidate = () => qc.invalidateQueries({ queryKey: ['staged'] })
  const unstage = useMutation({ mutationFn: (uris: string[]) => unstageUris(uris), onSuccess: invalidate })
  const dispatch = useMutation({
    mutationFn: (plan: number) => dispatchPlan({ plan: String(plan) }),
    onSuccess: () => { setPreview(null); invalidate() },
  })
  const deleteOne = useMutation({
    mutationFn: ({ plan, uri }: { plan: number; uri: string }) => dispatchPlan({ plan: String(plan), uris: [uri] }),
    onSuccess: () => { setPreview(null); invalidate() },
  })
  const dryRun = useMutation({
    mutationFn: (plan: number) => dispatchPlan({ plan: String(plan), forReal: false }),
    onSuccess: (result, plan) => { setPreview({ plan, result }); invalidate() },
  })

  if (caps === undefined) return <p className="dim">loading…</p>
  if (!caps.stageDelete) return <Alert severity="info">This deployment has no staged-delete queue.</Alert>
  if (enabled && whoami === null) return <Alert severity="info">Sign in to see the queue — <a href={ssoUrl('/staged')}>sign in</a>.</Alert>
  if (staged.error) return <Alert severity="error">{String(staged.error)}</Alert>

  const plans = staged.data?.plans ?? []
  const runs = staged.data?.runs ?? []
  const error = dispatch.error ?? deleteOne.error ?? dryRun.error

  return (
    <Box className="staged" sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
      <Typography variant="h6">Staged for deletion</Typography>
      <Typography variant="body2">
        Nothing here is deleted until it's dispatched — staging is opt-in, with no deadline. Stage paths from a
        bucket's listing (the trash icon); {canDispatch ? 'dispatch a plan to delete it' : 'an admin dispatches a plan to delete it'}
        {perItem ? ', or delete items one at a time.' : '.'}
      </Typography>

      {plans.length === 0 && <Alert severity="info">No open plans. Stage a path to start one.</Alert>}

      {plans.map((p: StagedPlan) => {
        const uris = p.items.map(i => i.uri)
        const base = commonDir(uris)
        const bytes = sum(p.items, 'bytes')
        const sized = bytes != null
        const busy = dispatch.isPending || deleteOne.isPending || dryRun.isPending
        return (
          <Box key={p.id} sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
            <Box sx={{ display: 'flex', gap: 1, alignItems: 'center', flexWrap: 'wrap' }}>
              <Typography variant="subtitle2">
                Plan {p.id} “{p.name}” — {p.items.length} item{p.items.length === 1 ? '' : 's'}{sized ? `, ${formatSize(bytes)}` : ''}
              </Typography>
              <span className="dim" style={{ fontSize: '0.8rem' }}>by {p.created_by}</span>
              {canDispatch && p.items.length > 0 && (confirming === p.id ? (
                <>
                  <Button size="small" color="error" variant="contained" disabled={busy}
                    onClick={() => { dispatch.mutate(p.id); setConfirming(null) }}>
                    confirm — delete {p.items.length}{sized ? ` (${formatSize(bytes)})` : ''}
                  </Button>
                  <Button size="small" onClick={() => setConfirming(null)}>cancel</Button>
                </>
              ) : (
                <>
                  {perItem && <Button size="small" disabled={busy} onClick={() => dryRun.mutate(p.id)}>preview</Button>}
                  <Button size="small" color="error" disabled={busy} onClick={() => setConfirming(p.id)}>dispatch</Button>
                </>
              ))}
            </Box>
            {preview?.plan === p.id && (
              <Alert severity="info" onClose={() => setPreview(null)}>
                dry run {preview.result.run_id}: would delete {scope(preview.result)} — nothing was removed
              </Alert>
            )}
            {base && <div className="dim staged-base">under <code>{base}</code></div>}
            <table className="access-table staged-table">
              <thead>
                <tr>
                  <th>path</th>
                  {sized && <th className="num col-size">size</th>}
                  {canDispatch && <th className="col-actions"></th>}
                </tr>
              </thead>
              <tbody>
                {p.items.map(it => (
                  <tr key={it.uri}>
                    <td><PathCell uri={it.uri} base={base} /></td>
                    {sized && <td className="num col-size">{formatSize(it.bytes ?? 0)}</td>}
                    {canDispatch && (
                      <td className="col-actions">
                        {perItem && (confirmingUri === it.uri ? (
                          <>
                            <Button size="small" color="error" variant="contained" disabled={busy}
                              onClick={() => { deleteOne.mutate({ plan: p.id, uri: it.uri }); setConfirmingUri(null) }}>
                              confirm
                            </Button>
                            <Button size="small" onClick={() => setConfirmingUri(null)}>cancel</Button>
                          </>
                        ) : (
                          <Button size="small" color="error" disabled={busy} onClick={() => setConfirmingUri(it.uri)}>delete</Button>
                        ))}
                        {confirmingUri !== it.uri && (
                          <Button size="small" disabled={unstage.isPending} onClick={() => unstage.mutate([it.uri])}>unstage</Button>
                        )}
                      </td>
                    )}
                  </tr>
                ))}
              </tbody>
            </table>
          </Box>
        )
      })}
      {error && <Alert severity="error">{String(error)}</Alert>}

      <Typography variant="subtitle2">Recent runs</Typography>
      {runs.length === 0 ? <span className="dim">none yet</span> : (
        <table className="access-table">
          <thead><tr><th>started</th><th>state</th><th>who</th><th className="num">bytes</th><th className="num">objects</th></tr></thead>
          <tbody>
            {runs.map(r => (
              <tr key={r.run_id}>
                <td>{minute(r.started_ts)}</td>
                <td>{runState(r)}</td>
                <td>{r.actor}</td>
                <td className="num">{formatSize(r.deleted_bytes)}</td>
                <td className="num">{r.deleted_objects}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </Box>
  )
}
