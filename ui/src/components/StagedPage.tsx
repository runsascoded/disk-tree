/** `/staged` — the staged-delete queue (spec `specs/staged-delete.md`, CP3).
 *  Everyone who can see it sees the staged sets + the runs feed; on a gated
 *  deployment only an admin can dispatch (the local server has no gate, so its
 *  single user can). Dispatch enqueues on the cloud edge, deletes inline on the
 *  Flask peer — either way the plan closes and a run is recorded. */
import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Alert, Box, Button, Typography } from '@mui/material'
import { dispatchPlan, fetchStaged, unstageUris } from '../api'
import type { StagedPlan, StagedRun } from '../api'
import { useCapabilities } from '../hooks/useCapabilities'
import { isAdmin, ssoUrl, useWhoami } from '../auth'
import { formatSize } from '../utils/format'

const minute = (s: number | null | undefined): string =>
  s == null ? '—' : new Date(s * 1000).toISOString().slice(0, 16).replace('T', ' ') + 'Z'

function runState(r: StagedRun): string {
  if (r.finished_ts == null) return 'enqueued'
  return r.mode === 'real' ? 'done' : 'dry'
}

export function StagedPage() {
  const caps = useCapabilities()
  const { enabled, whoami } = useWhoami()
  const qc = useQueryClient()
  const [confirming, setConfirming] = useState<number | null>(null)

  // Gated deployments need admin to dispatch; the local server (no gate) lets
  // its single user dispatch.
  const canDispatch = enabled === false || isAdmin(whoami)

  const staged = useQuery({ queryKey: ['staged'], queryFn: fetchStaged, enabled: caps?.stageDelete === true, retry: false })
  const invalidate = () => qc.invalidateQueries({ queryKey: ['staged'] })
  const unstage = useMutation({ mutationFn: (uris: string[]) => unstageUris(uris), onSuccess: invalidate })
  const dispatch = useMutation({ mutationFn: (plan: number) => dispatchPlan(String(plan)), onSuccess: invalidate })

  if (caps === undefined) return <p className="dim">loading…</p>
  if (!caps.stageDelete) return <Alert severity="info">This deployment has no staged-delete queue.</Alert>
  if (enabled && whoami === null) return <Alert severity="info">Sign in to see the queue — <a href={ssoUrl('/staged')}>sign in</a>.</Alert>
  if (staged.error) return <Alert severity="error">{String(staged.error)}</Alert>

  const plans = staged.data?.plans ?? []
  const runs = staged.data?.runs ?? []

  return (
    <Box className="staged" sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
      <Typography variant="h6">Staged for deletion</Typography>
      <Typography variant="body2">
        Nothing here is deleted until it's dispatched — staging is opt-in, with no deadline. Stage paths from a
        bucket's listing (the trash icon); {canDispatch ? 'dispatch a plan to delete it.' : 'an admin dispatches a plan to delete it.'}
      </Typography>

      {plans.length === 0 && <Alert severity="info">No open plans. Stage a path to start one.</Alert>}

      {plans.map((p: StagedPlan) => (
        <Box key={p.id} sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
          <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
            <Typography variant="subtitle2">
              Plan {p.id} “{p.name}” — {p.items.length} item{p.items.length === 1 ? '' : 's'}
            </Typography>
            <span className="dim" style={{ fontSize: '0.8rem' }}>by {p.created_by}</span>
            {canDispatch && p.items.length > 0 && (confirming === p.id ? (
              <>
                <Button size="small" color="error" variant="contained" disabled={dispatch.isPending}
                  onClick={() => { dispatch.mutate(p.id); setConfirming(null) }}>
                  confirm — delete {p.items.length}
                </Button>
                <Button size="small" onClick={() => setConfirming(null)}>cancel</Button>
              </>
            ) : (
              <Button size="small" color="error" onClick={() => setConfirming(p.id)}>dispatch</Button>
            ))}
          </Box>
          <table className="access-table">
            <thead><tr><th>uri</th>{canDispatch && <th></th>}</tr></thead>
            <tbody>
              {p.items.map(uri => (
                <tr key={uri}>
                  <td><code style={{ wordBreak: 'break-all' }}>{uri}</code></td>
                  {canDispatch && (
                    <td className="col-action">
                      <Button size="small" disabled={unstage.isPending} onClick={() => unstage.mutate([uri])}>unstage</Button>
                    </td>
                  )}
                </tr>
              ))}
            </tbody>
          </table>
        </Box>
      ))}
      {dispatch.error && <Alert severity="error">{String(dispatch.error)}</Alert>}

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
