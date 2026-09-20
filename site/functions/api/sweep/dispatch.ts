// POST /api/sweep/dispatch — launch a sweep executor run on GCP Batch from
// the /sweep console (specs/sweep-executor.md, "web dispatch bridge").
//
// Body: { mode: 'dry' | 'real', date: 'YYYY-MM-DD', buckets?: string[] }.
// Admin scope only. The submitted job runs the daily-snapshot image with the
// entrypoint overridden to `sweep manifest -S` (consuming the console's
// `sweep_approvals` sign-offs) followed by `sweep execute` — which re-lists,
// generation-matches, records to D1 (`deletion_runs`/`deletion_bands`, so the
// run surfaces in the console within its refetch window), and for `real`
// requires ≥7d soft delete on every bucket before deleting anything.
//
// Auth to GCP: `_lib/gcp.ts` (the `GCP_SA_KEY` Pages secret — a dedicated SA
// that can submit Batch jobs and act as the job SA, and nothing else).
import { ADMIN_SCOPE, type Env as AuthEnv, json, requireScope } from '../../_lib/auth.js'
import { GCP_PROJECT, batchJobsUrl, batchRegionFor, gcpToken } from '../../_lib/gcp.js'

interface Env extends AuthEnv {
  GCP_SA_KEY?: string
}

const PROJECT = GCP_PROJECT
const IMAGE = `us-central1-docker.pkg.dev/${PROJECT}/cloud-run-source-deploy/gcs-usage-snapshot:latest`
const JOB_SA = `gcs-usage-job@${PROJECT}.iam.gserviceaccount.com`
const CF_ACCOUNT_ID = '74981a43be0de7712369306c7b19133d'
const SECRET = (name: string) => `projects/${PROJECT}/secrets/${name}/versions/latest`

export const onRequestPost = async (ctx: { request: Request; env: Env }): Promise<Response> => {
  const gated = await requireScope(ctx, ADMIN_SCOPE)
  if (gated instanceof Response) return gated
  if (!ctx.env.GCP_SA_KEY) return json({ error: 'dispatch not configured (GCP_SA_KEY secret missing)' }, 503)

  const body = (await ctx.request.json().catch(() => null)) as
    | { mode?: string; date?: string; buckets?: string[] } | null
  const mode = body?.mode
  const date = body?.date
  if (mode !== 'dry' && mode !== 'real') return json({ error: "mode must be 'dry' or 'real'" }, 400)
  if (!date || !/^\d{4}-\d{2}-\d{2}$/.test(date)) return json({ error: 'date must be YYYY-MM-DD (the plan scan)' }, 400)
  const buckets = body?.buckets ?? []
  if (buckets.some(b => !/^marin-[a-z0-9-]+$/.test(b))) return json({ error: 'bad bucket name' }, 400)
  const region = batchRegionFor(buckets)

  const ts = new Date().toISOString().replace(/[-:]/g, '').slice(0, 15).replace('T', '-').toLowerCase()
  const jobId = `gcs-sweep-${mode}-${ts}z`
  const plan = `gs://oa-gcs-usage-dvx/sweep/runs/${jobId}`
  const bflags = buckets.map(b => `-b ${b}`).join(' ')
  const script = [
    'set -euo pipefail',
    `dt-cloud sweep manifest -d "$SWEEP_DATE" -S ${bflags} -o "${plan}"`,
    `dt-cloud sweep execute ${bflags} ${mode === 'real' ? '--for-real ' : ''}"${plan}"`,
  ].join('\n')

  const spec = {
    taskGroups: [{
      taskCount: 1,
      taskSpec: {
        runnables: [{ container: { imageUri: IMAGE, entrypoint: '/bin/bash', commands: ['-c', script] } }],
        computeResource: { cpuMilli: 8000, memoryMib: 60000 },
        maxRetryCount: 0,
        // 72 h: the 35M-object bucket needs ~10 h of deletes at the bucket's
        // write ceiling on top of its listing; 4 h (the old cap) fit only east5.
        maxRunDuration: '259200s',
        environment: {
          variables: {
            SWEEP_DATE: date,
            // `sweep execute` records the run's `actor` from $USER
            USER: gated.email ?? 'sweep-console',
            CLOUDFLARE_ACCOUNT_ID: CF_ACCOUNT_ID,
          },
          secretVariables: {
            GCS_USAGE_TOKEN: SECRET('gcs-sheet-sync-token'),
            CLOUDFLARE_API_TOKEN: SECRET('cf-pages-token'),
          },
        },
      },
    }],
    allocationPolicy: {
      instances: [{ policy: { machineType: 'n2-highmem-8', bootDisk: { type: 'pd-balanced', sizeGb: '100' } } }],
      serviceAccount: { email: JOB_SA },
      location: { allowedLocations: [`regions/${region}`] },
    },
    logsPolicy: { destination: 'CLOUD_LOGGING' },
  }

  const token = await gcpToken(ctx.env.GCP_SA_KEY)
  const r = await fetch(
    `${batchJobsUrl(region)}?job_id=${jobId}`,
    { method: 'POST', headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' }, body: JSON.stringify(spec) },
  )
  const text = await r.text()
  let out: unknown = {}
  try { out = JSON.parse(text) } catch { out = { body: text.slice(0, 1000) } }
  // 500, not 502: Cloudflare swaps an origin 502 for its own branded error
  // page, which threw away this detail on the 2026-09-11 17:20Z dispatch.
  if (!r.ok) {
    console.error('batch submit failed', r.status, text.slice(0, 2000))
    return json({ error: `batch submit failed (${r.status})`, status: r.status, detail: out }, 500)
  }
  return json({ job_id: jobId, mode, date, plan, region, by: gated.email })
}
