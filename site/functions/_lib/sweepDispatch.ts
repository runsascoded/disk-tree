// The `sweep` executor (gcs's bridge; `_lib/executor.ts` is the seam,
// specs/done/staged-slack.md), and its pure parts: the run dir, the `-b` cut a plan
// allows, and the executor script over the staged set (`sweep manifest
// --plan`: specs/staged-delete.md).
//
// The plan's items are the delete set: they are snapshotted into `plan.json`
// in the run dir (a gcs plan MAY span buckets), and the submitted job runs the
// daily-snapshot image with the entrypoint overridden to `sweep manifest
// --plan` followed by `sweep execute` — which re-lists, generation-matches,
// records to D1 (`deletion_runs`/`deletion_bands`; `plan_id` comes from the
// plan.json — no row is inserted here), and for `real` requires ≥7d soft
// delete on every bucket before deleting anything. The `-b` cut is the plan's
// buckets (∩ `buckets`, when given). The run's item digest rides in the job
// env (`PLAN_DIGEST`); `_lib/sweepReflect.ts` copies it onto the run's row once
// the executor has recorded it finished.
//
// Auth to GCP: `_lib/gcp.ts` (the `GCP_SA_KEY` Pages secret — a dedicated SA
// that can submit Batch jobs, act as the job SA, and write the plan.json into
// the data bucket).
import type { D1Database } from '@cloudflare/workers-types'
import { type BatchConfig, batchConfig, notConfigured } from './batchConfig.js'
import { type DispatchReq, type ExecEnv, type Executor, type Prepared, refuse } from './dispatch.js'
import { batchJobsUrl, batchRegionFor, gcpToken } from './gcp.js'
import { bucketOf, NO_SHAPE, type PlanBucketsSnapshot, prefixShape, snapshotPlanBuckets } from './plans.js'
import { listSweepJobs, reflectSweepRuns } from './sweepReflect.js'

/** A run's dir in the data bucket (`DATA_BUCKET`). */
export const runDir = (cfg: Pick<BatchConfig, 'dataBucket'>, jobId: string): string => `gs://${cfg.dataBucket}/sweep/runs/${jobId}`
export const planJsonPath = (cfg: Pick<BatchConfig, 'dataBucket'>, jobId: string): string => `${runDir(cfg, jobId)}/plan.json`
/** `plan.json`'s object name in the data bucket (the JSON upload API's `name`). */
export const planJsonObject = (jobId: string): string => `sweep/runs/${jobId}/plan.json`

/** The buckets a plan-sourced run touches: the plan's, cut to `requested`
 * when the body names any. Empty = the request names none of the plan's. */
export const bucketCut = (planBuckets: readonly string[], requested: readonly string[]): string[] =>
  requested.length ? planBuckets.filter(b => requested.includes(b)) : [...planBuckets]

export interface SweepScript {
  cfg: Pick<BatchConfig, 'dataBucket'>
  mode: 'dry' | 'real'
  jobId: string
  buckets: readonly string[]
  /** The run's plan.json — the staged set the manifest reads. */
  plan: string
}

/** The Batch container's bash: manifest then execute, both against the run
 * dir. On exit (success or failure) it pings the site's `/api/sweep/jobs`
 * with the job's read grant, so the finished run is reflected — and its
 * result posted to the plan's Slack thread — without anyone polling. */
export const sweepScript = ({ cfg, mode, jobId, buckets, plan }: SweepScript): string => {
  const run = runDir(cfg, jobId)
  const bflags = buckets.map(b => `-b ${b}`).join(' ')
  return [
    'set -euo pipefail',
    `trap 'curl -fsS -m 60 -o /dev/null -H "Authorization: Bearer $SITE_TOKEN" "$SITE_URL/api/sweep/jobs" || true' EXIT`,
    `dt-cloud sweep manifest -d "$SWEEP_DATE" --plan "${plan}" ${bflags} -o "${run}"`,
    `dt-cloud sweep execute ${bflags} ${mode === 'real' ? '--for-real ' : ''}"${run}"`,
  ].join('\n')
}

async function prepare(env: ExecEnv, db: D1Database, req: DispatchReq): Promise<Prepared | ReturnType<typeof refuse>> {
  if (!env.GCP_SA_KEY) return refuse(503, 'dispatch not configured (GCP_SA_KEY secret missing)')
  if (!env.JOB_SA) return refuse(503, 'dispatch not configured (JOB_SA var missing)')
  const jobSa = env.JOB_SA
  const cfg = batchConfig(env, ['GCP_PROJECT', 'DATA_BUCKET', 'SWEEP_IMAGE', 'CF_ACCOUNT_ID'])
  if ('missing' in cfg) return refuse(503, notConfigured('dispatch', cfg.missing))
  const shape = prefixShape(env)
  if (!shape) return refuse(503, `dispatch ${NO_SHAPE}`)
  const SECRET = (name: string) => `projects/${cfg.project}/secrets/${name}/versions/latest`
  const requested = req.buckets ?? []
  // A cut names only scanned buckets (`STORE_BUCKETS`).
  if (requested.some(b => !shape.buckets.includes(b))) return refuse(400, 'bad bucket name')
  const snapshot: PlanBucketsSnapshot | null = await snapshotPlanBuckets(db, req.planId, shape)
  if (!snapshot) return refuse(404, 'no such plan')
  if (!snapshot.sweep.length) return refuse(400, 'plan has no items to sweep')
  const buckets = bucketCut(snapshot.buckets, requested)
  if (!buckets.length) return refuse(400, 'buckets name none of the plan\'s', { plan_buckets: snapshot.buckets })
  // The run acts on the items in its cut: that set is what the digest names.
  const prefixes = snapshot.sweep.filter(p => buckets.includes(bucketOf(p, shape.buckets)))

  const launch: Prepared['launch'] = async (date, digest) => {
    const region = batchRegionFor(cfg, buckets)
    const ts = new Date().toISOString().replace(/[-:]/g, '').slice(0, 15).replace('T', '-').toLowerCase()
    const jobId = `gcs-sweep-${req.mode}-${ts}z`
    const plan = runDir(cfg, jobId)
    const script = sweepScript({ cfg, mode: req.mode, jobId, buckets, plan: planJsonPath(cfg, jobId) })

    const spec = {
      taskGroups: [{
        taskCount: 1,
        taskSpec: {
          runnables: [{ container: { imageUri: cfg.image, entrypoint: '/bin/bash', commands: ['-c', script] } }],
          computeResource: { cpuMilli: 8000, memoryMib: 60000 },
          maxRetryCount: 0,
          // 72 h: the 35M-object bucket needs ~10 h of deletes at the bucket's
          // write ceiling on top of its listing; 4 h (the old cap) fit only east5.
          maxRunDuration: '259200s',
          environment: {
            variables: {
              SWEEP_DATE: date,
              // `sweep execute` records the run's `actor` from $USER
              USER: req.actor,
              CLOUDFLARE_ACCOUNT_ID: cfg.cfAccountId,
              // `sweep manifest`'s listing root (`gs://$DATA_BUCKET`): dt-cloud
              // has no default bucket (specs/oa-decoupling.md steps 9–10)
              DATA_BUCKET: cfg.dataBucket,
              // the run's item digest, for `sweepReflect` (the executor ignores it)
              PLAN_DIGEST: digest,
              SITE_URL: req.siteUrl,
            },
            secretVariables: {
              SITE_TOKEN: SECRET('gcs-sheet-sync-token'),
              CLOUDFLARE_API_TOKEN: SECRET('cf-pages-token'),
            },
          },
        },
      }],
      allocationPolicy: {
        instances: [{ policy: { machineType: 'n2-highmem-8', bootDisk: { type: 'pd-balanced', sizeGb: '100' } } }],
        serviceAccount: { email: jobSa },
        location: { allowedLocations: [`regions/${region}`] },
      },
      logsPolicy: { destination: 'CLOUD_LOGGING' },
    }

    const token = await gcpToken(env.GCP_SA_KEY!)
    // Drop plan.json into the run dir; the executor reads it back over gs://.
    const up = await fetch(
      `https://storage.googleapis.com/upload/storage/v1/b/${cfg.dataBucket}/o?uploadType=media&name=${encodeURIComponent(planJsonObject(jobId))}`,
      { method: 'POST', headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' }, body: JSON.stringify(snapshot) },
    )
    if (!up.ok) return refuse(500, 'plan.json write failed', { status: up.status, detail: (await up.text()).slice(0, 300) })
    const r = await fetch(
      `${batchJobsUrl(cfg, region)}?job_id=${jobId}`,
      { method: 'POST', headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' }, body: JSON.stringify(spec) },
    )
    const text = await r.text()
    let out: unknown = {}
    try { out = JSON.parse(text) } catch { out = { body: text.slice(0, 1000) } }
    // 500, not 502: Cloudflare swaps an origin 502 for its own branded error
    // page, which threw away this detail on the 2026-09-11 17:20Z dispatch.
    if (!r.ok) {
      console.error('batch submit failed', r.status, text.slice(0, 2000))
      return refuse(500, `batch submit failed (${r.status})`, { status: r.status, detail: out })
    }
    return { job_id: jobId, extra: { plan, region, buckets } }
  }
  return { prefixes, launch }
}

export const sweep: Executor = {
  dateRe: /^\d{4}-\d{2}-\d{2}$/,
  dateHint: 'YYYY-MM-DD',
  prepare,
  async refresh(env, db) {
    if (!env.GCP_SA_KEY) return []
    const cfg = batchConfig(env, ['GCP_PROJECT', 'DATA_BUCKET'])
    if ('missing' in cfg) return []
    return reflectSweepRuns(cfg, db, await listSweepJobs(cfg, await gcpToken(env.GCP_SA_KEY)))
  },
}
