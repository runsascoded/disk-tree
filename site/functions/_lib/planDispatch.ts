/**
 * Launch a plan-first deletion run on GCP Batch (cw's executor) — shared by
 * `/api/plan-sweep/dispatch` (the /staged console) and `/slack/actions`
 * (specs/staged-slack.md). Snapshots the plan into plan.json on GCS, submits
 * the Batch job, records an in-progress `deletion_runs` row carrying the
 * plan's item digest (what a later real run from Slack is checked against).
 *
 * The job's exit trap pings `<site>/api/plan-sweep/jobs` with the job's read
 * grant (`cw-s3-job-grant`), so the run summary is reflected into D1 — and its
 * result posted to the plan's Slack thread — the moment the executor finishes,
 * not when someone next opens /staged.
 */
import type { D1Database } from '@cloudflare/workers-types'
import { gcpToken } from './gcp.js'
import { DATA_BUCKET, jobStamp, runGsPath, runMountPath, secretRef, submitBatch, sweepBatchSpec } from './cwBatch.js'
import { PlanSpansBuckets, snapshotPlan } from './plans.js'
import { planDigest } from './slack.js'

export const DATE_RE = /^\d{4}-\d{2}-\d{2}(?:T\d{4})?$/

export type DispatchResult =
  | { ok: true; job_id: string; plan_id: number; mode: 'dry' | 'real'; date: string; run: string; digest: string }
  | { ok: false; status: number; error: string; detail?: unknown }

export async function dispatchPlanSweep(
  env: { DB?: D1Database; GCP_SA_KEY?: string },
  o: { planId: number; mode: 'dry' | 'real'; date: string; actor: string; siteUrl: string },
): Promise<DispatchResult> {
  if (!env.DB) return { ok: false, status: 503, error: 'plans store not configured (no D1 binding)' }
  if (!env.GCP_SA_KEY) return { ok: false, status: 503, error: 'dispatch not configured (GCP_SA_KEY secret missing)' }
  if (!DATE_RE.test(o.date)) return { ok: false, status: 400, error: 'date must be a scan id (YYYY-MM-DD[THHMM])' }
  const db = env.DB

  let snapshot: Awaited<ReturnType<typeof snapshotPlan>>
  try {
    snapshot = await snapshotPlan(db, o.planId)
  } catch (e) {
    // one bucket per run: a plan naming several is refused, not split
    if (e instanceof PlanSpansBuckets) return { ok: false, status: 400, error: e.message, detail: { buckets: e.buckets } }
    throw e
  }
  if (!snapshot) return { ok: false, status: 404, error: 'no such plan' }
  if (!snapshot.sweep.length) return { ok: false, status: 400, error: 'plan has no items to sweep' }
  const items = (await db.prepare('SELECT prefix FROM plan_items WHERE plan_id = ?').bind(o.planId).all<{ prefix: string }>()).results
  const digest = await planDigest(items.map(i => i.prefix))

  const jobId = `cw-sweep-${o.mode}-${jobStamp()}z`
  const runGs = runGsPath(jobId)
  const runMnt = runMountPath(jobId)
  const token = await gcpToken(env.GCP_SA_KEY)

  // Drop plan.json into the run dir (the executor reads it via the FUSE mount).
  const planObj = encodeURIComponent(`sweep/cw/runs/${jobId}/plan.json`)
  const up = await fetch(
    `https://storage.googleapis.com/upload/storage/v1/b/${DATA_BUCKET}/o?uploadType=media&name=${planObj}`,
    { method: 'POST', headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' }, body: JSON.stringify(snapshot) },
  )
  if (!up.ok) return { ok: false, status: 500, error: 'plan.json write failed', detail: { status: up.status, body: (await up.text()).slice(0, 300) } }

  const script = [
    'set -euo pipefail',
    // Ping the site on exit (success or failure) so it reflects the run now.
    `trap 'curl -fsS -m 60 -o /dev/null -H "Authorization: Bearer $GCS_USAGE_TOKEN" "$SITE_URL/api/plan-sweep/jobs" || true' EXIT`,
    `RUN="${runMnt}"`,
    `dt-cloud plan-sweep manifest --plan "$RUN/plan.json" -d "$SWEEP_DATE" -o "$RUN"`,
    `dt-cloud plan-sweep execute ${o.mode === 'real' ? '--for-real ' : ''}"$RUN"`,
  ].join('\n')

  // the executor deletes from the plan's bucket (`CW_BUCKET` in the job env)
  const spec = sweepBatchSpec(script, { JOB_ID: jobId, SWEEP_DATE: o.date, CW_BUCKET: snapshot.bucket, SITE_URL: o.siteUrl },
    { GCS_USAGE_TOKEN: secretRef('cw-s3-job-grant') })
  const { ok, status, text } = await submitBatch(token, jobId, spec)
  if (!ok) {
    console.error('batch submit failed', status, text.slice(0, 2000))
    let detail: unknown = { body: text.slice(0, 1000) }
    try { detail = JSON.parse(text) } catch { /* keep raw */ }
    return { ok: false, status: 500, error: `batch submit failed (${status})`, detail }
  }

  // `head` / `exec_head` recorded a ledger position a plan-first run doesn't
  // have (it reads no ledger), so both are 0.
  await db.prepare(`
    INSERT INTO deletion_runs (run_id, plan_id, manifest, scan, head, exec_head, actor, mode, started_ts, log_dir, plan_digest)
    VALUES (?, ?, ?, ?, 0, 0, ?, ?, ?, ?, ?)
  `).bind(jobId, o.planId, runGs, o.date, o.actor, o.mode, Math.floor(Date.now() / 1000), runGs, digest).run()

  return { ok: true, job_id: jobId, plan_id: o.planId, mode: o.mode, date: o.date, run: runGs, digest }
}
