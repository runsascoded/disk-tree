// GET /api/sweep/jobs — recent `cw-sweep-*` Batch jobs with live state, so the
// console shows a dispatch from the moment it's submitted (the executor's D1 row
// starts in-progress). Any authenticated viewer may read; the payload holds no
// bucket data.
//
// This is also where the executor's gs:// run summary is **reflected into D1**:
// the Batch job writes only gs:// artifacts, so when a run reaches a terminal
// state we read its `<mode>-summary.json` and fill in `deletion_runs` totals +
// `deletion_bands` (idempotent — only while finished_ts IS NULL). A cw-s3
// simplification vs gcs writing D1 from inside Batch.
import type { D1Database } from "@cloudflare/workers-types"
import { type Ctx, type Env as AuthEnv, json, requireViewer } from "../../_lib/auth.js"
import { batchJobsUrl, BATCH_REGION, GCP_PROJECT, gcpToken } from "../../_lib/gcp.js"
import { CW_BUCKET, DATA_BUCKET } from "../../_lib/cwBatch.js"

type Env = AuthEnv & { DB?: D1Database }

const HOLD_SECS = 7 * 24 * 3600 // undo window before purge (matches gcs's ≥7d soft-delete)
const TERMINAL = new Set(["SUCCEEDED", "FAILED", "CANCELLED"])

interface BatchJob {
  name: string
  uid: string
  createTime: string
  updateTime?: string
  status?: { state?: string; runDuration?: string; statusEvents?: { description?: string }[] }
  taskGroups?: { taskSpec?: { environment?: { variables?: Record<string, string> } } }[]
}

interface RunSummary {
  mode: string
  bucket?: string // the run's bucket (specs/cw-multi-bucket.md §4); older summaries: the primary
  deleted_objects: number
  deleted_bytes: number
  skipped_gone: number
  skipped_overwritten: number
  drift_new: number
  delete_failed: number
  finished_ts: number
  bands: { prefix: string; bytes: number; objects: number; gone: number; overwritten: number; drift_new: number }[]
}

async function readSummary(token: string, jobId: string, mode: string): Promise<RunSummary | null> {
  const dir = mode === "real" ? "deleted" : "would-delete"
  const obj = encodeURIComponent(`sweep/cw/runs/${jobId}/${dir}-summary.json`)
  const r = await fetch(`https://storage.googleapis.com/storage/v1/b/${DATA_BUCKET}/o/${obj}?alt=media`, {
    headers: { authorization: `Bearer ${token}` },
  })
  if (!r.ok) return null
  return (await r.json().catch(() => null)) as RunSummary | null
}

async function reflect(db: D1Database, jobId: string, mode: string, s: RunSummary): Promise<void> {
  const real = mode === "real"
  const undoDeadline = real ? s.finished_ts + HOLD_SECS : null
  const purgeState = real && s.deleted_objects > 0 ? "pending" : "none"
  await db.prepare(`
    UPDATE deletion_runs SET
      finished_ts = ?, deleted_bytes = ?, deleted_objects = ?, skipped_gone = ?,
      skipped_overwritten = ?, drift_dirs = ?, undo_deadline = ?, purge_state = ?
    WHERE run_id = ? AND finished_ts IS NULL
  `).bind(
    s.finished_ts, s.deleted_bytes, s.deleted_objects, s.skipped_gone,
    s.skipped_overwritten, s.drift_new, undoDeadline, purgeState, jobId,
  ).run()
  for (const b of s.bands ?? []) {
    await db.prepare(`
      INSERT OR REPLACE INTO deletion_bands
        (run_id, prefix, bytes, objects, gone, overwritten, drift_new_objects, undone_objects)
      VALUES (?, ?, ?, ?, ?, ?, ?, 0)
    `).bind(jobId, `s3://${s.bucket ?? CW_BUCKET}/${b.prefix}`, b.bytes, b.objects, b.gone, b.overwritten, b.drift_new).run()
  }
}

export const onRequestGet = async (ctx: Ctx & { env: Env }): Promise<Response> => {
  const gated = await requireViewer(ctx)
  if (gated instanceof Response) return gated
  if (!ctx.env.GCP_SA_KEY) return json({ jobs: [], configured: false })
  const token = await gcpToken(ctx.env.GCP_SA_KEY)

  const r = await fetch(`${batchJobsUrl(BATCH_REGION)}?pageSize=100&orderBy=${encodeURIComponent("create_time desc")}`, {
    headers: { authorization: `Bearer ${token}` },
  })
  if (!r.ok) {
    console.error("batch list failed", r.status)
    return json({ error: `batch list failed (${r.status})` }, 500)
  }
  const { jobs = [] } = (await r.json()) as { jobs?: BatchJob[] }
  const mine = jobs.filter(j => /\/jobs\/cw-sweep-(dry|real)-/.test(j.name)).slice(0, 20)

  const db = ctx.env.DB
  if (db) {
    // Reflect any terminal sweep run whose D1 row is still in-progress.
    const pending = new Set(
      (await db.prepare("SELECT run_id FROM deletion_runs WHERE finished_ts IS NULL").all<{ run_id: string }>())
        .results.map(x => x.run_id),
    )
    for (const j of mine) {
      const jobId = j.name.slice(j.name.lastIndexOf("/") + 1)
      if (!pending.has(jobId) || !TERMINAL.has(j.status?.state ?? "")) continue
      const mode = jobId.startsWith("cw-sweep-real-") ? "real" : "dry"
      const summary = await readSummary(token, jobId, mode)
      if (summary) await reflect(db, jobId, mode, summary)
      else await db.prepare("UPDATE deletion_runs SET finished_ts = ? WHERE run_id = ? AND finished_ts IS NULL")
        .bind(Math.floor(Date.now() / 1000), jobId).run()
    }
    // Reflect terminal undo/purge ops onto their target run (state guards keep it
    // idempotent). TARGET_RUN + OP are in the op job's env.
    for (const j of jobs) {
      if (!/\/jobs\/cw-(undo|purge)-/.test(j.name) || j.status?.state !== "SUCCEEDED") continue
      const vars = j.taskGroups?.[0]?.taskSpec?.environment?.variables ?? {}
      const target = vars.TARGET_RUN
      if (!target) continue
      if (vars.OP === "undo") {
        await db.prepare("UPDATE deletion_runs SET undo_state = 'full' WHERE run_id = ? AND undo_state = 'partial'").bind(target).run()
      } else if (vars.OP === "purge") {
        await db.prepare("UPDATE deletion_runs SET purge_state = 'done' WHERE run_id = ? AND purge_state = 'pending'").bind(target).run()
      }
    }
  }

  const out = mine.map(j => {
    const jobId = j.name.slice(j.name.lastIndexOf("/") + 1)
    const vars = j.taskGroups?.[0]?.taskSpec?.environment?.variables ?? {}
    const ev = j.status?.statusEvents ?? []
    const dur = j.status?.runDuration
    return {
      job_id: jobId,
      mode: jobId.startsWith("cw-sweep-real-") ? "real" : "dry",
      state: j.status?.state ?? "UNKNOWN",
      created: j.createTime,
      updated: j.updateTime ?? null,
      run_secs: dur ? Number(dur.replace(/s$/, "")) : null,
      date: vars.SWEEP_DATE ?? null,
      run: `gs://${DATA_BUCKET}/sweep/cw/runs/${jobId}`,
      last_event: ev.length ? ev[ev.length - 1].description ?? null : null,
      logs: `https://console.cloud.google.com/logs/query;query=${encodeURIComponent(`labels.job_uid="${j.uid}"`)}?project=${GCP_PROJECT}`,
    }
  })
  return json({ jobs: out, configured: true, region: BATCH_REGION }, 200, { "cache-control": "private, max-age=10" })
}
