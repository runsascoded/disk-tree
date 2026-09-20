// GET /api/sweep/jobs — the recent `gcs-sweep-*` Batch jobs with their live
// state, so the console can show a dispatch from the moment it is submitted
// (the executor only writes `deletion_runs` once its manifest step is done,
// which can be an hour of listing). Read via the same dispatch SA as
// `dispatch.ts` (`batch.jobsEditor` covers list). Any signed-in viewer of the
// console may read this; the payload holds no bucket data.
import { type Env as AuthEnv, json, requireViewer } from '../../_lib/auth.js'
import { BATCH_REGIONS, BUCKET_REGION, GCP_PROJECT, batchJobsUrl, gcpToken } from '../../_lib/gcp.js'

interface Env extends AuthEnv {
  GCP_SA_KEY?: string
}

interface BatchJob {
  name: string
  uid: string
  createTime: string
  updateTime?: string
  status?: { state?: string; runDuration?: string; statusEvents?: { type?: string; description?: string; eventTime?: string }[] }
  taskGroups?: { taskSpec?: { environment?: { variables?: Record<string, string> }; runnables?: { container?: { commands?: string[] } }[] } }[]
}

export interface SweepJob {
  job_id: string
  mode: 'dry' | 'real'
  state: string
  created: string
  updated: string | null
  run_secs: number | null
  by: string | null
  date: string | null
  /** The `-b` cut the job was dispatched with (empty = every bucket). */
  buckets: string[]
  /** The Batch region it runs in (its bucket's, for a one-bucket cut). */
  region: string
  /** The bucket's own region, for a one-bucket cut (null: several buckets). */
  bucket_region: string | null
  plan: string
  last_event: string | null
  logs: string
}

export const onRequestGet = async (ctx: { request: Request; env: Env }): Promise<Response> => {
  const gated = await requireViewer(ctx)
  if (gated instanceof Response) return gated
  if (!ctx.env.GCP_SA_KEY) return json({ jobs: [], configured: false })
  const token = await gcpToken(ctx.env.GCP_SA_KEY)
  // Jobs live in their bucket's region: list every region a sweep can be
  // dispatched to and merge, newest first.
  const lists = await Promise.all(BATCH_REGIONS.map(async region => {
    const r = await fetch(`${batchJobsUrl(region)}?pageSize=100&orderBy=${encodeURIComponent('create_time desc')}`, {
      headers: { authorization: `Bearer ${token}` },
    })
    if (!r.ok) throw new Error(`batch list ${region} failed: ${r.status} ${(await r.text()).slice(0, 200)}`)
    const { jobs = [] } = (await r.json()) as { jobs?: BatchJob[] }
    return jobs.map(j => ({ ...j, region }))
  })).catch(e => e as Error)
  if (lists instanceof Error) { console.error('batch list failed', lists.message); return json({ error: lists.message }, 500) }
  const jobs = lists.flat().sort((a, b) => (a.createTime < b.createTime ? 1 : -1))
  const out: SweepJob[] = jobs
    .filter(j => /\/jobs\/gcs-sweep-(dry|real)-/.test(j.name))
    .slice(0, 20)
    .map(j => {
      const job_id = j.name.slice(j.name.lastIndexOf('/') + 1)
      const vars = j.taskGroups?.[0]?.taskSpec?.environment?.variables ?? {}
      const script = j.taskGroups?.[0]?.taskSpec?.runnables?.[0]?.container?.commands?.join(' ') ?? ''
      const buckets = [...new Set([...script.matchAll(/(?:^|\s)-b\s+(marin-[a-z0-9-]+)/g)].map(m => m[1]))].sort()
      const ev = j.status?.statusEvents ?? []
      const last = ev.length ? ev[ev.length - 1] : null
      const dur = j.status?.runDuration
      return {
        job_id,
        mode: job_id.startsWith('gcs-sweep-real-') ? 'real' : 'dry',
        state: j.status?.state ?? 'UNKNOWN',
        created: j.createTime,
        updated: j.updateTime ?? null,
        run_secs: dur ? Number(dur.replace(/s$/, '')) : null,
        by: vars.USER ?? null,
        date: vars.SWEEP_DATE ?? null,
        buckets,
        region: j.region,
        bucket_region: buckets.length === 1 ? BUCKET_REGION[buckets[0]] ?? null : null,
        plan: `gs://oa-gcs-usage-dvx/sweep/runs/${job_id}`,
        last_event: last?.description ?? null,
        logs: `https://console.cloud.google.com/logs/query;query=${encodeURIComponent(`labels.job_uid="${j.uid}"`)}?project=${GCP_PROJECT}`,
      }
    })
  return json({ jobs: out, configured: true }, 200, { 'cache-control': 'private, max-age=10' })
}
