/**
 * POST /slack/actions — Slack interactivity for the staged-deletion thread
 * (specs/staged-slack.md). Buttons on the plan's parent message: Dry-run,
 * Delete for real; on each batch reply: Reject batch. ("Open in www" is a URL
 * button; Slack still calls here, and it's acked.)
 *
 * Authority is the site's, not Slack's: the request is verified with the
 * app's signing secret, the clicking user is mapped to their email
 * (`users.info`), and the same rules as www apply — dispatch needs `admin`
 * (staff domain or an `admin_emails` row); rejecting a batch needs admin or
 * being the one who staged it. A real deletion additionally needs a finished
 * dry-run of exactly the current item set, and runs against that dry-run's
 * scan. Slack wants an answer within 3 s, so this acks at once and does the
 * work after the response; outcomes go to the thread (everyone) or back to
 * the clicker ephemerally (refusals, errors).
 */
import type { D1Database } from '@cloudflare/workers-types'
import { type Env as AuthEnv, isAdmin } from '../_lib/auth.js'
import { gcpToken } from '../_lib/gcp.js'
import { audit } from '../_lib/plans.js'
import { dispatchPlanSweep } from '../_lib/planDispatch.js'
import { listBatchJobs, reflectRuns } from '../_lib/runReflect.js'
import { slackUserEmail, verifySlackSignature } from '../_lib/slack.js'
import { announceFinished, notifyPlan, planGate, runEvent, type NotifyEnv, type RunRow } from '../_lib/stagedSlack.js'

type Env = AuthEnv & NotifyEnv & { DB?: D1Database; GCP_SA_KEY?: string }
interface PagesCtx { request: Request; env: Env; waitUntil: (p: Promise<unknown>) => void }

interface BlockActions {
  type: string
  user: { id: string }
  response_url?: string
  actions: { action_id: string; value?: string }[]
}

async function tell(url: string | undefined, text: string): Promise<void> {
  if (!url) return
  await fetch(url, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ response_type: 'ephemeral', replace_original: false, text }) })
    .catch(() => undefined)
}

/** Bring runs up to date before judging a gate (a dry-run may have just ended). */
async function refreshRuns(env: Env, db: D1Database, siteUrl: string): Promise<void> {
  if (!env.GCP_SA_KEY) return
  const token = await gcpToken(env.GCP_SA_KEY)
  const jobs = await listBatchJobs(token)
  if (!Array.isArray(jobs)) return
  const done = await reflectRuns(db, token, jobs)
  if (done.length) await announceFinished(env, db, done, siteUrl)
}

async function latestScan(db: D1Database): Promise<string | null> {
  const r = await db.prepare("SELECT max(date) AS d FROM index_schema WHERE variant = 'path'").first<{ d: string | null }>()
  return r?.d ?? null
}

async function handle(env: Env, p: BlockActions, siteUrl: string): Promise<void> {
  const db = env.DB!
  const a = p.actions[0]
  const email = await slackUserEmail(env, p.user.id)
  if (!email) return tell(p.response_url, "Couldn't resolve your Slack account to an email (the app needs `users:read.email`).")
  const admin = await isAdmin(env, email)

  if (a.action_id === 'staged_reject') {
    const [planId, batchId] = (a.value ?? '').split(':').map(Number)
    if (!Number.isInteger(planId) || !Number.isInteger(batchId)) return tell(p.response_url, 'Bad button value.')
    const batch = await db.prepare('SELECT created_by FROM stage_batches WHERE id = ? AND plan_id = ?').bind(batchId, planId).first<{ created_by: string }>()
    if (!batch) return tell(p.response_url, `Batch #${batchId} no longer exists.`)
    if (!admin && batch.created_by.toLowerCase() !== email) return tell(p.response_url, 'Only an admin or the person who staged this batch can reject it.')
    const plan = await db.prepare('SELECT state FROM plans WHERE id = ?').bind(planId).first<{ state: string }>()
    if (plan?.state !== 'open') return tell(p.response_url, 'That plan is closed.')
    const items = (await db.prepare('SELECT prefix FROM plan_items WHERE plan_id = ? AND batch_id = ?').bind(planId, batchId).all<{ prefix: string }>()).results
    if (!items.length) return tell(p.response_url, `Batch #${batchId} has nothing left staged.`)
    await db.prepare('DELETE FROM plan_items WHERE plan_id = ? AND batch_id = ?').bind(planId, batchId).run()
    await audit(db, 'plan_items', String(planId), 'delete', email, { batch_id: batchId, prefixes: items.map(i => i.prefix), via: 'slack' }, null)
    return notifyPlan(env, db, planId, siteUrl, { text: `:no_entry_sign: ${email.replace(/@.*$/, '')} rejected batch #${batchId} (${items.length} ${items.length === 1 ? 'prefix' : 'prefixes'} unstaged)` })
  }

  if (a.action_id !== 'staged_dry' && a.action_id !== 'staged_real') return
  if (env.EXECUTOR !== 'plan-sweep') return tell(p.response_url, 'Dispatch from Slack is not wired for this deployment; use www.')
  if (!admin) return tell(p.response_url, 'Only admins can dispatch runs.')
  const [planId, clickedDigest] = (a.value ?? '').split(':')
  const id = Number(planId)
  if (!Number.isInteger(id)) return tell(p.response_url, 'Bad button value.')
  await refreshRuns(env, db, siteUrl)
  const g = await planGate(db, id)
  if (!g) return tell(p.response_url, 'That plan no longer exists.')
  if (g.closed) return tell(p.response_url, 'That plan is closed.')

  let mode: 'dry' | 'real'
  let date: string | null
  if (a.action_id === 'staged_dry') {
    mode = 'dry'
    date = await latestScan(db)
    if (!date) return tell(p.response_url, 'No indexed scan to run against.')
  } else {
    mode = 'real'
    if (!g.gate.ok) return tell(p.response_url, `Not deleting: ${g.gate.reason}.`)
    if (clickedDigest !== g.digest) return tell(p.response_url, 'Not deleting: the plan changed after that button was drawn. Check the updated message.')
    date = g.gate.dry.scan
  }
  const r = await dispatchPlanSweep(env, { planId: id, mode, date, actor: email, siteUrl })
  if (!r.ok) return tell(p.response_url, `Dispatch failed: ${r.error}`)
  const row = await db.prepare('SELECT * FROM deletion_runs WHERE run_id = ?').bind(r.job_id).first<RunRow>()
  if (row) await notifyPlan(env, db, id, siteUrl, { text: runEvent(row, 'dispatched', 'Slack') })
}

export const onRequestPost = async (ctx: PagesCtx): Promise<Response> => {
  const { env, request } = ctx
  if (!env.SLACK_SIGNING_SECRET || !env.DB) return new Response('slack actions not configured', { status: 503 })
  const body = await request.text()
  const ok = await verifySlackSignature(env.SLACK_SIGNING_SECRET, request.headers.get('x-slack-request-timestamp'), request.headers.get('x-slack-signature'), body)
  if (!ok) return new Response('bad signature', { status: 401 })
  const raw = new URLSearchParams(body).get('payload')
  let p: BlockActions
  try { p = JSON.parse(raw ?? '') as BlockActions } catch { return new Response('bad payload', { status: 400 }) }
  if (p.type !== 'block_actions' || !p.actions?.length) return new Response('', { status: 200 })
  if (p.actions[0].action_id === 'staged_open') return new Response('', { status: 200 })
  const siteUrl = new URL(request.url).origin
  ctx.waitUntil(handle(env, p, siteUrl).catch(e => tell(p.response_url, `Something went wrong: ${(e as Error).message}`)))
  return new Response('', { status: 200 })
}
