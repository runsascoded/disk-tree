// POST /api/plan-sweep/dispatch — launch a plan-first deletion run on GCP
// Batch from /staged (specs/staged-delete.md). Admin only. Body: { plan_id,
// mode: 'dry' | 'real', date: <scan id> }. The work is `_lib/planDispatch.ts`
// (shared with `/slack/actions`); a dispatch is announced in the plan's Slack
// thread (specs/staged-slack.md).
import type { D1Database } from "@cloudflare/workers-types"
import { type Ctx, type Env as AuthEnv, json, requireAdmin } from "../../_lib/auth.js"
import { dispatchPlanSweep } from "../../_lib/planDispatch.js"
import { notifyPlan, runEvent, type NotifyEnv, type RunRow } from "../../_lib/stagedSlack.js"

type Env = AuthEnv & NotifyEnv & { DB?: D1Database }

export const onRequestPost = async (ctx: Ctx & { env: Env; waitUntil?: (p: Promise<unknown>) => void }): Promise<Response> => {
  const gated = await requireAdmin(ctx)
  if (gated instanceof Response) return gated
  const body = (await ctx.request.json().catch(() => null)) as { plan_id?: number; mode?: string; date?: string } | null
  const planId = body?.plan_id
  const mode = body?.mode
  if (!Number.isInteger(planId)) return json({ error: "plan_id required" }, 400)
  if (mode !== "dry" && mode !== "real") return json({ error: "mode must be 'dry' or 'real'" }, 400)
  const siteUrl = new URL(ctx.request.url).origin
  const actor = gated.email ?? gated.name ?? "admin"
  const r = await dispatchPlanSweep(ctx.env, { planId: planId!, mode, date: body?.date ?? "", actor, siteUrl })
  if (!r.ok) return json({ error: r.error, ...(r.detail ? { detail: r.detail } : {}) }, r.status)
  if (ctx.env.DB) {
    const row = await ctx.env.DB.prepare("SELECT * FROM deletion_runs WHERE run_id = ?").bind(r.job_id).first<RunRow>()
    if (row) {
      const p = notifyPlan(ctx.env, ctx.env.DB, planId!, siteUrl, { text: runEvent(row, "dispatched", "www") })
      if (ctx.waitUntil) ctx.waitUntil(p)
      else await p
    }
  }
  return json({ job_id: r.job_id, plan_id: r.plan_id, mode: r.mode, date: r.date, run: r.run, by: actor })
}
