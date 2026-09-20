// /api/marks — the mark axis (specs/cw-sweep.md).
//
//   GET  /api/marks   -> { marks: [{ prefix, keep, who, ts, note }] }   viewer
//   POST /api/marks   { prefixes: [...], keep: 'keep'|'keep_last_ckpt'|'sweep'|null, scan?, note? }
//
// A mark records intent only — it never deletes. Any authenticated viewer may
// mark (who is recorded); an admin later curates `sweep`-marked prefixes into a
// plan and dispatches. `keep: null` un-marks. Both the resolved `marks` row and
// the append-only `mark_log` history are written.
import type { D1Database } from "@cloudflare/workers-types"
import { type Ctx, type Env as AuthEnv, json, requireViewer } from "../_lib/auth.js"
import { canonicalPrefix } from "../_lib/plans.js"

type Env = AuthEnv & { DB?: D1Database }

const KEEPS = new Set(["keep", "keep_last_ckpt", "sweep"])

export const onRequestGet = async (ctx: Ctx & { env: Env }): Promise<Response> => {
  const gated = await requireViewer(ctx)
  if (gated instanceof Response) return gated
  if (!ctx.env.DB) return json({ marks: [] })
  const { results } = await ctx.env.DB.prepare(
    "SELECT prefix, keep, who, ts, note FROM marks ORDER BY prefix",
  ).all()
  return json({ marks: results })
}

export const onRequestPost = async (ctx: Ctx & { env: Env }): Promise<Response> => {
  const gated = await requireViewer(ctx)
  if (gated instanceof Response) return gated
  if (!ctx.env.DB) return json({ error: "marks store not configured (no D1 binding)" }, 503)
  const db = ctx.env.DB

  const body = (await ctx.request.json().catch(() => null)) as
    | { prefixes?: unknown; keep?: unknown; scan?: unknown; note?: unknown } | null
  const keep = body?.keep ?? null
  if (keep !== null && (typeof keep !== "string" || !KEEPS.has(keep))) {
    return json({ error: "keep must be 'keep' | 'keep_last_ckpt' | 'sweep' | null" }, 400)
  }
  const raw = Array.isArray(body?.prefixes) ? body!.prefixes : []
  if (!raw.length) return json({ error: "prefixes required" }, 400)
  const prefixes: string[] = []
  for (const r of raw) {
    const c = typeof r === "string" ? canonicalPrefix(r) : null
    if (!c) return json({ error: `bad prefix ${JSON.stringify(r)}` }, 400)
    prefixes.push(c)
  }
  const scan = typeof body?.scan === "string" ? body.scan : ""
  const note = typeof body?.note === "string" ? body.note : null
  const who = gated.email
  const ts = Math.floor(Date.now() / 1000)

  for (const prefix of prefixes) {
    if (keep === null) {
      await db.prepare("DELETE FROM marks WHERE prefix = ?").bind(prefix).run()
    } else {
      await db.prepare(
        "INSERT OR REPLACE INTO marks (prefix, keep, who, scan, ts, note) VALUES (?, ?, ?, ?, ?, ?)",
      ).bind(prefix, keep, who, scan, ts, note).run()
    }
    await db.prepare(
      "INSERT INTO mark_log (prefix, keep, who, scan, ts, note) VALUES (?, ?, ?, ?, ?, ?)",
    ).bind(prefix, keep, who, scan, ts, note).run()
  }
  return json({ marked: prefixes, keep })
}
