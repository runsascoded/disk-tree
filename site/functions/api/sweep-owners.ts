/**
 * GET /api/sweep-owners?date=<scan>
 *
 * The sweeper × owner matrix: for every live sweep mark, the person who swept
 * it (`actions.actor`) × the owners of the bytes under it (the scan's per-user
 * attribution, claims applied). Off the diagonal = a sweeper marked data an
 * owner other than themselves holds — the conflicts to vet before dispatching
 * a deletion.
 */
import { type Ctx, json, requireScope, requireViewer } from '../_lib/auth.js'
import { markTotals } from '../_lib/totals.js'

interface Cell { by: string; to: string; bytes: number; prefixes: string[] }

export const onRequestGet = async (ctx: Ctx): Promise<Response> => {
  const { env, request } = ctx
  if (!env.DB) return json({ error: 'ledger backend not configured (DB)' }, 503)
  if (!env.GCS_HMAC_KEY_ID || !env.GCS_HMAC_SECRET) return json({ error: 'index reader not configured' }, 503)
  const gated = await requireViewer(ctx)
  if (gated instanceof Response) return gated
  const date = new URL(request.url).searchParams.get('date') ?? ''
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return json({ error: 'date=YYYY-MM-DD required' }, 400)
  try {
    const emap = new Map<string, string>()
    const ue = await env.DB.prepare('SELECT email, user FROM user_emails').all<{ email: string; user: string }>()
    for (const r of ue.results) emap.set(r.email.toLowerCase(), r.user)
    const t = await markTotals(env, date)
    const cells = new Map<string, Cell>()
    for (const m of t.marks) {
      // Live sweep marks only (a repainted mark decides nothing).
      if (m.eff !== 'sweep' || m.repainted_by) continue
      const by = m.who ? (emap.get(m.who.toLowerCase()) ?? m.who) : 'unknown'
      for (const [to, bytes] of Object.entries(m.us)) {
        if (bytes <= 0) continue
        const key = `${by} ${to}`
        const cell = cells.get(key) ?? { by, to, bytes: 0, prefixes: [] }
        cell.bytes += bytes
        cell.prefixes.push(m.prefix)
        cells.set(key, cell)
      }
    }
    const out = [...cells.values()].sort((a, b) => b.bytes - a.bytes)
    return json({ scan: t.scan, head: t.head, cells: out }, 200, { 'cache-control': 'private, no-store' })
  } catch (e) {
    return json({ error: (e as Error).message }, 503)
  }
}
