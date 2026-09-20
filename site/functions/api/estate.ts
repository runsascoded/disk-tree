/** One person's estate for a scan — what `/user/:id` shows, folded
 * server-side from the index tiers and the live ledger (no `tree.json`):
 *
 *   GET /api/estate?date=<scan>&user=<canonical id>
 *   → { user, date, head, states, marks, claims, undecided }
 *
 * - `states`: their keep / last-ckpt / sweep / undecided bytes (+ class mixes),
 *   claims applied — the same numbers `/users` and the map's rollup use.
 * - `marks`: every live mark whose band holds some of their bytes (`b` = that
 *   share), plus every mark they authored — the "decided" rows.
 * - `claims`: their live owner claims, sized from the index.
 * - `undecided`: their bytes with no keep/sweep decision above or below, as
 *   the outermost such subtrees (the review backlog) — from the user lens
 *   scoped to unmarked bytes, walked down only while marks sit inside.
 */
import { type Ctx, json, requireScope, requireViewer } from '../_lib/auth.js'
import { marksUnder } from '../_lib/markAxes.js'
import { canonId } from '../_lib/identity.js'
import { markTotals } from '../_lib/totals.js'
import { buildView, type ViewNode } from '../_lib/view.js'

export const onRequestGet = async (ctx: Ctx): Promise<Response> => {
  const { env, request } = ctx
  if (!env.DB) return json({ error: 'ledger backend not configured (DB)' }, 503)
  if (!env.GCS_HMAC_KEY_ID || !env.GCS_HMAC_SECRET) return json({ error: 'index reader not configured' }, 503)
  const gated = await requireViewer(ctx)
  if (gated instanceof Response) return gated
  const url = new URL(request.url)
  const date = url.searchParams.get('date') ?? ''
  const user = url.searchParams.get('user') ?? ''
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return json({ error: 'date=YYYY-MM-DD required' }, 400)
  if (!/^[a-z0-9_-]+$/.test(user)) return json({ error: 'user=<canonical id> required' }, 400)

  try {
    const totals = await markTotals(env, date)
    const mine = (who: string | null | undefined) => !!who && canonId(who) === user
    // Their share of each band: the manifest keys `us` by the index's usr (a
    // canonical id) or a claimant (an email) — canonicalize both.
    const share = (us: Record<string, number>): number => {
      let b = 0
      for (const [k, v] of Object.entries(us)) if (mine(k)) b += v
      return b
    }
    const marks = totals.marks
      .filter(m => m.keep != null)
      .map(m => ({ prefix: m.prefix, keep: m.keep!, eff: m.eff, who: m.who ?? null, ts: m.ts, bytes: m.bytes, b: share(m.us), authored: mine(m.who), ...(m.repainted_by ? { repainted_by: m.repainted_by } : {}) }))
      .filter(m => m.b > 0 || m.authored)
      .sort((a, b) => b.b - a.b || b.bytes - a.bytes)
    const claims = totals.claims.filter(c => mine(c.owner)).map(c => ({ prefix: c.prefix, ts: c.ts, bytes: c.bytes, objects: c.objects, ...(c.repainted_by ? { repainted_by: c.repainted_by } : {}) }))
    const states = Object.entries(totals.users).find(([k]) => mine(k))?.[1] ?? null
    const userBytes = states ? states.keep + states.keep_last_ckpt + states.sweep + states.unmarked : 0

    // Undecided: the user lens scoped to unmarked bytes, at a byte threshold
    // that keeps the walk to the sizes worth a decision.
    const undecided: { prefix: string; b: number }[] = []
    if (userBytes > 0) {
      const under = marksUnder(totals.marks.filter(m => m.keep != null && !m.repainted_by))
      const view = await buildView(env, {
        date, path: '', w: 1, h: 1, minArea: 1, atten: 1,
        threshold: Math.max(1e9, userBytes * 0.0005),
        lens: { key: user }, states: new Set(['unmarked']),
      })
      const walk = (n: ViewNode, path: string) => {
        if (!under(path)) { if (n.b > 0) undecided.push({ prefix: `gs://${path}/`, b: n.b }); return }
        let rest = n.b
        for (const c of n.c ?? []) {
          if (c.n.startsWith('(')) continue
          rest -= c.b
          walk(c, `${path}/${c.n}`)
        }
        // Folded children (and share below the threshold) stay at this node.
        if (rest > 0) undecided.push({ prefix: `gs://${path}/`, b: rest })
      }
      for (const bucket of view.tree.c ?? []) if (!bucket.n.startsWith('(')) walk(bucket, bucket.n)
      undecided.sort((a, b) => b.b - a.b)
    }
    return json({ user, date, head: totals.head, states, marks, claims, undecided }, 200, { 'cache-control': 'private, no-store' })
  } catch (e) {
    return json({ error: (e as Error).message }, 503)
  }
}
