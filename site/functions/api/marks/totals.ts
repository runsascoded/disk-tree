/**
 * GET /api/marks/totals?date=<scan>[&path=gs://marin-<bucket>/…/][&marks=1]
 *
 * Exact keep / sweep / last-ckpt / undecided bytes for the whole estate or a
 * drilled subtree — `_lib/totals.ts` does the fold; this is the HTTP face.
 * `marks=1` includes the per-mark manifest (the sweep executor's input).
 */
import { type Ctx, json, requireScope, requireViewer } from '../../_lib/auth.js'
import { markTotals } from '../../_lib/totals.js'
import { storeReady } from '../../_lib/index.js'

export const onRequestGet = async (ctx: Ctx): Promise<Response> => {
  const { env, request } = ctx
  if (!env.DB) return json({ error: 'ledger backend not configured (DB)' }, 503)
  if (!storeReady(env)) return json({ error: 'index reader not configured' }, 503)
  const gated = await requireViewer(ctx)
  if (gated instanceof Response) return gated
  const url = new URL(request.url)
  const date = url.searchParams.get('date') ?? ''
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return json({ error: 'date=YYYY-MM-DD required' }, 400)
  const withMarks = url.searchParams.get('marks') === '1'
  // Optional subtree scope: exact totals for a drilled prefix P (the map's
  // per-node rollup). `gs://marin-<bucket>/…/`, trailing slash normalized.
  const rawPath = url.searchParams.get('path')
  let scopePfx: string | undefined
  if (rawPath) {
    scopePfx = rawPath.endsWith('/') ? rawPath : rawPath + '/'
    if (!/^gs:\/\/marin-[a-z0-9-]+\/(?:[^\s]*\/)?$/.test(scopePfx)) return json({ error: 'path must be gs://marin-<bucket>/…/' }, 400)
  }
  try {
    const body = await markTotals(env, date, scopePfx)
    const out = withMarks ? body : { ...body, marks: undefined, mark_count: body.marks.length }
    return json(out, 200, { 'cache-control': 'private, no-store' })
  } catch (e) {
    return json({ error: (e as Error).message }, 503)
  }
}
