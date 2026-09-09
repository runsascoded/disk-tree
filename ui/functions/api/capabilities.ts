import { json } from '../../cfn/http'
import { type Env, isOpen } from '../../cfn/env'

/** `GET /api/capabilities` — what this deployment can do. The static (cloud)
 *  deployment serves scans; it can't walk a filesystem, mutate one, or run
 *  the heavier analyses. The UI hides those affordances. (The Flask server
 *  answers the same shape with everything on.) */
export const CAPABILITIES = {
  static: true,
  scan: false,
  delete: false,
  reveal: false,
  histogram: false,
  filter: false,
  preview: false,
  compare: false,
  progress: false,
  library: false,
  backend: false,
  s3: false,
  // `/api/*` is gated (spec `specs/done/pages-auth.md`): the UI probes
  // `/api/auth/whoami` and shows the wall when nobody is signed in. An open
  // demo (`PUBLIC_OPEN`) reports `auth: false`, so the UI skips both.
  auth: true,
}

export const onRequestGet: PagesFunction<Env> = async ({ env }) =>
  json({ ...CAPABILITIES, auth: !isOpen(env) }, { maxAge: 300 })
