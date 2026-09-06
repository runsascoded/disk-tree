import type { Env } from '../../cfn/env'
import { json } from '../../cfn/http'
import { getScans, latestPerPath } from '../../cfn/manifests'

/** `GET /api/scans` — most recent scan per path, with root stats. */
export const onRequestGet: PagesFunction<Env> = async ({ env }) =>
  json(latestPerPath(await getScans(env)))
