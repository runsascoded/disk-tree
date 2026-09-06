import { json } from '../../../cfn/http'

/** `GET /api/scans/progress` — no scans run here, so no progress. */
export const onRequestGet: PagesFunction = async () => json([], { maxAge: 0 })
