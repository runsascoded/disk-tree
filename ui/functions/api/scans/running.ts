import { json } from '../../../cfn/http'

/** `GET /api/scans/running` — nothing ever runs here. */
export const onRequestGet: PagesFunction = async () => json([], { maxAge: 0 })
