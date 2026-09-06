import { error } from '../../cfn/http'

/** Every `/api/*` route without a Function of its own: not available here.
 *  `/api/capabilities` tells the UI which ones, so it shouldn't ask. */
export const onRequest: PagesFunction = async ({ request }) =>
  error('not available in the static (cloud) deployment', 501, { path: new URL(request.url).pathname })
