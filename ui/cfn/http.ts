/** Response helpers shared by the `/api/*` Functions. */

export function json(data: unknown, init: ResponseInit & { maxAge?: number } = {}): Response {
  const { maxAge = 60, headers, ...rest } = init
  return new Response(JSON.stringify(data), {
    ...rest,
    headers: {
      'content-type': 'application/json; charset=utf-8',
      'cache-control': maxAge > 0 ? `public, max-age=${maxAge}` : 'no-store',
      ...(headers ?? {}),
    },
  })
}

/** The Flask API's error shape: `{ error }` with a 4xx/5xx status. */
export const error = (message: string, status = 400, extra: Record<string, unknown> = {}): Response =>
  json({ error: message, ...extra }, { status, maxAge: 0 })

/** `uri` as the Flask handlers normalize it: no trailing slash, `/` for empty. */
export const normUri = (raw: string | null): string => {
  const uri = (raw ?? '/').replace(/\/+$/, '')
  return uri === '' ? '/' : uri
}
