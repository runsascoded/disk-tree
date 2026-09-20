// CF Pages Function: auth bounce. CF Access gates this path, so an
// unauthenticated hit triggers the Access login (IdP); once through, we 302 back
// to the app root. The client-side <AuthGate> links here when `get-identity`
// shows no session — replicating the old "log in first" flow now that Access
// gates only the data API (`/data*`, `/v1*`, `/login`) rather than the whole
// host, leaving the static shell (+ og:image) publicly readable for unfurl.
export const onRequest = async (ctx: { request: Request }): Promise<Response> =>
  Response.redirect(new URL('/', ctx.request.url).toString(), 302)
