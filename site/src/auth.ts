// Identity plumbing (@open-athena/auth): where whoami comes from, per host.
//
// In the app tier the host is public shell + app-gated data — identity is the
// app session (`/api/auth/whoami`), minted by our own Google OIDC client
// (`/auth/google`), an emailed code (`/auth/email/*`), or by redeeming a
// `?key=` share link (specs/oidc-cutover-cw.md).
import { displayName, useForgetWhoami, useWhoami, type Whoami, type WhoamiSource } from '@open-athena/auth/react'

// Deployment seam (specs/denovo-factor.md): the whoami source is a build-time
// flag. `edge` = the whole host sits behind a CF Access gate
// (`/cdn-cgi/access/get-identity`, sign-in bounces through `/login`); `app` =
// the app session (`/api/auth/whoami`, minted at `/signin`). The default lives
// in vite.config.ts; the cutover deploy flips it (specs/oidc-cutover-cw.md).
// `public` = no gate (r2.rbw.sh, per-project embeds): the app renders for an
// anonymous viewer, no whoami fetch or login wall (specs/federated-scans.md).
export const AUTH_MODE: 'app' | 'edge' | 'public' =
  import.meta.env.VITE_AUTH_MODE === 'edge' ? 'edge'
    : import.meta.env.VITE_AUTH_MODE === 'public' ? 'public'
      : 'app'
// The whoami source only applies to the gated modes; public bypasses the Gate.
export const WHOAMI_SOURCE: WhoamiSource = { kind: AUTH_MODE === 'edge' ? 'edge' : 'app' }

// `?wall` forces the wall in dev (which otherwise short-circuits to authed,
// since neither identity source exists locally). A local session disables the
// stub entirely — dev then exercises the real whoami/scopes path, including
// guest grants. The real `oa_auth` cookie is HttpOnly, so a sign-in minted by
// the local Functions (Google / email-code) is announced by the JS-visible
// `oa_dev_session` marker (functions/_lib/devsession.ts); a cookie forged
// against the local wrangler's SESSION_SECRET via document.cookie also counts.
// Evaluated per render (not once at load) so an in-page code sign-in flips
// the gate without a reload.
const forceWall = new URLSearchParams(window.location.search).has('wall')
const hasLocalSession = (): boolean =>
  document.cookie.includes('oa_auth=') || document.cookie.includes('oa_dev_session=')
export const devIdentity = (): Whoami | null | undefined =>
  import.meta.env.DEV && !hasLocalSession()
    ? (forceWall ? null : { email: import.meta.env.VITE_DEV_EMAIL ?? 'dev@example.test' })
    : undefined

/** Where the inline "sign in" links go: the edge tier's `/login`, or the app
 *  tier's own `/signin` page (Google / emailed code), returning here after. */
export const signInUrl = (): string =>
  AUTH_MODE === 'edge' ? '/login' : `/signin?next=${encodeURIComponent(window.location.pathname + window.location.search)}`

export interface Ident {
  email: string
  name?: string
}

/** Sign out of the app session (POST /api/auth/logout clears the cookie). */
export function useSignOut(): () => void {
  const forget = useForgetWhoami()
  return () => {
    if (AUTH_MODE === 'edge') { forget(); window.location.assign('/cdn-cgi/access/logout'); return }
    void fetch('/api/auth/logout', { method: 'POST', credentials: 'include' }).then(() => {
      forget()
    })
  }
}

/** The header chip's identity: null until (unless) someone is signed in. */
export function useIdent(): Ident | null {
  const { whoami } = useWhoami(WHOAMI_SOURCE, { devIdentity: devIdentity() })
  if (!whoami) return null
  const name = displayName(whoami) ?? undefined
  const email = (whoami as { email?: string | null }).email ?? name ?? 'guest'
  return { email, name }
}

/**
 * Mark/claim writes require an email-bearing identity — anonymous guest
 * links are read-only (the server enforces the same rule).
 */
export function useCanMark(): boolean {
  const { whoami } = useWhoami(WHOAMI_SOURCE, { devIdentity: devIdentity() })
  return !!(whoami as { email?: string | null } | null)?.email
}
