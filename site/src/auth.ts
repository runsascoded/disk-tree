// Identity plumbing (@open-athena/auth): where whoami comes from, per host.
//
// gcs.oa.dev is public shell + app-gated data — identity is the app session
// (`/api/auth/whoami`), minted at `/auth/sso` (CF Access as SSO IdP) or by
// redeeming a `?key=` share link.
import { displayName, useForgetWhoami, useWhoami, type Whoami, type WhoamiSource } from '@open-athena/auth/react'

// Deployment seam (specs/denovo-factor.md): the whoami source is a build-time
// flag. `edge` = the whole host sits behind a CF Access gate (cw-s3.oa.dev:
// `/cdn-cgi/access/get-identity`, sign-in bounces through `/login`); `app`
// (default) = the app session (`/api/auth/whoami`, minted at `/auth/sso`).
// `public` = no gate (r2.rbw.sh, per-project embeds): the app renders for an
// anonymous viewer, no whoami fetch or login wall (specs/federated-scans.md).
export const AUTH_MODE: 'app' | 'edge' | 'public' =
  import.meta.env.VITE_AUTH_MODE === 'edge' ? 'edge'
    : import.meta.env.VITE_AUTH_MODE === 'public' ? 'public'
      : 'app'
// The whoami source only applies to the gated modes; public bypasses the Gate.
export const WHOAMI_SOURCE: WhoamiSource = { kind: AUTH_MODE === 'edge' ? 'edge' : 'app' }

// `?wall` forces the wall in dev (which otherwise short-circuits to authed,
// since neither identity source exists locally). A real `oa_auth` cookie
// (forged against the local wrangler's SESSION_SECRET, set via
// document.cookie so it's visible here) disables the stub entirely — dev
// then exercises the real whoami/scopes path, including guest grants.
const forceWall = new URLSearchParams(window.location.search).has('wall')
const hasLocalSession = document.cookie.includes('oa_auth=')
export const DEV_IDENTITY: Whoami | null | undefined =
  import.meta.env.DEV && !hasLocalSession
    ? (forceWall ? null : { email: import.meta.env.VITE_DEV_EMAIL ?? 'dev@example.test' })
    : undefined

export const signInUrl = (): string =>
  AUTH_MODE === 'edge' ? '/login' : `/auth/sso?next=${encodeURIComponent(window.location.pathname + window.location.search)}`

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
  const { whoami } = useWhoami(WHOAMI_SOURCE, { devIdentity: DEV_IDENTITY })
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
  const { whoami } = useWhoami(WHOAMI_SOURCE, { devIdentity: DEV_IDENTITY })
  return !!(whoami as { email?: string | null } | null)?.email
}
