/**
 * The vendored half of `@open-athena/auth` (spec `specs/done/pages-auth.md`): the
 * package ships logic-only primitives; the copy, layout and class names live
 * here. Everything is a pass-through when the server reports no `auth`
 * capability (the Flask server), so local dev never sees a wall.
 */
import type { ReactNode } from 'react'
import { Link } from 'react-router-dom'
import { Box, Button } from '@mui/material'
import {
  AuthGate,
  SignInPanel as PkgSignInPanel,
  displayName,
  hasScope,
  useForgetWhoami,
  useKeyExchange,
  useWhoami as usePkgWhoami,
} from '@open-athena/auth/react'
import type { AppWhoami } from '@open-athena/auth/react'
import { useCapabilities } from './hooks/useCapabilities'

const SOURCE = { kind: 'app' } as const

export const ssoUrl = (next: string): string => `/auth/sso?next=${encodeURIComponent(next)}`
const here = (): string => location.pathname + location.search

/** Can mint / revoke links and read the log. The package's `admin` flag means
 *  "in `adminEmails`" (wildcard scope); the allowlist grants the scope instead. */
export const isAdmin = (whoami: AppWhoami | null | undefined): boolean => hasScope(whoami, 'admin')

/** `undefined` while capabilities load; `false` on a server without a gate. */
export function useAuthEnabled(): boolean | undefined {
  const caps = useCapabilities()
  return caps === undefined ? undefined : caps.auth
}

/** `whoami`: `undefined` loading, `null` signed out (or no gate), else the identity. */
export function useWhoami(): { enabled: boolean | undefined; whoami: AppWhoami | null | undefined; refresh: () => void } {
  const enabled = useAuthEnabled()
  // Hold the probe until a `?key=` link has been redeemed: the header mounts
  // before the gate, and a probe that wins the race caches the *old* identity.
  const ready = useKeyExchange()
  const { whoami, refresh } = usePkgWhoami<AppWhoami>(SOURCE, { enabled: enabled === true && ready })
  return { enabled, whoami: enabled === true ? whoami : enabled === undefined ? undefined : null, refresh }
}

/** Probe identity (after redeeming a `?key=` link) and render the app or the wall. */
export function Gate({ children }: { children: ReactNode }) {
  const enabled = useAuthEnabled()
  if (enabled === undefined) return null
  if (!enabled) return <>{children}</>
  return (
    <AuthGate<AppWhoami> source={SOURCE} signIn={<Wall />}>
      {children}
    </AuthGate>
  )
}

function Wall() {
  return (
    <div className="auth-wall">
      <PkgSignInPanel
        signInUrl={ssoUrl(here())}
        withNext={false}
        title="This deployment is private."
        signInLabel="Sign in"
        hint="Have an access link? Open it — it signs this browser in automatically. Access is logged."
        classNames={{ root: 'signin', title: 'signin-title', button: 'signin-btn', hint: 'dim' }}
      />
    </div>
  )
}

/** Header chip: who's signed in + sign out; admins also get the Access page. */
export function WhoamiChip() {
  const { enabled, whoami } = useWhoami()
  const forget = useForgetWhoami()
  if (!enabled || whoami === undefined) return null
  if (whoami === null) {
    return (
      <Button size="small" href={ssoUrl(here())} sx={{ textTransform: 'none', mr: 1 }}>
        Sign in
      </Button>
    )
  }
  const signOut = async () => {
    await fetch('/api/auth/logout', { method: 'POST', credentials: 'include' })
    forget()
    // Already-fetched private responses stay in memory otherwise; reloading
    // is what signing out should mean anyway.
    location.reload()
  }
  return (
    <Box className="whoami" sx={{ mr: 1, fontSize: '0.8rem' }}>
      {isAdmin(whoami) && (
        <Button component={Link} to="/access" size="small" sx={{ textTransform: 'none' }}>
          Access
        </Button>
      )}
      <span className="dim" title={whoami.kind === 'grant' ? 'signed in with an access link' : whoami.email}>
        {displayName(whoami) ?? (whoami.kind === 'grant' ? 'access link' : 'signed in')}
      </span>
      <button className="linkish" onClick={signOut}>sign out</button>
    </Box>
  )
}

export class ApiError extends Error {
  status: number
  constructor(status: number, message: string) {
    super(message)
    this.status = status
  }
}

/** JSON fetch for the `/api/auth/*` surface; a non-2xx becomes an `ApiError`. */
export async function api<T>(path: string, body?: unknown): Promise<T> {
  const res = await fetch(path, {
    method: body === undefined ? 'GET' : 'POST',
    credentials: 'include',
    headers: { accept: 'application/json', ...(body === undefined ? {} : { 'content-type': 'application/json' }) },
    body: body === undefined ? undefined : JSON.stringify(body),
  })
  if (!res.ok) {
    const err = (await res.json().catch(() => ({}))) as { error?: string }
    throw new ApiError(res.status, err.error ?? `${res.status} ${path}`)
  }
  return (await res.json()) as T
}
