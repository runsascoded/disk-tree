import type { ReactNode } from 'react'
import { AuthGate as Gate } from '@open-athena/auth/react'
import { DEV_IDENTITY, signInUrl, WHOAMI_SOURCE } from './auth'
import { DEFAULT_STORE } from './stores'

// Gate the human-facing routes on an identity: the app session on gcs.oa.dev
// (minted at /auth/sso, or by a `?key=` share link, which <Gate> redeems
// before probing), the CF Access edge session on cw-* hosts. The static shell
// + og:image stay publicly crawlable for link unfurls either way — crawlers
// read the og: meta from <head> regardless of which body we render.
export function AuthGate({ children }: { children: ReactNode }) {
  return (
    <Gate source={WHOAMI_SOURCE} devIdentity={DEV_IDENTITY} signIn={<LoginWall />}>
      {children}
    </Gate>
  )
}

function LoginWall() {
  return (
    <div className="authwall">
      <div className="card">
        <h1>{DEFAULT_STORE.title}</h1>
        <p>{DEFAULT_STORE.desc}</p>
        <p className="restrict">{DEFAULT_STORE.wall.restrict}</p>
        <a className="signin" href={signInUrl()}>{DEFAULT_STORE.wall.signIn}</a>
        {DEFAULT_STORE.wall.how && <p className="signin-how">{DEFAULT_STORE.wall.how}</p>}
      </div>
    </div>
  )
}
