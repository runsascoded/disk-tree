import { Navigate, Route, Routes } from 'react-router-dom'
import { HotkeysProvider } from 'use-kbd'
import { HelpCard, HelpProvider } from './Help'
import { AdminDbPage } from './AdminDbPage'
import { AdminPage } from './AdminPage'
import App from './App'
import { AuthGate, SignInPage } from './AuthGate'
import { PrivacyPage } from './PrivacyPage'
import { FilesPage } from './FilesPage'
import { AssignmentsPage } from './AssignmentsPage'
import { StagedPage } from './StagedPage'
import { OgPage } from './OgPage'
import { UserOgPage, UserPage, UsersOgPage, UsersPage } from './UserPage'
import { DEFAULT_STORE, STORES } from './stores'
import { useLoadIdentities } from './identities'

// `/files/*` → scan browser; `<store>/og` → redacted fixed-size treemap for that
// store's og:image screenshot (public, ungated — it's what unfurl crawlers
// render); every other path → the treemap app, which picks its store from the
// path. The two data-backed routes sit behind
// <AuthGate>, which shows a login wall when there's no CF Access session.
// One hotkey/omnibar registry for the whole site (SiteKbd renders the chrome
// on each page; pages register their own actions on top of the shared ones).
export default function Root() {
  useLoadIdentities()
  return (
    <HotkeysProvider config={{ storageKey: 'gcs-usage' }}>
    <HelpProvider>
    <Routes>
      {STORES.map(s => (
        <Route key={s.key} path={`${s.path.replace(/\/$/, '')}/og`} element={<OgPage store={s} />} />
      ))}
      {/* The wall as a page (ungated): where the inline "sign in" links go. */}
      <Route path="/signin" element={<SignInPage />} />
      <Route path="/privacy" element={<PrivacyPage />} />
      <Route path="/admin" element={<AuthGate><AdminPage /></AuthGate>} />
      <Route path="/admin/db" element={<AuthGate><AdminDbPage /></AuthGate>} />
      <Route path="/admin/db/:table" element={<AuthGate><AdminDbPage /></AuthGate>} />
      <Route path="/files/*" element={<AuthGate><FilesPage /></AuthGate>} />
      {/* The owner pages exist only on an attribution store; elsewhere they go home. */}
      {DEFAULT_STORE.owners ? (<>
      <Route path="/assignments" element={<AuthGate><AssignmentsPage /></AuthGate>} />
      <Route path="/users/og" element={<UsersOgPage />} />
      <Route path="/user/:id/og" element={<UserOgPage />} />
      <Route path="/users" element={<AuthGate><UsersPage /></AuthGate>} />
      <Route path="/user/:id" element={<AuthGate><UserPage /></AuthGate>} />
      </>) : (
      // caseSensitive: React Router matches case-insensitively, and a laptop
      // store's drill paths start `/Users/…`.
      <Route path="/users/*" caseSensitive element={<Navigate to="/" replace />} />
      )}
      {/* The opt-in deletion console: what the trash gesture staged, and its runs. */}
      <Route path="/staged" element={<AuthGate><StagedPage /></AuthGate>} />
      {/* Retired pages: the mark & sweep console became /staged; the review
          lenses became the home page's owner axis. Old links land somewhere sane. */}
      <Route path="/sweep" element={<Navigate to="/staged" replace />} />
      <Route path="/marks" element={<Navigate to="/" replace />} />
      <Route path="/mark" element={<Navigate to="/" replace />} />
      <Route path="*" element={<AuthGate><App /></AuthGate>} />
    </Routes>
    <HelpCard />
    </HelpProvider>
    </HotkeysProvider>
  )
}
