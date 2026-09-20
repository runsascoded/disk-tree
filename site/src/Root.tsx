import { Navigate, Route, Routes } from 'react-router-dom'
import { HotkeysProvider } from 'use-kbd'
import { HelpLine, HelpProvider } from './Help'
import { AdminDbPage } from './AdminDbPage'
import { AdminPage } from './AdminPage'
import App from './App'
import { AuthGate } from './AuthGate'
import { FilesPage } from './FilesPage'
import { MarksPage } from './MarksPage'
import { AssignmentsPage } from './AssignmentsPage'
import { SweepPage } from './SweepPage'
import { OgPage } from './OgPage'
import { UserOgPage, UserPage, UsersOgPage, UsersPage } from './UserPage'
import { DEFAULT_STORE, STORES } from './stores'

// `/files/*` → scan browser; `<store>/og` → redacted fixed-size treemap for that
// store's og:image screenshot (public, ungated — it's what unfurl crawlers
// render); every other path → the treemap app, which picks its store from the
// path. The two data-backed routes sit behind
// <AuthGate>, which shows a login wall when there's no CF Access session.
// One hotkey/omnibar registry for the whole site (SiteKbd renders the chrome
// on each page; pages register their own actions on top of the shared ones).
export default function Root() {
  return (
    <HotkeysProvider config={{ storageKey: 'gcs-usage' }}>
    <HelpProvider>
    <Routes>
      {STORES.map(s => (
        <Route key={s.key} path={`${s.path.replace(/\/$/, '')}/og`} element={<OgPage store={s} />} />
      ))}
      <Route path="/admin" element={<AuthGate><AdminPage /></AuthGate>} />
      <Route path="/admin/db" element={<AuthGate><AdminDbPage /></AuthGate>} />
      <Route path="/admin/db/:table" element={<AuthGate><AdminDbPage /></AuthGate>} />
      <Route path="/files/*" element={<AuthGate><FilesPage /></AuthGate>} />
      {/* The ledger pages exist only on a marks store; elsewhere they go home. */}
      {DEFAULT_STORE.marks ? (<>
      <Route path="/marks" element={<AuthGate><MarksPage /></AuthGate>} />
      <Route path="/assignments" element={<AuthGate><AssignmentsPage /></AuthGate>} />
      <Route path="/users/og" element={<UsersOgPage />} />
      <Route path="/user/:id/og" element={<UserOgPage />} />
      <Route path="/users" element={<AuthGate><UsersPage /></AuthGate>} />
      <Route path="/user/:id" element={<AuthGate><UserPage /></AuthGate>} />
      </>) : (
      <Route path="/users/*" element={<Navigate to="/" replace />} />
      )}
      <Route path="/sweep" element={<AuthGate><SweepPage /></AuthGate>} />
      {/* The review lenses became the home page's mark/owner axes — /mark is just the map. */}
      <Route path="/mark" element={<Navigate to="/" replace />} />
      <Route path="*" element={<AuthGate><App /></AuthGate>} />
    </Routes>
    <HelpLine />
    </HelpProvider>
    </HotkeysProvider>
  )
}
