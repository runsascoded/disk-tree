import { BrowserRouter, Routes, Route } from 'react-router-dom'
import { ThemeProvider, CssBaseline, Tooltip } from '@mui/material'
import { HotkeysProvider, ShortcutsModal, Omnibar, SequenceModal, LookupModal } from 'use-kbd'
import 'use-kbd/styles.css'
import { theme } from './theme'
import { Header } from './components/Header'
import { ScanList } from './components/ScanList'
import { ScanDetails } from './components/ScanDetails'
import { S3BucketList } from './components/S3BucketList'
import { RecentList } from './components/RecentList'
import { CompareView } from './components/CompareView'
import './App.scss'
import type { ReactNode } from 'react'

// MUI Tooltip wrapper for use-kbd
function MuiTooltip({ title, children }: { title: ReactNode; children: ReactNode }) {
  return (
    <Tooltip title={title} placement="top" arrow>
      <span style={{ display: 'inline' }}>{children}</span>
    </Tooltip>
  )
}

function App() {
  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <HotkeysProvider config={{ storageKey: 'disk-tree-hotkeys' }}>
        <BrowserRouter>
          <Header />
          <div className="app">
            <Routes>
              <Route path="/" element={<ScanList />} />
              <Route path="/file/*" element={<ScanDetails />} />
              <Route path="/s3" element={<S3BucketList />} />
              <Route path="/s3/*" element={<ScanDetails />} />
              <Route path="/ssh/*" element={<ScanDetails />} />
              <Route path="/recent" element={<RecentList />} />
              <Route path="/compare/*" element={<CompareView />} />
            </Routes>
          </div>
          <ShortcutsModal
            editable
            groupOrder={['Navigation', 'Table: Navigation', 'Table: Selection']}
            TooltipComponent={MuiTooltip}
          />
          <Omnibar />
          <SequenceModal />
          <LookupModal />
        </BrowserRouter>
      </HotkeysProvider>
    </ThemeProvider>
  )
}

export default App
