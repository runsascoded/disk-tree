import { BrowserRouter, Routes, Route } from 'react-router-dom'
import { ThemeProvider, CssBaseline } from '@mui/material'
import { theme } from './theme'
import { Header } from './components/Header'
import { ScanList } from './components/ScanList'
import { ScanDetails } from './components/ScanDetails'
import { S3BucketList } from './components/S3BucketList'
import './App.css'

function App() {
  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <BrowserRouter>
        <Header />
        <div className="app">
          <Routes>
            <Route path="/" element={<ScanList />} />
            <Route path="/file/*" element={<ScanDetails />} />
            <Route path="/s3" element={<S3BucketList />} />
            <Route path="/s3/*" element={<ScanDetails />} />
          </Routes>
        </div>
      </BrowserRouter>
    </ThemeProvider>
  )
}

export default App
