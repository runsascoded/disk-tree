import { BrowserRouter, Routes, Route } from 'react-router-dom'
import { ScanList } from './components/ScanList'
import { ScanDetails } from './components/ScanDetails'
import './App.css'

function App() {
  return (
    <BrowserRouter>
      <div className="app">
        <Routes>
          <Route path="/" element={<ScanList />} />
          <Route path="/file/*" element={<ScanDetails />} />
          <Route path="/s3/*" element={<ScanDetails />} />
        </Routes>
      </div>
    </BrowserRouter>
  )
}

export default App
