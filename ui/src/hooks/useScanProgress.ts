import { useEffect, useState } from 'react'
import type { ScanProgress } from '../api'

export function useScanProgress(): ScanProgress[] {
  const [progress, setProgress] = useState<ScanProgress[]>([])

  useEffect(() => {
    const eventSource = new EventSource('/api/scans/progress/stream')

    eventSource.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data) as ScanProgress[]
        setProgress(data)
      } catch (e) {
        console.error('Failed to parse SSE data:', e)
      }
    }

    eventSource.onerror = (e) => {
      console.error('SSE error:', e)
      // EventSource will automatically reconnect
    }

    return () => {
      eventSource.close()
    }
  }, [])

  return progress
}
