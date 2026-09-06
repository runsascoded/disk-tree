import { useEffect, useState } from 'react'
import type { ScanProgress } from '../api'
import { useCapabilities } from './useCapabilities'

export function useScanProgress(): ScanProgress[] {
  const [progress, setProgress] = useState<ScanProgress[]>([])
  // No stream where nothing ever scans (the static deployment): an
  // EventSource on a 501 would reconnect forever.
  const enabled = useCapabilities()?.progress === true

  useEffect(() => {
    if (!enabled) return
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
  }, [enabled])

  return progress
}
