import { lazy, Suspense } from 'react'
import type { PlotParams } from 'react-plotly.js'

// Lazy load the heavy Plotly library (5MB+)
const Plot = lazy(() => import('react-plotly.js'))

export function LazyPlot(props: PlotParams) {
  return (
    <Suspense fallback={<div style={{ height: props.style?.height || 400, display: 'flex', alignItems: 'center', justifyContent: 'center', opacity: 0.5 }}>Loading chart...</div>}>
      <Plot {...props} />
    </Suspense>
  )
}
