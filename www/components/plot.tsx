"use client"

import dynamic from "next/dynamic"
import { PlotParams } from "react-plotly.js";

const Plot0 = dynamic(() => import('react-plotly.js'), { ssr: false })

export function Plot(
  { clickHandler, ...props }: PlotParams & { clickHandler?: () => void }
) {
  const event = 'plotly_treemapclick'
  const handler = function(data: any) {
    console.log('Treemap click event data:', data);
  }
  return <Plot0
    {...props}
    onInitialized={(fig, div) => {
      console.log("initialized", fig, div)
      // div.on(event, clickHandler)
      div.on(event, handler)
    }}
  />
}
