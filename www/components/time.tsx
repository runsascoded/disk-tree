'use client'

import { Tooltip } from "@/components/Tooltip"
import { fmtDate, relativeDateStr } from "@/src/time"

export function Time(
  { time, now }: {
    time: string | number
    now?: Date
  }
) {
  const date = new Date(time)
  now = now ?? new Date()
  return (
    <Tooltip title={fmtDate(date)}>
      <span>{relativeDateStr(date, now)}</span>
    </Tooltip>
  )
}

export function Scanned({ time, now }: { time: string, now: Date }) {
  const date = new Date(time)
  return (
    <Tooltip title={fmtDate(date)}>
      <span>{relativeDateStr(date, now)}</span>
    </Tooltip>
  )
}
