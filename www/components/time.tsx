import { Tooltip } from "@/components/Tooltip"
import { fmtDate, relativeDateStr } from "@/src/time"
import { useState } from "react"
import { IoMdRefresh } from "react-icons/io"
import css from "./time.module.scss"

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

export function Scanned({ time, now, onRefresh, }: { time: string, now: Date, onRefresh: () => void }) {
  const date = new Date(time)
  const [ showRefresh, setShowRefresh ] = useState(false)
  return (
    <div
      className={css.scanned}
      onMouseEnter={() => setShowRefresh(true)}
      onMouseLeave={() => setShowRefresh(false)}
    >
      <Tooltip title={fmtDate(date)}>
        <span>{relativeDateStr(date, now)}</span>
      </Tooltip>
      {
        showRefresh &&
          <IoMdRefresh
              className={css.icon}
              onClick={(e) => {
                e.stopPropagation()
                onRefresh()
              }}
          />
      }
    </div>
  )
}
