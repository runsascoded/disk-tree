import { floor } from "@rdub/base"

export function relativeDateStr(date: Date, now: Date): string {
  const diff = now.getTime() - date.getTime()
  const seconds = floor(diff / 1000)
  const minutes = floor(seconds / 60)
  const hours = floor(minutes / 60)
  const days = floor(hours / 24)
  const weeks = floor(days / 7)

  if (seconds < 60) {
    return `${seconds}s`
  } else if (minutes < 60) {
    return `${minutes}m`
  } else if (hours < 24) {
    return `${hours}hr`
  } else if (days < 7) {
    return `${days}d`
  } else {
    // Show months, years+months, or days
    const year0 = date.getFullYear()
    const year1 = now.getFullYear()
    const month0 = date.getMonth()
    const month1 = now.getMonth()
    const day0 = date.getDate()
    const day1 = now.getDate()
    const fullMonthsAgo = (year1 - year0) * 12 + (month1 - month0) + (day0 < day1 ? -1 : 0)
    if (fullMonthsAgo == 0) {
      return `${weeks}wk`
    } else if (fullMonthsAgo >= 12) {
      const fullYearsAgo = floor(fullMonthsAgo / 12)
      const monthsRemainder = fullMonthsAgo % 12
      return `${fullYearsAgo}y` + (monthsRemainder ? ` ${monthsRemainder}m` : '')
    } else {
      return `${fullMonthsAgo}mo`
    }
  }
}

export function Time(
  { time, now }: {
    time: string | number
    now?: Date
  }
) {
  const date = new Date(time)
  now = now ?? new Date()
  const options: Intl.DateTimeFormatOptions = {
    year: "2-digit",
    month: "numeric",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }
  const tooltip = date.toLocaleString("en-US", options)
  return <span title={tooltip}>
    {relativeDateStr(date, now)}
  </span>
}
