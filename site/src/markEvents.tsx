import { useMemo } from 'react'
import { ACTION_COLORS } from './MarkControls'
import { ACTION_LABELS, useMarks } from './marks'

// The ledger's keep + owner rows, folded into one newest-first event stream —
// shared by the full `/marks` feed and the path-scoped "Mark history" section.

/** Ledger timestamps render in the viewer's zone; the tables name it in their
 *  header so "2:24 AM" is never mistaken for UTC (the ledger stores epoch s). */
export const LOCAL_TZ: string =
  new Intl.DateTimeFormat(undefined, { timeZoneName: 'short' }).formatToParts(new Date())
    .find(p => p.type === 'timeZoneName')?.value ?? 'local'
export const fmtWhen = (ts: number): string =>
  new Date(ts * 1000).toLocaleString(undefined, { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' })

export type MarkAct = 'keep' | 'keep_last_ckpt' | 'sweep' | 'clear' | 'claim' | 'release'
/** Feed-filter letters (`?mk=`), in display order. */
export const MARK_ACTS: { act: MarkAct; key: string; label: string; glyph: string; color: string }[] = [
  { act: 'keep', key: 'k', label: 'keep', glyph: '✓', color: 'var(--mk-keep)' },
  { act: 'keep_last_ckpt', key: 'l', label: 'keep last', glyph: '◐', color: 'var(--mk-keep)' },
  { act: 'sweep', key: 's', label: 'sweep', glyph: '✕', color: 'var(--mk-del)' },
  { act: 'clear', key: 'c', label: 'clear', glyph: '○', color: 'var(--line)' },
  { act: 'claim', key: 'o', label: 'assign', glyph: '◆', color: 'var(--t-oa)' },
  { act: 'release', key: 'r', label: 'unassign', glyph: '◇', color: 'var(--t-oa)' },
]

export interface MarkEvent {
  ts: number
  who: string
  prefix: string
  id: number
  /** Which axis the row came from — one action can emit both a keep and an
   * owner event for the same prefix, so (id, prefix) alone isn't unique. */
  kind: 'keep' | 'owner'
  /** The concrete action, for filtering the feed by type. */
  act: MarkAct
  label: string
  color: string
  glyph: string
  memo: string | null
}

// Action glyph — matches the treemap's state marks (✓ keep / ◐ keep-last-ckpt /
// ✕ sweep), with distinct marks for clear and ownership changes.
const ACTION_GLYPH: Record<string, string> = {
  keep: '✓', keep_last_ckpt: '◐', sweep: '✕',
}

export function useMarkEvents(): { events: MarkEvent[]; isLoading: boolean; isFetching: boolean; error: Error | null } {
  const { data, isLoading, isFetching, error } = useMarks(true)
  const events = useMemo((): MarkEvent[] => {
    if (!data) return []
    const evs: MarkEvent[] = []
    for (const r of data.keeps)
      evs.push({
        ts: r.ts, who: r.who, prefix: r.prefix, id: r.action_id, kind: 'keep', memo: r.memo,
        act: r.keep == null ? 'clear' : r.keep,
        label: r.keep == null ? 'cleared' : ACTION_LABELS[r.keep],
        color: r.keep == null ? 'var(--line)' : ACTION_COLORS[r.keep],
        glyph: r.keep == null ? '○' : ACTION_GLYPH[r.keep],
      })
    for (const r of data.owners)
      evs.push({
        ts: r.ts, who: r.who, prefix: r.prefix, id: r.action_id, kind: 'owner', memo: r.memo,
        act: r.owner == null ? 'release' : 'claim',
        label: r.owner == null ? 'unassigned' : `assigned to ${r.owner === r.who ? 'self' : r.owner}`,
        color: 'var(--t-oa)',
        glyph: r.owner == null ? '◇' : '◆',
      })
    // Newest first; action_id breaks ties within the same second.
    return evs.sort((a, b) => b.ts - a.ts || b.id - a.id)
  }, [data])
  return { events, isLoading, isFetching, error: error ?? null }
}

/** The colored action chip (glyph + label) shared by the feed and history. */
export function ActionChip({ e }: { e: MarkEvent }) {
  return (
    <span className="chip" style={{ borderColor: e.color, color: e.color }}>
      <span className="glyph">{e.glyph}</span>{e.label}
    </span>
  )
}

/** Events touching a prefix: those under it, plus ancestor marks that cover it. */
export const eventsUnder = (events: MarkEvent[], prefix: string): MarkEvent[] => {
  const p = prefix.endsWith('/') ? prefix : prefix + '/'
  return events.filter(e => {
    const ep = e.prefix.endsWith('/') ? e.prefix : e.prefix + '/'
    return ep.startsWith(p) || p.startsWith(ep)
  })
}
