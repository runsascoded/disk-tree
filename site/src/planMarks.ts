// The mark axis (specs/cw-sweep.md): keep/keep_last_ckpt/sweep intent over S3
// prefixes, resolved deepest-mark-wins per URI. Plain fetch/useState (no
// react-query). A mark is intent only; the delete gate is plan curation +
// admin dispatch on /sweep. No owner axis.
import { useCallback, useEffect, useMemo, useState } from 'react'

export type MarkAction = 'keep' | 'keep_last_ckpt' | 'sweep'

export const ACTION_LABELS: Record<MarkAction, string> = {
  keep: 'keep',
  keep_last_ckpt: 'keep last ckpt',
  sweep: 'sweep',
}

// Mark colors (also as CSS vars in app.scss for the children-table dots).
export const ACTION_COLORS: Record<MarkAction, string> = {
  keep: 'var(--mk-keep)',
  keep_last_ckpt: 'var(--mk-klc)',
  sweep: 'var(--mk-sweep)',
}

export interface Mark {
  prefix: string // s3://bucket/path/ (trailing slash)
  keep: MarkAction
  who: string
  ts: number
  note?: string | null
}

export interface ResolvedMark {
  mark: Mark
  /** true when the mark is on exactly this URI (not inherited from an ancestor). */
  own: boolean
}

/** Deepest-mark-wins resolver over the current marks (a mark on a deeper prefix
 * beats one on a shallower ancestor). */
export class MarkIndex {
  private marks: Mark[]
  constructor(marks: Mark[]) {
    this.marks = marks
  }
  resolve(uri: string): ResolvedMark | null {
    const p = uri.endsWith('/') ? uri : uri + '/'
    let best: Mark | null = null
    for (const m of this.marks) {
      if (p.startsWith(m.prefix) && (!best || m.prefix.length > best.prefix.length)) best = m
    }
    return best ? { mark: best, own: best.prefix === p } : null
  }
  all(): Mark[] {
    return this.marks
  }
}

export interface MarksApi {
  idx: MarkIndex
  canMark: boolean
  /** Set (or clear, action=null) a mark on `uri`; refetches on success. */
  mark: (uri: string, action: MarkAction | null, scan?: string) => Promise<void>
  reload: () => void
}

/** Load the mark index + a mutation. `canMark` is any authenticated viewer
 * (server enforces on POST); we read it from /api/whoami. */
export function useMarks(): MarksApi {
  const [marks, setMarks] = useState<Mark[]>([])
  const [canMark, setCanMark] = useState(false)

  const reload = useCallback(() => {
    void fetch('/api/marks', { credentials: 'include' })
      .then(r => (r.ok ? r.json() : { marks: [] }))
      .then((d: { marks?: Mark[] }) => setMarks(d.marks ?? []))
      .catch(() => setMarks([]))
  }, [])

  useEffect(() => {
    reload()
    void fetch('/api/whoami', { credentials: 'include' })
      .then(r => (r.ok ? r.json() : null))
      .then((d: { email?: string | null } | null) => setCanMark(!!d?.email))
      .catch(() => {})
  }, [reload])

  const mark = useCallback(async (uri: string, action: MarkAction | null, scan?: string) => {
    const prefix = uri.endsWith('/') ? uri : uri + '/'
    const r = await fetch('/api/marks', {
      method: 'POST', credentials: 'include',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ prefixes: [prefix], keep: action, scan }),
    })
    if (r.ok) reload()
  }, [reload])

  const idx = useMemo(() => new MarkIndex(marks), [marks])
  return { idx, canMark, mark, reload }
}
