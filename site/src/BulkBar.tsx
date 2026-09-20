// Bulk actions over the filter's matched prefixes (specs/selection-actions.md
// step 1). The filter already computes the outermost matched roots; this bar
// applies one keep/owner action to all of them — client-side expansion into
// exact-prefix ledger actions (server-side pattern rows are the v2 follow-up).
import { useState } from 'react'
import { type ActionPost, type MarkAction, useMarkMutations } from './marks'
import { allUsers } from './UserChip'
import { useUnits } from './units'

const CHUNK = 100      // prefixes per POST (mirrors the CLI's batches())
const WARN = 500       // confirm shows a caution above this
const HARD_CAP = 5000  // beyond this it's a rule, not a gesture — use prefix_owners

interface Pending {
  label: string
  make: (pattern: string) => ActionPost
}

export function BulkBar({ matches, scheme, query }: {
  matches: { path: string; b: number }[]
  scheme: string
  query: string
}) {
  const { post } = useMarkMutations()
  const { fmtBytes } = useUnits()
  const [pending, setPending] = useState<Pending | null>(null)
  const [assign, setAssign] = useState('')
  const [memo, setMemo] = useState('')
  const [progress, setProgress] = useState<string | null>(null)

  if (!matches.length) return null
  const bytes = matches.reduce((s, m) => s + m.b, 0)
  const over = matches.length > HARD_CAP

  const run = async (p: Pending) => {
    const note = `bulk filter:'${query}'${memo ? ` — ${memo}` : ''}`
    const actions = matches.map(m => ({ ...p.make(`${scheme}${m.path}/`), memo: note }))
    for (let i = 0; i < actions.length; i += CHUNK) {
      setProgress(`${p.label}: ${Math.min(i + CHUNK, actions.length)}/${actions.length}…`)
      await post.mutateAsync(actions.slice(i, i + CHUNK))
    }
    setProgress(null)
    setPending(null)
    setMemo('')
  }

  const states: [string, MarkAction | null][] = [['keep', 'keep'], ['sweep', 'sweep'], ['clear', null]]
  return (
    <span className="bulkbar">
      <span className="bb-scope">{matches.length.toLocaleString()} prefixes:</span>
      {states.map(([label, keep]) => (
        <button key={label} type="button" className={`act ${label}`} disabled={over || progress != null}
          onClick={() => setPending({ label, make: pattern => ({ pattern, keep }) })}>
          {label}
        </button>
      ))}
      <button type="button" className="act claim" disabled={over || progress != null}
        onClick={() => {
          const v = assign.trim()
          const owner = v ? (allUsers().find(u => u.name.toLowerCase() === v.toLowerCase())?.id ?? v) : '@me'
          setPending({ label: v ? `assign→${v}` : 'assign→me', make: pattern => ({ pattern, owner }) })
        }}>
        {assign.trim() ? 'assign' : 'assign to me'}
      </button>
      <input list="bb-assign-users" value={assign} onChange={e => setAssign(e.target.value)}
        placeholder="you" size={7} aria-label="Assign matches to user" />
      <datalist id="bb-assign-users">{allUsers().map(u => <option key={u.id} value={u.name} />)}</datalist>
      <input value={memo} onChange={e => setMemo(e.target.value)} placeholder="memo" size={10} aria-label="Bulk memo" />
      {over && <span className="bb-warn">&gt;{HARD_CAP.toLocaleString()} matches — that's a rule, not a gesture (use a `prefix_owners` glob)</span>}
      {progress && <span className="bb-progress">{progress}</span>}
      {pending && !progress && (
        <span className="bb-confirm">
          {pending.label} <b>{matches.length.toLocaleString()}</b> prefixes ({fmtBytes(bytes)})
          {matches.length > WARN ? ' — that’s a lot' : ''}?
          <button type="button" className="act go" onClick={() => void run(pending)}>confirm</button>
          <button type="button" className="act" onClick={() => setPending(null)}>cancel</button>
        </span>
      )}
      {post.error && <span className="bb-warn">{String(post.error.message ?? post.error)}</span>}
    </span>
  )
}
