import { Explain } from './Help'
import { type ReactNode, useState } from 'react'
import { DEFAULT_STORE } from './stores'
import { Avatar } from './Avatar'
import { UserChip, ghHandle, shortName } from './UserChip'
import { AssignSelect } from './AssignSelect'
import { OwnerFactChip } from './OwnerFactChip'
import { signInUrl, useCanMark } from './auth'
import type { Mark, MarkAction, MarkIndex } from './marks'
import { ACTION_LABELS, useMarkMutations } from './marks'
import { looksCkpt } from './sweep'
import { Tooltip } from './Tooltip'
import type { TreeNode } from './types'
import type { UserIndexEntry } from './colors'
import { ClassBar, OwnerBar, ownerShares } from './OwnerBar'

// Marking UI for one prefix — rendered inside the pinned treemap tooltip and
// above the map for the current drill root. Shows the node's effective state
// (most-recent-wins over ancestor marks) with provenance, and buttons to
// set/clear its mark. "No mark" is *undecided* (the review backlog) and
// renders neutral — never in sweep red, which reads as a sweep decision
// (the state chips and rollup use the same ○ / gray). Marking a prefix that
// has deeper marks inside repaints them (recency beats specificity) — hence
// the inline override confirm.

// KLC stays amber wherever it renders *as itself* (chips, buttons, history) —
// green made it indistinguishable from keep. Aggregations (state cells,
// stripes, rollups) instead *decompose* it into real keep/sweep proportions
// via `klcSplits` (sweep.ts): last-ckpt child kept, siblings swept.
export const ACTION_COLORS: Record<MarkAction, string> = {
  keep: 'var(--mk-keep)',
  keep_last_ckpt: 'var(--mk-klc)',
  sweep: 'var(--mk-del)',
}

export const KLC_TIP =
  'Keep only the newest checkpoint under this prefix: the sweep deletes older step-/checkpoint-numbered dirs and keeps the highest step in each run. Offered on checkpoint-shaped directories.'

// Per-button tooltips. Nothing here deletes on click — "sweep" only *marks*
// for the mark-and-sweep campaign; removal happens after the deadline.
export const KEEP_TIP =
  'Keep this prefix — protect everything under it from the sweep. Takes no immediate action; nothing is deleted.'
export const SWEEP_TIP =
  'Mark this prefix for the sweep. Takes no immediate action — deletions happen only through reviewed sweep runs (approved band by band on /sweep).'
export const ASSIGN_TIP =
  'Assign this prefix an owner — you by default, or anyone you name. Overrides the inferred owner (paths, W&B runs, sidecars) and pulls it out of the “Unowned” pool so it counts as that person’s data.'
export const NOTE_TIP =
  'Optional memo stored on the keep/sweep/clear action you take next — a reason others (and future you) can see in the mark history. Not the same as the owner.'
export const underTip = (n: number): string =>
  `${n} deeper prefix${n === 1 ? '' : 'es'} inside this directory carry their own keep/sweep mark. Marking here repaints all of them (the newest mark wins) — drill in to see them.`

export const clearTip = (own: boolean): string =>
  own ? 'Remove this mark — back to unmarked (undecided).'
      : 'Override the inherited mark: explicitly unmark this subtree.'

/** Short date for a mark's timestamp, e.g. "Aug 24, 2026". */
export const fmtMarkDate = (ts: number): string =>
  new Date(ts * 1000).toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' })

/** Tooltip body for a state chip: who set the mark, when, and whether inherited. */
export function markProvenance(mark: Mark, own: boolean): ReactNode {
  return (
    <span className="mark-prov">
      <span className="sw" style={{ background: ACTION_COLORS[mark.action] }} />
      <Avatar github={ghHandle(mark.who)} name={shortName(mark.who)} size={16} />
      <span>
        <b>{ACTION_LABELS[mark.action]}</b> by {shortName(mark.who)} · {fmtMarkDate(mark.ts)}
        {!own && <> · inherited from <code>{mark.prefix}</code></>}
      </span>
    </span>
  )
}

/**
 * `node`: the tree node behind `uri`, when the caller has it — gates the
 * `keep_last_ckpt` option to checkpoint-shaped dirs and feeds the ownership
 * bar. Omit (typed prefixes, below the depth cap) and the option stays
 * available. `lensed`: the view is already filtered to one owner, so `node`'s
 * user split is that person alone. `userIdx`: the site's user palette, so the
 * ownership bar's colors match the color-by-owner map.
 */
export function MarkControls({ uri, idx, node, lensed, userIdx, onPickUser }: {
  uri: string; idx: MarkIndex; node?: TreeNode; lensed?: boolean; userIdx?: Map<string, UserIndexEntry>; onPickUser?: (u: string) => void
}) {
  const { put, claim } = useMarkMutations()
  const canMark = useCanMark()
  const [note, setNote] = useState('')
  // The select is a DRAFT until "save": picking an option doesn't POST.
  const [draft, setDraft] = useState<MarkAction | 'none' | null>(null)
  const [pending, setPending] = useState<{ action: MarkAction | null } | null>(null)
  if (!uri.startsWith('gs://') || uri.indexOf('/', 5) === -1) {
    // store root ("gs://…" with no bucket path) — nothing markable
    return null
  }
  const prefix = uri.endsWith('/') ? uri : uri + '/'
  const { mark, own, under } = idx.resolve(uri)
  const cl = idx.claimOf(uri)
  const klcOk = node ? looksCkpt(node, uri) : true
  const ov = idx.overridesOf(uri)

  const write = (action: MarkAction | null) => {
    setPending(null)
    setDraft(null)
    put.mutate({ prefix, action, note: note.trim() || undefined }, { onSuccess: () => setNote('') })
  }
  // Recency semantics: a mark here repaints every deeper mark in the subtree.
  // Confirm before doing that to a subtree someone already reviewed.
  const set = (action: MarkAction | null) => (ov.n > 0 ? setPending({ action }) : write(action))

  // The decision as ONE control: a select colored by the current state.
  // `keep_last_ckpt` is offered on checkpoint-shaped dirs (and kept visible
  // when it's the current value, so the select never shows a phantom).
  const cur: MarkAction | 'none' = mark?.action ?? 'none'
  const options: { v: MarkAction | 'none'; label: string; tip: string }[] = [
    { v: 'none', label: '—', tip: 'No mark on the whole directory (its subtrees may still carry their own).' },
    { v: 'keep', label: 'keep', tip: KEEP_TIP },
    ...(klcOk || cur === 'keep_last_ckpt' ? [{ v: 'keep_last_ckpt' as const, label: 'keep last ckpt', tip: KLC_TIP }] : []),
    { v: 'sweep', label: 'sweep', tip: SWEEP_TIP },
  ]
  const stateTip = (
    <span className="state-tip">
      <div>The keep/sweep decision covering this <b>whole</b> directory — its own mark, or one inherited from a directory above. Usually there is none: decisions live on deeper prefixes (see “marked inside”). Saving one here repaints everything under it.</div>
      {options.map(o => <div key={o.v}><b>{o.label}</b> — {o.tip}</div>)}
      {mark && !own && <div>Saving <b>—</b> explicitly unmarks this subtree, overriding the inherited mark.</div>}
    </span>
  )
  const sel = draft ?? cur

  // Ownership: the assignment if there is one; otherwise the scan's
  // attribution — one person by name, a mix as a bar (OwnerBar).
  const shares = ownerShares(node ?? { n: '', b: 0, o: 0 } as TreeNode)
  const soleOwner = !cl && shares.length > 0 && shares[0][1] >= 0.98 * node!.b ? shares[0][0] : null
  const mixed = !cl && !soleOwner && shares.length > 0

  return (
    <div className="mark-controls" onClick={e => e.stopPropagation()}>
      {/* The decision: one select (or, read-only, the word), then where the
          decision came from and what it covers. */}
      <span className="state">
        <Explain text={stateTip}><span className="lbl">mark all</span></Explain>
        {canMark ? (
          <>
            <select
              className={`state-sel ${sel}${mark && !own && sel === cur ? ' inh' : ''}${draft && draft !== cur ? ' dirty' : ''}`}
              value={sel}
              aria-label="Keep / sweep decision for this whole directory"
              onChange={e => setDraft(e.target.value as MarkAction | 'none')}
            >
              {options.map(o => <option key={o.v} value={o.v}>{o.label}</option>)}
            </select>
            <Explain text={NOTE_TIP}>
              <input className="note" value={note} onChange={e => setNote(e.target.value)} placeholder="note (optional)" size={14} />
            </Explain>
            <button
              type="button" className="save" disabled={!draft || draft === cur}
              onClick={() => draft && set(draft === 'none' ? null : draft)}
            >save</button>
          </>
        ) : (
          <b className={`state-word ${cur}`}>{mark ? ACTION_LABELS[mark.action] : '—'}</b>
        )}
        {/* Provenance stays one short line; the inherited-from prefix and the
            marker's memo (often a sentence of boilerplate) live in its tooltip,
            flagged by a "memo" tag when there is one. */}
        {mark && (
          <Tooltip content={
            <span className="prov-tip">
              {!own && <div>inherited from <code>{mark.prefix}</code></div>}
              {mark.note && <div className="memo">“{mark.note}”</div>}
              {own && !mark.note && <div>set directly on this prefix</div>}
            </span>
          }>
            <span className="prov has-tt">
              {own ? 'set by' : 'inherited ·'}{' '}
              <Avatar github={ghHandle(mark.who)} name={shortName(mark.who)} size={14} /> {shortName(mark.who)}
              {' · '}{new Date(mark.ts * 1000).toLocaleDateString()}
              {mark.note && <span className="memo-tag">memo</span>}
            </span>
          </Tooltip>
        )}
        {under > 0 && (
          <Tooltip content={underTip(under)}>
            <span className="under has-tt">
              {under} marked inside
              <span className="split"> · <i style={{ background: 'var(--mk-keep)' }} />{ov.keeps} keep · <i style={{ background: 'var(--mk-del)' }} />{under - ov.keeps} sweep</span>
            </span>
          </Tooltip>
        )}
      </span>
      {/* Ownership — assigned owner, or the attribution bar — and the assign
          control folded behind one button until it's wanted. Only where the
          store has attribution + claims (`Store.owners`). */}
      {DEFAULT_STORE.owners && <span className="owner">
        <span className="lbl">{mixed ? 'owners' : 'owner'}</span>
        {cl
          ? <><OwnerFactChip who={cl.who} assigned={{ by: cl.by, ts: cl.ts, memo: cl.memo }} /><span className="prov">assigned {fmtMarkDate(cl.ts)}</span></>
          : soleOwner
            ? <OwnerFactChip who={soleOwner} inferred={node?.pv ?? null} />
            : mixed
              ? <OwnerBar node={node!} userIdx={userIdx} onPickUser={onPickUser} note="Nobody has assigned this prefix. The scan attributes its bytes to:" />
              // The lens keeps only one person's bytes: with none here, the
              // attributed owners are people the lens hides — not "none".
              : <span className="none" title={lensed ? 'the owner filter hides other people\'s bytes here' : undefined}>—</span>}
        {canMark && <AssignSelect prefix={prefix} assigned={cl?.who ?? null} />}
        {node && node.cb && Object.values(node.cb).some(b => b > 0) && (
          <span className="classes">
            <span className="lbl">class</span>
            <ClassBar node={node} />
          </span>
        )}
        {!canMark && (
          <span className="guest-note">
            viewing as guest — <a href={signInUrl()}>sign in</a> with your email to mark
          </span>
        )}
      </span>}
      {pending && (
        <span className="override-confirm">
          overrides <b>{ov.n}</b> more-specific mark{ov.n === 1 ? '' : 's'} inside
          {ov.keeps > 0 && <> (<b>{ov.keeps}</b> currently kept)</>} —
          <button type="button" onClick={() => write(pending.action)}>
            {pending.action === null ? 'clear' : ACTION_LABELS[pending.action]} anyway
          </button>
          <button type="button" onClick={() => setPending(null)}>cancel</button>
        </span>
      )}
      {(put.error ?? claim.error) && <span className="err">{(put.error ?? claim.error)!.message}</span>}
    </div>
  )
}
