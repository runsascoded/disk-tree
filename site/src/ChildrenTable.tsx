import { useEffect, useMemo, useState } from 'react'
import type { MouseEvent } from 'react'
import { intParam, useUrlState } from 'use-prms'
import { useCanMark, useCanStage } from './auth'
import { dateColor, epochDaysToDate, epochDaysToMonthShort } from './colors'
import type { UserIndexEntry } from './colors'
import { FaRegTrashCan } from 'react-icons/fa6'
import { ACTION_COLORS } from './MarkControls'
import type { MarkIndex } from './marks'
import { OwnerBar, ownerShares } from './OwnerBar'
import { looksCkpt, subtreeStateTotals } from './sweep'
import type { MarkState, MarkAxis, KlcIndex } from './sweep'
import { Tooltip } from './Tooltip'
import { elideMid } from './CopyName'
import { AssignSelect } from './AssignSelect'
import { OwnerFactChip } from './OwnerFactChip'
import { useRowSelection, useRowSelectionKeys } from './rowSelection'
import { useStage } from './plans'
import type { TreeNode } from './types'
import { fmtN } from './types'
import { useUnits } from './units'

// Sortable, paged listing of the treemap's current node's children — the
// tabular twin of the map above it (same drill: clicking a row opens it).
// Row selection + bulk marking: specs/children-table-selection.md.

type SortKey = 'n' | 'b' | 'o' | 'd' | 'a'

// Rows per page: `?n=` (default 20); the pager offers the usual sizes.
const PAGE_SIZES = [20, 50, 100, 200]

/** Names longer than this elide from the middle (the full path is in the
 *  tooltip); ~60 chars fills the column's 480px at 12px mono. */
const NAME_MAX = 60

export function ChildrenTable({ node, segs, scheme, markIdx, marksCol, klcIdx, states, userIdx, onPickUser, onOpen, readAxis, ownerAxis }: {
  /** The treemap's currently-viewed node. */
  node: TreeNode
  /** Path segments from the tree root to `node` (no scheme, no root). */
  segs: string[]
  scheme: string
  markIdx?: MarkIndex | null
  /** Show the read-only "marks" distribution column (owner claims come from
   *  `markIdx` regardless; this gates only the keep/sweep column). */
  marksCol?: boolean
  /** KLC splits, so a keep-last-ckpt subtree's bytes settle into real keep / sweep. */
  klcIdx?: KlcIndex
  /** The page's mark-state axis: list only children whose effective decision
   * is in it (`{unmarked}` = the old To-do lens). Absent = every child. */
  states?: ReadonlySet<MarkAxis> | null
  userIdx?: Map<string, UserIndexEntry>
  onPickUser?: (u: string) => void
  onOpen: (segs: string[]) => void
  /** Whether this scan carries the read / owner axes at all (see above). */
  readAxis?: boolean
  ownerAxis?: boolean
}) {
  const { fmtBytes } = useUnits()
  const stage = useStage()
  const canMark = useCanMark()
  const canStage = useCanStage()
  const [sort, setSort] = useState<{ k: SortKey; asc: boolean }>({ k: 'b', asc: false })
  const [page, setPage] = useState(0)
  const [nP, setNP] = useUrlState('n', intParam(20))
  const PAGE = PAGE_SIZES.includes(nP) ? nP : 20
  // Stagers (allowlisted users + admins, not read-only guest links) can select
  // + trash. Owner-assign / mark dots are admin-only and render only in mark mode.
  const showSel = canStage
  const showActions = !!markIdx && canMark
  const trash = (uri: string) => stage.mutate({ prefixes: [uri + '/'] })
  // One memo for the whole multi-select gesture (stored on the stage batch).
  const [memo, setMemo] = useState('')

  const kids = useMemo(() => {
    let ks = (node.c ?? []).slice()
    // Mark axis: the server already cut every node's bytes to the selected
    // states (`/api/subtree?k=`), so a child with bytes left holds some — by
    // its own decision or a deeper mark's. Judging children by their own
    // effective decision here was wrong: the marks that keep bytes alive
    // under a swept band usually sit below the pixel-budgeted tree.
    if (states) ks = ks.filter(k => !k.n.startsWith('(') && k.b > 0)
    const dir = sort.asc ? 1 : -1
    const val = (n: TreeNode): number | string =>
      sort.k === 'n' ? n.n
      : sort.k === 'b' ? n.b
      : sort.k === 'o' ? n.o
      : sort.k === 'a' ? n.a ?? -Infinity
      : n.d ?? -Infinity
    return ks.sort((a, b) => {
      const va = val(a)
      const vb = val(b)
      return (typeof va === 'string' ? (va as string).localeCompare(vb as string) : (va as number) - (vb as number)) * dir
    })
  }, [node, sort, states])
  // A new listing (drill, sort, lens) starts on page 1.
  useEffect(() => setPage(0), [node, sort, states])

  // Columns the data can't fill are left out (no access logs → no `read`; no
  // attribution and no marks → no `owner(s)`): a store without those axes
  // shouldn't read as a table of dashes.
  // The caller says whether the *scan* carries each axis (so a page whose
  // children happen to be unread / unowned still shows the column as dashes,
  // which is information); without that, fall back to the rows on the page.
  const hasRead = readAxis ?? kids.some(k => k.a != null)
  const hasOwners = !!markIdx || (ownerAxis ?? kids.some(k => k.us?.length))

  // Created-month ink: an age gradient over the listed rows' range, so a
  // column of "May / Jun / Apr" also reads at a glance as older ↔ newer.
  const [dMin, dMax] = useMemo(() => {
    const ds = kids.map(k => k.d).filter((d): d is number => d != null)
    return ds.length ? [Math.min(...ds), Math.max(...ds)] : [0, 0]
  }, [kids])
  const ageInk = (d: number) => dateColor(dMax > dMin ? (d - dMin) / (dMax - dMin) : 1)

  const th = (k: SortKey, label: string, num = true) => (
    <th
      className={(num ? 'num ' : '') + 'sortable' + (sort.k === k ? ' on' : '')}
      onClick={() => setSort(s => ({ k, asc: s.k === k ? !s.asc : k === 'n' }))}
      title="sort"
    >
      {label}{sort.k === k ? (sort.asc ? ' ▲' : ' ▼') : ''}
    </th>
  )
  const pages = Math.max(1, Math.ceil(kids.length / PAGE))
  const pg = Math.min(page, pages - 1)
  const shown = useMemo(() => kids.slice(pg * PAGE, (pg + 1) * PAGE), [kids, pg, PAGE])
  const uriOfKid = (k: TreeNode) => scheme + [...segs, k.n].join('/')
  // Rows select (click / shift / ⌘, checkboxes, j/k) into one set keyed by
  // uri; the bar above the table marks or assigns the whole selection at once.
  const selectable = useMemo(() => shown.filter(k => !k.n.startsWith('(')), [shown])
  const sel = useRowSelection(selectable, uriOfKid)
  useRowSelectionKeys(sel, 'tbl', 'Children table')
  // Selection survives paging and sort by key, but not a drill: the rows
  // picked under one path aren't candidates for a bulk mark under another.
  const path = segs.join('/')
  const clearSel = sel.clear
  useEffect(() => clearSel(), [path, clearSel])
  const selUris = [...sel.selected]
  const trashSel = () => { if (selUris.length) stage.mutate({ prefixes: selUris.map(u => u + '/'), note: memo }, { onSuccess: () => { sel.clear(); setMemo('') } }) }
  const selBytes = kids.filter(k => sel.selected.has(uriOfKid(k))).reduce((s, k) => s + k.b, 0)
  // Everything a row derives from the tree and the ledger — owner shares, the
  // resolved mark and claim, the state bar's subtree walk, the last-ckpt
  // offer — computed once per page of rows × ledger, so a selection change
  // (which re-renders the table) rebuilds only the JSX.
  const rowData = useMemo(() => shown.map(k => {
    const synthetic = k.n.startsWith('(')
    const kidSegs = [...segs, k.n]
    const uri = scheme + kidSegs.join('/')
    const cl = markIdx && !synthetic ? markIdx.claimOf(uri) : null
    const mk = markIdx && !synthetic ? markIdx.resolve(uri) : null
    const totals = markIdx && !synthetic && k.b ? subtreeStateTotals(k, uri, markIdx, klcIdx) : null
    return { k, synthetic, kidSegs, uri, shares: ownerShares(k), cl, mk, totals, ckpt: !synthetic && looksCkpt(k, uri), si: selectable.indexOf(k) }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }), [shown, path, scheme, markIdx, klcIdx, selectable])
  // Every hook above runs on every render: an empty page (the mark axis or
  // a drill can leave no children) must not shorten the hook list, or React
  // throws "Rendered fewer hooks than expected" on the way in.
  if (!kids.length) {
    return states
      ? <section className="children-tbl"><p className="tab-note">No prefix under this view is {[...states].join(' / ')}.</p></section>
      : null
  }
  const selBar = showSel && sel.selected.size > 0 && (
    <span className="sel-bar">
      <b>{sel.selected.size}</b> selected · {fmtBytes(selBytes)}
      <span className="acts">
        <Tooltip content="Optional: one note for this deletion — why these prefixes go. Stored with the batch, visible to the admin who dispatches.">
          <input className="memo" value={memo} onChange={e => setMemo(e.target.value)} placeholder="note (optional)" aria-label="deletion note" />
        </Tooltip>
        <Tooltip content={<>Stage every selected prefix for deletion — an admin approves and dispatches from <b>/staged</b></>}>
          <button type="button" className="trash" onClick={trashSel} aria-label="trash selected"><FaRegTrashCan /> trash {sel.selected.size}</button>
        </Tooltip>
        {showActions && <AssignSelect prefix={selUris.map(u => u + '/')} label={`assign ${sel.selected.size}…`} />}
        <button type="button" className="quiet" onClick={sel.clear}>deselect</button>
      </span>
    </span>
  )
  const pager = pages > 1 && (
    <span className="pg">
      <button type="button" disabled={pg === 0} onClick={() => setPage(0)} aria-label="first page">«</button>
      <button type="button" disabled={pg === 0} onClick={() => setPage(pg - 1)} aria-label="previous page">‹</button>
      <span>{pg * PAGE + 1}–{Math.min(kids.length, (pg + 1) * PAGE)} of {kids.length.toLocaleString('en-US')}</span>
      <button type="button" disabled={pg >= pages - 1} onClick={() => setPage(pg + 1)} aria-label="next page">›</button>
      <button type="button" disabled={pg >= pages - 1} onClick={() => setPage(pages - 1)} aria-label="last page">»</button>
      <select className="psize" value={PAGE} onChange={e => { setNP(+e.target.value); setPage(0) }} aria-label="rows per page">
        {PAGE_SIZES.map(n => <option key={n} value={n}>{n} / page</option>)}
      </select>
    </span>
  )
  // The top bar holds the pager on the left and the selection on the right.
  // It renders only when it has something to show — an always-present empty
  // bar read as a stray gap between the map and the table (a phone's whole
  // first fold). The first selection therefore shifts the rows down by one
  // bar; the row just clicked stays selected, so nothing is lost.
  // A click on the section's own dead space (not a row / control) deselects.
  const clearOnDeadClick = (e: MouseEvent) => {
    if (sel.selected.size && !(e.target as HTMLElement).closest('tr, button, input, select, a, .sel-bar')) sel.clear()
  }
  // MarkState of the bytes UNDER a row: keep / last-ckpt / sweep / undecided, as a
  // bar — a directory is rarely one thing (an inherited keep with swept
  // subtrees, a KLC with its kept step), and a single word hid that.
  const STATE_COLORS: Record<MarkState, string> = { keep: ACTION_COLORS.keep, keep_last_ckpt: ACTION_COLORS.keep_last_ckpt, sweep: ACTION_COLORS.sweep, unmarked: 'var(--other)' }
  const STATE_LABELS: Record<MarkState, string> = { keep: 'keep', keep_last_ckpt: 'last ckpt', sweep: 'sweep', unmarked: 'undecided' }
  const stateBar = (k: TreeNode, f: Record<MarkState, number> | null) => {
    if (!f) return <span className="none">—</span>
    const parts = (Object.keys(f) as MarkState[]).filter(x => f[x] > 0)
    return (
      <Tooltip content={
        <span className="own-tip">
          <div>Bytes under this directory by decision:</div>
          {parts.map(x => <div className="row" key={x}><i style={{ background: STATE_COLORS[x] }} />{STATE_LABELS[x]}<span className="n">{fmtBytes(f[x])} · {Math.round((100 * f[x]) / k.b)}%</span></div>)}
        </span>
      }>
        <span className="own-bar state-bar" style={{ width: 70 }} aria-label="marks distribution">
          {parts.map(x => <i key={x} style={{ width: `${(100 * f[x]) / k.b}%`, background: STATE_COLORS[x] }} />)}
        </span>
      </Tooltip>
    )
  }
  return (
    <section className="children-tbl" onClick={clearOnDeadClick}>
      {(pager || (showSel && sel.selected.size > 0)) && <div className="pager top">{pager}{selBar}</div>}
      <table className="worklist selectable">
        <thead>
          <tr>
            {showSel && <th className="col-sel"><input type="checkbox" title="select / deselect this page (⇧x)" checked={sel.pageAll} onChange={sel.togglePage} /></th>}
            {th('n', 'name', false)}
            {th('b', 'bytes')}
            <th className="num">share</th>
            {th('o', 'objects')}
            {th('d', 'created')}
            {hasRead && th('a', 'read', false)}
            {hasOwners && <th>owner(s)</th>}
            {marksCol && <th>marks</th>}
            {showSel && <th>actions</th>}
          </tr>
        </thead>
        <tbody>
          {rowData.map(({ k, synthetic, kidSegs, uri, shares, cl, totals, si }) => {
            return (
              <tr key={k.n} ref={si >= 0 ? sel.rowRef(si) : undefined} {...(si >= 0 && showSel ? sel.rowProps(si) : {})}>
                {showSel && <td className="col-sel">{!synthetic && <input type="checkbox" checked={sel.isSelected(k)} onChange={() => sel.toggle(si)} />}</td>}
                <td className="prefix">
                  <Tooltip content={<code className="elide-full">{uri}</code>}>
                    {synthetic || !k.c?.length ? (
                      <span>{elideMid(k.n, NAME_MAX)}</span>
                    ) : (
                      <a role="link" tabIndex={0} onClick={() => onOpen(kidSegs)}>{elideMid(k.n, NAME_MAX)}</a>
                    )}
                  </Tooltip>
                </td>
                <td className="num">{fmtBytes(k.b)}</td>
                <td className="num">{node.b ? ((100 * k.b) / node.b).toFixed(1) : 0}%</td>
                <td className="num">{fmtN(k.o)}</td>
                <td className="created num">{k.d != null ? (() => {
                  const [mon, yr] = epochDaysToMonthShort(k.d).split(' ')
                  return <span className="cm"><i style={{ background: ageInk(k.d) }} /><span className="mon">{mon}{yr && <span className="yr"> {yr}</span>}</span></span>
                })() : '—'}</td>
                {hasRead && <td title={k.a != null ? 'most recent GET/HEAD/LIST under this prefix (access logs)' : undefined}>
                  {k.a != null ? epochDaysToDate(k.a) : '—'}
                </td>}
                {/* An assignee, or a single attributed owner, by name; a mix
                    as a bar (names and shares on hover). */}
                {hasOwners && (
                <td className="owners">
                  {cl ? <OwnerFactChip who={cl.who} assigned={{ by: cl.by, ts: cl.ts, memo: cl.memo }} />
                    : shares.length === 1 && shares[0][1] >= 0.98 * k.b ? <OwnerFactChip who={shares[0][0]} inferred={k.pv ?? null} />
                    // Picking a person from a ROW's bar means "this directory,
                    // theirs only": drill into the row, then apply the lens.
                    : shares.length ? <OwnerBar node={k} userIdx={userIdx} width={70} onPickUser={onPickUser && !synthetic ? u => { onOpen(kidSegs); onPickUser(u) } : undefined} />
                    : <span className="none">—</span>}
                </td>
                )}
                {marksCol && <td className="state">{stateBar(k, totals)}</td>}
                {showSel && (
                  <td className="actions">
                    {synthetic ? null : (
                      <>
                        <Tooltip content="Stage this prefix for deletion — an admin approves and dispatches">
                          <button type="button" className="trash" onClick={() => trash(uri)} aria-label="trash"><FaRegTrashCan /></button>
                        </Tooltip>
                        {showActions && <AssignSelect prefix={uri + '/'} assigned={cl?.who ?? null} compact />}
                      </>
                    )}
                  </td>
                )}
              </tr>
            )
          })}
        </tbody>
        <tfoot>
          {/* Totals of the LISTED rows — under a scoping lens (to-do) this is
              the lens total, not the parent node's. */}
          <tr className="total-row">
            {showSel && <td />}
            <td>total{kids.length !== (node.c ?? []).length ? ` (${kids.length} shown)` : ''}</td>
            <td className="num">{fmtBytes(kids.reduce((s, k) => s + k.b, 0))}</td>
            <td className="num">{node.b ? ((100 * kids.reduce((s, k) => s + k.b, 0)) / node.b).toFixed(1) : 0}%</td>
            <td className="num">{kids.reduce((s, k) => s + k.o, 0).toLocaleString('en-US')}</td>
            <td colSpan={1 + (hasRead ? 1 : 0) + (hasOwners ? 1 : 0) + (marksCol ? 1 : 0) + (showSel ? 1 : 0)} />
          </tr>
        </tfoot>
      </table>
      {pager && <div className="pager">{pager}</div>}
    </section>
  )
}
