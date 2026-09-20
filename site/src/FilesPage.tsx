import { cloneElement, isValidElement, useMemo, type ReactElement } from 'react'
import { FileTree } from '@rdub/file-tree/react'
import { HttpStore } from '@rdub/file-tree/stores/http'
import { makeParquetViewer, type ParquetCellRenderer } from '@rdub/file-tree/renderers/parquet'
import type { ElideCtx } from '@rdub/file-tree/renderers/table'
import { useLocation } from 'react-router-dom'
import { SiteNav } from './SiteNav'
import { SiteKbd } from './SiteKbd'
import { useDocTitle } from './title'
import { Tooltip } from './Tooltip'
import { CopyName } from './CopyName'
import { CLASS_NAMES, fmtN } from './types'
import { useUnits } from './units'

// Same-origin proxy (CF Pages Function, app session required) → the raw scan
// bucket. `prefixes` in the function allow-lists listing/ + snapshots/.
const store = HttpStore('/v1/files')
const BUCKET_URI = 'gs://oa-gcs-usage-dvx'

// The parquet viewer with file-tree's `elide` strategy for wide cells (the
// sweep logs' `name`/`dir` columns are long GCS paths): the column still clips
// at 30em so the table stays scannable, and the clipped tail comes back on
// hover as a rich floating tooltip — our floating-ui `Tooltip`, wrapping the
// cell's own node — rather than the browser's slow native `title`. file-tree
// ships no tooltip dependency; the consumer supplies the panel (spec:
// file-tree `specs/table-wide-columns-and-tooltips.md`, feature #4).
// Only values long enough to plausibly clip get the hover panel; short scalars
// (sizes, timestamps, ids after `renderCell` below) would just grow a
// tooltip that repeats the cell. file-tree can't yet tell us whether a cell
// actually overflowed (`onlyWhenClipped` is its documented next axis), so
// this is a length heuristic in the meantime.
const ELIDE_MIN = 40
const elideTooltip = (ctx: ElideCtx) =>
  ctx.text == null || ctx.text.length < ELIDE_MIN ? ctx.node : <Tooltip content={<code className="elide-full">{ctx.text}</code>}>{ctx.node}</Tooltip>

const SIZE_COLS = /^(size|size_bytes|bytes|b)$/
const HEX_ID_COLS = /(^|_)(id|version_id|etag|md5|sha\w*|hash)$/
const HEX_SHOW = 8
// Object keys / paths: every row shares a long prefix, so an end-clipped cell
// shows the same 40 characters on every line and hides the part that differs.
const PATH_COLS = /^(name|path|key|dir|prefix|parent)$/
const CLASS_COLS = /^(storage_class|storage_class_id|class|cl)$/
// A byte cell as `<number> <unit>` with the unit in its own 2ch slot, so the
// numbers of a right-aligned column line up whatever the unit (`Gi`, `TiB`,
// or none: plain bytes show just the number — the column name already says
// bytes, and a `B` suffix would only echo it).
const byteCell = (n: number, fmtBytes: (b: number) => string) => {
  // the site formatter floors at Ki (a 340-byte object would read `0 Ki`); below that, the count itself
  const [num, unit] = n < 1024 ? [fmtN(n), ''] : fmtBytes(n).split(' ')
  return (
    <span style={{ fontVariantNumeric: 'tabular-nums' }}>
      {num}<span style={{ display: 'inline-block', width: '2.6ch', marginLeft: '0.35ch', textAlign: 'left', opacity: 0.75 }}>{unit ?? ''}</span>
    </span>
  )
}
// Per-cell formatting on top of file-tree's defaults: byte columns read in the
// site's units (TiB / TB, the user menu's toggle) with the exact count on
// hover; storage classes by name; long hex ids show a prefix with the full
// value (click-to-copy) on hover; path-ish columns keep their *tail* visible
// (an RTL box clips and ellipsizes at the visual left; `<bdi>` keeps the
// text itself LTR so trailing punctuation doesn't reorder — CSS-only, until
// file-tree's elide seam grows `ellipsis: 'start'`); and file-tree's default
// temporal cell carries a native `title` (the raw epoch) that we drop — the
// browser tooltip is slow, and the formatted timestamp is the value anyone
// wants.
const makeRenderCell = (fmtBytes: (b: number) => string): ParquetCellRenderer => ({ value, column, defaultNode }) => {
  const name = column.name
  if (SIZE_COLS.test(name) && (typeof value === 'number' || typeof value === 'bigint')) {
    const n = Number(value)
    return <Tooltip content={<code>{fmtN(n)} B</code>}>{byteCell(n, fmtBytes)}</Tooltip>
  }
  if (CLASS_COLS.test(name) && (typeof value === 'number' || typeof value === 'bigint' || typeof value === 'string')) {
    const label = CLASS_NAMES[String(value)]
    if (label) return <Tooltip content={<code>storage class {String(value)}</code>}><span>{label}</span></Tooltip>
  }
  if (HEX_ID_COLS.test(name) && typeof value === 'string' && value.length > HEX_SHOW + 4) {
    return <CopyName text={value}><code>{value.slice(0, HEX_SHOW)}…</code></CopyName>
  }
  if (PATH_COLS.test(name) && typeof value === 'string') {
    return <span className="cell-tail" dir="rtl"><bdi>{value}</bdi></span>
  }
  return isValidElement(defaultNode) && (defaultNode.props as { title?: string }).title != null
    ? cloneElement(defaultNode as ReactElement<{ title?: string }>, { title: undefined })
    : defaultNode
}
// Column widths are draggable (double-click a handle to auto-fit) and remembered
// per column *set*: every scan's `path-index.parquet` shares one schema, so a
// width pinned on one date carries to the others.
// file-tree's `<th>` is inline-styled at weight 500, which on the dark theme
// reads as one more row; `headerProps` is the seam for a heavier header.
const headerProps = () => ({ style: { fontWeight: 650, borderBottom: '1px solid var(--ink-3)' } })
const viewerOpts = { elide: { tooltip: elideTooltip }, headerProps, resizableColumns: { scope: 'schema' as const } }

export function FilesPage() {
  // Reflect where in the store the reader is drilled: `.../sweep/runs` →
  // "runs · Files", a parquet file → "<file> · Files", the root → "Files".
  const { pathname } = useLocation()
  const seg = decodeURIComponent(pathname.replace(/^\/files\/?/, '').replace(/\/$/, '').split('/').pop() ?? '')
  useDocTitle(seg || undefined, 'Files')
  const { fmtBytes } = useUnits()
  const parquetViewer = useMemo(() => makeParquetViewer({ ...viewerOpts, renderCell: makeRenderCell(fmtBytes) }), [fmtBytes])
  return (
    <main className="files-page" style={{ padding: '1rem', '--pad-t': '1rem', '--pad-x': '1rem', maxWidth: 1100, margin: '0 auto' } as React.CSSProperties}>
      <SiteNav />
      <p className="sub" style={{ margin: '0 0 0.6em' }}>
        Raw scan store — <code>{BUCKET_URI}</code> (<code>listing/</code> + <code>snapshots/</code>), access-gated.
      </p>
      <FileTree
        store={store}
        routeBase="/files"
        title="Scan data — raw listings + snapshots"
        parquetRenderer={parquetViewer}
      />
      <SiteKbd />
    </main>
  )
}
