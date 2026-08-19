# `@disk-tree/react`

Reusable React widgets for [disk-tree] consumers. **Chart-lib-free** (no
Plotly, no recharts, no d3) — everything is DIY SVG or plain DOM. All
components are accessor-based, so any node shape works.

Ships:

| Widget | What |
|---|---|
| [`<Treemap>`](#treemap) | Squarified layout, drill-on-click, pin-on-leaf tooltip, keyboard nav, fold-small "…" tiles, fullscreen, slot-based coloring/tooltip/legend/rollup. |
| [`<TimeSeries>`](#timeseries) / [`<BytesOverTime>`](#timeseries) | Multi-series line/area chart with hover-follow crosshair. ~330 LOC total, zero deps. |
| [`<StalenessScatter>`](#stalenessscatter) | Log-log age-vs-bytes "triage frontier": marker area ∝ a count channel, exact iso-sum-TB·year diagonals, hover/pin tooltip, click-to-drill. |
| [`<AgeHistograms>`](#agehistograms) | Byte-weighted mtime distribution per child, with a draggable threshold that reads out reclaimable bytes. |
| [`<VoronoiTreemap>`](#voronoitreemap) | Circle- (or polygon-) clipped Voronoi tessellation, areas ∝ value. Separate `@disk-tree/react/voronoi` subpath — the only part with dependencies. |
| [`useHoverPin`](#usehoverpin) | Headless hover+pin state (single pin, touch-safe, outside-click / Esc clear). |
| `squarify` / `foldSmall` | Pure layout primitives if you want to render the cells yourself. |
| `divergingColor` / `divergingInk` | Red/green diverging scale for Δ views. |
| `ageFade` / `ageDomain` / `age01` | Age lens: fade cells toward the panel background by age (`color-mix` in OKLCH, so equal age reads as equal fade across hues). Compose via the treemap's `lens` slot — stacks on any `colorForCell`. |
| `parseQuery` / `filterNodes` / `dimUnmatched` | Filter plane: substring or `/regex/` path matching (a half-typed regex degrades to substring, never throws), plus a dim transform for the treemap's `lens` slot. |
| `sumTbYears` / `formatTbYears` | Additive staleness score: Σ descendant-file size·age = `size × (now − mtime_mean)` in TB·years. Cascades like `size` (parent = Σ children), so it's honest as a treemap size accessor and exact as iso-score diagonals on a log-log (age, bytes) scatter. |

## Install

The library is published to a SHA-pinnable **dist branch** (via
[`npm-dist`][npm-dist]) — no npm publish (yet). Pin by SHA so upgrades
are explicit:

```bash
pnpm add github:runsascoded/disk-tree#<dist-sha>
```

Or, with [`pnpm-dep-source`][pds]:

```bash
pds init packages/react
pds gh disk-tree-react   # switch to the latest GH dist SHA
```

You'll also need to `import '@disk-tree/react/styles.css'` once
somewhere in your app for the default cell/tooltip styling.

`react` and `react-dom` ≥ 18 are peer deps.

## `<Treemap>`

```tsx
import { Treemap } from '@disk-tree/react'
import '@disk-tree/react/styles.css'

interface Node { name: string; size: number; children?: Node[] }

const root: Node = {
  name: 'root', size: 300,
  children: [
    { name: 'foo', size: 200, children: [
      { name: 'a.txt', size: 100 },
      { name: 'b.txt', size: 100 },
    ] },
    { name: 'bar', size: 100 },
  ],
}

<Treemap<Node>
  root={root}
  getSize={n => n.size}
  getChildren={n => n.children}
  getLabel={n => n.name}
  // Optional slots — every one has a working default.
  formatSize={n => `${n} B`}
  colorForCell={(n, path, depth) =>
    // Return `null` to defer to the default 8-slot categorical palette.
    n.isPlaceholder ? { bg: '#4a4a52', ink: '#d0d0d8' } : null
  }
  renderTooltip={(n, path) => <>{n.name}<br/>{n.size} B</>}
  renderLegend={(current, path) => …}
  renderRollup={(current, path) => …}
  onCellClick={(n, path, e) => { /* return true to skip built-in drill/pin */ }}
  onPathChange={path => console.log(path.map(n => n.name).join('/'))}
/>
```

Built-in interaction: click a branch to drill in, click a leaf to pin
its tooltip, Backspace/Escape pops the drill stack, Escape/outside-click
unpins.

### Lazy subtrees

A tree that arrives one page at a time — disk-tree's
`/api/scan?uri=…&depth=N`, a pruned static `tree.json` — can't say "this
node has children" through `getChildren`, because nothing in hand and
genuinely a leaf look identical. Two slots close that gap:

```tsx
<Treemap<Node>
  root={root}
  getSize={n => n.size}
  getChildren={n => n.children}          // what this page carried
  hasChildren={n => n.n_children > 0}    // what the server says exists
  loadChildren={async n => fetchChildren(n.uri)}
  renderLoading={n => `Loading ${n.name}…`}
  renderLoadError={(n, path, err, retry) => <button onClick={retry}>{err.message} — retry</button>}
  onChildrenLoaded={(n, path, kids) => cache(n, kids)}
/>
```

A node is drillable when it has children *or* can fetch them. Only the
**viewed** node loads — one request per drill, not one per cell on
screen — and results are cached per node for as long as `root` is
unchanged, so drilling back in is free. Rejections render with a retry;
without `loadChildren`, `hasChildren` is ignored entirely, so eagerly
materialized consumers are unaffected.

## `<TimeSeries>`

Multi-series line/area with hover-follow crosshair:

```tsx
import { TimeSeries } from '@disk-tree/react'

<TimeSeries<{ t: number; y: number }>
  series={[
    { key: 'a', label: 'foo', points: [{ t: 0, y: 10 }, { t: 1, y: 20 }] },
    { key: 'b', label: 'bar', points: [{ t: 0, y: 5 },  { t: 1, y: 8 }] },
  ]}
  getX={p => p.t}
  getY={p => p.y}
  formatX={x => new Date(x).toLocaleDateString()}
  formatY={y => y.toLocaleString()}
  yScale="linear"       // or "log"
  area={true}
/>
```

And the convenience wrapper for the disk-tree default (bytes over time):

```tsx
import { BytesOverTime } from '@disk-tree/react'

<BytesOverTime
  points={[
    { time: '2026-08-01T00:00:00Z', bytes: 1000 },
    { time: '2026-08-05T00:00:00Z', bytes: 500 },
  ]}
  formatBytes={n => `${n.toLocaleString()} B`}
/>
```

## `<StalenessScatter>`

One marker per node: x = age, y = bytes, both log — so **iso-score
diagonals are exact straight lines**, because on (years, TB) axes the
product `x·y` *is* the sum-TB·years score (`sumTbYears`). Upper-right of
a labeled diagonal is the delete-candidate frontier.

```tsx
import { StalenessScatter } from '@disk-tree/react'

const now = Date.now() / 1000

<StalenessScatter<Row>
  nodes={children}
  getAge={r => (r.mtime_mean == null ? null : now - r.mtime_mean)}  // seconds
  getSize={r => r.size}                                            // bytes
  getLabel={r => r.path}
  getWeight={r => r.n_desc}      // marker *area* ∝ this (uniform if omitted)
  onNodeClick={r => navigate(pathFor(r))}
  // Optional: getColor, formatSize, formatAge, formatScore, renderTooltip,
  // isoLines={false}, xLabel/yLabel, height.
/>
```

Nodes lacking a positive age *and* size can't sit on log axes; they're
counted in a footer note rather than silently dropped. Sizes default to
SI (1 TB = 1e12 B) so the axis agrees with the score's units.

The layout math is exported separately if you want to render your own
marks: `logDomain`, `logPos`, `logTicks`, `isoScoreSegment`,
`isoScoresForData`, `decadesBetween`, `radiusFor`.

## `<AgeHistograms>`

One column per child, y = mtime, bars weighted by **bytes** against a
shared scale — so a column's area is its byte total and the area below
the threshold line is exactly what deleting everything older reclaims.
A mean can't tell you a directory is half ancient and half fresh; this
can.

```tsx
import { AgeHistograms } from '@disk-tree/react'

// From disk-tree's /api/histogram: shared `edges`, per-child `bytes`.
<AgeHistograms<Child>
  items={data.children}
  edges={data.edges}            // epoch seconds, length = bins + 1
  getBins={c => c.bytes}        // length = edges.length - 1
  getLabel={c => c.path}
  threshold={threshold}
  onThresholdChange={(t, reclaimable) => { … }}   // drag anywhere in the plot
  normalize={false}             // true = per-column shape, area no longer comparable
/>
```

`normalize` exists because real directory trees span orders of
magnitude: honest shared scaling renders a 5 MB child next to a 2 GB one
as a hairline. Expose it as an explicitly-labeled toggle ("shape only"),
not as the default.

Math is exported too: `bytesOlderThan` (whole bins plus a linear split
of the straddling one), `totalBytes`, `peakBin`, `timeTicks`.

## `<VoronoiTreemap>`

An organic alternative to rectangles, from a **separate subpath** so the
core package stays dependency-free:

```bash
pnpm add d3-voronoi-treemap d3-hierarchy   # optional peers
```

```tsx
import { VoronoiTreemap } from '@disk-tree/react/voronoi'

<VoronoiTreemap<Row>
  items={children}
  getValue={r => r.size ?? 0}
  getLabel={r => r.path}
  shape="circle"          // or "rect", or pass an explicit `clip` polygon
  seed={uri}              // same dir ⇒ same layout, every render
  formatValue={formatSize}
  onCellClick={r => navigate(pathFor(r))}
/>
```

Read the caveats before reaching for it — they're why `<Treemap>` stays
the default:

- **It cannot render a wide value range.** The solver clamps tiny site
  weights, so a child holding 0.1% of the bytes comes out ~200% too
  large and drags the whole tessellation off target (measured: 421,710%
  worst-case error on a 13-child listing). `minShare` (default 0.005)
  drops those, and the component reports how many and how much. Rect
  treemaps have no such floor.
- **It is iterative, not exact.** `converged` / `error` report the worst
  per-cell *relative* area error; the component labels the chart when it
  misses `tolerance` (default 2%). Note the solver's own
  `convergenceRatio` is a fraction of the *clip* area, a different and
  much weaker guarantee — hence our tighter 0.001 default.
- **It is randomly seeded.** Unseeded, the same data lays out differently
  on every render; `seed` (number or string) makes it a pure function.
- Labels need room, so cells under ~900px² get none.

`voronoiLayout` is exported for custom rendering, along with
`circlePolygon`, `rectPolygon`, `polygonArea`, `polygonCentroid`,
`maxAreaError`, `toPointsAttr`, and the `mulberry32` / `hashSeed` PRNG.

## `useHoverPin`

Headless single-pin state — pairs with any hover UI:

```tsx
import { useHoverPin } from '@disk-tree/react'

function Legend({ items }: { items: string[] }) {
  const pin = useHoverPin<string>({ excludeRefs: [] })
  return (
    <>
      {items.map(k => (
        <span
          key={k}
          onMouseEnter={() => pin.hover(k)}
          onMouseLeave={() => pin.hover(null)}
          onClick={() => pin.togglePin(k)}
          style={{ opacity: pin.active === k ? 1 : 0.4 }}
        >
          {k}
        </span>
      ))}
    </>
  )
}
```

## Theming

Every color and background is a CSS custom property with a sensible
default — override on any ancestor:

```css
.dt-treemap {
  --dt-treemap-container-bg: #202024;
  --dt-treemap-ink: #d0d0d8;
  --dt-treemap-folded: #4a4a52;
  --dt-treemap-tip-bg: #1a1a1e;
  /* … */
}
```

Same pattern for `.dt-timeseries` (grid, axis, tooltip, crosshair).

## Contributing

`packages/react/` is a workspace member of the main [disk-tree]
monorepo. Iterate from the repo root:

```bash
pnpm install                                  # workspace-wide
cd packages/react
pnpm typecheck
pnpm test        # Vitest
pnpm test:watch
```

The DT app under `ui/` consumes this package via `"@disk-tree/react":
"workspace:*"` — changes flow through instantly during `pnpm dev`.

## License

Apache 2.0 (same as disk-tree).

[disk-tree]: https://github.com/runsascoded/disk-tree
[npm-dist]: https://github.com/runsascoded/npm-dist
[pds]: https://github.com/runsascoded/pnpm-dep-source
