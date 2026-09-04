# `@disk-tree/react`

disk-tree's **bytes/mtime/age-domain** React widgets, built on the
[`@rdub/treemap`](../treemap) core — which this package **re-exports**, so
`import { Treemap, … } from '@disk-tree/react'` keeps working. New / non-disk
consumers should depend on `@rdub/treemap` directly; reach for this package when
you want the disk-flavored views below.

**Chart-lib-free** (DIY SVG), accessor-based.

| Widget | What |
|---|---|
| [`<TimeSeries>`](#timeseries) / [`<BytesOverTime>`](#timeseries) | Multi-series line/area chart with hover-follow crosshair. Zero deps. |
| [`<StalenessScatter>`](#stalenessscatter) | Log-log age-vs-bytes "triage frontier": marker area ∝ a count channel, exact iso-sum-TB·year diagonals, hover/pin tooltip, click-to-drill. |
| [`<AgeHistograms>`](#agehistograms) | Byte-weighted mtime distribution per child, with a draggable threshold that reads out reclaimable bytes. |
| `sumTbYears` / `formatTbYears` | Additive staleness score: Σ descendant-file size·age = `size × (now − mtime_mean)` in TB·years. Cascades like `size` (parent = Σ children), so it's honest as a treemap size accessor and exact as iso-score diagonals on a log-log (age, bytes) scatter. |

Everything from `@rdub/treemap` (`<Treemap>`, `useHoverPin`, `squarify`,
`DEFAULT_PALETTE`, `ageFade`, `parseQuery`, …) is also re-exported here. For the
treemap itself — including its `styles.css` and `/voronoi` subpath — see the
[`@rdub/treemap` README](../treemap/README.md).

## Install

Same SHA-pinnable **dist branch** mechanism as the core (via [`npm-dist`][npm-dist]):

```bash
pnpm add github:runsascoded/disk-tree#<dist-sha>
```

`react` and `react-dom` ≥ 18 are peer deps; `@rdub/treemap` comes along as a
dependency.

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

One marker per node: x = age, y = bytes, both log — so **iso-score diagonals are
exact straight lines**, because on (years, TB) axes the product `x·y` *is* the
sum-TB·years score (`sumTbYears`). Upper-right of a labeled diagonal is the
delete-candidate frontier.

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

Nodes lacking a positive age *and* size can't sit on log axes; they're counted in
a footer note rather than silently dropped. Sizes default to SI (1 TB = 1e12 B) so
the axis agrees with the score's units.

The layout math is exported separately if you want to render your own marks:
`logDomain`, `logPos`, `logTicks`, `isoScoreSegment`, `isoScoresForData`,
`decadesBetween`, `radiusFor`.

## `<AgeHistograms>`

One column per child, y = mtime, bars weighted by **bytes** against a shared scale
— so a column's area is its byte total and the area below the threshold line is
exactly what deleting everything older reclaims. A mean can't tell you a directory
is half ancient and half fresh; this can.

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

`normalize` exists because real directory trees span orders of magnitude: honest
shared scaling renders a 5 MB child next to a 2 GB one as a hairline. Expose it as
an explicitly-labeled toggle ("shape only"), not as the default.

Math is exported too: `bytesOlderThan` (whole bins plus a linear split of the
straddling one), `totalBytes`, `peakBin`, `timeTicks`.

## Theming

`.dt-timeseries` (grid, axis, tooltip, crosshair) themes the same CSS-var way as
the treemap; see the [`@rdub/treemap` theming section](../treemap/README.md#theming).

## Contributing

`packages/react/` is a workspace member of the [disk-tree] monorepo. Iterate from
the repo root:

```bash
pnpm install                                  # workspace-wide
cd packages/react
pnpm typecheck
pnpm test        # Vitest
```

The DT app under `ui/` consumes this package via `"@disk-tree/react":
"workspace:*"` — changes flow through instantly during `pnpm dev`.

## License

Apache 2.0 (same as disk-tree).

[disk-tree]: https://github.com/runsascoded/disk-tree
[npm-dist]: https://github.com/runsascoded/npm-dist
