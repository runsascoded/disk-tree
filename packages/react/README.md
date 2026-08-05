# `@disk-tree/react`

Reusable React widgets for [disk-tree] consumers. **Chart-lib-free** (no
Plotly, no recharts, no d3) — everything is DIY SVG or plain DOM. All
components are accessor-based, so any node shape works.

Ships:

| Widget | What |
|---|---|
| [`<Treemap>`](#treemap) | Squarified layout, drill-on-click, pin-on-leaf tooltip, keyboard nav, fold-small "…" tiles, fullscreen, slot-based coloring/tooltip/legend/rollup. |
| [`<TimeSeries>`](#timeseries) / [`<BytesOverTime>`](#timeseries) | Multi-series line/area chart with hover-follow crosshair. ~330 LOC total, zero deps. |
| [`useHoverPin`](#usehoverpin) | Headless hover+pin state (single pin, touch-safe, outside-click / Esc clear). |
| `squarify` / `foldSmall` | Pure layout primitives if you want to render the cells yourself. |
| `divergingColor` / `divergingInk` | Red/green diverging scale for Δ views. |

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
