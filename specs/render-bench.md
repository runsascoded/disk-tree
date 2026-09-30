# Render benchmarks: time-to-render per plot, measured, not felt

**Status:** draft, 2026-09-30. Base (`site/`). Ryan: *"we should have benchmarking tools/helpers so we can really measure time to render for each plot on the page"* — after a 10 s over-time load on cw-s3.oa.dev that was diagnosed by hand (memory `diff-perf`: a rejected `private` cache put, then batched lookups; 20 s → 5 s → 0.15–0.3 s), and with `path-store` about to change what every plot reads.

## 0. What exists

- **Server:** `serverTiming()` (`functions/_lib/edgeCache.ts:97`) is a `(phase, ms)` sink whose `header()` becomes `Server-Timing: fetch;dur=812,spans;dur=40,…` on `/api/subtree`, `/api/diff`, `/api/series`; `readRects` reports `spans`, `rgjson`, `ngroups`, `groups`, and the `IndexHandle` wraps `file.slice` to count `fetch`. Nothing reads the header client-side; nothing on `/api/scans`, `/api/age`, `/api/children`, `/v1/files`, `/data/*`.
- **Client:** the render spy (`src/dev/renderSpy.ts`, `?spy=1`) counts React commits and per-component render time; `e2e/renders.spec.ts` asserts *commit shapes* after clicks. It measures React work only — not the fetch, not the decode, not the paint — and nothing reports time-to-render per widget.
- **Widgets that paint from a fetch:** `Treemap.tsx` (`/api/subtree`), `DiffTreemap.tsx` (`/api/diff`), `SizeOverTime.tsx` (`/api/series`), `AgeChart.tsx` (age pyramid), the children table and the dTM table (share their map's fetch). Each has the same lifecycle: **request → response → model → first paint → settled** (deeper levels / tooltips / legends land in later commits).

## 1. The measurement

One vocabulary, from the User Timing API, so DevTools, Playwright and the page's own overlay all read the same marks:

```
<widget>:<key>:request      performance.mark at fetch start
<widget>:<key>:response     headers in (+ Server-Timing phases attached as detail)
<widget>:<key>:decoded      body parsed → the widget's model (rows, tree, points)
<widget>:<key>:painted      first commit that draws it (requestAnimationFrame after commit)
<widget>:<key>:settled      no commit touching the widget for 500 ms
```

`widget ∈ {treemap, dtm, series, age, table, dtable}`; `key` is the request key (path + date/pair + w/h + lens), so two treemap loads on one page are two series. `performance.measure` between consecutive marks gives the four spans (`wait`, `decode`, `layout+paint`, `settle`) and the total. Server phases ride along on the `response` mark's `detail`, parsed from `Server-Timing` (which the browser exposes on `PerformanceResourceTiming.serverTiming` for same-origin responses — no client parsing needed when the API is same-origin; the dev proxy case parses the header).

A tiny helper owns the marks: `src/perf.ts` — `perf.start(widget, key) → { response(res), decoded(), painted(), settled() }`; every widget's fetch hook calls it. No-op cost when nothing listens: marks are cheap and buffered by the browser; the helper is on always (not `?spy=1`-gated), because the interesting numbers are production's.

## 2. Surfaces

1. **In-page overlay, `?perf=1`:** a small fixed panel listing each widget's last load as a horizontal bar (wait | decode | paint | settle, coloured), the total in ms, the server phases beneath (from `Server-Timing`), cache hit/miss. Refreshes on each new load. This is what you look at on cw-s3.oa.dev when something feels slow — the 10 s over-time load would have read "wait 9,600 ms (server: overtime 9,400)" without a DevTools session.
2. **Console table:** `window.__perf.table()` prints the same as `console.table` rows; `window.__perf.entries()` returns them (what Playwright reads).
3. **Bench runner, `site/bench/`** (Playwright, like `e2e/`): `pnpm bench -- <base-url> <paths…> [--runs N] [--cold]` loads each URL N times (cold: a fresh context, `Cache-Control: no-cache` on the API; warm: same context), waits for every widget's `settled`, collects `__perf.entries()`, and prints one table per widget: p50/p95 per phase, plus the server phases — and writes `bench/out/<stamp>.json` so two runs diff (`pnpm bench:diff a.json b.json`: per widget/phase Δ and %). Targets: the local dev stack, `dev.*` and prod alike (auth: the bench takes a bearer token env for gated deploys, like `dt-cloud healthcheck`).
4. **CI smoke (later):** the r2.rbw.sh deploy's smoke step runs the bench once over three URLs and fails on a p95 regression above a per-widget budget (`bench/budgets.json`). Not in phase 1 — budgets need a baseline first.

## 3. Phases

1. **Marks + overlay + console** (`src/perf.ts`, wire the four fetching widgets and both tables, `?perf=1` panel, unit tests on the helper's measure math with a fake `performance`). CIC: the overlay on the demo, numbers plausible against DevTools' Network panel.
2. **Bench runner + diff** (`site/bench/`), README section, a first baseline JSON for r2.rbw.sh committed under `bench/baselines/`.
3. **Server phases everywhere:** `serverTiming()` on the remaining routes (`/api/scans`, `/api/age`, `/api/children`, `/data/*` proxy), the D1 query time as its own phase, cache hit as `cache;desc=hit`.
4. **Budgets in CI** once two weeks of baselines exist.

## 4. Non-goals

- Not a replacement for the render spy (commit *shapes*); the bench measures *time*. Both read from `?spy=1`/`?perf=1` without interfering.
- Not synthetic micro-benchmarks of `squarify` or hyparquet decode; those belong in the packages' own test suites.
- No third-party RUM; nothing leaves the page unless the bench runner reads it.
