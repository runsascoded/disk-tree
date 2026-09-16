# Treemap: export the current view as an image (copy / download)

**From:** marin-gcs-usage (cw-s3), 2026-09-16. During a quota incident a teammate clipped the treemap by screenshot and pasted it into Slack; a one-click "copy PNG" / "download PNG" would have been the natural affordance, and every deployment of this treemap (gcs, cw-s3, the planned R2/S3/GCS demos) wants the same thing.

## Why it's cheap

The cells and labels are canvas-rendered (`packages/treemap/src/TreemapCanvas.tsx`), so the pixels already exist: `canvas.toBlob('image/png')` → `navigator.clipboard.write([new ClipboardItem({'image/png': blob})])` for copy, or an object URL on an `<a download>` for download. Retina: the canvas is already drawn at `devicePixelRatio`, so the export is crisp without re-rendering.

## Proposal (`@rdub/treemap`)

- `Treemap` prop `exportable?: boolean | { filename?: (ctx) => string; legend?: boolean; title?: boolean }`. When set, the header/crumb row gets two small icon buttons (copy, download) next to the fullscreen toggle; keyboard: `⌘/Ctrl+Shift+C` while the treemap has focus.
- Composition: default = the treemap canvas only (what the teammate clipped). Options add the **legend** (colour key) and a **title line** (root path + total, i.e. the crumb text) composited above it on an offscreen canvas, in the page's current theme colours (light/dark), with a small margin. No DOM-to-image dependency — everything we'd want is drawable.
- `filename` default: `<root-basename>-<YYYYMMDD-HHMM>.png` (e.g. `marin-us-east-02a-20260916-1201.png`); consumers can derive from their scan id.
- `onExport?: (blob, {kind: 'copy'|'download'})` for analytics/toasts; the consumer shows the "copied ✓" affordance (the site already has one for names).
- Clipboard write needs a user gesture and a secure context — both hold for a button click on an HTTPS page; fall back to download when `ClipboardItem` is unavailable (Firefox behind a flag).

## Consumer side

cw-s3/gcs: pass `exportable` with `legend: true, title: true`; nothing else. Optional later: a "share" that uploads the PNG to the deployment's public icons Pages project and posts the URL to Slack — out of scope here.
