# Dynamic Open Graph cards (per-path treemap unfurls)

Status: **feature landed** (2026-09-19); one **open operational blocker** (R2 write token, §5).

Make every shareable disk-tree link unfurl with a treemap of *that* path, so pasting `https://r2.rbw.sh/r2/ctbk` (or a drilled `…/r2/ctbk/avail-v3`) into Slack/Twitter/Discord shows the bucket's actual space breakdown, not one static card. Applies to the public serverless demo (`disk-tree-demo` → r2.rbw.sh); the gated `disk-tree` project inherits the same code.

## 1. Why it needs server work

Crawlers don't run JS. The SPA's og tags are static in `index.html`, so every route unfurled identically — and worse, the original `og:image` pointed at `/og.png`, which never existed, so the card was a broken thumbnail. Two fixes: (a) a real default card, and (b) rewrite the tags per request path on the server, since that's the only thing a crawler sees.

## 2. Architecture

**`ui/functions/_middleware.ts`** — for a scan-route *document* (`/r2/<bucket>[/…]`, `/file/…`; not `/`, `/access`, `/staged`, `/s3`, `/compare/*`), pipe the SPA HTML through `HTMLRewriter` and set `og:title` / `og:image` / `og:url` / `twitter:*` from the path. `og:image` → `${origin}/og/<key>` where `<key>` is the path key (`r2/ctbk/avail-v3`). Pure path↔uri↔title mapping in **`ui/cfn/og.ts`** (`ogRoute`, `keyToUri`), tested in `cfn/tests/og.test.ts`. The `/api/*` gate branch is unchanged.

**`ui/functions/og/[[path]].ts`** — serves the card image, resolving in order:

1. **R2 `og/<key>.jpg`** — refreshed daily from the live treemap by `rescan-demo.yml` (tier A, freshest, tracks re-scans).
2. **static `/_og/<key>.jpg`** — seed cards bundled with the deploy (tier A, day-one). NB a *missing* static asset resolves to the SPA shell (200 `text/html`), not a 404, so the seed branch trusts the response only when its `content-type` is an image.
3. **edge render** — `squarify` the covering scan's depth-1 children → SVG → PNG (tier B, any uri). Guarded: any failure falls through to (4).
4. **static `/og.jpg`** — the site-default card.

## 3. Tier A — pre-rendered bucket cards

Pixel-perfect (real renderer, fonts, dust): captured from the live treemap by headless Chromium.

- **Seeds** (`ui/public/_og/r2/{ctbk,nj-crashes,jc-taxes}.jpg`, 1200×630) bundle with the deploy so bucket links work immediately.
- **Daily refresh**: `rescan-demo.yml` runs `ui/scripts/og-capture.mjs` (Puppeteer, installed on the fly so a normal `pnpm install` doesn't pull Chromium) after re-scanning each bucket, and uploads to R2 `og/r2/<bucket>.jpg`. `continue-on-error` — a capture flake never fails the scan run. **Currently blocked, see §5.**

## 4. Tier B — edge-rendered subtree cards

Any drilled path renders on demand at the edge, reusing the live widget's layout so it matches:

- **`ui/cfn/ogSvg.ts`** — `treemapCardSvg({uri, children, total, itemCount})` → SVG, using the pure `squarify` and a local `slotColor` mirror (parity with the package pinned by `cfn/tests/ogSvg.test.ts`). Header = wordmark + uri + `<size> · <n> items`.
- **`ui/cfn/ogRender.ts`** — `svgToPng` via resvg-wasm.
- `ogRender.test.ts` renders a real 1200×630 PNG end-to-end (Node reads the wasm bytes) and writes `tmp/og-b-sample.png` for inspection.
- Cost: ~1.7–5.3 s cold render, then CF edge-cached (`max-age=3600`).

### Edge gotchas (see memory `cfn-edge-wasm-and-dom-free`)

- **cfn is DOM-free** (`tsconfig.cfn.json` = ES2022 + workers-types). Importing the `@rdub/treemap` barrel drags the React/DOM graph into that compilation (~46 spurious errors). Fix: import only the pure `@rdub/treemap/squarify` (new subpath export); mirror `slotColor` locally (its module type-imports `CellStyle` from the DOM-heavy `Treemap.tsx`). Tests are excluded from `tsconfig.cfn`, so they *can* import the barrel for the parity assertion.
- **Workers can't compile wasm from bytes at runtime** (`CompileError: Wasm code generation disallowed by embedder`). `initWasm(fetch(url))` fails; the wasm must be imported as a build-time-compiled module. Vendored at `ui/cfn/vendor/resvg.wasm` + `cfn/wasm.d.ts` + `import wasm from '…/resvg.wasm'`. Node (tests/CI) *can* compile from bytes.
- **Fonts**: resvg wants ttf/otf (not woff2), served as static assets (`ui/public/_fonts/Inter-{400,600}.ttf`, fetched at runtime — that's allowed, unlike wasm). Static Inter TTFs come from the Google Fonts css2 API with an old User-Agent.

## 5. OPEN — R2 write token is read-only (blocks tier-A refresh + the whole rescan)

`rescan-demo.yml` has failed **every run since ~2026-09-09**: `PermissionError: Access Denied` on `PutObject` to `disk-tree-demo`. The R2 credential (CI secrets `R2_ACCESS_KEY_ID`/`R2_SECRET_ACCESS_KEY`, and the local `.envrc` keys) is **read-only**; `wrangler r2 object put` also 403s (the `CLOUDFLARE_API_TOKEN` has Pages-deploy scope, not R2-object write). Consequences: demo scans are stale ("10d ago"), and the tier-A OG daily refresh can't upload.

**Fix (account-side, RAC `0dcad…`):** rotate the R2 API token to include **Object Read & Write** on `disk-tree-demo`; update the repo secrets + `.envrc`. Code is correct — nothing to change. The **bundled seed cards** and **tier-B edge renders** need no R2 write and work regardless. Memory: `rescan-demo-r2-write-denied`.

## 6. Also landed alongside (README / site OG)

- README: prominent clickable live-demo treemap hero + refreshed `screenshots/treemap.png` (was the retired plotly renderer) (`0ea42b0`).
- Site default OG: real `ui/public/og.jpg` (1200×628 ctbk treemap crop) + absolute `og:image`/`og:url`/`twitter:card` in `index.html` — fixes the broken r2.rbw.sh unfurl (`b39ebfa`).

## 7. Commits

`0ea42b0` README hero · `b39ebfa` site og.jpg + meta · `d42f389` tier A (middleware + seeds) · `703b3f7` tier-A daily refresh CI · `153ab98` tier B (edge render).

## 8. Possible follow-ups

- Tier B **write-through to R2** via the Function's R2 binding (bindings can write even with a read-only *API* token), so a cold sub-path render is cached durably rather than only at the CF edge — reduces repeat cold renders and survives isolate churn.
- Whether the demo's `nj-crashes` (5.3 G on RAC) should mirror the larger HCCS `r2://crashes` corpus. Scanning that bucket needs HCCS read+**list** creds (R2 public-object-read ≠ publicly-listable over the S3 API). Not required for anything above.
