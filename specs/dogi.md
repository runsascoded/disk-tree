# Dynamic OG images for every page

Status: proposed 2026-10-02 (gcs session); revised the same day to two tiers (anonymous shape-only cards, full-info by minted per-view token). Code is `[cloud]`; each deployment opts in.

## Why

Links to the site are pasted into Slack all day (the staged-deletion thread, #marin-alerts, DMs). Today every page unfurls with one static `og.jpg`, plus a title and counts (`functions/_lib/unfurl.ts`). Ryan wants each link's card to show *that view*: the map at that path, scan, filter and lens; the staged plan; a user's estate. The staged-plan Slack thread should carry the same image.

## Decision: two tiers of card

The first draft gave any unsigned fetch the full view. Ryan caught the hole (2026-10-02): a page fetch hands out a signed image for *that* page, child names included, so a scraper could walk the whole fleet tree, sizes and all, one card at a time. Signing image URLs stops forgery, not enumeration.

- **Anonymous card (the default for every page fetch):** the view's treemap as shapes with **no labels**, its total size, the scan date and the page title. Owner colours without a named legend. Nothing on it names a child, so it can't be walked. (A guessed path still reveals whether it exists and its size; accepted. Optionally, unknown or empty paths get the generic card.)
- **Full-info card, only for an explicitly shared link:** labels, sizes and owner names, as a signed-in viewer sees the view.
  - A signed-in viewer mints it from a "share with preview" button (and a use-kbd action). The server returns the page URL plus `og=<token>`. The token is an HMAC (deployment key, derived from an existing secret) over the **canonical view**: path, scan, filter, lens, colour and expiry. A token is valid for that exact view only: not a child, parent or sibling, and not other params.
  - Each mint is recorded in D1 (who, view, when, expiry) for audit and per-token revocation, like minted share links. `/admin` lists them.
  - Server-side posts (the staged-plan Slack thread) mint their own full-info link.
- **Image URLs stay signed and expiring in both tiers** (`/og/<kind>.png?<view>&tier=anon|full&exp&sig`). The signature covers the tier, so an anonymous image URL can't be upgraded.
- **The page shell stays as today:** an anonymous fetch gets the SPA shell with meta stamped, never data beyond the card.

## Pages and their cards

1200×630 PNG. Header: site name, page title, scan date. Footer: key totals.

| Page | Card |
|---|---|
| `/` and `/<bucket>/<path…>` (+ `date`, `f` filter, `o` owner lens, `c` colour, `from` diff) | the treemap of that view, coloured as the page colours it (owner by default); totals; the filter text if any. Anonymous tier: unlabelled shapes + total |
| `/staged` (+ `q`, `s`) | treemap of the staged set (filtered), coloured by owner; "N prefixes · X TiB · M objects"; top owners |
| `/users` | per-user bars (top ~12 by bytes) |
| `/user/<id>` | that user's estate treemap + total |
| `/assignments` | the assigner × assignee matrix as a heatmap |
| anything else | today's static card |

## Rendering

- SVG built server-side from the same data the page's API returns. Reuse `@rdub/treemap/squarify` (DOM-free) for layout and the site's palette and owner colours, so a card matches the page.
- SVG → PNG with `@resvg/resvg-wasm`. Pages Functions can't compile wasm at runtime: vendor the `.wasm` and import it as a module (see the `cfn-edge-wasm-and-dom-free` lesson). Bundle a small Latin font subset (OFL), as `@rdub/file-tree/og` does. Look at `@rdub/file-tree/og` (`renderOgCard`, `ogCardData`, `injectOgTags`) and its spec `specs/done/cfw-og-images.md` first; reuse what fits rather than re-deriving.
- Cache rendered PNGs in the Cache API keyed by the signed URL. Data per scan is immutable; the ledger head goes in the key for owner-coloured views.
- Budget: a card must render within the Worker CPU/memory limits at gcs scale. Use the same thresholded, depth-capped reads the page uses (a ~1200×520 canvas), never a full read.

## Meta stamping

Generalise `unfurl.ts`: one middleware (or a catch-all Function ahead of the SPA) that, for a navigation, computes `{title, desc, image}` per route from the URL and stamps the shell. `image` is the signed card URL for that exact view. Titles say what the view is ("marin-us-central2/checkpoints · 2026-10-02 · filter: tomat").

## Slack

The staged-plan parent message (`stagedSlack.ts`) gets an `image` block with the `/staged` card (signed, 7-day). A re-render refreshes it.

## Tests

- Tiers: the anonymous card's SVG contains no path names (assert the exact text nodes); a view token verifies only for its exact canonical view (child, parent, sibling, changed param, expired, revoked → anonymous card).
- Signing: exact canonicalisation (param order, defaults dropped), verify accepts the signed URL, rejects tampered params, a wrong sig and an expired `exp`.
- Route → meta: exact `{title, desc, image path}` per page shape, from URL fixtures.
- Card SVG: structural snapshot on a fixture tree (rect count, labels present), plus a PNG render smoke test.

## Implementation

**Phase 1a (map cards, anonymous tier): built.**
- `functions/_lib/og/sign.ts`: one key, HMAC(`SESSION_SECRET`, `og-card:v1`). Tags are HMAC-SHA256 truncated to 64 bits, base64url (11 chars); expiries are whole days since 2026-01-01, packed into 2 base64url chars. An image URL is `/og/<kind>.png?<view>&sig=<t><ee><tag>` (14 chars; `t` = `a`/`f` tier). A view token is `og=<ee><tag>` (13 chars). Test vectors are checked against Python's `hmac`.
- `routes.ts` maps a page URL to its kind, its canonical view params and title. `card.ts` draws the SVG (anon: no tile labels, no legend). `data.ts` reads the page's own view (`buildView` at the card's 1120×410 box, `maxDepth: 2`), applies the live ledger as the map does, and colours by the scan's owner rank. `render.ts` is resvg-wasm (vendored `vendor/resvg.wasm`, imported as a module) with Inter 400/600 from `public/_fonts/`.
- `functions/_middleware.ts` stamps every HTML response's meta (`stampPage`) when `OG_CARDS` is set. `functions/og/[[path]].ts` verifies and renders, cached in the colo cache under the signed URL plus the ledger head. `public/_routes.json` keeps `/assets/*` and fonts off the Functions.
- `src/ownerIndex.ts`: the ledger's resolver, moved out of `owners.ts` (React-free) so the cards fold it exactly as the map does.

**Phase 1b (full tier by view token): built; needs gcs migration `0034_og_tokens` applied to prod D1 (Ryan's go).**
- `migrations/gcs/0034_og_tokens.sql`: one row per mint (token, kind, canonical view, page, minter, minted, expiry day, revoked by/at). A new table with no references. Tested on the full gcs lineage with foreign keys on (`tokens.test.ts` via `testD1`; `storeMigration.test.ts` lists it).
- `POST /api/og/mint {url, days?}` (full viewers, not read-only guest links: a token outlives the session; default 30 days, max 90) → the page URL with `og=<token>`. `GET /api/og/tokens` and `DELETE /api/og/tokens/<token>` are admin-only.
- The middleware upgrades to `full` only when `og=` was minted for exactly this view, is unexpired, and has a live D1 row. The full image URL never outlives the token. Without the table (cw), no token is honoured.
- UI: "Copy link with preview" in the site menu and the omnibar (`share:preview`); `/admin` lists mints with revoke ("Preview links").

**Next:** 2 the other pages' cards; 3 the Slack image block.

## Rollout

`site/deploy --dev` first; check unfurls with Slack's link fetcher UA and a real paste in a test DM. Prod after Ryan's go.
