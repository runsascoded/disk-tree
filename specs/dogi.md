# Dynamic OG images for every page

Status: proposed 2026-10-02 (gcs session). Code is `[cloud]`; each deployment opts in.

## Why

Links to the site are pasted into Slack all day (the staged-deletion thread, #marin-alerts, DMs). Today every page unfurls with one static `og.jpg`, plus a title and counts (`functions/_lib/unfurl.ts`). Ryan wants each link's card to show *that view*: the map at that path, scan, filter and lens; the staged plan; a user's estate. The staged-plan Slack thread should carry the same image.

## Decision: what an unsigned fetch may see

Unfurlers (Slackbot, iMessage, Discord) fetch without signing in. Ryan chose (2026-10-02) that cards show the full view: treemap, byte totals, owner colours and names, as a signed-in viewer sees that page. Anyone holding a page link can therefore see that page's card. Two limits:

- **Signed, expiring image URLs.** `og:image` is `/og/<kind>.png?<view params>&exp=<unix>&sig=<hmac>`. The signature covers the canonical view params and `exp`, keyed by a secret the deployment already holds (derive a sub-key, e.g. HKDF or HMAC(`og-image`, <existing secret>), so no new secret is needed). Default lifetime 7 days. Unsigned, tampered or expired → 403. So images can't be guessed or enumerated, only obtained by fetching a page.
- **The page shell stays as today:** an anonymous fetch gets the SPA shell with meta stamped, never data beyond the card.

## Pages and their cards

1200×630 PNG. Header: site name, page title, scan date. Footer: key totals.

| Page | Card |
|---|---|
| `/` and `/<bucket>/<path…>` (+ `date`, `f` filter, `o` owner lens, `c` colour, `from` diff) | the treemap of that view, coloured as the page colours it (owner by default); totals; the filter text if any |
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

- Signing: exact canonicalisation (param order, defaults dropped), verify accepts the signed URL, rejects tampered params, a wrong sig and an expired `exp`.
- Route → meta: exact `{title, desc, image path}` per page shape, from URL fixtures.
- Card SVG: structural snapshot on a fixture tree (rect count, labels present), plus a PNG render smoke test.

## Rollout

`site/deploy --dev` first; check unfurls with Slack's link fetcher UA and a real paste in a test DM. Prod after Ryan's go.
