# m3 on `site/`: the laptop as a store

**Status:** proposed (2026-09-28). Branch `m3`.

## Why

As a rule, a deployment ships one app, `site/`. gcs.oa.dev, cw-s3.oa.dev and r2.rbw.sh all run it. disk.rbw.sh is the exception: Pages project `disk-tree` is built from `ui/`, whose Pages functions (`ui/functions/`) re-implement the Flask server's `/api/scan` contract over raw scan blobs in R2. That is a second app, with a second auth (Cloudflare Access on `/auth/sso`, which `site/` and `@open-athena/auth` have already dropped). It also has a second copy of staged-delete and of the OG cards, and none of `site/`'s over-time, diff, owner or age features.

Goal: disk.rbw.sh runs `site/`, with this laptop as one more store, like `gcs`, `cw` and `r2`. `ui/functions/` is retired. The Flask app (`disk-tree-server` + `ui/`) is sunset as a deployment target (see "The Flask app" below).

## The two data models are layers, not rivals

- **Scan blob** (what `ui/` reads): hybrid chunked parquet plus a SQLite row, and a `.scan.json` manifest when the blob is remote. It is the *producer* format. It supports incremental rescans, chunk reuse across scans, and patching in fresher child scans. It is cheap to write from a laptop.
- **Snapshot library plus path index** (what `site/` reads):
  - per-store, per-date `snapshots/<store>/<date>/`;
  - a union path index sorted `(depth, path)`, with its footer in D1, so row groups of about 8k rows cost nothing to locate;
  - over-time groups and diff indexes.

  It is the *serving* format: edge-native, and the only one that answers series, diffs and ages.

For serving, the second is strictly better. The first stays as the laptop's write format, or is skipped entirely. The cloud stores never make scan blobs: they go listing shards → `dt-cloud path-index` → snapshot. The laptop can do the same with `disk-tree capture` (layer-1 listing shards, bounded memory, zero local disk, streamed to R2).

## Plan

### Phase 0: spike (does a 7.4M-row home scan fit `site/`'s model?)
1. Publish scan 136 (`/Users/ryan`, `c69fe255`, R2) into the snapshot model:
   - a listing from the blob's files, or a fresh `disk-tree capture /Users/ryan -t r2://disk-tree/captures`;
   - then `dt-cloud path-index -l … -P … -o …` into `tmp/`.
2. Measure:
   - wall time and peak RSS (the laptop is the constraint);
   - output sizes;
   - D1 footer row count.
3. Run `site/` locally over it: `site/./dev --local-db` with a `laptop` store stub. CIC `/`, `~/c`, `~/Library` at phone width.
4. Go/no-go. The main risk is path-index build memory on the laptop. The fallback is to reduce in the cloud (`.github/workflows/reduce.yml`, or node `mgu`).

### Phase 1: the `laptop` store
- A `REGISTRY` entry in `site/src/stores.ts`:
  - `scheme: 'file'` and `rootLabel` `~`;
  - `prices: false`, `owners: false`, `staging: true`;
  - the wall copy.
- Local paths as treemap roots. Verify nothing assumes `<scheme>://bucket/`.
- `site/wrangler.toml` for this branch: R2 `disk-tree`, D1 (a new one, or migrate `disk-tree-auth`), `AUTH_MODE=app`, allowlist `ryan@runsascoded.com`.
- Auth: the `site/` Google OAuth client for disk.rbw.sh (a new client, or a redirect added to an existing one: **user step**), `GOOGLE_CLIENT_ID`/`SECRET` secrets, and optionally Resend for emailed codes.

### Phase 2: publishing
- The 12 h index LaunchAgent becomes `capture` (or `index`) plus path-index/snapshot publish plus D1 footer sync, as the ingest does for `r2`.
- An R2 keep-policy for old generations: roughly one generation per 12 h. Decide the retention.

### Phase 3: deletes go to the laptop
- `site/`'s plan dispatch today hands a run to the store's `executor` (`'plan-sweep' | 'sweep'`, cloud jobs). Add a `'drainer'` executor: enqueue the run in D1, and the laptop's `disk-tree dispatch -s` (LaunchAgent `com.runsascoded.disk-tree.drain`) polls and executes it through `LocalBackend`.
- Reconcile the two schemas: `site/`'s plans/staged tables vs `ui/`'s `deletion_runs` (migrations 0001–0009 in `ui/migrations`). The drainer reads the latter.
- After a run, re-publish (or patch) the touched subtrees so the Map reflects the freed bytes.

### Phase 4: cut over and retire
- Deploy `site/` to Pages `disk-tree`.
- Delete the Access app `disk-tree` and the `ACCESS_*` secrets.
- Remove `ui/functions/`, `ui/cfn/` and `ui/wrangler.toml` from `m3`. Whether to remove them from `cloud` is a `[base]` question: nothing else deploys them.

## The Flask app

`disk-tree-server` + `ui/` live in the base Python package, shipped via PyPI (`pyproject.toml` bundles `ui/dist`). They are not a branch. They were still worked on in 2026-09 (`server.py` 9 commits, `ui/src` 24), almost all of it for this laptop loop. Once disk.rbw.sh runs `site/`, that loop no longer needs them, so sunsetting is the natural outcome rather than porting `ui/` into another `site/`. What only Flask offers today, all needing a process on the laptop:
- rescan;
- reveal in Finder;
- immediate delete;
- recursive filter;
- mtime histogram;
- file preview.

The CLI keeps all of these, except reveal and preview, which have no CLI command. Any we want in the browser should come back as drainer-style laptop verbs behind `site/`, not as a second app.

## Harmonizing

The `ui/` ↔ `site/` delta should be distilled like the gcs/cw unification: each `ui/`-only feature either ports into `site/` or is consciously dropped. Candidates to inventory during Phase 1:
- hybrid-chunk drill;
- compare view vs `site/` diff;
- recent paths;
- the staged page;
- OG cards (tier B treemaps);
- the phone path-column layout.
