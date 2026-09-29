# Sheet mirror — opt-in "tabular data → Google Sheet tab, on a schedule"

**Status (2026-09-28): proposed** (written from the gcs session; base work, for the root session). Ryan: "it can just be an optional thing each app opts into or not, specifying tabular data that updates on a schedule, and providing a nice way to update a gsheet where the Version History looks nice." Defaults and stubs were left to the author's judgment; the choices below are that judgment.

## Why now

gcs's `sheet-sync/` (hourly `/users` "who still needs to mark" → Percy's sheet) was hard-wired to marks. It had been failing silently since **2026-09-07** (its `dt-cloud report` read a per-scan `tree.json` the newer scans no longer publish), and the marks demolition removes its data source outright. Its Scheduler trigger `gcs-sheet-sync-hourly` is **paused** (2026-09-28); the Cloud Run job, image, grant token (`gcs-sheet-sync-token` in Secret Manager) and the sheet itself are left in place. gcs drops `sheet-sync/` from its delta. Nothing is lost that this spec doesn't replace.

## Shape

An app opts in by declaring **mirrors**; with none declared, nothing is built, deployed, or scheduled. No deployment enables one by default.

```yaml
# <deployment>/sheet-mirror.yml  (gcs: next to site/wrangler.toml; absent = off)
site: https://gcs.oa.dev            # the deployment's own API
token_secret: gcs-sheet-sync-token  # read-only grant, Secret Manager
schedule: "5 * * * *"
mirrors:
  - sheet: 1k_11LA…RFc               # the Google Sheet key
    tab: Storage by user
    source: owners                   # a named exporter (below)
    key: user                        # stable row identity (sort + diff)
    footer: "⟳ Auto-synced hourly from {site}/users — edits are overwritten; add derived views in another tab"
```

### Sources: named exporters over the live API, not new queries

`dt-cloud export <source> -u <site> [-d <scan>] -o out.csv` — each source is a thin reader of an endpoint the site already serves, with a fixed column contract:

| source | endpoint | columns |
|---|---|---|
| `owners` | `GET /api/owners?date=<latest>` | `user, bytes, objects, <storage-class mix…>` |
| `staged` | `GET /api/plans/staged` | `prefix, staged_by, staged_at, note, bytes` |
| `runs` | `GET /api/sweep/jobs` (gcs) / plan-sweep runs (cw) | `run, mode, by, started, state, deleted_bytes` |

Keep it to these three to start; adding a source is one function plus its column list. A `json:<path>[,<jq-ish field list>]` escape hatch is tempting but is exactly the generic query layer to avoid until a second app wants something the named ones don't cover.

### Writer: `sheet-push`, made key-aware

`sheet-push` already has the Version-History properties: cell-level diff against the tab, only changed cells written, numeric normalisation (`0.0` vs `0`), a footer "last change" stamp that advances only when data changes, other tabs untouched. Two changes:

1. **Stable order by `key`** (`-k/--key <column>`), with the existing rows' order preserved and new keys appended at the end. Today the diff is positional, so one user added mid-table shows every later row as changed; key-aware placement makes an addition one new row and a removal one cleared row (compacted only when the tab is otherwise unchanged).
2. **Docstring/names drop "mark-status"**; the command is the generic CSV → named-tab writer.

Everything else stays: named tab only (`-w`), ADC as the job SA (Editor on the sheet), `-n` dry run.

### Runner: one template job, per-deployment config

Move `sheet-sync/{Dockerfile,build.sh,deploy.sh,sync.sh}` to the base as `deploy/sheet-mirror/`, parameterised entirely by the YAML above: `sync.sh` loops over `mirrors`, runs `export` then `sheet-push -k` per entry. `deploy.sh <config.yml>` upserts the Cloud Run job + Scheduler + the two IAM bindings (secret access, run.invoker), idempotently, as today. **Drop** the healthcheck piggyback: `health.yml` on the default branch now checks both hosts hourly.

## Stubs to ship

- `deploy/sheet-mirror/example.yml` — one commented `owners` mirror; no real sheet id.
- `dt-cloud export --list` — prints the sources and their columns.
- Tests: exporter column contracts against recorded endpoint JSON; `sheet-push` key-aware placement against a fake worksheet (append, remove, reorder-stable, no-op writes nothing).

## Adoption

- **gcs:** once this is on `cloud`, a `sheet-mirror.yml` with one `owners` mirror into Percy's sheet (a new tab, "Storage by user", leaving the old "mark status" tab for him to delete), re-run `deploy.sh`, then unpause/replace the trigger. Reuses the existing grant token.
- **cw-s3:** opt in or not; `staged` + `runs` are the natural pair.
