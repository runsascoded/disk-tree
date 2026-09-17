# Staged delete: the `chat-approve` cloud path, upstreamed generic

**From:** the mgu gcs "delete not mark" pivot (`specs/gcs-toward-union.md` §5 seam 1 + `specs/sweep-plan-union.md`, mgu-side), routed here as roadmap item 3 (`specs/mgu-cp-2026-09-16.md`). Cross-repo peer, so **generalize-and-upstream** (`/cp` `handoff: spec`): disk-tree holds the reusable engine, each deployment keeps its policy; gcs eventually imports it.

**Goal:** a public demo against Ryan's **S3 + R2**, in the repo — you log in and do admin dispatches. It doubles as the forcing function to make the ported pieces appropriately generic (S3/R2/GCS, not gcs-only).

## The model (the pivot)

Opt-out mark/sweep + deadline → opt-in **delete**. Three nouns collapse to one user-facing one:

- **stage** (the gesture) — a per-row trash-can (dt already has it in local mode; shift-range; treemap meta-drag later). Stages exactly what it names. *Nothing is deleted by inaction — there is no deadline.*
- **[approve]** — the per-deploy authority gate; absent in local mode.
- **run** — the executor deletes, recoverably.

Lifecycle (cloud): `staged → pending-approval → running → done, recoverable until X` — no fake-instant checkmark; the run is async. Local collapses to `trash → confirm → delete inline`.

Per-deploy policy is **config, not forked code** (`Store.*` in mgu; a `delete:` block here):

| policy | values | meaning |
|---|---|---|
| `approval` | `local-confirm` \| `chat-approve` | single-user confirm dialog vs stage→chat→admin→dispatch |
| `chat` | `slack` \| `discord` \| none | where a staged path is announced (reuses `disk_tree.notify`) |
| `undo` | `soft-delete` \| `versioning` \| `none` | what makes one-click delete safe; set by the store adapter's *offer* |
| `eligibility` | `owner-slice` \| `any` | an *optional* auto-approve shortcut, **not** a safety gate; default: approve every real delete |

## What gcs built — generic vs specific

Generic (upstream): the **plan object** (`plans`/`plan_items`, the shared cw+gcs schema — a staged set auto-populated by trash gestures), the run records (`deletion_runs`/`deletion_bands`), the `/api/plans` CRUD + `/stage` shape, the generic `/api/db` registry-CRUD, the `auth.ts` scope/gate, `edgeCache.ts`, and the **stage → approve → dispatch → runs** UX.

gcs-specific (abstract behind an interface): GCP Batch dispatch (`gcp.ts`, `api/sweep/dispatch.ts`), the `sweep_exec.py` executor (re-list + generation-match + ≥7d soft-delete guard + drift), the storage-class axis, the attribution/ownership gate (`sweep_approvals`, `attr_index`, `classify_dir`), KLC checkpoint-keeping, and every `gs://marin-<bucket>/` prefix + hardcoded bucket.

## What disk-tree already has (so we don't rebuild it)

- **Auth is not greenfield.** dt ships the *same* `@open-athena/auth` stack gcs uses, live on `disk-tree.pages.dev`: D1 `DB` binding, `view`/`admin` scopes, `ALLOWED_EMAILS`→admin, `hasScope(auth,'admin')`, an `AccessPage.tsx` admin dashboard, migrations 0001–0007 (`grants`/`access_log`/`access_requests`). So "mirror what gcs uses" for admin auth **falls out for free** — reuse `gateFor`/`gateApi` + the `admin` scope; `view` to stage, `admin` to dispatch.
- **The executor already exists locally.** `S3Backend.delete()` = `aws s3 rm --recursive` (R2 via endpoint+profile), `local`/`ssh` deletes too, all wired through `backend_for()` with `buckets.yml` creds. `POST /api/delete` (Flask) does an immediate delete today.
- **The edge is read-only.** Every non-GET `/api/*` 501s (`ui/functions/api/[[path]].ts`); the edge R2 binding (`SCANS`) reaches only the dt scans bucket, not arbitrary user buckets. So the *edge cannot delete* — the executor must stay server-side (laptop/worker with `buckets.yml` creds), and the edge only records intent + approval.

That shapes the split: **edge stages + approves (D1); a server-side dispatcher drains approvals and runs `backend.delete()`.** Local-first proves the whole mechanism before any of the edge/D1/UI exists.

## disk-tree design

**Data model** — mirror gcs's D1 tables in dt's SQLite (so the later D1 layer is a direct mirror), on `Base` (auto-created by `create_all`):

- `Plan` — `id, name, note, state('open'|'closed'), created_by, created_ts, closed_ts`.
- `PlanItem` — `(plan_id, uri)` PK, `note, added_by, added_ts`. The staged set.
- `DeletionRun` — `run_id, plan_id, mode('dry'|'real'), actor, started_ts, finished_ts, deleted_bytes, deleted_objects, skipped_gone, undo_state('none'|'partial'|'full'|'expired'), undo_deadline`.
- `DeletionBand` — `(run_id, uri)` PK, `bytes, objects, deleted, gone` — the per-item result of a run.

**Executor** — generic, over the existing backend seam: for each plan item, size it from the freshest covering scan, then `backend.delete(uri)` (dry: skip, report; real: delete + record a band). Undo is the store's own affordance (S3/R2 versioning → delete-markers; GCS soft-delete window), recorded on the run, not re-implemented — dt relies on the object store for recoverability rather than gcs's PB-scale generation-match walk.

**Policy** — a `delete:` block per bucket (or `defaults`) in `buckets.yml`: `approval`, `chat`, `undo`, `eligibility`. Local dt (no block) = `local-confirm`. On stage under `chat-approve`, announce via `disk_tree.notify`.

## Checkpoints

1. **Local data model + CLI (this stage). Done.** `Plan`/`PlanItem`/`DeletionRun`/`DeletionBand` SQLite models (`sqla/deletion.py`, mirroring gcs's D1 shape; auto-created) + the engine `disk_tree/staged.py` (`stage`/`unstage`/`open_plan`/`plan_by_ref`/`dispatch`, with `delete_fn`/`size_fn` injected so it's backend- and DB-init-free) + the CLI `disk-tree stage URI…` / `staged` / `unstage URI…` / `dispatch [PLAN] [-f]`. The executor runs over the existing `backend_for(uri).delete()`, dry by default; a run + per-URI bands recorded; sizing from the freshest covering scan. Tested: engine units (in-proc session + fake backend) + a real CLI end-to-end (index → stage → dry → real delete of local temp files). *The dispatcher is the admin — no approval gate yet.* (`run_id` carries a random suffix — a plan takes many dry runs + one real, so a second-resolution timestamp alone collides.)
2. **Edge write layer.** Mirror the four tables into D1 (`ui/migrations/`), add authenticated POST Functions (`/api/plans`, `/api/plans/stage`, `/api/dispatch`) gated on the existing `@open-athena/auth` scopes (`view` to stage, `admin` to dispatch), and a `stageDelete` capability. Read routes for the staged set + runs feed.
3. **`/staged` React page + trash-can rewire.** Under `chat-approve`, `ScanDetails`'s trash-can *stages* (the new route) instead of the immediate `POST /api/delete`; a `/staged` page (reusing `AccessPage` patterns) lets admins multi-select → dispatch; non-admins see it + the runs feed read-only. New `stageDelete` capability drives the affordance swap.
4. **Server-side dispatcher + chat.** A drainer (`disk-tree dispatch --serve`, or a Flask route) that reads approved D1 requests and runs the CP1 executor with `buckets.yml` creds (the edge can't). `chat-approve` announces staged paths + run results via `disk_tree.notify`.
5. **Pluggable undo.** `undo`/`restore` per store adapter — S3/R2 version-restore, GCS soft-delete restore — behind `delete.undo`; an `undo` CLI/endpoint over `DeletionRun`.

## Open questions

- **Edge → arbitrary-bucket delete.** The edge has no creds for user buckets, so the executor stays server-side (CP4 drainer). Is the demo's dispatcher a laptop poller, a small always-on worker, or a Flask route the admin hits? (Leaning: a `disk-tree dispatch --serve` poller for the demo, documented.)
- **Eligibility.** dt has no attribution/owner ledger (that's gcs's `actions` WAL). `eligibility: any` (approve everything) is the dt default; `owner-slice` is deferred until/unless dt grows an owner axis.
- **Undo parity.** S3/R2 give versioning (delete-markers) only — no soft-delete window. The run's `undo_deadline` is meaningful only where the store has a retention window; for plain versioning it's "restore the marker" with no deadline.
