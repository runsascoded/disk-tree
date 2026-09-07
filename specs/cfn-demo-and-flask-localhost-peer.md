# CFN public demo (fleet over R2) + Flask as the localhost peer

Supersedes `two-reference-deploys.md` (2026-08-28). Revised through discussion
2026-09-04/05. The original "two archs, compare the branch diff" framing is
dropped — Ryan has no story for deploying Flask publicly, and the prod apps he
actually develops are Vite + Cloudflare-Functions (CFW) over cloud stores. So:

## Settled shape

**One public story: the CFW arch.** A Vite SPA + Pages Functions over an object
store, published as a `*.pages.dev` demo. This is the arch marin runs and the
one Ryan builds on. Read *and* write live here — the "static FE can't mutate"
line was wrong: a Function holds the *store's* credentials, so it can delete
objects directly or dispatch a Batch job to (the flow marin's gcs side is
building). Each backend mutates what its environment can reach.

**Flask stays as the localhost peer — not sunset.** It's Ryan's most advanced UI
for cleaning his *laptop* disk, and it's the only thing that can: a cloud
Function can't reach local files; the Python server can. So the split isn't
arch-vs-arch, it's reach-vs-reach:

| | serves | can mutate | corpus |
|---|---|---|---|
| **Flask** (`server.py` + `ui/`, today's root) | localhost | local FS (delete/rescan) | local scans + any cloud bucket |
| **CFW demo** (`apps/cfn/`) | `*.pages.dev` | cloud objects (binding / Batch dispatch) | public R2 fleet |

Flask is a *peer*, at best — the public-facing artifact is the CFW demo.

## Corpus — a union of real public R2 buckets

Confirmed reachable (2026-09-05) via aws profile **`cf`**, CF account
`0dcad5654e9744de6616f74b8df4af63`, endpoint
`https://0dcad5654e9744de6616f74b8df4af63.r2.cloudflarestorage.com`:

- `r2://nj-crashes` — `cells/`, `raw/` (HCCS crashes)
- `r2://ctbk` — DVC-tracked Citi Bike data, large (`avail-*`, `.dvc/`, …) (HCCS ctbk)
- `r2://jc-taxes` — `.dvc/`, `data/` (Jersey City taxes)

All three are in the **same CF account**, so one Pages project binds them all and
the `/data` drill works cross-bucket. This 3-bucket fleet is the starting corpus;
more public buckets in the account can join later (`anything public in the
account is fair game`).

**Multi-bucket union is first-class**, not a new feature: marin's fleet view
*is* this — the publish job lists each bucket and feeds all the listings into one
tree build, yielding a synthetic fleet root with the buckets as top-level
children + one `path-index.parquet`. disk-tree's core now has the builder
(`tree_build.build_tree`, CP'd in `5be454c`); the cfn job wires
`[r2://nj-crashes, r2://ctbk, r2://jc-taxes]` into it. Cross-account would break the request-
time `/data` drill (a Function binds only its own account's buckets), but the
offline union itself is credential-agnostic — irrelevant here since both are
same-account.

## Layout

```
packages/treemap, packages/react   shared widgets                    (unchanged)
src/disk_tree                       shared core + CLI + offline jobs  (unchanged)
ui/ + server.py                     Flask localhost peer              (stays at root)
apps/cfn/                           Vite SPA + Pages Functions + publish job  (new)
```

Flask stays at root (it's the shipped `disk-tree` PyPI product — no packaging
churn). Only `apps/cfn/` is new. If a cleaner symmetry is wanted later, moving
Flask to `apps/flask/` is a separable follow-up, not a blocker.

## Seeding `apps/cfn/` from marin (current shapes, re-verify before scaffolding)

marin `cw-s3` is the lightest single-bucket deployment; `gcs` is the fleet one.
Seed the SPA + functions from marin, dropping the marin-specific layers:

```
site/functions/data/[[path]].ts     /data/* store proxy               KEEP
site/functions/login.ts             auth                              STRIP
job/run.sh (gcs) / cw-run.sh (cw-s3) fleet publish job                KEEP (adapt to R2 + build_tree)
job/*-diff.py                        offline recursive_diff → diff.json KEEP (adapt)
job assets: *-mark.{png,svg}         attribution mark                  STRIP
webdata / batch-submit / identities  marin batch + attribution layers  STRIP
```

- Re-survey marin's *current* `site/` + `job/` before copying — the 2026-08-28
  `api/subtree`/`api/path-index` function names are stale; mirror what marin
  actually serves so CP stays cheap.
- Diff in the CFW arch: prefer the job-precomputed `diff.json` (lighter, more
  legible) over porting the Flask diff index.

## Sequence

**Step 1 status (DT, 2026-09-07).** Corpus sized via the `cf` profile: `nj-crashes` 1,354 objects / 5.32 GB; `jc-taxes` 70,343 objects / 3.65 GB; `ctbk` is large — the first 20,000 objects alone are 239 GB and the listing is truncated there (full count unknown; awaiting go per below). Both small buckets are listed and imported as dated scans (`disk-tree pull -c <buckets.yml>` over the bulk lister: 1,409 and 70,620 layer-2 rows, ~30 s each) and browse in the Flask UI at `/r2/<bucket>`. Found on the way: `disk-tree index r2://…` silently fell through to the *local* backend and recorded an empty scan — now `r2://` lists live through `S3Backend` with the bucket's endpoint (`r2://` uris), and `gcs://` refuses loudly instead of faking success. The union itself (one fleet root) is not built yet: `tree_build.build_tree` produces marin's layer-3 `{n, b, o, …}` JSON, which marin's SPA no longer reads (it moved to `/api/subtree` over the path index + D1), so the "serve statically, confirm the treemap" leg needs a re-survey of what to seed `apps/cfn/` from before it's meaningful.

1. **Prove the union locally (CIC).** `disk-tree` list both R2 buckets (bounded
   first — check ctbk's scale before a full recursive scan), `build_tree` them
   into one fleet snapshot to a local dir, serve statically, confirm the treemap
   renders nj-crashes + ctbk as one fleet. No branch, no deploy. Await go on the
   full ctbk scan (it may be large).
2. Scaffold `apps/cfn/` (SPA + `data` proxy + publish job) building locally over
   that fleet snapshot; wire the two-bucket union into the job.
3. Deploy `*.pages.dev` in account `0dcad…`, binding both R2 buckets; confirm
   each external step.
4. (Later, optional) CFW delete flow (binding delete / Batch dispatch), mirroring
   marin's gcs side; `apps/flask/` symmetry move.

## Not changed

Flask keeps all its localhost powers (in-UI delete/rescan/ad-hoc filter over big
indexes). The CFW demo is read-first; its mutate story (step 4) follows marin.
