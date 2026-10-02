# infra — a deployment's cloud resources as code

Pulumi components, one directory per provider, shared by every deployment. `cloud` carries the components every deployment uses and `local` adds the laptop side's (`aws/`, `cf/capture_trigger.py`); both carry only components and their environment (`pyproject.toml`, `uv.lock`); each deployment branch adds its own stack programs next to them (`__main__.py`, `Pulumi.yaml`, `Pulumi.<stack>.yaml`). A program instantiates the components with its deployment's names, so the components carry no account ids, zone ids, buckets or secret names.

| dir | component | what it owns | who else writes there |
|---|---|---|---|
| `cf/` | `cfn_dashboard.py` (`CfnDashboard`) | the Cloudflare surface of a `site/` deployment: the Pages project shell, its custom domain + CNAME, preview-branch aliases, the D1 database, an optional `CACHE_KV` namespace, an optional R2 serving bucket, an optional Zero Trust Access gate | `wrangler pages deploy` fills each deploy's bindings and vars from the branch's `site/wrangler.toml` |
| `gcp/` | `gcp_jobs.py` (`JobAccount`, `Secrets`, `BatchCron`, `RunJobCron`, `grant_secret`, `grant_bucket`) | a deployment's scheduled jobs: their service accounts and project roles, Secret Manager containers and accessors, Cloud Scheduler crons (→ Batch, or → a Cloud Run job), bucket grants | the Batch spec is generated from the branch's `job/*-submit.sh` under `PIN=1 DRY=1`; a Cloud Run job's image and plain env are its deploy script's |
| `cf/` (`local`) | `capture_trigger.py` (`CaptureTrigger`) | a laptop deployment's capture trigger: the queue, and the R2 event notification that publishes a finished capture's marker (`captures/…/_SUCCESS.json`) to it | the consumer Worker (`capture-trigger/`) is wrangler's, from its own `wrangler.toml` |
| `aws/` (`local`) | `batch_ingest.py` (`BatchIngest`) | a laptop deployment's ingest: the image (built on CodeBuild by `build-image`, no local Docker), ECR repo, Fargate Spot queue, job definition, its roles and Secrets Manager containers, and the IAM user the capture trigger submits jobs as | secret values and the trigger user's access key go in out of band (the deployment's `put-secrets`) |

Two rules hold across `gcp/`: secret **values** are never managed (containers and IAM only; payloads go in with `gcloud secrets versions add`), and IAM is additive (`IAMMember`), so a stack can't clobber anyone else's grants on a shared project or bucket. `aws/` keeps the first rule the same way (Secrets Manager containers only).

## Running a stack

```bash
cd infra/<cf|gcp>
pulumi preview -s <stack>   # read-only; `up` is a human's call
```

Pulumi runs the program under `uv`, from this directory's `pyproject.toml`. Each program checks that the selected stack is its branch's own.

## Tests

The components' tests run under Pulumi's mocks, with no cloud account: every resource a component declares, its names and its key inputs.

```bash
uv run --project infra --with pytest pytest infra/tests
```

Give it its own environment (`UV_PROJECT_ENVIRONMENT=infra/.venv`). Run in a shell where the repo's `.venv` is active, `uv run` would sync infra's dependencies into the engine's venv.

## Moving inline resources into a component

A stack that declared resources inline adopts a component with `moved_from_root=True` (`BatchIngest`, `CaptureTrigger`): each child is aliased to the same-named resource at the stack root, so its URN moves with no replacement. The bar is a `pulumi preview` with 0 to create, replace or delete.

## Adopting live resources

A deployment's resources mostly predate its stack, so a stack starts by importing them. Every component takes `existing=` per resource, and the stack's `adopting` config turns those into import ids all at once (`gcp_jobs.Adopt`). The first `up` with `adopting: "true"` imports; then set it to `"false"`. Read the adopting preview before running `up`: anything other than `= import`, or an `~ update` you can explain, means the code doesn't match what's live.
