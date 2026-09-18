# `@disk-tree/iac` — the CFN reference deploy, as code

The staged-delete executors (spec [`specs/staged-delete.md`](../specs/staged-delete.md)) need per-deployment infrastructure. This package holds the **`CfnDashboard`** Pulumi component — "the Vite + Cloudflare-Pages-Functions reference deploy" the R2 / S3 / GCS deployments share — and the repo ships a generator (`disk-tree iac`) that derives its config from `buckets.yml`, so setup is one source of truth rather than hand-maintained parallel config.

Design rationale + the broader inventory: marin-gcs-usage `specs/cf-iac.md` (Pulumi, one stack per deployment branch).

## The two axes it provisions (spec CP6–CP8)

Deletion has two orthogonal axes; IaC provisions the **executor** axis:

| executor | small scope | large scope |
|---|---|---|
| **R2** | `CfnDashboard` binds `R2_<bucket>` → the edge Worker deletes inline (**CP7, done**) | Worker queue / Durable-Object chain |
| **S3** | drainer inline `aws s3 rm` (**CP4, done**) | AWS Batch (`batch: {provider: 'aws'}`) |
| **GCS** | Cloud Function | GCP Batch (what gcs runs today) |

The R2 small cell is live (CP7); the `batch` arg is the extension point for the large-scope cells.

## `CfnDashboard` (`index.ts`)

One component stands up: the **Pages project** (with `DB` D1 + `SCANS` R2 + `R2_<bucket>` executor bindings + `SCANS_PREFIX`/`DELETE_THRESHOLD` vars), the **D1 auth database**, the **R2 data bucket**, and one **R2 bucket per CP7 executor binding**. Identity-bearing config (`SESSION_SECRET`, `ALLOWED_EMAILS`, `ACCESS_*`) stays Pulumi secrets set per stack — not in the component.

```ts
new CfnDashboard('demo', {
  accountId: cfAccount,
  ...JSON.parse(iacConfigJson),   // `disk-tree iac config`
})
```

## The generator (`disk-tree iac`, tested in `tests/test_iac.py`)

```bash
disk-tree iac r2-bindings   # [[r2_buckets]] wrangler.toml blocks — activates the CP7 CFN
disk-tree iac config        # the CfnDashboard component config (JSON) for this deployment
```

`iac r2-bindings` is immediately usable: paste its output into `ui/wrangler.toml` and redeploy to bind your R2 buckets (each `r2://<bucket>` in `buckets.yml` → an `R2_<bucket>` binding), which turns small R2 dispatches from drainer-deferred into edge-inline.

## Applying

Not applied from this repo (which ships no `@pulumi` deps). In an environment with the Pulumi SDK + CF credentials (e.g. OA's `~/c/oa/ops`):

```bash
pnpm install && pnpm typecheck   # provider fields track @pulumi/cloudflare v5
pulumi up                        # import existing resources first, so the first up is a no-op diff
```

Deployments themselves (`wrangler pages deploy`) and D1 migrations (`wrangler d1 migrations apply`) stay with the app — Pulumi owns the resources, not the container or the schema.
