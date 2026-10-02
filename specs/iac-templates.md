# IaC templates: stand up a deployment from public components + one config file per stack

**From:** m3 (`wt/m3`), 2026-10-02. **For:** the root session (`cloud` / `local` components), then m3 as the first consumer.

**Status (2026-10-02):** phase 1 done (root).
- `local` be830d5: `BatchIngest` (`infra/aws/batch_ingest.py`, + `build-image`) and `CaptureTrigger` (`infra/cf/capture_trigger.py`). Both take `moved_from_root=True`, which aliases each child to its old root-level logical name. `infra/tests/` checks both under Pulumi's mocks.
- `cloud` bf8ff8e: `site/wrangler.example.toml` + `functions/_lib/wranglerExample.test.ts`.
- On `aws/` → `infra/aws/`: yes, the component already lives there. m3's program and `Pulumi.yaml` move beside it.
- Next: m3's rewire (phase 2), then the README walkthrough (phase 3).

## Goal

Someone who isn't us should be able to stand up a disk-tree deployment from the public repo by writing config, not code: copy `Pulumi.example.yaml` → `Pulumi.<stack>.yaml`, fill in names and ids, `pulumi up`. The same goes for each Worker's and the site's `wrangler.toml`.

`infra/README.md` already sets the rule: components carry no account ids, zone ids, buckets or secret names; each deployment branch adds thin stack programs that instantiate them. `CfnDashboard` (`infra/cf/`) and `gcp_jobs.py` (`infra/gcp/`) follow it. What's missing is (1) components for the laptop/capture side (the AWS Batch ingest, the capture trigger), and (2) programs that take *all* their names from stack config, so a new deployment's program is identical to an existing one's and only the yaml differs.

Supersedes `specs/iac.md` (2026-09-10, the pre-`infra/` design).

## What stays where

- **Public, on `cloud`:** the components (`infra/cf/cfn_dashboard.py`, `infra/gcp/gcp_jobs.py`), `infra/README.md`, `site/wrangler.example.toml`.
- **Public, on `local`:** the laptop-only components below (Batch ingest of captures, capture trigger) and their examples.
- **Per deployment branch (committed):** the stack programs, `Pulumi.<stack>.yaml`, the real `wrangler*.toml`, and Pulumi state (`state/.pulumi/stacks/*.json` only; backups, history and `.bak` sidecars are ignored, as m3 does since `1fd2804`).
- **Never in git or state:** secret values. They go in with `aws/put-secrets` (Secrets Manager), `infra/cf/capture-trigger/put-secrets` (Worker secrets), `wrangler pages secret put`, `gcloud secrets versions add`. Pulumi manages the containers only. Deployment-specific ids (account ids, ARNs, D1 ids) are not secrets and may be committed; m3's audit of its two state files on 2026-10-02 found no plaintext secret.

## Work

### 1. `infra/aws/batch_ingest.py` (on `local`): `BatchIngest`

Extract from m3's `aws/__main__.py` (≈280 lines, all m3-named via `PREFIX` + config already):
- ECR repo + lifecycle, the CodeBuild image build (source zip → S3 build bucket → CodeBuild → `command.local.Command` waits, returns the image ref), the build bucket's 30-day lifecycle.
- Fargate Spot compute env, job queue, log group, job definition (vcpu / memory / ephemeral disk, Spot-retry strategy, timeout).
- Execution + task roles; Secrets Manager containers from a `{env_var: secret_name}` map, readable by the execution role.
- `CaptureTriggerUser`: the IAM user allowed only `batch:SubmitJob` on the queue + job definition (no access key; `put-secrets` mints it).

Inputs: `prefix`, `vcpu`, `memory_mib`, `ephemeral_gib`, `env` (plain), `secrets` (name map), `source_files` (what goes into the image), `network` (default VPC unless given). m3's `aws/Pulumi.rac.yaml` already carries most of these; the program shrinks to reading config and calling the component.

### 2. `infra/cf/capture_trigger.py` (on `local`): `CaptureTrigger`

The queue + R2 event notification m3 added to `infra/cf/__main__.py` (`3e8e6c3`): inputs `account_id`, `bucket`, `queue_name`, `prefix` (`captures/`), `suffix` (`_SUCCESS.json`). The Worker itself stays wrangler's (`infra/cf/capture-trigger/`), with a `wrangler.example.toml`; its real `wrangler.toml` (account id, bucket, queue, `AWS_REGION` / `JOB_QUEUE` / `JOB_DEFINITION`) is per deployment.

### 3. Config-driven programs

Each stack program reads every name from `pulumi.Config()`; no literals. For m3's `infra/cf/__main__.py` that means the `Store(...)` fields (`pages_project`, `domain`, `d1_name`), the optional dev project (`devProject`, `devDomain`), the capture trigger block, and the stack-name guard (`STACK = "rac"` → the program accepts whatever stack its yaml configures). Ship `Pulumi.example.yaml` beside each program with every key, placeholder values and a one-line comment each.

Once programs are config-only they're the same file on every branch; whether they then move to `cloud`/`local` (with branches holding only yaml) is the root session's call.

### 4. `site/wrangler.example.toml` (on `cloud`)

Every binding and var the Functions read (`STORE_*`, `D1`, `EXECUTOR`, `STAGING`, `AUTH_MODE`, …) with placeholders and a comment each; the deployments' real `wrangler*.toml` stay on their branches. Optional: a test that every `env.X` the Functions read appears in the example (catches the `STAGING` miss m3 hit on 2026-10-02, which 404'd staging on prod).

### 5. Toolchain alignment

`infra/README.md` says programs run under `uv` from `infra/pyproject.toml`; m3's `infra/cf/Pulumi.yaml` and `aws/Pulumi.yaml` still use `toolchain: pip` with per-dir `.venv`s. m3 switches to the shared uv environment as part of rewiring (and `aws/` moves under `infra/aws/` if the root session agrees).

### 6. Docs

`infra/README.md` gets a "Starting a deployment" walkthrough: the accounts and tokens needed (with the exact CF token permissions: Pages, D1, Queues, Workers Scripts, Workers R2 Storage Write, DNS Write on the zone), the order (`aws` → `cf` → Worker deploy → `put-secrets` → `site/deploy`), and the out-of-band secrets.

## Verification (m3, after each extraction)

Moving resources into a component changes their URNs, so a naive rewire plans delete + create, which would replace the live Batch queue, the D1 database and the Pages project. Every moved resource gets `aliases=[pulumi.Alias(parent=pulumi.ROOT_STACK_RESOURCE)]` (or the old name), and the bar for merging is **`pulumi preview` on m3's stacks shows 0 to create / replace / delete** (updates explained line by line). The site and Worker tests and a `site/deploy-m3 --dev` round-trip stay green.

## Phasing

1. Root: `BatchIngest` + `CaptureTrigger` components on `local`, `site/wrangler.example.toml` on `cloud`.
2. m3: rewire `aws/` and `infra/cf/` onto them, config-only, uv; verify no-replace previews; commit `Pulumi.example.yaml`s.
3. Root: README walkthrough; decide whether config-only programs move up to `local`/`cloud`.
4. Later: the same treatment for the gcs / cw-s3 programs (already on `CfnDashboard` + `gcp_jobs`, so mostly step 3's config-only rewrite).

## Open

- Do config-only programs live on `cloud`/`local` (branches hold yaml only), or stay per branch as examples? Proposed: decide after m3's rewire shows how much differs.
- `aws/` → `infra/aws/`: move it when the component lands, or leave m3's path alone?
- Scripts with m3 literals (`site/deploy-m3`, `aws/laptop-*`, `aws/submit`: R2 endpoint, host, AWS profiles) → env / config. Lower priority; they're per deployment today.
