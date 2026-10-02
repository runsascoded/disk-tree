"""m3's AWS Batch ingest (RAC account; specs/m3-site.md): the `BatchIngest`
component (`batch_ingest.py`) wired with m3's names from `Pulumi.rac.yaml`.

The laptop only walks (`disk-tree capture` → layer-1 shards in R2); the
component owns what runs after: the arm64 ingest image built on CodeBuild,
the Fargate Spot queue, the job definition, and the capture trigger's IAM
user. Secrets are created empty; `aws/put-secrets` fills them from `.envrc`,
so values never enter Pulumi state.
"""
from pathlib import Path

import pulumi

from batch_ingest import BatchIngest, IngestSpec

cfg = pulumi.Config()
REPO_ROOT = Path(__file__).resolve().parents[2]

ingest = BatchIngest(
    "ingest",
    IngestSpec(
        prefix=cfg.require("prefix"),
        repo_root=REPO_ROOT,
        sources=tuple(cfg.require_object("sources")),
        dockerfile=cfg.require("dockerfile"),
        vcpu=cfg.get("vcpu") or "4",
        memory_mib=cfg.get("memory_mib") or "16384",
        ephemeral_gib=cfg.get_int("ephemeral_gib") or 50,
        # Plain env for the job (e.g. `R2_ENDPOINT_URL`).
        env=cfg.get_object("env") or {},
        # Env var → Secrets Manager secret name (created empty here).
        secrets=cfg.get_object("secrets") or {},
    ),
    # The resources were declared at the stack root before the component.
    moved_from_root=True,
)

pulumi.export("queue", ingest.queue.name)
pulumi.export("job_definition", ingest.job_def.name)
pulumi.export("image", ingest.image)
pulumi.export("log_group", ingest.log_group.name)
pulumi.export("capture_trigger_user", ingest.trigger_user.name)
