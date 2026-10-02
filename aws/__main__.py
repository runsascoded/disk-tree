"""m3's AWS Batch ingest (RAC account; specs/m3-site.md).

The laptop only walks (`disk-tree capture` → layer-1 shards in R2); this
stack owns what runs after: an arm64 image built on CodeBuild from this
worktree's sources (no local Docker), a
Fargate Spot queue, and the `disk-tree-m3-ingest` job definition, whose
container reads/writes R2 with keys injected from Secrets Manager. Pattern
and naming follow `hccs/crashes/batch/infra` (nj-crashes).

Secrets are created empty; `aws/put-secrets` fills them from `.envrc`, so
values never enter Pulumi state.
"""
import hashlib
import json
from pathlib import Path

import pulumi
import pulumi_aws as aws
import pulumi_command as command

cfg = pulumi.Config()
REGION = aws.config.region or "us-east-1"
PREFIX = "disk-tree-m3"
VCPU = cfg.get("vcpu") or "4"
MEMORY_MIB = cfg.get("memory_mib") or "16384"
EPHEMERAL_GIB = cfg.get_int("ephemeral_gib") or 50
# Plain env for the job (e.g. `R2_ENDPOINT_URL`).
ENV: dict[str, str] = cfg.get_object("env") or {}
# Env var → Secrets Manager secret name (created empty here).
SECRETS: dict[str, str] = cfg.get_object("secrets") or {}
REPO_ROOT = Path(__file__).resolve().parent.parent

default_vpc = aws.ec2.get_vpc(default=True)
subnets = aws.ec2.get_subnets(filters=[aws.ec2.GetSubnetsFilterArgs(name="vpc-id", values=[default_vpc.id])])
default_sg = aws.ec2.get_security_group(vpc_id=default_vpc.id, name="default")

ECS_TASKS_TRUST = json.dumps({
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": {"Service": "ecs-tasks.amazonaws.com"},
        "Action": "sts:AssumeRole",
    }],
})

# --- Execution role: the ECS agent (image pull, logs, read the injected secrets) ---
execution_role = aws.iam.Role("execution-role", name=f"{PREFIX}-batch-execution", assume_role_policy=ECS_TASKS_TRUST)
aws.iam.RolePolicyAttachment(
    "execution-ecs-policy",
    role=execution_role.name,
    policy_arn="arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy",
)

secret_arns: dict[str, pulumi.Output[str]] = {
    env_var: aws.secretsmanager.Secret(f"secret-{name.replace('/', '-')}", name=name, recovery_window_in_days=0).arn
    for env_var, name in SECRETS.items()
}
if secret_arns:
    aws.iam.RolePolicy(
        "execution-secrets-policy",
        role=execution_role.id,
        policy=pulumi.Output.all(*secret_arns.values()).apply(lambda arns: json.dumps({
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Action": "secretsmanager:GetSecretValue", "Resource": list(arns)}],
        })),
    )

# --- Task role: the container's identity. Data lives in R2 (keys from
#     SECRETS), so it gets no AWS grants. ---
task_role = aws.iam.Role("task-role", name=f"{PREFIX}-batch-task", assume_role_policy=ECS_TASKS_TRUST)

repo = aws.ecr.Repository(
    "repo",
    name=PREFIX,
    image_tag_mutability="MUTABLE",
    force_delete=True,
    encryption_configurations=[aws.ecr.RepositoryEncryptionConfigurationArgs(encryption_type="AES256")],
)
aws.ecr.LifecyclePolicy(
    "repo-lifecycle",
    repository=repo.name,
    policy=json.dumps({"rules": [{
        "rulePriority": 1,
        "description": "keep the 5 newest images",
        "selection": {"tagStatus": "any", "countType": "imageCountMoreThan", "countNumber": 5},
        "action": {"type": "expire"},
    }]}),
)

# --- The image, built on CodeBuild (not the laptop's Docker): Pulumi zips the
#     files the Dockerfile needs (content-hashed), uploads them, and
#     `aws/build-image` runs one build per new hash, printing the pinned digest. ---
SOURCES = ["pyproject.toml", "uv.lock", "README.md", "src", "cloud/pyproject.toml", "cloud/src", "aws/Dockerfile", "aws/ingest.sh"]


def _source_files() -> list[Path]:
    files = []
    for rel in SOURCES:
        p = REPO_ROOT / rel
        files += sorted(f for f in p.rglob("*") if f.is_file() and "__pycache__" not in f.parts) if p.is_dir() else [p]
    return files


def _source_hash(files: list[Path]) -> str:
    h = hashlib.sha256()
    for f in files:
        h.update(str(f.relative_to(REPO_ROOT)).encode() + b"\0" + f.read_bytes() + b"\0")
    return h.hexdigest()[:16]


files = _source_files()
SRC_HASH = _source_hash(files)

build_bucket = aws.s3.Bucket("build-src", bucket=f"{PREFIX}-build-src-{aws.get_caller_identity().account_id}", force_destroy=True)
aws.s3.BucketLifecycleConfiguration(
    "build-src-lifecycle",
    bucket=build_bucket.id,
    rules=[aws.s3.BucketLifecycleConfigurationRuleArgs(
        id="expire-sources", status="Enabled",
        filter=aws.s3.BucketLifecycleConfigurationRuleFilterArgs(prefix="src/"),
        expiration=aws.s3.BucketLifecycleConfigurationRuleExpirationArgs(days=30),
    )],
)
src_key = f"src/{SRC_HASH}.zip"
src_obj = aws.s3.BucketObjectv2(
    "build-src-zip",
    bucket=build_bucket.id,
    key=src_key,
    source=pulumi.AssetArchive({str(f.relative_to(REPO_ROOT)): pulumi.FileAsset(str(f)) for f in files}),
)

codebuild_role = aws.iam.Role("codebuild-role", name=f"{PREFIX}-codebuild", assume_role_policy=json.dumps({
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Principal": {"Service": "codebuild.amazonaws.com"}, "Action": "sts:AssumeRole"}],
}))
aws.iam.RolePolicy(
    "codebuild-policy",
    role=codebuild_role.id,
    policy=pulumi.Output.all(build_bucket.arn, repo.arn).apply(lambda a: json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"], "Resource": "*"},
            {"Effect": "Allow", "Action": ["s3:GetObject", "s3:GetObjectVersion"], "Resource": f"{a[0]}/*"},
            {"Effect": "Allow", "Action": "ecr:GetAuthorizationToken", "Resource": "*"},
            {"Effect": "Allow", "Action": [
                "ecr:BatchCheckLayerAvailability", "ecr:BatchGetImage", "ecr:CompleteLayerUpload",
                "ecr:GetDownloadUrlForLayer", "ecr:InitiateLayerUpload", "ecr:PutImage", "ecr:UploadLayerPart",
            ], "Resource": a[1]},
        ],
    })),
)
BUILDSPEC = """version: 0.2
phases:
  pre_build:
    commands:
      - aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin ${REPO_URL%%/*}
  build:
    commands:
      - docker build -f aws/Dockerfile -t $REPO_URL:$TAG .
  post_build:
    commands:
      - docker push $REPO_URL:$TAG
"""
codebuild_logs = aws.cloudwatch.LogGroup("codebuild-logs", name=f"/{PREFIX}/codebuild", retention_in_days=30)
project = aws.codebuild.Project(
    "image-build",
    name=f"{PREFIX}-image",
    service_role=codebuild_role.arn,
    build_timeout=30,
    artifacts=aws.codebuild.ProjectArtifactsArgs(type="NO_ARTIFACTS"),
    environment=aws.codebuild.ProjectEnvironmentArgs(
        type="ARM_CONTAINER",
        image="aws/codebuild/amazonlinux-aarch64-standard:3.0",
        compute_type="BUILD_GENERAL1_SMALL",
        privileged_mode=True,
        environment_variables=[aws.codebuild.ProjectEnvironmentEnvironmentVariableArgs(name="REPO_URL", value=repo.repository_url)],
    ),
    source=aws.codebuild.ProjectSourceArgs(
        type="S3",
        location=pulumi.Output.concat(build_bucket.bucket, "/", src_key),
        buildspec=BUILDSPEC,
    ),
    logs_config=aws.codebuild.ProjectLogsConfigArgs(
        cloudwatch_logs=aws.codebuild.ProjectLogsConfigCloudwatchLogsArgs(group_name=codebuild_logs.name),
    ),
)
build = command.local.Command(
    "image-build-run",
    create=pulumi.Output.concat(
        "python3 aws/build-image ", project.name, " ", build_bucket.bucket, "/", src_key, " ", repo.name, " ", SRC_HASH,
    ),
    dir=str(REPO_ROOT),
    triggers=[SRC_HASH],
    opts=pulumi.ResourceOptions(depends_on=[src_obj, project]),
)
image_ref = build.stdout

compute_env = aws.batch.ComputeEnvironment(
    "spot",
    name=f"{PREFIX}-spot",
    type="MANAGED",
    compute_resources=aws.batch.ComputeEnvironmentComputeResourcesArgs(
        type="FARGATE_SPOT",
        max_vcpus=16,
        subnets=subnets.ids,
        security_group_ids=[default_sg.id],
    ),
)
queue = aws.batch.JobQueue(
    "spot",
    name=PREFIX,
    state="ENABLED",
    priority=1,
    compute_environment_orders=[aws.batch.JobQueueComputeEnvironmentOrderArgs(order=1, compute_environment=compute_env.arn)],
)

log_group = aws.cloudwatch.LogGroup("log-group", name=f"/{PREFIX}-ingest/batch", retention_in_days=30)


def _container_props(args: dict) -> str:
    return json.dumps({
        "image": args["image"],
        "runtimePlatform": {"operatingSystemFamily": "LINUX", "cpuArchitecture": "ARM64"},
        "resourceRequirements": [{"type": "VCPU", "value": VCPU}, {"type": "MEMORY", "value": MEMORY_MIB}],
        "executionRoleArn": args["exec_arn"],
        "jobRoleArn": args["task_arn"],
        "environment": [{"name": "PYTHONFAULTHANDLER", "value": "1"}, *({"name": k, "value": v} for k, v in ENV.items())],
        "secrets": [{"name": k, "valueFrom": v} for k, v in args["secrets"].items()],
        "networkConfiguration": {"assignPublicIp": "ENABLED"},
        "fargatePlatformConfiguration": {"platformVersion": "LATEST"},
        "ephemeralStorage": {"sizeInGiB": EPHEMERAL_GIB},
        "logConfiguration": {
            "logDriver": "awslogs",
            "options": {"awslogs-group": args["log_group"], "awslogs-region": REGION, "awslogs-stream-prefix": PREFIX},
        },
    })


job_def = aws.batch.JobDefinition(
    "ingest",
    name=f"{PREFIX}-ingest",
    type="container",
    platform_capabilities=["FARGATE"],
    # Spot reclaims retry; a genuine failure exits.
    retry_strategy=aws.batch.JobDefinitionRetryStrategyArgs(
        attempts=3,
        evaluate_on_exits=[
            aws.batch.JobDefinitionRetryStrategyEvaluateOnExitArgs(action="RETRY", on_status_reason="Your Spot Task*"),
            aws.batch.JobDefinitionRetryStrategyEvaluateOnExitArgs(action="EXIT", on_reason="*"),
        ],
    ),
    timeout=aws.batch.JobDefinitionTimeoutArgs(attempt_duration_seconds=2 * 3600),
    container_properties=pulumi.Output.all(
        image=image_ref,
        secrets=pulumi.Output.all(**secret_arns),
        log_group=log_group.name,
        exec_arn=execution_role.arn,
        task_arn=task_role.arn,
    ).apply(_container_props),
)

# --- The capture trigger's caller (specs/capture-ingest-trigger.md) ---
# The Cloudflare Worker that submits an ingest when a capture's `_SUCCESS.json`
# lands in R2. It may submit this one job definition to this one queue and
# nothing else. Its access key is made outside Pulumi (`cf/capture-trigger/
# put-secrets` pipes it straight into the Worker's secrets), so no key value
# enters state.
account_id = aws.get_caller_identity().account_id
trigger_user = aws.iam.User("capture-trigger", name=f"{PREFIX}-capture-trigger")
aws.iam.UserPolicy(
    "capture-trigger-submit",
    user=trigger_user.name,
    policy=queue.arn.apply(lambda queue_arn: json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": "batch:SubmitJob",
            "Resource": [
                queue_arn,
                f"arn:aws:batch:{REGION}:{account_id}:job-definition/{PREFIX}-ingest",
                f"arn:aws:batch:{REGION}:{account_id}:job-definition/{PREFIX}-ingest:*",
            ],
        }],
    })),
)

pulumi.export("queue", queue.name)
pulumi.export("job_definition", job_def.name)
pulumi.export("image", image_ref)
pulumi.export("log_group", log_group.name)
pulumi.export("capture_trigger_user", trigger_user.name)
