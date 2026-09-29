"""m3's AWS Batch ingest (RAC account; specs/m3-site.md).

The laptop only walks (`disk-tree capture` → layer-1 shards in R2); this
stack owns what runs after: an arm64 image built from this worktree, a
Fargate Spot queue, and the `disk-tree-m3-ingest` job definition, whose
container reads/writes R2 with keys injected from Secrets Manager. Pattern
and naming follow `hccs/crashes/batch/infra` (nj-crashes).

Secrets are created empty; `aws/put-secrets` fills them from `.envrc`, so
values never enter Pulumi state.
"""
import json
from pathlib import Path

import pulumi
import pulumi_aws as aws
import pulumi_docker_build as docker_build

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
ecr_auth = aws.ecr.get_authorization_token_output(registry_id=repo.registry_id)

image = docker_build.Image(
    "image",
    context=docker_build.BuildContextArgs(location=str(REPO_ROOT)),
    dockerfile=docker_build.DockerfileArgs(location=str(REPO_ROOT / "aws" / "Dockerfile")),
    platforms=[docker_build.Platform.LINUX_ARM64],
    tags=[repo.repository_url.apply(lambda url: f"{url}:latest")],
    push=True,
    registries=[docker_build.RegistryArgs(
        address=repo.repository_url,
        username=ecr_auth.user_name,
        password=ecr_auth.password,
    )],
)

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
        image=image.ref,
        secrets=pulumi.Output.all(**secret_arns),
        log_group=log_group.name,
        exec_arn=execution_role.arn,
        task_arn=task_role.arn,
    ).apply(_container_props),
)

pulumi.export("queue", queue.name)
pulumi.export("job_definition", job_def.name)
pulumi.export("image", image.ref)
pulumi.export("log_group", log_group.name)
