"""`disk-tree iac` — generate executor deployment config from buckets.yml (spec
``specs/staged-delete.md``, CP8).

- ``iac config`` — the ``CfnDashboard`` Pulumi component config (``iac/``).
- ``iac aws-batch`` — the Terraform tfvars for the AWS Batch delete executor
  (``iac/aws/``).
"""
from __future__ import annotations

import json

from click import echo, option

from disk_tree.cli.base import cli


@cli.group("iac")
def iac():
    """Infrastructure-as-code helpers: derive deployment config from buckets.yml."""


@iac.command("config")
@option("-c", "--config", "config_path", default=None, help="buckets.yml path (default: <DISK_TREE_ROOT>/buckets.yml)")
@option("-p", "--project", default="disk-tree", help="CfnDashboard project name")
def config_cmd(config_path: str | None, project: str):
    """Emit the `CfnDashboard` Pulumi component config (JSON) for this deployment."""
    from disk_tree.cli.sync import load_config
    from disk_tree.iac import dashboard_config

    echo(json.dumps(dashboard_config(load_config(config_path), project=project), indent=2))


@iac.command("aws-batch")
@option("-c", "--config", "config_path", default=None, help="buckets.yml path (default: <DISK_TREE_ROOT>/buckets.yml)")
@option("-p", "--project", default="disk-tree", help="Batch resource name prefix")
@option("-r", "--region", default="us-east-1", help="AWS region")
def aws_batch(config_path: str | None, project: str, region: str):
    """Emit the Terraform tfvars for the AWS Batch delete executor (`iac/aws/`)."""
    from disk_tree.cli.sync import load_config
    from disk_tree.iac import aws_batch_tfvars

    echo(aws_batch_tfvars(load_config(config_path), region=region, project=project), nl=False)
