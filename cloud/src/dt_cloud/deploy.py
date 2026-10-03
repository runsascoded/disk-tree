"""The deployment's own resources, from its environment (specs/oa-decoupling.md).

dt-cloud carries no deployment's bucket or site as a default: each job script
(or an operator's `.envrc`) exports them. A missing one is an error naming the
variable, never a fallback to some other deployment's resources.
"""
import os


def data_bucket() -> str:
    """The data bucket: snapshots, index tiers, sweep plans and state (`$DATA_BUCKET`)."""
    b = os.environ.get("DATA_BUCKET", "").strip()
    if not b:
        raise SystemExit("DATA_BUCKET is unset: export the deployment's data bucket (snapshots, index tiers, sweep state)")
    return b


def site_url(explicit: str | None = None) -> str:
    """The deployment's site: `explicit`, else `$SITE_URL`, else `$GCS_USAGE_URL`."""
    u = explicit or os.environ.get("SITE_URL") or os.environ.get("GCS_USAGE_URL")
    if not u:
        raise SystemExit("no site URL: pass -u or export SITE_URL")
    return u.rstrip("/")
