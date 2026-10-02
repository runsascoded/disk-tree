"""m3's Cloudflare stack, thin wiring over the CfnDashboard component.

The component (`cfn_dashboard.py`) is the shared one, byte-identical to the
gcs / cw-s3 branches' copies; this file is m3's instance: the `disk-tree` Pages
project (disk.rbw.sh), the rbw.sh CNAME, and the `laptop` store's D1
(specs/m3-site.md Phase 1). No Access app: the site gates in-app, and the
legacy Access app on `/auth/sso` (ui/'s SSO) is retired by hand at cut-over.
"""

import pulumi

from capture_trigger import CaptureTrigger
from cfn_dashboard import CfnDashboard, Store

cfg = pulumi.Config()
account_id = cfg.require("accountId")
zone_id = cfg.require("zoneId")
import_ids = cfg.get_object("importIds") or None

STACK = pulumi.get_stack()
STORE = Store(
    pages_project=cfg.require("pagesProject"),
    production_branch=cfg.get("productionBranch") or "main",
    domain=cfg.require("domain"),
    d1_name=cfg.require("d1Name"),
)

dash = CfnDashboard(STACK, account_id=account_id, zone_id=zone_id, store=STORE, import_ids=import_ids)

# dev.disk.rbw.sh — a SEPARATE Pages project (`disk-tree-dev`) for the `site/`
# preview while disk.rbw.sh still serves `ui/`, then the staging mirror. It
# binds the same D1 (`site/wrangler.dev.toml`), so it's the Pages shell, the
# domain and the CNAME only — not a second CfnDashboard (which would create a
# D1 of its own); the same three resources the component makes for production.
import pulumi_cloudflare as cloudflare

DEV_PROJECT, DEV_DOMAIN = cfg.require("devProject"), cfg.require("devDomain")
dev_pages = cloudflare.PagesProject(
    "dev-pages",
    account_id=account_id,
    name=DEV_PROJECT,
    production_branch="main",
    build_config=cloudflare.PagesProjectBuildConfigArgs(destination_dir="dist"),
    opts=pulumi.ResourceOptions(ignore_changes=["deployment_configs"]),   # wrangler's
)
dev_cname = cloudflare.DnsRecord(
    "dev-cname",
    zone_id=zone_id,
    name=DEV_DOMAIN,
    type="CNAME",
    content=f"{DEV_PROJECT}.pages.dev",
    ttl=1,
    proxied=True,
    opts=pulumi.ResourceOptions(ignore_changes=["comment"]),
)
dev_domain = cloudflare.PagesDomain(
    "dev-domain",
    account_id=account_id,
    project_name=dev_pages.name,
    name=DEV_DOMAIN,
    opts=pulumi.ResourceOptions(depends_on=[dev_cname]),
)

# The capture trigger (specs/capture-ingest-trigger.md): a capture's
# `_SUCCESS.json` (written last, so the capture is complete) landing under
# `captures/` in the `disk-tree` bucket → this queue → the `capture-trigger`
# Worker (`infra/cf/capture-trigger/`, deployed by wrangler, which attaches it as the
# consumer) → Batch `SubmitJob`. Laptops then need only R2 write.
trigger = CaptureTrigger(
    "captures",
    account_id=account_id,
    bucket=cfg.require("capturesBucket"),
    queue_name=cfg.require("capturesQueue"),
    moved_from_root=True,   # declared at the stack root before the component
)

pulumi.export("pages_project", dash.pages.name)
pulumi.export("dev_pages_project", dev_pages.name)
pulumi.export("dev_domain", dev_domain.name)
pulumi.export("custom_domain", dash.domain.name)
pulumi.export("d1_database", dash.d1.name)
pulumi.export("d1_database_id", dash.d1.id)   # → site/wrangler.toml `database_id`
pulumi.export("captures_queue", trigger.queue.queue_name)
