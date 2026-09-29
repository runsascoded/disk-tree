"""m3's Cloudflare stack, thin wiring over the CfnDashboard component.

The component (`cfn_dashboard.py`) is the shared one, byte-identical to the
gcs / cw-s3 branches' copies; this file is m3's instance: the `disk-tree` Pages
project (disk.rbw.sh), the rbw.sh CNAME, and the `laptop` store's D1
(specs/m3-site.md Phase 1). No Access app: the site gates in-app, and the
legacy Access app on `/auth/sso` (ui/'s SSO) is retired by hand at cut-over.
"""

import pulumi

from cfn_dashboard import CfnDashboard, Store

cfg = pulumi.Config()
account_id = cfg.require("accountId")
zone_id = cfg.require("zoneId")
import_ids = cfg.get_object("importIds") or None

STACK = "rac"
STORE = Store(
    pages_project="disk-tree",
    production_branch="main",
    domain="disk.rbw.sh",
    d1_name="disk-tree-m3-db",
)

if pulumi.get_stack() != STACK:
    raise pulumi.RunError(f"m3's cf/ wires the {STACK!r} stack only; selected {pulumi.get_stack()!r}")

dash = CfnDashboard(STACK, account_id=account_id, zone_id=zone_id, store=STORE, import_ids=import_ids)

pulumi.export("pages_project", dash.pages.name)
pulumi.export("custom_domain", dash.domain.name)
pulumi.export("d1_database", dash.d1.name)
pulumi.export("d1_database_id", dash.d1.id)   # → site/wrangler.toml `database_id`
