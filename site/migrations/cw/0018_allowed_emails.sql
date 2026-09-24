-- App-owned sign-in allowlist (gcs's migration 0008, minus the `admin_edits`
-- audit table cw already has from 0004).
--
-- Off Zero Trust (specs/oidc-cutover-cw.md) the app's email policy decides who
-- may view: staff (`STAFF_DOMAIN`) and the viewer domains (`VIEWER_DOMAINS`,
-- what the Access policy's email-domain include used to admit) need no row;
-- anyone else gets the base scope only while listed here. Sessions re-derive
-- scopes per request, so removing a row de-authorizes existing sessions
-- immediately. Editable at /admin/db.

CREATE TABLE allowed_emails (
  email TEXT PRIMARY KEY,               -- lowercased
  note  TEXT,                           -- who this is / where they're from
  who   TEXT NOT NULL,                  -- admin who added the row
  ts    INTEGER NOT NULL                -- epoch seconds
);
