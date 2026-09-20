-- Admin allowlist + audit trail for admin table edits (specs/cw-sweep.md).
-- Adapted from gcs migration 0008 (allowed_emails + admin_edits).
--
-- ROLE DIFFERS FROM gcs: gcs's `allowed_emails` is a *sign-in* allowlist (who
-- gets any scope at all) because gcs.oa.dev is a public shell with app-owned
-- email policy. cw-s3 is whole-host Access-gated (app `4c463052`, OA +
-- coreweave.com domains) — sign-in is already gated at the edge — so this table
-- is the *admin* gate on top of it: only listed emails may mark `sweep` and
-- fire deletes. The sweeper identity itself comes from CF Access
-- (`/cdn-cgi/access/get-identity`); this table decides who among the
-- authenticated may dispatch. It can be seeded/synced from the Access app's
-- identity list, but D1 is the source of truth (editable, IaC-able, removals
-- bite per request).

CREATE TABLE admin_emails (
  email TEXT PRIMARY KEY,               -- lowercased; may mark sweep + dispatch runs
  note  TEXT,                           -- who this is
  who   TEXT NOT NULL,                  -- admin who added the row
  ts    INTEGER NOT NULL                -- epoch seconds
);

-- Append-only history of every write made through the admin API (any table).
CREATE TABLE admin_edits (
  id       INTEGER PRIMARY KEY AUTOINCREMENT,
  tbl      TEXT NOT NULL,
  pk       TEXT NOT NULL,
  action   TEXT NOT NULL CHECK (action IN ('insert', 'update', 'delete')),
  who      TEXT NOT NULL,
  ts       INTEGER NOT NULL,
  old_json TEXT,
  new_json TEXT
);
CREATE INDEX admin_edits_tbl ON admin_edits (tbl, ts);
