-- Personal agent tokens (gcs's migration 0011; specs/actions-ledger.md § API).
--
-- One row per user: the id of the grant currently serving as their personal
-- token (scoped to this deployment's base scope, `cw`), so rotation can revoke
-- the predecessor and the UI can show "active since <created>" without ever
-- seeing the token (only its hash lives in the grants table). The raw token is
-- shown exactly once, at mint. The job's warm-cache stage authenticates with
-- one of these (`GCS_USAGE_TOKEN`) once the host is off Zero Trust.
CREATE TABLE agent_tokens (
  email    TEXT PRIMARY KEY,          -- lower-cased owner email
  grant_id TEXT NOT NULL,             -- grants.id of the live token grant
  created  INTEGER NOT NULL           -- unix seconds, server-assigned
);
