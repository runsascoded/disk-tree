-- @open-athena/auth package migration 0009 (`migrations/0009_pending_auth.sql` at the
-- pinned dist), verbatim below. cw-s3 adopts the package's session model —
-- grants, access log, requests, pending email-code auth — for the ZT-free
-- sign-in (specs/oidc-cutover-cw.md); gcs carries the same tables in its own
-- lineage.

-- Pending email-code sign-ins: the passwordless path for addresses that can't
-- (or won't) authenticate with Google. One row backs both a magic link and a
-- 6-digit code — the link is the happy path, the code rescues the cross-device
-- case (email on your phone, the tab on your laptop) and survives link-
-- prefetching scanners that would otherwise consume a single-use URL.
--
-- Both the token and the code are stored hashed (SHA-256, base64url), exactly
-- like a grant token; the row is single-use (`consumed_at`) and short-lived
-- (`expires_at`), and `attempts` is capped so the 6-digit code can't be
-- brute-forced before it expires.
CREATE TABLE pending_auth (
  id          TEXT PRIMARY KEY,
  email       TEXT NOT NULL,
  token_hash  TEXT NOT NULL,
  code_hash   TEXT NOT NULL,
  created_at  INTEGER NOT NULL,
  expires_at  INTEGER NOT NULL,
  consumed_at INTEGER,
  attempts    INTEGER NOT NULL DEFAULT 0
);

-- The magic link is looked up by its hashed token.
CREATE UNIQUE INDEX pending_auth_token ON pending_auth (token_hash);

-- Rate-limiting reads "how many sends to this address recently".
CREATE INDEX pending_auth_email ON pending_auth (email, created_at);
