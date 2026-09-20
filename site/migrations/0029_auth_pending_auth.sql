-- Pending email-code / magic-link sign-ins (package migrations 0009 + 0010):
-- opt-in (only used by the email-code / OIDC handlers, which gcs doesn't mount).
-- Token and code are stored hashed, single-use, short-lived; `ip_hash` (HMAC'd)
-- backs per-IP send rate-limiting. Added for schema alignment with the package.
CREATE TABLE IF NOT EXISTS pending_auth (
  id          TEXT PRIMARY KEY,
  email       TEXT NOT NULL,
  token_hash  TEXT NOT NULL,
  code_hash   TEXT NOT NULL,
  created_at  INTEGER NOT NULL,
  expires_at  INTEGER NOT NULL,
  consumed_at INTEGER,
  attempts    INTEGER NOT NULL DEFAULT 0,
  ip_hash     TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS pending_auth_token ON pending_auth (token_hash);
CREATE INDEX IF NOT EXISTS pending_auth_email ON pending_auth (email, created_at);
CREATE INDEX IF NOT EXISTS pending_auth_ip ON pending_auth (ip_hash, created_at);
