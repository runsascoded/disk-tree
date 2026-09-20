-- Self-set SSO identity (package migration 0008): a signed-in user's own chosen
-- name/face, keyed by email. Opt-in (only read when a `profiles` store is
-- configured on the gate — gcs doesn't yet). Added now purely to keep this
-- deployment's auth schema aligned with @open-athena/auth rather than carrying a
-- divergence forward; adopting the feature later then needs no migration.
CREATE TABLE IF NOT EXISTS profiles (
  email       TEXT PRIMARY KEY,
  first       TEXT,
  last        TEXT,
  avatar      TEXT,
  avatar_src  TEXT,          -- 'upload' | 'url' | 'github' | 'gravatar'
  updated_at  INTEGER NOT NULL
);
