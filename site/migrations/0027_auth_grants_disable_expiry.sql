-- Adopt @open-athena/auth's disable/expiry policy (package migration 0007): the
-- current package's grant store selects these columns, so the bump needs them.
-- `disabled_at` stops new redemptions while leaving live sessions alone;
-- `expiry_ends_sessions` decides whether a link's own expiry also ends the
-- sessions derived from it (1 = yes, the data-room default). See
-- specs/share-link-hardening.md (the auth-package adoption).
ALTER TABLE grants ADD COLUMN disabled_at INTEGER;
ALTER TABLE grants ADD COLUMN expiry_ends_sessions INTEGER NOT NULL DEFAULT 1;
CREATE INDEX IF NOT EXISTS grants_disabled_at ON grants (disabled_at);
