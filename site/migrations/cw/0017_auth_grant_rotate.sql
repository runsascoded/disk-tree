-- @open-athena/auth package migration 0011 (`migrations/0011_grant_rotate.sql` at the
-- pinned dist), verbatim below. cw-s3 adopts the package's session model —
-- grants, access log, requests, pending email-code auth — for the ZT-free
-- sign-in (specs/oidc-cutover-cw.md); gcs carries the same tables in its own
-- lineage.

-- Grant rotation: re-key a leaked share link without losing its identity.
-- `sessions_invalid_before` is the rotation epoch (epoch seconds), stamped by
-- `gate.rotate({ endSessions: true })`. A grant session whose `iat` predates it
-- is rejected on its next request, so re-keying can also boot whoever is already
-- inside. A plain re-key leaves it NULL and existing sessions untouched.
-- `0009`/`0010` already shipped on `dist`, so this is the next number.
ALTER TABLE grants ADD COLUMN sessions_invalid_before INTEGER;
