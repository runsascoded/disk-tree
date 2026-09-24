-- @open-athena/auth package migration 0010 (`migrations/0010_pending_auth_ip.sql` at the
-- pinned dist), verbatim below. cw-s3 adopts the package's session model —
-- grants, access log, requests, pending email-code auth — for the ZT-free
-- sign-in (specs/oidc-cutover-cw.md); gcs carries the same tables in its own
-- lineage.

-- Per-IP rate limiting for email-code sign-ins. `0009` capped sends per address;
-- this adds the hashed client IP so `start` can also cap sends per source,
-- blunting an IP that fans out across many allowed addresses. The IP is HMAC'd
-- (never stored raw), exactly as `access_requests.ip_hash` is.
--
-- Added as a follow-on migration rather than editing `0009` because `0009` has
-- already shipped on the `dist` branch; a consumer that applied it must get this
-- as a separate step.
ALTER TABLE pending_auth ADD COLUMN ip_hash TEXT;

-- Rate-limiting reads "how many sends from this IP recently".
CREATE INDEX pending_auth_ip ON pending_auth (ip_hash, created_at);
