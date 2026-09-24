-- @open-athena/auth package migration 0005 (`migrations/0005_dedupe_by_event.sql` at the
-- pinned dist), verbatim below. cw-s3 adopts the package's session model —
-- grants, access log, requests, pending email-code auth — for the ZT-free
-- sign-in (specs/oidc-cutover-cw.md); gcs carries the same tables in its own
-- lineage.

-- Widen the dedupe key to include `event`.
--
-- The index previously keyed on (session_sub, path, bucket), which was fine when
-- only `view` rows carried a bucket. Repeat `deny` rows from one dead session
-- now get bucketed too — a revoked link's browser would otherwise write one deny
-- per page load — and without `event` in the key a deny and a view for the same
-- session/path/hour would collide, silently dropping whichever landed second.
DROP INDEX IF EXISTS access_log_view_dedupe;

CREATE UNIQUE INDEX access_log_dedupe ON access_log (event, session_sub, path, bucket) WHERE bucket IS NOT NULL;
