-- @open-athena/auth package migration 0004 (`migrations/0004_access_log_daily.sql` at the
-- pinned dist), verbatim below. cw-s3 adopts the package's session model —
-- grants, access log, requests, pending email-code auth — for the ZT-free
-- sign-in (specs/oidc-cutover-cw.md); gcs carries the same tables in its own
-- lineage.

-- Daily rollups. Raw `access_log` rows answer "what exactly did Bob do last
-- Tuesday"; that question decays fast, while "how much traffic, from whom, over
-- the last year" doesn't. Rolling up and dropping the raw rows keeps the log
-- bounded without losing the long-range shape — the retention job a hosted
-- analytics tool would otherwise run for you.
CREATE TABLE access_log_daily (
  day      INTEGER NOT NULL,   -- floor(ts / 86400)
  event    TEXT NOT NULL,
  grant_id TEXT,
  path     TEXT,
  country  TEXT,
  events   INTEGER NOT NULL,   -- rows collapsed into this bucket
  clients  INTEGER NOT NULL,   -- distinct ip_hash values within it
  PRIMARY KEY (day, event, grant_id, path, country)
);

CREATE INDEX access_log_daily_day ON access_log_daily (day DESC);
CREATE INDEX access_log_daily_grant ON access_log_daily (grant_id, day DESC);
