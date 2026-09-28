-- The keep/sweep mark system is retired (specs/marks-demolition.md): the
-- opt-in staged-deletion model (`plans` / `plan_items` / `stage_batches`,
-- 0024–0026) is the only delete path, and the actions ledger keeps its owner
-- axis only. Composes after 0027 (which rebuilt the pre-ledger `marks` with a
-- keep/delete CHECK and pruned `keep_prefixes`): every mark table goes, the
-- keep-only actions go, and `actions` is rebuilt without its keep columns.
--
-- BEFORE APPLYING ON PROD (Ryan gates the apply): the rows are exported —
--   npx wrangler d1 export oa-gcs-usage-db --remote --table actions --table keep_prefixes \
--     --table mark_totals --table sweep_approvals --table marks --table mark_log \
--     --output tmp/marks-backup-2026-09-28.sql
-- and the file uploaded to the store's bucket under `backups/`
-- (gs://oa-gcs-usage-dvx/backups/marks-backup-2026-09-28.sql.gz is the one
-- taken 2026-09-28). The `d1_migrations` rows of earlier migrations stay as
-- history.
--
-- `owner_prefixes` references `actions (id)`: the drop / rename below runs
-- inside the migration's transaction, so the constraint is checked once
-- `actions` exists again (with the same ids) rather than at the DROP.
PRAGMA defer_foreign_keys = true;

DROP TABLE IF EXISTS marks;
DROP TABLE IF EXISTS mark_log;
DROP TABLE IF EXISTS marks_new;
DROP TABLE IF EXISTS keep_prefixes;
DROP TABLE IF EXISTS mark_totals;
DROP TABLE IF EXISTS sweep_approvals;

-- Actions that only set a keep decided nothing that survives; an action
-- that also set an owner keeps its owner half (and its provenance row).
DELETE FROM actions WHERE set_keep = 1 AND set_owner = 0;

-- SQLite can't drop a column that a CHECK names: create-copy-drop-rename.
CREATE TABLE actions_new (
  id         INTEGER PRIMARY KEY,
  actor      TEXT NOT NULL,             -- email of the acting identity
  ts         INTEGER NOT NULL,          -- unix seconds, server-assigned
  scan       TEXT NOT NULL,             -- scan id the actor was viewing
  pattern    TEXT NOT NULL,             -- prefix (regex patterns arrive later)
  set_owner  INTEGER NOT NULL DEFAULT 1,
  owner      TEXT,                      -- user id; NULL = clear
  memo       TEXT,
  CHECK (set_owner)
);
INSERT INTO actions_new (id, actor, ts, scan, pattern, set_owner, owner, memo)
  SELECT id, actor, ts, scan, pattern, set_owner, owner, memo FROM actions;
DROP TABLE actions;
ALTER TABLE actions_new RENAME TO actions;
CREATE INDEX idx_actions_actor ON actions (actor, ts);

-- Per-user owned bytes per (scan, ledger head): the claims fold priced
-- against the floor-free path index (`_lib/ownerTotals.ts`), cached here so
-- the index reads happen once per ledger change, not per request — what
-- `mark_totals` was, owner axis only. `claims` is the per-claim slice a user
-- lens reads per scan. Rows for old heads are dead weight; the writer prunes
-- anything but the newest head.
CREATE TABLE owner_totals (
  scan        TEXT NOT NULL,             -- snapshot date the totals are priced on
  head        INTEGER NOT NULL,          -- max(actions.id) the fold included
  body        TEXT NOT NULL,             -- JSON response body
  claims      TEXT NOT NULL,             -- JSON: the body's `claims` alone
  computed_ts INTEGER NOT NULL,
  ms          INTEGER,                   -- compute wall time
  PRIMARY KEY (scan, head)
);
