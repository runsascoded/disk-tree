-- Deletion history — executed sweeps as first-class records (specs/cw-sweep.md).
-- Marks are intent; these are fact. One `deletion_runs` row per executor
-- invocation; `deletion_bands` aggregates it per covering band prefix (the
-- queryable per-path unit — a path's deletion history = bands ancestor-or-equal
-- to it, plus bands under it for "deletions of descendants"). Object-level
-- detail stays in the run's log parquets (`log_dir`), which the /files browser
-- renders. Adapted from gcs migrations 0015 + 0023 (buckets column folded in),
-- plus a `plan_id` FK to the first-class plan (0002) the run executed.
--
-- CAIOS vs GCS soft-delete: on a versioned CoreWeave bucket a delete writes a
-- delete marker (prior version recoverable) but does NOT reclaim space until
-- the noncurrent versions are purged. So a real run is two-stage: `undo_state`
-- tracks reversibility (remove delete markers) before `undo_deadline`, and
-- `purge_state` tracks whether the noncurrent versions have been GC'd (space
-- actually freed) after the hold — see specs/cw-sweep.md Phase 3.

CREATE TABLE deletion_runs (
  run_id TEXT PRIMARY KEY,           -- <plan>-<scan>/<utc compact ts>
  plan_id INTEGER NOT NULL REFERENCES plans(id),  -- the first-class plan executed
  manifest TEXT NOT NULL,            -- gs:// dir the object-level manifest was written to
  scan TEXT NOT NULL,
  head INTEGER NOT NULL,             -- mark_log head at pin (drift detection)
  exec_head INTEGER NOT NULL,        -- mark_log head at execution (re-verified)
  actor TEXT NOT NULL,               -- CF Access identity that dispatched
  mode TEXT NOT NULL CHECK (mode IN ('dry', 'real')),
  started_ts INTEGER NOT NULL,
  finished_ts INTEGER,
  deleted_bytes INTEGER NOT NULL DEFAULT 0,   -- would-delete bytes for dry runs
  deleted_objects INTEGER NOT NULL DEFAULT 0,
  skipped_gone INTEGER NOT NULL DEFAULT 0,     -- key already absent
  skipped_overwritten INTEGER NOT NULL DEFAULT 0, -- versionId/ETag drifted since scan
  drift_dirs INTEGER NOT NULL DEFAULT 0,      -- dirs skipped: new keys since the scan
  ledger_drift_dirs INTEGER NOT NULL DEFAULT 0, -- dirs dropped: newer marks
  buckets TEXT,                      -- bucket cut dispatched with (CSV); NULL = whole plan
  undo_deadline INTEGER,             -- finished_ts + hold window (real runs); undo before this
  undo_state TEXT NOT NULL DEFAULT 'none' CHECK (undo_state IN ('none', 'partial', 'full', 'expired')),
  purge_state TEXT NOT NULL DEFAULT 'none' CHECK (purge_state IN ('none', 'pending', 'done')),
  log_dir TEXT NOT NULL              -- gs:// dir holding {would-delete,deleted}/ parquets + summary
);

CREATE TABLE deletion_bands (
  run_id TEXT NOT NULL REFERENCES deletion_runs(run_id),
  prefix TEXT NOT NULL,              -- s3://<bucket>/<dir>/ band the deletions fell under
  bytes INTEGER NOT NULL,
  objects INTEGER NOT NULL,
  gone INTEGER NOT NULL DEFAULT 0,
  overwritten INTEGER NOT NULL DEFAULT 0,
  drift_new_objects INTEGER NOT NULL DEFAULT 0,
  undone_objects INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (run_id, prefix)
);

-- Per-path lookups: covering bands via the ancestor IN-list, descendant
-- listings via the prefix range scan — both want this index.
CREATE INDEX idx_deletion_bands_prefix ON deletion_bands (prefix);
