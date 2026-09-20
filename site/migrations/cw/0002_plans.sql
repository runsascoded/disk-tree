-- Plans (specs/cw-sweep.md) — a deletion plan as a first-class object.
--
-- Flow: mark (0001, raw signal, anyone) → plan (admin curates sweep-marked
-- prefixes into a named plan) → dispatch (a deletion_run against the plan;
-- 0003) → runs (many per plan). The plan is the curation step that replaces
-- gcs's owner==marker attribution slice: instead of auto-filtering marks to the
-- marker's own owned dirs, an admin explicitly assembles the prefixes to delete.
--
-- Multiple plans coexist independently (draft plan A = old checkpoints, plan B =
-- tmp/, each with its own dry-run → real-run lifecycle). A plan's item set is
-- the source of truth for a dispatch; the run's gs:// manifest snapshots the
-- object-level expansion at execution time (deletion_runs.manifest).

CREATE TABLE plans (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  name       TEXT NOT NULL,
  note       TEXT,
  state      TEXT NOT NULL DEFAULT 'open' CHECK (state IN ('open', 'closed')),
  created_by TEXT NOT NULL,            -- CF Access identity that drafted it
  created_ts INTEGER NOT NULL,         -- epoch seconds
  closed_ts  INTEGER                   -- set when state → closed
);

-- The prefixes a plan will sweep. Curated (added/removed) while the plan is
-- open; typically drawn from the current `sweep`-marked set but decoupled from
-- it once added (removing the underlying mark is surfaced as drift, not an
-- auto-removal). One prefix per (plan, prefix).
CREATE TABLE plan_items (
  plan_id  INTEGER NOT NULL REFERENCES plans(id),
  prefix   TEXT NOT NULL,             -- s3://<bucket>/<path>/ (trailing slash)
  note     TEXT,
  added_by TEXT NOT NULL,             -- CF Access identity that added it
  added_ts INTEGER NOT NULL,
  PRIMARY KEY (plan_id, prefix)
);
