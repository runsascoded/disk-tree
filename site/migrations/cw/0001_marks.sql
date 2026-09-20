-- Mark axis (specs/cw-sweep.md): human keep/sweep intent over CoreWeave S3
-- prefixes, ahead of a Batch sweep. Adapted from gcs's mark tables (gcs
-- migrations 0007 marks + 0010 actions-ledger), keep-axis only — cw-s3 has no
-- owner attribution, so the owner/claims axis is dropped.
--
-- Default state is **unmarked** — the absence of a row, neither swept nor
-- affirmatively kept. This is the init state, distinct from `keep`: `keep` /
-- `keep_last_ckpt` are affirmative human "protect this" signals (e.g. carving a
-- child out of a `sweep`ed parent), NOT the default. Nothing is ever swept
-- unless an explicit `sweep` mark applies (deepest-mark-wins resolution). This
-- differs from gcs, which treats absence-of-mark as delete-eligible and leans
-- on an owner==marker attribution slice to stay safe — cw-s3 has no attribution
-- gate, so the explicit-`sweep`-only rule + the curated plan (0002) are the
-- safety model instead. Marks are plain prefixes for now (regex patterns, and
-- the raw-action / expanded-prefix split gcs added for them, arrive later).

-- Current resolved mark per prefix — one read gives the live mark state.
CREATE TABLE marks (
  prefix TEXT PRIMARY KEY,              -- s3://<bucket>/<path>/ (trailing slash)
  keep   TEXT NOT NULL CHECK (keep IN ('keep', 'keep_last_ckpt', 'sweep')),
  who    TEXT NOT NULL,                 -- CF Access identity (email) that set it
  scan   TEXT NOT NULL,                 -- scan id the actor was viewing
  ts     INTEGER NOT NULL,              -- epoch seconds
  note   TEXT
);

-- Append-only history: every change, including un-marks (keep = NULL), so the
-- provenance of a sweep survives later edits.
CREATE TABLE mark_log (
  id     INTEGER PRIMARY KEY AUTOINCREMENT,
  prefix TEXT NOT NULL,
  keep   TEXT,                          -- NULL = mark removed (unmark)
  who    TEXT NOT NULL,
  scan   TEXT NOT NULL,
  ts     INTEGER NOT NULL,
  note   TEXT
);
CREATE INDEX mark_log_prefix ON mark_log (prefix, ts);
