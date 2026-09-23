-- The multi-scan routing manifest (pyrmts Phase 2c): one row per sealed capped-K
-- over-time group, mapping a group's archive key → its member scans, so the
-- reader routes a scan/line to the group(s) holding it (specs/obs-axis-indexing.md).
-- Byte-for-byte the `pyrmts_engine.multiscan_index.multiscan_d1_ddl` /
-- `pyrmts-cfw` `multiScanDdl()` shape — cw owns the D1 instance + write (via
-- `index_footer.sync_d1`'s CF-D1 path, replaying the producer's JSONL manifest);
-- pyrmts owns the schema + the `MultiScanD1Index` read query.
CREATE TABLE IF NOT EXISTS "pyramid_multiscans" (
  dataset TEXT NOT NULL,
  tier TEXT NOT NULL,
  shard_dur TEXT NOT NULL,
  period_start INTEGER NOT NULL,
  period_end INTEGER NOT NULL,
  key TEXT NOT NULL,
  scans TEXT NOT NULL,
  encoder TEXT NOT NULL,
  digests TEXT,
  written_at INTEGER NOT NULL,
  PRIMARY KEY (dataset, key)
);
