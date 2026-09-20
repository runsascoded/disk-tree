-- The index tiers' parquet footers, decomposed into D1 so the Cloudflare
-- reader (`functions/_lib/index.ts`) builds a subset FileMetaData for a
-- prefix query without parsing a footer on a cold isolate
-- (specs/path-agnostic-serving.md §2.1, specs/view-serving.md). This is
-- gcs's 0013 + 0014 + 0018 + 0020 folded into their end state — one shape
-- shared with gcs so the reader is byte-identical. cw has no user-sorted
-- variants, so `u_min`/`u_max` stay NULL here; the columns exist so the
-- reader's span predicate needs no branch.
--
-- `index_schema` is the per-(date, variant) POINTER: the generation `gen`
-- and bucket-relative `dir` of the file set a run published, plus the
-- schema and a coarse tier's floor. `index_row_groups` holds every group's
-- pruning stats + compact (~250 B) metadata, keyed by that gen, so two
-- generations coexist while a run lands and the pointer flips last.
-- Populated per scan by `dt-cloud index-sync`; `index-gc` sweeps the rest.
CREATE TABLE index_schema (
  date        TEXT NOT NULL,            -- scan id (SNAP_ID, e.g. 2026-09-16T0001)
  variant     TEXT NOT NULL DEFAULT 'path',  -- 'path' (floor-free) or 'coarse<E>'
  version     INTEGER NOT NULL,         -- parquet FileMetaData.version
  schema_json TEXT NOT NULL,            -- hyparquet SchemaElement[] (root + leaves)
  floor_bytes INTEGER,                  -- a coarse tier's absolute byte floor F; NULL = floor-free
  gen         TEXT,                     -- generation stamp of the synced file set
  dir         TEXT,                     -- bucket-relative dir holding this variant's parquet
  PRIMARY KEY (date, variant)
);

CREATE TABLE index_row_groups (
  date      TEXT NOT NULL,
  variant   TEXT NOT NULL,
  gen       TEXT NOT NULL,
  rg        INTEGER NOT NULL,   -- row-group ordinal
  d_min     INTEGER NOT NULL,   -- depth stats (row-group pruning)
  d_max     INTEGER NOT NULL,
  p_min     TEXT NOT NULL,      -- path stats
  p_max     TEXT NOT NULL,
  b_max     INTEGER NOT NULL,   -- max descendant-inclusive bytes (threshold prune)
  u_min     TEXT,               -- usr range (a user-sorted variant's primary key; NULL here)
  u_max     TEXT,
  row_start INTEGER NOT NULL,   -- absolute row range of this group
  row_end   INTEGER NOT NULL,
  rg_json   TEXT NOT NULL,      -- compact [num_rows, codec, [[off, size, dict_off], …]]
  PRIMARY KEY (date, variant, gen, rg)
);
CREATE INDEX idx_index_row_groups_depth ON index_row_groups (date, variant, gen, d_min, d_max);
CREATE INDEX idx_index_row_groups_user  ON index_row_groups (date, variant, gen, u_min, u_max);
