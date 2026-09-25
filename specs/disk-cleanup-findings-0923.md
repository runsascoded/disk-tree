# Findings from the `~/.disk` cleanup session (2026-09-23)

Written by the `~/.disk` session while running a local-laptop browse + staged-delete flow off `cloud` @ `47eee40` (own `disk-tree-server` on :5791 with `DISK_TREE_DELETE_APPROVAL=staged`, own Vite on :7791 proxying to it).

## 1. Missing migration: `deletion_run.batch_job` (bug; `/api/staged` 500s on existing DBs)

`sqla/deletion.py` added `batch_job: Mapped[str | None]` to `DeletionRun`, but nothing adds the column to an existing `~/.config/disk-tree/disk-tree.db` (`create_all` doesn't `ALTER`, and `disk-tree migrate` only covers `scan`). Every `/api/staged` then fails:

```
sqlite3.OperationalError: no such column: deletion_run.batch_job
```

Diffing every `Base.metadata` table against the live DB found this as the only gap. I applied `ALTER TABLE deletion_run ADD COLUMN batch_job VARCHAR` by hand (backup at `~/.disk/tmp/disk-tree.db.bak-20260923`). **Fix**: a generic "add missing nullable columns" pass (models vs `PRAGMA table_info`) run by `migrate` and ideally on server/CLI startup, so the next added column doesn't repeat this.

## 2. `disk-tree repos` shells out to `disk-tree` by name

It fails with `FileNotFoundError: 'disk-tree'` whenever the CLI isn't on `PATH` (e.g. outside the repo's direnv venv). Invoke via `sys.executable -m …` / `sys.argv[0]` instead.

## 3. `index` peak RSS up ~65% since Sep 9

Full `/Users/ryan` scan (6.6M files, `-C -q -D --to r2://…`): peak RSS **7.1 GB** (footprint 10.6 GB), vs **4.3 GB** on Sep 9 (`5c32460`, same tree size, same flags minus `-D`). On this 98%-full laptop that goes straight to swap (the `VM` volume holds 31 GB). Worth a `-M`/`--measure-memory` bisect between `5c32460` and `47eee40`.

## 4. The bundled `src/disk_tree/static` UI is stale (Aug 30)

`STATIC_DIR` prefers the packaged `static/` over `ui/dist/` (Sep 20), so a bare `disk-tree-server` serves an Aug 30 UI that predates staged deletes. Either refresh `static/` on build, or prefer whichever is newer in a dev checkout.
