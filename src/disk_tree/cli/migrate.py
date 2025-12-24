"""Migration commands for disk-tree database."""
from os.path import isfile

import pandas as pd
from click import command
from utz import err

from disk_tree.cli.base import cli
from disk_tree.config import SQLITE_PATH as DB_PATH


@cli.command('migrate')
def migrate():
    """Run database migrations (add columns, backfill stats)."""
    import sqlite3

    if not isfile(DB_PATH):
        err(f"Database not found: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Check existing columns
    cursor.execute("PRAGMA table_info(scan)")
    columns = {row['name'] for row in cursor.fetchall()}

    # Add new columns if they don't exist
    new_columns = [
        ('size', 'INTEGER'),
        ('n_children', 'INTEGER'),
        ('n_desc', 'INTEGER'),
    ]
    for col_name, col_type in new_columns:
        if col_name not in columns:
            err(f"Adding column: {col_name}")
            cursor.execute(f"ALTER TABLE scan ADD COLUMN {col_name} {col_type}")
            conn.commit()
        else:
            err(f"Column already exists: {col_name}")

    # Backfill stats from parquet files
    cursor.execute("SELECT id, path, blob, size FROM scan")
    rows = cursor.fetchall()
    err(f"Backfilling stats for {len(rows)} scans...")

    updated = 0
    skipped = 0
    errors = 0
    for row in rows:
        scan_id = row['id']
        blob_path = row['blob']
        existing_size = row['size']

        # Skip if already has size (already migrated)
        if existing_size is not None:
            skipped += 1
            continue

        if not isfile(blob_path):
            err(f"  Parquet not found: {blob_path}")
            errors += 1
            continue

        try:
            df = pd.read_parquet(blob_path)
            # Try 'parent == ""' first (local scans), fallback to 'path == "."' (S3 scans)
            root_rows = df[df['parent'] == '']
            if root_rows.empty:
                root_rows = df[df['path'] == '.']
            if root_rows.empty:
                err(f"  No root row found: {blob_path}")
                errors += 1
                continue

            root = root_rows.iloc[0]
            size = int(root['size']) if pd.notna(root['size']) else None
            n_children = int(root['n_children']) if pd.notna(root.get('n_children')) else None
            n_desc = int(root['n_desc']) if pd.notna(root.get('n_desc')) else None

            cursor.execute(
                "UPDATE scan SET size = ?, n_children = ?, n_desc = ? WHERE id = ?",
                (size, n_children, n_desc, scan_id),
            )
            conn.commit()
            updated += 1
        except Exception as e:
            err(f"  Error processing {blob_path}: {e}")
            errors += 1

    conn.close()
    err(f"Migration complete: {updated} updated, {skipped} skipped, {errors} errors")


@cli.command('migrate-depth')
def migrate_depth():
    """Add depth column to existing parquet files for predicate pushdown."""
    import sqlite3
    import pyarrow.parquet as pq

    if not isfile(DB_PATH):
        err(f"Database not found: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT id, path, blob FROM scan")
    rows = cursor.fetchall()
    err(f"Checking {len(rows)} parquet files for depth column...")

    updated = 0
    skipped = 0
    errors = 0
    for row in rows:
        blob_path = row['blob']

        if not isfile(blob_path):
            err(f"  Parquet not found: {blob_path}")
            errors += 1
            continue

        try:
            schema = pq.read_schema(blob_path)
            has_depth = 'depth' in schema.names

            df = pd.read_parquet(blob_path)

            # Add depth column if missing
            if not has_depth:
                df['depth'] = df['path'].apply(lambda p: 0 if p == '.' else p.count('/') + 1)

            # Always re-sort by depth for efficient parquet filtering (breadth-first order)
            df = df.sort_values(['depth', 'path']).reset_index(drop=True)
            df.to_parquet(blob_path)
            updated += 1
        except Exception as e:
            err(f"  Error processing {blob_path}: {e}")
            errors += 1

    conn.close()
    err(f"Depth migration complete: {updated} updated, {errors} errors")
