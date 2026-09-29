"""`path-index-zstd.parquet`: a real overlay path-index (`dt_cloud.index.write_index`,
zstd since spec `listing-slim.md`) over a 3-object listing, for `zstd.test.ts`.
Regenerate from `cloud/` with `uv run python ../site/functions/_lib/fixtures/gen.py`."""
import shutil
import tempfile
from datetime import datetime, timezone
from os.path import dirname, join

import duckdb
import pandas as pd

from disk_tree.find.aggregate_duckdb import aggregate_listing_to_parquet
from disk_tree.listing import prepare_listing
from dt_cloud.index import write_index

TS = datetime(2026, 9, 1, tzinfo=timezone.utc)


def main() -> None:
    here = dirname(__file__)
    with tempfile.TemporaryDirectory() as tmp:
        listing = join(tmp, 'listing.parquet')
        pd.DataFrame({
            'bucket': ['bk'] * 3,
            'name': ['a/x', 'a/y', 'b/z'],
            'size_bytes': [100, 200, 400],
            'created': [TS] * 3,
            'storage_class_id': [1] * 3,
        }).to_parquet(listing)
        con = duckdb.connect()
        l2 = join(tmp, 'l2.parquet')
        aggregate_listing_to_parquet(prepare_listing(con, (listing,)), bucket='bk', scheme='s3', out_parquet=l2, con=con, mean_mtime=True)
        write_index([('bk', l2)], join(tmp, 'out'), mem='1GB', threads=1)
        shutil.copy(join(tmp, 'out', 'path-index.parquet'), join(here, 'path-index-zstd.parquet'))


if __name__ == '__main__':
    main()
