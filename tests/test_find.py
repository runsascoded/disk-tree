from io import StringIO
from os import environ
from os.path import join, dirname
from unittest.mock import patch, MagicMock

import pandas as pd
from pandas._testing import assert_frame_equal
from utz import err

from disk_tree import find

TESTS = dirname(__file__)
TESTDATA = join(TESTS, 'data')


def check(df: pd.DataFrame, name: str):
    pqt_path = join(TESTDATA, f'{name}.parquet')
    if environ.get('DISK_TREE_TEST_WRITE_EXPECTED'):  # or True:
        err(f"Writing expected output: {pqt_path}")
        df.to_parquet(pqt_path, index=False)
        df.to_csv(join(TESTDATA, f'{name}.csv'), index=False)
    df0 = pd.read_parquet(pqt_path)
    assert_frame_equal(df, df0)


def _mock_aws_ls(mock_popen, entries: list[tuple[int, int, str]]):
    """Wire `mock_popen` to emit `aws s3 ls --recursive` lines for `entries` (size, mtime, key)."""
    from datetime import datetime, timezone
    lines = ''.join(
        f"{datetime.fromtimestamp(m, timezone.utc):%Y-%m-%d %H:%M:%S} {s:>10} {k}\n"
        for s, m, k in entries
    )
    mock_proc = MagicMock()
    mock_proc.stdout = StringIO(lines)
    mock_popen.return_value = mock_proc


# (bytes, mtime, key): one zero-byte object (contributes to neither wsum nor
# size), so `b` has no weight at all.
MM_ENTRIES = [
    (1024, 1_000_000_000, 'root/a/x.bin'),
    (3072, 2_000_000_000, 'root/a/y.bin'),
    (0, 1_500_000_000, 'root/b/zero.bin'),
]


def _nan_none(x):
    return None if pd.isna(x) else x


@patch('subprocess.Popen')
def test_index_mean_mtime(mock_popen):
    """`mean_mtime=True` on a bucket listing: every object contributes size·mtime
    (dir rows are size 0, so they weigh nothing).

    Hand-computed means:
    - a: (1024·1e9 + 3072·2e9) / 4096 = 1_750_000_000
    - b: no bytes below it → NULL (not a 1970 epoch mean)
    - root: the same as `a` (b adds nothing to either term)
    """
    _mock_aws_ls(mock_popen, MM_ENTRIES)
    result = find.index('s3://bkt/root', mean_mtime=True)
    df = result.df
    assert 'mt_wsum' not in df.columns
    assert list(zip(df['path'], df['kind'], df['size'], map(_nan_none, df['mtime_mean']))) == [
        ('.', 'dir', 4096, 1_750_000_000.0),
        ('a', 'dir', 4096, 1_750_000_000.0),
        ('b', 'dir', 0, None),
        ('a/x.bin', 'file', 1024, 1_000_000_000.0),
        ('a/y.bin', 'file', 3072, 2_000_000_000.0),
        ('b/zero.bin', 'file', 0, 1_500_000_000.0),
    ]


@patch('subprocess.Popen')
def test_index_mean_mtime_off_is_unchanged(mock_popen):
    """Without the flag the frame is byte-identical to the flagged frame minus `mtime_mean`."""
    _mock_aws_ls(mock_popen, MM_ENTRIES)
    plain = find.index('s3://bkt/root').df
    assert 'mtime_mean' not in plain.columns
    _mock_aws_ls(mock_popen, MM_ENTRIES)
    flagged = find.index('s3://bkt/root', mean_mtime=True).df
    assert_frame_equal(plain, flagged.drop(columns=['mtime_mean']))


@patch('subprocess.Popen')
def test_index_mean_mtime_empty(mock_popen):
    """An empty listing still carries a NULL `mtime_mean` column when requested."""
    _mock_aws_ls(mock_popen, [])
    df = find.index('s3://bkt/root', mean_mtime=True).df
    assert df['mtime_mean'].isna().tolist() == [True]
    assert df['mtime_mean'].dtype == 'float64'


@patch('subprocess.Popen')
def test_s3_index(mock_popen):
    """Test S3 indexing with aws s3 ls output."""
    with open(join(TESTDATA, 's3.txt'), 'r') as f:
        find_txt = f.read()
    mock_proc = MagicMock()
    # S3 still uses line-by-line iteration (text mode)
    mock_proc.stdout = StringIO(find_txt)
    mock_popen.return_value = mock_proc
    test_path = 's3://runsascoded/gopro'
    result = find.index(test_path)
    check(result.df, 's3')
