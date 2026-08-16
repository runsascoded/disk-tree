from io import BytesIO, StringIO
from os import environ
from os.path import join, dirname
import subprocess
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


@patch('subprocess.Popen')
def test_index(mock_popen):
    """Test local filesystem indexing with gfind output."""
    with open(join(TESTDATA, 's8g.txt'), 'r') as f:
        find_txt = f.read()
    # Convert newline-separated text to null-terminated bytes (gfind -printf uses \0)
    null_terminated = find_txt.replace('\n', '\0').encode('utf-8')
    mock_proc = MagicMock()
    mock_proc.stdout = BytesIO(null_terminated)
    mock_proc.stderr = BytesIO(b'')  # No errors
    mock_proc.wait.return_value = 0
    mock_popen.return_value = mock_proc
    test_path = '/Volumes/s8/gopro'
    result = find.index(test_path)
    check(result.df, 's8g')


def _mock_gfind(mock_popen, entries: list[tuple[str, int, int, str]]):
    """Wire `mock_popen` to emit gfind `%y %b %T@ %p\\0` records for `entries` (kind, blocks, mtime, path)."""
    raw = ''.join(f'{k} {b} {m}.0000000000 {p}\0' for k, b, m, p in entries).encode('utf-8')
    mock_proc = MagicMock()
    mock_proc.stdout = BytesIO(raw)
    mock_proc.stderr = BytesIO(b'')
    mock_proc.wait.return_value = 0
    mock_popen.return_value = mock_proc


# (kind, 512B blocks, mtime, path): dirs carry their own 4096B inode blocks,
# one zero-byte file (contributes to neither wsum nor size).
MM_ENTRIES = [
    ('d', 8, 1_000, '/root'),
    ('d', 8, 2_000, '/root/a'),
    ('f', 2, 1_000_000_000, '/root/a/x.bin'),
    ('f', 6, 2_000_000_000, '/root/a/y.bin'),
    ('d', 8, 500, '/root/b'),
    ('f', 0, 1_500_000_000, '/root/b/zero.bin'),
]


@patch('subprocess.Popen')
def test_index_mean_mtime(mock_popen):
    """`mean_mtime=True` on the local walk: every inode contributes size·mtime.

    Hand-computed means:
    - a: (4096·2000 + 1024·1e9 + 3072·2e9) / 8192 = 875_001_000
    - b: (4096·500 + 0·1.5e9) / 4096 = 500 (own inode only — an empty dir
      keeps its own mtime rather than collapsing to a 1970 epoch mean)
    - root: (4096·1000 + wsum_a + wsum_b) / 16384 = 437_500_875
    """
    _mock_gfind(mock_popen, MM_ENTRIES)
    result = find.index('/root', mean_mtime=True)
    df = result.df
    assert 'mt_wsum' not in df.columns
    assert list(zip(df['path'], df['kind'], df['size'], df['mtime_mean'])) == [
        ('.', 'dir', 16384, 437_500_875.0),
        ('a', 'dir', 8192, 875_001_000.0),
        ('b', 'dir', 4096, 500.0),
        ('a/x.bin', 'file', 1024, 1_000_000_000.0),
        ('a/y.bin', 'file', 3072, 2_000_000_000.0),
        ('b/zero.bin', 'file', 0, 1_500_000_000.0),
    ]


@patch('subprocess.Popen')
def test_index_mean_mtime_off_is_unchanged(mock_popen):
    """Without the flag the frame is byte-identical to the flagged frame minus `mtime_mean`."""
    _mock_gfind(mock_popen, MM_ENTRIES)
    plain = find.index('/root').df
    assert 'mtime_mean' not in plain.columns
    _mock_gfind(mock_popen, MM_ENTRIES)
    flagged = find.index('/root', mean_mtime=True).df
    assert_frame_equal(plain, flagged.drop(columns=['mtime_mean']))


@patch('subprocess.Popen')
def test_index_mean_mtime_empty(mock_popen):
    """Empty walk still carries a NULL `mtime_mean` column when requested."""
    _mock_gfind(mock_popen, [])
    df = find.index('/root', mean_mtime=True).df
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
