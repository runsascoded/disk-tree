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
