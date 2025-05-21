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


class MockProc:
    def lines(self, *args):
        pass


def check(df: pd.DataFrame, name: str):
    pqt_path = join(TESTDATA, f'{name}.parquet')
    if environ.get('DISK_TREE_TEST_WRITE_EXPECTED'): # or True:
        err(f"Writing expected output: {pqt_path}")
        df.to_parquet(pqt_path, index=False)
        df.to_csv(join(TESTDATA, f'{name}.csv'), index=False)
    df0 = pd.read_parquet(pqt_path)
    assert_frame_equal(df, df0)


@patch('subprocess.Popen')
def test_index(mock_popen):
    with open(join(TESTDATA, 's8g.txt'), 'r') as f:
        find_txt = f.read()
    mock_proc = MagicMock()
    mock_proc.stdout = iter(find_txt.splitlines())
    mock_popen.return_value = mock_proc
    test_path = '/Volumes/s8/gopro'
    df = find.index(test_path)
    check(df, 's8g')


@patch('subprocess.Popen')
def test_s3_index(mock_popen):
    with open(join(TESTDATA, 's3.txt'), 'r') as f:
        find_txt = f.read()
    mock_proc = MagicMock()
    mock_proc.stdout = iter(find_txt.splitlines())
    mock_popen.return_value = mock_proc
    test_path = 's3://runsascoded/gopro'
    df = find.index(test_path)
    check(df, 's3')
