from os.path import join, dirname
import subprocess
from unittest.mock import patch, MagicMock

from disk_tree import find

TESTS = dirname(__file__)
TESTDATA = join(TESTS, 'data')


class MockProc:
    def lines(self, *args):
        pass


@patch('subprocess.Popen')
def test_index(mock_popen):
    with open(join(TESTDATA, 's8g.txt'), 'r') as f:
        find_txt = f.read()
    mock_proc = MagicMock()
    mock_proc.stdout = iter(find_txt.splitlines())
    mock_popen.return_value = mock_proc
    test_path = '/Volumes/s8/gopro'
    df = find.index(test_path)
    df.to_csv(join(TESTDATA, 's8g.csv'), index=False)
    df.to_parquet(join(TESTDATA, 's8g.parquet'), index=False)
    assert len(df) == 875


@patch('subprocess.Popen')
def test_s3_index(mock_popen):
    with open(join(TESTDATA, 's3.txt'), 'r') as f:
        find_txt = f.read()
    mock_proc = MagicMock()
    mock_proc.stdout = iter(find_txt.splitlines())
    mock_popen.return_value = mock_proc
    test_path = 's3://runsascoded/gopro'
    df = find.index(test_path)
    df.to_csv(join(TESTDATA, 's3.csv'), index=False)
    df.to_parquet(join(TESTDATA, 's3.parquet'), index=False)
    assert len(df) == 952
