from os.path import join, dirname
from unittest.mock import patch

from disk_tree import find

TESTS = dirname(__file__)
TESTDATA = join(TESTS, 'data')


class MockProc:
    def lines(self, *args):
        pass


@patch('utz.proc')
def test_index(mock_proc):
    with open(join(TESTDATA, 's8g.txt'), 'r') as f:
        find_txt = f.read()
    mock_proc.lines.return_value = find_txt
    test_path = '/Volumes/s8/gopro'
    df = find.index(test_path)
    df.to_csv(join(TESTDATA, 's8g.csv'), index=False)
    df.to_parquet(join(TESTDATA, 's8g.parquet'), index=False)
    assert len(df) == 875
