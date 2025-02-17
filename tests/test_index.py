import asyncio
from os.path import dirname, join

from disk_tree.index import expand


TESTS = dirname(__file__)
REPO = dirname(TESTS)


def test_index():
    cur = join(REPO, 'disk-tree')
    # cur = REPO
    e = asyncio.run(expand(cur))
    assert (e.size, e.num_descendants) == (1_715_141, 166)
    # print()
    # json.dump(e, stdout, indent=2, cls=Encoder)
