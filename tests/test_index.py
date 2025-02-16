import asyncio
import json
from asyncio import gather
from dataclasses import dataclass, field
from datetime import datetime, timezone
from os.path import dirname, join
from sys import stdout
from typing import Literal

from utz import err

from disk_tree.json import Encoder

utc = timezone.utc

from aiopath import AsyncPath
from stdlb import fromtimestamp

TESTS = dirname(__file__)
REPO = dirname(TESTS)


Kind = Literal['file', 'dir']


@dataclass
class Entry:
    path: str
    mtime: datetime
    size: int
    parent: str
    kind: Kind
    num_descendants: int
    children: list["Entry"] = field(default_factory=list)


async def expand(path: str | AsyncPath) -> Entry | None:
    if isinstance(path, str):
        path = AsyncPath(path)
    stat = await path.stat()
    mtime = fromtimestamp(stat.st_mtime, tz=utc)
    if await path.is_dir():
        paths = [ child async for child in path.iterdir() ]
        entries = await gather(*[ expand(child) for child in paths ], return_exceptions=True)
        num_descendants = 1
        size = 0
        children = []
        for entry in entries:
            children.append(entry)
            num_descendants += entry.num_descendants
            size += entry.size
        entry = Entry(
            path=str(path),
            mtime=mtime,
            size=size,
            parent=str(path.parent),
            kind='dir',
            num_descendants=num_descendants,
            children=children,
        )
        return entry
    elif await path.is_file():
        entry = Entry(
            path=str(path),
            mtime=mtime,
            size=stat.st_size,
            parent=str(path.parent),
            kind='file',
            num_descendants=1,
        )
        return entry
    else:
        err("Skipping non-{file,dir}: %s" % path)
        return None


def test_index():
    cur = join(REPO, 'disk-tree')
    # cur = REPO
    e = asyncio.run(expand(cur))
    assert (e.size, e.num_descendants) == (1_715_141, 166)
    # print()
    # json.dump(e, stdout, indent=2, cls=Encoder)
