from asyncio import gather
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal

from utz import err

utc = timezone.utc

from aiopath import AsyncPath
from stdlb import fromtimestamp

Kind = Literal['file', 'dir']


@dataclass
class Entry:
    path: str
    mtime: datetime
    size: int
    parent: str
    kind: Kind
    num_descendants: int
    children: list["Entry"] | None = None


async def expand(path: str | AsyncPath, children: bool = False) -> Entry | None:
    if isinstance(path, str):
        path = AsyncPath(path)
    stat = await path.stat()
    mtime = fromtimestamp(stat.st_mtime, tz=utc)
    if await path.is_dir():
        paths = [ child async for child in path.iterdir() ]
        entries = await gather(*[ expand(child) for child in paths ], return_exceptions=True)
        num_descendants = 1
        size = 0
        _children = [] if children else None
        for entry in entries:
            if isinstance(entry, BaseException):
                err(f"{entry}")
                continue
            if children:
                _children.append(entry)
            num_descendants += entry.num_descendants
            size += entry.size
        entry = Entry(
            path=str(path),
            mtime=mtime,
            size=size,
            parent=str(path.parent),
            kind='dir',
            num_descendants=num_descendants,
            **(dict(children=_children) if children else {}),
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
