from asyncio import gather
from dataclasses import dataclass
from datetime import datetime, timezone
from os import stat_result
from stat import S_ISLNK, S_ISREG, S_ISDIR
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
    stat = await path._accessor.stat(path, follow_symlinks=False)
    mode = stat.st_mode
    mtime = fromtimestamp(stat.st_mtime, tz=utc)
    if S_ISDIR(mode):
        num_descendants = 1
        size = 0
        _children = [] if children else None
        async for child in path.iterdir():
            entry = await expand(child)
            if isinstance(entry, BaseException):
                err(f"{entry}")
                continue
            elif not entry:
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
    elif S_ISREG(mode):
        entry = Entry(
            path=str(path),
            mtime=mtime,
            size=stat.st_size,
            parent=str(path.parent),
            kind='file',
            num_descendants=1,
        )
        return entry
    elif S_ISLNK(mode):
        # err("Skipping symlink: %s" % path)
        return None
    else:
        # err("Skipping: %s" % path)
        return None
