from datetime import timezone
from itertools import batched
from stat import S_ISLNK, S_ISREG, S_ISDIR
from typing import Literal

from utz import err

from .model import FileEntry
from .task_pool import TaskPool

utc = timezone.utc

from aiopath import AsyncPath
from stdlb import fromtimestamp

Kind = Literal['file', 'dir']


async def expand(
    path: str | AsyncPath,
    pool: TaskPool,
    children: bool = False,
    parent: FileEntry | None = None,
    level: int = 0,
) -> FileEntry | None:
    # err(f"{'+' * (level + 1)} {path}")
    if isinstance(path, str):
        path = AsyncPath(path)
    stat = await path.stat(follow_symlinks=False)
    mode = stat.st_mode
    mtime = fromtimestamp(stat.st_mtime, tz=utc)
    if S_ISDIR(mode):
        num_descendants = 1
        size = 0
        _children = [] if children else None

        batch_size = max(1, pool.semaphore._value // 2)
        child_paths = [ child async for child in path.iterdir() ]
        cur = FileEntry(
            path=str(path),
            mtime=mtime,
            size=size,
            parent=parent,
            kind='dir',
            num_descendants=num_descendants
        )
        cur0 = await FileEntry.get_or_none(path=str(path))
        if cur0:
            err(f"Updating {cur.path}")
            await cur0.delete()
        await cur.save()

        for batch in batched(child_paths, batch_size):
            pending_tasks = []
            for child in batch:
                task = await pool.submit_task(expand(child, pool, children=children, parent=cur, level=level + 1))
                pending_tasks.append(task)

            for task in pending_tasks:
                entry = await task
                if isinstance(entry, BaseException):
                    err(f"{entry}")
                    continue
                elif not entry:
                    continue
                if children:
                    _children.append(entry)
                num_descendants += entry.num_descendants
                size += entry.size

        cur.num_descendants = num_descendants
        cur.size = size
        await cur.save()
        # err(f"{'-' * (level + 1)} {path}")
        return cur
    elif S_ISREG(mode):
        entry = FileEntry(
            path=str(path),
            mtime=mtime,
            size=stat.st_size,
            parent=parent,
            kind='file',
            num_descendants=1,
        )
        await entry.save()
        # err(f"{'-' * (level + 1)} {path}")
        return entry
    elif S_ISLNK(mode):
        # err("Skipping symlink: %s" % path)
        return None
    else:
        # err("Skipping: %s" % path)
        return None
