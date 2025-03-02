from asyncio import Semaphore, create_task, Task
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import batched
from stat import S_ISLNK, S_ISREG, S_ISDIR
from typing import Literal, Coroutine, TypeVar, Any

from utz import err

utc = timezone.utc

from aiopath import AsyncPath
from stdlb import fromtimestamp

Kind = Literal['file', 'dir']
T = TypeVar("T")


class TaskPool:
    def __init__(self, max_workers: int):
        self.semaphore = Semaphore(max_workers)
        self.tasks = set()

    async def submit_task(self, coro: Coroutine[Any, Any, T]) -> Task[T]:
        await self.semaphore.acquire()
        task = create_task(coro)

        def done_callback(_):
            self.semaphore.release()
            self.tasks.remove(task)

        task.add_done_callback(done_callback)
        self.tasks.add(task)
        return task


@dataclass
class Entry:
    path: str
    mtime: datetime
    size: int
    parent: str
    kind: Kind
    num_descendants: int
    children: list["Entry"] | None = None


async def expand(path: str | AsyncPath, pool: TaskPool, children: bool = False, level: int = 0) -> Entry | None:
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
        for batch in batched(child_paths, batch_size):
            pending_tasks = []
            for child in batch:
                task = await pool.submit_task(expand(child, pool, children=children, level=level + 1))
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
        entry = Entry(
            path=str(path),
            mtime=mtime,
            size=size,
            parent=str(path.parent),
            kind='dir',
            num_descendants=num_descendants,
            **(dict(children=_children) if children else {}),
        )
        # err(f"{'-' * (level + 1)} {path}")
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
        # err(f"{'-' * (level + 1)} {path}")
        return entry
    elif S_ISLNK(mode):
        # err("Skipping symlink: %s" % path)
        return None
    else:
        # err("Skipping: %s" % path)
        return None
