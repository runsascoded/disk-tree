from asyncio import Semaphore, Task, create_task
from typing import TypeVar, Coroutine, Any

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


