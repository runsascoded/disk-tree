from time import perf_counter
from types import TracebackType
from typing import Self


class Time:
    def __init__(self):
        self.times = {}
        self.cur_timer = None
        self.cur_start = 0

    def __call__(self, name: str | None = None) -> Self:
        now = perf_counter()
        if self.cur_timer:
            self.times[self.cur_timer] = now - self.cur_start
        if name:
            self.cur_timer = name
            self.cur_start = perf_counter()
        else:
            self.cur_timer = None
            self.cur_start = 0
        return self

    def __enter__(self):
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_tb: TracebackType | None,
    ):
        self()
        if exc_value:
            raise exc_value

    def __getitem__(self, name: str) -> float:
        return self.times[name]
