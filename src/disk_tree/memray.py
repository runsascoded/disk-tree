import json
from os import getcwd, remove
from os.path import splitext, exists
from tempfile import NamedTemporaryFile
from types import TracebackType

from memray import Tracker
from utz import proc, err


class MemTracker:
    def __init__(
        self,
        path: str | None = None,
        keep: bool | None = None,
        native_traces: bool = True,
        **kwargs,
    ):
        self.path = path
        self.keep = keep
        self.tmpfile = False
        self.kwargs = dict(native_traces=native_traces, **kwargs)
        self.peak_mem = None
        self.stats = None
        self.tracker = None

    def __enter__(self):
        self.peak_mem = self.stats = None
        assert not self.tracker, f"Attempted to `__enter__` MemTracker before `__exit__`ing"
        path = self.path
        if path is None:
            path = self.path = NamedTemporaryFile(dir=getcwd(), suffix=".bin").name
            self.tmpfile = True
        elif exists(path):
            err(f"MemTracker removing {path}")
            remove(path)

        err(f"memray logging to {path}")
        self.tracker = Tracker(path, **self.kwargs)
        self.tracker.__enter__()
        return self

    @property
    def stats_path(self) -> str | None:
        path = self.path
        if path is None:
            return path
        else:
            return f'{splitext(self.path)[0]}.stats.json'

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_tb: TracebackType | None,
    ):
        self.tracker.__exit__(exc_type, exc_value, exc_tb)
        self.tracker = None
        rm = self.keep is False or (self.keep is None and self.tmpfile)
        stats_path = self.stats_path
        if not exc_value:
            proc.run('memray', 'stats', '--json', '-fo', stats_path, self.path)
        if rm:
            remove(self.path)
            self.path = None
        if exc_value:
            raise exc_value
        with open(stats_path, 'r') as f:
            stats = json.load(f)
        if rm:
            remove(stats_path)
        self.stats = stats
        self.peak_mem = stats['metadata']['peak_memory']
