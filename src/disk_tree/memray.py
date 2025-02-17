import json
from os import getcwd, remove
from os.path import splitext, exists
from tempfile import NamedTemporaryFile

from humanize import naturalsize
from memray import Tracker
from utz import proc, err


class MemTracker:
    def __init__(self, path: str | None= None, **kwargs):
        self.path = path
        self.tmpfile = path is None
        self.kwargs = kwargs
        self.peak_mem = None

    def __enter__(self):
        self.peak_mem = None
        path = self.path
        if path is None:
            path = self.path = NamedTemporaryFile(dir=getcwd())
        elif exists(path):
            err(f"MemTracker removing {path}")
            remove(path)

        yield Tracker(path, **self.kwargs)

    def __exit__(self):
        stats_path = f'{splitext(self.path)[0]}.stats.json'
        proc.run('memray', 'stats', '--json', '-fo', stats_path, self.path)
        if self.tmpfile:
            remove(self.path)
        with open(stats_path, 'r') as f:
            stats = json.load(f)
        peak_mem = self.peak_mem = stats['metadata']['peak_memory']
        err(f"Peak memory use: {peak_mem} ({naturalsize(peak_mem, binary=True, format="%.3g")})")
