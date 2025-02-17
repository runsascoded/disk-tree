import asyncio
from contextlib import nullcontext
from os import getcwd

from click import argument, option
from humanize import naturalsize
from utz import err

from disk_tree.cli.base import cli
from disk_tree.index import expand
from disk_tree.memray import MemTracker
from disk_tree.time import Time


@cli.command('index')
@option('-m', '--measure-memory', is_flag=True)
@argument('url', required=False)
def index(
    measure_memory: bool,
    url: str | None,
):
    """Index a directory, in memory."""
    url = url or getcwd()
    if measure_memory:
        memray = MemTracker()
        ctx = memray
    else:
        memray = None
        ctx = nullcontext()

    time = Time()
    with ctx, time("expand"):
        res = asyncio.run(expand(url))

    elapsed = time['expand']
    num_desc = res.num_descendants
    speed = num_desc / elapsed

    if memray:
        peak_mem = memray.peak_mem
        err(f"Peak memory use: {peak_mem:,} ({naturalsize(peak_mem, binary=True, format='%.3g')})")

    print(f"{num_desc:,} descendents ({elapsed:.3g}s, {round(speed):,d}/s), {naturalsize(res.size, binary=True, format='%.3g')}")
