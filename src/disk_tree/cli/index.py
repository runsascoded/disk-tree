from contextlib import nullcontext
from os import getcwd

from click import argument, option
from disk_tree.cli.base import cli
from disk_tree.sqla.db import init
from humanize import naturalsize
from utz import err, Time
from utz.mem import Tracker


@cli.command('index')
@option('-C', '--no-cache-read', is_flag=True)
@option('-m', '--measure-memory', is_flag=True)
@argument('url', required=False)
def index(
    no_cache_read: bool,
    measure_memory: bool,
    url: str | None,
):
    """Index a directory, in memory."""
    db = init()
    from disk_tree.sqla.model import Scan
    db.create_all()
    url = url or getcwd()
    url = url.rstrip('/')
    if measure_memory:
        mem = Tracker()
        ctx = mem
    else:
        mem = None
        ctx = nullcontext()

    time = Time()
    with ctx, time("expand"):
        if no_cache_read:
            df = Scan.create(url)
        else:
            df = Scan.load_or_create(url)
        # df = find.index(url)

    elapsed = time['expand']
    res = df.set_index('path').loc[url]
    n_desc = res.n_desc
    size = res['size']
    speed = n_desc / elapsed

    if mem:
        peak_mem = mem.peak_mem
        err(f"Peak memory use: {peak_mem:,} ({naturalsize(peak_mem, binary=True, format='%.3g')})")

    print(f"{n_desc:,} descendents ({elapsed:.3g}s, {round(speed):,d}/s), {naturalsize(size, binary=True, format='%.3g')}")
