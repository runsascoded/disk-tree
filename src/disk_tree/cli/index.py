from contextlib import nullcontext
from functools import wraps
from os import getcwd

from click import argument, option
from disk_tree.model import init_db
from humanize import naturalsize
from tortoise import run_async
from utz import err, Time
from utz.mem import Tracker

from disk_tree.cli.base import cli
from disk_tree.index import expand, TaskPool


def tortoise_main(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        run_async(fn(*args, **kwargs))
    return wrapper


@cli.command('index')
@option('-m', '--measure-memory', is_flag=True)
@option('-n', '--n-workers', type=int, default=20)
@argument('url', required=False)
@tortoise_main
async def index(
    measure_memory: bool,
    n_workers: int,
    url: str | None,
):
    """Index a directory, in memory."""
    url = url or getcwd()
    if measure_memory:
        mem = Tracker()
        ctx = mem
    else:
        mem = None
        ctx = nullcontext()

    await init_db()
    time = Time()
    pool = TaskPool(n_workers)
    with ctx, time("expand"):
        res = await expand(url, pool)
    assert not pool.tasks

    elapsed = time['expand']
    num_desc = res.num_descendants
    speed = num_desc / elapsed

    if mem:
        peak_mem = mem.peak_mem
        err(f"Peak memory use: {peak_mem:,} ({naturalsize(peak_mem, binary=True, format='%.3g')})")

    print("yay")
    print(f"{num_desc:,} descendents ({elapsed:.3g}s, {round(speed):,d}/s), {naturalsize(res.size, binary=True, format='%.3g')}")
