import asyncio
from os import getcwd

from click import argument
from humanize import naturalsize

from disk_tree.cli.base import cli
from disk_tree.index import expand


@cli.command('index')
@argument('url', required=False)
def index(url: str | None):
    """Index a directory, in memory."""
    url = url or getcwd()
    res = asyncio.run(expand(url))
    print(f"{res.num_descendants} descendents, {naturalsize(res.size, binary=True, format='%.3g')}")


