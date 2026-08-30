"""`disk-tree overcount` — how much a subtree's apparent size overstates the
physical bytes it actually holds, because of APFS clones and hardlinks.

disk-tree's scan sizes (and `du`, and every GUI analyzer) sum per-path block
counts, so bytes shared between files are charged to each. This walks the tree
asking APFS for each file's *private* size (`ATTR_CMNEXT_PRIVATESIZE`, no open
needed) and reports apparent vs. exclusive per top-level child.

`shared = apparent − exclusive` is the overcount: bytes this subtree shares with
files elsewhere on the volume, or duplicates within itself. `exclusive` is what
deleting the whole subtree would actually free.
"""

from __future__ import annotations

import json
import os
import sys

from click import argument, option
from humanize import naturalsize

from disk_tree.cli.base import cli


def _fmt(n: int, human: bool) -> str:
    return naturalsize(n, binary=True, format='%.3g') if human else str(n)


@cli.command('overcount')
@option('-H', '--no-human', is_flag=True, help='Print raw bytes instead of human-readable sizes')
@option('-j', '--json', 'as_json', is_flag=True, help='Emit JSON')
@option('-n', '--top', default=20, help='Max children shown, biggest-apparent first (0 = all)')
@argument('uri')
def overcount_cmd(no_human: bool, as_json: bool, top: int, uri: str):
    """Apparent-vs-physical accounting for the subtree at URI (macOS only)."""
    from disk_tree.extents import SUPPORTED, measure_overcount

    if not SUPPORTED:
        raise SystemExit(f'ATTR_CMNEXT_PRIVATESIZE is Darwin-only (got {sys.platform})')
    root = os.path.abspath(os.path.expanduser(uri)).rstrip('/')
    if not os.path.isdir(root):
        raise SystemExit(f'not a directory: {root}')

    seen: set[int] = set()
    children = sorted(
        (e.path for e in os.scandir(root) if e.is_dir(follow_symlinks=False)),
    )
    # Loose files directly under root are folded into a single row.
    rows = []
    for child in children:
        oc = measure_overcount(child, seen)
        rows.append(oc)
    # Loose files directly under root (shallow — measure_overcount would re-walk).
    from disk_tree.extents import Overcount, alloc_and_private
    loose = Overcount(path=root)
    for e in os.scandir(root):
        if e.is_file(follow_symlinks=False):
            try:
                st = e.stat(follow_symlinks=False)
                if st.st_ino in seen:
                    continue
                seen.add(st.st_ino)
                alloc, priv = alloc_and_private(e.path)
            except OSError:
                loose.n_errors += 1
                continue
            loose.n_files += 1
            loose.apparent += alloc
            loose.exclusive += priv
    rows.sort(key=lambda o: -o.apparent)
    shown = rows if top == 0 else rows[:top]

    total_app = sum(o.apparent for o in rows) + loose.apparent
    total_exc = sum(o.exclusive for o in rows) + loose.exclusive
    total_files = sum(o.n_files for o in rows) + loose.n_files

    if as_json:
        print(json.dumps({
            'uri': root,
            'apparent': total_app, 'exclusive': total_exc, 'shared': total_app - total_exc,
            'n_files': total_files,
            'rows': [{
                'path': os.path.basename(o.path), 'apparent': o.apparent,
                'exclusive': o.exclusive, 'shared': o.shared, 'n_files': o.n_files,
            } for o in rows],
        }, indent=2))
        return

    human = not no_human
    pct = (total_app - total_exc) / total_app * 100 if total_app else 0
    print(f"{root}: {_fmt(total_app, human)} apparent, {_fmt(total_exc, human)} exclusive "
          f"({_fmt(total_app - total_exc, human)} shared, {pct:.0f}%)")
    print(f"{'apparent':>9}  {'exclusive':>9}  {'shared':>9}  {'files':>9}  child")
    for o in shown:
        print(f"{_fmt(o.apparent, human):>9}  {_fmt(o.exclusive, human):>9}  "
              f"{_fmt(o.shared, human):>9}  {o.n_files:>9,}  {os.path.basename(o.path)}/")
    if loose.n_files:
        print(f"{_fmt(loose.apparent, human):>9}  {_fmt(loose.exclusive, human):>9}  "
              f"{_fmt(loose.shared, human):>9}  {loose.n_files:>9,}  (files)")
