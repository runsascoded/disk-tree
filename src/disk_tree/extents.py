"""Physical-extent mapping: which of a subtree's bytes are actually its own.

`gfind -printf '%b'` charges a file's allocated blocks to every path that links
them, so a subtree's *reported* size is an upper bound on what deleting it
frees. Hardlinks are at least visible to `stat` (`nlink > 1`); APFS **clones**
(reflinks) are not — distinct inodes, `nlink == 1`, full block count each — and
on this platform they are the dominant sharing mechanism: `uv` and `pnpm` both
populate project trees by cloning from their caches.

The only way to tell owned bytes from shared ones is to ask the filesystem
where a file physically lives. `fcntl(F_LOG2PHYS_EXT)` answers that per logical
range; two paths that map to the same device offsets share the same blocks.

Note the struct is *packed* — `u_int32_t` immediately followed by two `off_t`,
no alignment padding — which is why the format below is `=Iqq` (20 bytes) and
not the 24 bytes natural alignment would suggest.
"""

from __future__ import annotations

import fcntl
import os
import struct
import sys
from dataclasses import dataclass, field

F_LOG2PHYS_EXT = 65
_FMT = '=Iqq'  # l2p_flags, l2p_contigbytes, l2p_devoffset
_SZ = struct.calcsize(_FMT)

SUPPORTED = sys.platform == 'darwin'

#: (device offset, length) of one physically contiguous run.
Extent = tuple[int, int]


def file_extents(path: str, max_extents: int = 4096) -> list[Extent]:
    """Physical extents backing `path`, or [] if the file has none.

    An empty list is not an error: zero-length files have no extents, and so do
    decmpfs-compressed files, whose data lives in an xattr rather than in block
    storage.
    """
    if not SUPPORTED:
        raise NotImplementedError(f'physical extent mapping is Darwin-only (got {sys.platform})')
    fd = os.open(path, os.O_RDONLY)
    try:
        size = os.fstat(fd).st_size
        out: list[Extent] = []
        off = 0
        while off < size and len(out) < max_extents:
            res = fcntl.fcntl(fd, F_LOG2PHYS_EXT, struct.pack(_FMT, 0, size - off, off))
            _, length, phys = struct.unpack(_FMT, res[:_SZ])
            if length <= 0:
                break
            out.append((phys, length))
            off += length
        return out
    finally:
        os.close(fd)


@dataclass
class Subtree:
    """One candidate root's accounting."""
    path: str
    apparent: int = 0        # Σ st_blocks × 512 — what `du`/disk-tree report
    mapped: int = 0          # bytes we resolved to physical extents
    unmapped: int = 0        # compressed/sparse/unreadable — assumed owned
    n_files: int = 0
    n_errors: int = 0
    extents: set[Extent] = field(default_factory=set, repr=False)


@dataclass
class Reclaim:
    subtrees: list[Subtree]
    partners: list[str]
    shared: int              # candidate bytes whose extents also live in a partner
    unique: int              # candidate bytes nothing outside the set references
    partner_files: int
    shared_set: set[Extent] = field(default_factory=set, repr=False)

    @property
    def apparent(self) -> int:
        return sum(s.apparent for s in self.subtrees)


def _walk_files(root: str):
    """Regular files under `root` (no symlink following), depth-first."""
    if os.path.isfile(root):
        yield root
        return
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        for name in filenames:
            p = os.path.join(dirpath, name)
            if not os.path.islink(p):
                yield p


def scan_subtree(root: str, seen_inodes: set[int]) -> Subtree:
    """Map every file under `root`, charging hardlinked inodes only once."""
    sub = Subtree(path=root)
    for p in _walk_files(root):
        try:
            st = os.lstat(p)
        except OSError:
            sub.n_errors += 1
            continue
        if st.st_ino in seen_inodes:
            continue           # a hardlink we already charged
        seen_inodes.add(st.st_ino)
        sub.n_files += 1
        sub.apparent += st.st_blocks * 512
        try:
            ex = file_extents(p)
        except OSError:
            sub.n_errors += 1
            sub.unmapped += st.st_blocks * 512
            continue
        if not ex:
            sub.unmapped += st.st_blocks * 512
            continue
        for phys, length in ex:
            sub.extents.add((phys, length))
            sub.mapped += length
    return sub


def partner_extents(roots: list[str], want: set[Extent]) -> tuple[set[Extent], int]:
    """Extents under `roots` that also appear in `want` (the candidate set).

    Only intersecting extents are retained — the caches are far larger than any
    candidate set, and holding all of their extents would cost more memory than
    the answer is worth.
    """
    hits: set[Extent] = set()
    n = 0
    for root in roots:
        for p in _walk_files(root):
            n += 1
            try:
                ex = file_extents(p)
            except OSError:
                continue
            for e in ex:
                if e in want:
                    hits.add(e)
    return hits, n


def default_partners() -> list[str]:
    """Caches known to populate project trees by cloning."""
    home = os.path.expanduser('~')
    cache = os.environ.get('XDG_CACHE_HOME') or os.path.join(home, '.cache')
    candidates = [
        os.path.join(cache, 'uv'),
        os.path.join(home, 'Library', 'pnpm', 'store'),
        os.path.join(home, 'Library', 'Caches', 'pnpm'),
    ]
    return [p for p in candidates if os.path.isdir(p)]


def measure(roots: list[str], partners: list[str] | None = None) -> Reclaim:
    """How much deleting `roots` would actually free, given `partners` remain."""
    partners = default_partners() if partners is None else partners
    seen: set[int] = set()
    subtrees = [scan_subtree(r, seen) for r in roots]
    want: set[Extent] = set()
    for s in subtrees:
        want |= s.extents
    hits, n_partner = partner_extents(partners, want)
    shared = sum(length for _, length in hits)
    mapped = sum(length for _, length in want)
    unmapped = sum(s.unmapped for s in subtrees)
    return Reclaim(
        subtrees=subtrees,
        partners=partners,
        shared=shared,
        shared_set=hits,
        unique=mapped - shared + unmapped,
        partner_files=n_partner,
    )
