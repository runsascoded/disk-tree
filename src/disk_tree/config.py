from glob import glob
from os import environ as env, makedirs, pathsep, sep
from os.path import expanduser, exists, ismount, join

DISK_TREE_ROOT_VAR = 'DISK_TREE_ROOT'
DISK_TREE_SCAN_DIRS_VAR = 'DISK_TREE_SCAN_DIRS'
HOME = env['HOME']
CONFIG_DIR = join(HOME, '.config')
DEFAULT_ROOT_DIR = join(CONFIG_DIR, 'disk-tree')
ROOT_DIR = expanduser(env.get(DISK_TREE_ROOT_VAR, DEFAULT_ROOT_DIR))

if not exists(ROOT_DIR):
    makedirs(ROOT_DIR)

DEFAULT_SCANS_DIR = join(ROOT_DIR, 'scans')
SQLITE_PATH = join(ROOT_DIR, 'disk-tree.db')

#: Volumes are probed here for an opted-in `disk-tree/scans` directory.
VOLUMES = join(sep, 'Volumes')


def _volume_mounted(path: str) -> bool:
    """False when `path` sits under a `/Volumes/<name>` that isn't mounted.

    Creating a directory under an absent mount point silently writes to the
    boot disk — the very disk an external scans dir exists to spare — and the
    files vanish from view the moment the volume comes back. So an unmounted
    candidate is never a write target.
    """
    parts = path.split(sep)
    if len(parts) > 2 and parts[1] == 'Volumes':
        return ismount(join(VOLUMES, parts[2]))
    return True


def discovered_scan_dirs() -> list[str]:
    """External scans dirs, opted in by existing on a mounted volume.

    Creating `<volume>/disk-tree/scans` is the whole opt-in: no config file, and
    an unplugged volume simply drops out of the search path.
    """
    return sorted(d for d in glob(join(VOLUMES, '*', 'disk-tree', 'scans')) if _volume_mounted(d))


def configured_scan_dirs() -> list[str]:
    """Scan dirs in priority order — first writable one wins for new blobs."""
    raw = env.get(DISK_TREE_SCAN_DIRS_VAR)
    if raw:
        return [expanduser(p) for p in raw.split(pathsep) if p]
    if DISK_TREE_ROOT_VAR in env:
        # An explicit root is a deliberate choice (tests, alternate profiles);
        # discovery must not silently redirect writes out of it.
        return [DEFAULT_SCANS_DIR]
    return [*discovered_scan_dirs(), DEFAULT_SCANS_DIR]


def scan_write_dir() -> str:
    """Where new blobs go: the first configured dir on a mounted volume."""
    for d in configured_scan_dirs():
        if _volume_mounted(d):
            return d
    return DEFAULT_SCANS_DIR


def scan_read_dirs() -> list[str]:
    """Every dir a blob might live in, nearest first.

    `SCANS_DIR` leads so that a monkeypatched (or env-overridden) write dir is
    always searched first, and the internal default always trails so blobs
    written before an external volume existed stay reachable.
    """
    seen, out = set(), []
    for d in [SCANS_DIR, *configured_scan_dirs(), DEFAULT_SCANS_DIR]:
        if d not in seen:
            seen.add(d)
            out.append(d)
    return out


def resolve_scan_blob(name: str, prefer: str | None = None) -> str:
    """Absolute path for a blob basename — the first read dir that has it.

    Falls back to the write dir so callers creating a blob get a sensible path;
    a missing blob then fails at open time with the path it looked for.
    """
    for d in ([prefer] if prefer else []) + scan_read_dirs():
        p = join(d, name)
        if exists(p):
            return p
    return join(prefer or SCANS_DIR, name)


#: The write target. Resolved once per process; re-plugging a volume mid-run
#: won't be noticed, but a new blob never lands somewhere unreadable.
SCANS_DIR = scan_write_dir()
