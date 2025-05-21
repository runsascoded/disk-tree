import json
from dataclasses import asdict
from sys import stdout

from utz import err, Encoder

from disk_tree.cli.base import cli
from disk_tree.sqla import init, Scan


@cli.command
def scans():
    """Scan the disk tree for files and directories."""
    db = init()
    scans = db.session.query(Scan).all()
    # err(f"Found {len(scans)} scans")
    for scan in scans:
        json.dump(asdict(scan), stdout, cls=Encoder)
        print()
