"""SSH backend: scan remote hosts by running `gfind` over SSH.

Auth relies entirely on `~/.ssh/config` (host, user, port, identity, keys).
OpenSSH ControlMaster (if configured) multiplexes; we don't manage it here.
"""

import shlex
import subprocess
from typing import Iterator

from .base import Backend, ErrorCollector, ProgressCallback
from .gfind import run_gfind
from .url import parse_url


class SshBackend(Backend):
    """Remote filesystem via `ssh <host> gfind ...`."""

    scheme = 'ssh'

    @property
    def is_local(self) -> bool:
        return False

    @property
    def supports_sudo(self) -> bool:
        # Technically possible with NOPASSWD + -tt, but v1: require user to
        # configure passwordless sudo if they want it; we don't prompt.
        return False

    def _ssh_base(self, url: str) -> tuple[list[str], str, str]:
        """Return (ssh-prefix-argv, remote-abs-path, canonical-host-part).

        `canonical-host-part` is the `[user@]host[:port]` chunk used to build
        per-entry URIs (e.g. `ssh://<host>/<path>`).
        """
        p = parse_url(url)
        if p.scheme != 'ssh' or not p.host:
            raise ValueError(f"SshBackend got non-ssh url: {url!r}")
        ssh = ['ssh']
        if p.port:
            ssh.extend(['-p', str(p.port)])
        target = f'{p.user}@{p.host}' if p.user else p.host
        ssh.append(target)
        # Host part for URI construction (preserve user/port if present)
        userpart = f'{p.user}@' if p.user else ''
        portpart = f':{p.port}' if p.port else ''
        host_part = f'{userpart}{p.host}{portpart}'
        return ssh, p.path, host_part

    def list(
        self,
        url: str,
        *,
        errors: ErrorCollector | None = None,
        excludes: list[str] | None = None,
        sudo: bool = False,
        progress_callback: ProgressCallback | None = None,
        progress_interval: float = 1.0,
    ) -> Iterator[dict]:
        ssh, remote_path, host_part = self._ssh_base(url)

        # Try gfind first, fall back to find (Linux); error on BSD find.
        remote_cmd = (
            f"if command -v gfind >/dev/null 2>&1; then FIND=gfind; "
            f"elif find --version 2>/dev/null | grep -q GNU; then FIND=find; "
            f"else echo 'GNU find required on remote (install coreutils/findutils)' >&2; exit 127; fi; "
            f"$FIND {shlex.quote(remote_path)} -printf '%y %b %T@ %p\\0'"
        )
        cmd = [*ssh, remote_cmd]

        yield from run_gfind(
            cmd,
            remote_path,
            uri_for=lambda p: f'ssh://{host_part}{p}',
            errors=errors,
            progress_callback=progress_callback,
            progress_interval=progress_interval,
        )

    def delete(self, url: str) -> None:
        ssh, remote_path, _ = self._ssh_base(url)
        cmd = [*ssh, f'rm -rf {shlex.quote(remote_path)}']
        subprocess.run(cmd, check=True)

    def exists(self, url: str) -> bool:
        ssh, remote_path, _ = self._ssh_base(url)
        cmd = [*ssh, f'test -e {shlex.quote(remote_path)}']
        return subprocess.run(cmd).returncode == 0
