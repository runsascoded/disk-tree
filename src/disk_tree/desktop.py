"""Native macOS window around the disk-tree Flask server (pywebview).

Runs the existing Flask app (`disk_tree.server.app`) on a loopback port in a
daemon thread, then opens a WKWebView window pointed at it. Bundled as
`disk-tree.app` (see `packaging/macos/`), this gives scans a stable TCC
identity — prompts and Full Disk Access show "disk-tree", not "python3.13" —
and one FDA grant covers the app's child `gfind` reads. See
`specs/macos-app.md`.

Run directly for development: `disk-tree-app` (needs the `app` extra:
`uv sync --extra app`). The window and the CLI/server share the same config
and DB, so a scan started here shows up everywhere.
"""

from __future__ import annotations

import socket
import threading
import time


def _free_loopback_port() -> int:
    """An ephemeral port the OS just handed us, bound to loopback only."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def _wait_until_up(port: int, timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(('127.0.0.1', port)) == 0:
                return
        time.sleep(0.05)
    raise RuntimeError(f"Flask server did not come up on 127.0.0.1:{port} within {timeout}s")


def main() -> None:
    import webview

    from disk_tree.server import STATIC_DIR, app

    if not STATIC_DIR:
        # The window would render the API's SPA-fallback 404s; better to say so.
        raise SystemExit(
            "No bundled UI found. Build it first: `cd ui && pnpm build` "
            "(or install a wheel built with the UI included)."
        )

    port = _free_loopback_port()

    def serve() -> None:
        # threaded so the window's concurrent requests (SSE + data) don't
        # deadlock a single-threaded dev server; no reloader inside a thread.
        app.run(host='127.0.0.1', port=port, threaded=True, use_reloader=False, debug=False)

    threading.Thread(target=serve, daemon=True, name='disk-tree-flask').start()
    _wait_until_up(port)

    webview.create_window(
        'disk-tree',
        f'http://127.0.0.1:{port}/',
        width=1200,
        height=820,
        min_size=(720, 480),
    )
    webview.start()


if __name__ == '__main__':
    main()
