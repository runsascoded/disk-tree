#!/usr/bin/env bash
# Build (and self-sign) disk-tree.app with PyInstaller. See README.md.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
cd "$ROOT"

# 1. Build the UI and stage it where the spec bundles it from.
( cd ui && pnpm install && pnpm build )
rm -rf src/disk_tree/static
cp -R ui/dist src/disk_tree/static

# 2. Build tooling (PyInstaller is a build-time tool, not a runtime dep).
uv pip install pyinstaller

# 3. Freeze → dist/disk-tree.app
rm -rf build dist
pyinstaller --clean --noconfirm \
    --distpath "$ROOT/dist" --workpath "$ROOT/build" \
    packaging/macos/disk-tree.spec

# 4. Sign. A *self-signed* identity (not ad-hoc) keeps the Full Disk Access
#    grant stable across rebuilds — ad-hoc's cdhash changes every build.
IDENTITY="${DISK_TREE_SIGN_ID:-disk-tree-selfsigned}"
if security find-identity -v -p codesigning 2>/dev/null | grep -q "$IDENTITY"; then
  codesign --deep --force --sign "$IDENTITY" dist/disk-tree.app
  echo "signed dist/disk-tree.app with '$IDENTITY'"
  codesign -dv --verbose=2 dist/disk-tree.app 2>&1 | grep -E "Identifier|Authority" || true
else
  echo "WARNING: signing identity '$IDENTITY' not found — bundle left UNSIGNED."
  echo "  A rebuild would then need FDA re-granted each time. Create a stable"
  echo "  self-signed cert first — see packaging/macos/README.md."
fi

echo "built dist/disk-tree.app"
