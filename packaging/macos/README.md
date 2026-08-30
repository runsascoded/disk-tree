# Packaging disk-tree as `disk-tree.app` (macOS)

v1 wraps the existing Flask server + built React UI in a native WKWebView window
(`disk_tree.desktop`) and freezes it with **py2app**. This gives scans a stable
TCC identity — prompts and Full Disk Access show **"disk-tree"**, and one FDA
grant covers the app's child `gfind`. See `specs/macos-app.md` for the full
design (and the Tauri v2 plan).

## One-time: a stable self-signed signing identity

Ad-hoc signing (`codesign -s -`) works but its cdhash changes every rebuild, so
the FDA grant breaks each time. A **self-signed certificate** gives a stable
identity with no Apple Developer account:

Keychain Access → *Certificate Assistant* → *Create a Certificate…*
- Name: `disk-tree-selfsigned`
- Identity Type: *Self Signed Root*
- Certificate Type: *Code Signing*
- (Let it create; then in Keychain, set the cert to *Always Trust* for code signing.)

Verify it's usable: `security find-identity -v -p codesigning` should list
`disk-tree-selfsigned`. Override the name with `DISK_TREE_SIGN_ID=…`.

No full **Xcode** is needed at any point — only the Command Line Tools
(`xcode-select --install`) for `codesign`.

## Build

```bash
packaging/macos/build.sh      # → dist/disk-tree.app  (builds UI, freezes, signs)
open dist/disk-tree.app
```

## Grant Full Disk Access

System Settings → Privacy & Security → Full Disk Access → add
`dist/disk-tree.app` (or wherever you move it). Now it — and its scans — read
protected folders (Photos/Desktop/Documents/Downloads) with no prompts.

## Runtime dependency: `gfind`

v1 still shells out to GNU find. The bundle sets no PATH of its own, so install
`gfind` (Homebrew: `brew install findutils`) and ensure `/opt/homebrew/bin` is
reachable, or the scan errors. v2 (Tauri + a native `getattrlistbulk` walker)
removes this dependency.

## Notes / known-fiddly

- `disk_tree/static/` is a **build artifact** (copied from `ui/dist`); it's
  git-ignored, not committed.
- py2app + native wheels (pyarrow/duckdb/pandas) can need `packages`/`includes`
  tuning in `setup.py` if the first build reports missing modules or dylibs.
- For *distribution* to other machines (not just this one), you'd add hardened
  runtime + Developer ID + notarization (`notarytool`) — also CLT-only.
