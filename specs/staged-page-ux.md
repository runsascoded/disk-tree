# Staged page: long paths, per-item delete, sizes

From the `~/.disk` cleanup session (2026-09-24), after Ryan reviewed an 18-item local plan (`~/Downloads` installers and redundant zips) on his phone at `m3:7791/staged` (side Vite → `disk-tree-server` with `DISK_TREE_DELETE_APPROVAL=staged`).

## 1. Long paths break the table on mobile

One long URI (`…/Insta360_Studio_6.0.4_release_insta360(RC_build71)_20260904_180546_signed_1788517496623.zip`) widens the whole table, so every row scrolls horizontally and the `unstage` column lands off-screen (the first screenshot showed only `unstage` links and an empty URI column). `StagedPage.tsx` has `wordBreak: 'break-all'` on the `<code>`, but the table still overflows.

Proposal:
- **Display relative to the plan's common prefix** (a header line reads `/Users/ryan/Downloads/`; rows show `ChatGPT.dmg`). Most plans are one or two directories.
- **Middle-elide** what remains (`Insta360_Studio_6.0.4_rel…1788517496623.zip`: keep the head and the extension-bearing tail) with the full URI in a floating-ui tooltip on hover / long-press, plus copy-on-click.
- `table-layout: fixed` with the path column taking the remaining width, so the action column is always visible.

## 2. Per-item delete ("delete now") on each row

Ryan wants to go down the list deleting one at a time rather than dispatching the whole plan. Today only `POST /api/dispatch` exists, and it's whole-plan and immediate (`for_real` defaults true; the UI's confirm step says `confirm — delete 18`).

Proposal: `POST /api/dispatch` accepts an optional `uris: [...]` subset → deletes just those, recorded as a normal `deletion_run` (so `staged` history and `undo` bookkeeping stay uniform), and the items leave the plan. UI: a per-row `delete` next to `unstage`, with the same inline two-tap confirm pattern as plan dispatch (`delete` → `confirm`).

**Executor note:** this is a local-server capability. A CFN (Worker/Lambda) can't reach laptop paths, so for `file://`-scheme items the executor is always the local server/CLI; the CP6–8 CFN/Batch axis applies only to bucket URIs. On a deployment without a local executor, the per-row control should hide for local URIs rather than 501.

## 3. Show each item's size and a plan total

`size_fn` already exists in `staged_backend.py`. Show the size per row and a total in the plan header (`18 items — 2.05 GiB`), so a dispatch confirm states what it frees. For local items, apparent size overstates what reflinked/cloned data frees (see `reclaim`); a "measure" action calling the `reclaim` engine for the plan would make the confirm honest, but it's optional here.

## 4. Dry-run affordance

The CLI's `dispatch` is dry by default; the UI's is not. A `preview` (dry dispatch: `for_real: false`) showing per-item bytes/objects before the confirm would match the CLI.
