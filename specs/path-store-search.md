# Path-store search: a segment-name index for the filter view

**Status:** implemented at fixture scale, 2026-10-01 (branch `seg-idx`, off `cloud`); writer behind `-S`, reader on whenever a generation has the sidecars. Production build and request costs unmeasured (§7). Base work (`cloud`). Builds the "vocabulary + block index + trigrams" tiers of [diff-and-search] §"The (prefix × query) product space" for the **served** path store ([path-store]); the August sidecar (`src/disk_tree/sidecar.py`, removed from `cloud` in `360be7d`) served only the local engine.

## 0. Problem

The filter view (`q=` on `/api/subtree` and `/api/diff`, `view.ts` `readView` "the filter view") finds its **match roots** — the outermost paths under the view root whose full path matches the query — by reading rows and testing each path (`filter.ts` `matchRoots`). On a store generation there are no coarse tiers, so phase 1 is the plain view's thresholded read of the whole subtree (`readSubtree` at `thrAt`), then `matchRoots` over what came back. Measured on gcs.oa.dev (2026-10-01, ~777M rows per scan):

- 2–5 s per filtered view, most of it decoding rows that don't match;
- matches below the read's byte threshold are invisible (a small `ttl=7d` dir under a big bucket never comes back);
- a query with no match falls through to the widest read the view can make; on old v1 scans that read whole buckets and hit Cloudflare's 1102 (CPU/memory).

A Worker has ~128 MB and limited CPU; a request should read a few MB. The fix is an index from the query to the rows that can be match roots, so phase 1 reads only those.

## 1. Query semantics (unchanged; what the index must reproduce)

`scope.ts` `parseQuery` (mirrored by the client's `filterTree.ts`):

- `/…/` (length > 2): a JS `RegExp` with flag `i`, tested against the **full index path** (`bucket/dir/sub`);
- otherwise: `|`-separated needles, each trimmed and lowercased; a path matches when its lowercased full path contains any needle.

A match root under view root `P` is a path strictly under `P` that matches while none of its strict ancestors below `P` does (if `P` itself matches, the whole view is matched and no search runs).

**Lemma (what the last segment of a match root must satisfy).** Let `p` be a match root, `s` its last segment, `parent(p)` its parent (a prefix of `p`'s string). Any match inside `parent(p)`'s characters with the same surrounding context would make `parent(p)` match, so every match in `p` touches `s`. Hence:

- a slash-free needle `t`: `s.toLowerCase()` contains `t` (a slash-free substring can't straddle a separator);
- a needle with a slash, `t = …/u`: the occurrence's last `/` is the separator before `s`, so `s.toLowerCase()` **starts with** `u` (the needle's last piece). A needle ending in `/` (`u = ''`) constrains `s` not at all (every child of a dir named `…t` is a root); the index does not serve it (§5 fallback);
- a regex that **cannot match `/`** (no `.`, no `\D \W \S`, no class admitting `/`, no negated class without `/`, no lookaround or backreference): the match lies inside one segment, which must be `s`, and `re.test(s)` holds on `s` alone — `^` can only match at the bucket (the first segment, at the path's start), `$` only at `s`'s end, and `\b` at a segment edge sees `/` or the string edge, both non-word. A regex that can match `/` (`/ckpt.*final/`) can match across segments and constrains no single segment; the index does not serve it (§5).

So every match root's last segment is in a **candidate name set** computable from the query alone. The index maps a query to that set, the set to rows, and the rows' full paths are tested with the very predicate `parseQuery` returns — results are exact by construction, never "close".

## 2. Artifacts (per index generation, beside the `path` sort; zero D1 rows)

Written by `disk_tree.find.search.write_search` from the generation's `path-index.parquet` (any sort works; ordinals refer to the `path` sort because that is what the reader's row-group reads already use). Three files under the generation dir `index/<gen>/`:

### 2.1 `path-index.names.parquet` — the vocabulary, in impact order

One row per distinct last segment (`name`) over every row of the store (objects' basenames, dirs' names, buckets), columns:

| col | type | |
|---|---|---|
| `id` | int32 | row ordinal (dense `0..N-1`) |
| `name` | string | the segment, as stored (case kept) |
| `n` | int64 | store rows whose last segment is `name` (owner slices count) |
| `b` | int64 | Σ `size` of those rows (ranking only — nested rows double-count) |
| `n_rgs` | int32 | distinct `path`-sort row groups holding those rows |
| `rgs` | string | those row-group ordinals, ascending, comma-separated; NULL when `n_rgs > rg_cap` (§2.4) |

Sorted **`b desc, name`** (one DuckDB `COPY`: last segment by `regexp_extract`, the row's `path` group by an `ASOF` join of `file_row_number` against the groups' starts, a `(name, rg)` pre-aggregation so `rgs` is built from distinct groups): ids are an impact order (Anh & Moffat), so every candidate list is heaviest-first and any budgeted prefix of it is "the heaviest matches" (§4.3). `rgs` is a string, not `list<int32>`: the reader revives row groups from a flat schema (`reviveRowGroup` has no nested-column support), and a sorted digit string zstd-compresses about as well.

### 2.2 `path-index.trigrams.parquet` — trigram → name-id postings

One row per (trigram, name) with the trigram in the name: `tri` int32 (`c0<<16 | c1<<8 | c2`), `id` int32, sorted `(tri, id)`. Trigrams are taken from DuckDB `lower(name)` and **only all-ASCII trigrams are kept**; the reader likewise extracts only all-ASCII trigrams from the (lowercased) query. That keeps the index sound across case-mapping differences between DuckDB, Python and JS (`'İ'` lowercases to `i` in DuckDB, `i̇` in JS; `'K'` (Kelvin) to `k` in both): an ASCII run in a name stays the same ASCII run under any of them, extra non-ASCII trigrams are never looked up, and a JS `/x/i` regex (non-unicode mode) matches an ASCII literal only by its two ASCII cases.

### 2.3 `path-index.search.parquet` — the directory

What makes the two data files range-readable without parsing their footers (a gcs-sized postings footer is ~10 MB of thrift): one row per data row group of both files, the `.groups.parquet` idea ([path-store] §1.6) generalized:

| col | |
|---|---|
| `file` | int8: 0 = names, 1 = trigrams |
| `rg` | the data file's row-group ordinal |
| `row_start`, `row_end` | its absolute rows |
| `k_min`, `k_max` | its key range: `id` for names, `tri` for trigrams |
| `rg_json` | the compact `RowGroup` the reader revives (`[num_rows, codec, [[data_page_offset, size, dict_offset|0], …]]`) |

Sorted `(file, rg)`, 512 rows per directory row group, zstd, statistics on `file, rg, k_min, k_max` only. Key-value metadata: `search_v=1`, both data files' flat schemas (hyparquet `SchemaElement`s), `names`, `postings`, `rg_cap`, `path_groups` (the `path` sort's row-group count, a consistency check), `path_rows`.

A reader opens it with one tail read (colo-cached; NotFound = the generation has no search index → today's behaviour), prunes its own row groups by those stats, and decodes only the directory groups it needs.

### 2.4 Parameters

| | default | why |
|---|---|---|
| `names_rg_rows` | 2048 | names row group ≈ 40–60 KB: the unit of verification reads (DuckDB flushes groups in 2048-row steps, so both data files take multiples of 2048) |
| `postings_rg_rows` | 16384 | postings row group ≈ 40 KB |
| `rg_cap` | 512 | a name in more `path` groups than this is too wide to read anyway (§4.3); its `rgs` is NULL, `n_rgs` says how wide |
| directory rows/group | 512 | as `.groups.parquet` |

## 3. Sizes (gcs, estimated; to be measured — §7)

Inputs: ~777M rows/scan; distinct dir-segment names ~15.5M, distinct basenames ~101M (probe 2026-08-19 over 594M objects; ~104M at 613M), so N ≈ 115–120M names.

| artifact | rows | est. bytes/scan |
|---|---|---|
| names | ~120M | ~2–4 GB (names ~10–12 B zstd in impact order, `rgs` ~0.7 GB, counts) |
| trigrams | ~2.4B (≈ 20 distinct ASCII trigrams per name) | ~5–7 GB (sorted ids, zstd) |
| directory | ~120k + ~150k | ~20–40 MB; its footer ~150 KB (~500 groups) |

≈ 7–11 GB/scan beside ~27.5 GB of sorts (+25–40 %). **Decision for the user:** the basenames are ~85 % of it; a `dirs`-only vocabulary (~15.5M names → ~1 GB) is exact for directory roots but cannot find an object whose own name is the only match (`.safetensors` under non-matching dirs) — so a dirs-only reader would have to keep today's fallback for every query. Not built; `all` is the only mode.

## 4. How the Worker answers a query

### 4.1 Plan (pure; `searchQuery.ts`)

`planSearch(q)` returns `null` (the index can't serve it → today's path) or an OR of **branches**, each a trigram formula plus an exact name test:

- needle `t` without `/`: formula `AND(tri(t))`, test `name.toLowerCase().includes(t)`;
- needle with `/`, last piece `u ≠ ''`: `AND(tri(u))`, test `startsWith(u)`; `u = ''` → `null`;
- regex: parsed by a small recursive-descent parser over the JS syntax subset (alternation, groups `(…)`/`(?:…)`/`(?<n>…)`, quantifiers `* + ? {n,m}` and lazy forms, classes with ranges/escapes, `\d \w \s \D \W \S \b \B`, anchors, literal escapes). Anything else (lookaround, backreference, `\p{…}`, a parse failure) → `null`; a regex that can match `/` → `null`. Otherwise the formula is a simplified [Cox] compilation: each node yields an exact string set (when small: ≤ 16 strings, from literals, small classes, `?`, small products/unions) or a match formula; `* {0,…}` and wide classes are `TRUE`, `+ {n≥1,…}` keep their operand's formula; an exact set becomes `OR_s AND(tri(s))` (any string shorter than 3 makes it `TRUE`). Test: `re.test(name)`.

A formula of `TRUE` (a short needle, `/a.b/`-shaped regex fragments) is still served — by the names scan of §4.3, not trigrams.

### 4.2 Candidates

1. **Directory**: open `search.parquet` (footer, then the directory groups whose `file`/`k_min`/`k_max` stats admit each needed trigram).
2. **Postings**: a trigram whose postings span more than `triRgs` (8) row groups (> ~128k names) is unselective and treated as `TRUE` — sound, it only widens the candidate set; an AND keeps its selective children, an OR with an unselective child is `TRUE`. The selective trigrams' postings groups are range-read and intersected/unioned per the formula. A query whose formula ends up `TRUE` uses the names scan instead.
3. **Names** (verification): candidate ids ascending (= heaviest first) → their names row groups (directory `file=0`, by `k` range), at most `namesRgs` (32) of them; each candidate name is tested with its branch's exact name test. The **names scan** (formula `TRUE`) reads names groups `0, 1, …` up to the same budget and tests every name — "the heaviest names that match".

### 4.3 Rows, roots, guardrails

4. **Rows**: the verified names' `rgs`, unioned in id order until `pathRgs` (32) `path` groups; a NULL `rgs` (over `rg_cap`) or the budget running out stops the union and marks the result **truncated**. Those groups are read through the existing `path`-sort machinery (D1 / `.groups.parquet` / blob `rg_json`, the per-isolate decoded-group LRU) and filtered to rows strictly under `P` whose full path passes the query predicate.
5. **Roots**: each kept row's root is its **shallowest matching prefix** below `P` (computed from the path string). Not truncated ⇒ every root's own rows were read (by the lemma, its name is a candidate and all its rows' groups are in its `rgs`), so the roots and their aggregates come straight from the rows — the same set `matchRoots` returns over all rows. Truncated ⇒ the roots are those of every matching row in the groups read (lighter names' included, when they share a group with a heavier one), and a lifted root may be unread: its rows are fetched with one `readAsks` point lookup (≤ `liftGroups` = 60 groups; wider → those roots are dropped, still flagged truncated). The lookup also covers the one non-truncated way a root could go unread — DuckDB's and JS's lowercase tables disagreeing on a name — instead of failing the request. A search **cut before it found any root** (its heaviest name alone over budget) says nothing about the query, so the view falls back to the thresholded read of §5 rather than report "no matches".
6. **Phase 2** is unchanged: the roots' subtrees at one forest threshold, attenuated per root, from `bysize`/`path` (`readSubtree`).

So a request reads, cold: the directory footer (~150 KB, cached per colo and isolate), a few directory groups (~25 KB each), the selective trigrams' postings groups (~40 KB each, ≤ 8 per trigram), ≤ 32 names groups (~25 KB), ≤ 32 `path` groups (~150 KB at 8K rows) — a few MB in the worst case, ~1 MB for a selective query — and never an unbounded read. **No match is cheap**: an empty candidate set, not truncated, returns "no matches" without touching the store's sorts.

### 4.4 Ordering, response

`/api/subtree`'s `matched` (the per-root list behind bulk actions and the series) is ordered **bytes desc, then path**, whichever way the roots were found; `matches` stays path-sorted, and `/api/diff`'s `matched` stays the path-sorted union of both sides. `truncated` is true when the search was budget-cut (or as before, the node cap). `tier` reads `search+<sort>` when the index found the roots, and `Server-Timing` gains `search;desc="<mode> c<candidates> n<names groups> g<path groups>[ cut]"`.

## 5. Fallback (unchanged behaviour)

The filter view uses today's phase 1 when: the generation has no `search.parquet` (every generation written before this, all v1 scans), the query is a needle ending in `/`, or a regex the index can't serve (can match `/`, unsupported syntax). Deliberately not a semantics change: `/ckpt.*final/` keeps matching across segments.

## 6. Cross-scan reuse (later)

Vocabulary churn is ~0.1–0.2 %/scan (gcs, [path-store] §4.7), so the names' text and the trigram postings — ~90 % of the bytes — barely change between scans; only `n`, `b` and `rgs` (row-group ordinals of that scan's `path` sort) are per scan. A later layout: an append-only cross-scan vocabulary (stable ids, postings over it, sealed in K-scan groups like the store archive) plus a small per-scan `id → (n, b, rgs)` table. Cost: ids stop being an impact order (first-seen instead), so the budgeted prefix stops meaning "heaviest"; a per-scan rank column restores it for the names scan, not for postings. Not built.

## 7. As implemented, and what is open

- **Writer** — `src/disk_tree/find/search.py` `write_search(path_sort, *, con, names_rg_rows, postings_rg_rows, rg_cap, dir_rg_rows)` (local files; sets `preserve_insertion_order` for its `COPY`s, which `viz` turns off). `dt_cloud.index.write_sorts(search=True)` runs it on the cut `path` sort and reports `{names, postings, files}` as the `path` entry's `search` (and `write_index`'s summary `search`); `dt-cloud path-index -S` / `index-write -S` (off by default). The generation dir's upload carries the files (r2's `daily-ingest.yml` copies the dir recursively; it does not pass `-S` yet).
- **Reader** — `site/functions/_lib/searchQuery.ts` (`planSearch`, `trigrams`, formula `and`/`or` with absorption: `ckpt | ckpts` = `ckpt`), `search.ts` (`openSearch`, `searchRoots`, `SEARCH_LIMITS`), `index.ts` exports for it (`readGroupsAt` by ordinal, `readFooterBytes`, `cachedRange`, the group LRU, the handle's `dir`). `scope.ts` `parseQuery` attaches the query string to its predicate (`NamePred.q`). `view.ts` `readView`'s filter phase 1 calls `searchRoots` on a store generation without a lens (`ViewOpts.searchLimits` overrides the budgets) and keeps its old reads otherwise; `buildDiff` gets it through `readView` on each side.
- **Tests** — `tests/test_search.py` (exact names table incl. `rgs` per row group, the full postings list, directory rows and key-value metadata, statistics only on the pruning columns, `rg_cap`, the Kelvin sign and a non-ASCII trigram, rejects); `cloud/tests/test_index.py::test_write_index_search`; `site/functions/_lib/searchQuery.test.ts` (formulas per query, name tests, declined queries); `search.test.ts` over `fixtures/v2-search/` (`gen.py` `write_v2_search`: 2 buckets, 6,047 rows, 3 `path` groups, 6,044 names, 24,199 postings, 2 directory rows per group): roots = `matchRoots` over every row for 20 queries × 5 roots, the roots' rows exact, the blob-served copy equal, stage counts for `ttl`, the names scan, no `path` read on no match, each budget's cut (incl. a lifted ancestor), whole-view equality with the pre-index read at threshold 0, a sub-budget match only the index finds, `matched` order, the fallback on a cut-before-any-root, and a filtered diff.

Open, needing a production build/measurement:

- build wall/peak/bytes on gcs's Batch class (2.4B-row postings sort) and cw; whether DuckDB's default encodings are acceptable for `id` (`DELTA_BINARY_PACKED` via pyarrow would be ~2× smaller);
- the deployments' jobs passing `-S` and uploading the three files with the generation (r2 first: CI, small, public);
- request cost on real queries (`ttl`, `ckpt`, `checkpoints`, `.safetensors`, a 2-char needle): directory/postings/names/`path` groups read, CPU of the `path` group decode (phase 1 decodes whole groups; a `path`-column-only first pass is a possible saving), and the budgets above;
- the `dirs`-only or bytes-coverage-cut vocabulary (§3) if the size is unwelcome.

Decisions for the user:

- **Regex semantics.** A regex that can match `/` (most uses of `.`, e.g. `/ckpt.*final/`) keeps today's full-path, cross-segment meaning and so falls back to the thresholded read. Adopting the local engine's rule ([diff-and-search]: a slash-free regex matches *one segment*; `/` in it means a full-path match) would let every slash-free regex use the index, at the cost of a semantics change on the page.
- **Vocabulary scope** (§3): all names (exact for every query, ~7–11 GB/scan est.) vs dirs only (~1 GB, but object-name queries fall back).
- **`-S` default**: off until measured; on for r2 first?

[diff-and-search]: diff-and-search.md
[path-store]: path-store.md
[Cox]: https://swtch.com/~rsc/regexp/regexp4.html
