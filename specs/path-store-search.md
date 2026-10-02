# Path-store search: a segment-name index for the filter view

**Status:** implemented at fixture scale, 2026-10-01; the filter syntax (§1: AND, NOT, `*`, quoting; regex a hidden fallback) replaced regex 2026-10-02 (branch `seg-idx`, off `cloud`); writer behind `-S`, reader on whenever a generation has the sidecars. Production build and request costs unmeasured (§7). Base work (`cloud`). Builds the "vocabulary + block index + trigrams" tiers of [diff-and-search] §"The (prefix × query) product space" for the **served** path store ([path-store]); the August sidecar (`src/disk_tree/sidecar.py`, removed from `cloud` in `360be7d`) served only the local engine.

## 0. Problem

The filter view (`q=` on `/api/subtree` and `/api/diff`, `view.ts` `readView` "the filter view") finds its **match roots** — the outermost paths under the view root whose full path matches the query — by reading rows and testing each path (`filter.ts` `matchRoots`). On a store generation there are no coarse tiers, so phase 1 is the plain view's thresholded read of the whole subtree (`readSubtree` at `thrAt`), then `matchRoots` over what came back. Measured on gcs.oa.dev (2026-10-01, ~777M rows per scan):

- 2–5 s per filtered view, most of it decoding rows that don't match;
- matches below the read's byte threshold are invisible (a small `ttl=7d` dir under a big bucket never comes back);
- a query with no match falls through to the widest read the view can make; on old v1 scans that read whole buckets and hit Cloudflare's 1102 (CPU/memory).

A Worker has ~128 MB and limited CPU; a request should read a few MB. The fix is an index from the query to the rows that can be match roots, so phase 1 reads only those.

## 1. Query semantics (what the index must reproduce)

One parser, `site/functions/_lib/pathQuery.ts` (`parsePathQuery`, `parseQuery`), used by the server (`scope.ts` re-exports it) and the client (`src/filterTree.ts` re-exports it). Case-insensitive throughout; every term is tested against a node's **full index path** (`bucket/dir/sub`). Adopted 2026-10-02 in place of regex as the filter's syntax; the box reads "text · a|b · a b (both) · -x (not) · * (any chars in a name)".

| syntax | meaning |
|---|---|
| `tomat` | substring anywhere in the path |
| `a\|b` | OR; binds looser than AND: `a b\|c` = (a AND b) OR c |
| `a b` | AND: the path contains every term, in any segments (`ckpt final` matches `x/ckpt/run/final` and `x/ckpt-final`) |
| `-x` | NOT: nothing whose path contains `x` counts (below) |
| `*` | any characters within one segment (`[^/]*`), also in a term with `/` (`ckpt*final` matches the name `ckpt-run-b-final`) |
| `"a b"` | a quoted term is literal: spaces, a leading `-`, `\|`, `*` |
| `…/…` | a term with `/` is a substring of the full path, like any other |
| `/…/` | a JS regex (flag `i`) over the full path — an **undocumented fallback**, never served by the index (§5) |

A blank alternative (`a|`) adds nothing; an empty quoted term is dropped; a lone `-` is a literal term.

**The predicate.** A query is `pos ∧ ¬neg` on the lowercased path: `pos` = OR over alternatives of AND over their positive terms; `neg` = OR of the NOT terms. **Negatives apply to the whole query**, whatever alternative they were written in (`a -x|b` excludes `x` from `b`'s matches too); an alternative with no positive term — a query of only negatives — makes `pos` everywhere true ("everything except"). Both halves are monotone along a root-to-leaf chain (a substring of a prefix is a substring of every extension), so the predicate is an interval on every chain: false, then true from the first path where `pos` holds, then false again from the first path where `neg` does.

**Match roots** under view root `P` are the paths strictly under `P` where the predicate holds while it holds on no strict ancestor below `P` — on each chain, the first `pos` path, unless `neg` already holds there. If `P` itself matches, `P` is the single match root.

**NOT subtracts.** Under each match root `r`, the **excluded paths** are the outermost descendants where `neg` holds. `r`'s total is its own total minus theirs (scoped by the page's owner/class axes like any aggregate), every node between `r` and an excluded path is likewise net of the excluded paths under it, and the excluded paths and their subtrees are not drawn — so the treemap's `(other)` fold (P − Σ kept) is computed on the net values with no new arithmetic; an excluded direct child also leaves its parent's child count. `/api/subtree` returns them as `excluded`; `matched` is net. A filtered diff subtracts each side's own excluded paths, including from the point lookups it makes on the other side.

**Lemma (what the last segment of a path that becomes true must satisfy).** Let `p` be a path where a monotone term set becomes true (a match root: some alternative's AND holds at `p` but not at `parent(p)`; an excluded path: some NOT term), `s` its last segment. Some term of that set holds at `p` but not at `parent(p)` (a prefix of `p`'s string), so its occurrence touches `s`:

- a slash-free term (with or without `*`, which never matches `/`): the occurrence lies inside `s` — `s` contains the term (for `*`: matches it as an unanchored `[^/]*` pattern);
- a term with `/`: the occurrence's last `/` is the separator before `s` (no `*` can supply it), so `s` **starts with** the part after the term's last `/` (pieces joined by `[^/]*`, anchored). A term ending in `/` constrains `s` not at all (every child of a dir named `…t` qualifies); the index does not serve it (§5).

So every match root's — and every excluded path's — last segment is in a **candidate name set** computable from the query alone: for match roots the union of the positive terms' candidates (AND is a union here, the exact filter does the rest), for excluded paths the NOT terms'. The index maps a query to that set, the set to rows, and the rows' full paths are tested with the very predicate `parseQuery` returns — results are exact by construction, never "close".

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

One row per (trigram, name) with the trigram in the name: `tri` int32 (`c0<<16 | c1<<8 | c2`), `id` int32, sorted `(tri, id)`. Trigrams are taken from DuckDB `lower(name)` and **only all-ASCII trigrams are kept**; the reader likewise extracts only all-ASCII trigrams from the (lowercased) query. That keeps the index sound across case-mapping differences between DuckDB, Python and JS (`'İ'` lowercases to `i` in DuckDB, `i̇` in JS; `'K'` (Kelvin) to `k` in both): an ASCII run in a name stays the same ASCII run under any of them, and extra non-ASCII trigrams are never looked up.

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

### 3.1 Measured: r2 (2026-10-02)

The r2 demo's daily ingest builds the sidecars since `cloud` 968d3b5 (`path-index -S`). Over a 1.24M-row `path` sort (12.3 MiB, 152 groups), `write_search` took 0.7 s at 604 MB peak RSS (locally, on the 2026-10-01 sort). It wrote names 4.2 MiB (222K names, 109 groups), trigrams 16.5 MiB (6.0M postings, 368 groups) and the directory 0.02 MiB: 20.7 MiB, about 1.7× the `path` sort. The whole `path-index -S` step on GHA took ~8 s.

Filter latency on r2.rbw.sh, comparing the 2026-10-01 generation (no sidecars) with 2026-10-02 (sidecars), same deploy. Each figure is the median `server-timing` total of 7 cold requests (random `minArea`); `m` is the number of matches.

| `q` | view | no index | index |
|---|---|---|---|
| `gbfs` | root, `depth=1` | 137 ms, m=0 | 63 ms, m=1 |
| `gbfs` | root, `full=1` | 469 ms, m=1 | 167 ms, m=1 |
| `tripdata` | root, `depth=1` / `full=1` | 48 ms, m=0 | 65 ms, m=1 |
| `2019` | root, `depth=1` | 53 ms, m=0 | 193 ms, m=185 |
| `2019` | root, `full=1` | 73 ms, m=16 | 70 ms, m=185 |
| `2019` | `ctbk`, `depth=1` | 52 ms, m=0 | 64 ms, m=179 |
| no match | any | 42–49 ms | 31–37 ms |

The non-index path misses matches: it found 0 for `tripdata` at the root and 0 or 16 of `2019`'s 185. The index finds them all, at the same latency or better, except `2019` at the root (193 ms). There the trigrams are unselective (369 candidates) and the 185 matches cost an extra group read.

## 4. How the Worker answers a query

### 4.1 Plan (pure; `searchQuery.ts`)

`planPositive(query)` and `planNegative(query)` return `null` (the index can't serve it → today's path) or an OR of **branches**, one per term (`termBranch`), each a trigram formula plus an exact name test:

- a term's literal pieces (split at `*`) give `AND(tri(piece))` over all pieces — the `*` itself constrains nothing;
- slash-free: test `name.toLowerCase()` contains the term (a `[^/]*`-joined pattern when it has `*`);
- with `/`: the same over the part after the last `/`, anchored at the name's start; an empty part (the term ends in `/`) → `null`.

`planPositive` is `null` for the regex fallback and when an alternative has no positive term (then `pos` holds at `P` itself, and `P` is the match root — no search needed); `planNegative` is `null` without NOT terms. Formulas simplify by absorption (`ckpt | ckpts` = `ckpt`).

A formula of `TRUE` (a term under 3 characters, or only wildcards and short pieces) is still served — by the names scan of §4.3, not trigrams.

### 4.2 Candidates

1. **Directory**: open `search.parquet` (footer, then the directory groups whose `file`/`k_min`/`k_max` stats admit each needed trigram).
2. **Postings**: a trigram whose postings span more than `triRgs` (8) row groups (> ~128k names) is unselective and treated as `TRUE` — sound, it only widens the candidate set; an AND keeps its selective children, an OR with an unselective child is `TRUE`. The selective trigrams' postings groups are range-read and intersected/unioned per the formula. A query whose formula ends up `TRUE` uses the names scan instead.
3. **Names** (verification): candidate ids ascending (= heaviest first) → their names row groups (directory `file=0`, by `k` range), at most `namesRgs` (32) of them; each candidate name is tested with its branch's exact name test. The **names scan** (formula `TRUE`) reads names groups `0, 1, …` up to the same budget and tests every name — "the heaviest names that match".

### 4.3 Rows, roots, guardrails

4. **Rows**: the verified names' `rgs`, unioned in id order until `pathRgs` (32) `path` groups; a NULL `rgs` (over `rg_cap`) or the budget running out stops the union and marks the result **truncated**. Those groups are read through the existing `path`-sort machinery (D1 / `.groups.parquet` / blob `rg_json`, the per-isolate decoded-group LRU) and filtered to rows strictly under `P` whose full path passes the query predicate.
5. **Roots**: each kept row's root is its **shallowest matching prefix** below `P` (computed from the path string). Not truncated ⇒ every root's own rows were read (by the lemma, its name is a candidate and all its rows' groups are in its `rgs`), so the roots and their aggregates come straight from the rows — the same set `matchRoots` returns over all rows. Truncated ⇒ the roots are those of every matching row in the groups read (lighter names' included, when they share a group with a heavier one), and a lifted root may be unread: its rows are fetched with one `readAsks` point lookup (≤ `liftGroups` = 60 groups; wider → those roots are dropped, still flagged truncated). The lookup also covers the one non-truncated way a root could go unread — DuckDB's and JS's lowercase tables disagreeing on a name — instead of failing the request. A search **cut before it found any root** (its heaviest name alone over budget) says nothing about the query, so the view falls back to the thresholded read of §5 rather than report "no matches".
6. **Excluded paths (NOT)**: the same search with the negative part's predicate and `planNegative`, under `P`, keeping the outermost hits that lie under a match root; their rows give the aggregates to subtract (§1). A cut NOT search marks the view truncated. Without the index they come from the rows phase 1 read, and in both cases any further outermost `neg` path among phase 2's rows is subtracted too (only reachable without the index).
7. **Phase 2** is unchanged: the roots' subtrees at one forest threshold (from the net matched total), attenuated per root, from `bysize`/`path` (`readSubtree`), rows at or under an excluded path skipped and every kept node net of the excluded paths under it.

So a request reads, cold: the directory footer (~150 KB, cached per colo and isolate), a few directory groups (~25 KB each), the selective trigrams' postings groups (~40 KB each, ≤ 8 per trigram), ≤ 32 names groups (~25 KB), ≤ 32 `path` groups (~150 KB at 8K rows) — a few MB in the worst case, ~1 MB for a selective query — and never an unbounded read. **No match is cheap**: an empty candidate set, not truncated, returns "no matches" without touching the store's sorts.

### 4.4 Ordering, response

`/api/subtree`'s `matched` (the per-root list behind bulk actions and the series) is ordered **bytes desc, then path**, whichever way the roots were found; `matches` stays path-sorted, and `/api/diff`'s `matched` stays the path-sorted union of both sides. `truncated` is true when the search was budget-cut (or as before, the node cap). `tier` reads `search+<sort>` when the index found the roots, and `Server-Timing` gains `search;desc="<mode> c<candidates> n<names groups> g<path groups>[ cut]"`.

## 5. Fallback

The filter view uses the thresholded phase 1 (and finds excluded paths among the rows it read) when: the generation has no `search.parquet` (every generation written before this, all v1 scans), a term ends in `/`, or the query is a `/…/` regex. The regex keeps its full-path meaning (`/ckpt.*final/` matches across segments) and stays undocumented; the index never plans it. A user lens without claims also keeps the old read, and a lens view with claims filters after the read (`nameFilter`), which matches by the predicate but does not subtract excluded paths (open).

## 6. Cross-scan reuse (later)

Vocabulary churn is ~0.1–0.2 %/scan (gcs, [path-store] §4.7), so the names' text and the trigram postings — ~90 % of the bytes — barely change between scans; only `n`, `b` and `rgs` (row-group ordinals of that scan's `path` sort) are per scan. A later layout: an append-only cross-scan vocabulary (stable ids, postings over it, sealed in K-scan groups like the store archive) plus a small per-scan `id → (n, b, rgs)` table. Cost: ids stop being an impact order (first-seen instead), so the budgeted prefix stops meaning "heaviest"; a per-scan rank column restores it for the names scan, not for postings. Not built.

## 7. As implemented, and what is open

- **Writer** — `src/disk_tree/find/search.py` `write_search(path_sort, *, con, names_rg_rows, postings_rg_rows, rg_cap, dir_rg_rows)` (local files; sets `preserve_insertion_order` for its `COPY`s, which `viz` turns off). `dt_cloud.index.write_sorts(search=True)` runs it on the cut `path` sort and reports `{names, postings, files}` as the `path` entry's `search` (and `write_index`'s summary `search`); `dt-cloud path-index -S` / `index-write -S` (off by default). The generation dir's upload carries the files (r2's `daily-ingest.yml` copies the dir recursively; it does not pass `-S` yet).
- **Syntax** — `site/functions/_lib/pathQuery.ts` (`parsePathQuery` → `{alts, neg, regex}`, `parseQuery` → the predicate carrying `q`, `query`, `pos`, `neg`), re-exported by `scope.ts` and the client's `src/filterTree.ts`; `App.tsx`'s filter box shows the syntax as its placeholder and explainer. No filter parser exists on the `dt-cloud`/engine side (the local engine's `filter` CLI left `cloud` in `360be7d`); `@rdub/treemap`'s `parseQuery` is the widget library's own display filter and is unchanged.
- **Reader** — `site/functions/_lib/searchQuery.ts` (`termBranch`, `planTerms`, `planPositive`, `planNegative`, `trigrams`, formula `and`/`or` with absorption), `search.ts` (`openSearch`, `searchRoots(env, h, pred, plan, root, limits)`, `SEARCH_LIMITS`), `index.ts` exports for it (`readGroupsAt` by ordinal, `readFooterBytes`, `cachedRange`, the group LRU, the handle's `dir`). `view.ts` `readView`'s filter branch runs whenever the root doesn't match or the query has NOT terms: phase 1 by `searchRoots` on a store generation without a lens (`ViewOpts.searchLimits` overrides the budgets), the old reads otherwise; then the excluded paths and the net totals (§1, §4.3); `buildDiff` gets both through `readView` on each side and nets its point lookups by `Read.excl`.
- **Tests** — `tests/test_search.py` (exact names table incl. `rgs` per row group, the full postings list, directory rows and key-value metadata, statistics only on the pruning columns, `rg_cap`, the Kelvin sign and a non-ASCII trigram, rejects); `cloud/tests/test_index.py::test_write_index_search`; `site/functions/_lib/pathQuery.test.ts` (parse tables for every syntax row, predicate tables over a path list, the regex fallback); `searchQuery.test.ts` (formulas and name tests per term shape, declined plans, NOT branches, absorption); `search.test.ts` over `fixtures/v2-search/` (`gen.py` `write_v2_search`: 2 buckets, 6,047 rows, 3 `path` groups, 6,044 names, 24,199 postings, 2 directory rows per group): roots = `matchRoots` over every row for 24 queries (substrings, AND, `*`, quoted, `/` terms, AND with NOT) × 5 roots, the roots' rows exact, the blob-served copy equal, stage counts for `ttl`, the names scan, no `path` read on no match, each budget's cut (incl. a lifted ancestor), whole-view equality with the pre-index read at threshold 0, a sub-budget match only the index finds, `matched` order, the fallback on a cut-before-any-root, and a filtered diff; NOT: `tmp -ckpt`'s net totals, `excluded` and drawn tree exactly, only-negatives, index vs pre-index views equal for 8 NOT queries × 3 roots at threshold 0, an excluded path below the pixel budget only the index subtracts, and a filtered diff with NOT.

Open, needing a production build/measurement:

- build wall/peak/bytes on gcs's Batch class (2.4B-row postings sort) and cw; whether DuckDB's default encodings are acceptable for `id` (`DELTA_BINARY_PACKED` via pyarrow would be ~2× smaller);
- the deployments' jobs passing `-S` and uploading the three files with the generation (r2 first: CI, small, public);
- request cost on real queries (`ttl`, `ckpt`, `checkpoints`, `.safetensors`, a 2-char needle): directory/postings/names/`path` groups read, CPU of the `path` group decode (phase 1 decodes whole groups; a `path`-column-only first pass is a possible saving), and the budgets above;
- the `dirs`-only or bytes-coverage-cut vocabulary (§3) if the size is unwelcome.

Decisions for the user:

- **Per-alternative NOT.** Negatives are the whole query's (`a -x|b` = (a OR b) AND NOT x). Scoping them to their alternative would make the predicate non-monotone along a path (`b` could hold again below an `x` that ended `a`), which the root/exclusion model doesn't express.
- **Vocabulary scope** (§3): all names (exact for every query, ~7–11 GB/scan est.) vs dirs only (~1 GB, but object-name queries fall back).
- **`-S` default**: off until measured; on for r2 first?

[diff-and-search]: diff-and-search.md
[path-store]: path-store.md
