# Serverless reference: complete the CF-Functions demo + codify it in IaC

The public demo (r2.rbw.sh) is the **reference deployment** other checkpoints /
projects copy. It is already a serverless (SL) FE+BE — Cloudflare Pages + Pages
Functions over an R2 bucket of reduced scans — *not* a static site (the
`capabilities.static` flag is a misnomer for "the SL cloud deployment"). This
spec makes it the *complete, reproducible* reference on two axes:

1. **Capability parity within the SL envelope** — port the read-only endpoints
   the SL deployment still lacks, so the demo is capable, not a subset. Explicitly
   *not* a move to a serverful (SF) backend: SL edge-reads with row-group
   pushdown are faster and scale to zero; an always-on / scale-from-zero SF
   backend is slower for reads, costs at idle (or janks on 0→1 wakeup), and
   diverges from the SL fleet (mgu/cw-s3/marin).
2. **Infrastructure as code (Pulumi)** — every cloud resource the demo needs,
   provisioned reproducibly, so the reference is `pulumi up`-able.

## Why SL, not SF (settled)

The read path is range-GET reads of immutable parquet blobs with `(depth,
path-prefix)` row-group pruning (`ui/cfn/parquet.ts`). That is intrinsically
edge-friendly and scale-to-zero. The only things an SF backend would "unlock"
are (a) live scan/mutation — which a *public* demo must not expose anyway, and
(b) whole-blob computes — which fit the SL envelope with column projection +
sidecar indices (below). SF cold-start (Cloud Run / Fly min-instances 0) is
too janky for interactive use; Python Workers (Pyodide) are worse. So: stay SL.

## Capability inventory — Flask (all on) vs SL (`functions/api/capabilities.ts`)

| capability | Flask | SL | class | notes |
| --- | --- | --- | --- | --- |
| base reads (`scans`/`scan`/`history`) | ✓ | ✓ | done | `ui/cfn/parquet.ts` pushdown |
| `compare` | ✓ | ✓ | done | Phase 3 (`public-diff-demo.md`) |
| `scan`, `progress` | ✓ | ✗ | batch-by-design | live indexing + SSE → GHA cron; never public |
| `delete` | ✓ | ✗ | off-by-design | mutation on public data |
| `reveal` | ✓ | ✗ | local-only | "reveal in Finder" — no meaning over R2 |
| `filesystem` | ✓ | ✗ | local-only | browse the host FS (`/file/*`) |
| `backend` | ✓ | ✗ | local-only | storage-backend admin switcher |
| `s3` | ✓ | ✗ | batch-by-design | *live* bucket lister (a scanner op) |
| `histogram` | ✓ | ✗ | **PORTABLE GAP** | whole-blob read; envelope risk |
| `filter` | ✓ | ✗ | **PORTABLE GAP** | whole-tree re-agg; vocab sidecar bounds it |
| `preview` | ✓ | ✗ | **PORTABLE GAP** | source-object bytes; cross-account awkward |
| `library` | ✓ | ✗ | evaluate | snapshot-library switcher |

The demo-relevant gap is exactly **`histogram`, `filter`, `preview`** (+ maybe
`library`). Everything else is batch-by-design or local-only — leave off.

## The SL envelope (what the ports must fit)

A Pages Function is a Worker isolate: **~128 MB memory**, a **CPU-time cap** per
request, **no subprocess** (so no `gfind`/`aws` — scanning stays batch), **JS/WASM
only** (so the read API is TS in `ui/cfn`, not the Python — a bounded, mostly-paid
duplication; don't fight it with Pyodide), R2 **bindings are same-account**, and a
**subrequest cap** on outbound `fetch` (R2 binding reads are cheap and don't count;
cross-account S3 fetches do).

Per-endpoint fit:

- **`histogram`** — loads *every* descendant file row (no depth pushdown). The
  memory risk. Mitigation: project only `(mtime, size, depth)` and stream row
  groups without materializing the whole table (~920K × a few numbers ≈ tens of
  MB). **Validate against 128 MB at ctbk scale before committing** — this is the
  one endpoint that might not fit.
- **`filter`** — worst-case whole-tree. The Flask fix already exists: the **vocab
  sidecar** (`<blob>.vocab.parquet`, segment→row-group index, `disk-tree vocab`)
  turns it into a row-group-pruned lookup that fits. Port the *vocab-accelerated*
  path (not the brute one) → the indexer/rescan must **build + upload the vocab
  sidecar** alongside each blob (today `--extents`/vocab are local-only, skipped
  for remote blobs — that skip must lift for vocab).
- **`preview`** — needs the *source* object's bytes. R2 binding can't reach the
  HCCS account. Options: (a) SigV4-fetch the source S3 endpoint with a key held
  as a Worker secret (`aws4fetch`); (b) serve via the source bucket's own
  public/custom-domain URL. (a) is general; note each range GET is a subrequest.
  Lowest priority — a targeted feature, not the default read path.

`/api/filter/stream` (iterative-deepening SSE) ports too — Workers stream
responses; only the per-snapshot compute must fit the envelope.

## IaC / Pulumi

Codify every cloud resource so the reference reproduces from code. The CF
provider is instantiated **per account**:

- **RAC account** (personal): the `disk-tree-demo` R2 bucket; the Pages project
  (`disk-tree-demo`) + `SCANS` binding + env (`SCANS_PREFIX`, `PUBLIC_OPEN`); the
  `r2.rbw.sh` custom domain + DNS; (for `preview`) the source-account key as a
  Worker secret.
- **HCCS account**: the source R2 buckets (`ctbk`, `nj-crashes`, `jc-taxes`,
  `path`, `hbt`) once migrated (user handling the moves).

The scheduled-scan workflow (`.github/workflows/rescan-demo.yml`) + repo
secrets/vars can be Pulumi-managed (github provider) or left manual — **leave
secrets manual initially** (accounts-side, needs the user; see `public-diff-demo.md`
for the R2 token + per-bucket `profile:` topology this depends on).

## Phases

1. **`filter` (SL)** — port the vocab-accelerated path; lift the remote-blob skip
   so `index`/`reduce --to` build + upload `<blob>.vocab.parquet`. Flip
   `filter: true`. Highest value (search over public data), cleanly bounded.
2. **`histogram` (SL)** — column-projected streaming port; **validate vs 128 MB**
   at ctbk scale first. Flip `histogram: true` only if it fits.
3. **IaC / Pulumi** — the resource graph above; independent of 1–2, do in parallel.
4. **`preview` (SL, optional)** — cross-account source-object fetch (SigV4 +
   Worker secret). Lowest priority; decide if drill-to-content is wanted.

## Open decisions

1. **`histogram` fit** — does column-projected streaming stay under 128 MB at
   ctbk scale? Measure before promising the capability. If not, it stays Flask-only
   (acceptable — it's the least-central demo feature).
2. **`preview` mechanism** — SigV4-from-Worker vs source-bucket public URL, or
   skip entirely. Tie to the cross-account credential story (`public-diff-demo.md`
   Phase 4).
3. **`library`** — is the snapshot-library switcher meaningful on the demo, or
   local/file-tree-only? Evaluate before porting.
4. **Pulumi ownership** — does IaC also manage GHA secrets/vars (github
   provider), or only cloud resources? Default: cloud resources only, secrets
   manual.
