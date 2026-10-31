# Memory and large-query optimization plan

This is the working plan for keeping PetScan's memory below the pod limit while
preserving its main feature: intersecting very large source results down to a
small final list. It supersedes the interim safety limits added on
`fix/oom-result-bounds`.

## Context

`max_result_entries` is currently applied per **source** (and at SQL level via
`LIMIT max+1`), not only to the final result. A query such as "5M category
members that are also in this 10-item SPARQL result" therefore fails before the
intersection can shrink it. That is a product regression, not a safety
requirement.

The 2026-10-31 OOM showed RSS jumping from ~200 MB to 3.7 GB in under a minute
while completed queries in the same window had only a few thousand results. The
unbounded allocations were source row sets and post-processing growth, not the
final list.

Measured pod budget on Toolforge (October 2026):

| resource | value |
|---|---|
| CPU | 2 cores (limit), 1 core requested |
| memory | 4 GiB limit |
| `/tmp` | node overlay, disk-backed, ~91 GB free when checked (shared with all pods on the node) |
| concurrency | 50 requests, 8 sources, 5 DB batches per source |

## Recommendation (in order)

### 1. Push intersections down (implemented first)

For `big SQL source ∩ small list`, run the small side first and run the SQL
source **seeded** with that side's keys. Never materialize the big side.

- `10 SPARQL items ∩ 5M category members` becomes `cl_from IN (<10 ids>)`:
  milliseconds, no spill, final list only.
- Existing machinery to build on: `DsdbParams::seed`, `Filter::seed`,
  `category_batch_sql(..., seed)`, `get_pages_pagelist`, and the smaller-side
  heuristic in `subtract_negative_categories`.
- Plan the combination *before* running sources; add a `run_seeded` path to
  `DataSource` (default: run then intersect, so correctness never depends on the
  optimisation). Only same-wiki, at-most-`MAX_SEED_PAGES` seeds take the
  optimised path; everything else falls back.
- This is the only option that is both memory-free *and* faster than today.
- It covers intersections with `SourceDatabase` (categories/templates/links)
  and `SourceWikidata`. Unions and `big − big` still need option 2.

### 2. Key-only sorted-run spill (next)

For `∩`/`−` that cannot be pushed down, and for unions that are later reduced:

- Spill `(namespace, title)` keys only, not full `PageListEntry`s. Collect
  ~250k keys, sort+dedup, append a run file; repeat; merge runs for big×big.
- Probe with the small side for `big ∩ small`; k-way merge for big×big.
- Hydrate only the final small result (existing `load_missing_page_metadata`
  does this).
- ~200–300 MB disk per 5M keys, ~10–20 MB RAM buffer, a few seconds marginal
  cost. Keep `max_result_entries` for the final list; add
  `spill_threshold`/`spill_budget_mb` for intermediates and a global disk
  semaphore.

### 3. Full disk-backed `PageList` (only if needed)

Store whole entries (metadata + extras) on disk (redb/LMDB/SQLite) and spill
above ~250k. This is the most general option but also the most expensive:
fallible `PageList` API across ~25 methods, random read-modify-writes during
enrichment, and the final sort/render still materializes unless output is
streamed. Justified only if large lists routinely survive to output; metrics do
not show that yet.

### 4. Streaming output (last, as agreed)

Output is the last full-size copy. CSV/TSV/JSONL/plain can stream entry by
entry; JSON needs a streaming writer; HTML is already capped at 10k rows.
Streaming does not bound the set; pair it with external sort if large final
results are ever allowed.

## Rejected options

- **Temporary tables on the wiki replicas:** replicas are read-only, the
  privilege is not part of the documented access, and one source plus its
  filters can span clusters (`core`, `links.commonswiki`,
  `termstore.wikidatawiki`), so a single temp table cannot serve the joins.
- **Temporary tables on ToolsDB:** a different server from the replicas, so no
  cross-server joins; uploading millions of rows over TCP is slower than local
  disk and burdens shared infrastructure.
- **Local SQLite temp DB:** viable but strictly more machinery than sorted-run
  spill for the set operations we need; revisit only for relational joins across
  sources.
- **Cross-query disk cache of source results:** invalidation, disk growth and
  privacy (manual lists) make it a separate product decision.

## Admission control and the interim LIMITs

- Keep the 50-request admission limit; it bounds concurrent DB load and disk
  use. Seeded queries are cheap and need no special treatment.
- Keep `LIMIT max+1` for **unseeded** source queries: a bare source or union
  whose final result would exceed the cap should fail early rather than
  materialize.
- Seeded targets are bounded by the seed, so their `LIMIT` never bites. Do not
  remove it; it remains a safety net for a failed seed resolution.
- A `big − nearly-as-big` query is the one case where the source LIMIT is
  wrong. That needs option 2 (spill), not a cap.

## Metrics to watch

- `petscan_result_entries` (final list size) and
  `petscan_query_peak_rss_bytes` (process RSS during a query).
- `petscan_queries_total` / latency by format, before and after seeding.
- Disk usage under `/tmp` and query failures caused by the disk budget.

## Branch history

- `fix/oom-result-bounds`: interim bounds (source `LIMIT`s, subpage/redlink
  caps, post-processing backstop).
- `feat/seed-intersections`: step 1, stacked on the above.
