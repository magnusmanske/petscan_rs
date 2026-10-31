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

### 2. Key-only sorted-run spill (implemented second)

For `∩`/`−` that cannot be pushed down, and for unions that are later reduced:

- Spill `(namespace, title)` keys only, not full `PageListEntry`s. Collect
  ~250k keys, sort+dedup, append a run file; repeat; merge runs for big×big.
- Probe with the small side for `big ∩ small`; k-way merge for big×big.
- Hydrate only the final small result (existing `load_missing_page_metadata`
  does this).
- ~200–300 MB disk per 5M keys, ~10–20 MB RAM buffer, a few seconds marginal
  cost. Keep `max_result_entries` for the final list. The threshold is
  `PETSCAN_SPILL_THRESHOLD` (default 250k). A global disk budget is deferred;
  step 3 raises the key-only source cap, so a single keyed source can reach
  ~500 MB of runs. With the 50-request admission limit the `/tmp` share (~91
  GB) still has headroom, but a semaphore should come with any further raise.

Implemented in `src/key_set.rs` and `src/platform/combine.rs`:

- `KeySet` stays a `HashSet` up to `PETSCAN_SPILL_THRESHOLD` (default 250k)
  and then writes sorted, deduplicated runs under its own temp directory,
  which is removed on drop.
- The combination planner marks every value that only supplies membership
  (the subtracted/intersected-away side, and unions nested in such a side).
  Those sources are converted to keys and dropped before the larger side is
  filtered; the retained side keeps its entries and metadata.
- Intersection/difference filter the retained list run by run; key×key
  operations (nested unions) merge sorted runs. The final result is always a
  normal `PageList`; no entry is ever reconstructed from a key.
- Small operands never touch disk, so ordinary queries keep the old fast path.

**Scope limit (addressed by step 3):** sources still materialise their own
result in step 2, and the peak of a query is reached while that result is
built. Step 3 moves the spill into the sources that only feed membership, so
their rows never become a page list.

### 3. Stream the membership-only source into the disk store (implemented third)

Measurements of step 2 showed that the peak is the source fetch: spilling a
source after it has built a page list leaves peak RSS unchanged. Step 3 moves
the spill into the source itself.

- `DataSource::run_keyed` is the key-only counterpart of `run`. The platform
  decides which sources are key-only from the combination plan, before running
  anything, and calls `run_keyed` for them.
- `SourceDatabase` streams MySQL rows with `exec_stream` straight into a
  `KeySet`. Neither the row vector nor a `PageList` is built. This covers
  category members, template transclusions, links and the other base queries.
- `SourceSparql` parses its temp-file CSV into keys instead of a page list;
  `SourceWikidata` streams item IDs from its SQL query the same way.
- Other sources fall back to `run` + `KeySet::from_pagelist`, which is correct
  but does not reduce memory (they are small or already capped).
- Key-only sources may now hold up to `MAX_SPILLED_SOURCE_KEYS` (10M) instead
  of `max_result_entries`, because a following set operation can reduce them.
  The **final** result is still capped, and RAM stays bounded by the spill
  threshold. The cap is disk/CPU safety, not a product limit.
- Fallbacks to the materialised path: negative categories, deferred filters
  (they need the whole ID set) and a pagelist primary.

A full-entry disk-backed `PageList` (the earlier version of this step) was
**not** built. It would need a fallible API across ~25 methods and random
read-modify-writes during enrichment, and without source streaming it would
not lower the measured peak anyway. Streaming the membership-only side covers
where the large intermediates actually occur.

Remaining scope: the **retained** side of an intersection/difference still
materialises. `big ∩ small` is handled by step 1's seed push-down.

#### Negative seed for `big − small` (investigated, not implemented)

A negative seed would run the small side first and add `page_id NOT IN (…)`
to the retained side's SQL. It is **not worth the complexity**:

- If the subtracted side is small, `A − B ≈ A`, so the result is still large.
  Excluding a few thousand IDs neither lowers the peak meaningfully nor brings
  the result under the cap; it can only rescue a narrow case where `A` is just
  over the cap and `B` covers the excess.
- If `B` is large enough to make `A − B` small, `B` has too many IDs for one
  `NOT IN` list (placeholder/packet limits), and the replicas are read-only so
  a join table cannot carry them. Chunking millions of IDs makes the query
  slower than the in-memory difference.
- The supported small-side exclusion (`negcats`) already uses the cheaper of
  the two sides in `subtract_negative_categories`.

The real lever for a large retained side is a full-entry disk store, only
triggered if metrics show large retained results surviving to the cap. Until
then, a `big − small` result is large by definition and hits the final cap
anyway.

### 4. Streaming output (implemented last)

Output used to be the last full-size copy: the result list was formatted into
one `String` (and, for JSON, first into a `serde_json::Value` tree).

- `Platform::get_streaming_response` sorts the result as before, then hands the
  pages to a renderer that emits chunks. The webserver sends them with
  `Body::from_stream`; the admission-control permit travels with the stream so
  a slow download still counts as a running request.
- Streamed formats: CSV/TSV, plain, wikitext, JSON and JSONL. HTML (already
  capped at 10k rows), KML, PagePile and pretty JSON stay buffered.
- Chunks are ~64 KB. Results below `STREAMING_THRESHOLD` (10k pages, overridable
  with `PETSCAN_STREAM_THRESHOLD`) keep the buffered path, where the task and
  chunk overhead would outweigh the allocation saved.
- The CLI writes stream chunks to stdout, so local runs and benchmarks exercise
  the same path.
- Streaming does not bound the result set; the sorted page vector is still in
  memory. Pair it with an external sort only if the final cap is ever raised.

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
- **Key-only** sources are the exception: they stream into a spilled `KeySet`
  and are capped at `MAX_SPILLED_SOURCE_KEYS` (10M) instead. The following set
  operation may reduce them; the final result is still capped, and RAM stays
  at the spill threshold. The 10M bound protects disk and CPU only.
- Seeded targets are bounded by the seed, so their `LIMIT` never bites. Do not
  remove it; it remains a safety net for a failed seed resolution.
- A `big − nearly-as-big` query is now handled by key-only streaming when the
  subtracted side is the large one. `big − small` with a large retained side
  still materialises; a negative seed would fix that.

## Measured effect of step 1 (2026-10-31)

Release build, live replicas, same query before/after:

| query | before | after |
|---|---|---|
| `American_actors` depth 3 ∩ 2 manual pages | 11.0–12.4 s, 48 MB | 6.9–7.9 s, 27 MB |
| `American_actors` depth 4 ∩ 2 manual pages | 13.2 s, 66 MB | 6.8 s, 28 MB |
| cross-wiki SPARQL ∩ `American_actors` depth 3 | 12.9 s, 50 MB | 8.0 s, 28 MB |
| `Living_people` depth 0 ∩ 2 manual pages | **failed** at the 1M cap after 85 s, 134 MB | 4.4 s, 26 MB |

Union and difference results were byte-identical before/after (24,110 and
24,109 pages), confirming the planner changes timing only. The remaining
runtime is mostly startup, category-tree enumeration and API calls; the
member fetch is what the seed removes.

## Measured effect of step 2 (2026-10-31)

Release build, live replicas, same query and output before/after:

| query | before | after | peak RSS |
|---|---|---|---|
| `manual NOT categories` (American_actors depth 3, 50,639 keys, spill forced at 1,000) | 13.2 s | 10.7–11.8 s | 49 MB → 49 MB |
| `manual NOT categories` (`templates_any=Infobox person`, 676,462 keys, spills at the default threshold) | 46–59 s | 56–57 s | 302 MB → 302 MB |
| `manual NOT sparql` (400k QIDs on wikidatawiki, spill forced at 1,000) | 10–35 s | 6–36 s | 117 MB → 117 MB |

The result set was identical in every case (`Apple`, `Apple`, `Q12345`).
RSS is unchanged because the peak is the source fetch, as noted in the scope
limit above; the time differences are dominated by replica and SPARQL-endpoint
variance. The 676k-key run writes and merges several runs at the default
threshold without a measurable slowdown, and the 50k-key run lands in the same
range whether it stays in memory or is forced to disk.

All 31 live tests pass both with the default threshold and with
`PETSCAN_SPILL_THRESHOLD=500` (forcing the larger test operands through
disk), so the new path is exercised end to end without changing results.

## Measured effect of step 3 (2026-10-31)

Release build, live replicas, same query and output before/after:

| query | before | after | peak RSS |
|---|---|---|---|
| `manual NOT categories`, `templates_any=Infobox person` (676k rows) | 44–60 s | 43–59 s | **295–304 MB → 56–60 MB** |
| `manual NOT categories`, `American_actors` depth −1 (158k rows) | 41–46 s | 43–44 s | **107 MB → 50 MB** |
| `manual NOT categories`, `Living_people` depth 0 (>1M rows) | **failed** at the 1M cap, 70 s, 134 MB | 77–89 s, returns the correct result | **55–60 MB** |
| `manual NOT sparql` (400k QIDs on wikidatawiki) | 6–35 s (endpoint variance) | 6–12 s | **117 MB → 54–62 MB** |
| seeded `American_actors` depth 3 ∩ 2 manual pages | 6.7 s, 26.7 MB | 6.5–6.9 s, 27.3 MB | unchanged |

The result set was identical in every case. The DB-backed queries show an
82% / 53% RSS drop with no consistent slowdown; the SPARQL timings are
dominated by endpoint variance (one 35 s outlier appeared in each build).
`Living_people` is the important case: the right side alone exceeds
`max_result_entries`, which used to fail before the intersection could shrink
it, and now streams in ~55 MB.

All 33 live tests pass, including new cases that exercise the streamed
category source and a 676k-row spill.

## Measured effect of step 4 (2026-10-31)

Release build, live replicas, 50,640 `American_actors` depth 3 pages, same
parsed result:

| format | buffered | streamed | peak RSS |
|---|---|---|---|
| JSON | 34.8 s | 32.6 s | **192 MB → 68 MB** |
| CSV | 34.3 s | 34.1 s | 67 MB → 67 MB |

The JSON drop is the `serde_json::Value` tree plus the joined string; streaming
serialises one entry at a time. CSV's document is only ~3 MB here and the
source/result peak already dominates, so RSS is flat; the win is time to first
byte and no second full copy of the document. Output was verified by parsing
both JSON documents and comparing all 50,640 entries (the CSV bytes differ only
where a live page changed between runs).

## Metrics to watch

- `petscan_result_entries` (final list size) and
  `petscan_query_peak_rss_bytes` (process RSS during a query).
- `petscan_queries_total` / latency by format, before and after seeding.
- Disk usage under `/tmp` and query failures caused by the disk budget.

## Branch history

- `fix/oom-result-bounds`: interim bounds (source `LIMIT`s, subpage/redlink
  caps, post-processing backstop).
- `feat/seed-intersections`: step 1, stacked on the above.
- `feat/key-spill`: step 2 (key-only spill in the combination) and step 3
  (stream each membership-only source into that store).
- `feat/stream-output`: step 4 (streamed rendering) plus the negative-seed
  investigation and its rejection.
