# Covering index in Parquet form

Design spec. Branch `feat/covering-index-parquet`, worktree `~/claude/wt/pidx-parquet`,
based on `origin/master` @ `12c2934052`.

## Goal

When a partition is converted to Parquet, store its covering POSTING indexes as
Parquet artifacts rather than as native mmap sidecars, so that the index can later
participate in cold storage, replication and pull-back from S3 alongside
`data.parquet` — while the query engine prunes it effectively.

Cold storage upload, replication and pull-back are **not** built here. This spec
delivers the format and the engine integration that make them possible.

## Background: what exists today

### Native covering index files

Per indexed SYMBOL column, per partition directory:

| file | content |
| --- | --- |
| `<col>.pk` | double-buffered 4 KB seqlock header pages (A@0, B@4096) plus an append-only chain of immutable seal entries from offset 8192. Entry header carries `SEAL_TXN`, `VALUE_MEM_SIZE`, `MAX_VALUE`, `KEY_COUNT`, `GEN_COUNT`, a generation directory, and a per-cover-column end-offset footer. `V2_ENTRY_OFFSET_COVERING_FORMAT` (byte 44) is reserved and currently always 0 |
| `<col>.pv.{sealTxn}` | sealed postings. Stride-indexed at `DENSE_STRIDE = 256` keys per stride; each stride independently picks delta-FoR-bitpacked or flat-FoR, with an Elias-Fano variant marked by `EF_FORMAT_SENTINEL` |
| `<col>.pci` | cover-info sidecar: `COVER_INFO_MAGIC` (`"PCI1"`) plus the covered columns' writer indices |
| `<col>.pc{n}.{postingTxn}.{coveredTxn}.{sealTxn}` | covered column values in posting order, ALP-compressed for floats and FoR-bitpacked for integers by `CoveringCompressor` |

The companion document `core/src/main/java/io/questdb/cairo/idx/POSTING_INDEX_CHAIN_DESIGN.md`
describes the chain in detail.

### Behaviour on Parquet partitions

`switchNativePartitionWithParquet` hard-links the native index files verbatim into
the new Parquet partition directory (`TableWriter:3733` →
`linkPartitionIndexFiles`, `TableWriter:8236`). The index therefore already works
on Parquet partitions, but only as native files.

**There is no append path into a Parquet partition.** `TableWriter:5285` sets
`inOrderMinTimestamp = Long.MAX_VALUE` when the last partition is Parquet, and
`TableWriter:10083` gates append mode on `!isParquet`. Every row landing in a
Parquet partition is routed through `O3PartitionJob`, which chooses
(`O3PartitionJob:259`):

- **update mode** — new row groups appended to `data.parquet` in place, new row
  group blocks appended to `_pm`, `PARQUET_META_FILE_SIZE` patched last as the
  commit signal;
- **rewrite mode** — a whole new partition directory named by txn with a fresh
  `data.parquet` and `_pm`, the old directory queued for removal. Forced by schema
  change, legacy `Required` no-sentinel columns, `rowGroupCount == 1`, a
  coalescable dedup boundary tie, or dead bytes over
  `o3RewriteUnusedRatio` / `o3RewriteUnusedMaxBytes`.

In **both** modes the covering index is rebuilt **wholesale**:
`indexParquetColumn` calls `discardForRebuild()` (`TableWriter:7977`) and then
re-decodes every row group of the partition. A one-row O3 insert into a 50M-row
Parquet partition re-decodes and re-indexes all 50M rows.

Consequently the index has no incremental-update semantics to preserve. What it
needs is: a complete new version published atomically with the txn, readers pinned
to the old txn still seeing the old version, and the old version reclaimed once no
reader needs it.

### Why native sidecars block cold storage

`ObjectStoreParquetDispatcher.scheduleUpload` ships exactly one artifact per
partition — `data.parquet`. The `_pm` sidecar (`docs/parquet-metadata.md`) exists
so that column chunks can be byte-range-fetched from cold storage without reading
the Parquet footer. The index sidecars have no equivalent: they are seqlock/mmap
structures with a filename-embedded MVCC scheme, so they cannot be range-read, and
are not part of the replication stream.

### Query path that must keep working

`CoveringIndexRecordCursorFactory` answers a query entirely from `.pv` and `.pc*`
when every selected column is either the indexed symbol or in the `INCLUDE` list —
single-key, bind-variable, multi-key `IN`, and `LATEST ON` variants. It is
constructed at four sites in `SqlCodeGenerator`.

`CoveredColumnDecoder` is the single source of truth for the covered byte layout
and drives both the worker-side decode in `PageFrameMemoryPool` and the eager
multi-key frame path in the factory.

## Design

### Artifact set

Per Parquet partition, per indexed SYMBOL column:

```
<table>/<partition>.<nameTxn>/
  data.parquet              unchanged
  _pm                       unchanged, plus one new footer feature section
  <col>.pidx.parquet        the covering index, as Parquet
  <col>.pidx._im            byte-range and key-directory sidecar
```

No `.pk`, `.pv`, `.pci` or `.pc*` in a Parquet partition.

`_im` mirrors `_pm`'s contract exactly: `IM_FILE_SIZE` at offset 0 patched last as
the commit signal, a CRC over everything after it, a trailer carrying
`FOOTER_LENGTH`, and callers never using the filesystem's reported length as a
commit boundary.

### Version tokens

`_im` owns the index's *commit signal and crash safety* — `IM_FILE_SIZE` patched
last, CRC, trailer. It does not invent its own version numbering: the version
*token* that a reader matches against its snapshot is always anchored externally,
in `_txn` or in the `_pm` footer. Three regimes cover every way the index can
change:

| change | what moves | version token |
| --- | --- | --- |
| O3 rewrite mode | new partition dir, new name txn | partition name txn in `_txn` (already exists) |
| O3 update mode | `data.parquet` mutated in place, `_pm` grows a footer, index rebuilt | parquet file size (`_txn` field 3); index token rides in the new `_pm` footer |
| index-only change (`ADD`/`DROP INDEX`, `INCLUDE` change) | nothing in `data.parquet` | **force a new partition dir**, hard-linking `data.parquet` and `_pm` |

The third regime is load-bearing. If an index-only change merely appended a `_pm`
footer, the parquet file size would be unchanged, so two footers would derive the
same MVCC token; a reader pinned to the old snapshot walks to the newest matching
footer and would silently observe the new index. Forcing a new directory removes
the ambiguity, and is cheap — `switchNativePartitionWithParquet` already hard-links
`data.parquet` and `_pm` this way (`TableWriter:3719`).

The `_pm` addition is a **footer** feature bit. Footer bits 0–31 are optional, so
existing readers ignore it. The section holds per-indexed-column
`(column_id, im_file_size)`, and is written in all three regimes — not only the
update-mode one — so a reader always resolves the index version through the same
mechanism regardless of how the partition arrived at its current state.

Write order, crash-safe at every step:

```
write <col>.pidx.parquet
  -> write <col>.pidx._im, patch IM_FILE_SIZE last
    -> append _pm footer carrying im_file_size, patch PARQUET_META_FILE_SIZE last
      -> commit _txn
```

A crash at any point leaves an orphan index version that no committed `_pm` footer
references; it is reclaimed by the same GC pass that handles orphan partition
directories.

### Index file layout

Key-major. Row-group boundaries are **key-aligned with a size target**: a group
accumulates consecutive keys until it reaches the target row count and then closes
on a key boundary; a key larger than the target occupies consecutive dedicated
groups. A key is never split across a *shared* group.

```
<col>.pidx.parquet
 [ RG0  keys 0..11_402        packed, all small ]
 [ RG1  key 11_403            hot key, part 1   ]  \  contiguous ->
 [ RG2  key 11_403            hot key, part 2   ]  /  one ranged GET
 [ RG3  keys 11_404..24_881   packed            ]
 ...
 footer: sorting_columns = (key_id ASC, row_id ASC)
         bloom filter on key_id
         ColumnIndex / OffsetIndex written
```

This makes three pieces of ordinary Parquet machinery per-key:

1. **Dictionaries stop being polluted.** Covered values are dictionary/RLE encoded
   per column chunk. In a mixed group, reading one key's values still requires the
   group's dictionary page, built from every key in the group.
2. **Min/max stats become per-key**, which is what makes covered-column pruning
   (`WHERE sym = 'A' AND price > 100`) possible at all.
3. **`row_id` min/max becomes per-key.** Row id is monotone in timestamp within a
   partition, so a hot key split across groups gets real time-range pruning.

The size target is what keeps the footer bounded at high symbol cardinality.
Strict one-group-per-key at 110k symbols averaging 9 rows each would produce
110k row groups; at roughly 500 B of thrift per row group for a 3-column index
that is ~55 MB of footer to index ~2 MB of payload. The size target degrades the
layout gracefully from key-pure (low cardinality, hot keys) to stride-like packing
(high cardinality) — the same amortisation `DENSE_STRIDE = 256` performs in the
native format.

`_im` carries, per row group: `key_id` min/max, `row_id` min/max, and per-column
byte ranges — plus the exact `key_id -> [rg_lo .. rg_hi]` directory.

### Row addressing

Postings are **flat partition-local row ids**, exactly as today, and `_im`
additionally carries the `data.parquet` row-group boundary array so translation
from row id to data row group is a cached in-memory binary search rather than a
remote metadata read.

This was chosen over row-group-relative `(rg_idx, offset)` addressing. Relative
offsets would give direct row-group addressing and would be the only layout in
which a future incremental index append is expressible, but they fragment a key's
postings and covered values across as many runs as it touches row groups, and they
rewrite row-id semantics across every cursor. The encoding saving is modest: under
Elias-Fano the universe shrinks from partition scale to row-group scale, roughly
11 bits per posting down to 9. Flat ids plus a cached boundary table reach the same
pruning quality with one contiguous byte range per key and no cursor changes, and
keep the payload semantically identical to the native form — which makes the native
index a free correctness oracle.

### Two payload shapes, chosen by measurement

Arm N puts one row per posting; arm B puts one row per key. Parquet allows one
schema per file, so these are two file shapes selected by a format code in `_im`.
Both are built; the choice is made on measured size and query latency, not
argument.

```
Arm N ("Parquet-native")  -- one row per posting, ~partitionRowCount rows
   key_id  INT32   RLE_DICTIONARY        long runs; near-free in key-pure groups
   row_id  INT64   DELTA_BINARY_PACKED
   inc_0.. covered columns, native Parquet types, dict/RLE + stats + bloom

Arm B ("native encodings preserved") -- one row per key, ~keyCount rows
   key_id    INT32
   row_ids   BYTE_ARRAY   opaque: existing EF / flat-FoR / delta-FoR stride blob
   inc_0..   BYTE_ARRAY   opaque: existing CoveringCompressor ALP/FoR block
```

Arm N is self-describing, externally readable, and gets per-key min/max on covered
columns — the payoff of key-aligned row groups. Arm B reuses every existing encoder
and decoder unchanged and is byte-exactly today's payload, but Parquet sees only
opaque bytes and can prune nothing inside them.

### Write path

The existing rebuild already does most of the work. `indexParquetColumn` decodes
every row group of `data.parquet`, accumulates covered column data into mmap'd temp
files in row order, then seals. Only the seal changes:

```
decode all row groups -> temp mmaps (row order)      unchanged
build per-key posting lists in memory                unchanged
                    |
                    v  NEW: instead of sealing .pv/.pc*
for key k ascending:
    emit postings for k, gathering covered values by row id from the temp mmaps
    if current row group >= target size, flush at this key boundary
write footer (sorting_columns, stats, bloom, ColumnIndex/OffsetIndex)
write _im (directory, zone maps, byte ranges), patch IM_FILE_SIZE last
```

The scatter-gather from row-ordered temp mmaps into posting order is not new cost:
`.pc` files are already in posting order, so the same access pattern happens today.

`discardForRebuild()` and the seal/purge machinery drop out on the Parquet path.
A previous index version dies with its partition directory or its superseded `_pm`
footer, so `PostingSealPurgeJob` has nothing to do there.

Note that `master` has three `discardForRebuild()` call sites
(`TableWriter:7977`, `:13909`, `:13981`); all rebuild entry points that can target
a Parquet partition must route to the new seal.

### Read path

`IndexFactory.createReader` (`IndexFactory.java:41`) is the single dispatch point;
it already switches on index type and direction. Parquet partitions get
`ParquetPostingIndexFwdReader` / `ParquetPostingIndexBwdReader` implementing the
existing `IndexReader` contract, so nothing above the seam changes.

Two contract details fit better than expected:

- `getCursor(key, minValue, maxValue, int[] requiredCoverColumns)` already exists
  (`IndexReader.java:91`). `requiredCoverColumns` maps directly onto Parquet column
  projection, so only the chunks a query needs are decoded. This is strictly better
  than `.pc`, where each covered column is a separate whole-file read.
- `minValue`/`maxValue` is already the row-id range an interval scan derives from a
  timestamp predicate, so time-range pruning has a hook with no planner change.

`CoveredColumnDecoder` remains the single source of truth for the covered byte
layout; the Parquet cursor feeds it exactly as `AbstractPostingIndexReader`'s
cursors do, leaving `PageFrameMemoryPool` and the eager multi-key frame path
untouched.

### Pruning

```
1. _im directory        key -> [rg_lo .. rg_hi]                exact, no remote read
2. row-group zone map   row_id min/max vs [minValue, maxValue] skips row groups
3. Parquet ColumnIndex  row_id sorted ASC within a key         skips pages
4. covered col min/max  WHERE sym='A' AND price > 100          skips row groups
```

Levels 1–3 come with the layout and need no planner work. Level 3 is worth writing
`ColumnIndex`/`OffsetIndex` for, since it reduces a hot key under a narrow time
filter to a few pages rather than a whole row group.

**Level 4 is enabled but not delivered here.** Per-key covered-column statistics
are the headline benefit of key-aligned row groups, but nothing today pushes a
general filter into the covering index scan — only `latestByFilter` exists. Level 4
requires planner work and is scoped as a follow-up.

Both the `_im` directory and standard Parquet statistics are written. The file is
therefore externally prunable, and the standard-stats path serves as an independent
oracle for the directory fast path in tests.

### Parquet to native conversion

`convertPartitionParquetToNative` calls `restoreIndexFilesAfterParquetToNative`
(`TableWriter:1882`, defined at `:13436`), which prefers hard-linking the native
index files out of the Parquet partition directory and falls back to
`rebuildColumnIndex` when the key file is absent (`:13460`–`:13466`).

Because this design removes native index files from Parquet partition directories,
the link branch will never fire and the fallback rebuild becomes the only path.
The fallback is complete — `rebuildColumnIndex` calls `configureCoveringIfNeeded`
(`TableWriter:12662`) so it rebuilds `.pci`/`.pc*` as well as `.pv` — but it is
strictly more expensive than a link. This is a deliberate, stated regression in
`parquet -> native` conversion cost, and it must be covered by a test that exercises
the fallback directly rather than incidentally.

### Interface audit (resolved)

`IndexReader` exposes five mmap-oriented methods that a Parquet-backed reader
cannot answer meaningfully: `getKeyBaseAddress`, `getValueBaseAddress`,
`getKeyMemorySize`, `getValueMemorySize`, `getValueBlockCapacity`. The audit found
exactly two production callers, and **no interface split is required**:

- `LatestByAllIndexedRecordCursor` passes them to the native
  `GeoHashNative.latestByAndFilterPrefix` scan. Its factory is gated on
  `IndexType.BITMAP` at both construction sites (`SqlCodeGenerator:7100` and
  `:11555`), so a POSTING index never reaches it. `AbstractPostingIndexReader`
  already returns `0` from `getValueBlockCapacity` (`:576`), confirming the path is
  bitmap-only today.
- `TouchTableFunctionFactory` uses them to warm pages for `touch_table()`. Its
  `touchMemory` already guards `baseAddress == 0` (`:148`), so a Parquet reader
  returning `0` degrades the call to a no-op.

The Parquet reader therefore returns `0` from all five. A test must pin that
`touch_table()` on a Parquet-backed covering index succeeds and reports zero index
pages, so the degradation stays intentional rather than becoming an accident.

### Known implementation constraint

`PartitionEncoder.createStreamingParquetWriter` takes a fixed `rowGroupSize`
(`PartitionEncoder.java:60`) and the Rust side flushes on that row count. There is
no "flush row group now" primitive. Key-aligned boundaries require a new native
entry point in `core/rust/qdbr/src/parquet_write/`. This is Rust work, not just
Java.

## Testing

The native index is an exact oracle. A Parquet partition never carries both forms
at once — the differential tests build the *same source data* twice, once as a
native partition and once converted to Parquet, and compare the two readers' output
key by key.

- **Differential.** For every key, both directions, and a grid of
  `[minValue, maxValue]` ranges, assert the Parquet cursor emits an identical row-id
  sequence and identical covered values to the native cursor. Extend
  `PostingIndexOracleTest`.
- **Negative controls, not just green runs.** Perturb a row-group `row_id` max, a
  `_im` directory entry, and a covered value, and assert each differential test
  *fails*. A differential test that cannot fail proves nothing.
- **Fast path against slow path.** Both the `_im` directory and standard Parquet
  statistics are written; assert they select the same row groups.
- **Fault injection.** An `_im` analogue of `CoveringIndexSidecarFaultTest`: torn
  `IM_FILE_SIZE` patch, truncated footer, bad CRC, and a crash injected between each
  of the four write-order steps.
- **Fuzz.** Extend `FuzzAddCoveringIndexOperation` to run against Parquet partitions
  with O3 in both update and rewrite mode.
- **Conversion.** `parquet -> native` exercising the `restoreIndexFilesAfterParquetToNative`
  rebuild fallback directly.
- **`touch_table()`** on a Parquet-backed covering index.
- **Bake-off harness.** Arm N against arm B on size and query latency, over both a
  low-cardinality hot-key shape and a 110k-symbol shape.

## Scope

**In scope:** the on-disk format, `_im`, the `_pm` footer section, the write path,
the Parquet-backed `IndexReader`, pruning levels 1–3, `parquet -> native`, and the
two-arm bake-off.

**Out of scope, enabled but not built:** cold storage upload, replication, S3
pull-back, filter pushdown (pruning level 4), incremental index append.

## Risks

1. **Rust row-group flush primitive.** No key-aligned boundaries without it; it
   gates the whole layout.
2. **Write amplification.** Index rebuild is already wholesale on every commit
   touching a Parquet partition. Making it a *Parquet* write raises the cost of a
   one-row O3 insert into a large Parquet partition. This must be measured. A bad
   result is the strongest argument for revisiting row-group-major addressing, which
   is the only layout in which incremental index append is expressible.
3. **`parquet -> native` conversion slows down**, since the link fast path
   disappears and the rebuild fallback becomes the only path.
4. **Storage duplication.** The index duplicates covered column data. Already true
   of `.pc`, but now in a file that is also destined for upload.
