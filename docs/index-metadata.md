# Covering index metadata file (`_im`)

## Goals

- Per-index, per-partition metadata file stored alongside `<col>.pidx.parquet` in the partition
  directory, doing for the covering index exactly what `_pm` does for `data.parquet`.
- Give access (mandatory) to the index file's byte ranges, encodings and compression for each column
  chunk of each row group, so index data pulled from cold storage can be decoded **without reading the
  parquet footer**.
- Give a key directory so a single-key lookup resolves to a row-group range with no remote read.
- Give per-row-group statistics so the query planner can prune row groups by key, by row id (and hence
  by time), and by covered column value.
- Map each index column back to its QuestDB column, so a query's `requiredCoverColumns` becomes a
  parquet column projection.

The format specification for the sibling `_pm` file is `docs/parquet-metadata.md`. **`_im` deliberately
reuses `_pm`'s column descriptor, row group block and column chunk structures byte-for-byte**, and the
same implementations build and read them. Only the header and the index-specific sections differ. Where
this document says a structure is "as `_pm`", it means exactly that — do not re-specify it here.

## Relationship to `_pm`

| | `_pm` | `_im` |
| --- | --- | --- |
| describes | `data.parquet` | `<col>.pidx.parquet` |
| one per | partition | indexed column, per partition |
| commit signal | `PARQUET_META_FILE_SIZE` patched last | `IM_FILE_SIZE` patched last |
| MVCC | prev-footer chain within the file | none — each version is a complete file, versioned by the partition directory or the `_pm` footer token (see the design spec) |
| column descriptors | yes, 32 B each | yes, 32 B each, same layout |
| row group blocks | yes | yes, same layout |
| column chunks | yes, 64 B each | yes, 64 B each, same layout |
| extra | bloom filter sections | key directory, data row-group boundaries |

Because `_im` has no MVCC chain, it has no footer in `_pm`'s sense: there is exactly one set of row
group entries, they live in the index sections, and the file ends with a CRC.

## Overview

```
                  _im metadata file                          <col>.pidx.parquet
                 +==============================+             +==========================+
                 | HEADER                       |             |                          |
                 |  im_file_size ---------------+--> EOF      |  RG0  keys 0..11_402     |
                 |  im_magic  "QDBIDX\0\3"      |             |  RG1  key 11_403 pt 1    |
                 |  feature_flags               |      +----->|  RG2  key 11_403 pt 2    |
                 |  column_count                |      |      |  RG3  keys 11_404..      |
                 |  index_rg_count              |      |      +==========================+
                 |  data_rg_count               |      |
                 |  key_space_size              |      |             data.parquet
                 |  key_id_column               |      |      +==========================+
                 |  row_id_column               |      |      |  RG0 [0..500_000)        |
                 |                              |      |      |  RG1 [500_000..1M)       |
                 | COLUMN DESCRIPTORS  (32B ea) |      |      +==========================+
                 |  col 0: key_id               |      |                  ^
                 |  col 1: row_id               |      |                  |
                 |  col 2: covered `price` ...  |      |                  |
                 | NAME STRINGS                 |      |                  |
                 +------------------------------+      |                  |
                 | ROW GROUP BLOCK 0            |      |                  |
                 |  num_rows                    |      |                  |
                 |  chunk key_id:               |      |                  |
                 |    codec, encodings          |      |                  |
                 |    byte_range_start ---------+------+                  |
                 |    total_compressed          |                         |
                 |    null_count, num_values    |                         |
                 |    min_stat, max_stat        |                         |
                 |  chunk row_id: ...           |                         |
                 |  chunk price:  ...           |                         |
                 |  (out-of-line stats)         |                         |
                 +------------------------------+                         |
                 | ROW GROUP BLOCK 1 ...        |                         |
                 +------------------------------+                         |
                 | INDEX SECTIONS               |                         |
                 |  rg_block_offset[]           |                         |
                 |  rg_first_key[]  + sentinel  |                         |
                 |  rg_row_id_min[] / max[]     |                         |
                 |  data_rg_boundary[] ---------+-------------------------+
                 |  CRC32                       |
                 +==============================+
```

## Artifact naming

Each committed index version is a **complete, immutable pair of files named by an index txn**:

```
<col>.pidx.<indexTxn>.parquet
<col>.pidx.<indexTxn>._im
```

The txn is in the name because `_im` is *header*-located: `IM_FILE_SIZE`, the counts and
`INDEX_SECTIONS_OFFSET` all live in the first 128 bytes, so two versions cannot share one file the
way two `_pm` footers share one file. `_pm` gets a size-keyed MVCC chain only because it is
*trailer*-located, where a different committed size selects a different footer.

Without a txn in the name, O3 update mode — which reuses the partition directory — could not publish
a new index without destroying the one a pinned reader is entitled to open. The native form has
always solved this the same way, with `<col>.pv.{sealTxn}`.

The `_pm` footer feature section therefore carries `(column_id, index_txn, im_file_size)`. Superseded
versions are reclaimed by the same GC pass that handles orphan partition directories.

## File header (128 bytes)

| offset | size | field | type | description |
| --- | --- | --- | --- | --- |
| 0 | 8 | `IM_FILE_SIZE` | u64 | total committed `_im` size; patched last by the writer and acting as the MVCC commit signal. **Not covered by the CRC.** `0` means "not yet committed" |
| 8 | 8 | `IM_MAGIC` | u64 | `0x0300_5844_4942_4451` — the bytes `QDBIDX\0\x03`. Disambiguates `_im` from `_pm`, which carries `FEATURE_FLAGS` at this offset |
| 16 | 8 | `FEATURE_FLAGS` | u64 | bits 0-31 optional (unknown bits may be ignored), bits 32-63 required (unknown bits must cause rejection) |
| 24 | 4 | `FORMAT_VERSION` | u32 | `3` |
| 28 | 4 | `PAYLOAD_KIND` | u32 | `0` = row-per-posting, `1` = row-per-key |
| 32 | 4 | `COLUMN_COUNT` | u32 | columns in the index schema |
| 36 | 4 | `INDEX_RG_COUNT` | u32 | row groups in `<col>.pidx.<indexTxn>.parquet` |
| 40 | 4 | `DATA_RG_COUNT` | u32 | row groups in `data.parquet` |
| 44 | 4 | `KEY_SPACE_SIZE` | u32 | **exclusive upper bound on key ids**, equal to the native reader's `keyCountIncludingNulls`. Not a distinct-key count — see "Key space" below |
| 48 | 4 | `KEY_ID_COLUMN` | i32 | index of the synthetic `key_id` column in the descriptors |
| 52 | 4 | `ROW_ID_COLUMN` | i32 | index of the synthetic `row_id` column, or `-1` under `PAYLOAD_KIND = 1` |
| 56 | 8 | `INDEX_SECTIONS_OFFSET` | u64 | absolute file offset of the first index section (`RG_BLOCK_OFFSET`). 8-byte aligned |
| 64 | 8 | `PIDX_FOOTER_OFFSET` | u64 | byte offset in `<col>.pidx.<indexTxn>.parquet` where its parquet footer starts |
| 72 | 4 | `PIDX_FOOTER_LENGTH` | u32 | length of that parquet footer in bytes |
| 76 | 4 | `FIRST_COVER_COLUMN` | u32 | descriptor index of cover slot 0 — see "Cover slots" below |
| 80 | 48 | `RESERVED` | | must be 0 |

The index parquet's committed size is derived, exactly as `_pm` derives the data parquet's:
`pidx_file_size = PIDX_FOOTER_OFFSET + PIDX_FOOTER_LENGTH + 8` (4 bytes of footer length plus the
`PAR1` magic). Recording it here is what lets cold-storage upload, orphan validation and the
standard-statistics oracle path work without ever calling `ff.length()`.

`RESERVED` exists so the next field does not cost a format version. v2 filled its header exactly and
had no slack, which is part of why this is v3.

**Readers never use the filesystem's reported length to bound an `_im` read or mapping.** The on-disk
length may include bytes from an in-progress, unpublished write and is not a commit boundary; only
`IM_FILE_SIZE` is. A reader preads `IM_FILE_SIZE` at offset 0, rejects the file if the filesystem
length is smaller than that (a short read would otherwise fault on a page beyond EOF), and only then
maps exactly `IM_FILE_SIZE` bytes. Every subsequent bound is derived from `IM_FILE_SIZE`, never from
the mapping's or buffer's length — including when a caller supplies its own buffer.

## Column descriptors

`COLUMN_COUNT` descriptors of 32 bytes each, immediately after the header, **identical in layout to
`_pm`'s column descriptor** (`docs/parquet-metadata.md`, "Column descriptor"): `NAME_OFFSET`, `ID`,
`TYPE`, `FLAGS`, `FIXED_BYTE_LEN`, `NAME_LENGTH`, `PHYSICAL_TYPE`, `MAX_REP_LEVEL`, `MAX_DEF_LEVEL`,
`RESERVED`.

`_im` fixes the meaning of `ID`:

| column | `ID` | `TYPE` | notes |
| --- | --- | --- | --- |
| `key_id` | `-1` | `INT` | synthetic; located via the header's `KEY_ID_COLUMN` |
| `row_id` | `-1` | `LONG` | synthetic; located via the header's `ROW_ID_COLUMN` |
| covered column | the covered column's **writer index** | its QuestDB column type | this is the mapping a query's `requiredCoverColumns` uses to build a parquet column projection |

### Cover slots

`ID` is *not* the query-path lookup key. A query's `requiredCoverColumns` are **cover slots** —
ordinals into this index's own `INCLUDE` list, `0 .. coverCount-1`, the `n` in the native
`<col>.pc{n}` — as `AbstractPostingIndexReader.openRequiredSidecars` and every `includeIdx` on
`CoveringRowCursor` show. Writer indices and cover slots are different spaces, and confusing them
silently resolves to the wrong column or to a miss.

The mapping is therefore positional and O(1):

- Descriptor order is fixed: the synthetic columns first, then the covered columns **in cover-slot
  order**.
- `FIRST_COVER_COLUMN` in the header is the descriptor index of cover slot 0.
- `coverSlot -> descriptorIndex = FIRST_COVER_COLUMN + coverSlot`, bounds-checked against
  `COLUMN_COUNT`.

The writer validates the ordering; readers expose it as a `coverColumnIndex(slot)` accessor. `ID`
stays the writer index so the file remains meaningful to an external reader and survives
`DROP COLUMN`.

Lookup by `ID` is defined only for real columns: **a lookup with a negative `ID` is rejected**, not
matched against the synthetic columns. `key_id` and `row_id` are found through the header's
`KEY_ID_COLUMN` and `ROW_ID_COLUMN`, which is the only sanctioned way to reach them. This matches
`ParquetMetaFileReader.getColumnIndexById`, and it keeps `-1` meaning "not a table column" rather than
doubling as a lookup key that happens to match the first synthetic column.

Writer indices are used rather than positional table indices because they survive `DROP COLUMN`, which
is the same convention `data.parquet` uses for its `field_id`.

Column names follow the descriptors as a UTF-8 blob; each descriptor's `NAME_OFFSET` is an absolute
file offset and `NAME_LENGTH` its byte length. The name section is padded to an 8-byte boundary.

## Row group blocks

One block per index row group, written sequentially after the name strings, **identical in layout to
`_pm`'s row group block**: `NUM_ROWS` (u64) followed by `COLUMN_COUNT` column chunks of 64 bytes each,
followed by the out-of-line region holding min/max stats whose payload exceeds 8 bytes.

Blocks are 8-byte aligned so `RG_BLOCK_OFFSET` can store `offset >> 3` in a u32.

**`RG_BLOCK_OFFSET` entries strictly ascend, and a reader must validate that at open time.** The whole
extent model rests on it: "block `i` runs to block `i + 1`" is meaningless if the entries are unordered,
and an out-of-line bound derived from an inverted extent is meaningless too. A file whose entries do not
strictly ascend is rejected.

**A block's extent is bounded by the next block.** Block `i` runs from `RG_BLOCK_OFFSET[i]` to
`RG_BLOCK_OFFSET[i + 1]`, and the last block runs to `INDEX_SECTIONS_OFFSET`. An out-of-line stat
reference is `(offset << 16) | length` **relative to the block's out-of-line region** — that is, to
`block[8 + COLUMN_COUNT * 64 ..]`, not to the start of the block. This matches `_pm`'s wording
("a reference into the row group block's out-of-line region"); resolving it from the start of the
block instead lands `8 + COLUMN_COUNT * 64` bytes early, inside the column-chunk array, and yields a
`UUID` / `LONG256` / `VARCHAR` stat read out of chunk bytes. Nothing catches that — the address is
still inside the block — and the result is a wrong pruning decision, so row groups are silently
dropped rather than a decode failing.

Readers **must reject a reference whose `[offset, offset + length)` falls outside its own block's
out-of-line region**, which is stricter than merely inside the block's extent. Bounding it only by the end of the row-group region would
let a stat in one row group address bytes belonging to another — legal-looking, silently wrong, and
exactly the kind of cross-block read a crafted file would use.

Column chunks are `_pm`'s 64-byte structure verbatim: `CODEC`, `ENCODINGS`, `STAT_FLAGS`, `STAT_SIZES`,
`NUM_VALUES`, `BYTE_RANGE_START`, `TOTAL_COMPRESSED`, `NULL_COUNT`, `DISTINCT_COUNT`, `MIN_STAT`,
`MAX_STAT`. Stat encoding — inline for payloads of 1..=8 bytes, otherwise an `(offset << 16) | length`
reference into the block's out-of-line region — is `_pm`'s, unchanged. This matters for covered columns
of type `UUID`, `LONG256`, `VARCHAR` and friends, whose stats do not fit inline.

This is what makes an `_im` sufficient on its own: given a row group and a column, a reader has the
byte range to fetch, the codec to decompress it with, the encodings present, and the null count that
lets it skip the fetch entirely when the chunk is all-null.

## Index sections

Written after the last row group block, in this order. Each starts 8-byte aligned, and each section's
footprint is padded up to a multiple of 8 so the next one stays aligned.

### Locating them

`INDEX_SECTIONS_OFFSET` in the header points at the first section. **Readers must use it rather than
deriving it.**

Deriving it forwards is impossible: a row group block's size depends on the length of its out-of-line
stat region, which is not recorded anywhere. Deriving it backwards from the CRC is possible — every
section size follows from the header counts — but it makes the padding rule part of the read path, and
requires each subtraction to be individually overflow-checked against a hostile file. Two independent
reader implementations (Rust and Java) must agree byte-for-byte, and a stored offset is one value to
compare rather than a chain of inferences to keep in step.

Readers must still validate what they are given: `INDEX_SECTIONS_OFFSET` must be 8-byte aligned, must
lie after the column descriptors and name strings, and the sections it implies must fit within
`IM_FILE_SIZE - 4`. A file failing any of these is rejected.

| section | size | description |
| --- | --- | --- |
| `RG_BLOCK_OFFSET` | `INDEX_RG_COUNT * 4` | u32 per row group: byte offset of its block from file start, `>> 3`. **Entries strictly ascend** |
| `RG_FIRST_KEY` | `(INDEX_RG_COUNT + 1) * 4` | u32 per row group: the smallest key id present in it. Non-decreasing. The final entry is a sentinel equal to `KEY_SPACE_SIZE` |
| `RG_ROW_ID_MIN` | `INDEX_RG_COUNT * 8` | i64 per row group: smallest row id present in it |
| `RG_ROW_ID_MAX` | `INDEX_RG_COUNT * 8` | i64 per row group: largest row id present in it |
| `DATA_RG_BOUNDARY` | `(DATA_RG_COUNT + 1) * 8` | i64: cumulative row counts of `data.parquet`'s row groups. First entry `0`, non-decreasing |
| `CHECKSUM` | 4 | CRC32 over bytes `[8, CHECKSUM)` — everything after `IM_FILE_SIZE` |

### Redundancy is deliberate

Two of these sections duplicate information already present in the column chunks:

```
RG_FIRST_KEY[i]   ==  chunk(i, KEY_ID_COLUMN).MIN_STAT
RG_ROW_ID_MIN[i]  ==  chunk(i, ROW_ID_COLUMN).MIN_STAT     (PAYLOAD_KIND == 0 only)
RG_ROW_ID_MAX[i]  ==  chunk(i, ROW_ID_COLUMN).MAX_STAT     (PAYLOAD_KIND == 0 only)
```

`RG_FIRST_KEY` is kept as a dense array because key lookup binary-searches it on the hot path, and
striding 64-byte chunks for a 4-byte field is cache-hostile.

`RG_ROW_ID_MIN` / `RG_ROW_ID_MAX` are **unconditional**, and that is deliberate. Under
`PAYLOAD_KIND = 1` there is no `row_id` column at all — the row ids are an opaque blob — so a reader
that took the range from the `row_id` chunk stats would have no time pruning whatsoever for that
payload. Making the arrays conditional would also mean two code paths in every reader, and would turn
the payload bake-off into a comparison between an arm with zone maps and an arm without, which is a
difference having nothing to do with payload encoding.

**The duplication is an invariant, and it is asserted in tests**: `RG_FIRST_KEY[i]` must equal the
`key_id` chunk's `MIN_STAT` for every row group, and under `PAYLOAD_KIND = 0` the row-id arrays must
equal the `row_id` chunk's stats. This gives the fast path an independent oracle, which
is the same reason the design spec keeps both a directory and standard parquet statistics.

## Key lookup

`RG_FIRST_KEY` is non-decreasing, and a key that spans several consecutive row groups produces repeated
entries. For key `k`, the inclusive row-group range is:

- `rg_lo` = `lower_bound(RG_FIRST_KEY[0..INDEX_RG_COUNT], k)`; if that index is at the end or
  `RG_FIRST_KEY[rg_lo] != k`, use `rg_lo - 1` instead. If `rg_lo` would be `-1` — that is, `k` is below
  the first entry — the key is absent.
- `rg_hi` = `upper_bound(RG_FIRST_KEY[0..INDEX_RG_COUNT], k) - 1`.

Both searches are bounded at `INDEX_RG_COUNT`; the sentinel is never read by the search. It exists so a
consumer can derive "the keys in row group `i` are `[RG_FIRST_KEY[i], RG_FIRST_KEY[i+1])`" for the last
row group as well as the others.

`k >= KEY_SPACE_SIZE` is absent, as is any `k` below `RG_FIRST_KEY[0]`.

### Key space

`KEY_SPACE_SIZE` is the **exclusive upper bound on key ids**, equal to the native reader's
`keyCountIncludingNulls`. It is emphatically *not* a count of distinct keys present.

Posting-index keys are a dense key space with sparse occupancy: a partition holding symbols
`{5, 900, 12_000}` has three distinct keys and a key space of at least 12_001. If `KEY_SPACE_SIZE`
were written as `3`, keys 900 and 12_000 would fail the `k >= KEY_SPACE_SIZE` test, report absent,
and the query would return no rows with no error anywhere — which is precisely the failure the
writer's last-first-key check exists to prevent.

For the same reason, "the keys in row group `i` are `[RG_FIRST_KEY[i], RG_FIRST_KEY[i+1])`" is a
statement about the *key-id range* a row group may hold, not about which ids are occupied. Occupancy
is sparse, so a range containing `k` does not mean `k` is present — see "Access patterns".

Worked example, `RG_FIRST_KEY = [0, 11_403, 11_403, 11_404, KEY_SPACE_SIZE]`:

| key | `rg_lo` | `rg_hi` | why |
| --- | --- | --- | --- |
| `0` | 0 | 0 | exact match at index 0 |
| `5` | 0 | 0 | no exact match; packed inside row group 0 |
| `11_403` | 1 | 2 | exact match; spans two dedicated row groups |
| `11_404` | 3 | 3 | exact match at index 3 |
| `KEY_SPACE_SIZE` | — | — | absent |

Because row groups are key-aligned and the file is key-major, `[rg_lo, rg_hi]` is a **contiguous** run,
so a key's postings and covered values are one contiguous byte range per column — one ranged GET from
cold storage, not one per row group.

## Access patterns

### Resolving a single-key query

1. Binary-search `RG_FIRST_KEY` for the key → `[rg_lo, rg_hi]`, or absent.
2. For each required column (the indexed symbol's postings plus the query's `requiredCoverColumns`,
   mapped to column indices via the descriptors' `ID`), read `BYTE_RANGE_START` and `TOTAL_COMPRESSED`
   from that column's chunk in each block.
3. Fetch `[start, start + total_compressed)`. Contiguous row groups coalesce into one request.
4. Decompress with `CODEC`; page headers carry per-page encoding.

### Pruning by time

A timestamp predicate becomes a row-id range. Compare it against the `row_id` chunk's `MIN_STAT` /
`MAX_STAT` per row group and skip non-overlapping blocks. Row id is monotone in the designated
timestamp within a partition, so this is exact, not conservative.

### Pruning by covered column value

Because row groups are key-aligned, a covered column's `MIN_STAT` / `MAX_STAT` in a given block are that
key's range, not the whole partition's. `WHERE sym = 'A' AND price > 100` can therefore skip blocks.
This is the payoff of key-aligned row groups; the planner work to push such a filter into the covering
scan is a later phase.

### Deciding whether a key is present at all

The directory answers "which row groups *could* hold `k`", not "does `k` exist". Because the key
space is dense and occupancy sparse, a key that falls inside a packed row group's key range returns
a range whether or not it has any postings, and confirming absence costs one row-group fetch.

An exact-presence structure — an `RG_LAST_KEY` array, or a key-id bloom section — is a deliberate
follow-up behind an optional feature bit rather than part of v3. The parquet footer's own bloom
filter is not a substitute: reading it means reading the footer, which is the thing `_im` exists to
avoid.

### Skipping an all-null chunk

`NULL_COUNT == NUM_VALUES` means the chunk is entirely null; the reader materialises nulls without
fetching or decoding anything.

### Mapping a row id to a data row group

Binary-search `DATA_RG_BOUNDARY`. This is what lets an index hit name the `data.parquet` row groups a
non-covering query must read, without consulting `_pm`.

## Writer and reader order

Writer:

1. Write `<col>.pidx.parquet`.
2. Write `_im` with `IM_FILE_SIZE` left at `0`, then patch it as the last write. That patch is the
   commit signal.
3. Publish per the design spec — a new partition directory, or a new `_pm` footer carrying the `_im`
   size.

Reader:

1. `pread` `IM_FILE_SIZE` at offset 0. `0` means not yet committed: treat the index as absent.
2. Reject if the filesystem length is below `IM_FILE_SIZE` — the header must not be dereferenced before
   this check, or a short file faults on a page beyond EOF.
3. Map exactly `IM_FILE_SIZE` bytes.
4. Check `IM_MAGIC` and `FORMAT_VERSION`; reject unknown required feature bits.
5. Verify the CRC over `[8, IM_FILE_SIZE - 4)` before trusting any offset.
6. Read `INDEX_SECTIONS_OFFSET` and validate it: 8-byte aligned, and **the five sections it implies
   fit within `IM_FILE_SIZE - 4`**. That fit bound is a *precondition*, not merely one check among
   several — it is what proves the descriptor array lies inside the mapping. **No descriptor byte may
   be dereferenced before it holds.** A reader that checks the name ranges first will read past the
   end of a truncated file: that is a panic in Rust and a SIGSEGV in a reader using unchecked memory
   access, and it is a real bug this format shipped and fixed.
   Only then validate that `INDEX_SECTIONS_OFFSET` is at or after the end of the descriptors and
   after every descriptor's name range. Resolve the five sections forward from it using the header
   counts, then row group blocks via `RG_BLOCK_OFFSET`.
7. Because step 6 validates every descriptor's `NAME_OFFSET` / `NAME_LENGTH`, a bad name entry is
   rejected at open time rather than on first access. Both reader implementations must do this, or
   they disagree on which files are valid.
8. Validate the header's column selectors, which are otherwise trusted all the way to an address
   computation: `PAYLOAD_KIND` is `0` or `1`; `0 <= KEY_ID_COLUMN < COLUMN_COUNT`; and `ROW_ID_COLUMN`
   is `-1` if and only if `PAYLOAD_KIND == 1`, otherwise in range. These are the sanctioned route to the
   synthetic columns, so a caller passes them straight to a column-chunk accessor; an unvalidated value
   indexes past the mapping.

   The **writer** additionally requires both selectors to be **below `FIRST_COVER_COLUMN`**. Bounding
   them only by `COLUMN_COUNT` would let a caller name a *covered* column as `key_id` or `row_id`,
   contradicting "synthetic columns first" and making one descriptor reachable both as a synthetic
   column and as a cover slot. Readers take the weaker bound, since a reader's concern is only that
   the index is addressable.
9. Validate every `RG_BLOCK_OFFSET` entry: strictly ascending; each block starting at or after
   `128 + COLUMN_COUNT * 32` (the end of the descriptor array — note `128`, the v3 header size, not
   v2's `64`); each block ending at or before `INDEX_SECTIONS_OFFSET`; and each extent at least
   `8 + COLUMN_COUNT * 64`, the minimum a block needs for its chunks.

Two further bounds both readers enforce, stated here so a third does not omit them:

- `IM_FILE_SIZE >= 128 + 4` — the header plus the CRC trailer. Anything smaller is rejected before
  any field is read.
- The five sections are, in order: `RG_BLOCK_OFFSET`, `RG_FIRST_KEY`, `RG_ROW_ID_MIN`,
  `RG_ROW_ID_MAX`, `DATA_RG_BOUNDARY`. Omitting the two row-id sections when computing the fit bound
  places `DATA_RG_BOUNDARY` `16 * INDEX_RG_COUNT` bytes early, over the row-id minima, so every
  row-id-to-data-row-group lookup silently returns a row id instead of a boundary.

### What the reader does *not* re-check

These are writer-enforced invariants that readers deliberately trust, and a reader that additionally
enforces them would reject files the others accept:

- `RG_FIRST_KEY` non-decreasing, and its cross-check against the `key_id` chunk's `MIN_STAT`.
- `DATA_RG_BOUNDARY[0] == 0` and its monotonicity.
- `RG_ROW_ID_MIN` / `RG_ROW_ID_MAX` against the `row_id` chunk stats.
- The header's `RESERVED` bytes. They are `must be 0` for a *writer*; a reader ignores them, so that
  a later writer can spend them without a version bump.
- `FIRST_COVER_COLUMN`. It is bounds-checked at the point of use, not at open — unlike
  `KEY_ID_COLUMN` and `ROW_ID_COLUMN`, which are validated at open because they are the sanctioned
  route to the synthetic columns and reach an address computation unmediated.
- `PIDX_FOOTER_OFFSET` / `PIDX_FOOTER_LENGTH`. The writer requires them non-zero; a reader takes them
  as given.

Slack between the end of `DATA_RG_BOUNDARY` and the CRC is permitted — readers bound the sections with
`sections_end <= crc_offset`, not equality.

## Validation the writer performs

These are cheap at write time and produce silent wrong answers if violated, so the writer rejects them
rather than trusting callers:

- `RG_FIRST_KEY` non-decreasing.
- The last row group's first key `< KEY_SPACE_SIZE`. Otherwise a key physically present in the index reports
  as absent and a query silently returns no rows.
- `RG_FIRST_KEY[i] == chunk(i, KEY_ID_COLUMN).MIN_STAT` for every row group.
- `DATA_RG_BOUNDARY[0] == 0` and the array non-decreasing. Otherwise the row-id binary search maps rows
  to the wrong data row group.
- Every row group block carries exactly `COLUMN_COUNT` chunks.
- `NUM_ROWS > 0` for every block — a zero-row parquet row group is treated as corruption.
- `COLUMN_COUNT > 0`. (`PAYLOAD_KIND`, `KEY_ID_COLUMN` and `ROW_ID_COLUMN` are validated by the
  **reader** as well — see step 8 — because they reach an address computation.)
- Under `PAYLOAD_KIND = 0`, every row group's `row_id` chunk has `MIN_STAT` and `MAX_STAT` **present
  and inline**, and they equal `RG_ROW_ID_MIN[i]` / `RG_ROW_ID_MAX[i]`. The `key_id` stat already
  gets this treatment; time pruning depends on the row-id one identically.
- Covered columns occupy descriptor positions `FIRST_COVER_COLUMN ..` in cover-slot order, and
  `FIRST_COVER_COLUMN + coverCount == COLUMN_COUNT`.
- `PIDX_FOOTER_OFFSET` and `PIDX_FOOTER_LENGTH` are non-zero and describe the index parquet actually
  written.
- The `key_id` chunk's `MIN_STAT` used for the `RG_FIRST_KEY` cross-check must be **inline**. Key ids
  are 4-byte ints so this always holds in practice, but an out-of-line reference happens to be encoded
  as `(offset << 16) | length` and could otherwise collide with a small key value.

## Versioning

`FORMAT_VERSION` is `3`. Readers reject anything else.

Two interim layouts were never written to disk outside tests and are not readable:

- **v1** carried byte ranges and zone-map arrays only, with no column descriptors and no column
  chunks. It could locate index bytes but not decode them, and could not map an index column to a
  QuestDB column.
- **v2** added descriptors and chunks, but keyed the column projection on the writer index rather
  than the cover slot, defined `KEY_COUNT` as a distinct-key count rather than a key-space bound,
  recorded nothing about the index parquet's own footer, filled its 64-byte header with no slack,
  and dropped the row-id zone maps that `PAYLOAD_KIND = 1` has no other source for. Each of those
  produced a silently wrong answer rather than an error, which is why v3 exists.

Future additions go behind `FEATURE_FLAGS`: optional in bits 0-31 when an old reader can safely
ignore the section, required in bits 32-63 when it cannot. Small additions may also use the header's
`RESERVED` bytes without a version bump, provided a zero value means "absent".
