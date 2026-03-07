# QwpUdpSender Copying Problem

## Scope

This document tracks one issue only:

`QwpUdpSender` should avoid moving row data through multiple owners or
temporary representations before the datagram leaves the process.

This note reflects the current state after:

- direct-write row commit
- scatter-send for UDP payloads
- prefix flush with in-place row retention
- fast retained-column cleanup
- incremental pending-fill tracking

## Current State

Six important improvements are now in place.

### 1. Common-Path Row Commit No Longer Copies

The sender no longer stages the common-path row in a separate row buffer and
then copies it into `QwpTableBuffer` on commit.

The normal path writes directly into `QwpTableBuffer` while the row is in
progress. Commit is just:

- `nextRow(...)`
- estimate bookkeeping
- optional packet send later

That removes the former staged-row -> committed-column copy from the hot path.

### 2. MTU / Schema-Boundary Replay Has Been Removed

Rows that cross a flush boundary no longer snapshot into a replay buffer and
then replay back into `QwpTableBuffer`.

When committed rows must flush before the current row can continue, the sender
now:

- captures per-column prefix limits
- encodes and sends only the committed prefix
- compacts the current row in place inside the same `QwpTableBuffer`
- re-bases sender-side row marks to an empty-table origin

This is now used for:

- MTU overflow when `maxDatagramSize` is enabled
- schema widening while committed rows already exist in the current datagram

That removes the old cross-boundary snapshot-and-replay copy entirely.

### 3. No Final Contiguous Datagram Copy For Raw Blocks

The UDP path still uses scatter-send.

`QwpUdpSender` no longer materializes the entire datagram into one final
contiguous `NativeBufferWriter` before sending. Instead it sends:

- a small header buffer
- payload segments gathered from the encoder

For column data that is already in wire-compatible layout and is emitted via
`putBlockOfBytes()`, the committed-data path is now close to zero-copy inside
the process.

Examples include:

- fixed-width primitive blocks already stored in native column buffers
- null bitmaps
- string offset and string data blocks
- other already-contiguous native blocks emitted directly by the writer

### 4. No Replay Journal Or Native Replay Buffer Remains

The sender no longer keeps replayable row state in `ObjList<ColumnEntry>`, and
it no longer keeps a separate `NativeRowStaging` replay buffer either.

The in-progress row stays owned by `QwpTableBuffer`. The sender now keeps only
small per-column metadata needed for:

- duplicate-column detection
- datagram-size estimation
- committed-prefix encoding limits

That is a much cleaner ownership model than the earlier "row owner plus replay
owner" split.

### 5. Failed Rows Still Roll Back To Committed State

The sender still consistently rolls back to the last committed state when row
staging or row finalization fails.

That includes:

- failed column staging
- failed designated-timestamp staging inside `at(...)`
- failed `commitCurrentRow()` / `atNow()`

This is mainly a correctness improvement, not a copying optimization, but it
is part of the current steady-state design.

### 6. Omitted-Column Tracking No Longer Rescans The Whole Schema On Every Commit

The sender now maintains pending-fill columns incrementally instead of
rebuilding the "omitted columns" list by scanning the whole table on every row
commit.

This does not remove the actual `addNull()` / sentinel writes for omitted
columns, but it does remove the per-commit schema discovery scan from the hot
path.

## What Improved

The old design had three major sender-side ownership/copy problems:

1. separate staged-row ownership on the common path
2. snapshot/replay when rows crossed a flush boundary
3. final contiguous datagram materialization for raw blocks

The current implementation removes all three.

That means the sender is no longer in the shape that motivated this note in the
first place. For raw-copyable committed data, the path is now materially closer
to a QuestDB-style native-buffer send path.

Failed-row recovery is also materially better now: rows that fail during
staging or finalization no longer rely on the caller to manually clean up
partially staged sender state.

## What Still Copies

The main copying problem is no longer replay.

What remains is concentrated in wrapper-array ingestion, symbol representation,
and encode-time transformations for types whose storage layout is not already
wire-compatible.

### Wrapper Arrays Still Take One Extra Steady-State Copy

Plain Java array values write directly into `QwpTableBuffer`.

`LongArray` / `DoubleArray` wrappers still take an extra copy on the common
path today: they are first read into a temporary Java capture buffer and then
copied into committed array storage.

So the clearest remaining steady-state row-copy issue is now:

- `LongArray` / `DoubleArray` wrapper -> temporary capture arrays ->
  `QwpTableBuffer`

### Symbols Still Have Representation Costs

Symbols are better than before, but not fully there yet.

They no longer allocate a temporary `String` on dictionary hits during commit,
and the UDP flush path no longer materializes a fresh `String[]` dictionary just
to encode a packet.

However:

- lookup still decodes UTF-8 into a reusable UTF-16 sink
- dictionary misses still allocate a `String`
- symbol dictionary entries and row indexes are still encoded into scratch
  writer chunks at flush time

So symbols are improved, but they are still not wire-native.

### Some Column Types Still Need Transform Encoding

Even with scatter-send, not every column can be sent directly from stored
buffers.

Types that still require encode-time transformation are the ones whose storage
layout does not already match the wire format, for example:

- booleans
- symbols
- decimals
- geohashes
- array columns
- any future Gorilla-compressed timestamp path

Those still materialize encoded bytes into scratch buffers during flush.

That is now the main remaining copy/encode layer on the sender side.

### Omitted-Column Padding Still Writes Into Committed Storage

For columns omitted in the current row, commit still appends nulls or sentinel
values into column storage before the row becomes committed.

The expensive schema scan is gone, but the committed-column model still pays for
those writes.

That is more of a storage-model cost than a replay problem, but it is still
part of the steady-state row-finalization work.

## Updated Smell Test: Single `long`

The single-`long` path is still the right test.

### Old Path

- caller -> `ColumnBuffer`
- caller -> journal / replay state
- journal / replay state -> `ColumnBuffer`
- `ColumnBuffer` -> final contiguous datagram buffer
- final datagram buffer -> kernel

### Current Path

- caller -> `ColumnBuffer`
- `ColumnBuffer` -> kernel

For transformed columns, add:

- `ColumnBuffer` -> scratch encoder buffer
- scratch encoder buffer -> kernel

### Ideal Path

- caller -> sender-owned committed storage
- kernel

For fixed-width raw-copyable columns, the current path is already close to
ideal. The remaining gap is mostly transformed types, wrapper arrays, and
symbol representation.

## Root Cause

The original root cause was:

`QwpUdpSender` had multiple row owners and had to replay row data across flush
boundaries.

That is now mostly gone.

The current design has:

- committed table state in `QwpTableBuffer`
- in-progress row state in the same `QwpTableBuffer`
- small sender-side per-column marks for estimation, duplicate detection, and
  committed-prefix encoding limits
- scratch encode buffers only for headers or transformed columns

So the remaining problem is now narrower:

- some client-side inputs (`LongArray` / `DoubleArray`) still require an
  intermediate copy before they reach committed storage
- some committed storage layouts still do not match the wire format
- symbols still use a `String`-based local dictionary instead of a wire-ready
  representation

## Bottom Line

`QwpUdpSender` no longer has the cross-boundary snapshot/replay copy that
originally dominated this document.

The current design is now good enough to say:

- common-path commit no longer depends on replay
- flush-boundary handling no longer depends on replay
- raw-copyable committed data no longer needs a final contiguous datagram copy

The main remaining issues are now:

- wrapper-array copy on the common path
- symbol representation / miss-allocation cost
- scratch encoding for transformed column types

## Next Steps

### 1. Remove The `LongArray` / `DoubleArray` Capture Copy

Let wrapper arrays append directly into committed array storage instead of going
through `ArrayCapture` intermediary arrays.

### 2. Revisit Symbol Ownership

Push symbols closer to a UTF-8-native or wire-ready local representation so:

- lookup does not require UTF-8 -> UTF-16 decode
- misses do not require `String` allocation

### 3. Keep Shrinking Encode-Time Transform Work

The remaining unavoidable copy work is increasingly concentrated in:

- booleans
- symbols
- decimals
- geohashes
- arrays
- optional Gorilla timestamp encoding

Those should now be treated as the next optimization layer.

### 4. Revisit Omitted-Column Padding Only After The Above

If row-finalization cost still matters after the true copy issues are
addressed, evaluate whether omitted-column padding should stay eager or move
closer to an encode-time/default view model.

That is a second-phase optimization, not the first one.

## Validation Status

The current state described here is the one revalidated after:

- direct-write common-path row ownership
- scatter-send for UDP payloads
- committed-prefix flush while preserving the current row in place
- fast in-place retained-row compaction
- fast empty-column cleanup for unstaged retained columns
- removal of the replay buffer / `NativeRowStaging`
- incremental pending-fill tracking instead of per-commit schema rescans
- reduced symbol commit allocation on dictionary hits

Relevant coverage for this area includes:

- `QwpUdpSenderTest`
- `QwpTableBufferTest`
- broader `Qwp*Test` coverage in `core`

Focused coverage now also includes:

- nullable string null retention across overflow boundaries
- repeated overflow boundaries with distinct symbols
- retained nullable-string state after prefix flush
- retained symbol-dictionary compaction after prefix flush
- fast emptying of unstaged nullable columns during retained-row compaction
- wide-schema low-index writes after prior rows
- explicit `flush()` followed by more rows on the same table preserving
  pending-fill state
- oversize row failures rolling back without leaking sender state
- designated-timestamp rollback on oversize failure
