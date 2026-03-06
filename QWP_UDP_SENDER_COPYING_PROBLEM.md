# QwpUdpSender Copying Problem

## Scope

This document focuses on one issue only:

`QwpUdpSender` still moves row data through too many representations before the
datagram leaves the process.

This note reflects the current state after scatter-send, native array staging,
symbol-hit allocation reduction, and direct-write row commit have landed.

## Current State

Five important improvements are now in place.

### 1. Common-Path Row Commit No Longer Copies

The sender no longer stages the common-path row in a separate native row buffer
and then copies it into `QwpTableBuffer` on commit.

The normal path now writes directly into `QwpTableBuffer` while the row is in
progress. Commit is just:

- `nextRow(...)`
- estimate bookkeeping
- optional packet send later

That removes the former staged-row -> committed-column copy from the hot path.

### 2. Rare Replay Replaced The Old Hot-Path Replay

There is still a replay buffer, but it is no longer the steady-state owner of
the in-progress row.

It is now used only for rare cases:

- MTU overflow when committed rows must flush before the current row can fit
- schema widening after committed rows already exist in the current datagram

That is a much smaller and more acceptable use of replay than before.

### 3. No More Final Contiguous Datagram Copy For Raw Blocks

The UDP path now supports scatter-send.

`QwpUdpSender` no longer has to materialize the entire datagram into one final
contiguous `NativeBufferWriter` before sending. Instead it sends:

- a small header buffer
- payload segments gathered from the encoder

For column data that is already in wire-compatible layout and is emitted via
`putBlockOfBytes()`, the committed-data path is now much better.

Examples include:

- fixed-width primitive blocks already stored in native column buffers
- null bitmaps
- string offset and string data blocks
- other already-contiguous native blocks emitted directly by the writer

### 4. No More Object-Heavy `rowJournal`

The sender no longer keeps replayable row state in `ObjList<ColumnEntry>`.

When replay is required, the row is now staged in native memory:

- fixed-size native entry metadata
- native UTF-8 var-data for string-like values
- native array payloads for staged array values

This is still a real improvement over the old Java-object journal, especially
for UTF-8 strings and arrays.

### 5. Failed Rows Now Roll Back To Committed State

The sender now consistently rolls back to the last committed state when row
staging or row finalization fails.

That includes:

- failed column staging
- failed designated-timestamp staging inside `at(...)`
- failed `commitCurrentRow()` / `atNow()`

This is mainly a correctness improvement, not a copying optimization, but it is
part of the current steady-state design and validation story.

## What Improved

The old design had two major user-space copy problems:

1. rollback and replay of in-progress rows
2. copying committed column data into one final contiguous UDP buffer

The current implementation removes both of those from the hot path.

That means the sender is no longer in the worst possible shape. For committed
data, the design is materially closer to a QuestDB-style native-buffer send
path.

Failed-row recovery is also materially better now: rows that fail during staging
or finalization no longer rely on the caller to manually clean up partially
staged sender state.

## What Still Copies

The main copying problem is no longer common-path commit.

What remains is concentrated in rare replay paths and in encode-time
transformations for types whose storage layout is not already wire-compatible.

### Rare Overflow / Schema-Flush Replay Still Copies A Row

When a row must survive a flush boundary, the sender still snapshots it into a
replay buffer and then re-appends it into `QwpTableBuffer`.

That path is no longer on every row, but it still exists for:

- MTU overflow handling
- schema widening after committed rows already exist

So the remaining structural copy is now exceptional rather than steady-state.

### Strings Improved, Symbols Improved Partially

Strings stage as native UTF-8 and commit into `QwpTableBuffer` via raw UTF-8
append helpers instead of round-tripping through a temporary Java `String`.

Symbols are better than before, but not fully there yet.

They still stage as native UTF-8, but commit no longer allocates a fresh
`String` on symbol-dictionary hits. Instead the UTF-8 bytes are decoded into a
reusable UTF-16 sink for local-dictionary lookup, and a `String` is allocated
only on dictionary miss when a new symbol must be inserted.

So:

- string commit no longer needs a temporary Java `String`
- symbol commit no longer needs a temporary Java `String` on hits
- symbol misses still allocate once because the local dictionary is still
  `String` based

### Arrays Improved, And Now Follow The Direct-Write Model Too

Array values no longer sit in Java sidecar objects while a row is staged.

They now snapshot as native payload bytes when replay is required, but the
common path writes them directly into `QwpTableBuffer`.

The `LongArray` wrapper path is also supported again, so arrays are no longer a
special-case gap in the staging model.

The remaining array cost is the same rare replay cost as other row data:

- `QwpTableBuffer` current row
- replay snapshot buffer only when a flush boundary must be crossed

### Some Column Types Still Need Transform Encoding

Even after scatter-send, not every column can be sent directly from stored
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

That is acceptable as an intermediate state. It is no longer the first issue to
fix on the sender side.

## Updated Smell Test: Single `long`

The single-`long` path is still the right test.

### Old Path

- caller -> `ColumnBuffer`
- caller -> journal
- journal -> `ColumnBuffer` again after replay
- `ColumnBuffer` -> final contiguous datagram buffer
- final datagram buffer -> kernel

### Current Path

- caller -> `ColumnBuffer`
- `ColumnBuffer` -> kernel

### Ideal Path

- caller -> sender-owned native row or committed buffer
- kernel

So the sender has improved substantially, but the write path still is not down
to one clear owner plus the unavoidable kernel copy.

## Root Cause

The remaining problem is now narrower than before:

`QwpUdpSender` now uses `QwpTableBuffer` as the common-path row owner, but it
still needs a second replay representation when a row must survive a send
boundary before commit.

The current design has:

- committed table state in `QwpTableBuffer`
- in-progress row state directly in `QwpTableBuffer`
- replay row state in native staging only for rare flush-boundary cases
- scratch encode buffers only for header or transformed columns

It also has explicit rollback paths that restore the sender to committed state
when staging or finalization fails.

That is a much better split than before. The hot-path commit copy is gone.
What remains is the rare cross-flush replay path.

## Bottom Line

The sender is no longer doing the worst back-and-forth copying that motivated
this note in the first place.

The current design is now good enough to say:

- committed raw-copyable column data is no longer the main problem
- MTU handling no longer depends on replay

The main remaining issue is this:

- rows that must cross a flush boundary still need snapshot-and-replay

That is the next place to push.

## Next Steps

### 1. Reduce Or Eliminate Rare Replay Paths

The better long-term model is now:

- append into `QwpTableBuffer` once
- flush committed prefixes without snapshotting the current row
- cancel by rewinding row-local cursors only

That would remove the remaining overflow/schema-flush replay copy.

### 2. Keep Scatter-Send For Committed Raw Blocks

The scatter-send path should stay. It is already the right transport shape for
UDP.

Future work should build on it, not fall back to a single final contiguous
datagram buffer.

### 3. Keep Shrinking Encode-Time Transform Work

The remaining unavoidable copy work is increasingly concentrated in:

- booleans
- symbols
- decimals
- geohashes
- array encoding

Those should now be treated as the next optimization layer, after the rare
replay path.

### 3. Revisit Remaining Symbol And Transformed-Type Costs

Symbols are better than before, but the local dictionary still requires:

- UTF-8 -> UTF-16 decode for lookup
- `String` allocation on symbol miss

After the ownership model is improved further, revisit whether symbol handling
should also move closer to a wire-ready or UTF-8-native representation.

### 4. Revisit Other Transformed Types After The Ownership Fixes

Once arrays and symbols are cleaned up, decide case by case whether some
transformed column types should also move closer to wire-ready storage.

That is a second-phase optimization, not the first one.

## Validation Status

The current state described here is the one validated after:

- rollback/replay removal
- scatter-send for UDP payloads
- native row staging for scalars, decimals, timestamps, and UTF-8 strings
- native row staging for arrays
- rollback on failed staging and failed row finalization
- restored `LongArray` wrapper support
- reduced symbol commit allocation on dictionary hits

Relevant test coverage includes:

- `QwpUdpSenderTest`
- `LineSenderBuilderUdpTest`
- `QwpUdpInsertTest`
- `QwpUdpAllTypesTest`
- `QwpTableBufferTest`

Focused coverage now also includes:

- irregular-array staging failure does not leak schema into the same table
- `LongArray` wrapper staging snapshots mutation correctly
- direct `addSymbolUtf8()` reuses existing dictionary entries without growing the dictionary
- direct `addSymbolUtf8()` rollback/cancel rewinds symbol dictionary correctly
- direct `addSymbolUtf8()` rejects malformed UTF-8
- repeated UTF-8 symbols round-trip through `QwpUdpSender`
- `atNow()` oversize failure rolls back without explicit `cancelRow()`
- `at(..., MICROS)` oversize failure does not leak designated-timestamp state
- `at(..., NANOS)` oversize failure does not leak designated-timestamp state
