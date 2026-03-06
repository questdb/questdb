# QwpUdpSender Copying Problem

## Scope

This document focuses on one issue only:

`QwpUdpSender` still moves row data through too many representations before the
datagram leaves the process.

This note reflects the current state after the native row-staging slice has
landed.

## Current State

Three important improvements are now in place.

### 1. No More Rollback And Replay

The sender no longer writes the in-progress row directly into
`QwpTableBuffer` and then replays it on MTU overflow.

The row now stays staged until commit. That removed the worst old pattern:

- append into `ColumnBuffer`
- copy into journal
- truncate `ColumnBuffer`
- replay journal back into `ColumnBuffer`

That part of the design is gone.

### 2. No More Final Contiguous Datagram Copy For Raw Blocks

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

### 3. No More Object-Heavy `rowJournal`

The sender no longer keeps the in-progress row in `ObjList<ColumnEntry>`.

The row is now staged in native memory:

- fixed-size native entry metadata
- native UTF-8 var-data for string-like values
- sidecar objects only where this slice did not finish native staging yet

This is a real improvement, especially for hot fixed-width values and UTF-8
strings.

## What Improved

The old design had two major user-space copy problems:

1. rollback and replay of in-progress rows
2. copying committed column data into one final contiguous UDP buffer

The current implementation removes both of those.

That means the sender is no longer in the worst possible shape. For committed
data, the design is materially closer to a QuestDB-style native-buffer send
path.

## What Still Copies

The main copying problem is now concentrated in the handoff from staged row
state to committed column state.

### In-Progress Row Still Has Duplicate Ownership At Commit

The sender no longer stages rows in Java objects, but it still owns the same
row in two forms before send:

- staged native row state
- committed `QwpTableBuffer` column state

For a simple fixed-width `long`, the current path is now:

- caller -> native staged row
- native staged row -> `ColumnBuffer` on commit
- `ColumnBuffer` -> kernel via scatter-send

That is much better than before, but it is still not the ideal ownership model.

The copy direction is now cleaner, but the row still exists twice in
sender-owned memory before send.

### Strings Improved, Symbols Not Fully

Strings now stage as native UTF-8 and commit into `QwpTableBuffer` via raw
UTF-8 append helpers instead of round-tripping through a temporary Java
`String`.

Symbols are not fully there yet. They stage as native UTF-8, but commit still
decodes to `String` once because the local symbol dictionary is still
`CharSequence` / `String` based.

So the string path improved materially. The symbol path improved only
partially.

### Arrays Still Use Sidecar Objects

Array values are still staged with sidecar Java objects and then materialized
into `QwpTableBuffer` on commit.

That means the native row-staging slice did not yet clean up the array path.
This is now one of the clearest remaining ownership problems.

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

- caller -> native staged row
- native staged row -> `ColumnBuffer`
- `ColumnBuffer` -> kernel

### Ideal Path

- caller -> sender-owned native row or committed buffer
- kernel

So the sender has improved substantially, but the write path still is not down
to one clear owner plus the unavoidable kernel copy.

## Root Cause

The remaining problem is now narrower than before:

`QwpUdpSender` still uses a separate row-staging representation that is not the
same as the committed datagram representation.

The current design has:

- committed table state in `QwpTableBuffer`
- in-progress row state in native row staging
- scratch encode buffers only for header or transformed columns

That is a much better split than before, but it still means hot values are
first accepted into staged native state and then copied into committed column
state on commit.

## Bottom Line

The sender is no longer doing the worst back-and-forth copying that motivated
this note in the first place.

The current design is now good enough to say:

- committed raw-copyable column data is no longer the main problem
- MTU handling no longer depends on replay

The main remaining issue is this:

- the in-progress row is still not the same owning native form as committed
  column state

That is the next place to push.

## Next Steps

### 1. Replace Sidecar Array Staging With Native Array Staging

The next implementation step should stay focused on the in-progress row.

Goal:

- stop staging arrays as Java sidecar objects
- move array row state into native memory too

This is now the highest-value copy/ownership fix left in row staging.

### 2. Reduce Symbol Commit Allocation

Goal:

- avoid decoding staged UTF-8 symbols to `String` during commit if possible
- move symbol staging and dictionary interaction closer to final owned form

This is a narrower follow-up than the row-staging change, but it is now one of
the remaining hot-path allocation smells.

### 3. Move Toward Commit-By-Cursor Instead Of Commit-By-Copy

The better long-term model is still:

- append into sender-owned native state once
- commit by advancing row counters or publish cursors
- cancel by rewinding row-local cursors

That would remove the current staged-row -> `QwpTableBuffer` commit copy.

### 4. Keep Scatter-Send For Committed Raw Blocks

The scatter-send path should stay. It is already the right transport shape for
UDP.

Future work should build on it, not fall back to a single final contiguous
datagram buffer.

### 5. Revisit Transformed Types After The Ownership Fixes

Once arrays and symbols are cleaned up, decide case by case whether some
transformed column types should also move closer to wire-ready storage.

That is a second-phase optimization, not the first one.

## Validation Status

The current state described here is the one validated after:

- rollback/replay removal
- scatter-send for UDP payloads
- native row staging for scalars, decimals, timestamps, and UTF-8 strings

Relevant test coverage includes:

- `QwpUdpSenderTest`
- `LineSenderBuilderUdpTest`
- `QwpUdpInsertTest`
- `QwpUdpAllTypesTest`
