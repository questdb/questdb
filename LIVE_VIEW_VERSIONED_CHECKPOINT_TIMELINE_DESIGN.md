# Live View Versioned Checkpoint Timeline and Localized O3 Repair

**Status:** proposed; clean replacement of the unreleased checkpoint-ring design  
**Scope:** durable live-view window state, restart recovery, compact historical
checkpoints, and bounded repair after insert-only out-of-order writes  
**Integration boundary:** this document and its implementation phases cover
OSS only. Enterprise adaptation—including replication, role switch, backup and
restore, cold promotion, and replica lead reconstruction—is intentionally
deferred to a separate design and review. Replica, backup, and promotion
statements below describe an eventual integration contract, not work delivered
by this plan. Those capabilities already exist and are test-locked in enterprise
today; the deferred work ports them onto the new timeline format rather than
inventing them (§16.4). Note, however, that consuming the new OSS revision in
enterprise is not zero-touch: `EntLiveViewRefreshJob` reuses the OSS
snapshot/encoding primitives and several enterprise tests hard-depend on the
`.cp`/`_ring` surface, so a companion production/test migration is required at
the consumption point (§22).  
**Related documents:**

- `../LIVE_VIEW_REFRESH_LATENCY_HANDOFF.md`
- `../LIVE_VIEW_RESTART_CHECKPOINT_RING_HANDOFF.md`
- `../LIVE_VIEW_CHECKPOINT_ENCODING_DESIGN.md`
- `../LIVE_VIEW_LOCALIZED_O3_REPLAY_DESIGN.md`

## 1. Summary

The retained checkpoint ring solves only a short-horizon version of the O3
problem. Every checkpoint is a complete independent state image, so retaining
old checkpoints multiplies disk usage by the full live window size. Count and
byte limits eventually remove useful old anchors. An O3 row older than the
retained horizon then forces a replay from the live view's `START FROM`
boundary. On a long-lived view this work is proportional to view age.

This design replaces complete `.cp` files and `_ring` with a **versioned
checkpoint timeline**:

1. Every checkpoint event creates a permanent logical checkpoint boundary.
2. A logical checkpoint is a small root over shared immutable state pages,
   rather than a complete copy of all function state.
3. A copy-on-write timeline index finds any predecessor checkpoint in
   `O(log checkpointCount)` time.
4. All roots in one published timeline generation are valid against the same
   applied base-table snapshot.
5. An O3 change derives a finite low dependency boundary `L` and a tagged
   exclusive high influence boundary `H` (`FINITE(timestamp)` or pinned `EOF`).
   It reads only `[L, H)`, replaces durable output over `[R, H)`, and versions
   only logical roots in `[C, H)`, where `C` is the correction floor and `R`
   (§5) is the possibly-lower non-durable-output/materialization floor.
6. Roots before `C` and at or after `H` are structurally reused. A new timeline
   generation marks the entire set valid through the new base `seqTxn`, so the
   reused suffix does not need to be restamped checkpoint by checkpoint.
7. Large state streams use immutable, adaptively encoded chunks. Timestamp
   delta/delta-of-delta and exact double-XOR encoding are adopted from the
   (still unimplemented) checkpoint encoding proposal, with raw fallback. The
   current `.cp` format is version 1 and stores raw 16-byte pairs, so these
   codecs are greenfield rather than a port of existing code.
8. Authoritative metadata is checksummed. Immutable data segments are not
   checksummed, matching QuestDB's ordinary data/index integrity model.
9. Superseded physical versions are purged only after no reader can reference
   them, following the same publish-new-version-then-purge-old-version pattern
   used by QuestDB partition and column versioning.

The design deliberately rejects live-view plans without a bounded forward
influence contract. Unbounded cumulative windows without a fixed reset, and
`row_number`, `rank`, and `dense_rank` **without a bounding anchor**, therefore
do not qualify. Their anchored, segment-reset forms have a finite `H` (the
segment end) and remain eligible under the fixed-anchor path (§6.2, Phase 7). A
bounded contract may resolve to pinned EOF when the snapshot contains too few
following rows. Since live views have not been released, there is no old
checkpoint format, downgrade path, feature flag, or migration protocol to
preserve.

## 2. Problem statement

The current checkpoint representation couples three independent concerns:

- a logical recovery boundary;
- a complete serialized copy of window state; and
- membership in a small retained ring.

That coupling creates four failures.

### 2.1 Retention is size-capped rather than correctness-driven

For a bounded one-minute frame, each complete checkpoint may contain millions
of `(timestamp, value)` pairs. Once one or two checkpoints consume the byte
budget, older checkpoints are unlinked even though they are valid and could
serve a future O3 correction.

The retained ring is bounded by three knobs — `retention.count` (default 8),
`retention.max.bytes` (default 64 MiB), and `retention.micros` (event-time
horizon, default 0 = disabled). Count and bytes are the effective bounds in
practice; the event-time knob is off by default. The new timeline replaces all
three with permanent logical retention.

### 2.2 Old O3 work is bounded only by the oldest retained checkpoint

The refresh job can search the ring, but it cannot search checkpoints that have
been discarded. An O3 row before the oldest entry falls back to a replay from
`START FROM`, even when the window has a finite dependency and finite forward
influence.

### 2.3 Full snapshots repeat nearly identical data

Adjacent checkpoints of a bounded window overlap heavily. A one-minute frame
checkpointed every five seconds repeats approximately eleven-twelfths of the
same frame rows in each complete state image. Compression reduces the constant
but does not remove the repeated-copy asymptote.

### 2.4 Whole-file validation defeats localized access

The current `.cp` CRC requires touching the entire checkpoint at write and
restore time and imposes a two-GiB implementation limit. A localized repair or
lazy restore should not read unrelated state merely to validate it.

## 3. Goals

### 3.1 Correctness and retention

1. Keep every logical checkpoint boundary for the lifetime of one live-view
   definition and base-history epoch.
2. Never silently ignore an old valid checkpoint because it falls outside a
   count- or byte-limited in-memory structure.
3. Restore the newest checkpoint below an arbitrary timestamp in logarithmic
   index time.
4. Preserve exact checkpoint restore: a root restores the state bits it stores.
5. Keep runtime state, durable live-view output, timeline generation, base
   watermark, and WAL-purge floor mutually consistent across crashes.

### 3.2 O3 localization

1. Derive an inclusive correction floor `C`, a dependency/warm-up floor `L`,
   and an exclusive convergence boundary `H` from explicit function contracts.
2. Never scan below `L` or above `H` while repairing a historical O3 change.
3. Version only logical checkpoint roots whose state may have changed:
   `[C, H)`.
4. Reuse the unchanged checkpoint prefix and suffix without copying their
   payload pages.
5. Bound work per refresh turn. A large but finite repair may be resumable; it
   must not silently fall back to an age-unbounded scan.

### 3.3 Compactness and performance

1. Store a state row or chunk once when adjacent roots can share it.
2. Make checkpoint sealing proportional to state changed since the prior root,
   not total live state.
3. Use bounded-memory, lossless semantic encoding with raw fallback.
4. Keep predecessor lookup, root publication, and ordinary restore independent
   of total payload size.
5. Avoid a mandatory checksum pass over checkpoint data.

### 3.4 Operations

1. Expose logical size, physical size, sharing ratio, lookup depth, repair
   bounds, repaired-root count, and garbage-collection lag.
2. Preserve primary-only checkpoint ownership. Replicas and restored backups
   may rebuild the derived timeline.
3. Delete obsolete physical versions safely while retaining every current
   logical checkpoint.

## 4. Non-goals

- Backward or forward compatibility with the current `.cp` and `_ring` formats.
- Retaining superseded physical versions forever. The permanent object is the
  logical checkpoint boundary, not every byte sequence that ever represented
  it.
- Detecting every latent bit flip in data payloads.
- Supporting a window whose output influence has no finite high boundary.
- Making arbitrary filters or partition expressions efficiently seekable in
  the first implementation.
- Partition-selective live-view table replacement in the first implementation;
  `REPLACE_RANGE` remains timestamp-global.
- Treating base-table deletion, truncate, partition removal, TTL removal,
  schema change, or live-view definition change as insert-only localized O3.
- Promising a fixed wall-clock repair time when the bounded timestamp interval
  itself contains an arbitrarily large number of rows.

## 5. Terminology and invariants

| Name | Meaning |
|---|---|
| `S` | Resolved `START FROM` boundary. |
| `E` | Applied base-table `seqTxn` pinned for one localized-repair timeline publication. |
| `normalizedBaseSeqTxn` | Per-generation base-`seqTxn` through which every current root is validated. It is the authoritative base-transaction-inclusion boundary for recovery replay; a rebuild must not incorporate base transactions above it (see §14.1). |
| `F` | Inclusive designated-timestamp frontier through which durable live-view output is reconciled from the live-view table and `_lv.s`. `F` is a root-compatibility and output-reconciliation coordinate, **not** a base-transaction-inclusion bound: it cannot, by itself, decide which transaction version of a row belongs to the durable materialization. That role is held by the base-`seqTxn` boundary above. |
| `C` | Earliest timestamp whose existing live-view result may have changed. |
| `D` | Earliest qualifying output already incorporated in runtime state but not yet durable in the live-view table (a published lead or a rolled-back current-turn draft). Absent when all incorporated output is durable. Not to be confused with the `RANGE W PRECEDING` frame width. |
| `R` | Inclusive non-durable-output / materialization floor: `max(S, min(C, D))` when `D` exists, otherwise `max(S, C)`. Durable output is re-emitted from `R`; logical state roots are versioned only from `C`, so roots in `[R, C)` are re-emitted but not re-versioned. |
| `L` | Earliest timestamp or row position required to reconstruct state immediately before the replay floor `R`. |
| `H` | Tagged exclusive timestamp bound—`FINITE(timestamp)` or pinned `EOF`—after which every eligible function has converged. |
| `A` | Partition keys actually affected by the incorporated base changes. |
| `Q` | Partition keys whose output must be materialized in the timestamp-global replacement range `[R, H)`. |
| logical checkpoint | Permanent boundary and identity created by checkpoint cadence. |
| root version | Current state root for a logical checkpoint in one timeline generation. |
| timeline generation | One atomic mapping from all logical checkpoints to their current roots. |
| state page | Immutable encoded function-state payload. |
| metadata page | Checksummed page containing identities, bounds, page references, or catalog structure. |

The implementation must maintain these invariants:

1. A checkpoint boundary `B` represents function state after all qualifying
   rows with designated timestamp `<= B` in the canonical cursor order.
2. Every current root in timeline generation `G` is correct for the same pinned
   base snapshot `G.normalizedBaseSeqTxn`.
3. A generation is visible only after every metadata and data file it references
   has reached its final versioned name.
4. A reader pins one generation before resolving any root or page reference.
5. Files referenced by either published superblock slot or by a pinned reader
   are not deleted.
6. Insert-only advancement with `minNewTimestamp > oldHead.maxTimestamp` cannot
   affect an older root; the generation watermark may advance while those roots
   are reused.
7. An O3 publication replaces every root with `C <= B < H` and reuses every root
   outside that interval.
8. No current root points to a temporary, mutable, or retired physical object.
9. Generation normalization records snapshot validity, not a per-root recovery
   position. Restoring a root at `B` must still rebuild `(B, F]`, even when the
   generation's `normalizedBaseSeqTxn` already equals the applied base
   `seqTxn`. The rebuild is bounded by the base-`seqTxn` boundary, not by `F`:
   it incorporates exactly the base transactions through
   `normalizedBaseSeqTxn` (reconciled from `_lv.s` and the live-view table) and
   must exclude any transaction applied above that boundary, even one whose
   rows carry a timestamp at or below `F`. Timestamp `F` alone cannot identify
   which transaction version of a row belongs to the durable materialization;
   under apply-ahead the base table may already be applied past
   `normalizedBaseSeqTxn` while an O3 correction below `F` remains
   unincorporated.
10. The effective `lvRowPosition` of every logical checkpoint is correct in
    every generation. O3 replacement changes this cumulative position for
    repaired roots and for every later suffix root, even when the suffix's
    function state has converged.

The common generation watermark is essential. Without it, every unchanged
suffix root would retain an old `baseSeqTxn`; recovery would replay the same O3
transaction against an already converged root, or WAL retention would have to
remain pinned indefinitely. Generation-level normalization records that the
prefix, repaired interval, and converged suffix have all been validated against
snapshot `E` without rewriting the unchanged roots.

## 6. Eligibility and dependency contract

Snapshot support is not enough. Each function must declare both how much state
is needed before a correction and how far the correction can influence future
output.

Add a compiler-visible descriptor, conceptually:

```text
LiveViewCheckpointDependency
    kind
    partitionSignature
    orderSignature
    lowBoundStrategy
    highBoundStrategy
    supportsKeyRestore
    supportsKeyReset
    structuralConvergence
    numericConvergence
```

Initial dependency kinds are:

| Window shape | `L` | `H` | Disposition |
|---|---|---|---|
| `ROWS N PRECEDING ... CURRENT ROW` | Find at most `Nmax` qualifying predecessors for every key in `Q` | Find `Nmax` qualifying following rows for every key in `A`, extending through the final timestamp tie | Eligible |
| `RANGE W PRECEDING ... CURRENT ROW` with constant finite `W` | Saturating `R - W`, clamped to `S` (`R = C` when no non-durable floor lowers it) | After `maxChangedTimestamp + W`, including the complete upper tie | Eligible |
| Fixed compiler-derived anchor segment, including anchored `row_number`/`rank`/`dense_rank` with per-segment reset | Segment start, clamped to `S` | Segment end exclusive | Eligible (Phase 7) |
| Unbounded cumulative without a fixed reset | No finite bound | No finite `H` | Reject |
| Unanchored `row_number`, `rank`, `dense_rank` (no bounding anchor) | Historical prefix required | No finite `H` | Reject |
| `FOLLOWING`, arbitrary anchor, data-dependent frame | Function-specific/unknown | Unknown | Reject initially |

For multiple functions, the planner takes the union of affected keys, the
earliest proven `L`, and the latest proven `H`. The first rollout requires a
common designated timestamp, ordering, and partition signature. Support for
multiple compatible partition signatures can be added later by planning one
key domain per signature and taking the union of timestamp ranges.

`H` is a tagged bound, either `FINITE(timestamp)` or `EOF`; it is not a bare
`long`. `Long.MAX_VALUE` is a valid designated timestamp, so it cannot also
mean infinity. A `REPLACE_RANGE` that reaches EOF must carry an explicit
unbounded-high flag/tag through planning and WAL application rather than
encoding EOF as `Long.MAX_VALUE` or relying on `hi = maxTimestamp + 1`.

### 6.1 Floating-point convergence

Frame membership converges exactly at `H`, but an incrementally maintained
floating accumulator may retain a rounding difference after an added row has
been subtracted. The contract is:

- restored checkpoint bits are exact;
- frame contents, counts, deque structure, and non-floating state converge
  exactly;
- approved floating aggregate fields and outputs may differ within the
  documented floating-point tolerance after localized replay;
- no lossy checkpoint encoding is permitted.

This permits reuse of roots at and after `H` instead of propagating a harmless
floating-point delta through the entire future timeline. Integer, decimal, and
otherwise exact aggregates remain bit-exact. Differential tests must use exact
comparison for structural/exact fields and the approved tolerance only for the
explicit floating aggregates.

### 6.2 CREATE-time behavior

Because live views are unreleased, unsupported dependency shapes should be
rejected at `CREATE LIVE VIEW`, not accepted with an eventual full-history O3
fallback. The error must name the function/window and explain that it has no
finite O3 influence boundary. Rejected at CREATE are unbounded cumulative
aggregates without a fixed reset and **unanchored** `row_number`/`rank`/
`dense_rank`. Their anchored, segment-reset forms have a finite `H` (the segment
end) and remain eligible on the fixed-anchor path (Phase 7).

This is a deliberate, product-visible scope cut, not merely development cleanup.
The current gate accepts these unanchored ranking shapes on the common path —
`row_number()` and `OVER ()` variants — and, because eligibility is purely
`ZERO_PASS` + `supportsSnapshot()` on a non-`CachedWindow` factory with **no**
finite-influence check, they are already snapshot-capable and covered by
existing tests. (`lead()` is rejected only because it compiles to a cached
multi-pass factory, not on a finite-influence basis.) Phase 0 must therefore
confirm that no shipped fixture or customer depends on the removed unanchored
shapes before enforcing the allowlist. Do not assume all single-partition
cumulative aggregates work today: `OVER ()` unbounded is parser-accepted, but
end-to-end eligibility still turns on the concrete function's
`supportsSnapshot()`, so the allowlist must be recorded per function after
parser and snapshot validation rather than by category.

## 7. Logical checkpoint model

Each cadence event allocates a monotonic `checkpointId` and appends one logical
entry:

```text
LogicalCheckpointEntry
    checkpointId          LONG
    maxTimestamp          LONG
    createdLvSeqTxn        LONG
    baseLvRowPosition      LONG
    rootRef                META_PAGE_REF
    logicalStateBytes      LONG
```

`checkpointId` disambiguates two cadence events with the same maximum
timestamp. Search order is `(maxTimestamp, checkpointId)`. A predecessor lookup
for correction timestamp `C` returns the greatest entry whose
`maxTimestamp < C`; the strict inequality preserves complete timestamp ties.

`createdLvSeqTxn` records when the logical checkpoint boundary was first
created. It is diagnostic identity, not the watermark from which current
recovery resumes after later O3 repairs. `TimelineSuperblockSlot.coveredLvSeqTxn`
and `normalizedBaseSeqTxn` are authoritative generation-wide publication and
reconciliation coordinates. The entry's `maxTimestamp` and effective
`lvRowPosition` remain its boundary-specific recovery coordinates.

These coordinates live in three distinct spaces that must not be conflated:
base-table `seqTxn` progress (`normalizedBaseSeqTxn`), live-view-writer
`seqTxn` progress (`coveredLvSeqTxn`, advancing with live-view WAL commits), and
per-root designated timestamp plus effective `lvRowPosition`. Naming must keep
them apart, because the inherited representation does not. The current OSS `.cp`
key named `lvSeqTxn` is in fact **base-`seqTxn`-valued**: it is stamped from the
base `advanceTo`, and recovery compares it directly against the base
`appliedWatermark`. The genuinely LV-writer-space coordinate is a different
field entirely (today the in-memory tier read fence). The base-valued `lvSeqTxn`
name must be retired rather than carried into the new schema, and every new
field spelled `...LvSeqTxn` must declare whether it tracks base progress or
LV-writer progress.

`baseLvRowPosition` is interpreted through a persistent range-delta index owned
by the generation:

```text
effectiveLvRowPosition(entry, G) =
    entry.baseLvRowPosition
    + G.rowPositionDeltaIndex.prefixSum(entry.searchKey)
```

When appending a logical entry, store the observed runtime position minus the
current prefix correction so existing range deltas are not applied twice. An
O3 repair writes replay-derived positions for the `K` repaired boundaries and
adds the total output-row-count change to the unchanged suffix with one
persistent difference/range-add operation. This keeps publication
`O(log N + K)` without walking or rewriting every later checkpoint. `rootRef`
and `logicalStateBytes` are likewise generation-versioned leaf values, not
immutable properties of the logical identity.

Logical entries are not removed by count or byte retention settings. The
existing retained-ring settings and state are deleted. Definition changes and
non-localizable destructive base operations start a new history epoch; entries
from the retired epoch are obsolete rather than valid checkpoints of the new
history.

An O3 repair does not delete entries in `[C, H)`. It publishes new `rootRef`
versions for the same `checkpointId` values. The old physical roots become
garbage after reader release.

## 8. Persistent timeline index

### 8.1 Copy-on-write B+ tree

The timeline is a persistent B+ tree ordered by `(maxTimestamp, checkpointId)`.
It supports:

- predecessor and successor lookup in `O(log N)`;
- appending a new logical entry in `O(log N)` copied metadata pages;
- range iteration over roots in `[C, H)` in `O(log N + K)`;
- a bulk range splice that reuses prefix and suffix subtrees and replaces `K`
  repaired leaves;
- compaction of physical metadata without deleting logical entries.

Metadata tree pages are immutable and individually checksummed. A repair builds
new paths and a new tree root; readers of the prior generation continue through
the old paths.

### 8.2 Superblock

`_checkpoints/_timeline` contains two independently checksummed fixed-size
slots. Each slot contains only authoritative metadata:

```text
TimelineSuperblockSlot
    magic
    formatVersion
    generation
    definitionTxn
    historyEpoch
    normalizedBaseSeqTxn
    coveredLvSeqTxn
    timelineRootRef
    rowPositionDeltaRootRef
    segmentDirectoryRootRef
    nextCheckpointId
    nextSegmentId
    metadataBytes
    dataBytes
    crc32
```

Publication writes the inactive/older slot and publishes its generation last.
Startup selects the highest slot that passes *bounded* validation: its own
checksum, its root metadata pages, and the checksummed segment/completeness
catalogue it references through `segmentDirectoryRootRef` (§8.4, §16.2).
Bounded validation must not walk the whole timeline or prove that every
referenced deep tree node and data segment exists before committing to a slot;
deep tree paths and state pages are validated lazily on first access, with the
failure policy in §14.1/§14.2. The previous valid slot is a recovery fallback,
not a search through arbitrary untrusted checkpoint files.

The fallback slot and everything it references remain live until a later slot
successfully supersedes it. The base-WAL purge floor is the minimum
`normalizedBaseSeqTxn` required by the two valid slots and any in-progress
recovery. This permits recovery from the prior slot after a torn latest
publication without retaining WAL for the entire checkpoint history.

### 8.3 Why this cannot be a flat manifest

A flat `_ring`-style manifest would require rewriting or checksumming every
logical checkpoint entry on each append or O3 repair. Its publication cost
would grow with view age. The persistent tree copies only search paths and the
modified range, while unchanged subtrees remain shared.

### 8.4 Directory layout

The concrete layout is:

```text
<live-view-table>/
  _checkpoints/
    _timeline                 fixed A/B superblock
    meta/
      m.<segmentId>           immutable checksummed metadata pages
      m.<segmentId>.tmp       unpublished metadata
    data/
      d.<segmentId>           immutable checkpoint data bytes, no CRC
      d.<segmentId>.tmp       unpublished data
    repair/
      r.<repairId>            checksummed resumable-repair descriptor
```

Metadata and data segment IDs are monotonic within a history epoch and never
reused, even after purge. A repair descriptor refers only to its own temporary
segments until final publication. Startup removes unowned temporary files and
reconciles final-name orphans against the checksummed segment directory
(§16.2), not by enumerating every logical checkpoint leaf. Orphan reconciliation
is therefore proportional to the segment directory and independent of timeline
length.

## 9. State roots and immutable pages

### 9.1 Root hierarchy

Each logical checkpoint root contains:

```text
CheckpointRoot
    checkpointId
    maxTimestamp
    definitionTxn
    anchorRootRef          optional
    functionDirectoryRef
```

The function directory maps stable function identity to:

```text
FunctionRoot
    functionIdentity
    stateFormatVersion
    keySchema
    scalarStateRef         optional
    partitionMapRootRef
```

The partition map is another persistent tree or HAMT. A partition entry stores
small scalar state and references immutable state pages for rings, deques, or
other large buffers. Updating one partition path-copies only its map path and
reuses every other partition entry.

Stable function identity must be compiler-produced and independent of object
address or traversal accidents. It includes the canonical window name,
function factory/signature, output position, partition/order signature, and
state codec identity.

### 9.2 Data page references

Every reference to a data payload is held in checksummed metadata:

```text
StatePageRef
    segmentId
    offset
    storedLength
    decodedLength
    pageKind
    codec
    rowCount
    flags
```

Readers validate segment existence, exact published file length, checked
offset/length arithmetic, maximum decoded size, codec, flags, row count, and
complete decoder consumption before allocating or reading. A malformed page
invalidates that root version and schedules reconstruction of the same logical
checkpoint.

### 9.3 Immutable data segments

State pages are packed into version-named immutable files:

```text
_checkpoints/data/d.<segmentId>
```

A segment is written as `d.<segmentId>.tmp`, synced according to commit mode,
closed, and renamed before any metadata can reference it. Published segments
are never modified. Packing many pages in one segment avoids one file per
partition or chunk.

Data segment bytes do **not** carry a whole-file or per-page CRC. Integrity
comes from the same model used for ordinary table data and indexes:

- immutable publication;
- authoritative checksummed metadata;
- exact file length and bounded structural decoding;
- filesystem/storage integrity; and
- reconstruction from the base table when structural damage is detected.

A structurally valid payload bit flip may remain undetected. This is the
intentional QuestDB data-file integrity tradeoff. An optional diagnostic build
may calculate page hashes, but they are not part of the production format or
restore contract.

### 9.4 Metadata files

Versioned metadata files contain timeline nodes, roots, partition-map nodes,
page references, and segment reference counts. Metadata pages are individually
checksummed because corruption could redirect reads or select the wrong state
version. Checking one metadata page never requires scanning an unrelated data
segment.

## 10. Compact page encoding and structural sharing

### 10.1 Persistent chunks

Large ordered buffers use fixed-capacity logical chunks, initially 4096 rows.
A runtime may mutate only an unsealed tail chunk. At checkpoint time it freezes
that tail into an immutable page and starts a copy-on-write tail for subsequent
rows. A newer root:

- reuses complete chunks still inside the frame;
- advances a logical start offset into a shared head chunk when rows expire;
- copies only a partially modified tail chunk; and
- drops references to chunks no longer in its frame.

Old roots retain their own start/end offsets and continue to reference the old
chunks. A base row should normally be encoded once even though it participates
in many adjacent checkpoint frames.

Functions with monotonic deques or specialized buffers may define their own
immutable page kinds, but must obey the same freeze, reference, bounds, and
ownership contracts.

### 10.2 Semantic codecs

The first production codecs are:

- timestamp raw 64-bit;
- timestamp delta/delta-of-delta with checked ZigZag/LEB128;
- value raw 64-bit; and
- exact IEEE-754 XOR encoding using raw double bits.

Codec choice is adaptive per page. Encoded form is selected only when it saves
at least 6.25% of the raw stream with a 16-byte minimum; otherwise the writer
uses raw bytes. Decreasing timestamps, arithmetic overflow, or incompressible
data select raw form. Scratch memory is fixed by chunk size and charged to the
live-view memory tracker.

These codecs are new to `cairo/lv`. The current `.cp` format (version 1) stores
raw 16-byte pairs and implements none of them (only `LiveViewSnapshotKeyCodec`,
a fixed-width map-key codec, exists today), so this is greenfield work that
adopts the parameters — the 6.25% / 16-byte threshold and the codec set — from
the checkpoint encoding proposal rather than porting existing code.

NaN payloads, signed zero, infinities, and subnormals round-trip by raw bits in
the generic codec even when a particular function normally buffers only finite
values. Stored aggregate scalar state is restored rather than recomputed.

### 10.3 Expected storage complexity

With complete checkpoints, steady-state storage is approximately:

```text
O(checkpointCount * liveFrameRows)
```

With persistent chunks it becomes approximately:

```text
O(uniqueRowsCaptured + changedPartitionDescriptors + logicalRoots)
```

The metadata term includes persistent row-position delta nodes used to adjust
an unchanged checkpoint suffix without a linear suffix rewrite.

Keeping all logical checkpoints necessarily causes metadata and unique state
history to grow. The design does not pretend otherwise. Its objective is to
avoid multiplying every checkpoint by the complete live frame. Periodic
physical compaction may repack shared pages and tree nodes, but may not discard
logical roots.

## 11. Normal checkpoint sealing

On an in-order checkpoint cadence event:

1. Verify the processed changes are strictly above the current head boundary.
   Any overlap routes through O3 repair.
2. Ask every function to freeze its changed mutable chunks and construct a new
   function root, reusing prior pages and partition-map nodes where possible.
3. Allocate a new `checkpointId` and root at the current inclusive maximum
   timestamp.
4. Write and rename new immutable data segments.
5. Write new checksummed metadata pages and a timeline tree with one appended
   entry.
6. Set the new generation's `normalizedBaseSeqTxn` to the processed base
   watermark. Older roots are reusable because the in-order changes are above
   them.
7. Publish the inactive `_timeline` superblock slot.
8. Only after publication, advance the checkpoint head and the WAL-purge floor.
9. Enqueue metadata or data segments made unreachable from both superblock
   generations.

Checkpoint cadence still controls how densely logical roots are created, but no
retention count or byte budget removes them later.

## 12. Localized O3 repair

### 12.1 Pin and classify

The repair owns one pinned applied base reader:

1. Wait for the triggering base `seqTxn` to apply.
2. Pin the reader and set `E = reader.getSeqTxn()`.
3. Classify every base transaction after the current generation's
   `normalizedBaseSeqTxn` through `E`.
4. Reject localized repair for structural, delete, replacement, dedup, truncate,
   partition removal, TTL removal, or otherwise unclassifiable changes.
5. Compute `C` as the minimum qualifying changed timestamp, clamped to `S`.
6. Collect affected partition keys `A` and each key's first/last changed
   timestamp from the authoritative change set.

All subsequent planning and replay use this reader. Reopening against a newer
snapshot would invalidate the bounds and generation watermark.

### 12.2 Derive `H`, `Q`, and `L`

`R = max(S, min(C, D))` is the non-durable-output floor (`R = max(S, C)` when
`D` is absent). Output is materialized and replaced from `R`, so the forward
scan and warm-up below are bounded by `R`, not `C`.

For finite `RANGE`, timestamp arithmetic derives `H` directly, with saturation
and complete tie handling. A bounded forward scan over the replacement interval
`[R, H)` collects `Q`. If the inclusive influence endpoint is `Long.MAX_VALUE`,
the exclusive bound is tagged `EOF`, not an overflowing or colliding timestamp.

For finite `ROWS`:

1. Scan forward until every affected key has `Nmax` qualifying rows strictly
   after its last changed row.
2. Extend `H` past the complete timestamp tie containing the final required
   row.
3. If an affected key has too few following rows before pinned EOF, set `H` to
   EOF; runtime head state is affected and must be promoted.
4. Collect every output key `Q` in the timestamp-global replacement interval
   `[R, H)`.
5. Only after `Q` is complete, scan backward from `R`, preferably through an
   indexed direct partition key, until every key in `Q` has `Nmax` qualifying
   predecessors or `S` is reached. This determines `L`; warm-up later reads
   `[L, R)`.

For a fixed anchor, derive `[L, H)` from the compiler-owned segment boundary
and collect `Q` from that segment's replacement interval. The first anchor
rollout requires `D` absent, so `R` collapses to the segment start.

No supported path may substitute `START FROM` or current head merely because
planning is inconvenient. If a scan budget is reached, persist a resumable
repair cursor and continue in later refresh turns, or fail the view explicitly;
do not hide an unbounded fallback behind the optimization.

### 12.3 Select roots and finalize materialization keys

Use the timeline index to obtain:

- the predecessor root strictly below `C`, when useful;
- every logical root with `C <= maxTimestamp < H`; and
- the first unchanged suffix root at or above `H`.

Only roots in `[C, H)` receive new state versions. When `R < C` — a published
lead or a rolled-back current-turn draft lowered the output floor — the
replacement still spans `[R, H)`: any logical root in `[R, C)` has its durable
output re-emitted by the timestamp-global replacement but keeps its existing
state root. Its state is unchanged (§20.1 prefix reuse), so it is re-emitted,
not re-versioned.

`Q` is every partition key with qualifying output in `[R, H)`. Because
`REPLACE_RANGE` is timestamp-global, output for all of `Q` must be re-emitted,
not only keys in `A`. The old root at each boundary supplies unchanged state for
keys outside the repaired key domain.

The planner may choose the cheaper of:

- restoring a nearby predecessor root and replaying forward; or
- reconstructing only `Q` from the dependency floor `L`.

The second path is what makes a correction older than the first ordinary
checkpoint local: finite dependency, rather than checkpoint availability,
provides the lower bound.

### 12.4 Warm-up and replay

Construct scratch state without mutating the published runtime:

1. Restore or clone the necessary state directory into a scratch overlay.
2. Reset the repaired keys in every function and in anchor state.
3. Feed qualifying `Q` rows from `[L, R)` with output suppressed.
4. Replay every qualifying row in `[R, H)` in canonical timestamp order.
5. Emit every output row in `[R, H)` to a live-view WAL replacement.
6. After finishing each complete timestamp group that contains one or more
   logical checkpoint boundaries, freeze the scratch state needed for those
   roots. (Roots in `[R, C)`, if any, are re-emitted but keep their existing
   state version — see §12.3.)

If multiple checkpoint boundaries share a timestamp, they may share the same
function-state root while retaining separate logical identities and manifest
fields.

### 12.5 Version affected roots

For every logical checkpoint boundary `B` in `[C, H)`:

1. Start from the old root for `B`.
2. Replace only repaired function/partition entries with the state captured at
   `B`.
3. Store `baseLvRowPosition` as the replay-derived effective position minus the
   pre-existing delta-index prefix at `B`, and store the replay-derived
   `logicalStateBytes` for this root version.
4. Reuse all unchanged partition-map paths, chunks, scalar blocks, anchor
   blocks, and function roots.
5. Publish the new leaf version under the same `checkpointId`.

After the repaired leaves are known, add the replacement's total output-row
count delta to the suffix through the persistent row-position delta index. The
suffix's payload roots stay shared, but its cumulative recovery coordinate is
generation-correct.

The new timeline tree is a range splice:

```text
old prefix (< C)
    + repaired root versions ([C, H))
    + old converged suffix payload roots (>= H)
    + suffix row-position range delta
```

Its generation watermark advances to `E`, declaring all three regions valid
against the same applied snapshot. This is how the design updates an arbitrary
old O3 row without touching every later checkpoint.

### 12.6 Replace output and promote runtime

Commit `REPLACE_RANGE [R, H)` only after the complete candidate roots and
runtime disposition exist.

- If `H` is at or below the old runtime frontier, eligible state has converged.
  Apply the finite replacement and keep the primary runtime. The scratch
  overlay can be discarded after publication.
- If `H` reaches pinned EOF or crosses the runtime frontier, promote the scratch
  runtime after the replacement applies.
- If a published lead or rolled-back current-turn draft contains non-durable
  output, `D` is present and the floor drops to `R = max(S, min(C, D))` (§5),
  so the replacement starts at `R` (details in the localized O3 design);
  alternatively, normalize the non-durable output away so `D` is absent before
  planning. No watermark may advance past unmaterialized output.

The ordering is:

```text
PLAN
  -> CANDIDATE_ROOTS_AND_RUNTIME_READY
  -> LV_WAL_REPLACEMENT_COMMITTED
  -> LV_REPLACEMENT_APPLIED
  -> TIMELINE_GENERATION_PUBLISHED
  -> RUNTIME/TIER PROMOTED_IF_NEEDED
  -> CONSUMED_WATERMARK_AND_PURGE_FLOOR_ADVANCED
```

After WAL commit, refresh is blocked behind reconciliation until the replacement
is known applied or not applied. A crash may lose scratch memory, but durable
output plus the old/new timeline generations must always allow repair to be
repeated safely.

## 13. Repair work bounds and resumability

The mathematical influence interval is finite for every accepted plan, but
physical density and sparse partition keys can still make `[L, H)` large.
Track hard per-turn budgets for:

- base rows and page frames inspected;
- active/output partition keys;
- checkpoint roots versioned;
- new metadata pages;
- new state bytes;
- scratch native memory; and
- elapsed planning/replay time with circuit-breaker checks.

Crossing a budget creates a durable `LiveViewCheckpointRepairState` containing
the pinned target `E`, `C/L/H`, change-set identity, last completed timestamp
group, next logical checkpoint ID, temporary segment IDs, and phase. Ordinary
refresh remains paused for that view while later turns resume the same repair.

No partial root mapping is published. Temporary segments may accumulate across
turns, but the old timeline remains authoritative until the final generation
commit. Cancel, metadata drift, or a changed pinned snapshot discards the
temporary candidate and replans; it does not start a full-history replay.

The durability of `LiveViewCheckpointRepairState` makes cleanup and replanning
crash-safe; it does **not** make partial replay resumable on the same pinned
snapshot across a crash. Two cases must be kept distinct:

- **Yield-resume** (same process, reader still pinned): temporary progress —
  warm-up cursors, completed timestamp groups, staged temporary segments — may
  continue in a later refresh turn against the same pinned `E`.
- **Crash recovery** (process restarted, pin lost): the pinned `TableReader` is
  gone and the base table may already be applied beyond `E`. Because QuestDB
  exposes no historical/as-of reader at `E`, that snapshot cannot be recreated.
  Crash recovery therefore validates temporary-segment ownership, discards the
  partial candidate, and replans at a freshly pinned `E`; it does not resume
  partial replay. The persisted state exists to make that discard-and-replan
  safe and bounded, not to reopen the old `E`.

The first implementation may set conservative eligibility limits instead of
implementing resumability immediately, but exceeding them must produce an
explicit unsupported/cost status rather than an unbounded scan.

## 14. Recovery

### 14.1 Startup

1. Read and validate both `_timeline` slots independently.
2. Select the highest generation that passes *bounded* validation (§8.2): its
   superblock checksum, its root metadata pages, and the checksummed
   segment/completeness catalogue it references. Do not walk the full metadata
   graph or prove every referenced segment length here; deep tree paths and
   state pages are validated lazily on first access, with the failure policy in
   §14.2.
3. Pin that generation before exposing the live view to refresh.
4. Reconcile its `normalizedBaseSeqTxn`, `coveredLvSeqTxn`, live-view table
   writer txn, `_lv.s`, and base applied watermark into two recovery
   coordinates: (a) the authoritative base-`seqTxn` inclusion boundary — the
   base `seqTxn` through which durable live-view output is actually
   materialized, normally `normalizedBaseSeqTxn`, clamped down if `_lv.s` or the
   live-view table prove that less was durably applied; and (b) the durable
   designated-timestamp frontier `F` together with the selected root's effective
   `lvRowPosition`.
5. Restore the desired root at boundary `B`, including its effective
   `lvRowPosition`.
6. Rebuild runtime state over `(B, F]` with output suppressed, bounding
   transaction inclusion by the reconciled base-`seqTxn` boundary, **not** by
   `F`. Because QuestDB exposes no historical/as-of base reader (only the
   *current* applied snapshot or the raw WAL sequencer log), this rebuild is a
   `seqTxn`-ordered replay of base WAL transactions — from the root's base
   `seqTxn` up to that boundary, applying only rows in the `(B, F]`
   designated-timestamp window (the existing `drainBaseWal`/`replayToApplied`
   mechanism) — not a scan of the current applied snapshot. `F` reconciles which
   output is already durable; the base-`seqTxn` boundary decides which base
   transaction *versions* may be incorporated. The rebuild must exclude any
   transaction applied above the boundary even when its rows carry a timestamp
   at or below `F` (apply-ahead), because the current applied snapshot may sit
   ahead of it. The replay is required even when the boundary already equals the
   applied base `seqTxn`; normalization does not claim the root contains state
   beyond `B`.
7. Only then classify and process base changes above the reconciled boundary —
   including any apply-ahead O3 correction — through the ordinary O3/in-order
   paths.

A corrupt newest metadata slot falls back to the previous slot. WAL and files
needed by that slot remain protected by the purge rules. Two unusable slots or
a definition mismatch starts a derived-state rebuild; there is no compatibility
decoder for `.cp`.

### 14.2 Root selection

Ordinary restart chooses the newest current logical root compatible with the
materialized live-view table. Compatibility includes checkpoint timestamp,
effective `lvRowPosition`, history/definition epoch, and the reconciled
materialization frontier; `coveredLvSeqTxn` alone is insufficient because
checkpoint cadence and live-view WAL commits are independent. Historical
repair uses predecessor/range lookup. Recovery does not enumerate arbitrary
`.cp` files; it is not limited to the ≤8 retained ring anchors, and it does not
fall back to a full `START FROM` replay for older boundaries.

If a selected root has a structurally invalid data page discovered by lazy
validation, that failure invalidates only that one root *version*, not the
pinned generation: mark the version unusable, choose a safe predecessor or
dependency reconstruction point, and rebuild the same logical checkpoint ID. Do
not permanently delete the logical entry, and do not switch the whole live view
back to the previous superblock slot once readers have already observed the
selected generation — a late slot fallback is available only before the new
generation is exposed (§14.1). Bound repeated failures and surface them as
checkpoint storage corruption.

### 14.3 Development-format reset

Because the feature is unreleased, startup encountering `_ring`, `.cp`, `.scp`,
or an unsupported timeline version may remove the derived checkpoint directory
and rebuild it from the base/live-view state. It must not attempt mixed-format
recovery.

## 15. Crash consistency

### 15.1 Publication protocol

For generation `G + 1`:

1. Allocate monotonically versioned segment and metadata IDs.
2. Write data temp files.
3. Sync as configured, close, and rename data files.
4. Write checksummed metadata temp files referring only to final data names.
5. Sync as configured, close, and rename metadata files.
6. Write the inactive `_timeline` slot with generation/body first and its valid
   publication word last.
7. Sync `_timeline` as configured.
8. Publish in-memory generation `G + 1`.
9. Advance durable watermarks and enqueue unreachable `G - 1` physical objects
   when they are no longer a fallback root.

Atomic rename prevents a partial data/metadata file from becoming referenced;
the superblock slot is the sole commit point.

### 15.2 Required crash outcomes

| Crash point | Recovery outcome |
|---|---|
| During data write | Ignore/unlink temp data; use generation `G`. |
| After data rename, before metadata | Orphan data is unreferenced; use `G`. |
| During metadata write | Ignore/unlink temp metadata and new data; use `G`. |
| After metadata rename, before slot publish | New files are orphans; use `G`. |
| Torn new slot | Validate old slot and use `G`. |
| After new slot publish | Use `G + 1`; old files remain reader/fallback protected. |
| After LV replacement apply, before timeline publish | Reconcile against `G`, retain required WAL, and repeat the bounded repair. |
| After timeline publish, before runtime promotion | Restore/promote from `G + 1`; never continue with stale primary state. |
| During purge | Current and fallback generations remain complete; retry obsolete-file deletion. |

`NOSYNC` retains its normal QuestDB power-loss semantics. CRC cannot make
unsynced payload durable, so data checksums are not a substitute for ordering
and sync.

## 16. Garbage collection and lifecycle

### 16.1 Logical versus physical retention

The design keeps all **logical checkpoints**, not all obsolete files:

- an O3-replaced root version is obsolete after publication and reader release;
- metadata tree paths superseded by copy-on-write are obsolete;
- a data segment is obsolete when no current/fallback root references any page
  in it;
- current roots and shared pages remain, however old their checkpoint IDs are.

This distinction retains every useful historical anchor without leaking every
failed or superseded representation.

### 16.2 Reference accounting

The checksummed segment directory stores generation-transactional reference
counts at segment granularity. A root build reports its referenced segment set;
the range splice applies added/removed references in the same metadata
generation. Zero-reference segments receive `retireGeneration` and enter a
purge queue.

Deletion is allowed only when:

- the segment is unreachable from both valid superblock slots;
- the minimum pinned reader generation is greater than `retireGeneration`; and
- no in-progress repair candidate owns it.

Startup reconciles the purge queue against the segment directory's reference
counts — bounded by the number of segments, not by timeline length — rather than
by walking reachability across every logical checkpoint leaf. A failed unlink is
logged and retried; it never invalidates a generation.

### 16.3 Compaction

Small or sparsely live segments may be repacked by writing new versioned
segments and publishing roots that redirect the same logical pages. Compaction
uses the normal generation protocol and reader pins. It changes physical bytes,
not logical checkpoint identity or state.

### 16.4 Other lifecycle events

- `DROP LIVE VIEW`: retire the whole timeline and delete it after reader release.
- Definition/schema epoch change: build a new history epoch; retire the old one.
- Backup: checkpoint state remains optional derived data and may be excluded.
- Restore without timeline: rebuild locally.
- Replica: does not publish local timeline generations.
- Promotion to primary: restore a locally valid timeline if one exists;
  otherwise build a new genesis/head timeline from authoritative data.

These replica/backup/promotion behaviors are not invented here. Enterprise
already implements and test-locks them today: a refresh-disabled
`ReplicaLiveViewStateStore` with lead reconstruction, a primary/replica role
switch, backup that excludes the ring, and restore that rebuilds derived state
locally. The "eventual integration contract" is the contract for the *new
timeline*; this design ports an existing, mature lifecycle integration onto the
new format rather than defining that behavior from scratch.

The first OSS implementation intentionally excludes `_checkpoints` from the
OSS `CHECKPOINT`/snapshot copy set. Restore clears any destination timeline and
rebuilds it from the restored base and live-view state. A future optimization
may copy a timeline only by pinning one generation and copying its complete
reachable metadata/data graph; directory enumeration concurrent with purge is
not a valid snapshot protocol.

## 17. WAL-purge floor

The old ring coupled WAL retention to whichever checkpoint happened to be the
newest listed entry. The new rule is generation-based:

```text
checkpointWalFloor = min(
    normalizedBaseSeqTxn of every valid superblock slot,
    normalizedBaseSeqTxn required by every pinned recovery reader,
    base seqTxn required by an in-progress repair
)
```

The live-view WAL/base WAL purge jobs combine this with their existing floors.
A new generation may release the old floor only after its superblock is durable
and the previous slot no longer needs the older WAL delta. Keeping two slots
retains at most the delta between recent catalog generations, not WAL back to
the oldest logical checkpoint.

## 18. API changes

The exact names may change, but responsibilities should be explicit.

### 18.1 Window functions

Add interfaces equivalent to:

```java
LiveViewCheckpointDependency checkpointDependency();

void freezeCheckpointState(
        LiveViewStatePageWriter writer,
        LiveViewFunctionRoot previousRoot,
        LiveViewFunctionRootBuilder nextRoot
);

void restoreCheckpointState(
        LiveViewStatePageReader reader,
        LiveViewFunctionRoot root
);

void restoreCheckpointPartition(
        LiveViewStatePageReader reader,
        LiveViewFunctionRoot root,
        Record key
);

void resetCheckpointPartition(Record key);
```

Snapshot payloads are length-bounded by page references. Existing unbounded
`MemoryA`/`MemoryR` snapshot methods should be replaced rather than preserved as
a compatibility layer.

### 18.2 Timeline store

Introduce focused components under `io.questdb.cairo.lv`, for example:

- `LiveViewCheckpointTimeline`
- `LiveViewCheckpointTimelineReader`
- `LiveViewCheckpointTimelineWriter`
- `LiveViewCheckpointRoot`
- `LiveViewCheckpointMetaStore`
- `LiveViewCheckpointDataStore`
- `LiveViewCheckpointGenerationPin`
- `LiveViewCheckpointPurgeJob`
- `LiveViewCheckpointRepairPlanner`
- `LiveViewCheckpointRepairState`

The refresh job should orchestrate these components rather than implement file
format, tree mutation, page ownership, and purge logic in one class.

### 18.3 Runtime ownership

Group every mutable continuation component in one `LiveViewRefreshRuntime`:
compiled cursor factory/functions, anchor window, symbol cache and overlay
horizons, event-time frontier, memory-tracker bindings, checkpoint generation
pin, and in-memory tier disposition. Candidate promotion exchanges this owner
atomically, avoiding a mixed old/new runtime after timeline publication.

## 19. Observability

Add `live_views()` fields or equivalent metrics for:

```text
checkpoint_timeline_generation
checkpoint_timeline_entries
checkpoint_timeline_normalized_base_seqtxn
checkpoint_timeline_logical_bytes
checkpoint_timeline_physical_bytes
checkpoint_timeline_shared_bytes
checkpoint_timeline_sharing_ratio
checkpoint_timeline_row_position_delta_bytes
checkpoint_data_segment_count
checkpoint_obsolete_segment_bytes
checkpoint_oldest_pinned_generation
checkpoint_gc_lag_generations
checkpoint_last_write_micros
checkpoint_last_write_new_bytes
checkpoint_last_lookup_depth
checkpoint_repair_in_progress
checkpoint_repair_low_timestamp
checkpoint_repair_correction_timestamp
checkpoint_repair_high_timestamp
checkpoint_repair_rows_scanned
checkpoint_repair_rows_emitted
checkpoint_repair_roots_versioned
checkpoint_repair_new_bytes
checkpoint_repair_resumes
checkpoint_repair_failures
```

Completion logs should identify the selected dependency kind, `E/C/R/L/H` (and
`D` when a non-durable floor is present), affected/output key counts, roots
versioned, prefix/suffix roots reused, page bytes written/shared, output rows,
scan rows, and elapsed time. Steady-state checkpoint creation should not emit
per-root INFO logs.

This is a user-visible catalogue change. The current `live_views()`
checkpoint columns — `head_checkpoint_lv_seqtxn`, `head_checkpoint_max_ts`,
`head_checkpoint_state_bytes`, and the `checkpoint_ring_*` group — are all
replaced by the `checkpoint_timeline_*` fields above and removed in Phase 9.
Downstream queries and dashboards that reference the old columns must migrate.

## 20. Correctness argument

### 20.1 Prefix reuse

`C` is the earliest incorporated change. A root with `B < C` contains only rows
at or below `B`, so no incorporated change can affect it. Reusing its state under
generation watermark `E` is correct.

### 20.2 Repaired interval

For every accepted function, `L` contains all state on which output at `C`
depends. Replay processes the pinned snapshot in canonical order and captures
state after each boundary `B` in `[C, H)`. Replacing the affected partition
entries therefore produces the same structural state as localized
recomputation, subject only to the documented floating tolerance.

### 20.3 Suffix reuse

The dependency descriptor proves that changed frame membership and exact
structural state have converged before `H`. Therefore roots with `B >= H` are
semantically unchanged. Their payload roots remain shared. Their cumulative
`lvRowPosition` may still shift when replacement row count changes, so the
generation's persistent suffix delta supplies the corrected recovery metadata
without copying or walking the suffix. Generation-level normalization records
that the roots have been validated through `E`; it does not replace their
boundary-specific recovery coordinates.

### 20.4 Complete output replacement

`Q` contains every qualifying partition key in the timestamp-global range and
replay emits every qualifying row in `[R, H)` exactly once. Applying
`REPLACE_RANGE [R, H)` cannot delete an unrelated row without recreating it.
The output floor `R` equals `C` unless a non-durable lead or rolled-back draft
lowered it (§12.3); roots in `[R, C)` are re-emitted but keep their prior state
version, so complete replacement holds without over-versioning the prefix.

### 20.5 Failure isolation

Published runtime and timeline state are immutable during planning. Before WAL
commit, failure discards the candidate. After WAL commit, refresh blocks behind
reconciliation. The superblock commit selects either the old complete
generation or the new complete generation; it never exposes a partial root
splice.

## 21. Test plan

### 21.1 Timeline and metadata store

- empty timeline, one entry, millions of ordered entries;
- duplicate `maxTimestamp` with distinct checkpoint IDs;
- predecessor/successor and `[C, H)` range lookup at all edges;
- append and bulk range-splice property tests against a `TreeMap` oracle;
- checksummed metadata corruption in every node and superblock field;
- independently valid/invalid A/B slots and generation selection;
- metadata compaction preserving every logical entry;
- no operation linear in timeline size on ordinary append or predecessor
  lookup.

### 21.2 Page store and codecs

- raw and encoded timestamp/double round trips, including all bit patterns;
- empty, one-row, full-chunk, and chunk-boundary cases;
- circular buffer with partial shared head and copy-on-write tail;
- incompressible input chooses raw without material expansion;
- malformed lengths, offsets, codec tags, row counts, trailing bytes, and
  decoded-size overflow fail before unsafe access/allocation;
- no production CRC read or write over data segments;
- exact segment reference accounting under shared roots;
- tracker-accounted scratch with no leak or per-checkpoint allocation growth.

### 21.3 Normal checkpointing

- adjacent roots share unchanged chunks and partition-map paths;
- sealing cost follows changed pages rather than total frame size;
- all logical roots remain searchable after count/byte values that would have
  pruned the old ring;
- restart from the newest and from an arbitrarily old root;
- restart from a root at `B` in a generation already normalized through `E`
  while durable output extends to `F > B`, proving `(B, F]` runtime rebuild
  even when there is no base-`seqTxn` delta;
- restart under apply-ahead: the base table applied past the generation's
  reconciled base-`seqTxn` boundary with an O3 correction below `F` still
  unincorporated, proving the `(B, F]` rebuild includes only transactions
  through the boundary and defers that correction to post-restore classification
  rather than double-counting it;
- in-order generation watermark advancement reuses all older roots;
- same-timestamp cadence events remain separately addressable and tie-safe.

### 21.4 O3 repair

Differentially compare with a fresh build for:

- finite `RANGE` and `ROWS`, one and many partition keys;
- O3 newer than head, inside recent history, older than the former ring horizon,
  and before the first ordinary checkpoint;
- exact `C`, root-boundary, frame-boundary, and equal-timestamp ties;
- fixed anchor historical and current segments;
- several inserted rows and affected keys in one batch;
- `H` before the runtime frontier and `H == EOF`;
- a changed output-row count updates every suffix root's effective
  `lvRowPosition` through the delta index without a suffix leaf walk;
- designated timestamp `Long.MAX_VALUE`, proving it is data while tagged EOF
  remains representable and replaceable;
- roots immediately before `C`, in `[C, H)`, and at/after `H` proving exact
  prefix/suffix sharing;
- sparse ROWS keys and dense RANGE intervals;
- filters and null partition keys when those shapes are enabled;
- floating results under the documented tolerance and exact continuation for
  integer/decimal state;
- a subsequent sequence of in-order rows after repair, proving promoted state
  rather than output alone.

Assert that no source cursor reads below `L` or above `H`, and that the count of
root versions equals the logical entries in `[C, H)`.

### 21.5 Rejection tests

- `row_number`, `rank`, and `dense_rank` fail CREATE with no finite `H`;
- unanchored cumulative aggregates fail CREATE;
- arbitrary/data-dependent anchors and FOLLOWING frames fail initially;
- mixed incompatible partition/order signatures fail;
- dedup replacement, delete, truncate, partition drop, TTL removal, and schema
  change never enter insert-only repair.

### 21.6 Crash and purge tests

Inject failure at every row in section 15.2, plus:

- latest slot metadata corruption with successful previous-slot recovery;
- WAL purge racing slot replacement;
- readers pinned to both generations while repair/compaction publishes;
- zero-reference segment purge, failed unlink, and restart retry;
- crash during resumable repair at each persisted phase, asserting
  discard-and-replan at a freshly pinned `E` rather than same-snapshot resume
  (§13);
- DROP/definition epoch change with pinned readers;
- OSS `CHECKPOINT`/snapshot restore without timeline files clears stale
  destination checkpoint state and rebuilds locally.

### 21.7 Performance tests

- checkpoint write bytes and CPU versus complete v1 snapshots;
- restore latency from recent and very old logical roots;
- predecessor lookup at 1, 1K, 1M, and 100M logical entries;
- restart/startup cost (bounded slot validation, root selection, and the
  `(B, F]` rebuild) at 1M and 100M logical entries, proving it is independent of
  total payload size and does not grow with timeline length;
- long steady-state one-minute RANGE workload proving physical growth is near
  unique encoded rows plus descriptors, not frame size times checkpoint count;
- old O3 storms proving scans remain inside `[L, H)` and suffix roots are reused;
- metadata/page compaction throughput and foreground interference;
- CRC cost is absent from data write/restore profiles.

## 22. Implementation plan

The main OSS integration seams are `LiveViewRefreshJob`, `LiveViewInstance`,
`LiveViewRecovery`, `LiveViewState` (the `_lv.s` CORE_STATE writer — a genuine
live-view seam, but state/watermark storage, not checkpoint state; its mutable
runtime mirror is `LiveViewStateReader`), `CairoEngine`,
`PageFrameRecordCursorFactory`, `DatabaseCheckpointAgent`,
`TableSnapshotRestore`, `WindowFunction`, `AvgDoubleWindowFunctionFactory`, and
`LiveViewsFunctionFactory`. New storage components belong under
`io.questdb.cairo.lv`. Existing `LiveViewCheckpoint*` and ring classes remain
only until the equivalent timeline phase is tested, then are deleted rather
than adapted.

Core coverage should extend `LiveViewCheckpointTest`,
`LiveViewCheckpointRestoreTest`, `LiveViewSmokeTest`, `LiveViewFuzzTest`,
`LiveViewValidationTest`, and `LiveViewPageFrameCursorTest`, with focused new
timeline/page-store tests.

No enterprise *feature* work is implemented by these OSS phases; enterprise
replication, switch/demote, cold-promotion, and backup/restore integration are
planned and validated separately. That separation is clean only inside the OSS
submodule. When the enterprise repository consumes the new OSS revision it needs
companion changes, because enterprise already depends on the surface these
phases move:

- `EntLiveViewRefreshJob` reuses the OSS snapshot/encoding primitives
  (`LiveViewWindow.snapshot`/`restore`, `LiveViewFunctionSnapshot.write`) for
  its in-RAM replica lead rollback. Phase 3 (replace the `MemoryA`/`MemoryR`
  snapshot API) and Phase 9 (delete `LiveViewFunctionSnapshot`) therefore reach
  into enterprise production and require a re-pointed snapshot/encoding
  dependency.
- Enterprise tests hard-depend on the format and hook names — `BackupTest`,
  `LiveViewReplicationTest`, and `LiveViewColdPromoteTest` import
  `CP_FILE_EXT`/`CHECKPOINT_DIR_NAME`/`ringManifestPath` and pin lifecycle hooks
  — and break at compile or behavior time when `.cp` and `_ring` disappear, so
  they need a companion test migration.

Add an explicit enterprise-consumption gate at Phase 3/9 covering the
`EntLiveViewRefreshJob` snapshot dependency and this enterprise test migration.

### Phase 0: contracts and baseline

1. **[DONE]** Freeze the invariants, terminology, supported window matrix,
   floating-point tolerance, and failure ordering in tests/design comments. This
   must pin the three contracts the blocker findings pull forward before Phase 1
   freezes the schemas: the dual base-`seqTxn` + timestamp/row-position recovery
   model (§14.1), the `D`/`R` output-floor contract with the `[R, H)` output
   versus `[C, H)` state-version split (§5, §12), and the bounded
   startup-validation/lazy-corruption model (§8.2, §14).
   Pinned in `io.questdb.cairo.lv.LiveViewCheckpointContracts` (invariants,
   terminology, the three pulled-forward contracts, the supported-window
   `DependencyKind` matrix, the `HighBoundTag`, the `RepairPublicationStage`
   ordering, and the floating tolerance) and frozen under test in
   `LiveViewCheckpointContractsTest`.
2. **[DONE]** Add metrics for current checkpoint bytes, write time, restore
   time, ring evictions, O3 scan rows, and replay bounds to establish a baseline.
   Current checkpoint bytes (`head_checkpoint_state_bytes`) and replay bounds
   (`o3_resume_replay_rows` / `o3_boundary_replay_rows`, splitting bounded resume
   from unbounded boundary rebuild) already existed. Added four `live_views()`
   baseline columns wired to live per-instance values: `head_checkpoint_write_micros`
   and `head_checkpoint_restore_micros` (elapsed write/restore-from-head durations,
   NULL until the event first runs), `checkpoint_ring_evictions` (retention-budget
   evictions over the LV lifetime, counting only budget-driven pruning), and
   `o3_replay_scan_rows` (base rows the O3 replay paths scanned, `>=` the emit
   counters; a WHERE filter makes scan exceed emit). Covered by
   `LiveViewSmokeTest` (column set, eviction count, write timing, resume/boundary
   scan==emit, filtered scan>emit) and the restore path in
   `LiveViewCheckpointRestoreTest`.
3. **[DONE]** Build deterministic RANGE/ROWS fixtures with enough history to
   collapse the existing ring and reproduce an older O3 boundary replay.
   Added `LiveViewCheckpointRingBoundaryFixtureTest` with a partitioned
   `sum(x)` view over a bounded RANGE frame (`'30' SECOND PRECEDING`) and over a
   bounded ROWS frame (`3 PRECEDING`) - eligible dependency kinds under the
   section 6 matrix, so they survive the step 4 allowlist, and bit-exact
   (section 6.1) so a from-base recompute oracle compares with exact equality.
   Each fixture pins `checkpoint.rows = 1` (one head per flush) and
   `retention.count = 8`, drives 12 in-order commits so the ring collapses to
   the budget (`checkpoint_ring_evictions = 4`, oldest surviving anchor at 50s),
   then contrasts the two O3 paths on the same collapsed ring: an in-horizon O3
   (55s, above the oldest anchor) resumes from that anchor
   (`o3_resume_replay_rows > 0`, boundary untouched), while an older O3 (5s,
   below the whole ring) finds no anchor and falls back to the O(view age)
   boundary rebuild (`o3_boundary_replay_rows` grows, resume untouched - the two
   paths are disjoint). The view converges to the from-base recompute with no
   refresh fault after every step.
4. **[DONE]** Implement the initial CREATE-time allowlist and rejection
   diagnostics, move OSS tests/fixtures onto eligible functions, and pin the
   scope-cut decision: unanchored `row_number`/`rank`/`dense_rank` (previously
   accepted, snapshot-capable, and tested) are removed, while their anchored,
   segment-reset forms stay eligible and return in Phase 7.
   `SqlParser.validateLiveViewFiniteInfluence()` rejects at `CREATE LIVE VIEW`
   any `row_number`/`rank`/`dense_rank` whose window is not anchored - naming the
   function and explaining it has no finite out-of-order influence boundary -
   and accepts the anchored (`OVER w ... ANCHOR ...`) forms. The check runs at
   parse time, after `validateLiveViewAnchors`, and closes the single-partition
   `OVER ()` / `OVER (ORDER BY ts)` hole the bare-unbounded reject deliberately
   left open (partitioned-but-unanchored ranking was already turned away).
   `LiveViewCheckpointContracts.DependencyKind.UNANCHORED_RANK` (frozen in step 1)
   is the pinned contract this enforces. `LiveViewValidationTest`
   `testRejectUnanchoredRanking` locks the reject (row_number/rank/dense_rank,
   inline and via a named window, case-insensitive, nested in an expression) and
   the anchored `ANCHOR EXPRESSION`/`ANCHOR DAILY` positive controls. Every OSS
   test/fixture that built a live view on unanchored `row_number() OVER ()` was
   moved onto an eligible shape. Where a test asserts the ranking column's values
   as a gapless `1..N` witness, an identical-output substitute keeps the asserted
   values unchanged: a single-partition, full-look-behind
   `count(*) OVER (PARTITION BY <g> ORDER BY <ts> ROWS BETWEEN 1000000 PRECEDING
   AND CURRENT ROW)` over a single (usually all-`NULL`) partition column
   reproduces `1,2,...,N` bit-for-bit, so no expected value changed; filler sites
   use a natural bounded partitioned frame. The override- and DESC-scan rejection
   tests, whose exploit surface was reachable only through the removed unordered
   ranking shape, now assert the finite-influence gate (and the cached/multi-pass
   gate for ordered eligible windows) that subsume it. This migration - across
   `LiveViewSmokeTest`, `LiveViewInMemReadTest`, `LiveViewTest`, `LiveViewFuzzTest`
   (which also exercises anchored `rank`/`dense_rank`), `LiveViewConcurrencyTest`
   (gapless-`1..N` invariant preserved), the start-from/dedup/DDL/replace-range
   suites, and the non-`cairo/lv` catalogue/security/show-create/QWP/DDL-listener
   tests plus the in-mem read benchmark - confirmed no shipped fixture depends on
   the removed shapes. `ServerMainTest` and the `griffin` `CheckpointTest`
   already used anchored `row_number() OVER w`, so they stayed eligible unchanged.

**Deliverable:** measured baseline plus enforced CREATE-time eligibility; no
checkpoint format change.

### Phase 1: timeline metadata prototype

1. Implement immutable metadata pages, per-page checksum, page references, and
   A/B `_timeline` slots.
2. Implement the persistent B+ tree with append, predecessor, range iteration,
   and bulk splice.
3. Implement the persistent row-position difference/prefix-sum tree with
   suffix range-add and effective-position lookup.
4. Implement generation pins and a test-only in-memory payload root.
5. Add exhaustive store, corruption, and crash tests before integrating live
   views.

**Deliverable:** a durable logarithmic logical-checkpoint catalog independent of
window state.

### Phase 2: immutable state/data store

1. Implement versioned data segments, metadata page references, strict bounded
   reader validation, and segment-level reference accounting.
2. Implement persistent partition maps and function/root builders.
3. Implement semantic timestamp and exact double codecs with raw fallback. This
   is greenfield: the current `.cp` v1 stores raw pairs and implements none of
   them; the codec set and threshold are adopted from the encoding proposal.
4. Implement persistent ring/deque chunks for one motivating function:
   partitioned `avg(double)` over bounded `RANGE`.
5. Add generation-safe purge and repack compaction.

**Deliverable:** adjacent synthetic roots share pages and survive restart;
production data files contain no CRC.

### Phase 3: checkpoint API and normal sealing

1. Replace `LiveViewFunctionSnapshot`'s monolithic memory payload contract with
   the page-aware freeze/restore API.
2. Add compiler-stable function identity and `LiveViewCheckpointDependency`.
3. Integrate normal checkpoint cadence with `maybeWriteHeadCheckpoint()` through
   a dedicated timeline writer.
4. Publish generation watermarks and connect the WAL-purge floor.
5. Restore runtime state from any logical root.

**Deliverable:** normal refresh writes the new timeline exclusively; every root
is retained and predecessor lookup works after restart.

### Phase 4: recovery and lifecycle

1. Replace checkpoint-directory sweep/ring-candidate recovery with bounded
   superblock selection, generation pinning, dual-coordinate reconciliation (the
   base-`seqTxn` inclusion boundary plus frontier `F`), and the mandatory
   `seqTxn`-bounded `(B, F]` runtime rebuild from a selected root.
2. Implement orphan cleanup, old-slot protection, purge retry, DROP, epoch
   replacement, and primary-only ownership seams.
3. Exclude `_checkpoints` from OSS `CHECKPOINT`/snapshot copying; on restore,
   clear destination timeline state and rebuild from restored authoritative
   data.
4. Add OSS restart, crash, purge, DROP/epoch, and snapshot-restore coverage,
   including the apply-ahead rebuild case (base applied past the reconciled
   boundary with an unincorporated O3 correction below `F`) and restart cost at
   1M/100M logical entries.

**Deliverable:** complete restart/lifecycle protocol before O3 routing changes.

### Phase 5: bounded RANGE repair

1. Add RANGE dependency descriptors and compiler validation.
2. Refactor `o3Replay()` into shared snapshot/change-set planning and separate
   repair execution.
3. Add lower/upper-bounded forward page-frame cursor APIs so planning and
   replay cannot read above finite `H`.
4. Derive tagged RANGE `L/H` and the output floor `R`, collect `A/Q`, build a
   scratch partition overlay, and emit a finite or explicitly EOF-bounded
   `REPLACE_RANGE [R, H)`, including `Long.MAX_VALUE` timestamps.
5. Capture and range-splice every root in `[C, H)`, updating repaired positions
   and the suffix row-position delta index.
6. Implement the post-WAL-commit reconciliation and atomic runtime promotion
   state machine.

**Deliverable:** the motivating one-minute RANGE average repairs an O3 row of
arbitrary age without scanning before `L`, after `H`, or discarding old roots.

### Phase 6: bounded ROWS repair

1. Add backward/per-key cursor support required for predecessor discovery.
2. Implement the explicit `H -> Q -> L` planning order with per-key
   following/predecessor discovery and exact tie handling.
3. Prefer indexed direct-column partition lookup where available; add explicit
   scan budgets and metrics elsewhere.
4. Enable one exact aggregate/type at a time, then approved floating aggregates.

**Deliverable:** bounded ROWS O3 repair with data-dependent but finite bounds.

### Phase 7: fixed anchors and broader function coverage

1. Add compiler-owned fixed segment boundaries and anchor-map page roots.
2. Enable anchored cumulative aggregates and anchored `row_number`/`rank`/
   `dense_rank` (per-segment reset) with exact segment `[L, H)` repair —
   restoring the ranking shapes cut at Phase 0.
3. Add `sum`, `count`, `avg`, `min`, and `max` implementations type by type,
   including monotonic deque page kinds.
4. Add compatible multiple functions, filters, symbols, and partition
   expressions only after their key projector and seek behavior are proven.

**Deliverable:** explicit production allowlist; unsupported remains rejected.

### Phase 8: resumability and cost control

1. Persist repair phase/cursors and temporary segment ownership.
2. Yield large finite repairs across refresh turns without publishing partial
   roots, distinguishing same-process yield-resume (reader still pinned, `E`
   continued) from crash-time discard-and-replan at a freshly pinned `E` — no
   as-of reader can reopen the old `E` (§13).
3. Add cancellation, metadata-drift restart, circuit breaker, and memory-limit
   behavior.
4. Add cost-based choice between predecessor restore and dependency-only
   partition reconstruction.

**Deliverable:** no accepted plan requires an unbounded single refresh turn.

### Phase 9: remove obsolete implementation

After all new recovery and O3 tests pass:

1. Delete `LiveViewCheckpointWriter`, `LiveViewCheckpointReader`, ring manifest
   reader/writer/model/candidate classes, retained-ring fields/methods, and `.cp`
   sweep logic.
2. Delete checkpoint retention count/byte configuration and old ring catalogue
   columns/metrics.
3. Remove v1 snapshot compatibility branches, old format tests, `_ring` trust
   rules, full-file checkpoint CRC, and the two-GiB limit.
4. Keep a development-only cleanup path that removes old derived checkpoint
   directories and rebuilds.

**Deliverable:** one checkpoint architecture, with no dormant compatibility
surface.

### Phase 10: soak and acceptance

1. Run long RANGE and ROWS workloads through repeated old/new O3, restart,
   OSS checkpoint restore, compaction, and purge cycles.
2. Verify logical entry count never drops inside one history epoch.
3. Verify physical growth, write CPU, lookup latency, repair scan bounds, and
   refresh lag against acceptance criteria.
4. Run mutation tests against publication ordering, strict predecessor search,
   `C/H` range selection, generation watermarking, and purge guards.

**Deliverable:** default architecture; no rollout flag is needed for an
unreleased feature.

## 23. Alternatives considered

### 23.1 Keep every complete `.cp`

This preserves anchors but grows as checkpoint count times full live state. It
also retains whole-file CRC and restore scans. Compression improves constants
without fixing repeated state.

### 23.2 Increase or dynamically size the ring

Any finite cap eventually loses older checkpoints; no cap reproduces complete
`.cp` growth. It does not solve an O3 row older than the chosen horizon.

### 23.3 Enumerate checkpoint files on demand

Directory enumeration is linear, has no authoritative current-version mapping,
and can resurrect superseded or poisoned files. An indexed catalog is required
once checkpoints are versioned rather than merely appended.

### 23.4 Append-only flat timeline log

Append is cheap, but predecessor lookup and resolving the latest root version
become linear or require a second index. Log compaction would reproduce the
same publication problem as the proposed metadata tree with weaker lookup
semantics.

### 23.5 Content-addressed state pages

Content hashes could deduplicate identical pages, but hashing every payload is
another full data pass and effectively reintroduces a mandatory data checksum.
Monotonic page IDs plus function-aware persistent chunks provide deterministic
sharing without collision handling or global hash lookup.

### 23.6 Localized O3 replay without historical roots

Finite `L/H` replay fixes foreground repair but leaves restart recovery with
only a recent state image. The timeline is still needed to retain durable
historical boundaries and to repair roots transactionally.

### 23.7 Checksum every data page

Per-page checksums are better than a whole-file CRC for lazy reads, but still
provide a stronger and more expensive integrity contract than QuestDB ordinary
data/index files. The base design keeps checksums on authoritative metadata and
leaves optional diagnostic hashes outside the format.

## 24. Acceptance criteria

The design is ready when all of the following hold:

1. A checkpoint created near the beginning of a long soak remains addressable
   after millions of later checkpoint events.
2. An O3 row older than the former ring horizon performs no source read outside
   its proven `[L, H)` interval.
3. Only roots in `[C, H)` receive new payload versions; prefix and suffix
   payload root/page identities remain shared, while suffix recovery positions
   are corrected without a linear suffix walk.
4. Adjacent steady-state bounded-window roots do not duplicate complete frame
   payloads.
5. Normal append and predecessor lookup show no linear dependence on checkpoint
   count.
6. A restart can select either valid superblock generation and reconcile
   without resurrecting a poisoned root or losing required WAL.
7. No current or fallback reader observes a deleted physical version.
8. Obsolete physical versions are eventually purged, while current logical
   checkpoint count never decreases within an epoch.
9. Checkpoint data write/restore performs no CRC pass and no two-GiB size check.
10. Exact functions match a fresh rebuild bit-for-bit; approved floating
    aggregates satisfy the documented tolerance and continue correctly after
    subsequent refreshes.
11. Unsupported no-finite-`H` SQL is rejected at CREATE rather than routed to an
    unbounded repair.
12. Refresh lag remains stable under the motivating workload and repeated old
    O3 corrections.
13. Restoring a root at `B` rebuilds runtime state through durable frontier `F`
    even when the selected generation is already normalized to the current
    applied base `seqTxn`, and the rebuild includes only base transactions
    through the reconciled base-`seqTxn` boundary — never an apply-ahead
    transaction below `F` that the durable output does not yet reflect.
14. `Long.MAX_VALUE` remains a valid timestamp and an EOF-bounded replacement
    is represented without timestamp overflow or sentinel collision.
15. OSS `CHECKPOINT`/snapshot restore cannot resurrect a partial or stale
    timeline graph and successfully rebuilds after timeline exclusion.

## 25. Design decisions

1. **Permanent logical roots, temporary physical versions.** This preserves all
   useful checkpoints without leaking superseded bytes.
2. **One normalized base watermark per generation.** It allows an unchanged
   converged suffix to be reused without restamping every later payload root;
   boundary-specific recovery positions remain separate metadata.
3. **Persistent indexed timeline, not file enumeration.** Lookup is complete,
   deterministic, and logarithmic.
4. **Dependency bounds are part of SQL eligibility.** A snapshot codec alone is
   not evidence that O3 repair is finite.
5. **Both low and high bounds are required.** A finite look-behind with an
   unbounded forward influence is still unacceptable.
6. **Timestamp-global replacement remains complete.** Until storage supports
   partition-selective replacement, every key in `[R, H)` is re-emitted (`R`
   equals `C` unless a non-durable lead/draft lowered the output floor).
7. **Semantic chunks plus structural sharing.** Compression reduces page bytes;
   sharing removes repeated pages across roots.
8. **Checksummed metadata, ordinary data-file integrity.** Whole-payload CRC is
   unnecessary and actively conflicts with localized reads.
9. **Approximate floating convergence is documented.** Structural state is
   exact; approved floating rounding differences do not force an unbounded
   suffix rewrite.
10. **No backwards compatibility.** The old ring and complete checkpoint format
    are deleted before release instead of becoming permanent maintenance debt.
11. **Persistent suffix position deltas.** O3 repair preserves logarithmic
    publication while keeping cumulative checkpoint recovery positions exact.

The resulting checkpoint system is a historical, versioned state timeline—not
a cache of the most recent eight snapshots. Its storage cost follows unique
state and small logical roots; its O3 cost follows the function's proven
dependency interval; and its recovery behavior is defined by authoritative
metadata rather than whichever checkpoint files happen to remain on disk.
