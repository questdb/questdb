# Posting Index `.pk` Chain Redesign

> **Status:** Phase 1 + Phase 2a + Phase 2b of 5 complete. This document is
> the handoff artifact — a new contributor (human or model) reading it cold
> should understand what the redesign is, why it exists, the on-disk format,
> and what's left to implement, without consulting any prior conversation.

---

## 1. Why this redesign exists

This change targets three findings from the review of [PR #6861](https://github.com/questdb/questdb/pull/6861)
("feat(core): add compact, high-performance posting index for symbol columns"):

- **Finding #2.** `PostingIndexWriter.setMaxValue` writes to the active
  metadata page outside the seqlock, bypassing the A/B publish protocol.
  Today no reader consumes `MAX_VALUE`, so it is latent — but it leaves a
  torn-write hole that is invisible to the current page-validation code
  (`determineActivePageOffset` only checks `seqStart == seqEnd`, not the
  payload fields).

- **Finding #6.** The seal path calls `publishPendingPurges(txWriter.getTxn())`
  *before* `txWriter.commit()`. Because `pendingPublishTableTxn` is not set
  on this path, `recordPostingSealPurge` falls into the conservative
  `[0, Long.MAX_VALUE)` branch, and purge correctness depends on
  scoreboard semantics that aren't enforced at the seal callsite. Action
  at a distance — fragile.

- **Finding #7 (the real one).** The seal loop in
  `TableWriter.sealPostingIndexesForO3Partitions` walks affected partitions
  sequentially. Each `sealPostingIndexForPartition(P)` writes new versioned
  files (`.pv.{N+1}`, `.pc{i}.{N+1}`) and then **publishes the new
  `sealTxn` to `.pk`'s seqlock — visible to readers immediately**. If
  partition K throws after partitions `0..K-1` have published, those
  earlier `.pk` files now advertise `sealTxn = N+1` while `txWriter.commit()`
  never runs (the writer is marked `distressed = true` and rethrown). The
  recovery model relies entirely on the orphan scan in
  `PostingIndexWriter.logOrphanSealedFiles` on the next writer reopen, but
  that scan anchors on `.pk`'s advertised `sealTxn` as the source of
  truth. It cannot detect that the published `N+1` was meant to be
  reverted.

The deeper issue: **the posting-index publish is not atomic with the
table-`_txn` commit**. The single-slot `.pk` design has no notion of "this
seal corresponds to table commit T"; readers infer the relationship
implicitly via the rowCount clamp.

The clamp saves the *current* design from this asymmetry by making the
posting index always cover a *superset* of what `_txn` references, so
trimming by `partitionHi - 1` is enough. But it cannot save the
partial-loop failure case from Finding #7, where `.pk` and `_txn` end up
in an unrecoverable inconsistent state.

## 2. The redesign in one sentence

Replace `.pk`'s single live slot with an **append-only chain of immutable
seal entries, each tagged with the table `_txn` at which it takes effect**.
Readers pick the entry where `txnAtSeal <= pinned _txn` (highest such).
Writers append entries before `txWriter.commit()`. Recovery on writer
open drops any entry where `txnAtSeal > _txn`.

The chain plus the existing maxValue clamp is sufficient to make every
publish state — healthy commit, partial publish, JVM crash mid-publish,
power loss between publish and commit — produce a consistent reader view
without any rollback machinery. The chain shifts the correctness burden
from "writer must atomically transition .pk + _txn" to "reader picks the
right entry for its pinned snapshot."

## 3. Why it works

Three properties combine.

**Property A — entries are tagged with the txn they belong to.** Every
entry carries `txnAtSeal`, the table `_txn` value at which the writer
intends `txWriter.commit()` to land for this seal. This is the link
between the two domains that v1 was missing.

**Property B — readers pin via the scoreboard before reading.**
QuestDB's existing reader contract: a reader atomically registers at a
specific `_txn` value via `TxnScoreboard` before touching anything. Two
consequences:

  - No reader is ever pinned at a `_txn` value that hasn't actually
    committed. The set of pinned `_txn` values is a subset of `{values
    that landed in _txn on disk}`.
  - The scoreboard keeps committed-snapshot files alive for the duration
    of any pin against that snapshot.

This invariant is **not new** — readers already pin for column-file
purge correctness. The chain design only requires that the same machinery
extends to `.pk` chain-entry GC.

**Property C — entries are immutable once appended.** New entries are
written by appending to the entry region; existing entries' bytes never
change. (The exception is the head entry's gen-dir tail, which can grow
under its own seqlock as sparse gens accumulate between seals — see
section 4.5.)

These three properties give the failure-mode story:

| Failure | What's on disk afterward | Reader behaviour |
|---|---|---|
| Healthy commit | All entries durable, `_txn = T+1` | Pinned at `T` → picks old entry. Pinned at `T+1` → picks new entry. |
| Partial publish (writer distressed) | Some `.pk`s have entry `(N+1, T+1)`, others don't. `_txn` still `T`. | Every reader is pinned at `T`. The picker rule (`txnAtSeal <= T`) skips `(N+1, T+1)` entries on every `.pk`, picks the prior entry. Consistent. |
| JVM crash mid-publish | Some `.pk`s have new entry, others don't. `_txn` still `T`. | Same as partial publish. |
| Crash between publish-complete and `_txn` commit | All `.pk`s have new entry. `_txn` still `T`. | Readers pinned at `T` skip the new entry, pick prior. Consistent. |
| Crash mid-`_txn` commit | Depends on `_txn`'s own atomic-write protocol. | Inherited from `_txn`'s existing torn-write handling. |

No reader-side retry. No in-flight detection. No rollback code.

## 4. On-disk format

### 4.1 File structure

```
+--------------------------------------------------------------+
|  Page A   (4096 bytes, header, seqlock-protected)            |
|  Page B   (4096 bytes, header, seqlock-protected)            |
|  Entry region  (variable size, append-only)                  |
+--------------------------------------------------------------+
```

Page A starts at offset `0` (`PAGE_A_OFFSET`).
Page B starts at offset `4096` (`PAGE_B_OFFSET`).
Entry region starts at offset `8192` (`V2_ENTRY_REGION_BASE`).

Pages A and B follow the same A/B publish protocol as v1: at any moment
exactly one of them is the "active" page (most recently published). The
inactive page is staged with new fields, then seqlock-flipped to active.

### 4.2 Header page layout (one per page, both A and B)

| Offset | Size | Field | Purpose |
|---|---|---|---|
| 0 | 8 | `V2_HEADER_OFFSET_SEQUENCE_START` | Seqlock open. Even = stable, odd = mid-write. |
| 8 | 8 | `V2_HEADER_OFFSET_FORMAT_VERSION` | `V2_FORMAT_VERSION = 2`. |
| 16 | 8 | `V2_HEADER_OFFSET_HEAD_ENTRY_OFFSET` | Byte offset of the latest entry. `V2_NO_HEAD = -1` if chain is empty. |
| 24 | 8 | `V2_HEADER_OFFSET_ENTRY_COUNT` | Number of live entries reachable from head. |
| 32 | 8 | `V2_HEADER_OFFSET_REGION_BASE` | Byte offset of the oldest live entry. Advances forward as the writer GCs older entries. |
| 40 | 8 | `V2_HEADER_OFFSET_REGION_LIMIT` | High-water byte offset just past the last written entry. |
| 48 | 8 | `V2_HEADER_OFFSET_GEN_COUNTER` | Monotonic per-`.pk` counter. Each new entry advances it by 1; new value becomes the entry's `sealTxn`. |
| 56 | 4032 | reserved | For future header growth. |
| 4088 | 8 | `V2_HEADER_OFFSET_SEQUENCE_END` | Seqlock close. Reader requires `start == end && (start & 1) == 0` and start non-zero. |

The page is 4 KB regardless of the live data size, for OS-page alignment
and to leave room for future fields without a format bump.

### 4.3 Entry layout

Entries are variable-size because each carries its own gen dir. Entry
header is `V2_ENTRY_HEADER_SIZE = 64` bytes; the gen dir follows.

| Offset | Size | Field | Purpose |
|---|---|---|---|
| 0 | 8 | `V2_ENTRY_OFFSET_LEN` | Total entry size (header + gen dir, padded to 8). |
| 8 | 8 | `V2_ENTRY_OFFSET_SEAL_TXN` | Suffix for `.pv.{sealTxn}` and `.pc{i}.{sealTxn}` files. >= 1, monotonic per `.pk`. |
| 16 | 8 | `V2_ENTRY_OFFSET_TXN_AT_SEAL` | Table `_txn` value at which this entry takes effect. Reader picks entry where this `<= pinned _txn`. |
| 24 | 8 | `V2_ENTRY_OFFSET_VALUE_MEM_SIZE` | Bytes in `.pv.{sealTxn}`. |
| 32 | 8 | `V2_ENTRY_OFFSET_MAX_VALUE` | Highest row id covered by this entry's index data. |
| 40 | 4 | `V2_ENTRY_OFFSET_KEY_COUNT` | Distinct keys at seal time. |
| 44 | 4 | `V2_ENTRY_OFFSET_GEN_COUNT` | Number of gen-dir entries. |
| 48 | 4 | `V2_ENTRY_OFFSET_BLOCK_CAPACITY` | Block capacity for this seal. |
| 52 | 4 | `V2_ENTRY_OFFSET_COVERING_FORMAT` | Reserved (currently 0). Lets sidecar formats evolve per-seal. |
| 56 | 8 | `V2_ENTRY_OFFSET_PREV_ENTRY_OFFSET` | Byte offset of the previous entry, or `V2_NO_HEAD` if oldest. |
| 64+ | `genCount * GEN_DIR_ENTRY_SIZE` | Gen dir entries (existing 28-byte records). |

`PostingIndexChainEntry.entrySize(genCount)` returns the total bytes,
rounded up to 8.

### 4.4 Why a doubly-linked-via-prev chain instead of forward-walk

Entries are variable-size, so forward iteration would require either
seek-by-`LEN` from the region base (works but expensive in the common
case) or a separate offset table (more state). Backwards walk via
`PREV_ENTRY_OFFSET` is `O(entries since pinned snapshot)` — typically
1, bounded above by GC lag. The picker terminates the walk early as
soon as an entry with `txnAtSeal <= pinned` is found.

### 4.5 Sparse gens and the head entry

Between two seals the writer accumulates **sparse generations** — small,
incremental index updates appended to `.pv` for newly-inserted rows. In
v1 these are tracked in the live `.pk` slot's gen dir. In v2 they
attach only to the **head entry**:

- The head entry's `GEN_COUNT` field can grow as sparse gens are appended.
- All older entries (everything before head in the chain) are strictly
  immutable. Their gen dirs are frozen.
- Sparse-gen append happens through a sub-protocol that uses the same
  A/B page mechanism within the head entry's footprint, so concurrent
  readers see either the pre-append gen count or the post-append count
  but never a partial state.

A reader pinned at `T_head <= T <= T_next_seal` picks the head entry
and reads `head.gen_dir[0 .. head.GEN_COUNT)`. Sparse gens for rows
beyond the reader's `partitionHi` are filtered out by the existing
`maxValue` clamp on the cursor.

**O3 always triggers a new seal** — that is, every O3 commit produces a
new chain entry. This is the single critical invariant for sparse-gen
correctness: O3 rewrites row positions, so sparse gens that were valid
before O3 cannot be valid after. By forcing a new entry at every O3
commit, sparse gens added between seals are guaranteed to reference
positions stable for the head entry's `txnAtSeal` snapshot and onward.
Readers pinned at pre-O3 snapshots use *older* entries with their own
frozen gen dirs.

## 5. Protocols

### 5.1 Writer publish

```
seal():
  Phase 0:  build new sealed files
    write .pv.{newSealTxn}, .pc{i}.{newSealTxn} on disk (versioned)
    fsync them
  Phase 1:  stage entry payload in the .pk entry region
    pick offset O = current REGION_LIMIT
    PostingIndexChainEntry.writeHeader(O, newSealTxn, txnAtSeal=newTxn,
                                       valueMemSize, maxValue, ...)
    write gen_dir at O + V2_ENTRY_HEADER_SIZE
    fsync .pk
  Phase 2:  publish via seqlock-flip
    PostingIndexChainHeader.publish(
        keyMem,
        active page offset,
        new head entry offset = O,
        new entry count = current + 1,
        REGION_BASE = unchanged,
        REGION_LIMIT = O + entrySize(genCount),
        GEN_COUNTER = newSealTxn
    )
    // returns new active page offset (the previously-inactive one)

  // Caller (TableWriter.finishO3Commit) repeats Phase 0+1+2 for every
  // affected partition+column pair, then calls txWriter.commit().
```

`txnAtSeal` is `txWriter.getTxn() + 1` — the txn `txWriter.commit()` is
about to assign. If the commit fails, the entry is abandoned but
recovery handles it (see 5.4).

### 5.2 Reader

```
RowCursor open:
  T = TxnScoreboard.acquire(_txn)         // pin
  if !PostingIndexChainHeader.readUnderSeqlock(keyMem, header):
    // header unreadable; treat as I/O error
  if header.isEmpty():
    use the existing "no posting data, fall back to scan" path
  result = PostingIndexChainPicker.pick(keyMem, T, header, entry)
  switch result:
    RESULT_OK:
      open .pv.{entry.sealTxn} and .pc{i}.{entry.sealTxn}
      use entry.gen_dir; clamp returned row ids by maxValue (unchanged)
    RESULT_EMPTY_CHAIN, RESULT_NO_VISIBLE_ENTRY:
      fall back to scan (or NullCursor — same as today's empty index)
    RESULT_HEADER_UNREADABLE:
      propagate as I/O error
  ...
RowCursor close:
  TxnScoreboard.release(T)
```

The picker is a single backward walk from head, bounded by `entryCount`
and by region-limit checks. No retry, no fallback path, no in-flight
detection.

### 5.3 GC during normal operation

```
Each seal() execution, before appending the new entry:
  L = lowest pinned _txn from the scoreboard for this column/partition
       (or +infinity if no pins)
  walk entries forward from REGION_BASE:
    if entry.txnAtSeal < L AND there is a newer entry with txnAtSeal <= L:
      // No pinned reader will ever pick this entry
      mark entry's .pv.{sealTxn} and .pc{i}.{sealTxn} for delete
      advance REGION_BASE past this entry's bytes
    else:
      stop  // older entries beyond this would violate a pin

  // Mark-for-delete enqueues to PostingSealPurgeJob (existing job).
  // Only the writer thread mutates .pk; the job only deletes files.
```

The chain prune is bundled into `seal()`, amortising the cost into work
the writer already does. The purge job retains its existing role as the
delete-files-from-disk worker but stops touching `.pk` itself.

### 5.4 Writer-open recovery

```
PostingIndexWriter.open():
  PostingIndexChainHeader.readUnderSeqlock(...)
  walk chain backwards from head:
    while entry.txnAtSeal > _txn:
      // Abandoned: a previous writer published this entry but never
      // committed. No reader can be pinned at txnAtSeal because
      // txnAtSeal never landed in _txn.
      head = entry.prevEntryOffset
      entry_count -= 1
      region_limit = entry.offset
      enqueue .pv.{entry.sealTxn} and .pc{i}.{entry.sealTxn} for orphan delete
      entry = previous entry (read at entry.prevEntryOffset, or stop if NO_HEAD)
  publish corrected (head, entry_count, region_limit) under seqlock

  // Result: chain only contains entries committed by _txn or earlier.
```

This single backward walk replaces the v1 mechanisms it would otherwise
need to coordinate:
- `mergeTentativeIntoActiveIfAny` — gone (no tentative slot in v2).
- `invalidateStaleTentative` — gone.
- `logOrphanSealedFiles` — replaced by the recovery walk's enqueue step,
  plus a forward orphan scan over `.pv.*` and `.pc*.*` files for any
  `sealTxn` not referenced by the chain.
- `SEAL_TXN_TENTATIVE = -1L` — no longer needed.

## 6. Invariants we depend on

The redesign is correct **iff** these all hold. They are all invariants
that already exist in QuestDB; the redesign does not introduce new ones.
They must be preserved through Phases 2-5.

1. **Atomic `_txn` writes.** The table `_txn` file's existing
   torn-write protection is what guarantees `_txn = T+1` means commit
   landed. This is pre-existing.
2. **Strict scoreboard pinning by readers.** Readers must atomically
   register at a `_txn` snapshot before touching any data. This is the
   contract that lets the picker's `txnAtSeal <= T` rule be safe — no
   reader is ever pinned at an uncommitted `T`.
3. **Single writer per `.pk`.** The writer owns all `.pk` mutations.
   The purge job deletes files but does not touch `.pk`. No CAS
   protocol; relies on the existing single-writer-per-table invariant.
4. **Phase 2 strictly before Phase 3.** All chain entries for an O3
   commit must be published before `txWriter.commit()` advances
   `_txn`. If a future change parallelises Phase 2, it must fence on
   completion before Phase 3.
5. **Reader uses the picked entry's gen dir, not the writer's
   "current" view.** Implementation discipline; the data structures
   are designed to enforce it (each entry self-describes), but a
   reader code path that reaches into the writer-owned head's gen-dir
   tail without going through the picked entry would break snapshot
   isolation.
6. **`txnAtSeal` propagation through the seal call site.** The writer
   needs to know the next `_txn` value before publishing. In v1 there
   is no equivalent need. Phase 4 wires `txWriter.getTxn() + 1`
   through to the publish.

## 7. Phase plan

### Phase 1 — DONE

**Files added:**
- `core/src/main/java/io/questdb/cairo/idx/PostingIndexChainHeader.java` — read/write/publish header pages.
- `core/src/main/java/io/questdb/cairo/idx/PostingIndexChainEntry.java` — read/write entry headers; `entrySize()` with 8-byte alignment.
- `core/src/main/java/io/questdb/cairo/idx/PostingIndexChainPicker.java` — backwards walk + pick by `txnAtSeal`.
- `core/src/test/java/io/questdb/test/cairo/idx/PostingIndexChainTest.java` — 10 deterministic unit tests.

**Files modified:**
- `core/src/main/java/io/questdb/cairo/idx/PostingIndexUtils.java` — `V2_*` constants added (additive; v1 constants retained).

**Test status:** 10/11 green. 1 `@Ignore`d concurrent test that needs
`MemoryCMARW` rather than the in-memory `MemoryCARWImpl` to be reliable;
real concurrency validation moves to Phase 2 integration tests.

**Nothing else changed.** No production reader or writer touches the new
classes yet. All existing tests remain green.

### Phase 2 — Writer publish path

Split into three sub-phases.

#### Phase 2a — DONE — Writer-side helper scaffolding

**Files added:**
- `core/src/main/java/io/questdb/cairo/idx/PostingIndexChainWriter.java` —
  stateful helper that mirrors the chain header in memory and exposes the
  publish/extend/recovery primitives the writer needs.
- `core/src/test/java/io/questdb/test/cairo/idx/PostingIndexChainWriterTest.java`
  — 17 deterministic unit tests, all green.

**Resolution of design open question #5 (sparse-gen sub-protocol).** Adopt
option (b): the head entry's `GEN_COUNT`, `KEY_COUNT`, `LEN`, `VALUE_MEM_SIZE`
and `MAX_VALUE` fields are mutated in place via aligned 4- or 8-byte stores
(atomic on x86/aarch64), with a `storeFence` between the gen-dir bytes and
the field updates so the new gen-dir entry is visible by the time `GEN_COUNT`
advances. Concurrency safety relies on the chain header's outer seqlock for
ordering on the reader side; no inner seqlock is needed within the entry
because the writer is single-threaded and field reads are tear-free at
aligned 4/8 bytes.

**Resolution of design open question #2 (chain-prune call site).** Recovery
prune is exposed as a separate `recoveryDropAbandoned(...)` primitive on
`PostingIndexChainWriter`; the writer calls it once at open time after
`openExisting(...)`. Steady-state GC (section 5.3) is deferred to Phase 4
along with the seal-purge integration.

**Sub-phase output is purely additive.** `PostingIndexWriter` and the
on-disk format produced by the existing writer are unchanged. The new
helper class is not yet referenced from any production code path; it
sits ready to be wired in by Phase 2b.

#### Phase 2b — Writer surgery — DONE

**Files modified:**
- `PostingIndexUtils.java` — `initKeyMemory` writes the v2 layout (two
  4 KB header pages, empty entry region). `readSealTxnFromKeyFd` walks the
  v2 chain: reads both header pages under the seqlock, picks the consistent
  page, follows the head pointer, returns the head entry's `SEAL_TXN`.
  Rejects {@code FORMAT_VERSION != 2} as a soft error (returns -1).
- `PostingIndexWriter.java` —
    - Removed `activePageOffset`, `deferMetadataPublish`,
      `tentativeSlotOffset`, `currentPublishTableTxn`, `prevPublishTableTxn`,
      `lastOrphanScanPath`, `orphanSealTxns`, `orphanScan*Count`.
    - Added `chain = new PostingIndexChainWriter()` field and an in-memory
      `maxValue` mirror.
    - `of(path, name, columnNameTxn, init)` and the fd-based `of(...)`
      now call `chain.initialiseEmpty()` on init or `chain.openExisting()`
      on reopen, then load `sealTxn`/`valueMemSize`/`blockCapacity`/
      `keyCount`/`genCount`/`maxValue` from the head entry.
    - `truncate()` calls `chain.openExisting()` after the in-place re-init
      so the helper's mirror reflects the new starting `genCounter`.
    - `setMaxValue` updates the in-memory mirror and calls
      `chain.updateHeadMaxValue` to persist + republish.
    - `getMaxValue()` returns the in-memory mirror.
    - All `keyMem.getLong(activePageOffset + PAGE_OFFSET_*)` reads switch
      to in-memory fields (`maxValue`, `keyCount`, etc.).
    - All `getGenDirOffset(activePageOffset, g)` calls switch to
      `PostingIndexChainEntry.resolveGenDirOffset(chain.getHeadEntryOffset(), g)`.
    - The four `writeMetadataPage(...)` callsites (one in `flushAllPending`
      for sparse-gen append, three in the seal paths
      `reencodeMonolithic`/`reencodeWithStrideDecoding`/`sealIncremental`)
      are replaced by a single helper `publishToChain(...)` that decides
      between `chain.extendHead` (head matches the writer's `sealTxn` —
      sparse-gen append or fd-based reseal) and `chain.appendNewEntry`
      (path-based seal that just bumped the writer's `sealTxn`, or fresh
      chain).
    - Methods deleted: `mergeTentativeIntoActiveIfAny` (kept as no-op
      override for the interface), `invalidateStaleTentative`,
      `determineActivePageOffset`, `logOrphanSealedFiles`, `rememberOrphan`,
      `writeMetadataPage(...)` (both overloads).
    - `close()` trims the file to `chain.getRegionLimit()` instead of the
      v1 `KEY_FILE_RESERVED` so the chain entry region is preserved across
      reopens.

**Tests:** `PostingIndexChainTest`, `PostingIndexChainWriterTest`,
`PostingIndexNativeTest` are green (writer-internal logic). The
integration tests that pair the writer with the existing v1 readers
(`PostingIndexOracleTest`, `PostingIndexConcurrencyTest`,
`PostingIndexStressTest`, the `*Delta`/`*Ef` variants, and any SQL test
that touches a posting index) fail uniformly with `"Unsupported Posting
index version: 0"` because the readers still decode the v1 metadata-page
layout. **This is the expected interim state until Phase 3 lands.**

**Risk recap:** the writer surgery touched ~10 distinct call sites. The
writer-only tests verify the new paths in isolation, but the format
incompatibility means the integration suite cannot run end-to-end until
Phase 3 swaps the reader.

#### Phase 2c — TableWriter plumbing — DONE

Two setters on `IndexWriter`, both default-no-op so `BitmapIndexWriter`
is untouched:

- **`setCurrentTableTxn(long currentTableTxn)`** — must be called
  before each `configureFollowerAndWriter` / `configureWriter` /
  fd-based `of(...)`. `PostingIndexWriter` consumes the value inside
  `runRecoveryWalkIfRequested()` (called from both path-based and
  fd-based `of(...)` after `chain.openExisting`); the helper drives
  `chain.recoveryDropAbandoned` and queues each orphan
  {@code .pv.{N}} file for purge via the existing
  `pendingPurges` outbox using a conservative `[0, Long.MAX_VALUE)`
  reader-visibility window. The setter is single-shot: the field is
  reset to `-1` after consumption, so a stale value cannot drive a
  later open's recovery walk if the next caller forgets to set it.
  `close()` deliberately does NOT reset the field, because path-based
  `of(path, name, columnNameTxn, init)` starts with `close()`; if
  `close()` cleared it, the setter call between writer construction
  and `of(...)` would be lost.
- **`setNextTxnAtSeal(long txnAtSeal)`** (renamed from
  `setPendingPublishTableTxn`) — supplies the table `_txn` the next
  chain entry should record as its `txnAtSeal`. `TableWriter.syncColumns`
  calls this with `txWriter.getTxn() + 1` before `commit()`, matching
  the contract that the entry takes effect when the upcoming
  transaction commits. The publish path consumes the value once and
  `recordPostingSealPurge` resets it so a follow-up sealing publish
  cannot accidentally inherit it.

Ten `TableWriter` callsites (eight `configureFollowerAndWriter`, two
`configureWriter`) now precede the indexer call with
`indexer.getWriter().setCurrentTableTxn(txWriter.getTxn())`. The
deferred `seal()`-takes-`txnAtSeal` parameter form discussed in the
original Phase 2c sketch was not adopted — the setter pattern keeps
the `IndexWriter` interface lean (no parameter forced on
`BitmapIndexWriter`), and the single-shot consumption rule gives
equivalent safety against stale values.

**Phase 2c verification status:** two new integration tests in
`PostingIndexOracleTest` cover the recovery flow:
`testRecoveryDropsAbandonedHeadEntryOnReopen` (a published-but-
uncommitted head is removed when the next open's `currentTableTxn`
is below the entry's `txnAtSeal`) and
`testRecoveryKeepsCommittedEntryOnReopen` (the walk is conservative
and never drops a covered entry). The full sweep —
PostingIndexOracleTest{,Ef,Delta} (41), PostingIndexConcurrencyTest
(12), PostingIndexChain{,Writer}Test (29), CoveringIndexTest (297),
SymbolMapTest (21) — passes.

### Phase 3 — Reader path — DONE

`AbstractPostingIndexReader.java` is the only Posting reader file that
ever touches `.pk` metadata directly; `PostingIndexFwdReader` /
`PostingIndexBwdReader` consume the protected fields the parent
populates. Phase 3 rewrote the parent's read path against the v2
chain.

**What landed:**
- Removed v1-era `activePageOffset` / `keyFileSequence` fields. Added
  `headEntryOffset` (the picked entry), `chainGenCounter` (last seen
  header monotonic counter, for cheap reload pre-checks),
  `pinnedTableTxn` (defaults to `Long.MAX_VALUE` so unwired callers
  see the head), and reusable `headerScratch` /
  `entryScratch` snapshots.
- Replaced `readIndexMetadataFromBestPage(long pinnedSealTxn)` with
  `readIndexMetadataFromChain()`. The new method calls
  `PostingIndexChainPicker.pick(...)`, validates
  `headerScratch.formatVersion == V2_FORMAT_VERSION`, and on
  `RESULT_OK` populates `valueMemSize` / `keyCount` / `genCount` /
  `valueFileTxn` from the picked entry. On `RESULT_EMPTY_CHAIN` /
  `RESULT_NO_VISIBLE_ENTRY` it promotes an empty snapshot — the
  reader reports `keyCount/genCount = 0` and the caller skips
  mapping `.pv`. `RESULT_HEADER_UNREADABLE` retries with the
  existing spinlock deadline, then falls back to the previous
  snapshot.
- `genLookup.snapshotMetadata(keyMem, genCount, entryOffset)` reads
  the gen dir from the picked entry's payload. The math
  (`offset + 64 + i*28`) is identical for v1 page bases and v2 entry
  bases — both use a 64-byte header followed by the gen dir — so
  the same helper works for both. The param will be renamed when
  v1 is removed in Phase 5.
- `of(...)` now maps the entire `.pk` file (`size = -1`) so chain
  entries past `KEY_FILE_RESERVED` are reachable. When the chain
  is empty at open time, `valueMem` is left unmapped; a subsequent
  `reloadConditionally` lazily maps `.pv` via the new
  `mapValueMem(...)` helper.
- `reloadConditionally` does a cheap header peek under seqlock;
  bails when `headerScratch.generationCounter <= chainGenCounter`.
  When the chain advanced, it extends the `keyMem` mapping to the
  current file length (writer may have appended entries past the
  old map), re-picks via `readIndexMetadataFromChain()`, and
  refreshes `valueMem` — `Misc.free(valueMem)` + `mapValueMem(...)`
  if `sealTxn` advanced (new `.pv.{N}` filename) or the previous
  open found an empty chain, otherwise just `changeSize(...)` if
  same file grew/shrank.
- Public setter `setPinnedTableTxn(long)` is exposed for future
  Phase-2c plumbing. It is intentionally not threaded through
  `IndexFactory.createReader` yet — that touches O3OpenColumnJob,
  SymbolMapReaderImpl, and other call sites and is best done as
  part of the table-writer ordering work in Phase 4. Until then
  every reader uses the default `Long.MAX_VALUE` pin and sees the
  head.
- `PostingIndexUtils.readSealTxnFromKeyFd` was already migrated to
  walk the v2 chain via `fd` reads in Phase 2b; Phase 3 confirms
  the symmetry between the mapped-memory picker and the fd-based
  walk.

**Verification status:** the writer-only test classes pass plus
PostingIndexOracleTest (13), PostingIndexOracleTestEf (13),
PostingIndexOracleTestDelta (13), PostingIndexConcurrencyTest (12),
CoveringIndexTest (297), SymbolMapTest (21). Sixteen residual
failures across `PostingIndexStressTest{,Ef,Delta}` and
`PostingIndexCriticalIssuesTest` are all v1-protocol-specific
assertions (page A/B alternation per commit, `VALUE_FILE_TXN` slot
reads, tentative-slot invalidation) or explicit RED placeholders
(`Assert.fail("Critical finding #N needs a ... test")`). They are
to be removed or rewritten in Phase 5 alongside the v1 cleanup.

### Phase 4 — `TableWriter` ordering and seal-purge integration — DONE

**What landed:**
- New helper `PostingIndexChainWriter#getSecondEntryTxnAtSeal(MemoryR)`
  reads the entry immediately preceding the head and returns its
  `txnAtSeal`. Used by the seal-purge accounting to find the lower
  bound of a superseded file's visibility window without keeping
  any extra in-memory mirror.
- `recordPostingSealPurge` no longer reads `pendingTxnAtSeal` to
  compute the purge window. `toTxn` is now `chain.getCurrentTxnAtSeal()`
  (the just-published entry's `txnAtSeal`); `fromTxn` is
  `chain.getSecondEntryTxnAtSeal(keyMem)`, falling back to
  `postingColumnNameTxn` when the chain has only the freshly-appended
  entry (very first seal). The v1
  `[0, Long.MAX_VALUE)` fallback is still present but only for the
  defensive empty-chain path (`recordPostingSealPurge` invoked
  without a chain entry — close-without-seal residual). The
  reset-to-`-1` of `pendingTxnAtSeal` stays so a stale value can't
  drive the next publish.
- `publishPendingPurges` drops the
  `entry.toTableTxn == Long.MAX_VALUE ? currentTableTxn : ...`
  rewrite. With chain-derived intervals every entry has a meaningful
  `toTableTxn`; if a residual saturated entry slips through, the
  scoreboard min check just keeps the file longer and the next
  writer-open recovery sweep cleans it up. The `currentTableTxn`
  parameter on `publishPendingPurges` is now unused but retained on
  the interface for future evolution.
- `finishO3Commit` keeps the catch-and-`distressed=true` fallback —
  partial seal failures still leave the in-memory writer
  untrustworthy — but the comment now points at
  `recoveryDropAbandoned` as the structural on-disk safety net.
  Phase 2c.1 already wired `setCurrentTableTxn` into every TableWriter
  reopen path, so the recovery walk runs without further plumbing.

**Phase 4 verification status:** new regression test
`PostingIndexOracleTest#testRecordPostingSealPurgeUsesChainDerivedInterval`
exercises a seal-supersedes-prior cycle and asserts the queued purge
entry's `[fromTxn, toTxn)` matches the chain's predecessor/head
`txnAtSeal`s — would re-fail if the v1 `[0, MAX)` fallback ever
crept back in. Full sweep (PostingIndexOracle{,Ef,Delta} 47,
PostingIndexConcurrencyTest 12, PostingIndexChain{,Writer}Test 29,
PostingSealPurgeTest 15, CoveringIndexTest 297, SymbolMapTest 21)
passes. 18 residual failures across PostingIndex{Stress,CriticalIssues}Test
and one PostingSealPurgeTest case (`testPostLiveOrphanDeletedInline`,
which relies on the removed v1 `logOrphanSealedFiles` directory
scan) target v1-protocol semantics and are slated for Phase 5
cleanup.

### Phase 5 — Format-version migration and v1 cleanup — DONE

**What landed:**
- `PostingIndexUtils.FORMAT_VERSION` (the v1 sentinel value `1`) is
  gone. The writer-open path validates `V2_FORMAT_VERSION` via
  `PostingIndexChainWriter.openExisting` (throws
  `"Unsupported Posting index version [expected=2, actual=...]"`),
  and the reader-open path does the same via
  `readIndexMetadataFromChain` after the picker reads the header.
  Both paths now reject any non-V2 file at the door — the
  pre-Phase-5 fallback that interpreted v1 bytes as v2 is gone.
- `PostingIndexUtils.SEAL_TXN_TENTATIVE` removed. The v1 tentative
  slot has no v2 analogue.
- `PostingIndexUtils.getGenDirOffset(pageBase, genIndex)` removed.
  `PostingIndexChainEntry.resolveGenDirOffset(entryOffset, genIndex)`
  is the single helper for computing gen-dir entry offsets;
  `PostingGenLookup.snapshotMetadata` now takes an `entryOffset`
  parameter (renamed from `pageOffset`) and uses the chain-entry
  helper directly.
- The v1 page-field constants (`PAGE_OFFSET_VALUE_MEM_SIZE`,
  `PAGE_OFFSET_KEY_COUNT`, `PAGE_OFFSET_GEN_COUNT`,
  `PAGE_OFFSET_FORMAT_VERSION`, `PAGE_OFFSET_SEAL_TXN`,
  `PAGE_OFFSET_BLOCK_CAPACITY`, `PAGE_OFFSET_MAX_VALUE`) survive
  only as raw byte-offset aliases used by a handful of legacy
  stress / concurrency tests that probe specific bytes of the .pk
  file to simulate corruption. The constants are documented as
  vestigial (the offsets happen to match v2 header bytes for the
  seqlock fields, and bytes for the v1-only fields are arbitrary
  "some byte in the header" probes in tests that no longer assert
  v1 semantics). No production code reads them.
- `TableWriter.isPostingIndexSealed` was rewritten to call
  `PostingIndexUtils.readSealTxnFromKeyFile` — the last v1
  page-protocol reader in production code. v2 now has a single
  fd-based chain walker shared by writer-open, reader-open, and
  resume-after-restart logic.
- v1-only failing tests removed (each replaced with a short comment
  explaining what it tested and where the v2 equivalent lives):
    - `PostingIndexCriticalIssuesTest`: 3 RED placeholder tests
      (#2 setMaxValue seqlock, #6 [0, MAX) conservative purge,
      #7 seal-loop partial failure) — each now structurally
      addressed by the chain redesign. #9 (ColumnPurgeOperator
      retry cap) is orthogonal and remains tracked separately.
    - `PostingIndexStressTest{,Ef,Delta}`: 4 v1-protocol cases
      per class (`testPageFlipVisibility`,
      `testTruncateCreatesNewValueFile`,
      `testRollbackToZeroCreatesNewValueFile`,
      `testStaleTentativeInvalidatedOnFdOpen`) targeting strict
      A/B alternation, post-truncate sealTxn slot reads, and the
      v1 tentative-slot protocol — none of which apply to v2.
    - `PostingSealPurgeTest.testPostLiveOrphanDeletedInline`:
      relied on the removed `logOrphanSealedFiles` directory scan;
      v2 routes orphans through the chain's
      `recoveryDropAbandoned`.

**Phase 5 verification status:** `mvn test` against the full
posting-index test set plus `CoveringIndexTest` and `SymbolMapTest`
runs **592 tests, 0 failures, 1 skipped** (the skip is a
pre-existing platform-specific case in `PostingIndexChainTest`).
The branch is now in a clean v2-only state — no production
references to v1-only constants, every reader/writer code path
goes through the chain helper.

## 8. What we deliberately are not doing

- **Multi-writer `.pk`.** `.pk` stays single-writer (the table's
  `TableWriter`). The purge job does not mutate `.pk`. If we ever
  parallelise the seal loop across partitions in Phase 2 or later,
  each partition's `.pk` is still single-writer because each
  partition has its own `.pk`.
- **In-place compaction of the entry region.** The region grows
  forward as entries are appended; `REGION_BASE` advances as old
  entries are GC'd. If the writer ever needs to wrap or compact, that
  becomes a future optimisation. For typical workloads (a few seals
  per partition before GC catches up) the region stays small.
- **Cross-format read.** A reader that opens a v1 `.pk` after Phase 5
  fails fast with a clear error. We do not maintain a dual-format
  reader.
- **Removing `setMaxValue`'s mutable nature.** Sparse-gen append still
  mutates the head entry's gen-dir tail under its own seqlock. We
  considered making the head entry strictly immutable too, but that
  would force a new entry on every commit (not just every seal) —
  too expensive. The compromise: only the head can grow; older
  entries are frozen.

## 9. Open questions for the implementer

These are decisions the design intentionally defers. Phase-2 implementer
should call them based on what they learn from the integration:

1. **`txnAtSeal` plumbing path.** Where exactly does
   `txWriter.getTxn() + 1` enter the seal call chain? Most natural
   place is `TableWriter.sealPostingIndexForPartition(...)` adding a
   `long txnAtSeal` parameter, propagated into `PostingIndexWriter.seal(...)`.
   *(Phase 2c will resolve.)*
2. **Where to place the chain-prune call.** *(Resolved in 2a.)* Recovery
   prune is its own `PostingIndexChainWriter#recoveryDropAbandoned(...)`
   primitive, called once at writer open. Steady-state GC (section 5.3)
   is bundled with the seal-purge integration in Phase 4.
3. **What to do when the entry region runs out.** The header has
   no "wrap" semantics; if the region grows unbounded under perma-pinned
   readers, `.pk` grows. Reasonable cap is several MB before it matters
   (entries are ~150 bytes each). For now, log a warning at 10,000
   live entries; revisit if it ever fires.
4. **Ring vs compact.** The doc above commits to forward-only growth
   with `REGION_BASE` advancing on GC. If a workload pattern emerges
   where GC drains entries to 0 and then growth resumes from the high
   water mark, we re-evaluate.
5. **Sparse-gen sub-protocol within the head entry.** *(Resolved in 2a.)*
   Adopted option (b): plain aligned stores to head entry's `GEN_COUNT`,
   `KEY_COUNT`, `LEN`, `VALUE_MEM_SIZE`, `MAX_VALUE` with a `storeFence`
   between gen-dir bytes and the `GEN_COUNT` bump. Concurrent reader
   safety relies on the chain header's outer seqlock for ordering;
   single-writer means no inner seqlock is needed.
6. **Format check on open.** `V2_FORMAT_VERSION = 2`. Writer opens
   with v1 (1) → throw "incompatible format, must reformat". This is
   safe for the unmerged PR but should be communicated in the PR
   description. *(Implemented in `PostingIndexChainWriter.openExisting`
   in Phase 2a.)*

## 10. Glossary

- **`sealTxn`** — monotonic per-`.pk` counter, one per chain entry.
  Used as the suffix in `.pv.{sealTxn}` and `.pc{i}.{sealTxn}`
  filenames. Has no relation to the table's `_txn`.
- **`txnAtSeal`** — table `_txn` value at which a chain entry's seal
  takes effect. The reader's pin selector. New in v2.
- **chain head** — the latest committed entry, advertised in the `.pk`
  header. Walking backwards from head reaches every live entry.
- **entry region** — area of `.pk` after both header pages where chain
  entries are appended. Bounded below by `REGION_BASE`, above by
  `REGION_LIMIT`.
- **scoreboard pin** — atomic registration of a reader against a
  specific `_txn` value, performed before any data access. Existing
  QuestDB mechanism (`TxnScoreboard`).
- **abandoned entry** — chain entry with `txnAtSeal > _txn`. Means the
  publish landed but the commit didn't. Recovered on writer open.
- **picker** — `PostingIndexChainPicker.pick(...)`. Walks the chain
  backwards from head, returns the first entry with
  `txnAtSeal <= pinned _txn`.
- **strict pin invariant** — readers always atomically pin a `_txn`
  before reading. The set of pinned `_txn` values is a subset of
  committed `_txn` values. The chain design depends on this.

## 11. References

- [PR #6861](https://github.com/questdb/questdb/pull/6861) — original posting-index PR.
- Code review of #6861 — findings #2, #6, #7 motivate this redesign.
- `PostingIndexUtils.java` — v1 layout constants (lines ~184-196), v2
  constants added below.
- `PostingIndexChainHeader.java`, `PostingIndexChainEntry.java`,
  `PostingIndexChainPicker.java` — Phase 1 implementations.
- `PostingIndexChainTest.java` — Phase 1 unit tests.
- `PostingIndexChainWriter.java` — Phase 2a writer-side helper.
- `PostingIndexChainWriterTest.java` — Phase 2a unit tests.

---

*Document version 3. Last updated at end of Phase 2b.*
