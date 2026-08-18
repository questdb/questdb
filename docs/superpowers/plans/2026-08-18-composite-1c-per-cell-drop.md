# Composite 1C — Per-Cell DROP PARTITION Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `ALTER TABLE t DROP PARTITION LIST '<day>/<cell>'` remove **that cell only**, delivering the lifecycle spec's addressing rule — *a partition predicate selects cells; dropping every cell of a day drops the day.*

**Architecture:** The removal machinery already exists and is proven: `TxWriter.removeAttachedPartitions(ts, cellKey)`, `TxReader.getAnyPartitionIndexByTimestamp`, and the drain loops in `removePartition`/`forceRemovePartitions` that sub-projects 1B and 1D built and verified. What is missing is **plumbing**: nothing between the SQL statement and the writer carries a cellKey. `AlterOperationBuilder.addPartitionToList(long timestamp, int position)` takes only a timestamp, `AlterOperation.extraInfo` is a `LongList` of `(timestamp, partitionNamePosition)` **pairs**, and `applyDropPartition` calls `svc.removePartition(partitionTimestamp)`. 1C threads a cellKey through that chain.

**Tech Stack:** Java 25 (`JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64`), Maven offline (`mvn -o -pl core`), JUnit 4, `QDB_TEST_TMPDIR=/dev/shm`.

## Why this is the crux

The lifecycle spec calls the addressing decision "the crux… it propagates into sub-project 3 (parquet
conversion), 6 (Enterprise tiering) and 7 (materialized views)". Those three consume whatever
addressing 1C establishes, so getting the representation right matters more than getting the drop
working quickly.

## The measured starting point

1B measured this exact statement destroying data:

```
DROP PARTITION LIST '2023-01-01/E0'      -- names ONE cell
before: E0:1, E1:1, E2:1                 after: EMPTY
```

It is refused today by `refuseCellQualifiedPartitionName`. **That refusal is 1C's acceptance
criterion inverted**: when 1C lands, the same statement must remove E0 and leave E1 and E2 untouched.

## Global Constraints

- **Cardinal rule:** composite behaves exactly like its plain twin, or fails LOUDLY. No silent path.
- **Invariant 1:** plain-table behaviour is byte-identical, including the WAL-serialized form of an
  `AlterOperation` for a plain table.
- **Atomicity (spec §5.1):** removing N cells is ONE `_txn` commit.
- Negative controls use `cp`/restore — never `git stash`/`git checkout` in this worktree.
- **Never run two `mvn` commands against this worktree at once.**
- Long suites are killed intermittently in this environment; run them in small batches and record
  which batches actually completed rather than claiming a full-set pass.

---

### Task 1: Decide the wire representation — and check whether it is a format break

**Files:**
- Investigation. Produces `.superpowers/sdd/sp1c-task-1-wire-format.md`.

**This task exists because `extraInfo` crosses the WAL boundary.** An `AlterOperation` for a WAL table
is serialized into the WAL and replayed by `ApplyWal2TableJob`. Changing `extraInfo`'s stride from 2
to 3 is therefore not a private refactor — it is a change to what is written into WAL segments, and a
segment written by an older build must still replay on a newer one.

- [ ] **Step 1: Establish whether the stride is self-describing**

`_txn` solved this exact problem in Plan 3b with a self-describing stride marker
(`getLongsPerAttachedPartition`), which is why composite could widen that record without breaking
plain readers. Determine whether `AlterOperation`'s serialization has an equivalent, or whether the
reader infers pair-ness from `extraInfo.size() / 2`.

`applyDropPartition`'s loop is literally `for (int i = 0, n = extraInfo.size() / 2; ...)`, so at
minimum the READER assumes stride 2. Find every other reader of `extraInfo` for
`DROP_PARTITION`/`FORCE_DROP_PARTITION`/`ATTACH`/`DETACH` before changing anything.

- [ ] **Step 2: Choose between three representations, and write down why**

1. **Widen the stride to 3** `(timestamp, position, cellKey)`. Uniform, but touches every reader and
   is a WAL format change unless the stride is self-describing.
2. **A sentinel cellKey in a parallel list**, mirroring how `partitionRemoveCandidates` already
   carries `(timestamp, nameTxn, cellKey)` triples internally.
3. **Encode "all cells" as a distinct cellKey value** (e.g. `-1`) so a whole-day drop keeps the
   existing shape and only a cell-qualified drop carries a real key.

Option 3 is worth serious weight: it keeps a plain table's serialized operation **byte-identical**,
which is invariant 1 stated at the wire level, and whole-day drop is the overwhelmingly common case.

- [ ] **Step 3: Record the decision with its compatibility argument**

An unreleased feature can change its own on-disk formats freely, but `AlterOperation` is **shared with
plain tables**, which are released. State explicitly which parts of the change are composite-only.

---

### Task 2: Resolve a cell-qualified name to a cellKey

**Files:**
- Modify: `core/src/main/java/io/questdb/griffin/SqlCompilerImpl.java` (`alterTableDropConvertDetachOrAttachPartitionByList`)

**Interfaces:**
- Consumes: the reader's cell registry — `TableReader#getCompositeDictionaries().cellRegistry()`, already used by `ShowPartitionsRecordCursorFactory` and by `isRoutedCompositeTable`.
- Produces: `(timestamp, cellKey)` for Task 3 to carry.

- [ ] **Step 1: Parse `<day>/<cell>` into its two parts**

The day component parses exactly as today. The cell component is the **rendered cell segment** —
`renderCellSegment`'s output, e.g. `E0` or `exch=BTC` depending on layout. Resolve it by rendering
each attached cellKey for that day and comparing, rather than by parsing the segment back into
dimension values: rendering is the authority (`TableWriter#renderCellSegment` /
`TableReader#renderCellSegment`), and round-tripping through a parser would invent a second one.

- [ ] **Step 2: Refuse an unknown cell loudly**

A name that resolves to no attached cell must throw at the statement, naming the day and the cell.
Silently dropping nothing is the failure mode the cardinal rule forbids.

- [ ] **Step 3: Test both layouts**

`LAYOUT PLAIN` renders `E0`; the Hive layout renders `exch=E0`. Both must resolve. There is existing
coverage of the two layouts in `CompositeIntervalHiveLayoutTest` — follow its setup rather than
inventing a third.

---

### Task 3: Carry the cellKey to the writer

**Files:**
- Modify: `core/src/main/java/io/questdb/griffin/engine/ops/AlterOperationBuilder.java` (`addPartitionToList`)
- Modify: `core/src/main/java/io/questdb/griffin/engine/ops/AlterOperation.java` (`applyDropPartition`)
- Modify: `core/src/main/java/io/questdb/cairo/TableWriter.java` (`removePartition`)
- Modify: the `MetadataService` interface if `removePartition` gains an overload

- [ ] **Step 1: Implement Task 1's decision**

- [ ] **Step 2: `removePartition(timestamp, cellKey)`**

The existing `removePartition(long)` becomes the "all cells of the day" case and must stay
byte-identical for plain. The drain loop 1B built already removes the lowest surviving cell each pass;
a cell-qualified drop removes exactly one `(ts, cellKey)` record instead of draining.

- [ ] **Step 3: The day container**

Spec §5.1: when the LAST cell of a day is removed, the day container goes too. 1B implemented that
with two guards (`hasAnyAttachedPartitionForTimestamp` false AND the directory physically empty,
because `ff.rmdir` is recursive). A per-cell drop that removes a non-last cell must leave the
container alone — which the existing guards already handle, but assert it rather than assume.

---

### Task 4: Acceptance, and the destructive case inverted

**Files:**
- Modify: `core/src/test/java/io/questdb/test/cairo/CompositeDropPartitionWholeDayTest.java`
- Test: `core/src/test/java/io/questdb/test/cairo/CompositePerCellDropTest.java` (create)

- [ ] **Step 1: Invert 1B's refusal test**

`testCellQualifiedDropIsRefusedAndChangesNothing` asserts the refusal. It becomes the acceptance
test: `DROP PARTITION LIST '2023-01-01/E0'` removes E0's rows and directory, and leaves E1 and E2
with **every** row. Keep the "changes nothing" half as a separate test for an unknown cell name.

- [ ] **Step 2: Dropping every cell one at a time drops the day**

The spec's rule is that dropping all cells of a day drops the day. Drop E0, then E1, then E2 as three
statements, and assert the day container is gone after the third and present after the first two.

- [ ] **Step 3: The plain twin is unaffected**

There is no plain equivalent of a cell-qualified name, so the twin comparison here is against the
composite table's own prior state, not against `p`. Say so in the test — a twin assertion that cannot
fail is worse than no assertion.

- [ ] **Step 4: Full suites, in small batches, then griffin**

---

## Self-Review

**Spec coverage.** Implements the LIST half of spec §3 and the addressing rule of §2. It does **not**
implement §4's predicate columns (`WHERE exchange = 'BTC'`), which need the drop predicate's synthetic
metadata to expose dimension columns and `table_partitions()` to match — a separate slice, and one
whose value depends on this one's representation choice. That ordering is deliberate: LIST is exact
addressing and needs no new predicate machinery, so it proves the representation before the predicate
surface is built on top of it.

**Placeholder scan.** Task 3 does not pre-write its code because Task 1's representation decision
determines it. That is the same investigation-gate structure that saved 1A (wrong target file), 1B
(gate would have been lifted over a data-loss path) and 1D (hypothesis falsified). The pattern has now
paid for itself four times in one session.

**Known risk, stated rather than discovered.** `AlterOperation` is shared with plain tables and
crosses the WAL boundary. This is the first change in the composite project to touch a **released**
serialization path — every prior format change (`_txn` stride, `_cell` registry, cell directory
layout) was composite-only and therefore free to change while unreleased. Task 1 exists to establish
whether option 3 (a sentinel cellKey, keeping the plain wire form byte-identical) avoids the problem
entirely. If it does not, the compatibility argument must be written down before any code is changed.
