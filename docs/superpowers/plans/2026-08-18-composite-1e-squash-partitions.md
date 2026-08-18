# Composite 1E — SQUASH PARTITIONS Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make split-fragment squash cell-aware, closing the last of sub-project 1's partition-lifecycle gates that does not need new on-disk machinery — both the explicit `ALTER TABLE … SQUASH PARTITIONS` and the automatic squash that runs during commit.

> **This plan BLOCKS `DETACH PARTITION`.** Measured 2026-08-18: with the DETACH gates lifted, the
> statement is accepted and the table then SUSPENDS on a different gate entirely —
> `composite partitioning does not yet support SQUASH PARTITIONS`, thrown from
> `TableWriter#detachPartition` (~2381). `DETACH` calls squash internally, so no amount of work on
> DETACH's own machinery (the nested `.detached` container, re-interning by value) can land until
> squash is cell-aware. The lifecycle spec lists DETACH and ATTACH as independent items; that
> ordering constraint is not in it.

**Entry points, mapped 2026-08-18.** `squashPartitionForce` is a single gate point covering
`squashPartitions()` (the explicit ALTER), `detachPartition`, `convertPartitionNativeToParquet`,
`preparePartitionForParquetConversion`, `switchNativePartitionWithParquet` and
`squashAllPartitionsIntoOne`. The automatic path is different: `housekeep` calls
`squashSplitPartitions` **directly**, which is why it skips rather than throws. Two entry points, two
behaviours — do not assume one gate covers both.

**Root cause, from `squashPartitionForce`'s own comment:** the forward scan decides whether the NEXT
attached entry is a split sibling purely by calendar-FLOOR equality
(`getLogicalPartitionTimestamp`) — but two CELLS of one day share the exact same RAW timestamp, so a
sibling cell is indistinguishable from a true split fragment by that check. Misidentifying one
triggers a cross-cell merge through paths built with the bare 5-arg overload, which correspond to no
real directory.

**Architecture:** Measured 2026-08-18 with `CAIRO_O3_PARTITION_SPLIT_MIN_SIZE = 1`. A composite table **does** split, and the fragment is itself cell-structured:

```
c~1/2023-01-01                        <- day container
c~1/2023-01-01/E0    c~1/2023-01-01/E1  <- its cells
c~1/2023-01-01T010000-000001          <- SPLIT FRAGMENT: its own top-level container
c~1/2023-01-01T010000-000001/E0.1     <- holding only the cell that was written
```

So squashing a composite table is a merge of cells **across two containers** — `<fragment>/E0.1` into `<day>/E0` — not a merge within one directory as it is for plain. That is the whole difficulty, and it was not visible from the code.

## Two paths, not one

| Path | Today | Consequence |
|---|---|---|
| `ALTER TABLE … SQUASH PARTITIONS` | refused at the statement (1B Task 0) | user sees a clear error |
| automatic split-fragment squash during commit | **deliberate skip**, logged at INFO | fragment COUNT grows; read performance degrades |

> **Correction to this plan's first draft.** It called the automatic skip "the more important of the
> two" and implied it was a defect. Reading `squashSplitPartitions`' own rationale shows it is a
> reasoned decision, and the reasoning is sound: *"skipping this housekeeping step causes no wrong
> answers and no data loss — each split fragment remains an independently valid, fully queryable
> physical partition; the only cost is not consolidating fragment COUNT for read-performance"*, and
> throwing instead *"would suspend an otherwise healthy, high-volume composite table's ordinary
> commits purely because of a size threshold"*.
>
> That is a **performance and operability** residual, not a correctness one — a different class from
> the leaks and silent no-ops found elsewhere in this project, and it is logged rather than silent to
> the operator. It still needs fixing (unbounded fragment growth on a high-volume table is exactly why
> squash exists), but it does not outrank the explicit gate on correctness grounds, and the skip is
> the right behaviour until the merge is cell-aware.

## Global Constraints

- **Cardinal rule:** composite behaves exactly like its plain twin, or fails LOUDLY. A silent skip that lets fragments accumulate is exactly the shape this forbids.
- **Invariant 1:** plain-table behaviour is byte-identical.
- Negative controls use `cp`/restore — never `git stash`/`git checkout` in this worktree.
- **Never run two `mvn` commands against this worktree at once**; long suites are killed intermittently here, so run them in small batches and report which completed.
- **`UPDATE` is permanently banned for composite** (2026-08-18). Any correctness argument may rely on that, and must say so where it does.

---

### Task 1: A test that actually produces a fragment

**Files:**
- Test: `core/src/test/java/io/questdb/test/cairo/CompositeSquashTest.java` (create)

**This task exists because my first attempt measured nothing.** A three-row workload never splits, so the
probe issued `SQUASH` against a table with no fragments and learned only that the gate fires. The
precondition must be asserted, not assumed.

- [ ] **Step 1: Force a split and ASSERT the fragment exists**

`node1.getConfigurationOverrides().setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1)`, then
an O3 write into the middle of an already-written day. Assert a directory matching
`<day>T<time>-<seq>` exists **before** issuing anything. A squash test that squashes nothing passes
regardless — the same vacuity that made "3 of 5 column DDLs pass" wrong.

- [ ] **Step 2: Build the twin comparison around it**

The plain twin splits too, so the same workload gives a real oracle: after squashing, the twins must
agree on rows AND the composite table must have no fragment directories left.

- [ ] **Step 3: Cover the AUTOMATIC path separately**

Commit enough to trigger the in-commit squash without any `ALTER`. Assert fragments do not accumulate
across many commits. This is the path with no user-visible refusal, so it needs its own test rather
than sharing one with the explicit statement.

---

### Task 2: Cell-aware fragment merge

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/TableWriter.java` — `squashSplitPartitions` (~`17967` at spec time) and `squashPartitionForce` (~`17895`)

- [ ] **Step 1: Read both, and establish which the two paths use**

Instrument rather than assume. 1D found `FORCE DROP` and `removePartition` were separate entry points
where the plan assumed one, and 2A found only one of `ColumnPurgeOperator`'s two sites could safely
change.

> **The scan read 2026-08-18, so Step 2 starts from the actual code.** `squashPartitionForce` walks
> forward while the FLOOR matches and then squashes the whole range:
>
> ```java
> while (partitionIndex < txWriter.getPartitionCount()) {
>     long partitionTimestamp = txWriter.getPartitionTimestampByIndex(partitionIndex);
>     long logicalPartitionTimestamp = txWriter.getLogicalPartitionTimestamp(partitionTimestamp);
>     if (logicalPartitionTimestamp != lastLogicalPartitionTimestamp) {
>         if (partitionIndex > lastLogicalPartitionIndex + 1) {
>             squashSplitPartitions(lastLogicalPartitionIndex, partitionIndex, 1, true);
>         }
>         return;
>     }
>     partitionIndex++;
> }
> ```
>
> **Two distinct things are wrong for composite, not one:**
>
> 1. **The range is wrong.** A day with three cells is three consecutive entries sharing one raw
>    timestamp, so the walk includes all three and `partitionIndex > lastLogicalPartitionIndex + 1`
>    reads "there are splits here" when there are none. The range must count entries with DIFFERENT
>    raw timestamps — a true fragment has the same floor and a different raw ts, a sibling cell has
>    the SAME raw ts.
> 2. **The merge is wrong.** `squashSplitPartitions` then builds every path with the bare 5-arg
>    overload, so even a correctly-identified fragment would be merged through paths that name no real
>    directory.
>
> Fixing only (1) yields a scan that finds the right fragments and still merges them through
> nonexistent paths. Fixing only (2) yields correct paths applied to cells that were never fragments.
> Both are required, and the tests must distinguish them — a day with 3 cells AND a real fragment is
> the case that separates the two.

> ### Task 2 Step 2, RESPECIFIED FROM MEASUREMENT (2026-08-18)
>
> The original step said "merge per cell". Executing it showed *what shape* that has to take, and the
> answer changes the loop rather than patching it.
>
> **What was tried and reverted.** Fixing the range (`hasSplitFragments`, SHIPPED) plus rendering each
> path through the cell-aware overload is NOT sufficient. With the gates lifted, a 3-cell day holding
> one fragment logged three merges -- two of them SIBLING CELLS swallowed into the target. The source
> loop walks `targetPartitionIndex + 1` unconditionally, so on composite every sibling cell of the day
> reads as a fragment of the target cell.
>
> **The trap that makes this dangerous to iterate on:** the twin DATA comparison PASSES through that
> corruption. Every row survives, relocated into one cell, so rows and `count()` match the plain twin
> exactly. Only a structural assertion on the day's cell COUNT detects it. Never accept a green
> data-level test as evidence that a squash change is safe.
>
> **The unit of work is a FRAGMENT, not an attached entry.** `ColumnVersionWriter#squashPartition`
> already calls `removeAllCellsAtTimestamp(sourcePartitionTimestamp)`, whose javadoc states it discards
> the entire source partition -- *every* cell, not just cellKey 0. That is exactly right for a fragment,
> because a split fragment is its own container holding its own cells. So the correct operation is:
>
> ```
> for each FRAGMENT of the day (same calendar floor, different RAW timestamp):
>     for each cell k present in THAT FRAGMENT:          # the fragment's cells, not the day's
>         append  <fragment>/<cell k>   into   <day>/<cell k>
>     columnVersionWriter.squashPartition(dayTs, fragmentTs)   # discards the fragment wholesale
>     remove the fragment's attached entries, then its container (1B's two guards)
> ```
>
> **Why this is a redesign, not a patch.** The current loop opens ONE target frame
> (`frameFactory.open(rw, path, targetPartition, ...)`) and appends every source into it. The shape
> above needs one target frame PER CELL, opened and closed per cell pair. The following also assume
> adjacency or resolve by timestamp and must move to index-/cell-based forms:
> `targetPartitionIndex + 1` (every use), `lastPartitionSquashed = targetPartitionIndex + 2 == count`,
> `updatePartitionSizeByTimestamp` (ambiguous -- several cells share one raw timestamp), and
> `getPartitionTimestampOrMax(targetPartitionIndex + 1)`.
>
> **Do not skip the crash suites.** This is the in-commit path. `CompositeMultiCellFastAppendCrashTest`
> and `CompositeFastAppendCrashTest` run in under a second each, so there is no cost argument for
> deferring them -- run them with the change, not after it.
>
> **Acceptance tests already written** (`CompositeSquashTest`, currently `@Ignore`d, each naming the
> half it proves): `testSquashOnAThreeCellDayWithNoFragmentIsANoOp` (range half -- expected to go green
> FIRST, it already passed when the gates were briefly lifted),
> `testSquashDistinguishesFragmentsFromSiblingCells` (the discriminator: 3 cells AND a fragment),
> `testExplicitSquashMergesFragmentsIntoTheirCells`, `testAutomaticSquashDoesNotAccumulateFragments`.

- [ ] **Step 2: Merge per cell, matching fragment cells to target cells**

For each cell present in the FRAGMENT, merge into the SAME cell of the target day. A fragment holds a
**subset** of the day's cells — measured: the fragment had `E0` only while the day had `E0` and `E1` —
so the merge must iterate the fragment's cells, not the day's, and must not touch a cell the fragment
does not contain.

> **Mechanics established 2026-08-18, so the next session starts from facts rather than a search.**
>
> - **Write it as a SEPARATE method** guarded by `isRoutedComposite()`, delegated to from
>   `squashSplitPartitions`, and leave the plain loop untouched. Invariant 1 then holds by construction
>   rather than by review, and the abort path is deleting one method — which matters, because the gate
>   is the safety net: develop with it lifted, restore it if the work does not converge (done once
>   already this session).
> - **Restrict the first cut to day groups that are NOT the table's active tail**
>   (`partitionIndexHi < txWriter.getPartitionCount()`), and log the skip. That deliberately avoids
>   `lastPartitionSquashed`'s `fixedRowCount`/`transientRowCount` bookkeeping, which is the genuinely
>   crash-sensitive part. Landing mid-table fragments first is a real improvement and a much smaller
>   blast radius; the active-tail case is its own follow-up.
> - **Size updates:** use `TxWriter#updatePartitionSizeByRawIndex(rawIndex, partitionTimestampLo,
>   rowCount)` — index-based, so it is unambiguous when several cells share a raw timestamp. **Note the
>   asymmetry:** unlike the `...ByTimestamp` variants it does NOT bump `recordStructureVersion`. Check
>   whether the caller needs that bump before assuming parity.
> - **Column versions need no per-cell work:** `ColumnVersionWriter#squashPartition` already calls
>   `removeAllCellsAtTimestamp(sourceTs)`, discarding the fragment's every cell in one go. Call it ONCE
>   per fragment, after that fragment's cells have all been appended — not once per cell.
> - **Guard the adopt case.** A fragment cell whose day counterpart does not exist cannot be *appended*
>   into anything (it would be a move/adopt, not a merge). If any fragment cell lacks a day counterpart,
>   skip that fragment and log it, rather than inventing a target.
>
> **Fuzz coverage, checked (Task 3 Step 3 answered): there is NO squash generator.** Nothing under
> `core/src/test/java/io/questdb/test/fuzz/` emits `SQUASH`, so there is no fuzz classification to flip
> when the gates come off. Recording that rather than implying coverage that does not exist — the
> acceptance tests in `CompositeSquashTest` are the whole safety net for this work.

- [ ] **Step 3: Remove the fragment container when its last cell is merged**

Same shape as 1B's day-container housekeeping, and the same two guards apply: nothing attached at that
timestamp, and the directory physically empty, because `ff.rmdir` is recursive.

- [ ] **Step 4: Run, negative-control, commit**

---

### Task 3: Lift both gates

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/TableWriter.java` (the `SQUASH PARTITIONS` gate)
- Modify: `core/src/main/java/io/questdb/griffin/SqlCompilerImpl.java` (the statement-time refusal from 1B Task 0)
- Modify: `docs/superpowers/specs/2026-08-11-composite-scope-closure-index.md` (gate #5 and the silent-skips table)

- [ ] **Step 1: Remove the skip and prove fragments stop accumulating**

Ordering between this and the explicit gate is a judgement call, not a correctness one — see the
correction above. Removing the skip is what delivers the operational benefit (bounded fragment count);
lifting the explicit gate is what unblocks `DETACH`.

- [ ] **Step 2: Then lift the explicit gate, with both tests green**

- [ ] **Step 3: Flip the fuzz classification if a generator exists; if not, say so**

`CompositeFuzzRunner`'s table classifies DDL operations. Check what the generator actually emits before
flipping — 1B's flip was safe only because `FuzzDropPartitionOperation` emits the timestamp-bounded
`WHERE` form. If no generator covers squash, record that rather than implying coverage.

- [ ] **Step 4: Full suites in small batches, then griffin**

---

## Self-Review

**Spec coverage.** Closes gate #5 and the split-fragment-squash silent skip. Leaves `DETACH` and
`ATTACH` — the last two lifecycle gates — which need the nested `.detached` container and re-interning
dimension values by value, i.e. genuinely new machinery rather than cell-awareness over existing
machinery.

**Placeholder scan.** Task 2 names the two methods but not final code, because Task 2 Step 1 makes
establishing which path each entry point uses an explicit gate. That structure has now paid off four
times in this project: 1A (wrong file), 1B (gate narrowed, not lifted), 1D (hypothesis falsified), 2A
(only one of two sites may change).

**Known risk, stated rather than discovered.** The automatic in-commit squash runs on the commit path,
which is the hottest and most crash-sensitive code in the writer. A merge that is correct but not
crash-safe would trade a fragment leak for a torn partition. The crash-safety expectations are already
established by the fast-append work (`CompositeMultiCellFastAppendCrashTest`); this task must run those
suites, not only the squash tests.


---

## ATTACH PARTITION: design settled 2026-08-18 (read the artifact's `_txn`, do NOT parse directory names)

**Measured state.** With both gates lifted, DETACH round-trips as far as producing the artifact and then
ATTACH fails:

```
cannot read min, max timestamp from the [path=.../2023-01-01.attachable, partitionSizeRows=1, errno=2]
```

`TableWriter` reads the designated-timestamp column at the artifact's CONTAINER root
(`readNativeMinMaxTimestamps(path, columnName, partitionSize)`), but a composite artifact keeps its data
one level down, inside per-cell directories.

**The design question, and the answer.** Attaching needs a `cellKey` in THIS table for each cell in the
artifact. The registry interns by dimension ORDINALS (`CellRegistry#internCell(dimOrdinals, dimCount)`),
so the obvious route -- read the cell directory names and map those strings back to ordinals -- requires
parsing a rendered segment back into values.

**Do not do that.** Segment rendering is deliberately one-way in this codebase: it is path-safety
encoded, has its own NULL token, and multiple dimension kinds render through
`putCellSegmentPathSafe`. The existing cell-qualified DROP resolves names by RENDERING each attached
cellKey and comparing, precisely to avoid a reverse parse (`SqlCompilerImpl#resolveCellQualifiedPartitionName`).
Introducing a parser for ATTACH would create a second, lossier source of truth for the same mapping.

**Read the artifact's own metadata instead.** `detachPartition` already copies `_meta`, `_cv` and `_txn`
into the detached directory (see its "copy _meta, _cv and _txn to partition.detached" block). The `_txn`
copy carries the attached-partition entries -- including each entry's cellKey -- and `_cv` carries the
per-cell column versions. So ATTACH can enumerate the artifact's cells authoritatively, with their
ordinals, without looking at a single directory name.

**The remaining real work, in order:**

1. Open the artifact's copied `_txn` and `_cv` and enumerate its (timestamp, cellKey) entries.
2. For each, resolve the artifact's dimension ORDINALS to ordinals in THIS table's dictionaries. These
   can differ: the dictionaries are per-table and the artifact may come from another table entirely
   (which is the whole point of ATTACH). This is the genuinely new machinery -- a dictionary-to-dictionary
   ordinal remap -- and it is where the cellKey may need to be created rather than found.
3. `internCell` the remapped ordinals to get this table's cellKey, then read min/max per cell from that
   cell's directory rather than the container root.
4. Register one attached entry per cell, and write per-cell column versions.

**Why this is not a cell-awareness fix.** Every other composite gate lifted in SP1/SP2 was the same
shape -- a walk resolving by timestamp where it should resolve by index. ATTACH is different in kind: it
has to translate identity between two independent dictionaries. It deserves its own plan, and this
section is the starting point rather than the whole of it.
