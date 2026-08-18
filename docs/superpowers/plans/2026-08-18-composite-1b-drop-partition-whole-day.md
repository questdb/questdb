# Composite 1B — Whole-Day DROP PARTITION Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `DROP PARTITION` work on a routed composite table for predicates that select **whole days**, by fixing the three mechanisms that make it actively unsafe today — then lift the gate for that shape only.

**Architecture:** The lifecycle spec's rule is *"a partition predicate selects cells; dropping every cell of a day drops the day."* Its first table row — a predicate with no dimension constraint, selecting every cell of the matched days — is defined to be **identical to today's plain-table behaviour**. That row needs no new grammar, no dimension predicate columns and no `table_partitions()` changes; it needs only the removal machinery to stop being cell-blind. This plan delivers exactly that row. Dimension-constrained predicates (`WHERE exchange = 'BTC'`) are sub-project 1C.

**Tech Stack:** Java 25 (`JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64`), Maven offline (`mvn -o -pl core`), JUnit 4, `QDB_TEST_TMPDIR=/dev/shm`.

## Global Constraints

- **Cardinal rule:** composite behaves exactly like its plain twin, or fails LOUDLY. No silent path.
- **Invariant 1:** plain-table behaviour is byte-identical. `TxWriter.removeAttachedPartitions(long)` must remain a `cellKey = 0` delegate, and every plain path must keep using it unchanged.
- **Invariant 6:** a refusal fires at the statement that caused it.
- **Atomicity (spec §5.1):** removing N cells is **one `_txn` commit**. A partially-applied multi-cell drop must be impossible.
- Negative controls use `cp`/restore — **never `git stash` or `git checkout`** in this worktree.
- **Never run two `mvn` commands against this worktree at once** — concurrent builds share `core/target/classes` and produce a `NoClassDefFoundError` storm that reads as a mass regression.
- griffin baseline: 24,560 run / 0 failures / 4 known port-9000 errors.
- **Any test that could hang must carry a JUnit timeout.** One of the three defects here is a confirmed infinite loop; a regression must fail, not wedge CI.

## Task ordering note

Task 0 was added after the plan was written, when Task 1's tests were run for the first time. It comes
first because without it the other tests cannot fail for their own reasons. The rest of the plan is
unchanged.

## The three mechanisms (from the gate comment at `TableWriter:3864`)

All three are already documented in the code and were empirically confirmed during the Plan 4a sweep.
This plan does not rediscover them; it fixes them.

| # | Mechanism | Consequence |
|---|---|---|
| N1 | `dropPartitionByExactTimestamp`'s "removing active partition" branch resolves the new tail's min/max via the **cell-blind** 5-arg `setPathForNativePartition` | throws "file does not exist" on a routed composite tail |
| N2 | `TxWriter.removeAttachedPartitions(long)` defaults to `cellKey = 0`; the `getLogicalPartitionTimestamp`-driven loop re-probes the same raw index forever once cell 0's entry is gone | **infinite loop**, empirically reproduced (a forked test JVM spun until killed) |
| N3 | the physical-delete step (`processPartitionRemoveCandidates0`'s bare-path unlink) can collapse to the **shared day container** depending on which cell's nameTxn is the `-1` sentinel | deletes sibling cells' data that was never selected |

N3 is the dangerous one: N1 fails loudly and N2 hangs, but N3 silently destroys data. It is listed
third in the code comment and must be treated first in testing.

---

### Task 0: Make the refusal SYNCHRONOUS — found by running Task 1's tests

**Files:**
- Modify: `core/src/main/java/io/questdb/griffin/SqlCompilerImpl.java` (or the ALTER validation path)
- Test: `core/src/test/java/io/questdb/test/cairo/CompositeEarliestRefusalTest.java` (extend — wave 0 owns this file)

**Discovered 2026-08-18, not planned for.** Running Task 1's tests against the current build showed
they do NOT hit the gate. `ALTER TABLE c DROP PARTITION LIST '2023-01-02'` **succeeds**; the WAL apply
then throws and suspends the table:

```
C ApplyWal2TableJob job failed, table suspended [table=c~1, seqTxn=2,
  error=... composite partitioning does not yet support DROP PARTITION [table=c]]
```

The composite table silently kept the day (`count=3` where the plain twin had `0`), so the tests
failed on a twin mismatch rather than on a refusal.

This is precisely the invariant-6 violation **wave 0 existed to fix**, and wave 0 missed it — wave 0
covered `FORMAT PARQUET` and the O3 purge only. A user who types `DROP PARTITION` on a composite table
today gets a suspended table, not an error.

It must be fixed **before** Tasks 1–4, for two reasons: the red tests otherwise fail for a confusing
reason and cannot distinguish N1/N2/N3, and 1C's eventual dimension-constrained refusal would suspend
rather than refuse.

- [ ] **Step 1: Refuse at the statement**

Mirror wave 0 Task 2's `FORMAT PARQUET` fix: open a reader, check
`getMetadata().getPartitionSpec().getDimensionCount() > 0`, and throw `SqlException` at the
statement's position. Keep the `TableWriter` gate as the non-SQL backstop — wave 0 kept its writer-side
guard for the same reason.

- [ ] **Step 2: Assert the table is NOT suspended afterwards**

The regression that matters: after the refused statement, the table must still be usable. A test that
only asserts "an exception was thrown" would pass against the async behaviour too, because that also
throws — just later, on a different thread, after suspending the table. Assert both the refusal AND
that a subsequent `INSERT` + query still works.

- [ ] **Step 3: Check the other five lifecycle DDLs for the same shape**

`FORCE DROP PARTITION`, `DETACH`, `ATTACH`, `SQUASH`, TTL — every one of these gates lives in
`TableWriter`, which is the WAL-apply side. If they share the shape they share the defect. Record the
result per operation; fix the ones in this plan's scope and file the rest for 1C/1D rather than
silently leaving them.

- [ ] **Step 4: Commit**

---

### Task 1: Red tests for all three mechanisms

**Files:**
- Test: `core/src/test/java/io/questdb/test/cairo/CompositeDropPartitionWholeDayTest.java` (create)

**Interfaces:**
- Produces: the three failing tests Tasks 2–4 fix, one per mechanism, plus the twin-equality test that defines "done".

- [ ] **Step 1: Write the tests against the CURRENT gate**

Each test drops a whole day from a composite table with 2+ cells on that day, and from its plain
twin, then asserts the twin comparison. Today they all fail identically — with the gate's
`CairoException` — which proves only that the gate is present, not that the underlying mechanism is
fixed. So each test must be written so it *keeps* failing for its own reason once the gate is lifted:

1. `testDropWholeDayMatchesPlainTwin` — the acceptance test. Drops a middle day; composite and plain
   must return identical rows afterwards, and the composite table's day directory must be gone.
2. `testDropActivePartitionTail` (N1) — drops the **last** day, the active tail. Asserts the drop
   succeeds and `rowCount == transientRowCount + fixedRowCount` afterwards.
3. `testDropDayWithMultipleCellsTerminates` (N2) — `@Test(timeout = 60_000)`. A day with 3 cells;
   dropping it must terminate. **The timeout is the assertion.**
4. `testDropDayDoesNotTouchSiblingDays` (N3) — three days, each multi-cell; drop the middle one and
   assert both neighbours keep **every** row and every cell directory. This is the data-loss guard.

- [ ] **Step 2: Run and record**

```bash
cd /home/nick/claude/wt/oss/composite-partitioning
export JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 QDB_TEST_TMPDIR=/dev/shm
mvn -o -pl core test -Dtest=CompositeDropPartitionWholeDayTest
```

Expected: 4/4 FAIL, all with "composite partitioning does not yet support DROP PARTITION". Record the
message verbatim in the report — it is the baseline that proves the gate, not the fix, is what they
currently hit.

- [ ] **Step 3: Commit the red tests**

Commit them `@Ignore`d with a reason naming the gate, so the suite stays green while Tasks 2–4 land.
Un-ignoring is Task 5's job. (This project's precedent for a red acceptance test is
`CompositeO3PurgeSkipTest`, which sat `@Ignore`d from wave 0 until sub-project 1A fixed it.)

---

### Task 2: N2 — make the removal loop cell-aware and terminating

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/TableWriter.java` (`removePartition`, ~`3864`)
- Modify: `core/src/main/java/io/questdb/cairo/TxWriter.java` if a new primitive is genuinely needed

**Interfaces:**
- Consumes: `TxWriter.removeAttachedPartitions(long, int cellKey)` (`:429`) — **already exists**, added by Plan 3 Task 4. It resolves the exact `(ts, cellKey)` record via `findAttachedPartitionRawIndexBy`, so removing one cell cannot delete a sibling.
- Consumes: `TxReader.hasAnyAttachedPartitionForTimestamp(long)` — the cellKey-agnostic existence check.
- Produces: a terminating removal loop Tasks 3–4 build on.

- [ ] **Step 1: Identify the loop**

In `removePartition`, the `getLogicalPartitionTimestamp`-driven `while` loop below the gate. Read it
before changing it: the termination condition today is "cell 0's entry is gone", which a multi-cell
day never satisfies.

- [ ] **Step 2: Make it iterate cells**

For a routed composite table the loop must remove **every** cell of the matched day — enumerating the
day's attached `(ts, cellKey)` records and calling the two-argument
`removeAttachedPartitions(ts, cellKey)` for each — and terminate when
`hasAnyAttachedPartitionForTimestamp(ts)` is false. For a plain table the day has exactly one cell,
so the loop body runs once and the behaviour is unchanged; do **not** add a composite-detection
branch if the cell-enumerating form degenerates correctly, and say which you did in the report.

- [ ] **Step 3: Verify termination first, correctness second**

Run `testDropDayWithMultipleCellsTerminates` (un-ignored locally). It must pass **and** finish well
inside its timeout. A test that passes at 59s is a failure wearing a green hat — record the actual
duration.

- [ ] **Step 4: Commit**

---

### Task 3: N1 — resolve the active tail's path per cell

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/TableWriter.java` (`dropPartitionByExactTimestamp`, ~`9029`)

- [ ] **Step 1: Find the cell-blind resolution**

The "removing active partition" branch resolves the new tail's min/max through the 5-arg
`setPathForNativePartition`. Sub-project 1A's investigation catalogued every cell-blind call site;
this is one of them.

- [ ] **Step 2: Resolve the cell's own path**

Use the 6-arg overload with the cell segment rendered by `TableWriter#renderCellSegment`. Per spec
§5.1, **min/max recomputation uses the remaining cells, not the day floor.**

- [ ] **Step 3: Run `testDropActivePartitionTail`, assert the row-count identity, commit**

---

### Task 4: N3 — never let the physical delete collapse to the day container

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/TableWriter.java` (`processPartitionRemoveCandidates0`)

**This is the data-loss mechanism. Treat it as the highest-risk task in the plan.**

- [ ] **Step 1: Reproduce the collapse deliberately**

Before fixing, construct the state the gate comment describes: a cell whose nameTxn is still the
initial `-1` sentinel, on a day with siblings. Show that the unlink path resolves to the shared day
container. If you cannot reproduce it, **say so and stop** — do not "fix" a mechanism you have not
seen, and do not assume the comment is stale because you failed to trigger it. Record which.

- [ ] **Step 2: Make the unlink cell-exact**

`partitionRemoveCandidates` already carries cellKey (Plan 4b, 15 sites). The unlink must use it, and
must refuse to unlink a path that is the day container while any sibling cell remains attached — a
loud assertion, not a silent skip. Per spec §5.1, the day container is removed **only** when its last
cell is removed.

- [ ] **Step 3: Close a live cell non-truncatingly before removal**

Spec §5.1: a cell holding a live fast-append segment is closed **non-truncatingly**. The precedent is
fast-append T3, where a truncating close on a partially-opened cell shrank a committed cell to zero
bytes.

- [ ] **Step 4: Run `testDropDayDoesNotTouchSiblingDays` plus the fast-append crash suites**

```bash
mvn -o -pl core test -Dtest='CompositeDropPartitionWholeDayTest,CompositeMultiCellFastAppendCrashTest,CompositeFastAppendTest'
```

- [ ] **Step 5: Commit**

---

### Task 5: Narrow the gate, un-ignore, flip the classification

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/TableWriter.java` (the gate at ~`3864`)
- Modify: `core/src/test/java/io/questdb/test/cairo/CompositeDropPartitionWholeDayTest.java` (un-ignore)
- Modify: `core/src/test/java/io/questdb/test/cairo/fuzz/CompositeFuzzRunner.java` (DROP PARTITION `GATED → SUPPORTED`)
- Modify: `docs/superpowers/specs/2026-08-11-composite-scope-closure-index.md` (gate #1)

- [ ] **Step 1: Narrow the gate rather than deleting it**

`DROP PARTITION` is only supported here for **whole-day** predicates. A dimension-constrained
predicate must still refuse, loudly, with a message naming what is unsupported and what to do
instead — and it must refuse at the statement (invariant 6). Deleting the gate outright would ship a
silent wrong answer for the 1C shape, which is the one thing the cardinal rule forbids.

- [ ] **Step 2: Un-ignore the four tests; all must pass**

- [ ] **Step 3: Flip the fuzz classification**

DROP PARTITION moves `GATED → SUPPORTED` in `CompositeFuzzRunner`, which **auto-enrols it in the
differential fuzz**. If the harness cannot yet generate whole-day-only drop predicates, add that
generator in this step — a gate lifted without fuzz coverage is a gate lifted without coverage.

> Note from 9A: that classification table covers DDL *operations*, so unlike 9A's read shape, this
> flip genuinely applies here.

- [ ] **Step 4: Update the closure index**

Gate #1 becomes partially owned: whole-day done, dimension-constrained → 1C. The audit key count
changes only if the gate message changed; re-run the audit command from the index and paste the diff.

- [ ] **Step 5: Full suites, serially**

```bash
mvn -o -pl core test -Dtest='Composite*'
mvn -o -pl core test -Dtest='O3*,TableWriter*,WalWriter*,PartitionPurge*,AlterTable*'
mvn -o -pl core test -Dtest='io.questdb.test.griffin.**'
```

- [ ] **Step 6: Negative control and commit**

Restore `TableWriter.java` from HEAD with `cp`, confirm all four tests fail, restore, commit with the
result recorded.

---

## Self-Review

**Spec coverage.** This plan implements row 1 of the lifecycle spec §2 table (predicate with no
dimension constraint → whole days removed, identical to today) and the parts of §5.1 that row needs:
the two named failure modes, atomicity, housekeeping, non-truncating close, and min/max
recomputation. It does **not** implement §3 (addressing surfaces), §4 (predicate columns), or the
`table_partitions()` preview invariant — those are only needed once a predicate can name a dimension,
i.e. sub-project 1C. §5.2–5.6 (FORCE DROP, DETACH, ATTACH, SQUASH, TTL) are untouched.

**Placeholder scan.** Tasks 2–4 name the files and the mechanism but not the final code, because all
three are fixes to loops and path resolution whose current form must be read before it can be
rewritten — and two of the three (N2's loop, N3's unlink) have behaviour that the gate comment
describes but that no test has yet exercised. Task 4 Step 1 makes reproduction an explicit gate for
exactly this reason. This is the same trade sub-project 1A made, where the investigation gate turned
out to change the target file entirely.

**Type consistency.** `removeAttachedPartitions(long)` returns `int` and delegates to
`removeAttachedPartitions(long, int)`; the delegate form must stay untouched for invariant 1.
`hasAnyAttachedPartitionForTimestamp(long)` returns `boolean`.

**Known risk, stated rather than discovered.** N3 is a data-loss mechanism whose reproduction is not
yet demonstrated — the gate comment says "can, depending on which cell's nameTxn happens to be the
initial -1 sentinel". If Task 4 Step 1 cannot construct that state, the honest outcome is a recorded
negative result and a narrower fix, not a confident claim that N3 is handled. This project has
already had one gate withdrawn (wave-0 item 2) for resting on a premise nobody had tested.
