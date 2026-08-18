# Composite 1A — O3 Partition-Version Leak Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop composite tables leaking orphaned day-level partition-version directories under out-of-order writes, and un-ignore `CompositeO3PurgeSkipTest` as the acceptance criterion.

**RE-SCOPED 2026-08-18 by Task 2's finding: this is a WRITER defect, not a purge-job defect.** With
the purge job ON and OFF, the same composite churn leaves the byte-identical set of day-level
directories — so the writer produces them and the purge merely fails to reclaim them. The trigger is
narrow: an O3 write that **prepends below the partition's minimum timestamp**. Since composite
partitioning is unreleased there is no on-disk legacy to clean, so the fix is in the producer alone
and `O3PartitionPurgeJob`'s composite gate stays as it is. Task 3 targets the O3 write path; the
three cellKey-0 assumptions catalogued in Task 2 Step 3 are documentation for whoever later lifts the
purge gate, and are NOT part of 1A.

**Architecture:** Measured, not assumed (probe run 2026-08-18, recorded in the ledger). A composite table's live container is the **unversioned** day directory, with per-cell versions inside it (`2023-01-02/E0.18`). Alongside it, out-of-order writes leave **day-level** version directories holding full column files (`2023-01-02.6`, `.12`, `.18`) that hold no live rows — every row is accounted for in the cells. `O3PartitionPurgeJob` skips composite tables entirely, so those are never reclaimed. This plan first establishes *who creates them*, then fixes the producer if that is cheap and the purge otherwise.

**Tech Stack:** Java 25 (`JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64`), Maven offline (`mvn -o -pl core`), JUnit 4, `QDB_TEST_TMPDIR=/dev/shm`.

## Global Constraints

- **Cardinal rule:** composite behaves exactly like its plain twin, or fails LOUDLY. No silent path.
- **Invariant 1:** plain-table behaviour is byte-identical.
- **Invariant 2:** a skip is permitted only with a test proving it harmless. This plan exists because that test was written and *failed*.
- **THE DATA-LOSS TRAP.** The live composite container is the UNVERSIONED day directory. Any change that keeps "the newest `<day>.<txn>`" and deletes older entries will delete the unversioned directory — every live cell. `O3PartitionPurgeJob`'s existing gate comment predicted this; the probe measured it. Every task below must be tested against a composite day whose live cells sit under the unversioned dir, and against a day whose cells do **not** include cellKey 0.
- Negative controls use `cp`/restore — **never `git stash` or `git checkout`** in this worktree (it holds unrelated uncommitted work).
- **Never run two `mvn` commands against this worktree at once.** Concurrent builds share `core/target/classes` and clobber each other; the symptom is a `NoClassDefFoundError` storm across unrelated suites that reads as a mass regression. Run suites serially, in the foreground.
- `io.questdb.test.griffin.**` green before PR: baseline is 24,560 run / 0 failures / 4 known port-9000 errors (`CurrentDataIDFunctionFactoryTest` ×2, `SampleByConfigTest`, `SampleByNanoTimestampConfigTest`).

---

### Task 1: Pin the layout with an asserting characterisation test

**Files:**
- Test: `core/src/test/java/io/questdb/test/cairo/CompositeO3LayoutTest.java` (create)

**Interfaces:**
- Produces: the layout facts Tasks 2–3 depend on. If this test ever fails, those tasks' premises are void.

The throwaway probe that established these facts was deleted deliberately — a printing probe is not a
regression net. This turns its findings into assertions.

- [ ] **Step 1: Write the test**

Assert exactly four things, each of which a later task relies on:

1. after O3 churn with the purge job running, a **composite** table has more than one directory
   matching `<day>[.<txn>]` directly under the table root, and a **plain** twin has exactly one;
2. the composite table's live cells live under the **unversioned** day directory
   (`2023-01-02/E<n>.<txn>/`);
3. the `<day>.<txn>` directories contain day-level column files (`ts.d`) and **no** cell
   subdirectories;
4. the row count reachable by SQL equals the sum of rows in the cell directories — i.e. the
   day-level directories hold nothing live. This is the assertion that makes them garbage rather
   than data, and it is the one that licenses deleting them.

Use the churn shape from `CompositeO3PurgeSkipTest` (20 rounds, seed at 01:00 and 05:00, rounds
landing at `0<round%6>:30` across `E<round%3>`) so both tests describe the same workload.

- [ ] **Step 2: Run it**

```bash
cd /home/nick/claude/wt/oss/composite-partitioning
export JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 QDB_TEST_TMPDIR=/dev/shm
mvn -o -pl core test -Dtest=CompositeO3LayoutTest
```

Expected: PASS on today's build. This is a characterisation test — green from the start, by design.
If any of the four assertions fails, **stop**: the measured layout has changed and this plan's
premises are void.

- [ ] **Step 3: Commit**

```bash
git add core/src/test/java/io/questdb/test/cairo/CompositeO3LayoutTest.java
git commit -m "test(composite): pin the composite O3 on-disk layout

Characterisation test, green from the start. Turns a throwaway probe's findings
into assertions: live cells sit under the UNVERSIONED day dir, <day>.<txn> dirs
hold day-level column files and no cells, and every live row is accounted for in
the cells -- so the day-level dirs hold nothing live.

That last assertion is what licenses sub-project 1A to delete them."
```

---

### Task 2: Establish the producer — writer or purge?

**Files:**
- Investigation only. Produces a written decision, no production change.
- Report: `.superpowers/sdd/sp1a-task-2-producer.md`

**Interfaces:**
- Produces: the decision Task 3 implements. Task 3 cannot start until this is answered.

The ledger records this as the open question. Answer it before writing a fix, because the two
candidate fixes are in different files and only one of them is a root fix.

- [ ] **Step 1: Find who creates `<day>.<txn>` for a routed composite table**

Instrument or read the O3 commit path. The specific question: when a composite table takes an
out-of-order write, does it (a) create a day-level partition version *and then* route rows into
cells, leaving the day-level version unreferenced, or (b) create it as a legitimate intermediate that
something later abandons?

Start at `TableWriter`'s O3 commit path and `O3PartitionJob`, and check whether the day-level
partition version is registered in `_txn` at all. The probe showed the live cells carry nameTxn
18/19/20 while day-level dirs are `.6/.12/.18` — so at least some day-level versions were once
committed and later superseded. Establish whether the newest day-level version is ever referenced.

- [ ] **Step 2: Record the finding and the decision**

Write `.superpowers/sdd/sp1a-task-2-producer.md` stating which of these the fix is:

- **(A) Producer fix** — stop the writer creating unreferenced day-level versions for routed
  composite tables. Structural, correct-by-construction, and the project's stated preference. Choose
  this if the day-level version is genuinely never referenced.
- **(B) Purge fix** — make `O3PartitionPurgeJob` cell-aware so it reclaims them. Choose this if the
  day-level version *is* legitimately referenced during the commit and only becomes garbage
  afterwards, i.e. the writer is behaving correctly and only cleanup is missing.
- **(C) Both** — the producer stops making new garbage, the purge reclaims what existing tables
  already have on disk. Choose this if (A) is correct but leaves deployed tables leaking.

State the evidence for the choice. "It seemed cleaner" is not evidence.

- [ ] **Step 3: If (B) or (C), note the three cellKey-0 assumptions**

`O3PartitionPurgeJob` has exactly three, found by inspection:

| Line | Expression | Cell-aware replacement |
|---|---|---|
| 232 | `txReader.findAttachedPartitionRawIndexByLoTimestamp(ts) < 0` | `!txReader.hasAnyAttachedPartitionForTimestamp(ts)` |
| 350 | `partitionInTxnFile = ...findAttachedPartitionRawIndexByLoTimestamp(ts) >= 0` | `txReader.hasAnyAttachedPartitionForTimestamp(ts)` |
| 398 | `txReader.getPartitionNameTxnByPartitionTimestamp(ts)` | **not** a mechanical swap — this returns cellKey 0's nameTxn, and a composite day's cells each carry their own. Decide explicitly what "the day's name txn" means before touching it. |

`hasAnyAttachedPartitionForTimestamp` already exists on `TxReader` — added for
`TableWriter#removePartitionDirsNotAttached` to solve this same misclassification — and is documented
as agreeing with the cellKey-0 form for plain and dormant-composite tables, which is what keeps
invariant 1.

---

### Task 3: Implement the decision

**Files:**
- Determined by Task 2. Do not pre-empt it here.
- Test: `core/src/test/java/io/questdb/test/cairo/CompositeO3PurgeSkipTest.java` (un-ignore)

- [ ] **Step 1: Write the data-loss regression test FIRST**

Before any fix. A composite table with a day whose live cells sit under the unversioned directory,
**and** a day whose cells do not include cellKey 0 (drop or never create E0), must keep every row
after the purge job runs. This is the test that catches the trap the gate comment warned about.

It must fail if the fix is naive. Verify that by writing the naive version first (delete the gate,
swap nothing else), running this test, and recording the failure — then implement the real fix. A
data-loss guard that has never been seen to fail is not a guard.

- [ ] **Step 2: Implement**

Per Task 2's decision.

- [ ] **Step 3: Un-ignore the acceptance test**

Remove the `@Ignore` from `CompositeO3PurgeSkipTest#testCompositeReclaimsObsoletePartitionVersions`
and update its javadoc from "PROVEN LEAK … un-ignore when sub-project 1 makes the purge walk
cell-aware" to what the fix actually did.

- [ ] **Step 4: Run**

```bash
mvn -o -pl core test -Dtest='CompositeO3PurgeSkipTest,CompositeO3LayoutTest'
mvn -o -pl core test -Dtest='Composite*'
mvn -o -pl core test -Dtest='O3*,TableWriterTest,PartitionPurge*'
```

Expected: `CompositeO3PurgeSkipTest` passes un-ignored; `Composite*` at or above 431 with 0
failures; O3/purge suites 0 failures.

- [ ] **Step 5: Negative control**

Restore the changed production files from HEAD with `cp`, re-run
`CompositeO3PurgeSkipTest` and the data-loss test, record both outcomes in the commit message.

- [ ] **Step 6: griffin, serially**

```bash
mvn -o -pl core test -Dtest='io.questdb.test.griffin.**'
```

Expected: 24,560 / 0 failures / the 4 known port errors. Nothing else may be running.

- [ ] **Step 7: Commit, and update the closure index**

The index's orphan table entry for **O3 partition purge** currently reads "MEASURED 2026-08-17: NOT
harmless … sub-project 1's first task." Update it to the outcome. If the fix was (A) or (C), the
entry also stops being a purge-job item, which changes what sub-project 1 still owns.

---

## Self-Review

**Spec coverage.** The lifecycle spec (§5.1–5.6) covers six DDL operations; this plan covers none of
them. It covers the O3 purge orphan, which the closure index assigns to sub-project 1 as its *first*
task and which the spec's §6 table does not list. That is a genuine gap in the spec, not in this
plan: the spec was written 2026-08-11, before the leak was measured on 2026-08-17. Task 3 Step 7
closes the loop by updating the index; the lifecycle spec itself should gain a §5.7 when the six DDL
operations are planned.

**Placeholder scan.** Task 3's file list is deliberately empty, and Task 1's test is described by its
four assertions rather than given as code. Both are consequences of Task 2 being a genuine
investigation whose answer changes the target file — writing speculative code for both branches would
be worse than naming the decision. This is the one place this plan departs from "complete code in
every step", and it does so knowingly. Task 1's four assertions are precise enough to implement
without further decisions; Task 3's are not, by construction.

**Type consistency.** `hasAnyAttachedPartitionForTimestamp(long)` returns `boolean`;
`findAttachedPartitionRawIndexByLoTimestamp(long)` returns `int` compared against 0. The table in
Task 2 Step 3 inverts the sense correctly at both sites — line 232 needs `!`, line 350 does not.

**Known risk.** Task 2 may conclude (A), in which case this plan's title is wrong: the leak is a
writer defect and the purge job is innocent. That is an acceptable outcome and the reason Task 2
exists as its own gate rather than being folded into Task 3. Renaming the plan is cheaper than
building the wrong fix.
