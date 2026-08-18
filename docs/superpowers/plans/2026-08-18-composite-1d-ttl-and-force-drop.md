# Composite 1D — TTL Eviction and FORCE DROP PARTITION Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make TTL-based partition eviction and `FORCE DROP PARTITION` cell-aware on composite tables.

**Task 1 ran on 2026-08-18 and FALSIFIED this plan's opening hypothesis.** Neither operation inherited
1B's fix: with the gates lifted, both removed **nothing** on a composite table while the plain twin
dropped and evicted correctly. Had the gates been lifted on the strength of the shared-method
argument — they both call `dropPartitionByExactTimestamp`, which 1B made cell-correct — both would
have become **silent no-ops**: accepted, reporting success, changing nothing. That is strictly worse
than today's loud refusal. See `.superpowers/sdd/sp1d-task-1-measurement.md`.

Tasks 2 and 3 are therefore **implementation**, not gate removal. The selection logic upstream of the
shared removal is what is still blind — `enforceTtl` chooses what to evict, and `forceRemovePartitions`
is a separate entry point that never enters the loop 1B fixed.

**Architecture:** Both operations reach `dropPartitionByExactTimestamp`, which 1B made cell-correct — but that is the REMOVAL, and Task 1 showed the failure is upstream of it, in SELECTION. `enforceTtl` decides which partitions have aged out and selected nothing for a composite table; `forceRemovePartitions` is a separate entry point that never enters the loop 1B fixed. So the work is to make those two selection paths enumerate `(ts, cellKey)` records rather than assuming one partition per day. TTL needs no new addressing (whole days only, spec §5.6), and FORCE DROP needs no cell-qualified guard — its LIST parser already rejects `<day>/<cell>` with a date-format error, unlike `DROP`'s.

**Tech Stack:** Java 25 (`JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64`), Maven offline (`mvn -o -pl core`), JUnit 4, `QDB_TEST_TMPDIR=/dev/shm`.

## Global Constraints

- **Cardinal rule:** composite behaves exactly like its plain twin, or fails LOUDLY. No silent path.
- **Invariant 1:** plain-table behaviour is byte-identical.
- **Invariant 6:** a refusal fires at the statement that caused it. Both gates were made synchronous in 1B Task 0; if a gate is lifted, its synchronous refusal goes with it, and if it is kept, the synchronous form stays.
- **Any test that could hang carries a JUnit timeout.** The removal loop these operations share is the one that spun 34.3M times before 1B fixed it.
- Negative controls use `cp`/restore — never `git stash` or `git checkout` in this worktree.
- **Never run two `mvn` commands against this worktree at once.**
- griffin baseline: 24,560 run / 0 failures / 4 known port-9000 errors.

---

### Task 1: Measure before implementing

**Files:**
- Test: `core/src/test/java/io/questdb/test/cairo/CompositeTtlAndForceDropTest.java` (create)

**Interfaces:**
- Produces: the evidence that decides whether Tasks 2–3 are "lift the gate" or "fix what is still broken".

1B's most useful step was the probe that measured what an ungated operation actually does before
deciding how to gate it. That probe is what caught `DROP PARTITION LIST '<day>/E0'` destroying a whole
day. This task repeats that discipline for two operations rather than assuming they inherited 1B's fix.

- [ ] **Step 1: Write twin tests for both operations**

Against a composite table with 2+ cells per day and its plain twin:

1. `testTtlEvictsWholeDaysMatchingPlainTwin` — set a TTL that ages out the older days, commit enough
   to trigger eviction, assert the twins agree and the evicted day directories are gone.
2. `testForceDropWholeDayMatchesPlainTwin` — `FORCE DROP PARTITION LIST '<day>'` on both twins,
   assert agreement.
3. `testForceDropIndividualCellIsRefused` — the shape 1B proved destructive for `DROP`. FORCE DROP
   exists to bypass safety checks, so it needs its own test rather than an inherited assumption:
   **bypassing safety checks must not mean bypassing correctness.**

All three carry `@Test(timeout = 60_000)`.

- [ ] **Step 2: Run with the gates TEMPORARILY lifted, and record what happens**

Lift the writer-side gates (`enforceTtl` ~`9155`, `forceRemovePartitions` ~`2735`) and the
statement-side gates added in 1B Task 0, then run. Record for each operation, verbatim:

- pass / fail / hang;
- for a failure, whether it is a twin mismatch, an exception, or a directory left behind;
- for TTL specifically, whether eviction is triggered at all — a TTL test that never evicts is
  vacuous, so assert the plain twin DID lose partitions before comparing.

**Restore the gates before committing anything.** 1B established this rhythm: measure with gates
lifted, restore, then decide.

- [ ] **Step 3: Write the finding**

`.superpowers/sdd/sp1d-task-1-measurement.md`, stating per operation whether it works, and if not,
which mechanism fails. This decides Tasks 2 and 3.

---

### Task 2: TTL eviction

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/TableWriter.java` (`enforceTtl`, ~`9155`)
- Modify: `core/src/main/java/io/questdb/griffin/SqlCompilerImpl.java` (the 1B Task 0 `SET TTL` refusal)

- [ ] **Step 1: Act on Task 1's finding**

If TTL works: remove both gates, keeping the writer-side one only if a non-SQL path can still reach an
unsupported shape. If it does not: fix the mechanism Task 1 named, then remove them.

- [ ] **Step 2: The TTL-specific trap**

TTL is evaluated at **every commit**, not at its own DDL — that is why 1B Task 0 found it suspending
tables on ordinary `INSERT`s. So the acceptance test must exercise eviction through a plain insert,
not only through `ALTER TABLE … SET TTL`. A test that only sets a TTL proves nothing about the path
that actually evicts.

- [ ] **Step 3: Per-dimension TTL stays out of scope**

Spec §5.6 defers it explicitly. One retention policy per table; a day ages out as a whole.

- [ ] **Step 4: Run, negative-control, commit**

---

### Task 3: FORCE DROP PARTITION

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/TableWriter.java` (`forceRemovePartitions`, ~`2735`)

- [ ] **Step 1: Act on Task 1's finding, as Task 2**

- [ ] **Step 2: Keep the cell-qualified refusal**

FORCE DROP bypasses *safety checks*; it does not bypass the addressing rule. A cell-qualified name
must refuse exactly as it does for `DROP`, and Task 1 Step 1's third test is the guard. If FORCE DROP
reaches a different code path than `DROP`'s `refuseCellQualifiedPartitionName`, it needs its own call
— do not assume shared parsing.

- [ ] **Step 3: Run, negative-control, commit**

---

### Task 4: Close out

- [ ] **Step 1: Flip the fuzz classification for whichever operations became supported**

`CompositeFuzzRunner`'s table classifies DDL operations, so this genuinely applies (unlike 9A's read
shape). Check what the corresponding `Fuzz*Operation` actually generates before flipping — 1B's flip
was safe only because `FuzzDropPartitionOperation` emits the timestamp-bounded `WHERE` form.

- [ ] **Step 2: Update the closure index**

Gates #2 (`FORCE DROP`) and #6 (TTL) change owner or state. If a gate message changes rather than
disappears, the audit key count can hold steady while the meaning moves — 1B hit exactly this and
recorded the swap rather than letting a stable 37 imply nothing happened.

- [ ] **Step 3: Full suites, serially: `Composite*`, writer suites, then griffin**

---

## Self-Review

**Spec coverage.** Implements spec §5.2 (FORCE DROP) and §5.6 (TTL). Leaves §5.3 (DETACH), §5.4
(ATTACH) and §5.5 (SQUASH) to a later plan, and per-cell addressing to 1C. DETACH/ATTACH are deferred
deliberately: they need the nested `.detached` container layout and re-interning by value, which is
genuinely new machinery rather than a gate over fixed machinery.

**Placeholder scan.** Tasks 2 and 3 are conditional on Task 1's measurement and deliberately do not
pre-write the fix. That is the same structure as 1A, where the investigation gate changed the target
file entirely, and 1B, where the probe changed the gate from "lift" to "narrow". Writing speculative
code for both branches would be worse than naming the decision point.

**Known risk.** The hypothesis that both operations inherited 1B's fix is plausible and untested. If
Task 1 shows they did not, this plan's shape is wrong and it becomes an implementation plan rather
than a verification one — which is why Task 1 produces a written finding before any production edit.
The cost of being wrong is one measurement; the cost of assuming is a gate lifted over a broken path.
