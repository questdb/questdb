# Composite 1E — SQUASH PARTITIONS Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make split-fragment squash cell-aware, closing the last of sub-project 1's partition-lifecycle gates that does not need new on-disk machinery — both the explicit `ALTER TABLE … SQUASH PARTITIONS` and the automatic squash that runs during commit.

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
| automatic split-fragment squash during commit | **silent skip** (closure index, silent-skips table) | fragments accumulate with no refusal visible anywhere |

The silent skip is the more important of the two. A user who never types `SQUASH` still accumulates
fragments, and nothing tells them.

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

- [ ] **Step 2: Merge per cell, matching fragment cells to target cells**

For each cell present in the FRAGMENT, merge into the SAME cell of the target day. A fragment holds a
**subset** of the day's cells — measured: the fragment had `E0` only while the day had `E0` and `E1` —
so the merge must iterate the fragment's cells, not the day's, and must not touch a cell the fragment
does not contain.

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

- [ ] **Step 1: Remove the silent skip FIRST, and prove fragments stop accumulating**

The skip is the real defect; the loud gate is merely an inconvenience.

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
