# Composite 2A — Column-File Cell Awareness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the column-file machinery cell-aware, so `DROP COLUMN` and `RENAME COLUMN` operate on every cell's files rather than on the vestigial day-level ones — the shared foundation the rest of sub-project 2 needs.

**Architecture:** Column files live per partition, and a composite partition is a **cell**: `<day>/<cell>/px.d`. Three pieces of machinery still address them per **day**. `PurgingOperator` carries `(columnNameTxn, partitionTimestamp, partitionNameTxn)` with no cellKey, so `DROP COLUMN` queues one day-level path per cell and deletes none of them. `ColumnPurgeOperator` resolves paths through the cell-blind 5-arg `setPathForNativePartition` at two sites. And `RENAME COLUMN` renames files by the same day-level path. Threading cellKey through these is the shared prerequisite; the individual DDLs are thin on top of it.

**Tech Stack:** Java 25 (`JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64`), Maven offline (`mvn -o -pl core`), JUnit 4, `QDB_TEST_TMPDIR=/dev/shm`.

## The survey this rests on

Measured 2026-08-18 with the five writer-side gates temporarily lifted
(`CompositeColumnDdlSurveyTest`, results in its `@Ignore` reasons). **Zero of five work.**

| Operation | Measured behaviour |
|---|---|
| `ADD INDEX` | reports success, `isColumnIndexed` is **false** — a silent no-op |
| `DROP INDEX` | unverified (same blind spot; its test now asserts the flag before and after) |
| `DROP COLUMN` | metadata succeeds; `E0/px.d`, `E1/px.d`, `E2/px.d` all survive — only the day-level file goes |
| `RENAME COLUMN` | throws: `could not open, file does not exist: .../E0/price.d.1` |
| `ALTER COLUMN TYPE` | twins disagree on the column type afterwards |

**That survey was wrong twice before it was right**, and the reason is the single most useful thing
to carry into this plan. Its first summary said "3 of 5 pass". A twin DATA comparison cannot see a
STRUCTURE change (an index alters no query result), and neither can see what is left on disk (a
dropped column stops being read the moment metadata changes). Each false positive was caught only by
checking a different observable than the last: rows → structure flags → on-disk files.

> **Standing rule for every task here:** decide what the operation is SUPPOSED to change — rows,
> metadata, or files — and assert THAT. Never accept a twin comparison as evidence for a structure or
> file change.

## Global Constraints

- **Cardinal rule:** composite behaves exactly like its plain twin, or fails LOUDLY. No silent path — and a DDL that reports success while changing nothing IS the silent path.
- **Invariant 1:** plain-table behaviour is byte-identical. A plain day is its own single cell, so a cell-aware form must degenerate to today's exactly.
- Negative controls use `cp`/restore — never `git stash`/`git checkout` in this worktree.
- **Never run two `mvn` commands against this worktree at once**; long suites are killed intermittently here, so run them in small batches and report which batches actually completed.
- griffin baseline: 24,560 run / 0 failures / 4 known port-9000 errors.

---

### Task 1: Thread cellKey through `PurgingOperator`

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/PurgingOperator.java`
- Modify: `core/src/main/java/io/questdb/cairo/TableWriter.java` (`removeColumnFiles`)

**Interfaces:**
- Produces: a purge queue entry carrying `(columnNameTxn, partitionTimestamp, partitionNameTxn, cellKey)`. Tasks 2–3 consume it.

- [ ] **Step 1: Write the failing test first**

`CompositeColumnDdlSurveyTest#surveyDropColumn` already asserts it — `assertNoColumnFilesRemain`.
Un-ignore it, lift the `DROP COLUMN` gate locally, and record the failure list verbatim: it should
name `E0/px.d`, `E1/px.d`, `E2/px.d`.

- [ ] **Step 2: Add cellKey to the queue entry**

`partitionRemoveCandidates` already carries `(timestamp, nameTxn, cellKey)` triples — Plan 4b threaded
cellKey through that queue across 15 sites. Follow that precedent rather than inventing a second
convention. A plain table passes `cellKey = 0` and the rendered segment is `null`, which keeps its
paths byte-identical.

- [ ] **Step 3: `removeColumnFiles` passes the cell**

It already iterates every `(ts, cellKey)` partition — `getPartitionCount()` is per-cell — so the loop
is right and only the call is wrong. Pass `txWriter.getPartitionCellKey(i)`.

**Note the current bug's shape:** for a three-cell day the loop calls `add(...)` three times with the
same day timestamp and no cell, so the queue holds three identical day-level entries. Deduplication is
not the fix; addressing is.

- [ ] **Step 4: Run, negative-control, commit**

---

### Task 2: Make `ColumnPurgeOperator` resolve cell paths

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/ColumnPurgeOperator.java` (~`503`, ~`564`)

Both sites use the cell-blind 5-arg `setPathForNativePartition`. They must use the 6-arg overload with
the rendered cell segment, exactly as sub-projects 1B/1C/1D did for the partition paths.

- [ ] **Step 1: Confirm which of the two sites the DROP COLUMN path actually reaches**

Instrument rather than assume — this is the technique that pinned 1A's producer and 1C's whole-day
regression. One may be a retry/async path that a synchronous test never exercises, and "fixed" code on
an unreached path is worse than an untouched one.

- [ ] **Step 2: Fix the reached site, then decide about the other with evidence**

- [ ] **Step 3: Run, negative-control, commit**

---

### Task 3: `DROP COLUMN`

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/TableWriter.java` (the gate at ~`3789`)

- [ ] **Step 1: Lift the gate; `surveyDropColumn` must pass with its file assertion**

- [ ] **Step 2: Prove the symbol case**

`removeColumnFiles` has a separate branch for SYMBOL columns, queuing a `TABLE_ROOT_PARTITION` entry
for the symbol table. A composite table's dimension column IS a symbol, so dropping a **non-dimension**
symbol column must not disturb the dimension's dictionaries. Assert the table still routes and reads
correctly afterwards.

- [ ] **Step 3: Refuse dropping a DIMENSION column**

The partition spec pins its source column. Dropping it would leave the table addressed by a column that
no longer exists. If that is already refused elsewhere, assert it; if not, refuse it here, loudly.

- [ ] **Step 4: Run, negative-control, commit**

---

### Task 4: `RENAME COLUMN`

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/TableWriter.java` (`renameColumn`, gate at ~`4077`)

- [ ] **Step 1: Find where the physical rename happens**

The survey's error is precise: `could not open, file does not exist: .../E0/price.d.1`. So the reader
already looks for the renamed file at the CELL path, and the writer never put it there. Rename walks
partitions day-blind.

**This was predicted to be the cheapest operation in the sub-project — "metadata-only, touches no
partition data".** It is not; it renames physical files with a column-name-txn suffix. Recorded so the
next person does not re-derive the wrong estimate.

- [ ] **Step 2: Rename per cell; assert the files, not the rows**

- [ ] **Step 3: Refuse renaming a DIMENSION column**, or assert it is already refused — the cell
directory names are rendered from the dimension, and a rename would desynchronise them from metadata.

- [ ] **Step 4: Run, negative-control, commit**

---

## Self-Review

**Spec coverage.** Covers sub-project 2's file-level foundation and two of its eight gates. `ADD INDEX`
/ `DROP INDEX` (indexes are per-partition files, so they need this same foundation) and
`ALTER COLUMN TYPE` (a whole-column rewrite) are deliberately excluded — they are larger, and both
depend on this plumbing landing first. The checkpoint-restore gate (#38) is untouched.

**Placeholder scan.** Tasks 2–4 name files and mechanisms but not final code, because each needs the
current form read before it is rewritten, and Task 2 Step 1 makes instrumentation an explicit gate for
exactly that reason. This mirrors 1A, 1B and 1D, where the investigation step changed the target file,
the fix shape, or falsified the plan outright.

**Known risk, stated rather than discovered.** `PurgingOperator` and `ColumnPurgeOperator` are shared
with plain tables and run asynchronously. Widening a queue entry that a background job consumes is
exactly where an in-flight entry written by the old shape and read by the new one could be
misinterpreted — the same class of hazard 1C found in `AlterOperation`'s wire format, where the answer
was a new command code rather than a wider payload. Task 1 Step 2 must establish whether this queue
survives a restart or is purely in-memory before widening it. If it is persisted, the `AlterOperation`
precedent applies.
