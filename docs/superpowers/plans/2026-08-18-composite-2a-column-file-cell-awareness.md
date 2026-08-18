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

> **ANSWERED 2026-08-18, and it makes this step WRONG AS WRITTEN.** The prerequisite check found the
> column-purge queue is **not** in-memory only: `ColumnPurgeJob` persists entries into a real system
> table, `sys.column_versions_purge_log`, with an explicit positional schema —
> `column_name symbol(2), columnType int(5), table_partition_by int(6), column_version long(8),
> partition_timestamp timestamp(9), partition_name_txn long(10)`, `partition by MONTH BYPASS WAL`.
> There is no cellKey column.
>
> So adding one is a **persisted schema change to a system table shared with plain tables**, and rows
> written by an older build are read back by a newer one — the hazard 1C hit in `AlterOperation`,
> where the answer was a new command code rather than a wider payload. The
> `partitionRemoveCandidates` precedent cited below does **not** transfer: that queue is purely
> in-memory.
>
> Three options, to be decided with evidence:
> 1. add a cellKey column to the purge-log table and treat absent values as day-level — a schema
>    migration on a `BYPASS WAL` system table;
> 2. encode the cell into an existing field such as `partition_name_txn` — **rejected on sight**: it
>    overloads a field whose meaning plain tables depend on;
> 3. purge a composite table's column files **synchronously** at drop time and never enqueue them,
>    leaving the async log untouched. Narrower, and plausibly correct: a cell's files have the same
>    reader-visibility constraints as its partition, and `processPartitionRemoveCandidates` already
>    handles those cell-aware.
>
> Option 3 is the one this plan did not consider and currently looks strongest. Establish which is
> right before writing code — the same discipline that turned 1B from "lift the gate" into "narrow the
> gate".

`partitionRemoveCandidates` carries `(timestamp, nameTxn, cellKey)` triples in memory — cited here as
the shape of a cell-aware queue, NOT as a precedent for widening a persisted one.

- [ ] **Step 3: `removeColumnFiles` passes the cell**

It already iterates every `(ts, cellKey)` partition — `getPartitionCount()` is per-cell — so the loop
is right and only the call is wrong. Pass `txWriter.getPartitionCellKey(i)`.

**Note the current bug's shape:** for a three-cell day the loop calls `add(...)` three times with the
same day timestamp and no cell, so the queue holds three identical day-level entries. Deduplication is
not the fix; addressing is.

- [ ] **Step 4: Run, negative-control, commit**

---

### Task 2: Make `ColumnPurgeOperator` resolve cell paths — **REQUIRED, not optional**

> **MEASURED 2026-08-18, after Task 1 landed.** Task 1 fixed the SYNCHRONOUS purge, and on an idle
> table that is the whole story: the async fallback fires zero times and every cell file is removed.
> With a `TableReader` held open across the drop — the normal production case — synchronous removal
> fails, the fallback fires, and **every cell file leaks exactly as before**: `E0/px.d`, `E1/px.d`,
> `E2/px.d` and the day-level file all survive.
>
> So Task 1 fixed the path an idle TEST takes, not the path a live SYSTEM takes. This task is what
> makes `DROP COLUMN` actually safe, and it brings back the persisted-schema question Task 1
> sidestepped: the async path is precisely the one that writes `sys.column_versions_purge_log`. The
> three options under Task 1 Step 2 now apply HERE, and option 3 ("avoid the log entirely") is no
> longer available.
>
> **Method note worth carrying:** measure purge and cleanup work with a reader pinned. Idle is the
> unrepresentative case, and it is the one a test creates by default.

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/ColumnPurgeOperator.java` (~`503`, ~`564`)

Both sites use the cell-blind 5-arg `setPathForNativePartition`. They must use the 6-arg overload with
the rendered cell segment, exactly as sub-projects 1B/1C/1D did for the partition paths.

- [x] **Step 1: Confirm which of the two sites the DROP COLUMN path actually reaches** — DONE.

Both are reached only via the async fallback, which fires when synchronous removal fails. Instrumented
and measured: zero times on an idle table, four times with a reader pinned.

- [ ] **Step 2: Decide HOW, before touching either site — see `.superpowers/sdd/sp2a-task-2-decision.md`**

The blocker is not the two path resolutions; it is that the async path persists to
`sys.column_versions_purge_log`, which is **read and written by fixed positional index** and created
with `CREATE TABLE IF NOT EXISTS` and **no migration path**. Appending a cellKey column would leave a
newer build reading index 11 on a pre-existing table that will never have it.

**The equivalence was proven false on 2026-08-18, which kills that recommendation.** Column purge keys
on `isRangeAvailable(columnVersion + 1, updateTxn)` — the COLUMN's version and the txn that superseded
it — while partition purge keys on PARTITION nameTxn. An `UPDATE` supersedes a column WITHOUT changing
the partition's nameTxn, so a pinned reader can still need the old column file while the partition is
current. Routing column cleanup through the partition purge would delete files a live reader needs:
strictly worse than the leak.

**Verified further, and the route now depends on a PRODUCT DECISION.** Enumerating every producer of
a new column generation shows all of them — `changeColumnType`, `renameColumn`, `ConvertOperatorImpl`,
`UpdateOperatorImpl` — are ALREADY GATED for composite (`addColumn` mints a generation for a new
column and supersedes nothing). So the `UPDATE` counterexample cannot occur on a composite table
today, and option (d) is correct **as long as those gates stay shut**.

That is a conditional correctness argument, and sub-project 4 exists to implement `UPDATE`. The day it
lands, a purge routed through the partition queue silently gains the ability to delete a column file a
pinned reader still needs — nothing fails loudly, and the assumption lives in a comment.

- **If `UPDATE` is BANNED permanently for composite:** take (d). Correct by specification, no
  migration, and the assumption is checkable in one place.
- **If `UPDATE` is merely DEFERRED:** take (a) and pay for the migration, because (d) would be
  correct-until-someone-ships-sub-project-4.

See `.superpowers/sdd/sp2a-task-2-decision.md` for the full table.

- [ ] **Step 3: Fix the reached site — there is only ONE, and the other must NOT be touched**

> Found 2026-08-18. This plan treated `ColumnPurgeOperator`'s two `setPathForNativePartition` calls as
> a pair. They are not:
>
> - **`reopenPurgeLogPartition` (~503)** addresses the PURGE LOG'S OWN partitions.
>   `sys.column_versions_purge_log` is a **plain** table. Changing this would be wrong and would
>   corrupt the job's access to its own log.
> - **`setUpPartitionPath` (~564)** addresses the TARGET table, reached from the deletion loop at
>   ~317 and ~366. This is the only site that matters.
>
> **Implementation shape, given the permanent `UPDATE` ban.** The loop handles one
> `(columnVersion, partitionTimestamp)` entry and deletes one file; for a composite table it must
> iterate the day's CELLS and delete that column's file in each. The cell set is derivable from the
> table's own `_cell` registry, so **no new purge-log column is needed** — the schema migration is
> avoided entirely.
>
> **That is safe ONLY because `UPDATE` is permanently banned.** Column supersession is now always
> table-wide DDL, so "delete this column's file in every cell of the day" is exactly right. If
> `UPDATE` were ever reinstated for composite, a column could be superseded in ONE cell and
> enumerate-and-delete-all would destroy live data in the others. Do not implement this without
> re-reading that sentence.

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


## Async purge: the blocker, and the migration that clears it (measured 2026-08-18)

**State.** DROP COLUMN's SYNCHRONOUS purge is cell-aware and shipped. The ASYNC fallback -- which fires
when a reader is pinned across the drop -- still leaks every cell's column file. It is a disk leak, not
corruption: the column is gone from metadata, no query can reach the files, and nothing references them.

**Why it is not a one-line fix.** `ColumnPurgeOperator#setUpPartitionPath` builds the partition path
from `(partitionTimestamp, partitionTxnName)` and nothing else, because that is all the task carries:

```java
private void setUpPartitionPath(int timestampType, int partitionBy, long partitionTimestamp, long partitionTxnName) {
    path.trimTo(pathTableLen);
    TableUtils.setPathForNativePartition(path, timestampType, partitionBy, partitionTimestamp, partitionTxnName);
}
```

`ColumnPurgeTask` stores 4 longs per entry (`BLOCK_SIZE = 4`: columnVersion, partitionTimestamp,
partitionNameTxn, updateRowId) and the queue is drained into a PERSISTED system table with a POSITIONAL
schema -- `_column_versions_purge_log`, columns 0..11, created with `CREATE TABLE IF NOT EXISTS`. So the
cell cannot simply be threaded through: it has to survive a restart, in a table that already exists in
every deployment.

**The migration that clears it, and why this shape.** Add `cell_segment symbol` as column **12, at the
END**, and issue an `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` on job startup alongside the existing
`CREATE TABLE IF NOT EXISTS`.

- Appending at the end keeps every existing positional read (0..11) valid, so an old log file and a new
  one are both readable by the new code.
- `IF NOT EXISTS` on both statements makes startup idempotent and safe on a fresh install and on an
  upgrade alike.
- NULL in column 12 means "plain table, no cell" -- which is exactly what every pre-migration row means,
  so old rows need no backfill and no interpretation rule beyond `null -> day-level path`.
- `BLOCK_SIZE` goes 4 -> 5 in `ColumnPurgeTask`. Every `updatedColumnInfo.add(...)` call site and every
  `i += BLOCK_SIZE` walk must move together; the stride is not centralised, so grep it rather than
  trusting the constant.

**Blast radius, stated plainly.** This is the only remaining composite item that changes a table shared
by every QuestDB deployment, composite or not. It deserves its own change and its own review, which is
why it is specified here rather than folded into a column-DDL commit.

**Do not "fix" it by disabling the async path for composite.** The async fallback exists because a
pinned reader must not block a drop. Making composite drops synchronous-only would trade a bounded disk
leak for a stall on a live reader, which is worse.
