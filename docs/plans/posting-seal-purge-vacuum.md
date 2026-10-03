# Posting-seal purge recovery and VACUUM implementation handoff

## Status and operator intent

This is an implementation plan, not a record of completed implementation. The
original investigation reran the existing probes without changing production
code. The operator asked for a handoff that another session can use.

Read [the follow-up safety gate and transfer status](posting-seal-purge-vacuum-safety.md)
before continuing. The documentation-only checkpoint does not include the local
Java implementation or new regression tests described in that follow-up.

The operator's primary safety requirement is:

> VACUUM must not delete files that it is not supposed to delete.

Prefer leaving uncertain garbage behind over deleting live, staged,
reader-visible, or recovery-needed data. A green query after vacuum is not enough
proof: tests must also detect attempted deletion of protected files.

Read `CLAUDE.md` and applicable project instructions before starting. The operator
explicitly excluded the `fix-pr` skill during this investigation. Do not launch
subagents, commit, or push solely because this handoff exists; follow the next
session's operator instructions.

### Repository snapshot

- Repository: `/Users/vladilyushchenko/dev/questdb`
- Branch: `fix/parquet-last-stale-open-partition`
- HEAD at handoff: `9ceeb0294ce4913e598ee7f136d29b0abe195a9b`
- Unbounded-queue change: `316d3d4728580422ea91913013753eabc2fc316a`
- Tested base: `4e8ea7edc90b83afc8ede821e637c9e3772885f6`
- Remote master fetched during investigation:
  `b7bb98f43d32af424f897c56ef2098862fb948b1`

That remote master differs from the tested base only in three test files. Its
relevant production purge code is identical to the tested base. HEAD differs
from `316d3d47` only in an O3/parquet test. Recheck refs and the working tree in the
new session; do not assume these remain current.

## Agreed architectural direction

Follow the existing O3 and UPDATE cleanup architecture:

1. Best-effort automatic cleanup.
2. Reader-safe deletion and retry.
3. Independent disk rediscovery through existing `VACUUM TABLE` when cleanup
   notifications or log entries are missing.

Two observable behaviors must improve:

- A posting purge-log I/O failure must not stop the consumer from attempting
  cleanup, as `ColumnPurgeJob` already demonstrates.
- Losing volatile posting purge intents across restart must not make obsolete
  seals permanently undiscoverable: `VACUUM TABLE` must recover supported cases
  without the original queue entries, spill file, or purge-log rows.

Keep the unbounded posting queue and the indexer-release fix. They prevent the
original parquet-transition/backlog failure. Preserve existing legacy spill-file
recovery and existing direct-persist uses.

### Deliberately not part of this plan

- Restoring the bounded queue or adding a queue-capacity/backlog policy.
- Adding a contended queue-size counter to the normal publication path.
- A shutdown flush or a global shutdown-order redesign.
- A new SQL command, mandatory offline VACUUM mode, or a global maintenance lock.
- A new purge journal/storage format without a demonstrated safety need and an
  explicit decision about compatibility.
- Guaranteeing durable delivery of every cleanup notification across a crash.
- A general rewrite of O3, column purge, or all VACUUM infrastructure.

An earlier suggestion to restore a posting-specific deferred-overflow threshold
was superseded by the architectural comparison with O3 and UPDATE. Do not treat
that earlier suggestion as the selected implementation.

## What the investigation proved

On the base, the bounded ring is volatile. `PostingIndexWriter` also has a bounded
local outbox that can drop oldest intents under sustained saturation. Master
therefore already has cleanup-loss paths.

However, the O3/deferred `TableWriter` path retains overflow and can persist it or
spill it at physical writer close. The unbounded-queue change releases those
ready deferred copies immediately after enqueueing, so this particular fallback
no longer receives them. This is a real, but narrower, regression than saying
master previously guaranteed purge durability.

The identical probe produced:

| Scenario | Base files remaining after engine reopen | Queue-change head |
| --- | ---: | ---: |
| 64 O3 commits, one purge-log I/O failure | 24 / 655,360 bytes | 189 / 5,160,960 bytes |
| 64 commits, no fault, consumer runs | 0 | 0 |
| 4 commits, fault, below ring capacity | 9 / 245,760 bytes | 9 / 245,760 bytes |

The base recovered 55 persisted intents; each accounted for three files. Both
revisions passed the probe's query and live-file checks. The probe prints leftover
counts rather than asserting successful reclamation, so its green Maven result
alone is not a passing reclamation regression test. It recreates engines in one
JVM, not a separate server process.

### Reproduction artifacts

- Original fixture:
  `/tmp/questdb-pr7644-review.pb1eci/Pr7644PurgeRestartProbeTest.java`
- SHA256:
  `f135d5535e51a7485142849d623cb3611dec07770d3954ff7447036bc7faedf8`
- Fresh head run:
  `/tmp/questdb-seal-purge-validation.XpIv5U/head-restart-probe.log`
  (results around lines 898-899).
- Fresh base run:
  `/tmp/questdb-seal-purge-validation.XpIv5U/base-restart-probe.log`
  (55 recovered intents at line 951; results around 971-972).
- Command, run serially in the existing head/base worktrees:
  `mvn -pl core -Dtest=Pr7644PurgeRestartProbeTest test`
- Environment: macOS arm64, GraalVM CE 25.0.2, Maven 3.9.14.

Temporary paths may disappear. Convert the useful scenarios into maintained
repository tests. Existing `/tmp/questdb-pr7644-base` and `...-head` worktrees
contain review probes and, on base, other pre-existing test edits. Do not reset or
clean those worktrees indiscriminately.

## Existing implementations to follow

Paths below are relative to the repository root. Line numbers are approximate
at the handoff revision; follow method names if the source moves.

| Component | Relevant behavior |
| --- | --- |
| `core/src/main/java/io/questdb/cairo/TableWriter.java`, `processPartitionRemoveCandidates()` area, around 10549 | O3 attempts immediate safe removal, otherwise schedules partition discovery; a full discovery queue logs an error. |
| `core/src/main/java/io/questdb/cairo/O3PartitionPurgeJob.java`, `discoverPartitions()` and `processPartition0()` | Scans directory versions, reads `_txn`, and uses transaction-scoreboard/checkpoint checks. It does not require the original obsolete-partition intent. |
| `core/src/main/java/io/questdb/griffin/PurgingOperator.java`, `purgeColumnVersionAsync()`, around 251 | On queue overflow, explicitly logs: `cannot schedule column purge, purge queue is full. Please run 'VACUUM TABLE ...'`. |
| `core/src/main/java/io/questdb/cairo/ColumnPurgeJob.java`, `processInQueue()` and `saveToStorage()` | Continues dequeueing and attempting purge even if log persistence releases the log writer. Logging and cleanup liveness are separate. |
| `core/src/main/java/io/questdb/cairo/VacuumColumnVersions.java` | Rediscovers obsolete column versions from disk and committed column-version metadata; attempts cleanup and queues blocked work. |
| `core/src/main/java/io/questdb/cairo/ColumnPurgeOperator.java` | Uses table/truncate identity and reader checks; explicitly defers deletion during checkpoints. Already handles posting sidecars for obsolete whole column versions. |
| `core/src/main/java/io/questdb/griffin/SqlCompilerImpl.java`, `compileVacuum()`, around 4302 | Existing `VACUUM TABLE` execution, validation-only behavior, and scheduling integration. |

Posting-specific files:

- `core/src/main/java/io/questdb/cairo/PostingSealPurgeJob.java`
- `core/src/main/java/io/questdb/cairo/PostingSealPurgeOperator.java`
- `core/src/main/java/io/questdb/tasks/PostingSealPurgeTask.java`
- `core/src/main/java/io/questdb/cairo/idx/PostingIndexUtils.java`
  (`scanSealedFiles()`, existing filename parser and file visitors)
- `core/src/main/java/io/questdb/cairo/idx/PostingIndexChainHeader.java`
- `core/src/main/java/io/questdb/cairo/idx/PostingIndexChainEntry.java`
- `core/src/main/java/io/questdb/cairo/idx/PostingIndexChainWriter.java`
- `core/src/main/java/io/questdb/cairo/idx/PostingIndexWriter.java`

## Non-negotiable VACUUM safety contract

### Prove obsolescence, not merely absence from one snapshot

A file is not garbage just because its seal number differs from the current
head, because a log row is missing, or because its filename looks old.

Before permitting deletion, the implementation must establish all of these:

1. **Exact identity:** the table, partition version, posting-column version, seal,
   and covering-column/file identities still refer to the intended objects.
   Drop/recreate, truncate, reindex, column replacement, and partition conversion
   must not let a stale candidate target newly live data.
2. **Committed-state safety:** the current committed table view does not need the
   file. Account for a chain head that represents a not-yet-committed generation;
   the preceding committed generation may still be the live view.
3. **Reader safety:** no active reader can require the target generation. Use
   table-transaction semantics and the scoreboard, not seal numbers as a proxy.
4. **Writer/recovery safety:** the file is not staged for publication, needed for
   rollback/recovery, or eligible for reuse under a future publication that can
   race with deletion.
5. **Covering-file safety:** no live/recoverable generation shares or references
   the covering file, including legacy covering layouts and column-version aliases.
6. **Checkpoint safety:** a checkpoint must not lose files referenced by its
   metadata. Check deletion-time state, not only state at VACUUM invocation.

The existing posting operator's head checks are not, by themselves, a proof for
new disk-discovered tasks. In particular, checking again after unlink detects
some races only after damage has occurred. Establish an actual safe ordering or
prove the candidate cannot become live again. Do not accept a check/unlink race
as resolved by adding another check.

### Fail closed

- Missing/unreadable/corrupt metadata is not proof of obsolescence.
- Skip or report ambiguous generations and unsupported layouts; do not infer a
  safe transaction range from file age, file size, or a seal-number comparison.
- Use the existing posting filename parser. Leave unrelated or malformed files
  alone; do not implement a wildcard deletion of `.pv` or `.pc*`.
- Preserve existing path/symlink/detached-partition handling; do not broaden the
  filesystem traversal or follow links into unrelated data.
- Do not rewrite, truncate, or rebuild live indexes as a side effect of vacuum.
- Do not silently report complete reclamation when candidates remain deferred.

### Design gate before writing the scanner

Document the candidate-classification rule and its race argument first. For
retained chain entries, determine whether their publication transactions provide
safe retirement information. For files whose history is no longer present,
prove any conservative snapshot cutoff and generation-allocation assumptions
against normal seal, failed publication, rollback, recovery, and reopen paths.

The investigation has NOT yet proven a general safe classification algorithm for
all of these cases. Do not invent one from the field names. If the existing
metadata cannot prove safety for a case, defer that case and report the limitation.
If satisfying the required supported cases requires new on-disk information or
additional synchronization, bring that concrete need to the operator before
expanding scope. The safety requirement outranks complete reclamation.

## Implementation sequence

### 1. Add separate regressions for the two behaviors

Use real table/index operations to generate old seals, based on the existing
restart probe. Do not rely solely on forged tasks or synthetic filenames.

- **Consumer liveness:** inject a real purge-log open/append/commit failure;
  continue producing work and prove that cleanup continues, including after a
  reader-blocked task becomes eligible.
- **Independent rediscovery:** withhold consumer execution, create obsolete
  generations, close/reopen the engine so volatile intents disappear, and verify
  that no purge-log rows or spill file contain the missing work. Run
  `VACUUM TABLE`, then any scheduled purge work, and assert reclamation.

Do not use only the original log-failure scenario to test rediscovery: fixing
consumer liveness may clean its files before restart and leave VACUUM untested.
Use path identities rather than platform-specific byte totals as primary asserts.

### 2. Separate logging from cleanup liveness

In `PostingSealPurgeJob`:

- Remove the open-log-writer prerequisite from dequeueing.
- Continue purge attempts and existing backoff/retry behavior after persistence
  fails, following `ColumnPurgeJob`.
- Keep completion-log operations conditional on a usable writer and valid row ID.
- Audit append and commit failure paths, including partially persisted batches,
  pooled `RetryEntry` reuse, and stale `logRowId` values.
- Update tests/helpers that conflate an open log writer with a live consumer.
- Preserve native-resource cleanup on every error path.

Do not add log-writer reacquisition, a new retry scheduler, or shutdown draining
as an incidental part of this patch. Those are not needed to follow the existing
column-purge liveness behavior.

### 3. Add focused posting-seal discovery

A separate helper such as `VacuumPostingSealVersions` can keep the logic isolated.
Use the existing scanner/parser and chain-reading APIs after the safety design
gate above passes.

Initial responsibility: obsolete seals within a live posting-column version.
Keep obsolete whole partitions and whole column versions with their existing
purge implementations. Explicitly document supported layouts; never delete
ambiguous legacy, staged, or converted-partition files merely to meet a count.

Build valid posting purge tasks only from proven candidates, with a justified
reader window and complete identity. Reuse the existing operator for deletion,
with any necessary safety corrections, rather than duplicating unlink logic.
Keep discovery repeatable without relying on a durable discovery record.

Consider large directories and pinned readers: avoid unnecessary per-file object
allocation or an unbounded temporary object graph. Follow existing VACUUM
scheduling/failure conventions and report incomplete/deferred work rather than
silently dropping it. Do not introduce queue-size contention on ingestion.

### 4. Integrate with existing `VACUUM TABLE`

Invoke the helper from the existing vacuum execution path in
`SqlCompilerImpl`/`VacuumColumnVersions`, whichever yields the narrower ownership
and resource-lifetime change.

- Preserve SQL syntax, authorization, validation-only execution, checkpoint
  restrictions, and non-posting table behavior.
- Attempt safe cleanup and schedule reader-blocked work using existing machinery.
- Make repeated invocations and partial-failure retries harmless.
- Close helper resources with their owning compiler/vacuum component.
- Do not introduce a new SQL result shape merely to report counters; use existing
  diagnostics conventions and precise test assertions.

### 5. Audit deletion-time protection

`PostingSealPurgeOperator` currently has no explicit checkpoint check comparable
to `ColumnPurgeOperator`. Add the appropriate deletion-time protection, including
for a checkpoint that starts after discovery but before queued work executes.

Audit generation reuse and identity changes across delayed execution. Discovery
and deletion must implement the same safety argument; a task cannot become safe
merely because it reached the queue. Preserve ordinary posting purge correctness
as well as the new VACUUM cases.

## Required test matrix

All native-memory tests must use `assertMemoryLeak()`. Use the fluent
`assertQuery(...).returns(...)` assertions for deterministic SQL. Read project
guidance for any additional conventions.

### Positive recovery

- Lost queue intents plus engine reopen, empty log/spill, then VACUUM.
- Multiple posting indexes and covering columns.
- Repeated VACUUM: the second run changes nothing further.
- Partial deletion failure, retry, and restart between attempts.
- Purge-log failure while automatic cleanup still makes progress.
- Timestamp/partition variants supported by the implementation.

### Must-not-delete cases

- Current live `.pv` and all covering files it references.
- Old generations held by an active reader: preserve them until that reader
  releases its transaction, then prove eventual reclamation.
- A writer paused before publication and before table commit.
- Failed publication, rollback, writer reopen, and seal-number reuse.
- An uncommitted chain head with an older committed generation beneath it.
- Covering aliases/legacy formats and renamed/re-added columns.
- Drop/recreate, truncate, index replacement, and partition conversion racing
  with discovery or deferred deletion.
- Checkpoint active during scan; checkpoint starting after scan before deletion.
- Unreadable/corrupt metadata, malformed filenames, and unrelated files.
- Non-posting tables and existing O3/column VACUUM behavior.

Use deterministic barriers/fault hooks, not sleeps, for race tests. Exercise the
window between classification and actual unlink.

### Assert safety directly

For quiescent fixtures, capture a manifest of protected paths and file contents
before VACUUM, and verify it afterward and after engine reopen. Account explicitly
for legitimate metadata changes in concurrent-writer tests.

Instrument deletion attempts for protected posting/data/metadata paths. Fail if
VACUUM attempts to delete a protected file even if the filesystem rejects the
operation or a racing writer later recreates the path. Checking final existence
alone can miss a destructive race.

Check both old-reader and current-reader results, indexed predicates and an
independent data access path, including null symbol/covering values. Verify that
only the proven-obsolete generation groups disappear, not merely that the total
number of files decreases.

## Validation and acceptance

Run Maven test commands serially. Start with the new focused tests, then relevant
posting, column-purge, VACUUM, O3, and parquet suites. Existing useful suites include:

- `PostingSealPurgeTest`
- `PostingIndexCriticalIssuesTest`
- `O3ParquetLastPartitionTest` (retain the two-index/backlog regression)
- `O3PartitionPurgeTest`
- `VacuumColumnVersionTest`
- `VacuumTablePartitionTest`

Use repository build guidance for the pinned client; do not run `clean` in the
client module to repair dependencies. Investigate every failure rather than
labeling it unrelated without evidence. Check scan cost and concurrent-ingestion
impact; do not claim zero regression merely because the new scanner is off the
normal ingestion path.

Completion requires evidence for all three:

1. Purge-log failure does not permanently stop cleanup consumption.
2. VACUUM can reclaim the supported lost-intent restart fixture without the
   original intent records, while preserving query results.
3. Safety tests prove that live, reader-visible, staged, recovery-needed,
   checkpoint-needed, and unrelated files survive, including attempted-delete
   assertions across deterministic races.

Report any cases conservatively deferred by the scanner and any remaining
limitations. Describe the contract as best-effort automatic cleanup plus safe
rediscovery, not universal crash-durable queue delivery. Keep related changes on
the existing branch/PR; no new PR or push is authorized by this plan alone.

## Suggested new-session starting instruction

Read this file and `CLAUDE.md`, inspect the current branch/worktree, and implement
the scoped plan. Start by documenting the posting candidate-classification and
concurrent-deletion safety argument, and by adding the separate liveness and
lost-intent VACUUM regressions. Do not guess when file ownership is ambiguous.
