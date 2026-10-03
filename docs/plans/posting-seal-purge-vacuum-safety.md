# Posting-seal purge: implementation state and VACUUM safety gate

This supplements `posting-seal-purge-vacuum.md`. The implementation started from
`9ceeb0294ce4913e598ee7f136d29b0abe195a9b` on
`fix/parquet-last-stale-open-partition`. The operator requested a documentation-only
commit and push to continue from another device.

## Transfer scope

This checkpoint contains the two handoff documents, not the local Java changes.
The source device still holds these uncommitted files:

- `core/src/main/java/io/questdb/cairo/PostingSealPurgeJob.java` (modified)
- `core/src/main/java/io/questdb/cairo/PostingSealPurgeOperator.java` (modified)
- `core/src/test/java/io/questdb/test/cairo/PostingSealPurgeRecoveryTest.java` (new)

The implementation and validation sections below describe that local work. A
checkout on another device will not contain it or the local test logs. Obtain or
recreate those changes before continuing, and rerun the tests. The operator has
not yet approved the additional synchronization discussed below.

## Implemented locally so far

- `PostingSealPurgeJob` consumes new tasks and retries reader-blocked tasks even
  after log creation, open, append, or commit fails. It does not reacquire the log
  writer, drain on shutdown, or change either publication queue.
- Disabling the log writer closes the completion-file descriptor and invalidates
  queued log row IDs. The job registers a dequeued entry for retry before it tries
  persistence, and resets its log row ID before every persistence attempt.
- `isJobAliveForTesting()` reports the consumer's operator lifetime rather than
  the log writer's lifetime. Calling a closed job does not consume more tasks.
- `PostingSealPurgeOperator` checks checkpoint state before value-file deletion
  and before each covering-file deletion. Checkpoint-blocked tasks stay retryable.
- `PostingSealPurgeRecoveryTest` generates real O3 seals for two posting indexes
  with covering columns. It exercises NULL symbols and covering values, keeps an
  old reader pinned, submits more work after persistence fails, releases the
  reader, and asserts exact reclamation of obsolete seal groups. It checks both
  indexes and a base-column query, protects file contents and deletion attempts,
  and reopens the engine after partially committed log batches. A deterministic
  filesystem callback starts a real checkpoint between value-file and covering-
  file deletion to exercise the per-file checkpoint guard.

The original unbounded queue, indexer-release fix, direct persistence, and legacy
spill handling remain unchanged. The existing reuse-race diagnostic remains a
**diagnostic, not a safety fix**.

## Candidate rule considered, not implemented

For a live partition and posting-column instance, walk only reachable chain
entries. A retained entry E with publication transaction A has a retirement
transaction B only when a newer, distinct-seal entry S supersedes it and S is
committed in the pinned table view. Readers select the newest entry visible to
that table transaction. E would require the reader window [A, B), or a wider
conservative window [0, B). An uncommitted head does not retire the preceding
committed entry. Equal-publication-transaction entries need the chain's ordering,
not an ordering inferred from the filenames.

Copy-on-write head migration and recovery trimming can leave unreachable entries
that share the live seal's files. Their absence from the reachable chain is not
proof of garbage. A scanner must not turn them into additional purge candidates.
Files with no retained history need a separately proven allocation cutoff; this
implementation has not accepted such a cutoff.

Even the retained-entry rule requires a stable index-instance identity and a
proof that the target names cannot become live between classification and unlink.
That proof currently fails.

## Concrete ordering and identity gap

The following paths prevent treating a seal suffix as a permanent identity:

1. `PostingIndexChainWriter.peekNextSealTxn()` returns `genCounter + 1` without
   reserving it. `appendNewEntry()` advances the counter at publication. A failed,
   never-published seal can therefore reuse the same names after writer reopen.
   `PostingSealPurgeOperator` documents this exact race and checks for it *after*
   unlink, when it is too late to protect the file.
2. A committed-newer-seal cutoff can exclude that particular staged-reuse case,
   but it does not survive an index reset. `IndexBuilder.doReindex()` obtains the
   existing `columnNameTxn`, removes the index files, and calls
   `createIndexFiles()` with the same partition and column names.
   `PostingIndexWriter.initKeyMemory()` starts the generation counter at -1.
   `RebuildColumnBase` takes the table file lock for the standalone reindex path;
   the purge operator does not take that lock or coordinate with its owner.
3. `TableWriter.createIndexFiles(..., allowDestructiveRecovery=true)` also permits
   a reset in the existing partition/column instance during constructor recovery.
   `PostingIndexUtils.hasInitialisedKeyFileHeader()` returns false on an `openRO`
   failure as well as on a missing or invalid header. The caller can remove and
   reinitialize the existing `.pk`. This means a concurrent constructor can reset
   an index that a scanner just read successfully.
4. Covering names have an additional alias: `coverDataFileName()` represents both
   posting column name transactions -1 and 0 with host segment 0, and the sealed
   filename parser decodes that segment as -1. A task cannot treat a matching
   numeric suffix alone as complete ownership evidence.

A damaging ordering is consequently possible even for a retained candidate:

- Purge reads an old chain and identifies a retired seal N.
- Purge passes the reader-window and pre-unlink head checks.
- Rebuild/recovery resets the chain under the same partition and column names,
  then stages or publishes files at N again.
- Purge unlinks one of the newly needed files.

The transaction scoreboard gates reader transactions; it does not reserve seal
numbers, exclude a writer, or identify a `.pk` incarnation. A second header read
or an after-unlink existence test does not serialize these operations.

The baseline tests
`PostingSealPurgeTest.testPurgeLogsReuseRaceWhenLiveHeadDeletedByOrphanUnlink`
and `testPurgeLogsReuseRaceWhenLiveHeadCoverFileDeletedByOrphanUnlink` deliberately
exercise the existing after-unlink diagnostic. Their green result does not prove
that the operator prevents deletion. The broader rebuild/reset argument above
comes from the production call paths, not from a completed concurrency regression.

## Operator decision required before scanner implementation

The handoff explicitly requires operator approval before introducing additional
synchronization or on-disk identity information. No scanner or VACUUM integration
has been added while this gate remains open.

The preferred next investigation is writer exclusion using existing per-table
ownership, with purge deferring when it cannot acquire that ownership. The
critical section must span identity/committed-state validation through all
unlinks, and must exclude staging, reset, rebuild, and recovery, not just the final
chain-head publish. Deferred tasks must reclassify under the same protection.
Simply acquiring a `TableWriter` is not automatically acceptable: opening it can
run recovery and modify the index, which VACUUM must not do as a side effect.

This option avoids a new disk format but can delay reclamation under sustained
writer activity and affect writer-pool reuse or ingestion latency. The lock's
coverage and cost need validation before choosing its concrete API. If existing
ownership cannot provide that contract without destructive side effects, a
narrower shared purge/writer guard or an index-incarnation change needs a separate
explicit decision.

## Validation and remaining work

Serial Maven command:

```sh
mvn -pl core -Dtest=PostingSealPurgeRecoveryTest,PostingSealPurgeTest,PostingSealPurgeTaskTest,PostingIndexCriticalIssuesTest,O3ParquetLastPartitionTest,O3PartitionPurgeTest,VacuumColumnVersionTest,VacuumTablePartitionTest test
```

The run completed with 259 tests, zero failures/errors, and one skipped
`O3PartitionPurgeTest.testDedupWithPartitionPurge[timestamp=NANO]` assumption.
Local output: `core/target/posting-seal-purge-regression-suites.log`.
The new log-failure tests failed against the original consumer. A negative control
removed both checkpoint guards: both checkpoint tests failed on attempted deletion
of protected paths, even though the test facade refused to unlink them. The final
suite run used the restored guards. Negative-control output:
`core/target/posting-checkpoint-negative-control.log`.
The tests decode intercepted paths with `Utf8s.stringFromUtf8Bytes(LPSZ)`; neither
`PathLPSZ.toString()` nor its nullable `Utf8s.toString()` wrapper decodes the path.

The lost-intent/restart VACUUM regression, scanner, supported-layout proof,
classification/unlink race tests, and ingestion/scan-cost measurements remain
pending. This partial implementation does not satisfy the complete handoff's
acceptance gate and does not claim safe independent rediscovery or universal
crash-durable delivery.
