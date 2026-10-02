# Review of PR #7595 at level 3

Reviewed head `157f6e188c` against base and merge-base `f96b1179b7`.

**Verdict: approve with comments.** No open Critical findings. One Moderate finding, originally filed as Critical and downgraded after discussion (see "Severity resolution" below): after a table is switched from WAL to BYPASS WAL, a normal in-order INSERT into its composite last partition fails. It only happens with `cairo.o3.partition.merge.append.enabled=true`, which is off by default, and a 3-line fix works in a scratch tree.

## Critical

None open.

## Moderate

### INSERT fails after switching a composite table to BYPASS WAL

- **Problem:** A normal in-order INSERT fails with a bare `AssertionError`.
- **Net impact:** In-order INSERTs into the last partition of a converted merge-append table fail until the table is converted back.
- **Evidence:** The same probe fails at `157f6e188c` and passes at base `f96b1179b7`.

**Where:** `core/src/main/java/io/questdb/cairo/TableWriter.java`
- `openLastPartitionAndSetAppendPosition`, lines 11723–11737: leaves the columns of a composite last partition unopened. The check at lines 9770–9786 treats composite like Parquet whether or not the table uses WAL.
- `newRow`, lines 3341–3398: sends the row to the unopened columns.

**How to trigger it** (all through supported SQL):
1. Enable the merge-append flag.
2. Create a WAL table and make its last partition composite with a backdated insert.
3. Run `ALTER TABLE x SET TYPE BYPASS WAL`, then reload.
4. Run `INSERT INTO x VALUES (555, '2020-02-03T04:00:00')` for a time inside that partition, at or after its current maximum.

**What happens:**
- `TableConverter` switches the table to BYPASS WAL but leaves the partition composite.
- `newRow` only rejects direct writes while the table is still WAL, so after the switch that guard no longer applies.
- An in-order row inside the last partition skips both the out-of-order path and the partition switch. It goes to `updateMaxTimestamp`, which writes to a timestamp column that was never opened.
- The assertion `fd != -1` in `TableUtils.mapRW` (line 1594) fails.
- With assertions off, the behaviour was not tested.

**Base comparison:** On base, the same SQL opens a normal partition and the INSERT succeeds; the query then returns the row.

**Existing coverage misses it:** `PartitionCompactionScanJobTest.testCompositeSwapInsideAnOpenTransactionIsRefused` converts a table whose last partition is plain and inserts a backdated row, so it never reaches this path.

**Suggested fix, verified in a scratch tree:** in `newRow`, send rows for a composite last partition down the out-of-order path:

```java
if (txWriter.isPartitionComposite(txWriter.getPartitionCount() - 1)) {
    return newRowO3(timestamp);
}
updateMaxTimestamp(timestamp);
```

With this change, the probe plus `PartitionCompactionScanJobTest`, `CompositeConvertColumnTypeTest`, `CompositePartitionMergeAppendDisabledTest` and `O3CompositePartitionTest` pass: 66 tests. Adding the probe as a regression test in `composite/` would pin it.

### Severity resolution

The finding was first filed as Critical. After discussion with the author it is downgraded to Moderate:

> There is a workaround: convert back to WAL and squash partitions. Also, 99% of the time conversion to non-WAL is done purely to remove outstanding WAL transactions and then convert back to WAL.

I agree with the downgrade. The review rules count an established operating procedure as an offset when it clears the condition before users are affected:

- **The trigger is narrow.** It needs the merge-append flag (off by default), then a BYPASS WAL conversion while the last partition is composite, then an in-order INSERT into that partition before switching back.
- **The usual workflow avoids it.** When BYPASS WAL is a temporary step to clear outstanding WAL transactions, the table goes back to WAL without direct in-order inserts in between.
- **The failure is loud.** The INSERT is rejected with an error. Nothing showed rows lost, stored wrongly or left corrupted. Converting back to WAL and squashing gives a working table again.

Two caveats remain:
1. The remaining case is a table kept in BYPASS WAL long term for direct ingestion. There, every in-order INSERT into the last partition fails with a bare `AssertionError` and no hint about the workaround. The 3-line fix above removes that, so I'd still suggest taking it.
2. The WAL-and-squash recovery was not re-executed in this pass. It rests on the earlier review round's report and a reading of the code.

## Minor

- **PR title:** The title has no `breaking change 💥` marker, although `SHOW PARTITIONS` / `table_partitions()` now return three more columns. The PR description does document the change.

## Adjacent findings

None proved.

## Coverage

- **Test gate:** passes. There are no admitted coverage gaps, other than the missing regression test for the Moderate finding.
- **Tests at the PR head:** 852 tests in 50 classes, with 0 failures, 0 errors and 14 skipped (all in `UpdateTest`). They cover:
  - all of `composite/`
  - the compaction and scan-job suites
  - Parquet compaction
  - `CopyExportTest` and `SortedSymbolIndexOrderingTest`
  - page-frame tests
  - `WriterPoolTest`, `ShowPartitionsTest`, `UpdateTest` and `O3SquashPartitionTest`
- **Latest commit (`62e4c05bd6..157f6e188c`):** The change to MAKE-PLAIN's two-commit retry and the new Windows truncate simulation are covered by `testMakePlainDeclinesWhenTrimFilesFailsThenRetries`. That test checks the refusal, that the partition stays composite, that E equals the live rows, that the file still covers the rows, and the retry trim. It passes.

## Summary

- **Verdict:** approve with comments. Please consider the Moderate fix and its regression test.
- **Correctness gate:** passes; no open Critical.
- **Test gate:** passes.
- **Severity:** 0 Critical, 1 Moderate (downgraded from Critical), 1 Minor.
- **Where the Moderate sits:** in the diff.
- **Submodules:** no pointers moved.
- **Scope:**
  - I focused on the code that runs regardless of the flag (the compaction worker, Parquet compaction, the logical-partition merge, the removed sorted-index fast path and `SHOW PARTITIONS`), on the flag-on write and read paths, and on the latest commit.
  - The native kernels and committed binaries were reviewed from source only; I did not run them on other platforms.
  - Not run: the full suite, other platforms, the enterprise tandem PR #1206, and production-scale benchmarks.
- **Validation limits:** Several other hypotheses could not be run within this review and are not reported.

The probe source, head/base/fix logs and the surface and coverage maps are retained locally and available on request.
