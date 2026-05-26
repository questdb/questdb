# Handover: WalWriterFuzzTest.testAddDropColumnDropPartition posting-index corruption

## Update 2026-05-25: reduced reproducers and fix shape

The current working tree has deterministic reduced coverage for the stale
posting-index cursor/cache path. The reduced recipe is:

1. Build a sparse posting index where requested key `357` is present and has
   multiple rows.
2. Open a cursor for key `357` and consume only one row.
3. Rebuild or reload the same reader so key `357` is no longer present, while
   the new key count still covers `357`.
4. Continue the stale cursor.
5. Open a fresh cursor for key `357`.

The bug shape is not "arbitrary concurrent mutation of a
`PostingIndex*Reader`". The proven path is narrower: a live cursor can outlive
a same-reader reload, and the cursor was not an immutable snapshot. That
allowed the stale cursor to either return buffered rows from the old snapshot
or publish old sparse-gen cache entries into the reloaded reader. A fresh cursor
could then replay stale cache metadata against the new gen snapshot.

The current fix has two parts:

- `PostingGenLookup` now publishes a new gen snapshot and invalidates the cache
  in one synchronized `commitSnapshotAndInvalidateCache()` operation. This
  bumps both `cacheVersion` and `snapshotVersion`.
- Forward and backward cursors snapshot both versions at `of()` time. Cache
  hits are copied once into cursor-local `builderEntries` under the cache
  monitor, then replayed without locking. New cache entries are published only
  with the original cache version. `hasNext()` also checks `snapshotVersion`
  before serving any buffered state, so a cursor that survived a same-reader
  reload stops immediately.

This intentionally does not solve true cross-thread mutation of the same
reader object. Solving that globally would need a larger ownership change, for
example immutable/ref-counted snapshots that pin both metadata and mapped value
memory until all cursors are closed. That was not proven necessary for the
observed SQL/TableReader path.

Reduced tests now cover both directions and both entry paths:

- `PostingIndexStressTest#testSparseCacheBuildSkippedWhenReaderReloadsBeforeCursorEof`
- `PostingIndexStressTest#testSparseFwdCacheBuildSkippedWhenReaderReloadsBeforeCursorEof`
- `PostingIndexStressTest#testTableReaderSparseCacheBuildSkippedWhenReaderReloadsBeforeCursorEof`
- `PostingIndexStressTest#testTableReaderSparseFwdCacheBuildSkippedWhenReaderReloadsBeforeCursorEof`

Run them with the broader sparse-gen checks:

```
mvn -pl core -DforkCount=0 -DreuseForks=false \
  -Dtest=io.questdb.test.cairo.PostingIndexStressTest#testSparseCacheBuildSkippedWhenReaderReloadsBeforeCursorEof,io.questdb.test.cairo.PostingIndexStressTest#testSparseFwdCacheBuildSkippedWhenReaderReloadsBeforeCursorEof,io.questdb.test.cairo.PostingIndexStressTest#testTableReaderSparseCacheBuildSkippedWhenReaderReloadsBeforeCursorEof,io.questdb.test.cairo.PostingIndexStressTest#testTableReaderSparseFwdCacheBuildSkippedWhenReaderReloadsBeforeCursorEof,io.questdb.test.cairo.PostingIndexStressTest#testManySparseGensBwd,io.questdb.test.cairo.PostingIndexStressTest#testManySparseGensFwd \
  test
```

Current validation from 2026-05-25:

- The six posting-index tests above pass together:
  `Tests run: 6, Failures: 0, Errors: 0, Skipped: 0`.
- The pinned WAL fuzz probe is now self-contained. It sets the exhaustive
  test-only index scan and DESC-repeat properties internally, and it passes:
  `Tests run: 1, Failures: 0, Errors: 0, Skipped: 0`.

Run the WAL probe with:

```
mvn -pl core -DforkCount=0 -DreuseForks=false \
  -Dtest=io.questdb.test.cairo.fuzz.WalWriterFuzzTest#testAddDropColumnDropPartitionPostingIndexCrashRepro \
  test
```

There are no production debug verifier hooks in the current fix. The remaining
`questdb.debug.posting.repro.*` properties live in `FuzzRunner`, which is
test-only, and the pinned WAL test sets/restores them itself.

Performance note: the first correctness-only version synchronized every shared
cache replay entry. The current variant avoids shared-cache reads during replay
by copying a cache hit into the cursor once at `of()`, skips the cache monitor
entirely for dense-only snapshots, and uses a single volatile
`snapshotVersion` check at `hasNext()` entry. A temporary local micro probe over
50,000 cached backward scans of an 80-generation sparse key measured the final
shape at `1325 ns/cursor` and `16 ns/row`. Earlier local measurements were
about `1908 ns/cursor` for the all-synchronized replay variant.

A second temporary probe compared this fix against a clean shared clone of the
buggy master commit `a2ec6e18fd`. It used two JVM-process runs, five samples
per run, and reports medians over ten samples. Treat these as directional local
numbers, not JMH-grade benchmark results:

| case | buggy master ns/cursor | fixed ns/cursor | delta |
| --- | ---: | ---: | ---: |
| dense backward, 4096 rows | 8294 | 10334 | +24.6% |
| dense forward, 4096 rows | 8065 | 10334 | +28.1% |
| sparse backward, cache disabled, 80 gens | 2275 | 2228 | -2.1% |
| sparse forward, cache disabled, 80 gens | 2235 | 2193 | -1.9% |
| sparse backward, cached, 80 gens | 1136 | 1157 | +1.8% |
| sparse forward, cached, 80 gens | 1209 | 1232 | +1.9% |

The sparse/cache paths affected by the stale-cache fix are within about 2% of
buggy master in this probe. The measurable cost is dense cursor scanning:
approximately +2.0 to +2.3 us per 4096-row cursor, or about +0.5 ns per row,
from the per-`hasNext()` snapshot-version check.

## What you are inheriting

A CI failure in `io.questdb.test.cairo.fuzz.WalWriterFuzzTest.testAddDropColumnDropPartition`
on QuestDB master (commit `a2ec6e18fd` or its descendants). The test compares a
WAL table against a non-WAL reference and the WAL table's posting index returns
row IDs that point at rows whose actual symbol value does not match the
filter. Local repro is timing-dependent and has not been achieved with the
recorded seeds.

Working directory: `/home/jara/devel/oss/questdb-arrays` (QuestDB checkout).

## Failure signature

From `/home/jara/Downloads/testAddDropColumnDropPartition.log` (Azure build
236062, log lines noted below):

- L21178: `AssertionError: Expected cursor misses record 6` raised by
  `TestUtils.assertEquals` (TestUtils.java:376) inside
  `FuzzRunner.checkIndexRandomValueScan` (FuzzRunner.java:836).
- Query: `<table> WHERE "new_col_6" = 'DLM' ORDER BY ts DESC`.
- Expected cursor (`testAddDropColumnDropPartition_nonwal`) yields 6 rows, all
  with `new_col_6 = 'DLM'`.
- Actual cursor (`testAddDropColumnDropPartition_wal`) yields 9 rows: the same
  6 plus 3 extras whose projected `new_col_6` reads as `'VWFHGE'`, not `'DLM'`.
  Timestamps of the 3 extras: `2022-02-27T06:58:51`, `07:00:30`, `07:05:10`.

The data file is correct (projection shows VWFHGE). Only the WAL table's
posting index is wrong: its `DLM` bucket contains row IDs whose stored value
is `VWFHGE`.

Seeds the fuzz runner printed:

- `AbstractCairoTest random seeds: 8964636991863208L, 1779705560385L` (setUp #1)
- `AbstractCairoTest random seeds: 8964636994148316L, 1779705560387L` (setUp #2)
- `AbstractFuzzTest random seeds: 6555485809735410625L, 7649784381690840635L`
  (logged by `FuzzRunner.after()` -- this is the post-skip Rnd state; passing
  it back to `new Rnd(...)` reproduces the deterministic part of the run but
  not the worker-thread timing.)

## Sequence preceding the corruption (WAL apply trace)

For the WAL table `testAddDropColumnDropPartition_wal~4`, the 2024-02-27
partition went through these seqTxn applies before the assertion ran:

1. `seqTxn=125` REPLACE commit. `replaceRangeTsLo=2022-02-26T22:33:45`,
   `replaceRangeTsHi=2022-02-27T13:17:24`, new data
   `[2022-02-27T05:55:14, 2022-02-27T05:55:55]`. Creates partition `2022-02-27.78`
   with 107 rows. (log L18903, L18938)
2. `seqTxn=126` ALTER TABLE DROP PARTITION
   `WHERE ts > 2022-02-26T05:55:55 AND ts < 2022-02-27T05:55:55`. Purges
   `2022-02-26.59`. (log L19254)
3. `seqTxn=127` ALTER TABLE rename `c2` -> `new_col_4`. (log L19241)
4. `seqTxn=128..131` block of 4 commits, 645 rows in
   `[2022-02-27T06:57:41, 2022-02-27T08:41:31]`. The corresponding o3 task
   reports `partitionTs=2022-02-27T00:00:00`, `partitionIndex=-1`,
   `srcDataMax=0`, `partitionMutates=false`, `newSize=645`. New partition
   `2022-02-27.82` with index files `new_col_6.pk.40` /
   `new_col_6.pv.40.0` -- column-name-txn = 40, sealTxn = 0. (log L19280, L19336, L19360-L19399)
5. `seqTxn=133..135`, `137..138`, `140..147` further O3 appends into the same
   partition. The `.pv.40.0` file grows from ~20KB to ~98KB across these
   appends. Each o3 task reports `partitionMutates=false` and
   `newPartitionSize > oldPartitionSize`, so they all hit the
   `canSkipRebuild=true` branch in
   `TableWriter.sealPostingIndexForPartition` (TableWriter.java:11570).

The 3 bad row IDs land in the row range introduced by step 4 (the recreated
partition), at the earliest sorted offsets (06:58, 07:00, 07:05). The real
`DLM` rows come from steps 5+.

## Hypotheses on the table

### H1 -- canSkipRebuild fast path lets stale entries survive (recommended)

`TableWriter.sealPostingIndexForPartition` chooses between two code paths per
sealed partition (TableWriter.java:11434 - 11526):

```
if (canSkipRebuild) {
    indexer.getWriter().rollbackConditionally(partitionSize);
    indexer.getWriter().sealIfMultiGen(...);
} else {
    indexer.getWriter().discardForRebuild();
    long dataFd = openRO(ff, dFile(...), LOG);
    try {
        indexer.index(ff, dataFd, columnTop, partitionSize);
    } finally {
        ff.close(dataFd);
    }
    indexer.getWriter().commitDense();
}
```

Caller:
```
boolean canSkipRebuildForPartition = !partitionMutates
        && o3SplitPartitionSize == 0
        && newPartitionSize >= oldPartitionSize;
```
(TableWriter.java:11570)

`rollbackConditionally(partitionSize)` only evicts entries whose rowId is
**at or beyond** `partitionSize`. It cannot remove an in-range stale
`(key, rowId)` pair. The predicate is correct for an append-only growth of an
existing partition (caller comment claims "no rowid in `[columnTop, partitionSize)`
was rewritten") but it is also satisfied by the recipe in the failure:

- step 4: `partitionMutates=false`, `oldPartitionSize=0`, `newPartitionSize=645`
- steps 5+: `partitionMutates=false`, growth-only

If the dropped partition's posting state survives into the recreated
partition by any path the fast path cannot reach (in-memory pending state on
a reused indexer, chain head loaded from a sealTxn already used by the
dropped partition, sidecar mapping reused before reseal), the
`rollbackConditionally` call will not clean it up.

Codex previously evaluated this hypothesis as "more likely". See its analysis
on the conversation transcript.

### H2 -- writer not distressed on partial open failure (Vlad's hypothesis)

Conversation transcript: Vlad observed
> failure injected into opening posting index for last partition -- writer is
> not made distressed -- this means writer goes back into cache and gets
> reused in inconsistent state.

Log line 5499 shows the only matching injection event:
```
E i.q.c.i.PostingIndexWriter could not open posting index [path=.../_nonwal~/2022-02-25.45/sym2.pk]
I i.q.t.c.f.AbstractFuzzTest expected IO failure observed: io.questdb.cairo.CairoException: [0] Index file too short [expected>=8192, actual=-1]
  at io.questdb.cairo.idx.PostingIndexWriter.of(PostingIndexWriter.java:896)
  at io.questdb.cairo.idx.PostingIndexWriter.of(PostingIndexWriter.java:864)
  at io.questdb.cairo.SymbolColumnIndexer.configureFollowerAndWriter(SymbolColumnIndexer.java:127)
  at io.questdb.cairo.TableWriter.openPartition(TableWriter.java:8077)
  at io.questdb.cairo.TableWriter.openLastPartitionAndSetAppendPosition(TableWriter.java:7944)
  at io.questdb.cairo.TableWriter.initLastPartition(TableWriter.java:6695)
  at io.questdb.cairo.TableWriter.newRow(TableWriter.java:2534)
  at io.questdb.test.fuzz.FuzzInsertOperation.apply(FuzzInsertOperation.java:134)
  at io.questdb.test.cairo.fuzz.FuzzRunner.applyNonWal(FuzzRunner.java:275)
```

Two reasons this looks less likely as the *direct* cause of the corruption:

- The catch block at `TableWriter.openPartition` (TableWriter.java:8105) does
  set `distressed = true` and rethrows.
- The failure landed on the non-WAL comparison table; the WAL table's apply
  path uses the same shared `FailureFileFacade` (plumbed via
  `AbstractCairoTest.ff` from `assertMemoryLeak(fuzzer.getFileFacade(), ...)`
  in `AbstractFuzzTest.runFuzz`, AbstractFuzzTest.java:201) but there is no
  WAL-side `index error`, `commit failed`, `posting-index seal failed`, or
  `Table is suspended` event before the assertion fires.

That said, H2 may still be a real lifecycle bug worth a separate probe (see
"Probe ideas" below) even if it is not what is corrupting this particular run.

## What was tried locally and what failed

All attempts on QuestDB master (commit `a2ec6e18fd`):

1. **Replay with recorded seeds.**
   Edited `WalWriterFuzzTest.testAddDropColumnDropPartition` to pass the
   recorded seeds into `generateRandom(LOG, ...)` and `new Rnd(...)`. Bug did
   not reproduce. The `FuzzRunner.after()` seeds are the post-skip Rnd state
   and the run also depends on 4-worker apply timing, so seed alone is
   insufficient.

2. **Tight loop of 10 random-seed runs of the test.** No failures observed in
   the slice that finished before kill.

3. **Variant A deterministic test (simple SQL).**
   `PostingIndexCriticalIssuesTest.testIndexDoesNotLeakAfterDropAndRecreatePartition`:
   create WAL table with `SYMBOL INDEX TYPE POSTING`, seed `2024-01-01` with
   `'OLD'` plus a row in `2024-01-02`, drain, `DROP PARTITION '2024-01-01'`,
   drain, insert into `2024-01-01` again with `'NEW'`, drain, assert
   `count WHERE sym='OLD' == 0`. **Passed** -- bug not reproduced.

4. **Variant B mirror the fuzz commit shape.**
   Same test extended with explicit `WalWriter.commitWithParams(...,
   WAL_DEDUP_MODE_REPLACE_RANGE)` for the OLD partition, an `ALTER TABLE ADD
   COLUMN` between drop and recreate (mirroring `seqTxn=127`'s rename), an O3
   recreate batch carrying value `'EARLY'`, and two follow-up O3 append
   batches carrying value `'TARGET'`. The o3 task log confirms the predicate
   fires (`partitionMutates=false`, `partitionIndexRaw=-1`, `srcDataMax=0`,
   then `srcDataMax=3`, `srcDataMax=6`). **Still passed.**

Both variants were reverted from the working tree -- `git status` is clean.

## What is probably missing

Comparing variant B's o3 task output side-by-side with the fuzz log:

- Variant B's o3 task for the recreated partition reports `last=false`
  because there is a later `2024-01-02` partition. The fuzz log's seqTxn=128
  also reports `last=false`. So this is **not** the difference.
- The fuzz table has 22 columns going through many add/drop/rename/type-change
  operations before the failing partition is recreated.
- The fuzz test runs WAL apply on a `sharedWorkerPool` of 4 workers
  (`AbstractFuzzTest.sharedWorkerPool`). Variant B drains synchronously on
  the main thread via `drainWalQueue()`. There may be a concurrent-apply race
  the synchronous path does not reach.
- The fuzz IO-failure injector landed exactly one failure during this run,
  on the non-WAL table at `2022-02-25.45/sym2.pk`. If H2 is real and the
  WAL-side writer state is corrupted indirectly (shared symbol map, shared
  pool entry), the deterministic path would also need to inject a similar
  failure.

## What to do next

In rough priority order:

### 1. Add an in-process invariant assert and re-run the fuzz

Inside `TableWriter.sealPostingIndexForPartition`'s `canSkipRebuild=true`
branch, before returning, walk the persisted chain head's gen dirs and check
that no `(key, rowId)` pair with `rowId < partitionSize` resolves to a data
value that disagrees with `key`. This is expensive (loads the data file and
re-reads symbol IDs) so gate it behind a debug flag, e.g.
`DEBUG_VERIFY_INDEX_INVARIANTS` controlled by a `PropertyKey`. Then run the
fuzz test in a loop with that flag on:

```
node1.setProperty(PropertyKey.DEBUG_VERIFY_INDEX_INVARIANTS, true);
```

When it trips, you get a precise call site instead of a downstream cursor
mismatch. Code refs:

- `TableWriter.sealPostingIndexForPartition` at TableWriter.java:11358
- `canSkipRebuild` predicate at TableWriter.java:11570
- skip branch at TableWriter.java:11434-11445 (covering) and 11511-11516
  (no-covering)
- `PostingIndexWriter.rollbackConditionally` at PostingIndexWriter.java
  (search for the symbol)

Implementation sketch:
```
if (DEBUG_VERIFY_INDEX_INVARIANTS && canSkipRebuild) {
    // Open .d, scan rows [0, partitionSize), build expected
    // (rowId -> key) map. Then walk the chain head's encoded gens and
    // verify each (key, rowId) pair matches the expected map. Assert
    // failure includes column, partition timestamp, sealTxn, the
    // first conflicting rowId, expected key, observed key.
}
```

If the assert trips on the canSkipRebuild path with a workload that does not
inject IO failures, H1 is confirmed and you can promote the assertion into a
correctness fix (e.g. force `canSkipRebuild=false` when the partition was
recreated -- carry an explicit "recreated" flag through the o3 sink, or
detect via `partitionIndexRaw < 0 && oldPartitionSize == 0`).

If the assert never trips without IO injection, H2 becomes the leading
hypothesis.

### 2. Probe H2 directly with an IO injection test

Write a test in `core/src/test/java/io/questdb/test/cairo/PostingIndexCriticalIssuesTest.java`
that:

1. Creates a table partitioned by day with one indexed `SYMBOL INDEX TYPE
   POSTING` column.
2. Installs a `TestFilesFacadeImpl` that returns -1 from `length(LPSZ)` for
   the next call against a path matching `*/<column>.pk.*`, after a counter
   reaches a threshold.
3. Inserts rows that force `newRow -> initLastPartition ->
   openLastPartitionAndSetAppendPosition -> openPartition ->
   SymbolColumnIndexer.configureFollowerAndWriter -> PostingIndexWriter.of`
   for the indexed column.
4. Catches the CairoException, then immediately attempts another insert (no
   injection).
5. Asserts: the writer obtained for the second insert is a *different*
   instance than the first (i.e. the original was distress-closed and
   removed from the pool), AND a subsequent query against the indexed
   column returns consistent results.

Suggested name: `testIoFailureOnPostingIndexOpenDistressesWriter`. The catch
block under test is `TableWriter.openPartition` TableWriter.java:8105. The
pool path you want to assert against is
`WriterPool.returnToPool` -> `isDistressed`
(`core/src/main/java/io/questdb/cairo/pool/WriterPool.java:547-572`).

### 3. Targeted deterministic repro (only if 1 confirms H1)

Once an in-process assert can catch the leak, narrow it down by progressively
deleting columns / DDLs / commits from a fuzz-generated transaction list
until the assert still fires with the smallest input. Persist that input as a
test in `PostingIndexCriticalIssuesTest`. Likely shape:

- Indexed symbol column survives across one column rename.
- WAL replace commit creates partition `D` with rows containing some symbol
  `S_old`.
- DROP PARTITION `D`.
- One structural ALTER (column add / type change).
- O3 commit recreates partition `D` with rows containing a *different*
  symbol `S_filler`.
- One or more O3 append commits add later-timestamp rows containing
  `S_target`.
- Query `WHERE sym = S_target` and assert every returned row has projected
  value `S_target`.

Variant B above already encodes this shape; if H1 is confirmed under an
assertion, the missing element is one of the structural ALTERs or a
specific dedup/symbol-capacity interaction that the failing run hits but
mine does not.

## Source map you will need

- `core/src/main/java/io/questdb/cairo/TableWriter.java`
  - `sealPostingIndexForPartition` line 11358
  - `canSkipRebuild` predicate line 11570
  - `openPartition` distress catch line 8105
  - `openLastPartitionAndSetAppendPosition` line 7944
  - `initLastPartition` line 6697
- `core/src/main/java/io/questdb/cairo/idx/PostingIndexWriter.java`
  - `of(Path, CharSequence, long, boolean isInit)` line 867
  - `openFromO3Context` line 960
  - `setO3PathContext` line 1369
  - `discardForRebuild` (added in #7077)
- `core/src/main/java/io/questdb/cairo/O3CopyJob.java`
  - `updateIndex` line 864 (the loop that emits `(toIndexKey(symId), rowId)`)
  - `copyIdx` entry around line 808 (the `isInit = row == 0` branch)
- `core/src/main/java/io/questdb/cairo/O3PartitionJob.java`
  - o3 task setup
- `core/src/test/java/io/questdb/test/cairo/fuzz/WalWriterFuzzTest.java`
  - the failing test at line 97 (`testAddDropColumnDropPartition`)
- `core/src/test/java/io/questdb/test/cairo/fuzz/FuzzRunner.java`
  - `assertRandomIndexes` line 402, `checkIndexRandomValueScan` line 816
- `core/src/test/java/io/questdb/test/cairo/fuzz/FailureFileFacade.java`
  - `setToFailAfter` line 489, `checkForFailure` line 568, `length(LPSZ)`
    line 280 (returns -1 on injected failure)
- `core/src/main/java/io/questdb/cairo/pool/WriterPool.java`
  - `returnToPool` distressed check line 547-572

## Constraints

- Do not change behavior of `WalWriterFuzzTest.testAddDropColumnDropPartition`
  itself -- it is the canary.
- Do not weaken the existing `canSkipRebuild` predicate without an assertion
  that proves the bug. Forcing the slow rebuild path always would fix the
  bug if H1 is right, but at a measurable performance cost; need evidence
  first.
- The Rust crate is not involved in this bug.
- `java-questdb-client/` is a submodule and not involved.
- Follow QuestDB coding conventions in `CLAUDE.md`: alphabetized methods,
  ASCII-only log messages, `ObjList` instead of arrays, `assertMemoryLeak()`,
  `assertQueryNoLeakCheck` for queries (use `assertSql` only for storage
  tests where factory-property checks are irrelevant).
- PR title for any fix must follow Conventional Commits, e.g.
  `fix(core): fix posting index leak after drop+recreate partition`.

## Conversation transcript pointers

This handover summarizes prior work; the live conversation is in the user's
session. Key turning points already established:

- The IO failure at log L5499 is on the non-WAL table, not the WAL table.
- `partitionIndex=-1` in the o3 task at log L19336 confirms the 2022-02-27
  partition was treated as freshly recreated when the bad batch was applied.
- The `partitionMutates=false` flag in the o3 partition update at L19337
  was confirmed in local variant-B runs to match.
- Variants A and B were both removed from the working tree before this
  handover. `git status` is clean.

Good hunting.
