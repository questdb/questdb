# Posting Index Diff-Against-Chain Primitive — Design

## Status

Design captured, implementation not started. This doc records research and design decisions so a future implementation session can pick up cleanly.

## Goal

Replace the full-rebuild path that PR #7077 introduced in `TableWriter.sealPostingIndexForPartition` with a primitive that walks the existing chain head's gens, validates each `(key, rowId)` against the column `.d` file, and emits a fresh dense chain entry containing only the kept entries. This:

1. Fixes the same phantom-match bug (stale entries from O3 replace-range / dedup-replace / split where rowIds are reused with different values).
2. Closes the OOM hole that PR #7077's `discardForRebuild` path opened for partitions with billions of rows (it accumulated every `(key, rowId)` in private spill memory).
3. Avoids the chain-protocol violation (`extendHead` with `newGenCount < oldGenCount`) that PR #7077's first cut had — the diff produces a single dense gen and appends a new chain entry at a rotated `sealTxn`, which is the established `appendNewEntry` path.

## Why this is bigger than first sized

Three findings from the research pass:

1. **Sidecar verbatim-copy is partial.** Var-width covers and delta-mode per-key sub-blocks copy verbatim. Flat-mode stride sidecars cannot — bitwidth and base-value depend on the kept rowIds, so the stride is recomputed. A correct implementation needs both paths.
2. **The encode pipeline is long.** `reencodeAllGenerations` -> `reencodeWithStrideDecoding` -> per-stride trial encode (delta vs flat) -> `writeDeltaStride` / `writePackedStride`, plus `writeSidecarStrideData` for sidecars and `sealIncremental`'s dirty-stride logic for the incremental case. The diff has to either inject into this pipeline or clone a substantial part of it.
3. **`reencodeWithStrideDecoding` doesn't write sidecars.** Sidecars are written per-stride only in `sealIncremental`'s path (around `PostingIndexWriter.java:3905-3979`); `sealFull` -> `reencodeWithStrideDecoding` writes only the `.pv`, and sidecars come from elsewhere in the seal flow. Either the diff is layered onto `sealIncremental`'s dirty-stride pattern (force every stride to be "dirty") or it carries its own sidecar emit.

Realistic scope for a careful, well-tested implementation: ~1400 LoC across writer, callsites, and tests. ~600 LoC if we skip flat-mode sidecar handling and rely on `sealFull` to rebuild sidecars from cover `.d` files (loses the per-key copy-verbatim perf win for delta-mode sidecars but is much simpler).

## Design — full version

### New method on `PostingIndexWriter`

```java
public void diffAgainstChainHead(
        FilesFacade ff,
        long dataColumnFd,
        long columnTop,
        long partitionSize
);
```

Side effects, in publish order:
1. `flushAllPending()` first to settle any in-memory state.
2. If chain head has 0 gens or 0 keys: `setMaxValue(partitionSize - 1)` and return.
3. Allocate `keptCountsAddr` (`keyCount * 4` bytes) and a `~256 KB` `.d` read cache.
4. **Phase 1**: walk every gen of the head; for each `(key, rowId)`, validate `rowId in [columnTop, partitionSize)` AND `toIndexKey(.d[rowId]) == key`. Increment `keptCounts[key]` per validated entry. Compute `totalKept` and `maxStrideKept`.
5. If `totalKept == 0`: call `truncate()` (rotates `sealTxn`, queues purge) and return.
6. Allocate `newSealTxn = max(1, chain.peekNextSealTxn())`. Snapshot `oldSealTxn`. `closeSidecarMems()`. Open `valueMem` at the new `sealTxn` (mirrors `truncate()`).
7. If covering: reopen sidecar files at `newSealTxn`.
8. **Phase 2**: stride-by-stride decode + validate-filter + encode, mirroring `reencodeWithStrideDecoding` but with the validator filtering values per-key inside `strideValsAddr` between decode and encode. Sidecars: per-stride `writeSidecarStrideData` against the kept rowIds, reading cover bytes from the snapshotted old sidecar buffers (verbatim for delta-mode and var-width, recomputed for flat-mode).
9. `switchToSealedValueFile(newSealTxn)`. `genCount = 1`. `maxValue = partitionSize - 1`. `publishToChain(...)` (takes `appendNewEntry` because `sealTxn` rotated).
10. `recordPostingSealPurge(oldSealTxn)`.

### Phase 1 algorithm (pseudocode)

Mirror `reencodeAllGenerations` Phase 1 but validate during the decode:

```
allocate keptCountsAddr = malloc(keyCount * 4)
allocate readBuf, readBufCacheStart=-1, readBufCacheLen=0
totalKept = 0

for gen in 0 .. genCount-1:
    walk gen's keys (sparse: keyIds; dense: stride+key)
    for each (key, count, encodedAddr):
        decodeKeyToNative(encodedAddr, decodeBuf)  # existing primitive
        kept = 0
        for v in decodeBuf[0..count]:
            if v < columnTop or v >= partitionSize: continue
            sym = readDColumnAt(ff, dataColumnFd, v, columnTop, readBuf, ...)
            if toIndexKey(sym) != key: continue
            kept++
        keptCountsAddr[key] += kept
        totalKept += kept
```

`readDColumnAt` keeps a sliding window over `.d` (256 KB) so sequential rowIds within a key amortize file reads.

### Phase 2 algorithm

Clone of `reencodeWithStrideDecoding` (`PostingIndexWriter.java:3370`):

- Use `keptCountsAddr` for stride sizing.
- After per-stride decode into `strideValsAddr`, run the same validator over each key's slice and compress-in-place.
- Update `strideKeyCounts[j]` to the filtered count.
- Trial-encode delta and flat as today.
- Pick the smaller and emit via `writeDeltaStride` or `writePackedStride`.
- Sidecar emit per stride: same pattern as `sealIncremental`'s dirty-stride branch (`PostingIndexWriter.java:3943-3979`), but reading source cover bytes from snapshotted old sidecar buffers rather than `coveredColumnAddrs`.

### Sidecar source snapshot

Mirror the snapshot pattern at `PostingIndexWriter.java:1183-1215`: for each cover `c`, mmap the old `.pc{c}.{oldSealTxn}` RO, malloc a buffer, memcpy, munmap. Free in `finally`. This preserves cover bytes across the `closeSidecarMems()` call that precedes Phase 2.

For each kept `(key, indexWithinKey)` in dense-emit order, locate the source bytes in the old sidecar:
- Delta-mode source: `oldStrideStart + perKeyOffsets[j] + indexWithinKey * shift` (fixed-width) or via var-offset table (var-width).
- Flat-mode source: `oldStrideStart + flatHeaderSize + (prefixCount[j] + indexWithinKey) * shift`.

Sparse-gen sidecar source: append-order block at `dirOffset[gen] * 8` byte offset in the per-cover header; track per-`(gen, key)` cursors during Phase 1 so Phase 2 has O(1) lookup.

For flat-mode output, the per-stride bitwidth is recomputed from kept values; the cover bytes themselves are still copied from the old sidecar (not re-read from cover `.d`).

### Chain rotation

Mirror `truncate()` (`PostingIndexWriter.java:1334-1378`):
```java
final long oldSealTxn = sealTxn;
final long newSealTxn = Math.max(1, chain.peekNextSealTxn());
closeSidecarMems();
valueMem.close(false, (byte) 0);
LPSZ fileName = PostingIndexUtils.valueFileName(p, indexName, postingColumnNameTxn, newSealTxn);
valueMem.of(ff, fileName, ...);
sealTxn = newSealTxn;
valueMemSize = 0;
```

After Phase 2 completes:
```java
this.maxValue = partitionSize - 1;
this.genCount = 1;
publishToChain(1, 0, 0, valueMemSize, keyCount, 0, keyCount - 1);
if (sealTxn != oldSealTxn) recordPostingSealPurge(oldSealTxn);
```

`publishToChain`'s `newEntry` predicate (`sealTxn != chain.getHeadSealTxn()`) is true automatically because `sealTxn` rotated. No special-case flag needed.

### Call site changes in `TableWriter.sealPostingIndexForPartition`

Both branches replace `rollbackConditionally(partitionSize)` with the new primitive. Covering branch reorders so `configureCovering` runs BEFORE the diff (the diff needs cover schema to be configured for sidecar emit):

```
mapCoveringColumnsForSeal
configureFollowerAndWriter
mergeTentativeIntoActiveIfAny
configureCovering(...)              <-- BEFORE the diff
setCoveredColumnNameTxns
setNextTxnAtSeal(getTxn())
indexer.diffAgainstChainHead(ff, primaryColumnFd, columnTop, partitionSize)
publishPendingPurges
```

`rebuildSidecars()` and `seal()` go away from these callsites — the diff produces a sealed dense gen directly.

`rollbackConditionally` itself stays — still used by `index()`, `closeNoTruncate`, O3 squash.

## Design — simplified version (~600 LoC)

If full version is too much to land:

- Skip flat-mode sidecar handling (force delta-mode by always picking `useFlat = false` after the diff).
- Skip the `.d`-read cache (accept slower validation on cold partitions).
- Skip per-key delta-mode verbatim copy (rebuild sidecars from cover `.d` files like `sealFull` does today).

Net behavior: bounded memory (no spill accumulation), correct, modest perf vs PR #7077's full rebuild — wins are mostly OOM mitigation, not throughput.

## Tests

Writer-fixture in `PostingIndexCriticalIssuesTest`:
- `testDiffSparseAllMatch_NoOpResult` — every entry validates, output equals input.
- `testDiffSparseEvictsMismatch` — flip one `.d` value, that one entry evicted.
- `testDiffDenseGenMismatches` — pre-`seal()`, then mutate; dense walk works.
- `testDiffCrossFormat_DenseGen0_PlusSparse` — both formats handled.
- `testDiffCoverSidecarCorrectness` — fixed-width and var-width covers; kept entries' cover values match.
- `testDiffEvictsRowIdsBeyondPartitionSize` — split-shrink handling.
- `testDiffEvictsRowIdsBelowColumnTop_Defensive`.
- `testDiffOomGuard_ManyKeysManyValues` — peak `NATIVE_INDEX_READER` allocation stays bounded for 1M rowIds.
- `testDiffPureAppend_NoEviction` — every entry kept, output is byte-compacted input.
- `testDiffRotatesSealTxn_QueuesOldPvForPurge` — `pendingPurges` grew by 1, references `oldSealTxn`.

SQL fuzz in new `PostingIndexDiffFuzzTest`:
- 100 K-row partitioned table with POSTING-indexed SYMBOL + cover column.
- Random replace-range + dedup-replace mix.
- After each commit, assert per-symbol counts match a control table built without the index.
- Phantom matches manifest as count mismatches; OOM manifests as `OutOfMemoryError`.

## Build / validation

```
mvn clean package -DskipTests -P local-client
mvn -Dtest=PostingIndexCriticalIssuesTest test
mvn -Dtest=PostingIndexDiffFuzzTest test
mvn -Dtest=PostingIndexStressTest test       # existing concurrent-read regression
```

## Implementation order

1. Phase 1 validator + small `.d`-read cache. Allocate/free in `freeNativeBuffers()`.
2. Phase 2 stride emit — clone `reencodeWithStrideDecoding`, add validator, drop unused mode-specific tail.
3. Sidecar source snapshot + per-stride sidecar emit (delta-mode verbatim, flat-mode recompute).
4. Chain rotation + publish.
5. `ColumnIndexer` interface method + `SymbolColumnIndexer` forwarder.
6. Wire into `TableWriter.sealPostingIndexForPartition` (both branches).
7. Tests (a) - (j).
8. Fuzz test.
9. Validation sweep.

Each step is committable on its own. The diff primitive without sidecars is testable end-to-end on non-covering tables before sidecar work lands.

## Trade-off summary

|                          | PR #7077 rebuild | Option 2 diff | Option 1 chunked-flush |
|--------------------------|------------------|---------------|------------------------|
| Memory peak              | O(rows × 8B)     | O(stride × 8B + sidecar snapshots) | O(chunk × 8B) |
| OOM at 10B rows          | yes              | no            | no                     |
| Throughput (sparse mut.) | 1× (baseline)    | ~2.5×         | 1×                     |
| Throughput (full mut.)   | 1×               | ~1.7×         | 1×                     |
| LoC                      | already merged   | ~1400         | ~200                   |
| Sidecar verbatim copy    | n/a              | partial       | no                     |
