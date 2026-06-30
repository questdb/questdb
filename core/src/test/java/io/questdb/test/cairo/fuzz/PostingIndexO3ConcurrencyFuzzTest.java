/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.cairo.fuzz;

import io.questdb.PropertyKey;
import io.questdb.std.Rnd;
import org.junit.Test;

/**
 * Fuzz suite targeting thread-unsafe interactions between the POSTING/covering
 * index and the O3 jobs (O3CopyJob, O3PartitionJob, O3OpenColumnJob) and
 * TableWriter, run over the shared 4-worker O3 pool that {@link AbstractFuzzTest}
 * starts (which also runs PostingSealPurgeJob, ColumnIndexerJob and the partition
 * purge job). Each test:
 * <ul>
 *   <li>forces OUT-OF-ORDER inserts (setFuzzCounts isO3=true) so O3CopyJob /
 *       O3PartitionJob actually run on the worker threads, not inline;</li>
 *   <li>drives runtime covering POSTING indexes (addCoveringIndexProb &gt; 0 emits
 *       ALTER ... ADD INDEX TYPE POSTING INCLUDE (...)) on top of the random
 *       BITMAP/POSTING/DELTA/EF index types the framework already assigns to
 *       sym2/sym_top;</li>
 *   <li>cranks a tiny posting indexer spill budget so the mid-stream-flush /
 *       commitDense consolidation and seal/reseal path (the squash SIGSEGV class)
 *       is hit under O3 worker contention;</li>
 *   <li>relies on -ea assertions added during the concurrency audit as oracles:
 *       parquetSealPurgeLock Thread.holdsLock guards, the
 *       o3PartitionUpdRemaining==0 temporal-separation guards on the seal/purge
 *       paths, and the addr-based covered-read bounds check. A regression that
 *       reintroduces a race trips one of these on a worker thread, which the
 *       fuzz harness propagates as a test failure;</li>
 *   <li>asserts the WAL and parallel-WAL tables match the single-threaded non-WAL
 *       reference row-for-row AND index-for-index (assertRandomIndexes), so a
 *       posting/covering index returning wrong rows fails immediately.</li>
 * </ul>
 * Reproduce a failure by replacing {@code generateRandom(LOG)} with
 * {@code generateRandom(LOG, s0, s1)} using the {@code random seeds: ...} log line.
 */
public class PostingIndexO3ConcurrencyFuzzTest extends AbstractFuzzTest {

    // A tiny posting indexer spill budget forces compactIfOverBudget ->
    // flushAllPending mid-build, so a full index() rebuild over an O3-merged or
    // squashed partition trips the spill budget and commitDense must consolidate
    // sparse gens -- the exact path the squash/covering SIGSEGV came from. The
    // budget is engine-global, so the non-WAL oracle table spills identically and
    // the result-set comparison stays apples-to-apples.
    private void forcePostingSpill(Rnd rnd) {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_INDEXER_SPILL_BYTES_MAX, 256L + rnd.nextInt(64 * 1024));
    }

    @Test
    public void testCoveringPostingO3NativeSpillFuzz() throws Exception {
        Rnd rnd = generateRandom(LOG);
        forcePostingSpill(rnd);
        // Native-only (no parquet): exercises O3CopyJob.updateIndex on per-partition
        // O3Basket indexers + the writer-thread covering reseal sweep
        // (sealPostingIndexForPartition: discardForRebuild -> index -> commitDense ->
        // configureCovering -> rebuildSidecars) under spill pressure, across the pool.
        setFuzzProbabilities(
                0.05,  // cancelRowsProb -- rollback within a commit
                0.05,  // notSetProb -- column tops
                0.1,   // nullSetProb -- index implicit-null synthesis
                0.1,   // rollbackProb
                0.1,   // colAddProb -- adds SYMBOL cols, ~90% indexed (random posting variant)
                0.05,  // colRemoveProb
                0.1,   // colRenameProb -- posting aux-file relink
                0.0,   // colTypeChangeProb
                1.0,   // dataAddProb
                0.05,  // equalTsRowsProb -- equal-ts O3 merge edge
                0.05,  // partitionDropProb
                0.0,   // partitionToParquetProb
                0.0,   // partitionToNativeProb
                0.1,   // truncateProb
                0.0,   // tableDropProb -- keep oracle table stable
                0.8,   // setTtlProb
                0.15,  // replaceProb -- REPLACE RANGE, heavy index rewrite
                0.0,   // symbolAccessProb
                0.05,  // queryProb
                0.0,   // setParquetEncodingProb
                0.6,   // addCoveringIndexProb
                0.0    // setTableFormatProb
        );
        setFuzzCounts(true, 600_000, 400, 20, 10, 1000, 80_000, 20);
        // Tiny apply quota + split-min-size=1 + low max-splits force partition
        // splits then squashSplitPartitions, the original reseal trigger.
        setFuzzProperties(1, 1, getRndO3PartitionSplitMaxCount(rnd));
        runFuzz(rnd);
    }

    @Test
    public void testCoveringPostingParquetO3SpillFuzz() throws Exception {
        Rnd rnd = generateRandom(LOG);
        forcePostingSpill(rnd);
        // Parquet rewrite: O3PartitionJob.updateParquetIndexes runs on the workers
        // and each calls back into TableWriter.deferParquetPostingSealPurges under
        // parquetSealPurgeLock; several partitions in flight contend on the shared
        // deferredPostingSealPurges list + task pool. The PostingSealPurgeJob on the
        // pool then reclaims the superseded .pv/.pc, scoreboard-gated.
        setFuzzProbabilities(
                0.01,
                0.01,
                0.1,
                0.1,
                0.05,
                0.05,
                0.1,
                0.1,
                1.0,
                0.01,
                0.01,
                0.5,   // partitionToParquetProb
                0.5,   // partitionToNativeProb
                0.1,
                0.0,
                0.8,
                0.0,   // replaceProb -- DISABLED: replace-range commits are a mat-view-only
                //   operation in production (WalWriter.commitMatView via
                //   MatViewRefreshJob); they are never issued against a regular WAL
                //   table. With partitionToParquetProb>0 above, enabling replace here
                //   makes the fuzz apply a replace commit onto a Parquet partition --
                //   an unsupported, production-unreachable state that suspends the
                //   table ("commit replace mode is not supported for Parquet
                //   partitions"). Replace and Parquet must stay mutually exclusive;
                //   native-partition replace coverage lives in
                //   testCoveringPostingO3NativeSpillFuzz and testCoveringPostingSquashSpillFuzz.
                0.0,
                0.01,
                0.1,   // setParquetEncodingProb
                0.6,   // addCoveringIndexProb
                0.0    // setTableFormatProb
        );
        setFuzzCounts(true, 300_000, 300, 20, 10, 1000, 50_000, 12);
        setFuzzProperties(1, getRndO3PartitionSplit(rnd), getRndO3PartitionSplitMaxCount(rnd));
        runFuzz(rnd);
    }

    @Test
    public void testCoveringPostingSquashSpillFuzz() throws Exception {
        Rnd rnd = generateRandom(LOG);
        forcePostingSpill(rnd);
        // Maximal split/squash churn: split-min-size=1, max-splits=1 means every O3
        // insert into a partition splits it and the next commit squashes, repeatedly
        // reseal-ing the merged partition's covering index under spill pressure.
        setFuzzProbabilities(
                0.1,   // cancelRowsProb
                0.05,
                0.1,
                0.15,  // rollbackProb
                0.1,   // colAddProb
                0.05,
                0.05,
                0.05,
                1.0,
                0.1,   // equalTsRowsProb
                0.02,
                0.0,
                0.0,
                0.05,
                0.0,
                0.8,
                0.2,   // replaceProb -- heavy
                0.0,
                0.05,
                0.0,
                0.7,   // addCoveringIndexProb
                0.0    // setTableFormatProb
        );
        setFuzzCounts(true, 400_000, 500, 16, 8, 800, 60_000, 24);
        setFuzzProperties(1, 1, 1);
        runFuzz(rnd);
    }
}
