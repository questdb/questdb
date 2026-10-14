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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.fuzz.FuzzTransaction;
import org.junit.Assert;
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
                0.1,   // replaceProb -- mat-view refresh, live-view refresh, the ENT
                //   ACL compactor and direct WalWriter API users issue replace-range
                //   commits. In this first list a replace rarely reaches a Parquet
                //   partition; the follow-up list below is what lands replace on Parquet.
                0.0,
                0.01,
                0.1,   // setParquetEncodingProb
                0.6,   // addCoveringIndexProb
                0.0    // setTableFormatProb
        );
        setFuzzCounts(true, 300_000, 300, 20, 10, 1000, 50_000, 12);
        setFuzzProperties(1, getRndO3PartitionSplit(rnd), getRndO3PartitionSplitMaxCount(rnd));
        runFuzz(rnd, (tableNameNoWal, tableNameWal, tableNameWalParallel) ->
                prepareReplaceOnParquetList(rnd, tableNameNoWal, tableNameWal, tableNameWalParallel)
        );
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

    private static void convertAllButLastPartitionToParquet(String tableName) throws Exception {
        final StringSink partitionList = new StringSink();
        try (TableReader reader = engine.getReader(tableName)) {
            // The last partition is the active one and cannot be converted.
            for (int i = 0, n = reader.getPartitionCount() - 1; i < n; i++) {
                if (reader.getPartitionFormatFromMetadata(i) == PartitionFormat.PARQUET) {
                    continue;
                }
                if (!partitionList.isEmpty()) {
                    partitionList.put(',');
                }
                partitionList.put('\'');
                PartitionBy.setSinkForPartition(
                        partitionList,
                        reader.getMetadata().getTimestampType(),
                        reader.getPartitionedBy(),
                        reader.getPartitionTimestampByIndex(i)
                );
                partitionList.put('\'');
            }
        }
        if (!partitionList.isEmpty()) {
            execute("ALTER TABLE \"" + tableName + "\" CONVERT PARTITION TO PARQUET LIST " + partitionList);
        }
    }

    private static int countParquetPartitions(String tableName) {
        int count = 0;
        try (TableReader reader = engine.getReader(tableName)) {
            for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                if (reader.getPartitionFormatFromMetadata(i) == PartitionFormat.PARQUET) {
                    count++;
                }
            }
        }
        return count;
    }

    /**
     * Gives the table a covering POSTING index, unless a column already has one. The first list
     * adds covering indexes at random (addCoveringIndexProb), so a run can end without any.
     * Returns the key column's name.
     */
    private static String ensureCoveringPostingIndex(Rnd rnd, ObjList<String> tableNames, String tableNameWal) throws Exception {
        int keyIndex = -1;
        final StringSink includeList = new StringSink();
        String keyName;
        try (TableReader reader = engine.getReader(tableNameWal)) {
            final TableReaderMetadata metadata = reader.getMetadata();
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                if (ColumnType.isSymbol(metadata.getColumnType(i))
                        && IndexType.isPosting(metadata.getColumnIndexType(i))
                        && metadata.getCoveringColumnIndices(i) != null
                        && metadata.getCoveringColumnIndices(i).size() > 0) {
                    return metadata.getColumnName(i);
                }
                if (keyIndex < 0 && ColumnType.isSymbol(metadata.getColumnType(i))) {
                    keyIndex = i;
                }
            }
            Assert.assertTrue("no SYMBOL column left to index", keyIndex > -1);
            keyName = metadata.getColumnName(keyIndex);
            final IntList candidates = new IntList();
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                if (i != keyIndex && i != metadata.getTimestampIndex()
                        && ColumnType.tagOf(metadata.getColumnType(i)) != ColumnType.LONG128) {
                    candidates.add(i);
                }
            }
            Assert.assertTrue("no column left to cover", candidates.size() > 0);
            for (int k = 0, includeCount = Math.min(1 + rnd.nextInt(3), candidates.size()); k < includeCount; k++) {
                final int pick = rnd.nextInt(candidates.size());
                if (k > 0) {
                    includeList.put(", ");
                }
                includeList.put('"').put(metadata.getColumnName(candidates.getQuick(pick))).put('"');
                candidates.removeIndex(pick);
            }
        }
        final double kindPick = rnd.nextDouble();
        final String indexType = kindPick < 0.6 ? "POSTING" : (kindPick < 0.8 ? "POSTING DELTA" : "POSTING EF");
        for (int t = 0, tn = tableNames.size(); t < tn; t++) {
            final String tableName = tableNames.getQuick(t);
            try (TableReader reader = engine.getReader(tableName)) {
                final TableReaderMetadata metadata = reader.getMetadata();
                if (metadata.isColumnIndexed(metadata.getColumnIndex(keyName))) {
                    execute("ALTER TABLE \"" + tableName + "\" ALTER COLUMN \"" + keyName + "\" DROP INDEX");
                }
            }
            drainWalQueue();
            execute("ALTER TABLE \"" + tableName + "\" ALTER COLUMN \"" + keyName + "\" ADD INDEX TYPE " + indexType + " INCLUDE (" + includeList + ")");
        }
        drainWalQueue();
        LOG.info().$("follow-up list added covering index [column=").$safe(keyName)
                .$(", type=").$(indexType)
                .$(", include=").$(includeList)
                .I$();
        return keyName;
    }

    private static void insertSameRows(ObjList<String> tableNames, String tsColumnName) throws Exception {
        for (int t = 0, tn = tableNames.size(); t < tn; t++) {
            final String tableName = tableNames.getQuick(t);
            execute("INSERT INTO \"" + tableName + "\"(\"" + tsColumnName + "\") VALUES "
                    + "('2022-03-10T00:00:00.000000Z'), ('2022-03-11T00:00:00.000000Z')");
        }
        drainWalQueue();
    }

    // A tiny posting indexer spill budget forces compactIfOverBudget ->
    // flushAllPending mid-build, so a full index() rebuild over an O3-merged or
    // squashed partition trips the spill budget and commitDense must consolidate
    // sparse gens -- the exact path the squash/covering SIGSEGV came from. The
    // budget is engine-global, so the non-WAL oracle table spills identically and
    // the result-set comparison stays apples-to-apples.
    private void forcePostingSpill(Rnd rnd) {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_INDEXER_SPILL_BYTES_MAX, 256L + rnd.nextInt(64 * 1024));
    }

    /**
     * The follow-up list is where replace reaches Parquet partitions with posting indexes. The
     * first list does not get there. It is capped at 5 transactions
     * (1_500_000 / fuzzRowCount), only about a third of its iterations write data, the SET TTL
     * it usually generates stops replace for every later data block, and a replace lands on
     * Parquet only when a conversion of that partition precedes it.
     * <p>
     * So once the first list is verified, the three tables get the same preparation: TTL off,
     * a covering POSTING index if the first list left none, every partition but the active one
     * converted to Parquet on the WAL tables. Then a replace-heavy list is generated over the
     * day of one of those Parquet partitions. The spill budget from {@link #forcePostingSpill}
     * still applies, so each replace rewrites Parquet row groups and re-seals the posting index
     * (O3PartitionJob.updateParquetIndexes, deferParquetPostingSealPurges) under spill pressure.
     */
    private ObjList<FuzzTransaction> prepareReplaceOnParquetList(
            Rnd rnd,
            String tableNameNoWal,
            String tableNameWal,
            String tableNameWalParallel
    ) throws Exception {
        final ObjList<String> tableNames = new ObjList<>();
        tableNames.add(tableNameNoWal);
        tableNames.add(tableNameWal);
        tableNames.add(tableNameWalParallel);
        // Replace with TTL can drop partitions on the WAL tables that the oracle keeps: see the
        // setTtlIteration guard in FuzzTransactionGenerator.
        for (int t = 0, tn = tableNames.size(); t < tn; t++) {
            final String tableName = tableNames.getQuick(t);
            execute("ALTER TABLE \"" + tableName + "\" SET TTL 0 DAYS");
        }
        drainWalQueue();

        final String keyName = ensureCoveringPostingIndex(rnd, tableNames, tableNameWal);

        String tsColumnName;
        int partitionCount;
        try (TableReader reader = engine.getReader(tableNameWal)) {
            tsColumnName = reader.getMetadata().getColumnName(reader.getMetadata().getTimestampIndex());
            partitionCount = reader.getPartitionCount();
        }
        if (partitionCount < 2) {
            // A truncate or TTL near the end of the first list can leave at most the active
            // partition, which cannot be converted.
            insertSameRows(tableNames, tsColumnName);
        }

        convertAllButLastPartitionToParquet(tableNameWal);
        convertAllButLastPartitionToParquet(tableNameWalParallel);
        drainWalQueue();
        for (int i = 1, n = tableNames.size(); i < n; i++) {
            final TableToken tableToken = engine.verifyTableName(tableNames.getQuick(i));
            Assert.assertFalse("table suspended", engine.getTableSequencerAPI().isSuspended(tableToken));
            Assert.assertTrue("no parquet partition in " + tableNames.getQuick(i), countParquetPartitions(tableNames.getQuick(i)) > 0);
        }

        // The list's rows go into one Parquet partition's day. The table's partitions need not
        // be contiguous (TTL, drops, truncate), so a list spread over [min, max] can miss them
        // all. A replace range reaches about 7.4 h past its rows, so neighbours are hit too.
        final long startTimestamp;
        final long endTimestamp;
        try (TableReader reader = engine.getReader(tableNameWal)) {
            final IntList parquetPartitions = new IntList();
            for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                if (reader.getPartitionFormatFromMetadata(i) == PartitionFormat.PARQUET) {
                    parquetPartitions.add(i);
                }
            }
            final int partitionIndex = parquetPartitions.getQuick(rnd.nextInt(parquetPartitions.size()));
            startTimestamp = reader.getPartitionTimestampByIndex(partitionIndex);
            endTimestamp = ColumnType.getTimestampDriver(reader.getMetadata().getTimestampType()).addDays(startTimestamp, 1);
            LOG.info().$("follow-up list targets parquet [table=").$(tableNameWal)
                    .$(", partitions=").$(reader.getPartitionCount())
                    .$(", parquetPartitions=").$(parquetPartitions.size())
                    .$(", targetPartitionIndex=").$(partitionIndex)
                    .$(", targetPartitionRows=").$(reader.getPartitionRowCountFromMetadata(partitionIndex))
                    .$(", coveringKey=").$safe(keyName)
                    .I$();
        }

        setFuzzProbabilities(
                0.01,  // cancelRowsProb
                0.01,  // notSetProb
                0.1,   // nullSetProb
                0.05,  // rollbackProb
                0.02,  // colAddProb
                0.02,  // colRemoveProb
                0.02,  // colRenameProb
                0.02,  // colTypeChangeProb
                1.0,   // dataAddProb
                0.01,  // equalTsRowsProb
                0.0,   // partitionDropProb
                0.0,   // partitionToParquetProb -- already converted above
                0.0,   // partitionToNativeProb -- keep the targets Parquet
                0.0,   // truncateProb
                0.0,   // tableDropProb
                0.0,   // setTtlProb
                0.5,   // replaceProb
                0.0,   // symbolAccessProb
                0.01,  // queryProb
                0.0,   // setParquetEncodingProb
                0.2,   // addCoveringIndexProb
                0.0    // setTableFormatProb
        );
        // 20 transactions stay under the 1_500_000 / fuzzRowCount cap.
        setFuzzCounts(true, 20_000, 20, 20, 10, 1000, 50_000, 12);
        return fuzzer.generateTransactions(tableNameWal, rnd, startTimestamp, endTimestamp);
    }
}
