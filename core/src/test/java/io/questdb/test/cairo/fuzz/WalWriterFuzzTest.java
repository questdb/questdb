/*+*****************************************************************************
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
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.ParanoiaState.FD_PARANOIA_MODE;
import static io.questdb.test.cairo.fuzz.FuzzRunner.MAX_WAL_APPLY_TIME_PER_TABLE_CEIL;

/**
 * These tests are designed to produce unstable runs, e.g., random generator is created
 * using current execution time.
 * This improves coverage. To debug failures in CI find the line logging random seeds
 * and change line
 * {@code Rnd rnd = generateRandom(LOG);}
 * to
 * {@code Rnd rnd = new Rnd(A, B);}
 * where A, B are seeds in the failed run log.
 * <p>
 * When the same timestamp is used in multiple transactions,
 * the order of records when executed in parallel WAL writing is not guaranteed.
 * That creates failures in tests that assume that the order of records is preserved.
 * There are already measures to prevent invalid data generation, but it still can happen.
 * In order to verify that the test is not broken, we check that there are no duplicate
 * timestamps for the record where the comparison fails.
 */
public class WalWriterFuzzTest extends AbstractFuzzTest {
    private static boolean ASYNC_MUNMAP = false;
    private boolean existingFilesParanoia;
    private boolean fsAllowsMixedIO;
    private String fuzzTableNameOverride;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        setProperty(PropertyKey.CAIRO_FILE_ASYNC_MUNMAP_ENABLED, String.valueOf(ASYNC_MUNMAP));
        AbstractCairoTest.setUpStatic();
    }

    @AfterClass
    public static void tearDownStatic() {
        AbstractFuzzTest.tearDownStatic();
        Files.ASYNC_MUNMAP_ENABLED = false;
    }

    @Before
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.DEBUG_CAIRO_O3_COLUMN_MEMORY_SIZE, 512 * 1024);
        // Disable mixed I/O on some OSes and FSes (wink-wink Windows and ZFS).
        fsAllowsMixedIO = FilesFacadeImpl.INSTANCE.allowMixedIO(root);
        node1.setProperty(PropertyKey.DEBUG_CAIRO_ALLOW_MIXED_IO, fsAllowsMixedIO);
        setFuzzProperties(100, 1000, 2);
        existingFilesParanoia = FD_PARANOIA_MODE;
        FD_PARANOIA_MODE = true;
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        FD_PARANOIA_MODE = existingFilesParanoia;
    }

    @Test
    public void testAddDropColumnDropPartition() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setTestParams(rnd);

        setFuzzProbabilities(
                0.01,
                0.0,
                0.01,
                0.1,
                0.05,
                0.05,
                0.1,
                0.1,
                1.0,
                0.01,
                0.01,
                0.0,
                0.0,
                0.1,
                0.0,
                0.8,
                0.01,
                0,
                0.01
        );
        setFuzzCounts(rnd.nextBoolean(), 10_000, 300, 20, 10, 1000, 100, 3);
        runFuzz(rnd);
    }

    @Test
    public void testChunkedSequencerWalTransactionQueries() throws Exception {
        assertMemoryLeak(() -> {
            int chunkSize = TestUtils.generateRandom(LOG).nextInt(100) + 1;
            node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, chunkSize);
            chunkSize = node1.getConfiguration().getDefaultSeqPartTxnCount();

            execute("""
                    create table chunk_seq (
                      x long,
                      ts timestamp
                    ) timestamp(ts) PARTITION by day WAL""");
            execute("insert batch 2 into chunk_seq \n" +
                    "  select x, timestamp_sequence('2024-01-01', 312312) from long_sequence(1000)");

            drainPurgeJob();

            int expectedTxnCount = 500;
            assertQuery("select count(*) from wal_transactions('chunk_seq')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n" +
                            expectedTxnCount + "\n");

            drainWalQueue();

            assertQuery("select count(*) from wal_transactions('chunk_seq')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n" +
                            expectedTxnCount + "\n");

            drainPurgeJob();

            assertQuery("select count(*) from wal_transactions('chunk_seq')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n" +
                            (expectedTxnCount - (expectedTxnCount - 1) / chunkSize * chunkSize) + "\n");
        });
    }

    @Test
    public void testChunkedSequencerWriting() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setTestParams(rnd);

        fuzzer.setFuzzCounts(false, 5_000, 200, 20, 10, 20, rnd.nextInt(10), 5, 2, 0);
        setFuzzProperties(rnd);
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);
        Assert.assertEquals(10, node1.getConfiguration().getDefaultSeqPartTxnCount());
        runFuzz(rnd);
    }

    @Test
    public void testConvertPartitionToParquet() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setTestParams(rnd);

        setFuzzProbabilities(
                0.01,
                0.01,
                0.01,
                0.1,
                0.05,
                0.05,
                0.1,
                0.1,
                1.0,
                0.01,
                0.01,
                0.5,
                0.5,
                0.1,
                0.0,
                0.8,
                0.00,
                0,
                0.1,
                0.1,
                0.01, // addCoveringIndexProb
                0.1 // SET FORMAT PARQUET|NATIVE probability
        );
        setFuzzCounts(rnd.nextBoolean(), 10_000, 300, 20, 10, 1000, 100, 3);
        runFuzz(rnd);
    }

    @Test
    public void testCreateTableAsParquet() throws Exception {
        // Creates WAL table in parquet format and keeps then run all parquet supported operations.
        Rnd rnd = generateRandom(LOG);
        setTestParams(rnd);
        setCreateWalAsParquet(true);

        setFuzzProbabilities(
                0.01,
                0.01,
                0.01,
                0.1,
                0.05,
                0.05,
                0.1,
                0.1,
                1.0,
                0.01,
                0.01,
                0.0, // partitionToParquetProb — disabled
                0.0, // partitionToNativeProb — disabled
                0.1,
                0.0,
                0.8,
                0.00,
                0,
                0.01,
                0.1,
                0.01, // addCoveringIndexProb — disabled
                0.0 // setTableFormatProb — disabled
        );
        setFuzzCounts(rnd.nextBoolean(), 10_000, 300, 20, 10, 1000, 100, 3);
        runFuzz(rnd);
    }

    @Test
    public void testConvertPartitionToParquetWithCoveringIndex() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setTestParams(rnd);

        setFuzzProbabilities(
                0.01,
                0.01,
                0.01,
                0.1,
                0.05,
                0.05,
                0.1,
                0.1,
                1.0,
                0.01,
                0.01,
                0.5,
                0.5,
                0.1,
                0.0,
                0.8,
                0.00,
                0,
                0.01,
                0.1,
                0.1, // addCoveringIndexProb
                0.0

        );
        setFuzzCounts(rnd.nextBoolean(), 10_000, 300, 20, 10, 1000, 100, 3);
        runFuzz(rnd);
    }

    @Test
    public void testInOrderSmallTxns() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setTestParams(rnd);

        fuzzer.setFuzzCounts(false, 20000, 20000, 20, 10, 20, rnd.nextInt(10), 5, 2, 0);
        setFuzzProperties(rnd);
        node1.setProperty(PropertyKey.CAIRO_WAL_MAX_LAG_TXN_COUNT, -1);
        runFuzz(rnd);
    }

    @Test
    public void testPostingIndexCloseAfterPartitionSplitSquash() throws Exception {
        final Rnd rnd = new Rnd(6424878685474624049L, 7598331127391241803L);
        setTestParams(rnd);

        final PostingIndexStaleCloseFilesFacade staleCloseFf = new PostingIndexStaleCloseFilesFacade();
        final FilesFacade previousFf = ff;
        final String previousFuzzTableNameOverride = fuzzTableNameOverride;
        ff = staleCloseFf;
        fuzzTableNameOverride = "testConvertPartitionToParquet";
        fuzzer.withDb(engine, sqlExecutionContext);
        try {
            runPostingIndexCloseAfterPartitionSplitSquashFuzz(rnd);
            Assert.assertTrue(
                    "fixed seed did not exercise an empty-to-nonempty posting-index mapping close",
                    staleCloseFf.getStalePostingCloseCount() > 0
            );
            Assert.assertEquals(
                    "posting-index close truncated below a concurrently published chain limit",
                    0,
                    staleCloseFf.getTruncateBelowPublishedLimitCount()
            );
        } finally {
            fuzzTableNameOverride = previousFuzzTableNameOverride;
            ff = previousFf;
            fuzzer.withDb(engine, sqlExecutionContext);
        }
    }

    @Test
    public void testSimpleDataTransaction() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setTestParams(rnd);

        setFuzzProbabilities(
                0.01,
                0.2,
                0.1,
                0.01,
                0.02,
                0.02,
                0.08,
                0,
                1.0,
                0.01,
                0.1,
                0.0,
                0.0,
                0.01,
                0.01,
                0.8,
                0.05,
                0,
                0.01
        );
        setFuzzCounts(rnd.nextBoolean(), rnd.nextInt(1_000_000),
                rnd.nextInt(500), 20, 10, 200, 0, 1
        );
        runFuzz(rnd);
    }

    @Test
    public void testWalAddRemoveCommitDropFuzz() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setTestParams(rnd);

        setFuzzProbabilities(
                0.05,
                0.2,
                0.1,
                0.005,
                0.05,
                0.05,
                0.05,
                0.01,
                1.0,
                0.01,
                0.01,
                0.0,
                0.0,
                0.05,
                1.0,
                0.8,
                0.05,
                0,
                0.01
        );
        setFuzzCounts(true, 100_000, 500, 20, 1000, 20, 100_000, 5);
        setFuzzProperties(rnd);
        runFuzz(rnd);
    }

    @Test
    public void testWalAddRemoveCommitFuzzInOrder() throws Exception {
        setFuzzProbabilities(
                0.05,
                0.2,
                0.1,
                0.005,
                0.05,
                0.05,
                0.05,
                0.01,
                1.0,
                0.01,
                0.01,
                0.0,
                0.0,
                0.01,
                0.0,
                0.8,
                0.05,
                0,
                0.01
        );
        setFuzzCounts(false, 1_000_000, 300, 20, 1000, 20, 0, 10);
        runFuzz(generateRandom(LOG));
    }

    @Test
    public void testWalAddRemoveCommitFuzzO3() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setTestParams(rnd);

        setFuzzProbabilities(
                0.05,
                0.2,
                0.1,
                0.005,
                0.05,
                0.05,
                0.05,
                0.01,
                1.0,
                0.01,
                0.01,
                0.0,
                0.0,
                0.05,
                0.005,
                0.8,
                0.05,
                0,
                0.01
        );
        setFuzzCounts(true, 100_000, 500, 20, 1000, 20, 100_000, 5);
        setFuzzProperties(rnd);
        runFuzz(rnd);
    }

    @Test
    public void testWalApplyEjectsMultipleTables() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setTestParams(rnd);

        setFuzzProperties(rnd.nextLong(50), getRndO3PartitionSplit(rnd), getRndO3PartitionSplitMaxCount(rnd), getMaxWalSize(rnd), getMaxWalFdCache(rnd));
        int tableCount = Math.max(2, rnd.nextInt(3));
        fullRandomFuzz(rnd, tableCount);
    }

    @Test
    public void testWalMetadataAddDeleteColumnHeavy() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setTestParams(rnd);

        setFuzzProbabilities(
                0.05,
                0.2,
                0.1,
                0.005,
                0.25,
                1,
                0.02,
                0.004,
                1.0,
                0.01,
                0.01,
                0.0,
                0.0,
                0.01,
                0.005,
                0.1,
                0.1,
                0,
                0.01
        );
        setFuzzCounts(rnd.nextBoolean(), 50_000, 100, 20, 1000, 1000, 100, 5);
        setFuzzProperties(rnd);
        runFuzz(rnd);
    }

    @Test
    public void testWalMetadataChangeHeavy() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setTestParams(rnd);

        setFuzzProbabilities(
                0.05,
                0.2,
                0.1,
                0.005,
                0.25,
                0.25,
                0.25,
                0.25,
                1.0,
                0.01,
                0.01,
                0.0,
                0.0,
                0.01,
                0.0,
                0.8,
                0.05,
                0,
                0.01
        );
        setFuzzCounts(false, 50_000, 100, 20, 1000, 1000, 100, 5);
        setFuzzProperties(rnd);
        runFuzz(rnd);
    }

    @Test
    public void testWalMetadataChangeHeavyManyPartitions() throws Exception {
        // Too many partitions cause OSX to fail with file limit error
        Assume.assumeTrue(!Os.isOSX());
        Rnd rnd = generateRandom(LOG);
        setTestParams(rnd);

        setFuzzProbabilities(
                0.05,
                0.2,
                0.1,
                0.005,
                0.25,
                0.25,
                0.25,
                0.25,
                1.0,
                0.01,
                0.01,
                0.0,
                0.0,
                0.01,
                0.02,
                0.8,
                0.05,
                0,
                0.01
        );
        setFuzzCounts(rnd.nextBoolean(), rnd.nextInt(50_000) + 1000, rnd.nextInt(100), 20, 1000, 1000, rnd.nextInt(100), rnd.nextInt(400) + 1);
        setFuzzProperties(rnd);
        runFuzz(rnd);
    }

    @Test
    public void testWalSmallWalFdReuse() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setTestParams(rnd);

        fuzzer.setFuzzCounts(false, 100_000, 50, 20, 10, 20, 0, 5, 2, 0);
        setFuzzProperties(rnd.nextLong(MAX_WAL_APPLY_TIME_PER_TABLE_CEIL), getRndO3PartitionSplit(rnd), getRndO3PartitionSplitMaxCount(rnd), getMaxWalSize(rnd), 1);
        runFuzz(rnd);
    }

    @Test
    public void testWalSmallWalLag() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setTestParams(rnd);

        setFuzzProperties(rnd);
        fullRandomFuzz(rnd);
    }

    @Test
    public void testWalWriteEqualTimestamp() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_QUICKSORT_ENABLED, true);
        Rnd rnd = generateRandom(LOG);
        setTestParams(rnd);

        setFuzzProbabilities(
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                1,
                0.5,
                0.01,
                0.0,
                0.0,
                0,
                0.0,
                0.8,
                0.05,
                0,
                0.01
        );
        setFuzzCounts(
                true,
                5000,
                800,
                10,
                10,
                10,
                50,
                1
        );
        setFuzzProperties(rnd);
        runFuzz(rnd, getTestName(), 1);
    }

    @Test
    public void testWalWriteFullRandom() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setTestParams(rnd);

        setRandomAppendPageSize(rnd);
        setFuzzProperties(rnd);
        fullRandomFuzz(rnd);
    }

    @Test
    public void testWalWriteFullRandomMultipleTables() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setTestParams(rnd);

        int tableCount = Math.max(2, rnd.nextInt(4));
        setFuzzProperties(rnd);
        fullRandomFuzz(rnd, tableCount);
    }

    @Test
    public void testWalWriteManySmallTransactions() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_QUICKSORT_ENABLED, true);
        Rnd rnd = generateRandom(LOG);
        setTestParams(rnd);

        setFuzzProbabilities(
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                1,
                0.01,
                0.01,
                0.0,
                0.0,
                0,
                0.0,
                0.8,
                0.05,
                0,
                0.01
        );
        setFuzzCounts(
                true,
                1000,
                800,
                10,
                10,
                10,
                50,
                1
        );
        setFuzzProperties(rnd);
        runFuzz(rnd, getTestName(), 1);
    }

    @Test
    public void testWalWriteManyTablesInOrder() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_QUICKSORT_ENABLED, true);
        Rnd rnd = generateRandom(LOG);
        setTestParams(rnd);

        setRandomAppendPageSize(rnd);
        int tableCount = 3;
        setFuzzProbabilities(
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                1,
                0.01,
                0.01,
                0.0,
                0.0,
                0.001,
                0.0,
                0.8,
                0.05,
                0,
                0.01
        );
        setFuzzCounts(false, 500_000, 5_000, 10, 10, 5500, 0, 1);
        String tableNameBase = getTestName();
        runFuzz(rnd, tableNameBase, tableCount);
    }

    @Test
    public void testWalWriteRollbackHeavy() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setTestParams(rnd);
        setFuzzProbabilities(
                0.5,
                0.5,
                0.1,
                0.5,
                0.05,
                0.05,
                0.05,
                0.01,
                1.0,
                0.01,
                0.01,
                0.0,
                0.0,
                0.01,
                0.0,
                0.8,
                0.1,
                0,
                0.01
        );
        setFuzzCounts(rnd.nextBoolean(), 10_000, 300, 20, 1000, 1000, 100, 3);
        runFuzz(rnd);
    }

    @Test
    public void testWalWriteRollbackTruncateHeavy() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setTestParams(rnd);

        setFuzzProbabilities(
                0.5,
                0.5,
                0.1,
                0.5,
                0.05,
                0.05,
                0.05,
                0.01,
                1.0,
                0.01,
                0.01,
                0.0,
                0.0,
                0.15,
                0.0,
                0.8,
                0.05,
                0,
                0.01
        );
        setFuzzCounts(rnd.nextBoolean(), 300, 20, 20, 1000, 1000, 100, 3);
        runFuzz(rnd);
    }

    @Test
    public void testWalWriteTinyO3Memory() throws Exception {
        final int o3MemorySize = 256;
        node1.setProperty(PropertyKey.DEBUG_CAIRO_O3_COLUMN_MEMORY_SIZE, o3MemorySize);
        Assert.assertEquals(o3MemorySize, node1.getConfiguration().getO3ColumnMemorySize());
        Rnd rnd = generateRandom(LOG);
        setFuzzProbabilities(
                0,
                0.2,
                0.1,
                0,
                0,
                0,
                0,
                0,
                1.0,
                0.01,
                0.01,
                0.0,
                0.0,
                0.01,
                0.0,
                0.8,
                0.05,
                0.01,
                0.01
        );
        setFuzzCounts(true, 100_000, 10, 10, 10, 10, 50, 1);
        runFuzz(rnd, getTestName(), 1);
    }

    @Test
    public void testWriteO3DataOnlyBig() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setTestParams(rnd);

        setFuzzProbabilities(
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                1.0,
                0.01,
                0.01,
                0.0,
                0.0,
                0.01,
                0.0,
                0.8,
                0.05,
                0,
                0.01
        );
        setFuzzCounts(true, 1_000_000, 500, 20, 1000, 1000, 100, 20);
        setFuzzProperties(rnd);
        runFuzz(rnd);
    }

    private void runPostingIndexCloseAfterPartitionSplitSquashFuzz(Rnd rnd) throws Exception {
        setFuzzProbabilities(
                0.01,
                0.01,
                0.01,
                0.1,
                0.05,
                0.05,
                0.1,
                0.1,
                1.0,
                0.01,
                0.01,
                0.5,
                0.5,
                0.1,
                0.0,
                0.8,
                0.00,
                0,
                0.1,
                0.1,
                0.01, // addCoveringIndexProb
                0.1 // SET FORMAT PARQUET|NATIVE probability
        );
        setFuzzCounts(rnd.nextBoolean(), 10_000, 300, 20, 10, 1000, 100, 3);
        runFuzz(rnd);
    }

    private void setTestParams(Rnd rnd) throws Exception {
        int newScoreboardVersion = rnd.nextBoolean() ? 1 : 2;
        boolean newAsyncMunmapEnabled = Os.isPosix() && rnd.nextBoolean(); // windows does not support async munmap
        LOG.info().$("switching to [scoreboard-format=").$(newScoreboardVersion)
                .$(", asyncMunmapEnabled = ").$(newAsyncMunmapEnabled)
                .I$();
        if (ASYNC_MUNMAP != newAsyncMunmapEnabled) {
            // This restarts test initialization
            ASYNC_MUNMAP = newAsyncMunmapEnabled;
            tearDownStatic();
            setUpStatic();
            setUp();
        }
    }

    @Override
    protected String getTestName() {
        return fuzzTableNameOverride != null ? fuzzTableNameOverride : super.getTestName();
    }

    @Override
    protected void runFuzz(Rnd rnd) throws Exception {
        // Check that mixed IO is enabled by the test setup
        LOG.info().$("expected configuration fsAllowsMixedIO=").$(fsAllowsMixedIO).$();
        Assert.assertEquals(fsAllowsMixedIO, node1.getEngine().getConfiguration().isWriterMixedIOEnabled());
        super.runFuzz(rnd);
    }

    @Override
    protected void setFuzzProperties(Rnd rnd) {
        super.setFuzzProperties(rnd);
        node1.setProperty(PropertyKey.DEBUG_CAIRO_ALLOW_MIXED_IO, fsAllowsMixedIO);
        node1.setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 1 + rnd.nextLong(engine.getConfiguration().getWalSegmentRolloverRowCount()));
    }

    private static class PostingIndexStaleCloseFilesFacade extends FilesFacadeImpl {
        private final ConcurrentHashMap<Long, Long> initialPostingKeyLimits = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<Long, Boolean> postingKeyFds = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<Long, Long> postingKeyMappings = new ConcurrentHashMap<>();
        private final AtomicInteger stalePostingCloseCount = new AtomicInteger();
        private final AtomicInteger truncateBelowPublishedLimitCount = new AtomicInteger();

        @Override
        public boolean close(long fd) {
            initialPostingKeyLimits.remove(fd);
            postingKeyFds.remove(fd);
            return super.close(fd);
        }

        @Override
        public void fsync(long fd) {
            super.fsync(fd);
            recordInitialPublishedLimit(fd);
        }

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            return trackMapping(fd, offset, super.mmap(fd, len, offset, flags, memoryTag));
        }

        @Override
        public long mmapNoCache(long fd, long len, long offset, int flags, int memoryTag) {
            return trackMapping(fd, offset, super.mmapNoCache(fd, len, offset, flags, memoryTag));
        }

        @Override
        public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
            return trackRemap(addr, super.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag));
        }

        @Override
        public long mremapNoCache(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
            return trackRemap(addr, super.mremapNoCache(fd, addr, previousSize, newSize, offset, mode, memoryTag));
        }

        @Override
        public void msync(long address, long len, boolean async) {
            super.msync(address, len, async);
            final Long fd = postingKeyMappings.get(address);
            if (fd != null) {
                recordInitialPublishedLimit(fd);
            }
        }

        @Override
        public void munmap(long address, long size, int memoryTag) {
            final Long fd = postingKeyMappings.remove(address);
            if (fd != null) {
                final Long initialLimit = initialPostingKeyLimits.get(fd);
                if (initialLimit != null
                        && initialLimit == PostingIndexUtils.KEY_FILE_RESERVED
                        && readPublishedRegionLimit(fd) > initialLimit) {
                    stalePostingCloseCount.incrementAndGet();
                }
            }
            super.munmap(address, size, memoryTag);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final long fd = super.openRW(name, opts);
            if (fd > -1 && Utf8s.containsAscii(name, ".pk")) {
                postingKeyFds.put(fd, Boolean.TRUE);
                recordInitialPublishedLimit(fd);
            }
            return fd;
        }

        @Override
        public boolean truncate(long fd, long size) {
            if (postingKeyFds.containsKey(fd) && readPublishedRegionLimit(fd) > size) {
                truncateBelowPublishedLimitCount.incrementAndGet();
            }
            return super.truncate(fd, size);
        }

        int getStalePostingCloseCount() {
            return stalePostingCloseCount.get();
        }

        int getTruncateBelowPublishedLimitCount() {
            return truncateBelowPublishedLimitCount.get();
        }

        private long readPublishedRegionLimit(long fd) {
            for (int attempt = 0; attempt < 16; attempt++) {
                final long seqStartA = super.readNonNegativeLong(fd, PostingIndexUtils.PAGE_A_OFFSET
                        + PostingIndexUtils.V2_HEADER_OFFSET_SEQUENCE_START);
                final long seqEndA = super.readNonNegativeLong(fd, PostingIndexUtils.PAGE_A_OFFSET
                        + PostingIndexUtils.V2_HEADER_OFFSET_SEQUENCE_END);
                final long seqStartB = super.readNonNegativeLong(fd, PostingIndexUtils.PAGE_B_OFFSET
                        + PostingIndexUtils.V2_HEADER_OFFSET_SEQUENCE_START);
                final long seqEndB = super.readNonNegativeLong(fd, PostingIndexUtils.PAGE_B_OFFSET
                        + PostingIndexUtils.V2_HEADER_OFFSET_SEQUENCE_END);
                final boolean aStable = seqStartA != 0L && seqStartA == seqEndA && (seqStartA & 1L) == 0L;
                final boolean bStable = seqStartB != 0L && seqStartB == seqEndB && (seqStartB & 1L) == 0L;
                final long pageOffset;
                final long expectedSeq;
                if (aStable && bStable) {
                    pageOffset = seqStartA >= seqStartB
                            ? PostingIndexUtils.PAGE_A_OFFSET
                            : PostingIndexUtils.PAGE_B_OFFSET;
                    expectedSeq = Math.max(seqStartA, seqStartB);
                } else if (aStable) {
                    pageOffset = PostingIndexUtils.PAGE_A_OFFSET;
                    expectedSeq = seqStartA;
                } else if (bStable) {
                    pageOffset = PostingIndexUtils.PAGE_B_OFFSET;
                    expectedSeq = seqStartB;
                } else {
                    continue;
                }
                final long formatVersion = super.readNonNegativeLong(fd, pageOffset
                        + PostingIndexUtils.V2_HEADER_OFFSET_FORMAT_VERSION);
                final long regionLimit = super.readNonNegativeLong(fd, pageOffset
                        + PostingIndexUtils.V2_HEADER_OFFSET_REGION_LIMIT);
                final long postSeqStart = super.readNonNegativeLong(fd, pageOffset
                        + PostingIndexUtils.V2_HEADER_OFFSET_SEQUENCE_START);
                final long postSeqEnd = super.readNonNegativeLong(fd, pageOffset
                        + PostingIndexUtils.V2_HEADER_OFFSET_SEQUENCE_END);
                if (formatVersion == PostingIndexUtils.V2_FORMAT_VERSION
                        && regionLimit >= PostingIndexUtils.KEY_FILE_RESERVED
                        && postSeqStart == expectedSeq
                        && postSeqEnd == expectedSeq) {
                    return regionLimit;
                }
            }
            return -1L;
        }

        private void recordInitialPublishedLimit(long fd) {
            if (postingKeyFds.containsKey(fd)) {
                final long regionLimit = readPublishedRegionLimit(fd);
                if (regionLimit >= PostingIndexUtils.KEY_FILE_RESERVED) {
                    initialPostingKeyLimits.merge(fd, regionLimit, Math::min);
                }
            }
        }

        private long trackMapping(long fd, long offset, long address) {
            if (address != FilesFacade.MAP_FAILED && offset == 0L && postingKeyFds.containsKey(fd)) {
                postingKeyMappings.put(address, fd);
                recordInitialPublishedLimit(fd);
            }
            return address;
        }

        private long trackRemap(long previousAddress, long newAddress) {
            final Long fd = postingKeyMappings.get(previousAddress);
            if (fd != null && newAddress != FilesFacade.MAP_FAILED) {
                postingKeyMappings.remove(previousAddress);
                postingKeyMappings.put(newAddress, fd);
            }
            return newAddress;
        }
    }
}
