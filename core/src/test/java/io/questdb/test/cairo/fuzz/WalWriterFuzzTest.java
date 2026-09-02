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
import io.questdb.std.Files;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Numbers;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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

    /**
     * Partition compaction defaults to off - {@code Overrides} only turns merge-append itself on - so
     * without this, no fuzz run here ever exercises it. One run in three leaves it off, exactly like
     * every fuzz run before this method existed. The rest turn it on and draw its budgets independently,
     * each LOG-UNIFORM (its exponent, not the value itself, is what is drawn uniformly) so
     * a run is as likely to land tight as generous at every scale in between, rather than clustering near
     * the middle the way a plain {@code nextInt} over the same range would:
     * <ul>
     *     <li>{@code pieceThreshold}: 1 to 10,000. The piece-count rule's cap is
     *     {@code max(pieceThreshold, liveRows / avgRowsPieceLim)} (see
     *     {@code PartitionCompactionPolicy.effectiveMaxPieces}) - this is the flat floor half of that
     *     formula. At the low end - single digits - this is what makes {@code
     *     O3PartitionJob#shouldAssembleFreshPartitionVersion}'s proactive check fire routinely instead of
     *     only on the rare generation-exhausted case, so its interaction with dedup, replace-range,
     *     column-top backfill and every other fuzzed transaction kind gets covered. At the high end it
     *     never fires for a fuzz-sized table on its own, leaving the scaled half of the formula (below) and
     *     {@code runCompaction}'s other rules covered instead;</li>
     *     <li>{@code avgRowsPieceLim}: 1 to 10,000,000 - the scaled half of the same cap, {@code liveRows /
     *     this}. The LOW end (near 1) makes the scaled term track liveRows itself, so {@code pieceThreshold}
     *     alone decides the cap. The HIGH end (comfortably above every {@code setFuzzCounts} total row
     *     count in this class) drives the scaled term to 0, so it never out-competes {@code pieceThreshold}
     *     either - the interesting middle of the range is where the scaled term can exceed a small
     *     {@code pieceThreshold} draw and take over as the effective cap;</li>
     *     <li>{@code deadRowsRatio}: 0 to 999. The waste-ratio rule fires on dead rows exceeding a whole
     *     MULTIPLE of live rows, not a percentage, so 0 - dead exceeding zero times live, the tightest
     *     this can express - is one decade of the draw rather than a special case;</li>
     *     <li>{@code deadMinSize}: 100,000 to ~1,000,000,000,000 (1T) bytes, the floor below which the
     *     waste-ratio rule does not fire regardless of the ratio. Starts at 100K rather than single bytes
     *     so a tight draw still means "a meaningful stride of dead rows", not "one row".</li>
     *     <li>{@code prefixMinPercent}: the shipped default (50) nine runs out of ten, so that value - the
     *     one every production table actually runs under - stays the best-tested. The tenth run draws
     *     uniformly from 1 to 40, comfortably under the front share a typical fuzz workload's narrow
     *     backdated strides leave behind, so MOVE-TAIL wins over REWRITE close to every time a folder
     *     qualifies instead of only when the default happens to clear the bar - giving MOVE-TAIL, and the
     *     MAKE-PLAIN it immediately chains into, real odds of firing without dominating every run.</li>
     *     <li>{@code tableDeadThreshold}: zero four runs in five, so the table-pressure rule keeps firing off
     *     a mere handful of dead rows exactly like every fuzz run before this floor existed - fuzz tables
     *     are small, and a floor anywhere near the 50MB shipped default would suppress table-pressure on
     *     almost all of them, losing the coverage it currently gives REWRITE, MOVE-TAIL and MAKE-PLAIN.
     *     The fifth run draws LOG-UNIFORM from 1 byte up to that 50MB default, covering both the
     *     suppression itself and, at the top of the range, the value a production table actually runs
     *     under.</li>
     * </ul>
     */
    private static void setRndPartitionCompactionProperties(Rnd rnd) {
        final int pieceThreshold = (int) Math.round(Math.pow(10, rnd.nextDouble() * 4));
        final long avgRowsPieceLim = Math.round(Math.pow(10, rnd.nextDouble() * 7));
        final int deadRowsRatio = (int) Math.round(Math.pow(10, rnd.nextDouble() * 3)) - 1;
        final long deadMinSize = Math.round(Math.pow(10, 5 + rnd.nextDouble() * 7));
        final int prefixMinPercent = rnd.nextInt(10) == 0 ? 1 + rnd.nextInt(40) : 50;
        final long tableDeadThreshold = rnd.nextInt(5) == 0
                ? Math.round(Math.pow(10, rnd.nextDouble() * Math.log10(50 * Numbers.SIZE_1MB)))
                : 0;
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_PIECE_THRESHOLD, pieceThreshold);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, avgRowsPieceLim);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_ROWS_RATIO, deadRowsRatio);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, deadMinSize);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_PREFIX_MIN_PERCENT, prefixMinPercent);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_TABLE_DEAD_THRESHOLD, tableDeadThreshold);
        LOG.info().$("partition compaction fuzz mode [pieceThreshold=").$(pieceThreshold)
                .$(", avgRowsPieceLim=").$(avgRowsPieceLim)
                .$(", deadRowsRatio=").$(deadRowsRatio)
                .$(", deadMinSize=").$(deadMinSize)
                .$(", prefixMinPercent=").$(prefixMinPercent)
                .$(", tableDeadThreshold=").$(tableDeadThreshold)
                .I$();
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
        setRndPartitionCompactionProperties(rnd);
    }
}
