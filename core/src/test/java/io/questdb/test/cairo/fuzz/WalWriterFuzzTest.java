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
import io.questdb.cairo.ColumnPurgeJob;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.tasks.ColumnPurgeTask;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.fuzz.FuzzChangeColumnTypeOperation;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    // the purges of the last runTxnZeroCoveredSymbolFuzz() that a purge job refused because the truncate
    // version changed, as the truncate refusal listener of the configuration reports them: the check
    // before VACUUM TABLE skips their POSTING files. The purge jobs of the worker pool add to it.
    private final Set<String> txnZeroRunRefusedPurges = ConcurrentHashMap.newKeySet();
    private boolean existingFilesParanoia;
    private boolean fsAllowsMixedIO;
    private long txnZeroRunPurgeTasksConsumed = -1;
    private long txnZeroRunPurgeTasksLogged = -1;
    private int txnZeroRunSkippedRefusedPurgeFileCount;

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
    public void testConvertPartitionColumnTopsFuzz() throws Exception {
        // Row groups of 50..200 rows and frequent ADD COLUMN, CONVERT TO PARQUET and CONVERT TO NATIVE:
        // a column added to a partition with more rows than a row group gets a top that spans row
        // groups, which CONVERT TO PARQUET zeroes in _cv and CONVERT TO NATIVE has to restore.
        Rnd rnd = generateRandom(LOG);
        setTestParams(rnd);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 50 + rnd.nextInt(151));

        setFuzzProbabilities(
                0.01,
                0.01,
                0.01,
                0.05,
                0.2,
                0.02,
                0.02,
                0.05,
                1.0,
                0.01,
                0.0,
                0.5,
                0.5,
                0.0,
                0.0,
                0.0,
                0.0,
                0,
                0.0,
                0.0,
                0.0, // addCoveringIndexProb
                0.0 // SET FORMAT PARQUET|NATIVE probability
        );
        setFuzzCounts(false, 10_000, 300, 20, 10, 1000, 2_000, 3);
        runFuzz(rnd);
    }

    @Test
    public void testConvertPartitionTypeChangeColumnTopsFuzz() throws Exception {
        // As testConvertPartitionColumnTopsFuzz, with frequent type changes, one of them from STRING, SYMBOL
        // or VARCHAR to BOOLEAN, BYTE or SHORT lazily on a parquet partition, and changes back to text, see
        // FuzzTransactionGenerator.generateSet(). The column must keep its top through CONVERT TO NATIVE and
        // the ALTER COLUMN TYPE pre-pass, so that the change back reads the top as NULL, as the non-WAL twin.
        Rnd rnd = generateRandom(LOG);
        setTestParams(rnd);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 50 + rnd.nextInt(151));

        setFuzzProbabilities(
                0.01,
                0.01,
                0.01,
                0.05,
                0.2,
                0.02,
                0.02,
                0.3,
                1.0,
                0.01,
                0.0,
                0.5,
                0.5,
                0.0,
                0.0,
                0.0,
                0.0,
                0,
                0.0,
                0.0,
                0.0, // addCoveringIndexProb
                0.0 // SET FORMAT PARQUET|NATIVE probability
        );
        // one WAL writer for the parallel apply too: several would commit the in-order blocks out of order
        fuzzer.setFuzzCounts(false, 5_000, 500, 20, 10, 1000, 2_000, 5, 1, 1);
        fuzzer.setNoSentinelTypeChangeEnabled(true);
        FuzzChangeColumnTypeOperation.resetTextToNoSentinelChanges();
        runFuzz(rnd);
        // the generator makes the change after a CONVERT TO PARQUET of a partition with rows, and before
        // any CONVERT TO NATIVE, so the WAL tables apply it lazily on that parquet partition
        Assert.assertEquals(1, FuzzChangeColumnTypeOperation.getTextToNoSentinelChanges());
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
    public void testConvertPartitionToParquetWithCoveringIndexOnTxnZeroSymbol() throws Exception {
        // Pins the shape that hid the txn 0 sidecar bug, see FuzzRunner.setTxnZeroCoveredSymbol(): sym0
        // is added to the still-empty table (column name txn 0) and covered before the first row. These
        // seeds convert partitions to parquet and back, rename sym0 twice, drop other columns and change
        // their types, all on that covering index. The fuzz plants no IO failure, see
        // runTxnZeroCoveredSymbolRandomFuzz().
        Rnd rnd = generateRandom(LOG, 11L, 80L);
        setTestParams(rnd);

        setFuzzProbabilities(
                0.01,
                0.01,
                0.01,
                0.1,
                0.05,
                0.1,
                0.1,
                0.1,
                1.0,
                0.01,
                0.01,
                0.5,
                0.5,
                0.0,
                0.0,
                0.0,
                0.0,
                0,
                0.01,
                0.1,
                0.0, // addCoveringIndexProb
                0.0
        );
        fuzzer.setFuzzCounts(true, 2_000, 60, 20, 10, 100, 100, 3, -1, 0);
        runTxnZeroCoveredSymbolFuzz(rnd);
    }

    @Test
    public void testConvertPartitionToParquetWithCoveringIndexOnTxnZeroSymbolFuzz() throws Exception {
        runTxnZeroCoveredSymbolRandomFuzz(generateRandom(LOG), null, null, 1);
    }

    @Test
    public void testConvertPartitionToParquetWithCoveringIndexOnTxnZeroSymbolLostPurgeLog() throws Exception {
        // Pins a run of the fuzz test above where an IO failure hits the purge job as it saves a purge in
        // its log. The job saves no purge after that, so the purge job after the run cannot replay the
        // deferred purges of the WAL tables: the check before VACUUM TABLE skips, and the check after VACUUM
        // TABLE proves VACUUM removes the files. In the original run an IO failure that the fuzz plants for
        // the non-WAL writer hit the purge job by chance; here the fuzz plants none, and the files facade
        // below fails the first file the purge job opens in a partition of its log, which is the first
        // save, in every run. The fuzz test drew the default symbol index type from other seeds.
        final AtomicBoolean isPurgeLogOpenFailed = new AtomicBoolean();
        runTxnZeroCoveredSymbolRandomFuzz(
                generateRandom(LOG, 882492233733669L, 1792176408151L),
                "BITMAP",
                new TestFilesFacadeImpl() {
                    @Override
                    public long openRW(LPSZ name, int opts) {
                        if (isPurgeLogPartitionFile(name) && isPurgeLogOpenFailed.compareAndSet(false, true)) {
                            return -1;
                        }
                        return super.openRW(name, opts);
                    }
                },
                0
        );
        Assert.assertTrue(isPurgeLogOpenFailed.get());
        Assert.assertTrue(
                txnZeroRunPurgeTasksLogged + " < " + txnZeroRunPurgeTasksConsumed,
                txnZeroRunPurgeTasksLogged < txnZeroRunPurgeTasksConsumed
        );
    }

    @Test
    public void testConvertPartitionToParquetWithCoveringIndexOnTxnZeroSymbolRefusedPurge() throws Exception {
        // Pins a run of the fuzz test above where DROP PARTITION follows DROP INDEX of sym0 on the non-WAL
        // table while the purge of the dropped index waits in the purge log. DROP PARTITION changes the
        // truncate version, and the purge job refuses the purge: the check before VACUUM TABLE exempts
        // its files, and the check after VACUUM TABLE proves VACUUM removes them. The fuzz test drew the
        // default symbol index type from other seeds. The fuzz plants no IO failure: one that hit the purge
        // job would lose the purge log, and the check before VACUUM TABLE would skip.
        runTxnZeroCoveredSymbolRandomFuzz(generateRandom(LOG, 882356240241250L, 1792176272157L), "POSTING DELTA", null, 0);
        Assert.assertEquals(txnZeroRunPurgeTasksConsumed, txnZeroRunPurgeTasksLogged);
        // instance 0 of sym0 in partition 2022-02-24 (name txn 1) of the non-WAL table, whose index DROP
        // INDEX dropped before DROP PARTITION of 2022-02-25
        final TableToken tableToken = engine.verifyTableName(getTestName() + "_nonwal");
        final int timestampType;
        try (TableReader reader = getReader(tableToken.getTableName())) {
            timestampType = reader.getMetadata().getTimestampType();
        }
        final String sym0Key = refusedPurgeKey(
                tableToken.getDirName(),
                ColumnType.getTimestampDriver(timestampType).parseFloorLiteral("2022-02-24"),
                1,
                "sym0",
                0
        );
        Assert.assertTrue(sym0Key + " in " + txnZeroRunRefusedPurges, txnZeroRunRefusedPurges.contains(sym0Key));
        Assert.assertTrue("skipped files: " + txnZeroRunSkippedRefusedPurgeFileCount, txnZeroRunSkippedRefusedPurgeFileCount > 0);
    }

    @Test
    public void testVacuumWithCoveringIndexesOnTxnZeroAndTxnNoneSymbols() throws Exception {
        // Pins VACUUM TABLE over covering indexes on sym0, added to the still-empty table (column name
        // txn 0), and on c2, declared in CREATE TABLE (column name txn NONE) and covered by a fuzz
        // operation. The .pc sidecar names of both carry the token 0, and VACUUM once purged them as
        // files of a stale column version. The fuzz plants no IO failure, see
        // runTxnZeroCoveredSymbolRandomFuzz().
        Rnd rnd = generateRandom(LOG, 1L, 2L);
        setTestParams(rnd);

        setFuzzProbabilities(
                0.01,
                0.01,
                0.01,
                0.1,
                0.05,
                0.05,
                0.0,
                0.0,
                1.0,
                0.01,
                0.01,
                0.5,
                0.5,
                0.0,
                0.0,
                0.8,
                0.00,
                0,
                0.01,
                0.1,
                0.3, // addCoveringIndexProb
                0.0
        );
        fuzzer.setFuzzCounts(true, 2_000, 60, 20, 10, 100, 100, 3, -1, 0);
        runTxnZeroCoveredSymbolFuzz(rnd, "c2[-1, -1, -1] sym0[0, 0, 0]");
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
     * Checks that each POSTING index file in a live partition of tableName belongs to a live column
     * instance: a POSTING indexed column of that name whose column name txn in the partition is the txn
     * in the file name. The .pk, .pci and .pv names carry no txn for txn NONE. A .pc sidecar name carries
     * the txn token, 0 for txn NONE and for txn 0, the txn otherwise. A file of a dropped, renamed or
     * retyped column version, or of a dropped or changed index, fails the check.
     */
    private void assertNoPostingFilesOfDroppedColumnVersions(String tableName) {
        assertNoPostingFilesOfDroppedColumnVersions(tableName, new HashSet<>());
    }

    /**
     * As {@link #assertNoPostingFilesOfDroppedColumnVersions(String)}, but skips the files of the column
     * versions in refusedPurges, see {@link #listenToTruncateRefusals}.
     */
    private void assertNoPostingFilesOfDroppedColumnVersions(String tableName, Set<String> refusedPurges) {
        final Pattern coverFileName = Pattern.compile("^(.+)\\.pc[0-9]+\\.([0-9]+)(\\.[0-9]+)?\\.[0-9]+$");
        final Pattern keyOrCoverInfoFileName = Pattern.compile("^(.+)\\.(?:pk|pci)(?:\\.([0-9]+))?$");
        final Pattern valueFileName = Pattern.compile("^(.+)\\.pv(?:\\.([0-9]+))?\\.[0-9]+$");
        try (TableReader reader = getReader(tableName); Path path = new Path()) {
            final TableReaderMetadata metadata = reader.getMetadata();
            for (int p = 0, n = reader.getTxFile().getPartitionCount(); p < n; p++) {
                final long partitionTimestamp = reader.getTxFile().getPartitionTimestampByIndex(p);
                path.of(configuration.getDbRoot()).concat(reader.getTableToken());
                TableUtils.setPathForNativePartition(
                        path,
                        metadata.getTimestampType(),
                        reader.getPartitionedBy(),
                        partitionTimestamp,
                        reader.getTxFile().getPartitionNameTxn(p)
                );
                final String dir = path.toString();
                final String[] names = new File(dir).list();
                Assert.assertNotNull(dir, names);
                for (String name : names) {
                    final Matcher coverFileMatcher = coverFileName.matcher(name);
                    final Matcher keyOrCoverInfoFileMatcher = keyOrCoverInfoFileName.matcher(name);
                    final Matcher valueFileMatcher = valueFileName.matcher(name);
                    final Matcher matcher;
                    if (coverFileMatcher.matches()) {
                        matcher = coverFileMatcher;
                    } else if (keyOrCoverInfoFileMatcher.matches()) {
                        matcher = keyOrCoverInfoFileMatcher;
                    } else if (valueFileMatcher.matches()) {
                        matcher = valueFileMatcher;
                    } else {
                        continue;
                    }
                    final long fileTxn;
                    if (matcher == coverFileMatcher) {
                        fileTxn = Long.parseLong(matcher.group(2));
                    } else {
                        fileTxn = matcher.group(2) != null ? Long.parseLong(matcher.group(2)) : TableUtils.COLUMN_NAME_TXN_NONE;
                    }
                    final long partitionNameTxn = reader.getTxFile().getPartitionNameTxn(p);
                    final String tableDirName = reader.getTableToken().getDirName();
                    // a .pc name writes the column name txn NONE as 0
                    if (refusedPurges.contains(refusedPurgeKey(tableDirName, partitionTimestamp, partitionNameTxn, matcher.group(1), fileTxn))
                            || (matcher == coverFileMatcher && fileTxn == 0 && refusedPurges.contains(refusedPurgeKey(
                            tableDirName, partitionTimestamp, partitionNameTxn, matcher.group(1), TableUtils.COLUMN_NAME_TXN_NONE)))) {
                        LOG.info().$("skipping posting file of a refused purge [dir=").$(dir).$(", file=").$(name).I$();
                        txnZeroRunSkippedRefusedPurgeFileCount++;
                        continue;
                    }
                    final String context = dir + ", " + name + " in " + Arrays.toString(names);
                    final int columnIndex = metadata.getColumnIndexQuiet(matcher.group(1));
                    Assert.assertTrue("posting file of a dropped column: " + context, columnIndex > -1);
                    Assert.assertTrue(
                            "posting file of a dropped index: " + context,
                            IndexType.isPosting(metadata.getColumnIndexType(columnIndex))
                    );
                    final long columnNameTxn = reader.getColumnVersionReader().getColumnNameTxn(
                            partitionTimestamp,
                            metadata.getWriterIndex(columnIndex)
                    );
                    Assert.assertEquals(
                            "posting file of a dropped column version: " + context,
                            matcher == coverFileMatcher ? Math.max(0, columnNameTxn) : columnNameTxn,
                            fileTxn
                    );
                }
            }
        }
    }

    /**
     * Adds to purges the column versions of tableName that the purge log holds with another truncate
     * version than the table has. DROP PARTITION, ATTACH, DETACH and TRUNCATE change it. The purge job
     * refuses such a purge when it reads the truncate version after the change, and completes it when it
     * reads the version before, so these are the purges that the job could have refused.
     */
    private void addPurgesWithChangedTruncateVersion(String tableName, Set<String> purges) throws SqlException {
        final TableToken tableToken = engine.verifyTableName(tableName);
        final long truncateVersion;
        final int timestampType;
        try (TableReader reader = getReader(tableName)) {
            truncateVersion = reader.getTxFile().getTruncateVersion();
            timestampType = reader.getMetadata().getTimestampType();
        }
        final String sql = "SELECT partition_timestamp, partition_name_txn, column_name, column_version FROM \""
                + configuration.getSystemTableNamePrefix() + "column_versions_purge_log\" WHERE table_name = '"
                + tableToken.getDirName() + "' AND table_id = " + tableToken.getTableId() + " AND truncate_version <> " + truncateVersion;
        try (
                RecordCursorFactory factory = select(sql);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                final String key = refusedPurgeKey(
                        tableToken.getDirName(),
                        ColumnType.getTimestampDriver(timestampType).fromMicros(record.getTimestamp(0)),
                        record.getLong(1),
                        record.getSymA(2),
                        record.getLong(3)
                );
                purges.add(key);
            }
        }
    }

    // ALTER TABLE ... ADD INDEX TYPE POSTING INCLUDE of the column and include list of pickPostingIndexToAdd()
    private static String addPostingIndexSql(String tableName, String[] postingIndex) {
        return "ALTER TABLE \"" + tableName + "\" ALTER COLUMN \"" + postingIndex[0] + "\" ADD INDEX TYPE POSTING INCLUDE (" + postingIndex[1] + ")";
    }

    /**
     * The number of purges in the purge log. The purge job of the fuzz run truncates the log when it
     * starts, and saves one row per partition of each purge.
     */
    private long countPurgeLogTasks() throws SqlException {
        final String sql = "SELECT count() FROM (SELECT DISTINCT ts, table_name, column_name, updated_txn FROM \""
                + configuration.getSystemTableNamePrefix() + "column_versions_purge_log\")";
        try (
                RecordCursorFactory factory = select(sql);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            Assert.assertTrue(cursor.hasNext());
            return cursor.getRecord().getLong(0);
        }
    }

    /**
     * Picks a SYMBOL column of tableName without an index, and up to two other columns that are not the
     * designated timestamp, for ALTER TABLE ... ADD INDEX TYPE POSTING INCLUDE. Returns the quoted column
     * name and the quoted, comma-separated include list, or null when there is no such column.
     */
    private static String[] pickPostingIndexToAdd(String tableName, Rnd rnd) {
        try (TableReader reader = getReader(tableName)) {
            final TableReaderMetadata metadata = reader.getMetadata();
            final ArrayList<String> symbolColumns = new ArrayList<>();
            final ArrayList<String> otherColumns = new ArrayList<>();
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                if (ColumnType.isSymbol(metadata.getColumnType(i)) && !metadata.isColumnIndexed(i)) {
                    symbolColumns.add(metadata.getColumnName(i));
                } else if (i != metadata.getTimestampIndex()) {
                    otherColumns.add(metadata.getColumnName(i));
                }
            }
            if (symbolColumns.isEmpty() || otherColumns.isEmpty()) {
                LOG.info().$("no posting index to add while vacuum runs [table=").$(tableName).I$();
                return null;
            }
            final String column = symbolColumns.get(rnd.nextInt(symbolColumns.size()));
            final StringBuilder includeList = new StringBuilder();
            for (int i = 0, n = 1 + rnd.nextInt(Math.min(2, otherColumns.size())); i < n; i++) {
                includeList.append(i > 0 ? ", " : "").append('"').append(otherColumns.remove(rnd.nextInt(otherColumns.size()))).append('"');
            }
            LOG.info().$("adding posting index while vacuum runs [column=").$(column).$(", include=").$(includeList).I$();
            return new String[]{column, includeList.toString()};
        }
    }

    /**
     * Runs a column purge job until it purged every column version that the fuzz run queued, moving
     * the clock past the retry delay before each run. The job first replays the purges that the purge
     * job of the fuzz run left incomplete in the purge log.
     */
    private void purgeColumnVersions() throws SqlException {
        engine.releaseInactive();
        try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
            long now = configuration.getMicrosecondClock().getTicks();
            final long retryDelay = Math.max(configuration.getColumnPurgeRetryDelay(), configuration.getColumnPurgeRetryDelayLimit());
            for (int i = 0; i < 20 && (i == 0 || purgeJob.getOutstandingPurgeTasks() > 0); i++) {
                now += retryDelay + 1;
                setCurrentMicros(now);
                purgeJob.run();
            }
            Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
        } finally {
            setCurrentMicros(-1);
        }
    }

    // whether name is a file in a partition of the purge log of ColumnPurgeJob
    private static boolean isPurgeLogPartitionFile(LPSZ name) {
        final File partitionDir = new File(Utf8s.stringFromUtf8Bytes(name)).getParentFile();
        final File tableDir = partitionDir != null ? partitionDir.getParentFile() : null;
        return tableDir != null && tableDir.getName().contains("column_versions_purge_log~");
    }

    /**
     * Installs the truncate refusal listener of the configuration, which adds the key of each entry that
     * a purge job refuses to {@link #txnZeroRunRefusedPurges}. The purge log keeps partition timestamps
     * in micros, so the key takes the timestamp that the log gives back: the same value for a partition,
     * another one for the table root entry of a nanosecond table.
     */
    private void listenToTruncateRefusals() {
        txnZeroRunRefusedPurges.clear();
        node1.getConfigurationOverrides().setColumnPurgeTruncateRefusalListener((task, offset) -> {
            final LongList info = task.getUpdatedColumnInfo();
            final TimestampDriver driver = task.getTimestampTypeDriver();
            final String key = refusedPurgeKey(
                    task.getTableToken().getDirName(),
                    driver.fromMicros(driver.toMicros(info.getQuick(offset + ColumnPurgeTask.OFFSET_PARTITION_TIMESTAMP))),
                    info.getQuick(offset + ColumnPurgeTask.OFFSET_PARTITION_NAME_TXN),
                    task.getColumnName(),
                    info.getQuick(offset + ColumnPurgeTask.OFFSET_COLUMN_VERSION)
            );
            LOG.info().$("purge refused by truncate [key=").$(key).I$();
            txnZeroRunRefusedPurges.add(key);
        });
    }

    // the key of a purged column version in a partition, see listenToTruncateRefusals()
    private static String refusedPurgeKey(String tableDirName, long partitionTimestamp, long partitionNameTxn, CharSequence columnName, long columnNameTxn) {
        return tableDirName + "/" + partitionTimestamp + "/" + partitionNameTxn + "/" + columnName + "/" + columnNameTxn;
    }

    /**
     * The POSTING indexed columns of tableName that cover other columns, each with its column name txn
     * in every partition: {@code name[txn, ...]}, separated by spaces.
     */
    private static String coveringIndexInstances(String tableName) {
        final StringBuilder instances = new StringBuilder();
        try (TableReader reader = getReader(tableName)) {
            final TableReaderMetadata metadata = reader.getMetadata();
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                final IntList coveredIndices = metadata.getCoveringColumnIndices(i);
                if (!IndexType.isPosting(metadata.getColumnIndexType(i)) || coveredIndices == null || coveredIndices.size() == 0) {
                    continue;
                }
                instances.append(instances.isEmpty() ? "" : " ").append(metadata.getColumnName(i)).append('[');
                for (int p = 0, m = reader.getPartitionCount(); p < m; p++) {
                    instances.append(p > 0 ? ", " : "").append(reader.getColumnVersionReader().getColumnNameTxn(
                            reader.getPartitionTimestampByIndex(p),
                            metadata.getWriterIndex(i)
                    ));
                }
                instances.append(']');
            }
        }
        return instances.toString();
    }

    private void runTxnZeroCoveredSymbolFuzz(Rnd rnd) throws Exception {
        runTxnZeroCoveredSymbolFuzz(rnd, null);
    }

    /**
     * Runs the fuzz of {@link #runTxnZeroCoveredSymbolFuzz(Rnd)} with the operations and counts of
     * testConvertPartitionToParquetWithCoveringIndexOnTxnZeroSymbolFuzz drawn from rnd.
     *
     * @param defaultSymbolIndexType when not null, the default symbol index type, which otherwise
     *                               AbstractFuzzTest draws from its own seeds
     * @param filesFacade            when not null, the files facade under the IO failures of the fuzz
     * @param ioFailureCount         the number of IO failures the fuzz plants in the non-WAL apply. A pinned
     *                               seed plants none: the failure fires on the N-th call to the files
     *                               facade, from any thread, and the engine makes some of those calls on
     *                               one platform only (fsync of a directory on POSIX, removal before a
     *                               copy on Windows), so the same seed would fail other calls on Windows.
     */
    private void runTxnZeroCoveredSymbolRandomFuzz(
            Rnd rnd,
            String defaultSymbolIndexType,
            FilesFacade filesFacade,
            int ioFailureCount
    ) throws Exception {
        setTestParams(rnd);
        if (filesFacade != null) {
            // set after setTestParams(), which can set the test up again and reset ff
            ff = filesFacade;
            fuzzer.withDb(engine, sqlExecutionContext);
        }
        if (defaultSymbolIndexType != null) {
            node1.setProperty(PropertyKey.CAIRO_DEFAULT_SYMBOL_INDEX_TYPE, defaultSymbolIndexType);
        }

        setFuzzProbabilities(
                0.01,
                0.01,
                0.01,
                0.1,
                0.05,
                0.05,
                // Two separate bugs keep renames and type changes out of the random runs: RENAME COLUMN
                // of a covered column makes covering queries read NULL for it, and ALTER COLUMN TYPE of
                // an indexed symbol leaves its index files in parquet partitions. A type or symbol
                // capacity change also moves sym0 off column name txn 0. The pinned seed above renames
                // sym0 itself and changes the type of a column that is neither indexed nor covered.
                0.0,
                0.0,
                1.0,
                0.01,
                0.01,
                0.5,
                0.5,
                0.0,
                0.0,
                0.8,
                0.00,
                0,
                0.01,
                0.1,
                0.1, // addCoveringIndexProb
                0.0
        );
        fuzzer.setFuzzCounts(rnd.nextBoolean(), 10_000, 300, 20, 10, 1000, 100, 3, -1, ioFailureCount);
        // DROP INDEX of a column that a POSTING index covers makes covering queries read NULL, so
        // FuzzDropIndexOperation skips that shape.
        fuzzer.setDropIndexProbability(0.05);
        if (rnd.nextBoolean()) {
            // The purge job of the fuzz run saves the purges in the purge log and leaves them there, as
            // a restart before their retry delay would, and the purge job after the run replays them,
            // the purges of DROP INDEX and of index type changes among them.
            LOG.info().$("deferring column purges past the fuzz run").$();
            node1.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, Micros.DAY_MICROS);
        }
        runTxnZeroCoveredSymbolFuzz(rnd);
    }

    /**
     * Runs the fuzz on tables that start with a covering index on a column of column name txn 0, see
     * FuzzRunner.setTxnZeroCoveredSymbol(). runFuzz() checks that the covered queries read what the
     * uncovered ones read and that the WAL tables equal the non-WAL twin. After the column purge, which
     * replays the purges left in the purge log, no POSTING index file of a dropped column version or index
     * stays in a live partition of any of the three tables, apart from the files of the purges that a
     * purge job refuses because the truncate version changed, see {@link #listenToTruncateRefusals}. Each
     * of those must be in the purge log with another truncate version than the table has. The check is
     * skipped when the purge log lost purges. Then VACUUM TABLE and the column purge run on the three
     * tables, VACUUM TABLE of each WAL table while the WAL apply adds a POSTING index to a SYMBOL column, see
     * {@link #vacuumWhileAddingPostingIndex}. The tables must still match, read their covered values and
     * hold no POSTING index file of a dropped column version or index, with no exemption.
     *
     * @param expectedVacuumedCoveringIndexes when not null, the {@link #coveringIndexInstances} of the
     *                                        WAL table that VACUUM TABLE runs over
     */
    private void runTxnZeroCoveredSymbolFuzz(Rnd rnd, String expectedVacuumedCoveringIndexes) throws Exception {
        fuzzer.setTxnZeroCoveredSymbol(rnd);
        final long purgeTasksConsumedBeforeRun = engine.getMessageBus().getColumnPurgeSubSeq().current();
        // before runFuzz() starts the worker pool, whose purge job calls the listener
        listenToTruncateRefusals();
        runFuzz(rnd);
        assertMemoryLeak(() -> {
            final String tableName = getTestName();
            final String tableNameNoWal = tableName + "_nonwal";
            final String[] walTableNames = {tableName + "_wal", tableName + "_wal_parallel"};
            // The purge job of the fuzz run takes a purge off the queue and then saves it in the purge log.
            // An IO failure that the fuzz plants for the non-WAL writer can hit the log instead, and the job
            // saves no purge after that, so the purge job after the run, which stands for a restart, cannot
            // replay the purges it did not save.
            final long purgeTasksConsumed = engine.getMessageBus().getColumnPurgeSubSeq().current() - purgeTasksConsumedBeforeRun;
            final long purgeTasksLogged = countPurgeLogTasks();
            Assert.assertTrue(purgeTasksLogged + " > " + purgeTasksConsumed, purgeTasksLogged <= purgeTasksConsumed);
            txnZeroRunPurgeTasksConsumed = purgeTasksConsumed;
            txnZeroRunPurgeTasksLogged = purgeTasksLogged;
            // read before purgeColumnVersions(), whose purge job replays and truncates the purge log
            final Set<String> purgesWithChangedTruncateVersion = new HashSet<>();
            addPurgesWithChangedTruncateVersion(tableNameNoWal, purgesWithChangedTruncateVersion);
            for (String walTableName : walTableNames) {
                addPurgesWithChangedTruncateVersion(walTableName, purgesWithChangedTruncateVersion);
            }
            purgeColumnVersions();
            if (purgeTasksLogged == purgeTasksConsumed) {
                // the purge job saves each purge in its log before it tries it, and truncate versions only grow
                final Set<String> refusedPurgesOutsideLog = new TreeSet<>(txnZeroRunRefusedPurges);
                refusedPurgesOutsideLog.removeAll(purgesWithChangedTruncateVersion);
                Assert.assertEquals("refused purges outside the purge log", "[]", refusedPurgesOutsideLog.toString());
                final Set<String> refusedPurges = new HashSet<>(txnZeroRunRefusedPurges);
                assertNoPostingFilesOfDroppedColumnVersions(tableNameNoWal, refusedPurges);
                for (String walTableName : walTableNames) {
                    assertNoPostingFilesOfDroppedColumnVersions(walTableName, refusedPurges);
                }
            } else {
                LOG.info().$("purge log lost purges, skipping the check before VACUUM [consumed=").$(purgeTasksConsumed)
                        .$(", logged=").$(purgeTasksLogged).I$();
            }

            final String vacuumedCoveringIndexes = coveringIndexInstances(walTableNames[0]);
            LOG.info().$("vacuum covering indexes [table=").$(walTableNames[0]).$(", indexes=").$(vacuumedCoveringIndexes).I$();
            if (expectedVacuumedCoveringIndexes != null) {
                Assert.assertEquals(expectedVacuumedCoveringIndexes, vacuumedCoveringIndexes);
            }
            execute("VACUUM TABLE " + tableNameNoWal);
            final String[] postingIndex = pickPostingIndexToAdd(tableNameNoWal, rnd);
            if (postingIndex != null) {
                execute(addPostingIndexSql(tableNameNoWal, postingIndex));
            }
            for (String walTableName : walTableNames) {
                if (postingIndex != null) {
                    vacuumWhileAddingPostingIndex(walTableName, postingIndex);
                } else {
                    execute("VACUUM TABLE " + walTableName);
                }
            }
            purgeColumnVersions();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (String walTableName : walTableNames) {
                    TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableNameNoWal, walTableName, LOG);
                    fuzzer.assertRandomIndexes(tableNameNoWal, walTableName, rnd);
                }
            }
            assertNoPostingFilesOfDroppedColumnVersions(tableNameNoWal);
            for (String walTableName : walTableNames) {
                assertNoPostingFilesOfDroppedColumnVersions(walTableName);
            }
        });
    }

    /**
     * Runs VACUUM TABLE on the WAL table while another thread applies ALTER TABLE ... ADD INDEX TYPE
     * POSTING INCLUDE on a SYMBOL column without an index. The .pc files of the new index carry the host
     * token 0 for the column name txn NONE, which a VACUUM that does not see the index yet must keep.
     * The thread starts the WAL apply before VACUUM starts; the order of the two after that is free.
     */
    private void vacuumWhileAddingPostingIndex(String walTableName, String[] postingIndex) throws Exception {
        execute(addPostingIndexSql(walTableName, postingIndex));
        final AtomicReference<Throwable> applyError = new AtomicReference<>();
        final CountDownLatch applyStarted = new CountDownLatch(1);
        final Thread applyThread = new Thread(() -> {
            try {
                applyStarted.countDown();
                drainWalQueue();
            } catch (Throwable th) {
                applyError.set(th);
            } finally {
                Path.clearThreadLocals();
            }
        });
        applyThread.start();
        try {
            applyStarted.await();
            execute("VACUUM TABLE " + walTableName);
        } finally {
            applyThread.join();
        }
        if (applyError.get() != null) {
            throw new AssertionError("WAL apply failed", applyError.get());
        }
        Assert.assertFalse(walTableName, engine.getTableSequencerAPI().isSuspended(engine.verifyTableName(walTableName)));
    }

    private void setTestParams(Rnd rnd) throws Exception {
        int newScoreboardVersion = rnd.nextBoolean() ? 1 : 2;
        // draws on every platform so that a seed replays the same run on Windows, which does not support
        // async munmap
        boolean newAsyncMunmapEnabled = rnd.nextBoolean() && Os.isPosix();
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
    }
}
