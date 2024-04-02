/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.griffin.wal;

import io.questdb.PropertyKey;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.test.griffin.wal.FuzzRunner.MAX_WAL_APPLY_TIME_PER_TABLE_CEIL;

// These test is designed to produce unstable runs, e.g. random generator is created
// using current execution time.
// This improves coverage. To debug failures in CI find the line logging random seeds
// and change line
// Rnd rnd = generateRandom(LOG);
// to
// Rnd rnd = new Rnd(A, B);
// where A, B are seeds in the failed run log.
//
// When the same timestamp is used in multiple transactions
// the order of records when executed in parallel WAL writing is not guaranteed.
// The creates failures in tests that assume that the order of records is preserved.
// There are already measures to prevent invalid data generation, but it still can happen.
// In order to verify that the test is not broken we check that there are no duplicate
// timestamps for the record where the comparison fails.
public class WalWriterFuzzTest extends AbstractFuzzTest {

    @Before
    public void setUp() {
        super.setUp();
        // We disable mixed I/O on some OSes and FSes (wink-wink Windows).
        node1.setProperty(PropertyKey.DEBUG_CAIRO_O3_COLUMN_MEMORY_SIZE, 512 * 1024);
        setFuzzProperties(100, 1000, 2);
    }

    @Test
    public void testChunkedSequencerWalTransactionQueries() throws Exception {
        int chunkSize = TestUtils.generateRandom(LOG).nextInt(100) + 1;
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, chunkSize);
        chunkSize = node1.getConfiguration().getDefaultSeqPartTxnCount();

        ddl("create table chunk_seq (\n" +
                "  x long,\n" +
                "  ts timestamp\n" +
                ") timestamp(ts) PARTITION by day WAL");
        insert("insert batch 2 into chunk_seq \n" +
                "  select x, timestamp_sequence('2024-01-01', 312312) from long_sequence(1000)");


        runWalPurgeJob();

        int expectedTxnCount = 500;
        assertSql("count\n" +
                expectedTxnCount + "\n", "select count(*) from wal_transactions('chunk_seq')");

        drainWalQueue();

        assertSql("count\n" +
                expectedTxnCount + "\n", "select count(*) from wal_transactions('chunk_seq')");


        runWalPurgeJob();

        assertSql("count\n" +
                (expectedTxnCount - (expectedTxnCount - 1) / chunkSize * chunkSize) + "\n", "select count(*) from wal_transactions('chunk_seq')");
    }

    @Test
    public void testChunkedSequencerWriting() throws Exception {
        Rnd rnd = generateRandom(LOG);
        fuzzer.setFuzzCounts(false, 5_000, 200, 20, 10, 20, rnd.nextInt(10), 5, 2);
        setFuzzProperties(rnd);
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);
        Assert.assertEquals(10, node1.getConfiguration().getDefaultSeqPartTxnCount());
        runFuzz(rnd);
    }

    @Test
    public void testSimpleDataTransaction() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setFuzzProbabilities(0, 0.2, 0.1, 0, 0, 0, 0, 1.0, 0.01, 0.01, 0.0);
        setFuzzCounts(rnd.nextBoolean(), rnd.nextInt(10_000_000),
                rnd.nextInt(1500), 20, 10, 200, 0, 1
        );
        runFuzz(rnd);
    }

    @Test
    public void testWalAddRemoveCommitDropFuzz() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setFuzzProbabilities(0.05, 0.2, 0.1, 0.005, 0.05, 0.05, 0.05, 1.0, 0.05, 0.01, 1.0);
        setFuzzCounts(true, 100_000, 500, 20, 1000, 20, 100_000, 5);
        setFuzzProperties(rnd);
        runFuzz(rnd);
    }

    @Test
    public void testWalAddRemoveCommitFuzzInOrder() throws Exception {
        setFuzzProbabilities(0.05, 0.2, 0.1, 0.005, 0.05, 0.05, 0.05, 1.0, 0.01, 0.01, 0.0);
        setFuzzCounts(false, 1_000_000, 500, 20, 1000, 20, 0, 10);
        runFuzz(generateRandom(LOG));
    }

    @Test
    public void testWalAddRemoveCommitFuzzO3() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setFuzzProbabilities(0.05, 0.2, 0.1, 0.005, 0.05, 0.05, 0.05, 1.0, 0.05, 0.01, 0.0);
        setFuzzCounts(true, 100_000, 500, 20, 1000, 20, 100_000, 5);
        setFuzzProperties(rnd);
        runFuzz(rnd);
    }

    @Test
    public void testWalApplyEjectsMultipleTables() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setFuzzProperties(rnd.nextLong(50), getRndO3PartitionSplit(rnd), getRndO3PartitionSplitMaxCount(rnd), getMaxWalSize(rnd), getMaxWalFdCache(rnd));
        int tableCount = Math.max(2, rnd.nextInt(3));
        fullRandomFuzz(rnd, tableCount);
    }

    @Test
    public void testWalMetadataChangeHeavy() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setFuzzProbabilities(0.05, 0.2, 0.1, 0.005, 0.25, 0.25, 0.25, 1.0, 0.01, 0.01, 0.0);
        setFuzzCounts(false, 50_000, 100, 20, 1000, 1000, 100, 5);
        setFuzzProperties(rnd);
        runFuzz(rnd);
    }

    @Test
    public void testWalMetadataChangeHeavyCrashRepro() throws Exception {
        Rnd rnd = fuzzer.generateRandom(LOG, 605244862282L, 1711936717971L);
        setFuzzProbabilities(0.05, 0.2, 0.1, 0.005, 0.25, 0.25, 0.25, 1.0, 0.01, 0.01, 0.0);
        setFuzzCounts(false, 50_000, 100, 20, 1000, 1000, 100, 5);
        setFuzzProperties(rnd);
        runFuzz(rnd);
    }

    @Test
    public void testWalSmallWalFdReuse() throws Exception {
        Rnd rnd = generateRandom(LOG);
        fuzzer.setFuzzCounts(false, 100_000, 50, 20, 10, 20, 0, 5, 2);
        setFuzzProperties(rnd.nextLong(MAX_WAL_APPLY_TIME_PER_TABLE_CEIL), getRndO3PartitionSplit(rnd), getRndO3PartitionSplitMaxCount(rnd), getMaxWalSize(rnd), 1);
        runFuzz(rnd);
    }

    @Test
    public void testWalSmallWalLag() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setFuzzProperties(rnd);
        fullRandomFuzz(rnd);
    }

    @Test
    public void testWalWriteEqualTimestamp() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_QUICKSORT_ENABLED, true);
        Rnd rnd = generateRandom(LOG);
        setFuzzProbabilities(0, 0, 0, 0, 0, 0, 0, 1, 0, 0.5, 0.0);
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
        setRandomAppendPageSize(rnd);
        setFuzzProperties(rnd);
        fullRandomFuzz(rnd);
    }

    @Test
    public void testWalWriteFullRandomMultipleTables() throws Exception {
        Rnd rnd = generateRandom(LOG);
        int tableCount = Math.max(2, rnd.nextInt(4));
        setFuzzProperties(rnd);
        fullRandomFuzz(rnd, tableCount);
    }

    @Test
    public void testWalWriteManySmallTransactions() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_QUICKSORT_ENABLED, true);
        Rnd rnd = generateRandom(LOG);
        setFuzzProbabilities(0, 0, 0, 0, 0, 0, 0, 1, 0, 0.01, 0.0);
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
        setRandomAppendPageSize(rnd);
        int tableCount = 3;
        setFuzzProbabilities(0, 0, 0, 0, 0, 0, 0, 1, 0.001, 0.01, 0.0);
        setFuzzCounts(false, 500_000, 5_000, 10, 10, 5500, 0, 1);
        String tableNameBase = getTestName();
        runFuzz(rnd, tableNameBase, tableCount);
    }

    @Test
    public void testWalWriteRollbackHeavy() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setFuzzProbabilities(0.5, 0.5, 0.1, 0.5, 0.05, 0.05, 0.05, 1.0, 0.01, 0.01, 0.0);
        setFuzzCounts(rnd.nextBoolean(), 10_000, 300, 20, 1000, 1000, 100, 3);
        runFuzz(rnd);
    }

    @Test
    public void testWalWriteRollbackTruncateHeavy() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setFuzzProbabilities(0.5, 0.5, 0.1, 0.5, 0.05, 0.05, 0.05, 1.0, 0.15, 0.01, 0.0);
        setFuzzCounts(rnd.nextBoolean(), 300, 20, 20, 1000, 1000, 100, 3);
        runFuzz(rnd);
    }

    @Test
    public void testWalWriteTinyO3Memory() throws Exception {
        final int o3MemorySize = 256;
        node1.setProperty(PropertyKey.DEBUG_CAIRO_O3_COLUMN_MEMORY_SIZE, o3MemorySize);
        Assert.assertEquals(o3MemorySize, node1.getConfiguration().getO3ColumnMemorySize());
        Rnd rnd = generateRandom(LOG);
        setFuzzProbabilities(0, 0.2, 0.1, 0, 0, 0, 0, 1.0, 0.01, 0.01, 0.0);
        setFuzzCounts(true, 100_000, 10, 10, 10, 10, 50, 1);
        runFuzz(rnd, getTestName(), 1);
    }

    @Test
    public void testWriteO3DataOnlyBig() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setFuzzProbabilities(0, 0, 0, 0, 0, 0, 0, 1.0, 0.01, 0.01, 0.0);
        setFuzzCounts(true, 1_000_000, 500, 20, 1000, 1000, 100, 20);
        setFuzzProperties(rnd);
        runFuzz(rnd);
    }
}
