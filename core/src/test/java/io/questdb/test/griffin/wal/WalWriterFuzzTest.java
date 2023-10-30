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

import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

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
@RunWith(Parameterized.class)
public class WalWriterFuzzTest extends AbstractFuzzTest {

    protected boolean allowMixedIO;

    public WalWriterFuzzTest(IOMode ioMode) {
        this.allowMixedIO = (ioMode == IOMode.ALLOW_MIXED_IO);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {IOMode.ALLOW_MIXED_IO}, {IOMode.NO_MIXED_IO}
        });
    }

    @Before
    public void setUp() {
        super.setUp();
        // We disable mixed I/O on some OSes and FSes (wink-wink Windows).
        boolean mixedIOSupported = configuration.getFilesFacade().allowMixedIO(root);
        Assume.assumeFalse(allowMixedIO && !mixedIOSupported);

        configOverrideWriterMixedIOEnabled(allowMixedIO);
        configOverrideO3ColumnMemorySize(512 * 1024);
        setFuzzProperties(100, 1000, 2);
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
        setFuzzProperties(rnd.nextLong(MAX_WAL_APPLY_TIME_PER_TABLE_CEIL), getRndO3PartitionSplit(rnd), getRndO3PartitionSplitMaxCount(rnd));
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
        setFuzzProperties(rnd.nextLong(MAX_WAL_APPLY_TIME_PER_TABLE_CEIL), getRndO3PartitionSplit(rnd), getRndO3PartitionSplitMaxCount(rnd));
        runFuzz(rnd);
    }

    @Test
    public void testWalApplyEjectsMultipleTables() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setFuzzProperties(rnd.nextLong(50), getRndO3PartitionSplit(rnd), getRndO3PartitionSplitMaxCount(rnd));
        int tableCount = Math.max(2, rnd.nextInt(3));
        fullRandomFuzz(rnd, tableCount);
    }

    @Test
    public void testWalMetadataChangeHeavy() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setFuzzProbabilities(0.05, 0.2, 0.1, 0.005, 0.25, 0.25, 0.25, 1.0, 0.01, 0.01, 0.0);
        setFuzzCounts(false, 50_000, 100, 20, 1000, 1000, 100, 5);
        setFuzzProperties(rnd.nextLong(MAX_WAL_APPLY_TIME_PER_TABLE_CEIL), getRndO3PartitionSplit(rnd), getRndO3PartitionSplitMaxCount(rnd));
        runFuzz(rnd);
    }

    @Test
    public void testWalWriteEqualTimestamp() throws Exception {
        configOverrideO3QuickSortEnabled(true);
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
        setFuzzProperties(rnd.nextLong(MAX_WAL_APPLY_TIME_PER_TABLE_CEIL), getRndO3PartitionSplit(rnd), getRndO3PartitionSplitMaxCount(rnd));
        runFuzz(rnd, getTestName(), 1, false, false);
    }

    @Test
    public void testWalWriteFullRandom() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setRandomAppendPageSize(rnd);
        setFuzzProperties(rnd.nextLong(MAX_WAL_APPLY_TIME_PER_TABLE_CEIL), getRndO3PartitionSplit(rnd), getRndO3PartitionSplitMaxCount(rnd));
        fullRandomFuzz(rnd);
    }

    @Test
    public void testWalWriteFullRandomMultipleTables() throws Exception {
        Rnd rnd = generateRandom(LOG);
        int tableCount = Math.max(2, rnd.nextInt(4));
        setFuzzProperties(rnd.nextLong(MAX_WAL_APPLY_TIME_PER_TABLE_CEIL), getRndO3PartitionSplit(rnd), getRndO3PartitionSplitMaxCount(rnd));
        fullRandomFuzz(rnd, tableCount);
    }

    @Test
    public void testWalWriteManySmallTransactions() throws Exception {
        configOverrideO3QuickSortEnabled(true);
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
        setFuzzProperties(rnd.nextLong(MAX_WAL_APPLY_TIME_PER_TABLE_CEIL), getRndO3PartitionSplit(rnd), getRndO3PartitionSplitMaxCount(rnd));
        runFuzz(rnd, getTestName(), 1, false, false);
    }

    @Test
    public void testWalWriteManyTablesInOrder() throws Exception {
        configOverrideO3QuickSortEnabled(true);
        Rnd rnd = generateRandom(LOG);
        setRandomAppendPageSize(rnd);
        int tableCount = 3;
        setFuzzProbabilities(0, 0, 0, 0, 0, 0, 0, 1, 0.001, 0.01, 0.0);
        setFuzzCounts(false, 500_000, 5_000, 10, 10, 5500, 0, 1);
        String tableNameBase = getTestName();
        runFuzz(rnd, tableNameBase, tableCount, false, false);
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
        configOverrideO3ColumnMemorySize(o3MemorySize);
        Rnd rnd = generateRandom(LOG);
        setFuzzProbabilities(0, 0.2, 0.1, 0, 0, 0, 0, 1.0, 0.01, 0.01, 0.0);
        setFuzzCounts(true, 100_000, 10, 10, 10, 10, 50, 1);
        runFuzz(rnd, getTestName(), 1, false, false);
        Assert.assertEquals(o3MemorySize, node1.getConfigurationOverrides().getO3ColumnMemorySize());
    }

    @Test
    public void testWriteO3DataOnlyBig() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setFuzzProbabilities(0, 0, 0, 0, 0, 0, 0, 1.0, 0.01, 0.01, 0.0);
        setFuzzCounts(true, 1_000_000, 500, 20, 1000, 1000, 100, 20);
        setFuzzProperties(rnd.nextLong(MAX_WAL_APPLY_TIME_PER_TABLE_CEIL), getRndO3PartitionSplit(rnd), getRndO3PartitionSplitMaxCount(rnd));
        runFuzz(rnd);
    }

    public enum IOMode {
        ALLOW_MIXED_IO, NO_MIXED_IO
    }
}
