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

import io.questdb.cairo.*;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.log.Log;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.mp.TestWorkerPool;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

public class AbstractFuzzTest extends AbstractCairoTest {
    public final static int MAX_WAL_APPLY_O3_SPLIT_PARTITION_CEIL = 20000;
    public final static int MAX_WAL_APPLY_O3_SPLIT_PARTITION_MIN = 200;
    protected final FuzzRunner fuzzer = new FuzzRunner();
    protected final WorkerPool sharedWorkerPool = new TestWorkerPool(4, metrics);

    public static int getRndO3PartitionSplit(Rnd rnd) {
        return MAX_WAL_APPLY_O3_SPLIT_PARTITION_MIN + rnd.nextInt(MAX_WAL_APPLY_O3_SPLIT_PARTITION_CEIL - MAX_WAL_APPLY_O3_SPLIT_PARTITION_MIN);
    }

    public static int getRndO3PartitionSplitMaxCount(Rnd rnd) {
        return 1 + rnd.nextInt(2);
    }

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractCairoTest.setUpStatic();
    }

    public void applyWal(ObjList<FuzzTransaction> transactions, String tableName, int walWriterCount, Rnd applyRnd) {
        fuzzer.applyWal(transactions, tableName, walWriterCount, applyRnd);
    }

    public Rnd generateRandom(Log log) {
        return fuzzer.generateRandom(log);
    }

    public Rnd generateRandom(Log log, long s0, long s1) {
        return fuzzer.generateRandom(log, s0, s1);
    }

    public ObjList<FuzzTransaction> generateSet(Rnd rnd, TableRecordMetadata metadata, long start, long end, String tableName) {
        return fuzzer.generateSet(rnd, metadata, start, end, tableName);
    }

    @Before
    public void setUp() {
        super.setUp();
        fuzzer.withDb(engine, sqlExecutionContext);
        fuzzer.clearSeeds();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        sharedWorkerPool.halt();
        fuzzer.after();
    }

    private static void setZeroWalPurgeInterval() {
        node1.getConfigurationOverrides().setWalPurgeInterval(0);
    }

    protected void fullRandomFuzz(Rnd rnd) throws Exception {
        fuzzer.setFuzzProbabilities(
                0.5 * rnd.nextDouble(),
                rnd.nextDouble(),
                rnd.nextDouble(),
                0.5 * rnd.nextDouble(),
                rnd.nextDouble(),
                rnd.nextDouble(),
                rnd.nextDouble(),
                rnd.nextDouble(),
                0.1 * rnd.nextDouble(), 0.01,
                rnd.nextDouble()
        );

        fuzzer.setFuzzCounts(
                rnd.nextBoolean(),
                rnd.nextInt(2_000_000),
                rnd.nextInt(1000),
                rnd.nextInt(1000),
                rnd.nextInt(1000),
                rnd.nextInt(1000),
                rnd.nextInt(1_000_000),
                5 + rnd.nextInt(10)
        );

        assertMemoryLeak(() -> fuzzer.runFuzz(getTestName(), rnd));
    }

    protected void fullRandomFuzz(Rnd rnd, int tableCount) throws Exception {
        assertMemoryLeak(() -> fuzzer.runFuzz(rnd, getTestName(), tableCount, true, true));
    }

    protected String[] generateSymbols(Rnd rnd, int totalSymbols, int strLen, String baseSymbolTableName) {
        String[] symbols = new String[totalSymbols];
        int symbolIndex = 0;

        try (TableReader reader = getReader(baseSymbolTableName)) {
            TableReaderMetadata metadata = reader.getMetadata();
            for (int i = 0; i < metadata.getColumnCount(); i++) {
                int columnType = metadata.getColumnType(i);
                if (ColumnType.isSymbol(columnType)) {
                    SymbolMapReader symbolReader = reader.getSymbolMapReader(i);
                    for (int sym = 0; symbolIndex < totalSymbols && sym < symbolReader.getSymbolCount() - 1; sym++) {
                        symbols[symbolIndex++] = Chars.toString(symbolReader.valueOf(sym));
                    }
                }
            }
        }

        for (; symbolIndex < totalSymbols; symbolIndex++) {
            symbols[symbolIndex] = strLen > 0 ? Chars.toString(rnd.nextChars(rnd.nextInt(strLen))) : "";
        }
        return symbols;
    }

    protected String getTestName() {
        return testName.getMethodName().replace('[', '_').replace(']', '_');
    }

    protected void runFuzz(Rnd rnd) throws Exception {
        assertMemoryLeak(() -> {
            O3Utils.setupWorkerPool(sharedWorkerPool, engine, null);
            sharedWorkerPool.start(LOG);

            try {
                configOverrideO3ColumnMemorySize(rnd.nextInt(16 * 1024 * 1024));
                setZeroWalPurgeInterval();
                fuzzer.runFuzz(getTestName(), rnd);
            } finally {
                sharedWorkerPool.halt();
            }
        });
    }

    protected void runFuzz(Rnd rnd, String tableNameBase, int tableCount, boolean randomiseProbs, boolean randomiseCounts) throws Exception {
        assertMemoryLeak(() -> {
            O3Utils.setupWorkerPool(sharedWorkerPool, engine, null);
            sharedWorkerPool.start(LOG);

            try {
                setZeroWalPurgeInterval();
                fuzzer.runFuzz(rnd, tableNameBase, tableCount, randomiseProbs, randomiseCounts);
            } finally {
                sharedWorkerPool.halt();
            }
        });
    }

    protected void setFuzzCounts(boolean isO3, int fuzzRowCount, int transactionCount, int strLen, int symbolStrLenMax, int symbolCountMax, int initialRowCount, int partitionCount) {
        fuzzer.setFuzzCounts(isO3, fuzzRowCount, transactionCount, strLen, symbolStrLenMax, symbolCountMax, initialRowCount, partitionCount);
    }

    protected void setFuzzProbabilities(double cancelRowsProb, double notSetProb, double nullSetProb, double rollbackProb, double collAddProb, double collRemoveProb, double colRenameProb, double dataAddProb, double truncateProb, double equalTsRowsProb, double tableDropProb) {
        fuzzer.setFuzzProbabilities(cancelRowsProb, notSetProb, nullSetProb, rollbackProb, collAddProb, collRemoveProb, colRenameProb, dataAddProb, truncateProb, equalTsRowsProb, tableDropProb);
    }

    protected void setFuzzProperties(long maxApplyTimePerTable, long splitPartitionThreshold, int o3PartitionSplitMaxCount) {
        node1.getConfigurationOverrides().setWalApplyTableTimeQuota(maxApplyTimePerTable);
        node1.getConfigurationOverrides().setPartitionO3SplitThreshold(splitPartitionThreshold);
        node1.getConfigurationOverrides().setO3PartitionSplitMaxCount(o3PartitionSplitMaxCount);
    }

    protected void setRandomAppendPageSize(Rnd rnd) {
        int minPage = 18;
        dataAppendPageSize = 1L << (minPage + rnd.nextInt(22 - minPage)); // MAX page size 4Mb
        LOG.info().$("dataAppendPageSize=").$(dataAppendPageSize).$();
    }
}
