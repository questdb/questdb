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
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.log.Log;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.mp.TestWorkerPool;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import static io.questdb.test.cairo.fuzz.FuzzRunner.MAX_WAL_APPLY_TIME_PER_TABLE_CEIL;

public class AbstractFuzzTest extends AbstractCairoTest {
    public final static int MAX_WAL_APPLY_O3_SPLIT_PARTITION_CEIL = 20000;
    public final static int MAX_WAL_APPLY_O3_SPLIT_PARTITION_MIN = 200;
    protected final FuzzRunner fuzzer = new FuzzRunner();
    protected final WorkerPool sharedWorkerPool = new TestWorkerPool(4, node1.getMetrics());

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

    public ObjList<FuzzTransaction> generateSet(
            Rnd rnd,
            TableRecordMetadata sequencerMetadata,
            TableMetadata readerMetadata,
            long start,
            long end,
            String tableName
    ) {
        return fuzzer.generateSet(rnd, sequencerMetadata, readerMetadata, start, end, tableName);
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
        node1.setProperty(PropertyKey.CAIRO_WAL_PURGE_INTERVAL, 0L);
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
                rnd.nextDouble(),
                0.01,
                rnd.nextDouble(),
                0.1 * rnd.nextDouble(),
                rnd.nextDouble(),
                rnd.nextDouble(),
                rnd.nextDouble(),
                0.1 * rnd.nextDouble(),
                0.1 * rnd.nextDouble()
        );

        fuzzer.setFuzzCounts(
                rnd.nextBoolean(),
                rnd.nextInt(2_000_000),
                rnd.nextInt(1000),
                fuzzer.randomiseStringLengths(rnd, 1000),
                rnd.nextInt(1000),
                rnd.nextInt(1000),
                rnd.nextInt(1_000_000),
                5 + rnd.nextInt(10)
        );

        assertMemoryLeak(fuzzer.getFileFacade(), () -> fuzzer.runFuzz(getTestName(), rnd));
    }

    protected void fullRandomFuzz(Rnd rnd, int tableCount) throws Exception {
        assertMemoryLeak(fuzzer.getFileFacade(), () -> fuzzer.runFuzz(rnd, getTestName(), tableCount, true, true));
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

        Utf8StringSink sink = new Utf8StringSink();
        for (; symbolIndex < totalSymbols; symbolIndex++) {
            // Create strings with unicode chars
            sink.clear();
            rnd.nextUtf8Str(rnd.nextInt(strLen), sink);
            symbols[symbolIndex] = strLen > 0 ? Utf8s.toString(sink) : "";
        }
        return symbols;
    }

    protected int getMaxWalFdCache(Rnd rnd) {
        // Generate 0s in 30% of the cases
        // 0 forces to bypass fd cache which is an important case to test
        return Math.max(0, rnd.nextInt(1000) - 300);
    }

    protected long getMaxWalSize(Rnd rnd) {
        return rnd.nextLong(10 * Numbers.SIZE_1MB);
    }

    protected String getTestName() {
        return testName.getMethodName()
                .replace('[', '_')
                .replace(']', '_')
                .replace('=', '_');
    }

    protected void runFuzz(Rnd rnd) throws Exception {
        assertMemoryLeak(fuzzer.getFileFacade(), () -> {
            try {
                WorkerPoolUtils.setupWriterJobs(sharedWorkerPool, engine);
                WorkerPoolUtils.setupAsyncMunmapJob(sharedWorkerPool, engine);
                sharedWorkerPool.start(LOG);

                int size = rnd.nextInt(8 * 1024 * 1024);
                node1.setProperty(PropertyKey.DEBUG_CAIRO_O3_COLUMN_MEMORY_SIZE, size);
                setZeroWalPurgeInterval();
                fuzzer.runFuzz(getTestName(), rnd);
            } finally {
                sharedWorkerPool.halt();
            }
        });
    }

    protected void runFuzz(Rnd rnd, String tableNameBase, int tableCount) throws Exception {
        assertMemoryLeak(fuzzer.getFileFacade(), () -> {
            try {
                WorkerPoolUtils.setupWriterJobs(sharedWorkerPool, engine);
                WorkerPoolUtils.setupAsyncMunmapJob(sharedWorkerPool, engine);
                sharedWorkerPool.start(LOG);

                setZeroWalPurgeInterval();
                fuzzer.runFuzz(rnd, tableNameBase, tableCount, false, false);
            } finally {
                sharedWorkerPool.halt();
            }
        });
    }

    protected void setFuzzCounts(
            boolean isO3,
            int fuzzRowCount,
            int transactionCount,
            int strLen,
            int symbolStrLenMax,
            int symbolCountMax,
            int initialRowCount,
            int partitionCount
    ) {
        fuzzer.setFuzzCounts(isO3, fuzzRowCount, transactionCount, strLen,
                symbolStrLenMax, symbolCountMax, initialRowCount, partitionCount);
    }

    protected void setFuzzProbabilities(
            double cancelRowsProb,
            double notSetProb,
            double nullSetProb,
            double rollbackProb,
            double colAddProb,
            double colRemoveProb,
            double colRenameProb,
            double colTypeChangeProb,
            double dataAddProb,
            double equalTsRowsProb,
            double partitionDropProb,
            double truncateProb,
            double tableDropProb,
            double setTtlProb,
            double replaceProb,
            double symbolAccessProb
    ) {
        fuzzer.setFuzzProbabilities(
                cancelRowsProb, notSetProb, nullSetProb, rollbackProb,
                colAddProb, colRemoveProb, colRenameProb, colTypeChangeProb, dataAddProb,
                equalTsRowsProb, partitionDropProb, truncateProb, tableDropProb, setTtlProb,
                replaceProb, symbolAccessProb
        );
    }

    protected void setFuzzProbabilities(
            double cancelRowsProb,
            double notSetProb,
            double nullSetProb,
            double rollbackProb,
            double colAddProb,
            double colRemoveProb,
            double colRenameProb,
            double colTypeChangeProb,
            double dataAddProb,
            double equalTsRowsProb,
            double partitionDropProb,
            double truncateProb,
            double tableDropProb,
            double setTtlProb,
            double replaceProb,
            double symbolAccessProb,
            double queryProb
    ) {
        fuzzer.setFuzzProbabilities(
                cancelRowsProb, notSetProb, nullSetProb, rollbackProb,
                colAddProb, colRemoveProb, colRenameProb, colTypeChangeProb, dataAddProb,
                equalTsRowsProb, partitionDropProb, truncateProb, tableDropProb, setTtlProb,
                replaceProb, symbolAccessProb, queryProb
        );
    }

    protected void setFuzzProperties(long maxApplyTimePerTable, long splitPartitionThreshold, int o3PartitionSplitMaxCount) {
        node1.setProperty(PropertyKey.CAIRO_WAL_APPLY_TABLE_TIME_QUOTA, maxApplyTimePerTable);
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, splitPartitionThreshold);
        node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, o3PartitionSplitMaxCount);
    }

    protected void setFuzzProperties(
            long maxApplyTimePerTable,
            long splitPartitionThreshold,
            int o3PartitionSplitMaxCount,
            long walMaxLagSize,
            int maxWalFdCache
    ) {
        node1.setProperty(PropertyKey.CAIRO_WAL_APPLY_TABLE_TIME_QUOTA, maxApplyTimePerTable);
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, splitPartitionThreshold);
        node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, o3PartitionSplitMaxCount);
        node1.setProperty(PropertyKey.CAIRO_WAL_MAX_LAG_SIZE, walMaxLagSize);
        node1.setProperty(PropertyKey.CAIRO_WAL_MAX_SEGMENT_FILE_DESCRIPTORS_CACHE, maxWalFdCache);
    }

    protected void setFuzzProperties(Rnd rnd) {
        // Force non-handleable errors to be thrown if block application fails
        node1.setProperty(PropertyKey.DEBUG_WAL_APPLY_BLOCK_FAILURE_NO_RETRY, true);
        node1.setProperty(PropertyKey.CAIRO_WAL_APPLY_TABLE_TIME_QUOTA, rnd.nextLong(MAX_WAL_APPLY_TIME_PER_TABLE_CEIL));
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, getRndO3PartitionSplit(rnd));
        node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, getRndO3PartitionSplitMaxCount(rnd));
        node1.setProperty(PropertyKey.CAIRO_WAL_MAX_LAG_SIZE, getMaxWalSize(rnd));
        node1.setProperty(PropertyKey.CAIRO_WAL_MAX_SEGMENT_FILE_DESCRIPTORS_CACHE, getMaxWalFdCache(rnd));
        node1.setProperty(PropertyKey.CAIRO_WAL_APPLY_LOOK_AHEAD_TXN_COUNT, 1 + rnd.nextInt(200));

        int txnCount = Math.max(10, fuzzer.getTransactionCount());
        long walChunk = Math.max(0, rnd.nextInt((int) (3.5 * txnCount)) - txnCount);
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, walChunk);

        // Make call to move random even if it will not be used.
        // To avoid ZFS runs being very different to the non-ZFS
        // with the same seeds.
        boolean allowMixedIO = rnd.nextBoolean();
        if (configuration.getFilesFacade().allowMixedIO(root)) {
            node1.setProperty(PropertyKey.DEBUG_CAIRO_ALLOW_MIXED_IO, allowMixedIO);
        }
    }

    protected void setRandomAppendPageSize(Rnd rnd) {
        int minPage = 18;
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, 1L << (minPage + rnd.nextInt(22 - minPage))); // MAX page size 4Mb
        long dataAppendPageSize = configuration.getDataAppendPageSize();
        LOG.info().$("dataAppendPageSize=").$(dataAppendPageSize).$();
    }
}
