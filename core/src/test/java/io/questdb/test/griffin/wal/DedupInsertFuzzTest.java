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

import io.questdb.cairo.O3Utils;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableReaderRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.StringSink;
import io.questdb.test.fuzz.FuzzInsertOperation;
import io.questdb.test.fuzz.FuzzStableInsertOperation;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.fuzz.FuzzTransactionOperation;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DedupInsertFuzzTest extends AbstractFuzzTest {
    @Test
    public void testDedupWithRandomShiftAndStep() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            createEmptyTable(tableName, "DEDUP upsert keys(ts)");

            ObjList<FuzzTransaction> transactions = new ObjList<>();
            Rnd rnd = generateRandom(LOG);
            long initialDelta = Timestamps.MINUTE_MICROS * 15;
            int initialCount = 4 * 24 * 5;
            generateInsertsTransactions(
                    transactions,
                    1,
                    "2020-02-24T04:30",
                    initialDelta,
                    initialCount,
                    1 + rnd.nextInt(1),
                    null,
                    rnd
            );
            applyWal(transactions, tableName, 1, rnd);

            transactions.clear();

            double deltaMultiplier = rnd.nextBoolean() ? (1 << rnd.nextInt(4)) : 1.0 / (1 << rnd.nextInt(4));
            long delta = (long) (initialDelta * deltaMultiplier);
            long shift = (-100 + rnd.nextLong((long) (initialCount / deltaMultiplier + 150))) * delta;
            String from = Timestamps.toUSecString(parseFloorPartialTimestamp("2020-02-24") + shift);
            int count = rnd.nextInt((int) (initialCount / deltaMultiplier + 1) * 2);
            int rowsWithSameTimestamp = 1 + rnd.nextInt(2);

            generateInsertsTransactions(
                    transactions,
                    2,
                    from,
                    delta,
                    count,
                    rowsWithSameTimestamp,
                    null,
                    rnd
            );

            applyWal(transactions, tableName, 1, rnd);
            validateNoTimestampDuplicates(tableName, from, delta, count, null, 1);
        });
    }

    @Test
    @Ignore
    public void testDedupWithRandomShiftAndStepAndSymbolKey() throws Exception {
        assertMemoryLeak(() -> {
            Assume.assumeTrue(configuration.isMultiKeyDedupEnabled());
            Rnd rnd = generateRandom(LOG);

            String tableName = testName.getMethodName();
            compile(
                    "create table " + tableName +
                            " (ts timestamp, commit int, s symbol) " +
                            " , index(s) timestamp(ts) partition by DAY WAL "
                            + " DEDUP UPSERT KEYS(ts, s)"
                    ,
                    sqlExecutionContext
            );

            ObjList<FuzzTransaction> transactions = new ObjList<>();
            long initialDelta = Timestamps.MINUTE_MICROS * 15;
            int rndCount = rnd.nextInt(10);
            List<String> distinctSymbols = Arrays.stream(generateSymbols(rnd, 1 + rndCount, 4, tableName)).distinct()
                    .collect(Collectors.toList());
            String[] symbols = new String[distinctSymbols.size()];
            distinctSymbols.toArray(symbols);
            String[] initialSymbols = symbols.length == 1
                    ? symbols
                    : Arrays.copyOf(symbols, 1 + rnd.nextInt(symbols.length - 1));

            generateInsertsTransactions(
                    transactions,
                    1,
                    "2020-02-24T04:30",
                    initialDelta,
                    4 * 24 * 5,
                    1 + rnd.nextInt(1),
                    initialSymbols,
                    rnd
            );

            applyWal(transactions, tableName, 1, rnd);

            transactions.clear();
            long shift = rnd.nextLong(4 * 24 * 5) * Timestamps.MINUTE_MICROS * 15 +
                    rnd.nextLong(15) * Timestamps.MINUTE_MICROS;
            String from = Timestamps.toUSecString(parseFloorPartialTimestamp("2020-02-24") + shift);
            long delta = Timestamps.MINUTE_MICROS;
            int count = rnd.nextInt(48) * 60;
            int rowsWithSameTimestamp = 1 + rnd.nextInt(2);
            generateInsertsTransactions(
                    transactions,
                    2,
                    from,
                    delta,
                    count,
                    rowsWithSameTimestamp,
                    symbols,
                    rnd
            );

            applyWal(transactions, tableName, 1, rnd);
            validateNoTimestampDuplicates(tableName, from, delta, count, symbols, 1);
        });
    }

    @Test
    public void testDedupWithRandomShiftAndStepWithExistingDups() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            createEmptyTable(tableName, "");

            ObjList<FuzzTransaction> transactions = new ObjList<>();
            Rnd rnd = generateRandom(LOG);
            long initialDelta = Timestamps.MINUTE_MICROS * 15;
            int initialCount = 4 * 24 * 5;
            int initialDuplicates = 2 + rnd.nextInt(5);
            transactions.add(
                    generateInsertsTransactions(
                            1,
                            "2020-02-24T04:30",
                            initialDelta,
                            initialCount,
                            initialDuplicates,
                            null,
                            rnd
                    )
            );
            applyWal(transactions, tableName, 1, rnd);
            compile("alter table " + tableName + " dedup upsert keys(ts)");

            transactions.clear();

            double deltaMultiplier = rnd.nextBoolean() ? (1 << rnd.nextInt(4)) : 1.0 / (1 << rnd.nextInt(4));
            long delta = (long) (initialDelta * deltaMultiplier);
            long shift = (-100 + rnd.nextLong((long) (initialCount / deltaMultiplier + 150))) * delta;
            String from = Timestamps.toUSecString(parseFloorPartialTimestamp("2020-02-24") + shift);
            int count = rnd.nextInt((int) (initialCount / deltaMultiplier + 1) * 2);
            int rowsWithSameTimestamp = 1 + rnd.nextInt(2);

            transactions.add(
                    generateInsertsTransactions(
                            2,
                            from,
                            delta,
                            count,
                            rowsWithSameTimestamp,
                            null,
                            rnd
                    )
            );

            applyWal(transactions, tableName, 1, rnd);
            validateNoTimestampDuplicates(tableName, from, delta, count, null, initialDuplicates);
        });
    }

    @Test
    public void testDedupWithRandomShiftWithColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            createEmptyTable(tableName, "DEDUP upsert keys(ts)");

            ObjList<FuzzTransaction> transactions = new ObjList<>();
            Rnd rnd = generateRandom(LOG);
            long initialDelta = Timestamps.MINUTE_MICROS * 15;
            int initialCount = 2 * 24 * 5;
            generateInsertsTransactions(
                    transactions,
                    1,
                    "2020-02-24T04:30",
                    initialDelta,
                    initialCount,
                    1 + rnd.nextInt(1),
                    null,
                    rnd
            );
            String[] symbols = generateSymbols(rnd, 20, 4, tableName);
            applyWal(transactions, tableName, 1, rnd);
            transactions.clear();

            compile("alter table " + tableName + " add s symbol index", sqlExecutionContext);

            double deltaMultiplier = rnd.nextBoolean() ? (1 << rnd.nextInt(4)) : 1.0 / (1 << rnd.nextInt(4));
            long delta = (long) (initialDelta * deltaMultiplier);
            long shift = (-100 + rnd.nextLong((long) (initialCount / deltaMultiplier + 150))) * delta;
            String from = Timestamps.toUSecString(parseFloorPartialTimestamp("2020-02-24") + shift);
            int count = rnd.nextInt((int) (initialCount / deltaMultiplier + 1) * 2);
            int rowsWithSameTimestamp = 1 + rnd.nextInt(2);

            generateInsertsTransactions(
                    transactions,
                    2,
                    from,
                    delta,
                    count,
                    rowsWithSameTimestamp,
                    symbols,
                    rnd
            );

            applyWal(transactions, tableName, 1, rnd);
            validateNoTimestampDuplicates(tableName, from, delta, count, null, 1);
        });
    }

    @Test
    public void testWalWriteFullRandomDedupRepeat() throws Exception {
        Rnd rnd = generateRandom(LOG, 249149246868791L, 1687975427577L);
        setFuzzProbabilities(
                0,
                rnd.nextDouble(),
                rnd.nextDouble(),
                0.5 * rnd.nextDouble(),
                rnd.nextDouble() / 100,
                rnd.nextDouble() / 100,
                rnd.nextDouble(),
                rnd.nextDouble(),
                0.1 * rnd.nextDouble(),
                0.0
        );

        setFuzzCounts(
                rnd.nextBoolean(),
                rnd.nextInt(1000),
                rnd.nextInt(1000),
                rnd.nextInt(1000),
                rnd.nextInt(1000),
                rnd.nextInt(1000),
                rnd.nextInt(1_000_000),
                5 + rnd.nextInt(10)
        );

        runFuzzWithRepeatDedup(rnd);
    }

    private void assertAllSymbolsSet(
            boolean[] foundSymbols,
            String[] symbols,
            long timestamp
    ) {
        for (int i = 0; i < foundSymbols.length; i++) {
            if (!foundSymbols[i]) {
                CharSequence symbol = symbols[i];
                Assert.fail("Symbol '" + symbol + "' not found for timestamp " + Timestamps.toUSecString(timestamp));
            }
            foundSymbols[i] = false;
        }
    }

    private void createEmptyTable(String tableName, String dedupOption) throws SqlException {
        compile("create table " + tableName + " (ts timestamp, commit int) timestamp(ts) partition by DAY WAL " + dedupOption
                , sqlExecutionContext);
    }

    private ObjList<FuzzTransaction> duplicateInserts(ObjList<FuzzTransaction> transactions, Rnd rnd) {
        ObjList<FuzzTransaction> result = new ObjList<>();
        FuzzTransaction prevInsertTrans = null;

        for (int i = 0; i < transactions.size(); i++) {
            FuzzTransaction transaction = transactions.getQuick(i);

            if (!transaction.rollback && transaction.operationList.size() > 1) {
                int size = transaction.operationList.size();
                FuzzTransaction duplicateTrans = new FuzzTransaction();
                for (int op = 0; op < size; op++) {
                    int dupStep = Math.max(2, transaction.operationList.size() / (1 + rnd.nextInt(10)));
                    FuzzTransactionOperation operation = transaction.operationList.getQuick(op);
                    if (operation instanceof FuzzInsertOperation) {
                        FuzzInsertOperation insertOperation = (FuzzInsertOperation) operation;
                        if (op % dupStep == 1) {
                            FuzzInsertOperation duplicate = new FuzzInsertOperation(
                                    rnd.nextLong(),
                                    rnd.nextLong(),
                                    insertOperation.getTimestamp(),
                                    rnd.nextDouble(),
                                    rnd.nextDouble(),
                                    rnd.nextDouble(),
                                    insertOperation.getStrLen(),
                                    insertOperation.getSymbols()
                            );
                            if (prevInsertTrans != null && rnd.nextBoolean()) {
                                prevInsertTrans.operationList.add(duplicate);
                            } else {
                                duplicateTrans.operationList.add(duplicate);
                            }
                        }
                    }
                    duplicateTrans.operationList.add(operation);
                }

                result.add(duplicateTrans);
                prevInsertTrans = duplicateTrans;
            } else {
                result.add(transaction);
            }
        }
        return result;
    }

    private void generateInsertsTransactions(
            ObjList<FuzzTransaction> transactions,
            int commit,
            String from,
            long delta,
            int count,
            int rowsWithSameTimestamp,
            String[] symbols,
            Rnd rnd
    ) {
        FuzzTransaction transaction = new FuzzTransaction();
        transactions.add(transaction);
        long timestamp = parseFloorPartialTimestamp(from);
        for (int i = 0; i < count; i++) {
            for (int j = 0; j < rowsWithSameTimestamp; j++) {
                if (symbols != null && symbols.length > 0) {
                    for (int s = 0; s < symbols.length; s++) {
                        transaction.operationList.add(new FuzzStableInsertOperation(timestamp, commit, symbols[s]));
                    }
                } else {
                    transaction.operationList.add(new FuzzStableInsertOperation(timestamp, commit));
                }
            }
            timestamp += delta;
        }
        if (rnd.nextBoolean()) {
            shuffle(transaction.operationList, rnd);
        }
    }

    private long parseFloorPartialTimestamp(String from) {
        try {
            return IntervalUtils.parseFloorPartialTimestamp(from);
        } catch (NumericException e) {
            throw new RuntimeException(e);
        }
    }

    private void runFuzzWithRepeatDedup(Rnd rnd) throws Exception {
        configOverrideO3ColumnMemorySize(rnd.nextInt(16 * 1024 * 1024));

        assertMemoryLeak(() -> {
            String tableNameBase = getTestName();
            String tableNameWal = tableNameBase + "_wal";
            String tableNameNoWal = tableNameBase + "_nonwal";

            createInitialTable(tableNameWal, true, initialRowCount);
            createInitialTable(tableNameNoWal, false, initialRowCount);

            ObjList<FuzzTransaction> transactions;
            try (TableReader reader = getReader(tableNameWal)) {
                TableReaderMetadata metadata = reader.getMetadata();

                long start = IntervalUtils.parseFloorPartialTimestamp("2022-02-24T17");
                long end = start + partitionCount * Timestamps.DAY_MICROS;
                transactions = generateSet(rnd, metadata, start, end, tableNameNoWal);
            }

            transactions = uniqueInserts(transactions);
            O3Utils.setupWorkerPool(sharedWorkerPool, engine, null, null);
            sharedWorkerPool.start(LOG);

            try {
                applyNonWal(transactions, tableNameNoWal, rnd);

                ObjList<FuzzTransaction> transactionsWithDups = duplicateInserts(transactions, rnd);
                compile("alter table " + tableNameWal + " dedup upsert keys(ts)", sqlExecutionContext);
                applyWal(transactionsWithDups, tableNameWal, 1 + rnd.nextInt(4), rnd);

                String limit = "";// limit 5190, 5205";
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableNameNoWal + limit, tableNameWal + limit, LOG);
                assertRandomIndexes(tableNameNoWal, tableNameWal, rnd);
            } finally {
                sharedWorkerPool.halt();
            }
        });
    }

    private void shuffle(ObjList<FuzzTransactionOperation> operationList, Rnd rnd) {
        for (int i = operationList.size(); i > 1; i--) {
            swap(operationList, i - 1, rnd.nextInt(i));
        }
    }

    private void swap(ObjList<FuzzTransactionOperation> operationList, int i, int j) {
        FuzzTransactionOperation tmp = operationList.getQuick(i);
        operationList.setQuick(i, operationList.getQuick(j));
        operationList.setQuick(j, tmp);
    }

    private ObjList<FuzzTransaction> uniqueInserts(ObjList<FuzzTransaction> transactions) {
        ObjList<FuzzTransaction> result = new ObjList<>();
        LongHashSet uniqueTimestamps = new LongHashSet();

        for (int i = 0; i < transactions.size(); i++) {
            FuzzTransaction transaction = transactions.getQuick(i);
            if (!transaction.rollback) {
                FuzzTransaction unique = new FuzzTransaction();
                for (int j = 0; j < transaction.operationList.size(); j++) {
                    FuzzTransactionOperation operation = transaction.operationList.getQuick(j);
                    if (operation instanceof FuzzInsertOperation) {
                        if (uniqueTimestamps.add(((FuzzInsertOperation) operation).getTimestamp())) {
                            unique.operationList.add(operation);
                        }
                    } else {
                        unique.operationList.add(operation);
                    }
                }
                result.add(unique);
            } else {
                result.add(transaction);
            }
        }
        return result;
    }

    private void validateNoTimestampDuplicates(
            String tableName,
            String from,
            long delta,
            long commit2Count,
            String[] symbols,
            int existingDups
    ) {

        LOG.info().$("Validating no timestamp duplicates [from=").$(from)
                .$(", delta=").$(delta)
                .$(", commit2Count=").$(commit2Count)
                .I$();

        long lastTimestamp = Long.MIN_VALUE;
        long fromTimestamp = parseFloorPartialTimestamp(from);
        long toTimestamp = fromTimestamp + delta * commit2Count;
        StringSink sink = new StringSink();
        boolean started = false;
        ObjIntHashMap<CharSequence> symbolSet = new ObjIntHashMap<>();
        boolean[] foundSymbols = null;
        if (symbols != null) {
            for (int i = 0; i < symbols.length; i++) {
                symbolSet.put(symbols[i], i);
            }
            foundSymbols = new boolean[symbols.length];
        }

        try (TableReader rdr = getReader(tableName)) {
            TableReaderRecordCursor cursor = rdr.getCursor();
            Record rec = cursor.getRecord();
            AssertionError fail = null;
            int dups = existingDups;

            while (cursor.hasNext()) {
                try {
                    long timestamp = rec.getTimestamp(0);
                    int commit = rec.getInt(1);
                    if (timestamp >= (fromTimestamp - Timestamps.MINUTE_MICROS * 5) || started) {
                        // Keep printing whole insert time range, regardless of the failures
                        started = true;
                        sink.putISODate(timestamp).put(',').put(commit);
                        if (symbols != null) {
                            sink.put(',').put(rec.getSym(2));
                        }
                        sink.put('\n');
                    }

                    // Until first failure
                    if (fail == null && timestamp < toTimestamp && timestamp >= fromTimestamp) {
                        if (symbols != null) {
                            if (timestamp > lastTimestamp && lastTimestamp >= fromTimestamp) {
                                // New timestamp, check all symbols were set
                                // and reset foundSymbols to false
                                assertAllSymbolsSet(foundSymbols, symbols, lastTimestamp);
                            }
                            CharSequence sym = rec.getSym(2);
                            int symbolIndex = symbolSet.get(sym);
                            if (foundSymbols[symbolIndex]) {
                                Assert.fail("Duplicate timestamp " + Timestamps.toUSecString(timestamp) + " for symbol '" + sym + "'");
                            }
                            foundSymbols[symbolIndex] = true;
                        } else {
                            if (timestamp == lastTimestamp) {
                                if (++dups > existingDups) {
                                    Assert.fail("Duplicate timestamp " + Timestamps.toUSecString(timestamp));
                                }
                            } else {
                                dups = 1;
                            }
                        }

                        if (timestamp < lastTimestamp) {
                            Assert.fail("Out of order timestamp " +
                                    Timestamps.toUSecString(lastTimestamp) +
                                    " followed by " +
                                    Timestamps.toUSecString(timestamp)
                            );
                        }

                        if ((timestamp - fromTimestamp) % delta == 0) {
                            Assert.assertEquals("expected commit at timestamp " + Timestamps.toUSecString(timestamp), 2, commit);
                        }

                        Assert.assertTrue("commit must be 1 or 2", commit > 0);
                    }
                    lastTimestamp = timestamp;

                    if (timestamp > (toTimestamp + Timestamps.MINUTE_MICROS * 5)) {
                        break;
                    }
                } catch (AssertionError e) {
                    // Save failure, keep printing
                    fail = e;
                }
            }

            if (fail != null) {
                System.out.println(sink);
                throw fail;
            }
        }
    }

}
