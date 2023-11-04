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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.log.Log;
import io.questdb.log.LogRecord;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.StringSink;
import io.questdb.test.cairo.LogRecordSinkAdapter;
import io.questdb.test.fuzz.FuzzInsertOperation;
import io.questdb.test.fuzz.FuzzStableInsertOperation;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.fuzz.FuzzTransactionOperation;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.questdb.test.tools.TestUtils.assertEquals;

public class DedupInsertFuzzTest extends AbstractFuzzTest {

    @Test
    public void testDedupWithRandomShiftAndStep() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            createEmptyTable(tableName, "DEDUP upsert keys(ts, commit)");
            compile("alter table " + tableName + " dedup disable");
            compile("alter table " + tableName + " dedup enable upsert keys(ts)");

            ObjList<FuzzTransaction> transactions = new ObjList<>();
            Rnd rnd = generateRandom(LOG);
            long initialDelta = Timestamps.MINUTE_MICROS * 15;
            int initialCount = 4 * 24 * 5;
            generateInsertsTransactions(
                    transactions,
                    1,
                    parseFloorPartialTimestamp("2020-02-24T04:30"),
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
            long from = parseFloorPartialTimestamp("2020-02-24") + shift;
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
    public void testDedupWithRandomShiftAndStepAndSymbolKey() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = generateRandom(LOG);

            String tableName = testName.getMethodName();
            compile(
                    "create table " + tableName +
                            " (ts timestamp, commit int, s symbol) " +
                            " , index(s) timestamp(ts) partition by DAY WAL "
                            + " DEDUP UPSERT KEYS(ts, s)"
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
            int initialDuplicates = 1 + rnd.nextInt(1);

            generateInsertsTransactions(
                    transactions,
                    1,
                    parseFloorPartialTimestamp("2020-02-24T04:30"),
                    initialDelta,
                    4 * 24 * 5,
                    initialDuplicates,
                    initialSymbols,
                    rnd
            );

            int transactionCount = 1 + rnd.nextInt(3);
            splitTransactionInserts(transactions, transactionCount, rnd);
            applyWal(transactions, tableName, 1, rnd);

            transactions.clear();
            long shift = rnd.nextLong(4 * 24 * 5) * Timestamps.MINUTE_MICROS * 15 +
                    rnd.nextLong(15) * Timestamps.MINUTE_MICROS;
            long from = parseFloorPartialTimestamp("2020-02-24") + shift;
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

            transactionCount = 1 + rnd.nextInt(3);
            splitTransactionInserts(transactions, transactionCount, rnd);
            applyWal(transactions, tableName, 1, rnd);

            validateNoTimestampDuplicates(tableName, from, delta, count, symbols, 1);
        });
    }

    @Test
    public void testDedupWithRandomShiftAndStepAndSymbolKeyAndColumnTops() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = generateRandom(LOG);

            String tableName = testName.getMethodName();
            compile(
                    "create table " + tableName +
                            " (ts timestamp, commit int) " +
                            " timestamp(ts) partition by DAY WAL "
                            + " DEDUP UPSERT KEYS(ts)"
            );

            ObjList<FuzzTransaction> transactions = new ObjList<>();
            long initialDelta = Timestamps.MINUTE_MICROS * 15;

            int initialDuplicates = 1 + rnd.nextInt(1);
            long startTimestamp = parseFloorPartialTimestamp("2020-02-24T04:30");
            int startCount = 4 * 24 * 5;
            generateInsertsTransactions(
                    transactions,
                    1,
                    startTimestamp,
                    initialDelta,
                    startCount,
                    initialDuplicates,
                    null,
                    rnd
            );
            long maxTimestamp = startTimestamp + startCount * initialDelta;
            LOG.info().$("adding rows with commit = 1 from=").$ts(startTimestamp).$(", to=").$ts(maxTimestamp).$();

            int transactionCount = 1 + rnd.nextInt(3);
            splitTransactionInserts(transactions, transactionCount, rnd);
            applyWal(transactions, tableName, 1, rnd);

            LOG.info().$("adding S column after ").$ts(maxTimestamp).$();
            compile("alter table " + tableName + " add column s symbol");
            compile("alter table " + tableName + " dedup upsert keys(ts, s)");

            int rndCount = rnd.nextInt(10);
            List<String> distinctSymbols = Arrays.stream(generateSymbols(rnd, 1 + rndCount, 4, tableName)).distinct()
                    .collect(Collectors.toList());
            distinctSymbols.add(null);
            String[] symbols = new String[distinctSymbols.size()];
            distinctSymbols.toArray(symbols);
            String[] initialSymbols = symbols.length == 1
                    ? symbols
                    : Arrays.copyOf(symbols, 1 + rnd.nextInt(symbols.length - 1));

            long fromTops = startTimestamp + rnd.nextLong(startCount) * initialDelta;
            generateInsertsTransactions(
                    transactions,
                    1,
                    fromTops,
                    initialDelta,
                    startCount,
                    initialDuplicates,
                    initialSymbols,
                    rnd
            );
            LOG.info().$("adding more rows with commit = 1 from=").$ts(fromTops).$(", to=")
                    .$ts(fromTops + initialDelta * startCount).$();

            transactionCount = 1 + rnd.nextInt(3);
            splitTransactionInserts(transactions, transactionCount, rnd);
            applyWal(transactions, tableName, 1, rnd);

            transactions.clear();
            long shift = rnd.nextLong(startCount) * Timestamps.MINUTE_MICROS * 15 +
                    rnd.nextLong(15) * Timestamps.MINUTE_MICROS;
            long from = startTimestamp + shift;
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

            LOG.info().$("adding rows with commit = 2 from=").$ts(from).$(", to=")
                    .$ts(from + count * delta).$();

            transactionCount = 1 + rnd.nextInt(3);
            splitTransactionInserts(transactions, transactionCount, rnd);

            // adding rows with commit = 2 from=2020-02-25T10:29:00.000000Z, to=2020-02-26T10:29:00.000000Z
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
            generateInsertsTransactions(
                    transactions,
                    1,
                    parseFloorPartialTimestamp("2020-02-24T04:30"),
                    initialDelta,
                    initialCount,
                    initialDuplicates,
                    null,
                    rnd
            );
            applyWal(transactions, tableName, 1, rnd);
            compile("alter table " + tableName + " dedup upsert keys(ts)");

            transactions.clear();

            double deltaMultiplier = rnd.nextBoolean() ? (1 << rnd.nextInt(4)) : 1.0 / (1 << rnd.nextInt(4));
            long delta = (long) (initialDelta * deltaMultiplier);
            long shift = (-100 + rnd.nextLong((long) (initialCount / deltaMultiplier + 150))) * delta;
            long from = parseFloorPartialTimestamp("2020-02-24") + shift;
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
                    parseFloorPartialTimestamp("2020-02-24T04:30"),
                    initialDelta,
                    initialCount,
                    1 + rnd.nextInt(1),
                    null,
                    rnd
            );
            String[] symbols = generateSymbols(rnd, 20, 4, tableName);

            applyWal(transactions, tableName, 1, rnd);
            transactions.clear();

            ddl("alter table " + tableName + " add s symbol index", sqlExecutionContext);

            double deltaMultiplier = rnd.nextBoolean() ? (1 << rnd.nextInt(4)) : 1.0 / (1 << rnd.nextInt(4));
            long delta = (long) (initialDelta * deltaMultiplier);
            long shift = (-100 + rnd.nextLong((long) (initialCount / deltaMultiplier + 150))) * delta;
            long from = parseFloorPartialTimestamp("2020-02-24") + shift;
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
    public void testRandomColumnsDedupMultipleKeyCol() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setFuzzProbabilities(
                rnd.nextDouble() / 100,
                rnd.nextDouble(),
                rnd.nextDouble(),
                0.1 * rnd.nextDouble(),
                0.1 * rnd.nextDouble(),
                0,
                rnd.nextDouble(),
                rnd.nextDouble(),
                0.1 * rnd.nextDouble(),
                0.5,
                0.0
        );

        setFuzzCounts(
                rnd.nextBoolean(),
                rnd.nextInt(100_000),
                rnd.nextInt(20),
                rnd.nextInt(20),
                rnd.nextInt(20),
                rnd.nextInt(1000),
                rnd.nextInt(100_000),
                1 + rnd.nextInt(1)
        );

        runFuzzWithRandomColsDedup(rnd, -1);
    }

    @Test
    public void testRandomColumnsDedupOneKeyCol() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setFuzzProbabilities(
                rnd.nextDouble() / 100,
                rnd.nextDouble(),
                rnd.nextDouble(),
                0.1 * rnd.nextDouble(),
                0.1 * rnd.nextDouble(),
                0,
                rnd.nextDouble(),
                rnd.nextDouble(),
                0.1 * rnd.nextDouble(),
                0.5,
                0.0
        );

        setFuzzCounts(
                rnd.nextBoolean(),
                rnd.nextInt(100_000),
                rnd.nextInt(20),
                rnd.nextInt(20),
                rnd.nextInt(20),
                rnd.nextInt(1000),
                rnd.nextInt(100_000),
                1 + rnd.nextInt(1)
        );

        runFuzzWithRandomColsDedup(rnd, 1);
    }

    @Test
    public void testRandomDedupRepeat() throws Exception {
        Rnd rnd = generateRandom(LOG);
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
                0.0,
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

    private void assertSqlCursorsNoDups(
            String tableNameNoWal,
            ObjList<CharSequence> upsertKeyNames,
            String tableNameWal
    ) throws SqlException {
        Log log = LOG;
        try (RecordCursorFactory factory = select(tableNameNoWal);
             RecordCursorFactory factoryPreview = select(tableNameNoWal)) {
            try (RecordCursorFactory factory2 = select(tableNameWal)) {
                try (RecordCursor cursor1 = factory.getCursor(sqlExecutionContext);
                     RecordCursor previewCursor = factoryPreview.getCursor(sqlExecutionContext)) {
                    try (
                            RecordCursor dedupWrapper = new DedupCursor(factory.getMetadata(), cursor1, previewCursor, upsertKeyNames);
                            RecordCursor actualCursor = factory2.getCursor(sqlExecutionContext)
                    ) {
                        try {
                            assertEquals(dedupWrapper, factory.getMetadata(), actualCursor, factory2.getMetadata(), false);
                        } catch (AssertionError e) {
                            log.error().$(e).$();
                            dedupWrapper.toTop();
                            actualCursor.toTop();
                            log.xDebugW().$();

                            LogRecordSinkAdapter recordSinkAdapter = new LogRecordSinkAdapter();
                            LogRecord record = log.xDebugW().$("java.lang.AssertionError: expected:<");
                            printer.printHeaderNoNl(factory.getMetadata(), recordSinkAdapter.of(record));
                            record.$();
                            printer.print(dedupWrapper, factory.getMetadata(), false, log);

                            record = log.xDebugW().$("> but was:<");
                            printer.printHeaderNoNl(factory2.getMetadata(), recordSinkAdapter.of(record));
                            record.$();

                            printer.print(actualCursor, factory2.getMetadata(), false, log);
                            log.xDebugW().$(">").$();
                            throw e;
                        }
                    }
                }
            }
        }
    }

    private void chooseUpsertKeys(RecordMetadata metadata, int dedupKeyCount, Rnd rnd, IntList upsertKeyIndexes) {
        upsertKeyIndexes.add(metadata.getTimestampIndex());
        int dedupKeys = dedupKeyCount > -1 ? dedupKeyCount : rnd.nextInt(metadata.getColumnCount() - 1);
        for (int i = 0; i < dedupKeys; i++) {
            int start = rnd.nextInt(metadata.getColumnCount());
            for (int c = 0; c < metadata.getColumnCount(); c++) {
                int col = (c + start) % metadata.getColumnCount();
                int columnType = metadata.getColumnType(col);

                if (!upsertKeyIndexes.contains(col) && !ColumnType.isVariableLength(columnType)) {
                    upsertKeyIndexes.add(col);
                    break;
                }
            }
        }
    }

    private void collectUpsertKeyNames(TableRecordMetadata metadata, IntList upsertKeys, ObjList<CharSequence> upsertKeyNames) {
        for (int i = 0; i < upsertKeys.size(); i++) {
            int columnType = metadata.getColumnType(upsertKeys.get(i));
            if (columnType > 0) {
                upsertKeyNames.add(metadata.getColumnName(upsertKeys.get(i)));
            }
        }
    }

    private void createEmptyTable(String tableName, String dedupOption) throws SqlException {
        ddl("create table " + tableName + " (ts timestamp, commit int) timestamp(ts) partition by DAY WAL " + dedupOption
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
            long fromTimestamp,
            long delta,
            int count,
            int rowsWithSameTimestamp,
            String[] symbols,
            Rnd rnd
    ) {
        FuzzTransaction transaction = new FuzzTransaction();
        transactions.add(transaction);
        for (int i = 0; i < count; i++) {
            for (int j = 0; j < rowsWithSameTimestamp; j++) {
                if (symbols != null && symbols.length > 0) {
                    for (int s = 0; s < symbols.length; s++) {
                        transaction.operationList.add(new FuzzStableInsertOperation(fromTimestamp, commit, symbols[s]));
                    }
                } else {
                    transaction.operationList.add(new FuzzStableInsertOperation(fromTimestamp, commit));
                }
            }
            fromTimestamp += delta;
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

    private void runFuzzWithRandomColsDedup(Rnd rnd, int dedupKeys) throws Exception {
        assertMemoryLeak(() -> {
            String tableNameBase = getTestName();
            String tableNameDedup = tableNameBase + "_wal";
            String tableNameWalNoDedup = tableNameBase + "_nodedup";

            fuzzer.createInitialTable(tableNameWalNoDedup, true);
            fuzzer.createInitialTable(tableNameDedup, true);

            // Add long256 type to have to be a chance of a dedup key
            compile("alter table " + tableNameDedup + " add column col256 long256");
            compile("alter table " + tableNameWalNoDedup + " add column col256 long256");

            drainWalQueue();

            ObjList<FuzzTransaction> transactions;
            IntList upsertKeyIndexes = new IntList();
            String comaSeparatedUpsertCols;
            String timestampColumnName;

            try (TableReader reader = getReader(tableNameWalNoDedup)) {
                TableReaderMetadata metadata = reader.getMetadata();
                chooseUpsertKeys(metadata, dedupKeys, rnd, upsertKeyIndexes);
                timestampColumnName = metadata.getColumnName(metadata.getTimestampIndex());

                long start = IntervalUtils.parseFloorPartialTimestamp("2022-02-24T23:59:59");
                long end = start + 2 * Timestamps.SECOND_MICROS;
                transactions = generateSet(rnd, metadata, start, end, tableNameWalNoDedup);
                comaSeparatedUpsertCols = toCommaSeparatedString(metadata, upsertKeyIndexes);
            }
            String alterStatement = String.format(
                    "alter table %s dedup upsert keys(%s))",
                    tableNameDedup,
                    comaSeparatedUpsertCols
            );
            compile(alterStatement);

            O3Utils.setupWorkerPool(sharedWorkerPool, engine, null);
            sharedWorkerPool.start(LOG);

            try {
                applyWal(transactions, tableNameWalNoDedup, 1, new Rnd());
                applyWal(transactions, tableNameDedup, 1 + rnd.nextInt(4), rnd);

                String renamedUpsertKeys;
                ObjList<CharSequence> upsertKeyNames = new ObjList<>();
                try (TableWriter writer = getWriter(tableNameWalNoDedup)) {
                    collectUpsertKeyNames(writer.getMetadata(), upsertKeyIndexes, upsertKeyNames);
                    renamedUpsertKeys = toCommaSeparatedString(writer.getMetadata(), upsertKeyIndexes);
                }

                LOG.info().$("asserting no dups on keys: ").$(renamedUpsertKeys).$();
                assertSqlCursorsNoDups(
                        tableNameWalNoDedup,
                        upsertKeyNames,
                        tableNameDedup
                );

                fuzzer.assertCounts(tableNameDedup, timestampColumnName);
                fuzzer.assertStringColDensity(tableNameDedup);
            } finally {
                sharedWorkerPool.halt();
            }
        });
    }

    private void runFuzzWithRepeatDedup(Rnd rnd) throws Exception {
        assertMemoryLeak(() -> {
            String tableNameBase = getTestName();
            String tableNameDedup = tableNameBase + "_wal";
            String tableNameNoWal = tableNameBase + "_nonwal";

            TableToken dedupTt = fuzzer.createInitialTable(tableNameDedup, true);
            fuzzer.createInitialTable(tableNameNoWal, false);

            String timestampColumnName;
            try (TableMetadata meta = engine.getMetadata(dedupTt)) {
                timestampColumnName = meta.getColumnName(meta.getTimestampIndex());
            }

            long start = IntervalUtils.parseFloorPartialTimestamp("2022-02-24T17");
            long end = start + fuzzer.partitionCount * Timestamps.DAY_MICROS;
            ObjList<FuzzTransaction> transactions = fuzzer.generateTransactions(tableNameDedup, rnd, start, end);

            transactions = uniqueInserts(transactions);
            O3Utils.setupWorkerPool(sharedWorkerPool, engine, null);
            sharedWorkerPool.start(LOG);

            try {
                fuzzer.applyNonWal(transactions, tableNameNoWal, rnd);

                ObjList<FuzzTransaction> transactionsWithDups = duplicateInserts(transactions, rnd);
                ddl("alter table " + tableNameDedup + " dedup upsert keys(ts)", sqlExecutionContext);
                applyWal(transactionsWithDups, tableNameDedup, 1 + rnd.nextInt(4), rnd);

                String limit = "";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, tableNameNoWal + limit, tableNameDedup + limit, LOG);
                fuzzer.assertRandomIndexes(tableNameNoWal, tableNameDedup, rnd);
                // assert table count() values
                fuzzer.assertCounts(tableNameDedup, timestampColumnName);
                fuzzer.assertCounts(tableNameNoWal, timestampColumnName);
                fuzzer.assertStringColDensity(tableNameDedup);
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

    private void splitTransactionInserts(ObjList<FuzzTransaction> transactions, int count, Rnd rnd) {
        if (count > 1) {
            ObjList<FuzzTransactionOperation> operationList = transactions.get(0).operationList;

            if (operationList.size() > 0) {
                int[] sizes = new int[count];
                for (int i = 0; i < count - 1; i++) {
                    sizes[i] = rnd.nextInt(1 + rnd.nextInt(operationList.size()));
                }
                sizes[count - 1] = operationList.size();
                Arrays.sort(sizes);

                for (int i = count - 1; i > 0; i--) {
                    int chunkSize = sizes[i] - sizes[i - 1];
                    FuzzTransaction transaction = new FuzzTransaction();
                    transaction.operationList.addAll(operationList, operationList.size() - chunkSize, operationList.size());
                    operationList.setPos(operationList.size() - chunkSize);
                    transactions.insert(0, 1, transaction);
                }
            }
        }
    }

    private void swap(ObjList<FuzzTransactionOperation> operationList, int i, int j) {
        FuzzTransactionOperation tmp = operationList.getQuick(i);
        operationList.setQuick(i, operationList.getQuick(j));
        operationList.setQuick(j, tmp);
    }

    private String toCommaSeparatedString(RecordMetadata metadata, IntList upsertKeys) {
        StringSink sink = new StringSink();
        for (int i = 0; i < upsertKeys.size(); i++) {
            int columnType = metadata.getColumnType(upsertKeys.get(i));
            if (columnType > 0 && !ColumnType.isVariableLength(columnType)) {
                if (i > 0) {
                    sink.put(',');
                }
                sink.put(metadata.getColumnName(upsertKeys.get(i)));
            }
        }
        return sink.toString();
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
            long fromTimestamp,
            long delta,
            long commit2Count,
            String[] symbols,
            int existingDups
    ) {

        LOG.info().$("Validating no timestamp duplicates [from=").$ts(fromTimestamp)
                .$(", delta=").$(delta)
                .$(", commit2Count=").$(commit2Count)
                .I$();

        long lastTimestamp = Long.MIN_VALUE;
        long toTimestamp = fromTimestamp + delta * commit2Count;
        StringSink sink = new StringSink();
        boolean started = false;
        ObjIntHashMap<CharSequence> symbolSet = new ObjIntHashMap<>();
        String nullSymbolValue = "nullSymbolValue_unlikely_to_be_generated_by_random";
        boolean[] foundSymbols = null;
        if (symbols != null) {
            for (int i = 0; i < symbols.length; i++) {
                if (symbols[i] != null) {
                    symbolSet.put(symbols[i], i);
                } else {
                    symbolSet.put(nullSymbolValue, i);
                }
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
                            sink.put(',');
                            CharSequence cs = rec.getSym(2);
                            if (cs != null) {
                                sink.put(cs);
                            }
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
                            if (sym == null) {
                                sym = nullSymbolValue;
                            }
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

    private static class DedupCursor implements RecordCursor {

        private final StringSink currentRecordKeys = new StringSink();
        private final IntList keyColumns = new IntList();
        private final ObjHashSet<String> keyProcessed = new ObjHashSet<>();
        private final RecordMetadata metadata;
        private final RecordCursor nextRecCursor;
        private final StringSink nextRecordKeys = new StringSink();
        private final RecordCursor recordCursor;
        private final IntList skipRecords = new IntList();
        ObjList<String> recordKeys = new ObjList<>();
        private long lastTimestamp = -1;
        private int timestampColIndex = -1;

        public DedupCursor(
                RecordMetadata cursorMetadata,
                RecordCursor innerCursor,
                RecordCursor previewCursor,
                ObjList<CharSequence> keyColumnNames
        ) {
            this.recordCursor = innerCursor;
            this.metadata = cursorMetadata;
            this.nextRecCursor = previewCursor;

            for (int i = 0; i < cursorMetadata.getColumnCount(); i++) {
                CharSequence columnName = cursorMetadata.getColumnName(i);
                if (keyColumnNames.contains(columnName)) {
                    keyColumns.add(i);
                }
                if (Chars.equals(columnName, "ts")) {
                    this.timestampColIndex = i;
                }
            }
        }

        @Override
        public void close() {
        }

        @Override
        public Record getRecord() {
            return recordCursor.getRecord();
        }

        @Override
        public Record getRecordB() {
            return recordCursor.getRecordB();
        }

        @Override
        public boolean hasNext() {
            if (dispatchRecordsWithOffsets()) {
                return true;
            }

            boolean hasNext;
            do {
                hasNext = nextRecCursor.hasNext();
                long currentTs = hasNext ? nextRecCursor.getRecord().getLong(timestampColIndex) : Long.MIN_VALUE;
                if (currentTs != lastTimestamp) {
                    lastTimestamp = currentTs;

                    // unleash the last records for each key
                    for (int i = recordKeys.size() - 1; i > -1; i--) {
                        if (keyProcessed.add(recordKeys.get(i))) {
                            skipRecords.add(i);
                        }
                    }
                    recordKeys.clear();
                    keyProcessed.clear();

                    // find differences between indexes in skipRecords
                    reverse(skipRecords);
                    int last = -1;
                    for (int i = 0; i < skipRecords.size(); i++) {
                        int val = skipRecords.get(i);
                        skipRecords.set(i, val - last);
                        last = val;
                    }

                    // reverse skip records for easier removal from the end
                    reverse(skipRecords);
                    if (dispatchRecordsWithOffsets()) {
                        if (hasNext) {
                            nextRecordKeys.clear();
                            printRecordToSink(nextRecCursor.getRecord(), nextRecordKeys);
                            recordKeys.add(nextRecordKeys.toString());
                        }
                        return true;
                    }
                }

                if (hasNext) {
                    nextRecordKeys.clear();
                    printRecordToSink(nextRecCursor.getRecord(), nextRecordKeys);
                    recordKeys.add(nextRecordKeys.toString());
                }
            } while (hasNext);

            return false;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            recordCursor.recordAt(record, atRowId);
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
            recordCursor.toTop();
            nextRecCursor.toTop();
            nextRecordKeys.clear();
            currentRecordKeys.clear();
            skipRecords.clear();
            recordKeys.clear();
        }

        private boolean dispatchRecordsWithOffsets() {
            if (skipRecords.size() > 0) {
                int skip = skipRecords.getLast();
                skipRecords.setPos(skipRecords.size() - 1);
                for (int i = 0; i < skip; i++) {
                    if (!recordCursor.hasNext()) {
                        Assert.fail("expected to have a record");
                    }
                }
                return true;
            }
            return false;
        }

        private void printRecordToSink(Record record, StringSink currentRecordKeys) {
            for (int i = 0; i < keyColumns.size(); i++) {
                TestUtils.printColumn(record, metadata, keyColumns.get(i), currentRecordKeys, false, false, "<null>");
                currentRecordKeys.put('\t');
            }
        }

        private void reverse(IntList skipRecords) {
            int size = skipRecords.size();
            for (int i = 0; i < size / 2; i++) {
                int temp = skipRecords.get(i);
                skipRecords.set(i, skipRecords.get(size - i - 1));
                skipRecords.set(size - i - 1, temp);
            }
        }
    }
}
