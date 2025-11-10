/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.LogRecordSinkAdapter;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogRecord;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.Chars;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.LongHashSet;
import io.questdb.std.Misc;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjIntHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.fuzz.DuplicateFuzzInsertOperation;
import io.questdb.test.fuzz.FuzzDropCreateTableOperation;
import io.questdb.test.fuzz.FuzzDropPartitionOperation;
import io.questdb.test.fuzz.FuzzInsertOperation;
import io.questdb.test.fuzz.FuzzStableInsertOperation;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.fuzz.FuzzTransactionOperation;
import io.questdb.test.fuzz.FuzzTruncateTableOperation;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.questdb.test.tools.TestUtils.assertEquals;

public class DedupInsertFuzzTest extends AbstractFuzzTest {
    private final boolean convertToParquet;

    public DedupInsertFuzzTest() {
        this.convertToParquet = TestUtils.generateRandom(LOG).nextBoolean();
    }

    @Test
    public void testDedupWithRandomShiftAndStep() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = getTestName();
            createEmptyTable(tableName, "DEDUP upsert keys(ts, commit)");
            execute("alter table " + tableName + " dedup disable");
            execute("alter table " + tableName + " dedup enable upsert keys(ts)");

            ObjList<FuzzTransaction> transactions = new ObjList<>();
            Rnd rnd = generateRandomAndProps();
            try {
                long initialDelta = Micros.MINUTE_MICROS * 15;
                int initialCount = 4 * 24 * 5;
                generateInsertsTransactions(
                        transactions,
                        1,
                        MicrosTimestampDriver.floor("2020-02-24T04:30"),
                        initialDelta,
                        initialCount,
                        1 + rnd.nextInt(1),
                        null,
                        rnd
                );
                applyWal(transactions, tableName, 1, rnd);
                maybeConvertToParquet(tableName);
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
                validateNoTimestampDuplicates(tableName, from, delta, count, null, 1, ColumnType.NULL);
            } finally {
                Misc.freeObjListAndClear(transactions);
            }
        });
    }

    @Test
    public void testDedupWithRandomShiftAndStepAndSymbolKey() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = generateRandomAndProps();

            String tableName = getTestName();
            execute(
                    "create table " + tableName +
                            " (ts timestamp, commit int, s symbol) " +
                            " , index(s) timestamp(ts) partition by DAY WAL "
                            + " DEDUP UPSERT KEYS(ts, s)"
            );

            runDedupWithShiftAndStep(rnd, tableName, ColumnType.SYMBOL);
        });
    }

    @Test
    public void testDedupWithRandomShiftAndStepAndSymbolKeyAndColumnTops() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = generateRandomAndProps();

            String tableName = getTestName();
            execute(
                    "create table " + tableName +
                            " (ts timestamp, commit int) " +
                            " timestamp(ts) partition by DAY WAL "
                            + " DEDUP UPSERT KEYS(ts)"
            );
            testDedupWithRandomShiftAndStepAndColumnTops(rnd, ColumnType.SYMBOL, tableName);
        });
    }

    @Test
    public void testDedupWithRandomShiftAndStepAndVarcharKey() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = generateRandomAndProps();

            String tableName = getTestName();
            execute(
                    "create table " + tableName +
                            " (ts timestamp, commit int, s varchar) " +
                            " timestamp(ts) partition by DAY WAL "
                            + " DEDUP UPSERT KEYS(ts, s)"
            );

            runDedupWithShiftAndStep(rnd, tableName, ColumnType.VARCHAR);
        });
    }

    @Test
    public void testDedupWithRandomShiftAndStepAndVarcharKeyAndColumnTops() throws Exception {
        // TODO(eugene): Enable this test when adding columns after Parquet conversion is supported
        Assume.assumeFalse(convertToParquet);
        assertMemoryLeak(() -> {
            Rnd rnd = generateRandomAndProps();

            String tableName = getTestName();
            execute(
                    "create table " + tableName +
                            " (ts timestamp, commit int) " +
                            " timestamp(ts) partition by DAY WAL "
                            + " DEDUP UPSERT KEYS(ts)"
            );
            testDedupWithRandomShiftAndStepAndColumnTops(rnd, ColumnType.VARCHAR, tableName);
        });
    }

    @Test
    public void testDedupWithRandomShiftAndStepWithExistingDups() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = getTestName();
            createEmptyTable(tableName, "");

            ObjList<FuzzTransaction> transactions = new ObjList<>();
            Rnd rnd = generateRandomAndProps();
            try {
                long initialDelta = Micros.MINUTE_MICROS * 15;
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
                maybeConvertToParquet(tableName);
                execute("alter table " + tableName + " dedup upsert keys(ts)");

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
                validateNoTimestampDuplicates(tableName, from, delta, count, null, initialDuplicates, ColumnType.NULL);
            } finally {
                Misc.freeObjListAndClear(transactions);
            }
        });
    }

    @Test
    public void testDedupWithRandomShiftWithColumnTop() throws Exception {
        // TODO(eugene): Enable this test when adding columns after Parquet conversion is supported
        Assume.assumeFalse(convertToParquet);
        assertMemoryLeak(() -> {
            String tableName = getTestName();

            createEmptyTable(tableName, "DEDUP upsert keys(ts)");

            ObjList<FuzzTransaction> transactions = new ObjList<>();
            Rnd rnd = generateRandomAndProps();
            long initialDelta = Micros.MINUTE_MICROS * 15;
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

            int strLen = 4 + rnd.nextInt(20);
            String[] symbols = generateSymbols(rnd, 20, strLen, tableName);

            applyWal(transactions, tableName, 1, rnd);
            transactions.clear();

            execute("alter table " + tableName + " add s symbol index", sqlExecutionContext);

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

            maybeConvertToParquet(tableName);
            applyWal(transactions, tableName, 1, rnd);
            validateNoTimestampDuplicates(tableName, from, delta, count, null, 1, ColumnType.NULL);
        });
    }

    @Test
    public void testRandomColumnsDedupMultipleKeyCol() throws Exception {
        Rnd rnd = generateRandomAndProps();
        setFuzzProbabilities(
                rnd.nextDouble() / 100,
                rnd.nextDouble(),
                rnd.nextDouble(),
                0.1 * rnd.nextDouble(),
                // TODO(eugene): table column manipulation is not yet supported for Parquet
                convertToParquet ? 0 : 0.1 * rnd.nextDouble(),
                0,
                convertToParquet ? 0 : rnd.nextDouble(),
                0.0,
                convertToParquet ? 0 : rnd.nextDouble(),
                0.5,
                0.0,
                0.1 * rnd.nextDouble(),
                0.0,
                0.5,
                0.0,
                0
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
    public void testRandomColumnsDedupMultipleKeyColWithRCommits() throws Exception {
        // Replace commits not yet supported with Parquet
        Assume.assumeFalse(convertToParquet);
        Rnd rnd = generateRandomAndProps();
        setFuzzProbabilities(
                rnd.nextDouble() / 100,
                rnd.nextDouble(),
                rnd.nextDouble(),
                0.1 * rnd.nextDouble(),
                0.1 * rnd.nextDouble(),
                0,
                rnd.nextDouble(),
                0.0,
                rnd.nextDouble(),
                0.0,
                0.0,
                0.1 * rnd.nextDouble(),
                0.0,
                0.5,
                0.5,
                0
        );

        // Set isO3 to false to enforce unique timestamps inside the transactions,
        // replace commits do not run deduplication inside the transaction rows.
        setFuzzCounts(
                false,
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
        Rnd rnd = generateRandomAndProps();
        setFuzzProbabilities(
                rnd.nextDouble() / 100,
                rnd.nextDouble(),
                rnd.nextDouble(),
                0.1 * rnd.nextDouble(),

                // TODO(eugene): table column manipulation is not yet supported for Parquet
                convertToParquet ? 0 : 0.2 * rnd.nextDouble(),
                0,
                convertToParquet ? 0 : rnd.nextDouble(),
                0.0,
                convertToParquet ? 0 : rnd.nextDouble(),
                0.5,
                0.0,
                0.1 * rnd.nextDouble(),
                0.0,
                0.5,
                0.0,
                0
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
        // TODO(eugene): update the parquet partition with a missed column (e.g. column top) is not yet implemented
        Assume.assumeFalse(convertToParquet);
        Rnd rnd = generateRandomAndProps();
        setFuzzProbabilities(
                0,
                rnd.nextDouble(),
                rnd.nextDouble(),
                0.5 * rnd.nextDouble(),
                rnd.nextDouble() / 100,
                rnd.nextDouble() / 100,
                rnd.nextDouble(),
                rnd.nextDouble(),
                rnd.nextDouble(),
                0.0,
                0.0,
                0.1 * rnd.nextDouble(),
                // This test does not support Drop Partition operations,
                // it is not trivial to build the result set of data to assert against with drop partitions
                0,
                0.4,
                0.0,
                0
        );

        setFuzzCounts(
                rnd.nextBoolean(),
                rnd.nextInt(1000),
                rnd.nextInt(1000),
                fuzzer.randomiseStringLengths(rnd, 1000),
                rnd.nextInt(1000),
                rnd.nextInt(1000),
                rnd.nextInt(1_000_000),
                5 + rnd.nextInt(10)
        );

        runFuzzWithRepeatDedup(rnd);
    }

    private void addDuplicates(ObjList<FuzzTransaction> transactions, Rnd rnd, IntList upsertKeyIndexes) {
        int duplicateCommits = rnd.nextInt(transactions.size());

        IntHashSet upsertKeyIndexesMap = new IntHashSet();
        upsertKeyIndexesMap.addAll(upsertKeyIndexes);

        for (int i = 0; i < transactions.size(); i++) {
            FuzzTransaction transaction = transactions.getQuick(i);
            if (transaction.rollback
                    || transaction.operationList.size() <= 1
                    || transaction.hasReplaceRange()
            ) {
                continue;
            }

            if (duplicateCommits > 0 && rnd.nextInt(duplicateCommits) > (transactions.size() - i - duplicateCommits)) {
                // Add a duplicate commit
                FuzzTransaction duplicateTrans = new FuzzTransaction();
                duplicateTrans.waitAllDone = transaction.waitAllDone;
                duplicateTrans.reopenTable = transaction.reopenTable;
                duplicateTrans.rollback = false;
                duplicateTrans.structureVersion = transaction.structureVersion;
                duplicateTrans.waitBarrierVersion = transaction.waitBarrierVersion;

                ObjList<FuzzTransactionOperation> txnList = transaction.operationList;
                int dupCount = rnd.nextInt(txnList.size() - 1) + 1;

                ObjList<FuzzTransactionOperation> newTxnList = duplicateTrans.operationList;
                int generateNonIdenticalCount = rnd.nextInt(2);

                for (int t = 0, n = txnList.size(); t < n && dupCount > 0; t++) {
                    if (dupCount + rnd.nextInt(n - t) > (n - t - dupCount)) {
                        // Add a duplicate operation
                        FuzzInsertOperation operation = (FuzzInsertOperation) txnList.getQuick(t);

                        // Generate only 1 non-identical record, compares will have work harder to find 1 record only
                        boolean generatedNonIdentical = generateNonIdenticalCount > 0 && rnd.nextBoolean();
                        FuzzInsertOperation dup = new DuplicateFuzzInsertOperation(operation, upsertKeyIndexesMap, !generatedNonIdentical);
                        if (generatedNonIdentical) {
                            generateNonIdenticalCount--;
                        }

                        newTxnList.add(dup);
                        dupCount--;
                    }
                }

                if (newTxnList.size() > 0) {
                    shuffle(newTxnList, rnd);
                    transactions.insert(i + 1, 1, duplicateTrans);
                    i++;
                }
                duplicateCommits--;
            }
        }
    }

    private void assertAllSymbolsSet(
            boolean[] foundSymbols,
            String[] symbols,
            long timestamp
    ) {
        for (int i = 0; i < foundSymbols.length; i++) {
            if (!foundSymbols[i]) {
                CharSequence symbol = symbols[i];
                Assert.fail("Symbol '" + symbol + "' not found for timestamp " + Micros.toUSecString(timestamp));
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
        // Define where clause for debugging
        String filter = "";
        try (
                RecordCursorFactory factory = select(tableNameNoWal + filter);
                RecordCursorFactory factoryPreview = select(tableNameNoWal + filter)
        ) {
            try (RecordCursorFactory factory2 = select(tableNameWal + filter)) {
                try (
                        RecordCursor cursor1 = factory.getCursor(sqlExecutionContext);
                        RecordCursor previewCursor = factoryPreview.getCursor(sqlExecutionContext)
                ) {
                    try (
                            RecordCursor dedupWrapper = new DedupCursor(factory.getMetadata(), cursor1, previewCursor, upsertKeyNames);
                            RecordCursor actualCursor = factory2.getCursor(sqlExecutionContext)
                    ) {
                        try {
                            assertEquals(dedupWrapper, factory.getMetadata(), actualCursor, factory2.getMetadata(), false);
                        } catch (AssertionError e) {
                            dedupWrapper.toTop();
                            actualCursor.toTop();
                            log.xDebugW().$();

                            LogRecordSinkAdapter recordSinkAdapter = new LogRecordSinkAdapter();
                            LogRecord record = log.xDebugW().$("java.lang.AssertionError: expected:<");
                            CursorPrinter.printHeader(factory.getMetadata(), recordSinkAdapter.of(record));
                            record.$();
                            CursorPrinter.println(dedupWrapper, factory.getMetadata(), false, log);

                            record = log.xDebugW().$("> but was:<");
                            CursorPrinter.printHeader(factory2.getMetadata(), recordSinkAdapter.of(record));
                            record.$();

                            CursorPrinter.println(actualCursor, factory2.getMetadata(), false, log);
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
                if (!upsertKeyIndexes.contains(col)) {
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
        execute("create table " + tableName + " (ts timestamp, commit int) timestamp(ts) partition by DAY WAL " + dedupOption
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
                    if (operation instanceof FuzzInsertOperation insertOperation) {
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

                duplicateTrans.reopenTable = transaction.reopenTable;
                result.add(duplicateTrans);
                prevInsertTrans = duplicateTrans;
            } else {
                if (transaction.operationList.size() > 0) {
                    var operation = transaction.operationList.getQuick(0);
                    if (operation instanceof FuzzDropCreateTableOperation || operation instanceof FuzzTruncateTableOperation
                            || operation instanceof FuzzDropPartitionOperation) {
                        prevInsertTrans = null;
                    }
                }
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
        generateInsertsTransactions(
                transactions,
                commit,
                fromTimestamp,
                delta,
                count,
                rowsWithSameTimestamp,
                symbols,
                rnd,
                ColumnType.SYMBOL
        );
    }

    private void generateInsertsTransactions(
            ObjList<FuzzTransaction> transactions,
            int commit,
            long fromTimestamp,
            long delta,
            int count,
            int rowsWithSameTimestamp,
            String[] symbols,
            Rnd rnd,
            int columnType
    ) {
        FuzzTransaction transaction = new FuzzTransaction();
        transactions.add(transaction);
        Utf8StringSink sink = new Utf8StringSink();
        for (int i = 0; i < count; i++) {
            for (int j = 0; j < rowsWithSameTimestamp; j++) {
                if (symbols != null && symbols.length > 0) {
                    for (int s = 0; s < symbols.length; s++) {
                        transaction.operationList.add(new FuzzStableInsertOperation(fromTimestamp, commit, symbols[s], columnType, sink));
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

    private Rnd generateRandomAndProps() {
        Rnd rnd = fuzzer.generateRandom(io.questdb.test.AbstractCairoTest.LOG);
        setFuzzProperties(rnd);
        setRandomAppendPageSize(rnd);
        return rnd;
    }

    @SuppressWarnings("unused")
    private Rnd generateRandomAndProps(long s0, long s1) {
        Rnd rnd = fuzzer.generateRandom(io.questdb.test.AbstractCairoTest.LOG, s0, s1);
        setFuzzProperties(rnd);
        setRandomAppendPageSize(rnd);
        return rnd;
    }

    private void maybeConvertToParquet(String tableName) {
        if (convertToParquet) {
            // convert to parquet, so we can test dedup with parquet
            try {
                execute("alter table " + tableName + " convert partition to parquet where ts >= 0");
                drainWalQueue();
            } catch (SqlException e) {
                Assert.fail("Failed to convert to parquet: " + e.getMessage());
            }
        }
    }

    private CharSequence readStrValue(Record rec, int colType) {
        return switch (colType) {
            case ColumnType.SYMBOL -> rec.getSymA(2);
            case ColumnType.STRING -> rec.getStrA(2);
            case ColumnType.VARCHAR -> Utf8s.toString(rec.getVarcharA(2));
            default -> throw new IllegalArgumentException();
        };
    }

    private void reinsertSameData(Rnd rnd, String tableNameDedup, String timestampColumnName) throws SqlException {
        execute("ALTER TABLE \"" + tableNameDedup + "\" SQUASH PARTITIONS", sqlExecutionContext);
        drainWalQueue();

        LOG.info().$("Re-inserting same data into table ").$(tableNameDedup).$();
        TableToken tt = engine.verifyTableName(tableNameDedup);
        String partitions = readTxnToString(tt, false, true, true, false);
        int inserts = 2 + rnd.nextInt(10);
        long minTs, maxTs;
        try (TableReader reader = getReader(tableNameDedup)) {
            if (reader.size() == 0) {
                return;
            }
            minTs = reader.getMinTimestamp();
            maxTs = reader.getMaxTimestamp();
        }

        for (int i = 0; i < inserts; i++) {
            long fromTs = minTs + rnd.nextLong(maxTs - minTs);
            long toTs = (long) (fromTs + Micros.DAY_MICROS * Math.pow(1.2, rnd.nextDouble()));

            String insertSql = "insert into " + tableNameDedup +
                    " select * from " + tableNameDedup +
                    " where " + timestampColumnName + " >= " + fromTs + " and ts < " + toTs
                    + " LIMIT 100000";

            execute(insertSql);
        }
        drainWalQueue();

        Assert.assertFalse("table should not be suspended", engine.getTableSequencerAPI().getTxnTracker(tt).isSuspended());
        String partitionsAfter = readTxnToString(tt, false, true, true, false);
        Assert.assertEquals("partitions should be not rewritten", partitions, partitionsAfter);
    }

    private void runDedupWithShiftAndStep(Rnd rnd, String tableName, int colType) {
        ObjList<FuzzTransaction> transactions = new ObjList<>();
        try {
            long initialDelta = Micros.MINUTE_MICROS * 15;
            int rndCount = rnd.nextInt(10);
            int strLen = 4 + rnd.nextInt(20);
            List<String> distinctSymbols = Arrays
                    .stream(generateSymbols(rnd, 1 + rndCount, strLen, tableName))
                    .distinct()
                    .toList();
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
                    rnd,
                    colType
            );

            int transactionCount = 1 + rnd.nextInt(3);
            splitTransactionInserts(transactions, transactionCount, rnd);
            applyWal(transactions, tableName, 1, rnd);
            maybeConvertToParquet(tableName);

            transactions.clear();
            long shift = rnd.nextLong(4 * 24 * 5) * Micros.MINUTE_MICROS * 15 +
                    rnd.nextLong(15) * Micros.MINUTE_MICROS;
            long from = parseFloorPartialTimestamp("2020-02-24") + shift;
            long delta = Micros.MINUTE_MICROS;
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
                    rnd,
                    colType
            );

            transactionCount = 1 + rnd.nextInt(3);
            splitTransactionInserts(transactions, transactionCount, rnd);
            applyWal(transactions, tableName, 1, rnd);

            validateNoTimestampDuplicates(tableName, from, delta, count, symbols, 1, colType);
        } finally {
            Misc.freeObjListAndClear(transactions);
        }
    }

    private void runFuzzWithRandomColsDedup(Rnd rnd, int dedupKeys) throws Exception {
        assertMemoryLeak(() -> {
            String tableNameBase = getTestName();
            String tableNameDedup = tableNameBase + "_wal";
            String tableNameWalNoDedup = tableNameBase + "_nodedup";

            fuzzer.createInitialTableWal(tableNameWalNoDedup);
            fuzzer.createInitialTableWal(tableNameDedup);
            maybeConvertToParquet(tableNameDedup);

            // Add long256 type to have to be a chance of a dedup key
            execute("alter table " + tableNameDedup + " add column col256 long256");
            execute("alter table " + tableNameWalNoDedup + " add column col256 long256");

            drainWalQueue();

            ObjList<FuzzTransaction> transactions = null;
            IntList upsertKeyIndexes = new IntList();
            String comaSeparatedUpsertCols;
            String timestampColumnName;

            try {

                try (
                        TableReader reader = getReader(tableNameWalNoDedup);
                        TableRecordMetadata sequencerMetadata = engine.getSequencerMetadata(reader.getTableToken())
                ) {
                    TableReaderMetadata readerMetadata = reader.getMetadata();
                    chooseUpsertKeys(readerMetadata, dedupKeys, rnd, upsertKeyIndexes);
                    timestampColumnName = readerMetadata.getColumnName(readerMetadata.getTimestampIndex());

                    long start = MicrosTimestampDriver.floor("2022-02-24T23:59:59");
                    long end = start + 2 * Micros.SECOND_MICROS;
                    transactions = generateSet(rnd, sequencerMetadata, readerMetadata, start, end, tableNameWalNoDedup);
                    comaSeparatedUpsertCols = toCommaSeparatedString(readerMetadata, upsertKeyIndexes);
                }
                String alterStatement = String.format(
                        "alter table %s dedup upsert keys(%s)",
                        tableNameDedup,
                        comaSeparatedUpsertCols
                );
                execute(alterStatement);

                WorkerPoolUtils.setupWriterJobs(sharedWorkerPool, engine);
                sharedWorkerPool.start(LOG);

                try {
                    addDuplicates(transactions, rnd, upsertKeyIndexes);
                    applyWal(transactions, tableNameWalNoDedup, 1, new Rnd());
                    applyWal(transactions, tableNameDedup, 1 + rnd.nextInt(4), rnd);

                    String renamedUpsertKeys;
                    ObjList<CharSequence> upsertKeyNames = new ObjList<>();
                    try (TableWriter writer = getWriter(tableNameDedup)) {
                        collectUpsertKeyNames(writer.getMetadata(), upsertKeyIndexes, upsertKeyNames);
                        renamedUpsertKeys = toCommaSeparatedStringDebug(writer.getMetadata(), upsertKeyIndexes);
                    }

                    LOG.info().$("asserting no dups on keys: ").$(renamedUpsertKeys).$();
                    assertSqlCursorsNoDups(
                            tableNameWalNoDedup,
                            upsertKeyNames,
                            tableNameDedup
                    );

                    fuzzer.assertCounts(tableNameDedup, timestampColumnName);
                    fuzzer.assertStringColDensity(tableNameDedup);

                    // Re-insert exactly same data, random intervals to the table with dedup enabled
                    reinsertSameData(
                            rnd,
                            tableNameDedup,
                            timestampColumnName
                    );
                    assertSqlCursorsNoDups(
                            tableNameWalNoDedup,
                            upsertKeyNames,
                            tableNameDedup
                    );

                } finally {
                    sharedWorkerPool.halt();
                }
            } finally {
                Misc.freeObjListAndClear(transactions);
            }
        });
    }

    private void runFuzzWithRepeatDedup(Rnd rnd) throws Exception {
        assertMemoryLeak(() -> {
            String tableNameBase = getTestName();
            String tableNameDedup = tableNameBase + "_wal";
            String tableNameNoWal = tableNameBase + "_nonwal";

            TableToken dedupTt = fuzzer.createInitialTableWal(tableNameDedup);
            maybeConvertToParquet(tableNameDedup);
            fuzzer.createInitialTableNonWal(tableNameNoWal, null);

            String timestampColumnName;
            try (TableRecordMetadata meta = engine.getSequencerMetadata(dedupTt)) {
                timestampColumnName = meta.getColumnName(meta.getTimestampIndex());
            }

            long start = MicrosTimestampDriver.floor("2022-02-24T17");
            long end = start + fuzzer.partitionCount * Micros.DAY_MICROS;
            ObjList<FuzzTransaction> transactions = fuzzer.generateTransactions(tableNameDedup, rnd, start, end);

            try {
                long initialMinTs, initialMaxTs;
                try (TableReader reader = getReader(tableNameNoWal)) {
                    initialMinTs = reader.getMinTimestamp();
                    initialMaxTs = reader.getMaxTimestamp();
                }

                transactions = uniqueInserts(transactions, initialMinTs, initialMaxTs, 1000000L);
                WorkerPoolUtils.setupWriterJobs(sharedWorkerPool, engine);
                sharedWorkerPool.start(LOG);

                try {
                    fuzzer.applyNonWal(transactions, tableNameNoWal, rnd);

                    ObjList<FuzzTransaction> transactionsWithDups = duplicateInserts(transactions, rnd);
                    try {
                        execute("alter table " + tableNameDedup + " dedup upsert keys(ts)", sqlExecutionContext);
                        applyWal(transactionsWithDups, tableNameDedup, 1 + rnd.nextInt(4), rnd);

                        String limit = "";
                        TestUtils.assertSqlCursors(engine, sqlExecutionContext, tableNameNoWal + limit, tableNameDedup + limit, LOG);
                        fuzzer.assertRandomIndexes(tableNameNoWal, tableNameDedup, rnd);
                        // assert table count() values
                        fuzzer.assertCounts(tableNameDedup, timestampColumnName);
                        fuzzer.assertCounts(tableNameNoWal, timestampColumnName);
                        fuzzer.assertStringColDensity(tableNameDedup);
                    } finally {
                        Misc.freeObjListAndClear(transactionsWithDups);
                    }
                } finally {
                    sharedWorkerPool.halt();
                }
            } finally {
                Misc.freeObjListAndClear(transactions);
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

    private void testDedupWithRandomShiftAndStepAndColumnTops(Rnd rnd, short columType, String tableName) throws SqlException {
        ObjList<FuzzTransaction> transactions = new ObjList<>();
        try {
            long initialDelta = Micros.MINUTE_MICROS * 15;

            int initialDuplicates = 1 + rnd.nextInt(1);
            long startTimestamp = parseFloorPartialTimestamp("2020-02-24T04:30");
            int startCount = rnd.nextInt(24 * 60 / 15 * 10);
            generateInsertsTransactions(
                    transactions,
                    1,
                    startTimestamp,
                    initialDelta,
                    startCount,
                    initialDuplicates,
                    null,
                    rnd,
                    columType
            );
            long maxTimestamp = startTimestamp + startCount * initialDelta;
            LOG.info().$("adding rows with commit = 1 from=").$ts(startTimestamp).$(", to=").$ts(maxTimestamp).$();

            int transactionCount = 1 + rnd.nextInt(3);
            splitTransactionInserts(transactions, transactionCount, rnd);
            applyWal(transactions, tableName, 1, rnd);

            LOG.info().$("adding S column after ").$ts(maxTimestamp).$();
            execute("alter table " + tableName + " add column s " + ColumnType.nameOf(columType));
            execute("alter table " + tableName + " dedup upsert keys(ts, s)");

            int rndCount = rnd.nextInt(10);
            int strLen = 4 + rnd.nextInt(20);
            List<String> distinctSymbols = Arrays.stream(generateSymbols(rnd, 1 + rndCount, strLen, tableName)).distinct()
                    .collect(Collectors.toList());
            distinctSymbols.add(null);
            String[] symbols = new String[distinctSymbols.size()];
            distinctSymbols.toArray(symbols);
            String[] initialSymbols = symbols.length == 1
                    ? symbols
                    : Arrays.copyOf(symbols, 1 + rnd.nextInt(symbols.length - 1));

            long fromTops = startTimestamp + (startCount > 0 ? rnd.nextLong(startCount) : 0) * initialDelta;
            generateInsertsTransactions(
                    transactions,
                    1,
                    fromTops,
                    initialDelta,
                    startCount,
                    initialDuplicates,
                    initialSymbols,
                    rnd,
                    columType
            );
            LOG.info().$("adding more rows with commit = 1 from=").$ts(fromTops).$(", to=")
                    .$ts(fromTops + initialDelta * startCount).$();

            transactionCount = 1 + rnd.nextInt(3);
            splitTransactionInserts(transactions, transactionCount, rnd);
            applyWal(transactions, tableName, 1, rnd);

            transactions.clear();
            long shift = (startCount > 0 ? rnd.nextLong(startCount) : 0) * Micros.MINUTE_MICROS * 15 +
                    rnd.nextLong(15) * Micros.MINUTE_MICROS;
            long from = startTimestamp + shift;
            long delta = Micros.MINUTE_MICROS;
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
                    rnd,
                    columType
            );

            LOG.info().$("adding rows with commit = 2 from=").$ts(from).$(", to=")
                    .$ts(from + count * delta).$();

            transactionCount = 1 + rnd.nextInt(3);
            splitTransactionInserts(transactions, transactionCount, rnd);

            // adding rows with commit = 2 from=2020-02-25T10:29:00.000000Z, to=2020-02-26T10:29:00.000000Z
            applyWal(transactions, tableName, 1, rnd);
            validateNoTimestampDuplicates(tableName, from, delta, count, symbols, 1, columType);
        } finally {
            Misc.freeObjListAndClear(transactions);
        }
    }

    private String toCommaSeparatedString(RecordMetadata metadata, IntList upsertKeys) {
        StringSink sink = new StringSink();
        for (int i = 0; i < upsertKeys.size(); i++) {
            int columnType = metadata.getColumnType(upsertKeys.get(i));
            if (columnType > 0) {
                if (i > 0) {
                    sink.put(',');
                }
                sink.put(metadata.getColumnName(upsertKeys.get(i)));
            }
        }
        return sink.toString();
    }

    private String toCommaSeparatedStringDebug(RecordMetadata metadata, IntList upsertKeys) {
        StringSink sink = new StringSink();
        for (int i = 0; i < upsertKeys.size(); i++) {
            int columnType = metadata.getColumnType(upsertKeys.get(i));
            if (columnType > 0) {
                if (i > 0) {
                    sink.put(',');
                }
                sink.put(metadata.getColumnName(upsertKeys.get(i)) + ":" + ColumnType.nameOf(columnType));
            }
        }
        return sink.toString();
    }

    private ObjList<FuzzTransaction> uniqueInserts(ObjList<FuzzTransaction> transactions, long minTs, long maxTs, long step) {
        ObjList<FuzzTransaction> uniqueTransactions = new ObjList<>();
        LongHashSet uniqueTimestamps = new LongHashSet();

        for (int i = 0; i < transactions.size(); i++) {
            FuzzTransaction transaction = transactions.getQuick(i);
            if (!transaction.rollback) {
                FuzzTransaction unique = new FuzzTransaction();
                for (int j = 0; j < transaction.operationList.size(); j++) {
                    FuzzTransactionOperation operation = transaction.operationList.getQuick(j);
                    if (operation instanceof FuzzInsertOperation) {
                        long timestamp = ((FuzzInsertOperation) operation).getTimestamp();

                        boolean isInitialRange = timestamp >= minTs && timestamp <= maxTs && (timestamp - minTs) % step == 0;
                        if (!isInitialRange && uniqueTimestamps.add(timestamp)) {
                            unique.operationList.add(operation);
                        } else {
                            Misc.free(operation);
                        }
                    } else {
                        if (operation instanceof FuzzDropCreateTableOperation) {
                            ((FuzzDropCreateTableOperation) operation).setDedupEnable(true);
                        }
                        unique.operationList.add(operation);
                    }
                }
                unique.reopenTable = transaction.reopenTable;
                uniqueTransactions.add(unique);
            } else {
                uniqueTransactions.add(transaction);
            }
        }
        return uniqueTransactions;
    }

    private void validateNoTimestampDuplicates(
            String tableName,
            long fromTimestamp,
            long delta,
            long commit2Count,
            String[] symbols,
            int existingDups,
            int colType
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

        try (
                RecordCursorFactory factory = select(tableName);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            Record rec = cursor.getRecord();
            AssertionError fail = null;
            int dups = existingDups;

            while (cursor.hasNext()) {
                try {
                    long timestamp = rec.getTimestamp(0);
                    int commit = rec.getInt(1);
                    if (timestamp >= (fromTimestamp - Micros.MINUTE_MICROS * 5) || started) {
                        // Keep printing whole insert time range, regardless of the failures
                        started = true;
                        sink.putISODate(timestamp).put(',').put(commit);
                        if (symbols != null) {
                            sink.put(',');
                            CharSequence cs = readStrValue(rec, colType);
                            sink.put(cs != null ? cs : "<null>");
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
                            CharSequence sym = readStrValue(rec, colType);
                            if (sym == null) {
                                sym = nullSymbolValue;
                            }
                            int symbolIndex = symbolSet.get(sym);
                            if (symbolIndex < 0) {
                                Assert.fail("Unknown symbol found: timestamp " + Micros.toUSecString(timestamp) + ", symbol '" + sym + "'");
                            }
                            if (foundSymbols[symbolIndex]) {
                                Assert.fail("Duplicate timestamp " + Micros.toUSecString(timestamp) + " for symbol '" + sym + "'");
                            }
                            foundSymbols[symbolIndex] = true;
                        } else {
                            if (timestamp == lastTimestamp) {
                                if (++dups > existingDups) {
                                    Assert.fail("Duplicate timestamp " + Micros.toUSecString(timestamp));
                                }
                            } else {
                                dups = 1;
                            }
                        }

                        if (timestamp < lastTimestamp) {
                            Assert.fail("Out of order timestamp " +
                                    Micros.toUSecString(lastTimestamp) +
                                    " followed by " +
                                    Micros.toUSecString(timestamp)
                            );
                        }

                        if ((timestamp - fromTimestamp) % delta == 0) {
                            Assert.assertEquals("expected commit at timestamp " + Micros.toUSecString(timestamp), 2, commit);
                        }

                        Assert.assertTrue("commit must be 1 or 2", commit > 0);
                    }
                    lastTimestamp = timestamp;

                    if (timestamp > (toTimestamp + Micros.MINUTE_MICROS * 5)) {
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
        } catch (SqlException e) {
            Assert.fail(e.getMessage());
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
        public long preComputedStateSize() {
            return 0;
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
                CursorPrinter.printColumn(record, metadata, keyColumns.get(i), currentRecordKeys, false, false, "<null>");
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
