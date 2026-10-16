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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.LogRecordSinkAdapter;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
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
import io.questdb.std.Numbers;
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
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.questdb.test.tools.TestUtils.assertEquals;

public class DedupInsertFuzzTest extends AbstractFuzzTest {
    private static final String NULL_SYMBOL_VALUE = "nullSymbolValue_unlikely_to_be_generated_by_random";

    @Test
    public void testDedupWithRandomShiftAndStep() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = generateRandomAndProps();
            boolean createAsParquet = rnd.nextBoolean();
            boolean convertToParquet = !createAsParquet && rnd.nextBoolean();
            boolean isSkipMode = rnd.nextBoolean();

            String tableName = getTestName();
            createEmptyTable(tableName, "DEDUP " + dedupModeSql(rnd.nextBoolean()) + " keys(ts, commit)", createAsParquet);
            execute("alter table " + tableName + " dedup disable");
            execute("alter table " + tableName + " dedup enable " + dedupModeSql(isSkipMode) + " keys(ts)");

            ObjList<FuzzTransaction> transactions = new ObjList<>();
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
                maybeConvertToParquet(tableName, convertToParquet);
                ObjHashSet<String> firstBatchKeys = collectDedupKeys(transactions, false);
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

                splitTransactionInserts(transactions, 1 + rnd.nextInt(3), rnd);
                CommitOracle oracle = applyWithDedupModeSwitch(rnd, tableName, "ts", false, isSkipMode, transactions, firstBatchKeys);
                validateNoTimestampDuplicates(tableName, from, delta, count, null, 1, ColumnType.NULL, oracle);
            } finally {
                Misc.freeObjListAndClear(transactions);
            }
        });
    }

    @Test
    public void testDedupWithRandomShiftAndStepAndSymbolKey() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = generateRandomAndProps();
            boolean createAsParquet = rnd.nextBoolean();
            boolean convertToParquet = !createAsParquet && rnd.nextBoolean();
            boolean isSkipMode = rnd.nextBoolean();

            String tableName = getTestName();
            execute(
                    "create table " + tableName +
                            " (ts timestamp, commit int, s symbol) " +
                            " , index(s) timestamp(ts) partition by DAY " + (createAsParquet ? "FORMAT PARQUET " : "") + "WAL "
                            + " DEDUP " + dedupModeSql(isSkipMode) + " KEYS(ts, s)"
            );

            runDedupWithShiftAndStep(rnd, tableName, ColumnType.SYMBOL, convertToParquet, isSkipMode);
        });
    }

    @Test
    public void testDedupWithRandomShiftAndStepAndSymbolKeyAndColumnTops() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = generateRandomAndProps();
            boolean createAsParquet = rnd.nextBoolean();
            boolean convertToParquet = !createAsParquet && rnd.nextBoolean();

            String tableName = getTestName();
            execute(
                    "create table " + tableName +
                            " (ts timestamp, commit int) " +
                            " timestamp(ts) partition by DAY " + (createAsParquet ? "FORMAT PARQUET " : "") + "WAL "
                            + " DEDUP " + dedupModeSql(rnd.nextBoolean()) + " KEYS(ts)"
            );
            testDedupWithRandomShiftAndStepAndColumnTops(rnd, ColumnType.SYMBOL, tableName, convertToParquet);
        });
    }

    @Test
    public void testDedupWithRandomShiftAndStepAndVarcharKey() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = generateRandomAndProps();
            boolean createAsParquet = rnd.nextBoolean();
            boolean convertToParquet = !createAsParquet && rnd.nextBoolean();
            boolean isSkipMode = rnd.nextBoolean();

            String tableName = getTestName();
            execute(
                    "create table " + tableName +
                            " (ts timestamp, commit int, s varchar) " +
                            " timestamp(ts) partition by DAY " + (createAsParquet ? "FORMAT PARQUET " : "") + "WAL "
                            + " DEDUP " + dedupModeSql(isSkipMode) + " KEYS(ts, s)"
            );

            runDedupWithShiftAndStep(rnd, tableName, ColumnType.VARCHAR, convertToParquet, isSkipMode);
        });
    }

    @Test
    public void testDedupWithRandomShiftAndStepAndVarcharKeyAndColumnTops() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = generateRandomAndProps();
            boolean createAsParquet = rnd.nextBoolean();
            boolean convertToParquet = !createAsParquet && rnd.nextBoolean();

            String tableName = getTestName();
            execute(
                    "create table " + tableName +
                            " (ts timestamp, commit int) " +
                            " timestamp(ts) partition by DAY " + (createAsParquet ? "FORMAT PARQUET " : "") + "WAL "
                            + " DEDUP " + dedupModeSql(rnd.nextBoolean()) + " KEYS(ts)"
            );
            testDedupWithRandomShiftAndStepAndColumnTops(rnd, ColumnType.VARCHAR, tableName, convertToParquet);
        });
    }

    @Test
    public void testDedupWithRandomShiftAndStepWithExistingDups() throws Exception {
        assertMemoryLeak(() -> {
            ObjList<FuzzTransaction> transactions = new ObjList<>();
            Rnd rnd = generateRandomAndProps();
            boolean createAsParquet = rnd.nextBoolean();
            boolean convertToParquet = !createAsParquet && rnd.nextBoolean();

            String tableName = getTestName();
            createEmptyTable(tableName, "", createAsParquet);

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
                maybeConvertToParquet(tableName, convertToParquet);
                boolean isSkipMode = rnd.nextBoolean();
                execute("alter table " + tableName + " dedup " + dedupModeSql(isSkipMode) + " keys(ts)");

                ObjHashSet<String> firstBatchKeys = collectDedupKeys(transactions, false);
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

                splitTransactionInserts(transactions, 1 + rnd.nextInt(3), rnd);
                CommitOracle oracle = applyWithDedupModeSwitch(rnd, tableName, "ts", false, isSkipMode, transactions, firstBatchKeys);
                // SKIP keeps all pre-existing duplicates of a key, UPSERT replaces them with one row
                validateNoTimestampDuplicates(tableName, from, delta, count, null, initialDuplicates, ColumnType.NULL, oracle);
            } finally {
                Misc.freeObjListAndClear(transactions);
            }
        });
    }

    @Test
    public void testDedupWithRandomShiftWithColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            ObjList<FuzzTransaction> transactions = new ObjList<>();
            Rnd rnd = generateRandomAndProps();
            boolean createAsParquet = rnd.nextBoolean();
            boolean convertToParquet = !createAsParquet && rnd.nextBoolean();

            boolean isSkipMode = rnd.nextBoolean();

            String tableName = getTestName();
            createEmptyTable(tableName, "DEDUP " + dedupModeSql(isSkipMode) + " keys(ts)", createAsParquet);
            // the native twin takes the same commits, see assertByteColumnTopMatchesTwin()
            String twinTableName = tableName + "_native";
            createEmptyTable(twinTableName, "DEDUP " + dedupModeSql(isSkipMode) + " keys(ts)", false);
            Rnd twinRnd = new Rnd();

            long initialDelta = Micros.MINUTE_MICROS * 15;
            int initialCount = 2 * 24 * 5;
            node1.setProperty(PropertyKey.CAIRO_DEFAULT_SYMBOL_INDEX_TYPE, TestUtils.randomSymbolIndexTypeName(rnd));
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
            applyWal(transactions, twinTableName, 1, twinRnd);
            ObjHashSet<String> firstBatchKeys = collectDedupKeys(transactions, false);
            transactions.clear();

            for (String t : new String[]{tableName, twinTableName}) {
                execute("alter table " + t + " add s symbol index", sqlExecutionContext);
                // a column without a NULL value: the rows of the first batch read NULL only through its top
                execute("alter table " + t + " add b byte", sqlExecutionContext);
            }

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

            maybeConvertToParquet(tableName, convertToParquet);
            splitTransactionInserts(transactions, 1 + rnd.nextInt(3), rnd);
            // ts is the only dedup key, the rows of one timestamp differ in the s column
            CommitOracle oracle = applyWithDedupModeSwitch(rnd, tableName, twinTableName, twinRnd, "ts", false, isSkipMode, transactions, firstBatchKeys);
            validateNoTimestampDuplicates(tableName, from, delta, count, null, 1, ColumnType.NULL, oracle);
            replayStoredRows(rnd, tableName, twinTableName);
            assertByteColumnTopMatchesTwin(tableName, twinTableName);
        });
    }

    @Test
    public void testRandomColumnsDedupMultipleKeyCol() throws Exception {
        Rnd rnd = generateRandomAndProps();
        boolean createAsParquet = rnd.nextBoolean();
        boolean convertToParquet = !createAsParquet && rnd.nextBoolean();
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

        runFuzzWithRandomColsDedup(rnd, -1, createAsParquet, convertToParquet, rnd.nextBoolean());
    }

    @Test
    public void testRandomColumnsDedupMultipleKeyColWithRCommits() throws Exception {
        // Replace commits not yet supported with Parquet
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

        // a replace commit replaces its range in either dedup mode, the other commits deduplicate
        runFuzzWithRandomColsDedup(rnd, -1, false, false, rnd.nextBoolean());
    }

    @Test
    public void testRandomColumnsDedupOneKeyCol() throws Exception {
        Rnd rnd = generateRandomAndProps();
        boolean createAsParquet = rnd.nextBoolean();
        boolean convertToParquet = !createAsParquet && rnd.nextBoolean();
        setFuzzProbabilities(
                rnd.nextDouble() / 100,
                rnd.nextDouble(),
                rnd.nextDouble(),
                0.1 * rnd.nextDouble(),
                0.2 * rnd.nextDouble(),
                0,
                rnd.nextDouble(),
                0.0,
                rnd.nextDouble(),
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

        runFuzzWithRandomColsDedup(rnd, 1, createAsParquet, convertToParquet, rnd.nextBoolean());
    }

    @Test
    public void testRandomDedupRepeat() throws Exception {
        Rnd rnd = generateRandomAndProps();
        // The table does not start as FORMAT PARQUET: in that format, the commit that follows an O3
        // commit into the table emptied by TRUNCATE trips the last partition assertion in
        // TableWriter.processWalCommit(), with or without dedup.
        boolean convertToParquet = rnd.nextBoolean();
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
                // This test does not support Drop Partition operations,
                // it is not trivial to build the result set of data to assert against with drop partitions
                0.0,
                0.1 * rnd.nextDouble(),
                // uniqueInserts() makes the drop recreate the table with the same dedup key and mode
                0.25,
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

        runFuzzWithRepeatDedup(rnd, convertToParquet, rnd.nextBoolean());
    }

    // Collects the dedup keys of the stable inserts, see dedupKey().
    private static ObjHashSet<String> collectDedupKeys(ObjList<FuzzTransaction> transactions, boolean isSymbolKey) {
        ObjHashSet<String> keys = new ObjHashSet<>();
        for (int i = 0, n = transactions.size(); i < n; i++) {
            ObjList<FuzzTransactionOperation> operations = transactions.getQuick(i).operationList;
            for (int j = 0, m = operations.size(); j < m; j++) {
                if (operations.getQuick(j) instanceof FuzzStableInsertOperation insert) {
                    keys.add(dedupKey(insert.getTimestamp(), isSymbolKey ? insert.getSymbol() : null));
                }
            }
        }
        return keys;
    }

    // Identifies a row by the dedup key: ts, or ts and the s column. A row added before
    // the s column exists has null in it.
    private static String dedupKey(long timestamp, CharSequence symbol) {
        return timestamp + "|" + (symbol != null ? symbol : NULL_SYMBOL_VALUE);
    }

    private static String dedupModeSql(boolean isSkipMode) {
        return isSkipMode ? "SKIP" : "UPSERT";
    }

    private static boolean hasInsertCommit(ObjList<FuzzTransaction> transactions) {
        for (int i = 0, n = transactions.size(); i < n; i++) {
            if (isInsertCommit(transactions.getQuick(i))) {
                return true;
            }
        }
        return false;
    }

    // A committed batch of inserts, which addDuplicates() can repeat in a duplicate commit
    private static boolean isInsertCommit(FuzzTransaction transaction) {
        return !transaction.rollback && transaction.operationList.size() > 1 && !transaction.hasReplaceRange();
    }

    // Whether the table has a live BOOLEAN, BYTE or SHORT column, the types without a NULL value
    private static boolean hasNoSentinelColumn(TableToken tableToken) {
        try (TableReader reader = getReader(tableToken)) {
            final TableReaderMetadata metadata = reader.getMetadata();
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                final int columnType = metadata.getColumnType(i);
                if (columnType == ColumnType.BOOLEAN || columnType == ColumnType.BYTE || columnType == ColumnType.SHORT) {
                    return true;
                }
            }
        }
        return false;
    }

    // When every re-inserted row is a duplicate, SKIP leaves native and Parquet partitions as they are. UPSERT
    // keeps a native partition only after it checks that the new rows carry the values the partition has. The
    // Parquet merge makes that check only for tables with a BOOLEAN, BYTE or SHORT column, the types whose
    // column tops a merge would turn into 0/false: without such a column, an UPSERT O3 merge into a Parquet
    // partition writes the file even when the new rows equal the stored ones, so only native partitions must
    // keep their name txn.
    private static String readPartitionsToString(TableToken tableToken, boolean isUnchangedParquet) {
        final String partitions = readTxnToString(tableToken, false, true, true, false);
        return isUnchangedParquet ? partitions : partitions.replaceAll("txn: -?\\d+, parquetSize: \\d+", "parquet");
    }

    private void addDuplicates(ObjList<FuzzTransaction> transactions, Rnd rnd, IntList upsertKeyIndexes, boolean isSkipMode) {
        int duplicateCommits = rnd.nextInt(transactions.size());

        IntHashSet upsertKeyIndexesMap = new IntHashSet();
        upsertKeyIndexesMap.addAll(upsertKeyIndexes);
        int lastInsertIndex = -1;

        for (int i = 0; i < transactions.size(); i++) {
            FuzzTransaction transaction = transactions.getQuick(i);
            if (!isInsertCommit(transaction)) {
                continue;
            }
            lastInsertIndex = i;

            if (duplicateCommits > 0 && rnd.nextInt(duplicateCommits) > (transactions.size() - i - duplicateCommits)) {
                FuzzTransaction duplicateTrans = newDuplicateCommit(transaction, rnd, upsertKeyIndexesMap, isSkipMode);
                if (duplicateTrans != null) {
                    transactions.insert(i + 1, 1, duplicateTrans);
                    i++;
                }
                duplicateCommits--;
            }
        }

        if (isSkipMode && lastInsertIndex > -1) {
            // Whatever the loop added, the last commit re-sends the keys of the last insert commit
            // with other values, which the table must not take
            FuzzTransaction duplicateTrans = newDuplicateCommit(transactions.getQuick(lastInsertIndex), rnd, upsertKeyIndexesMap, true);
            if (duplicateTrans != null) {
                transactions.add(duplicateTrans);
            }
        }
    }

    // Writes the second batch to the WAL in two runs with an ALTER between them that re-enables the dedup in a
    // random mode. The WAL is applied after all of it is written, so the apply job meets the mode switch in the
    // middle of one pass, possibly with rows of the first run still in the O3 lag.
    private CommitOracle applyWithDedupModeSwitch(
            Rnd rnd,
            String tableName,
            String dedupKeys,
            boolean isSymbolKey,
            boolean isSkipMode,
            ObjList<FuzzTransaction> transactions,
            ObjHashSet<String> firstBatchKeys
    ) throws SqlException {
        return applyWithDedupModeSwitch(rnd, tableName, null, null, dedupKeys, isSymbolKey, isSkipMode, transactions, firstBatchKeys);
    }

    // twinTableName (null: none) takes the same commits with the same mode switch, after tableName
    private CommitOracle applyWithDedupModeSwitch(
            Rnd rnd,
            String tableName,
            String twinTableName,
            Rnd twinRnd,
            String dedupKeys,
            boolean isSymbolKey,
            boolean isSkipMode,
            ObjList<FuzzTransaction> transactions,
            ObjHashSet<String> firstBatchKeys
    ) throws SqlException {
        int switchIndex = rnd.nextInt(transactions.size() + 1);
        boolean isSkipAfterSwitch = rnd.nextBoolean();
        ObjList<FuzzTransaction> beforeSwitch = new ObjList<>();
        beforeSwitch.addAll(transactions, 0, switchIndex);
        ObjList<FuzzTransaction> afterSwitch = new ObjList<>();
        afterSwitch.addAll(transactions, switchIndex, transactions.size());

        LOG.info().$("switching dedup mode [table=").$(tableName)
                .$(", from=").$(dedupModeSql(isSkipMode))
                .$(", to=").$(dedupModeSql(isSkipAfterSwitch))
                .$(", afterTransactions=").$(switchIndex)
                .$(", transactions=").$(transactions.size())
                .I$();
        fuzzer.applyToWal(beforeSwitch, tableName, 1, rnd);
        execute("alter table " + tableName + " dedup enable " + dedupModeSql(isSkipAfterSwitch) + " keys(" + dedupKeys + ")");
        applyWal(afterSwitch, tableName, 1, rnd);
        if (twinTableName != null) {
            fuzzer.applyToWal(beforeSwitch, twinTableName, 1, twinRnd);
            execute("alter table " + twinTableName + " dedup enable " + dedupModeSql(isSkipAfterSwitch) + " keys(" + dedupKeys + ")");
            applyWal(afterSwitch, twinTableName, 1, twinRnd);
        }

        return new CommitOracle(
                firstBatchKeys,
                collectDedupKeys(beforeSwitch, isSymbolKey),
                isSkipMode,
                collectDedupKeys(afterSwitch, isSymbolKey),
                isSkipAfterSwitch
        );
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

    // Changes the BYTE column b of the table and of its native twin to SYMBOL, which reads the rows below a
    // column top as NULL and a materialized top as 0, then compares the tables
    private void assertByteColumnTopMatchesTwin(String tableName, String twinTableName) throws SqlException {
        for (String t : new String[]{tableName, twinTableName}) {
            execute("alter table " + t + " alter column b type symbol", sqlExecutionContext);
        }
        drainWalQueue();
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName(tableName)));
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName(twinTableName)));
        assertSqlCursors(twinTableName, tableName);
    }

    private void assertSqlCursorsNoDups(
            String tableNameNoWal,
            ObjList<CharSequence> upsertKeyNames,
            String tableNameWal,
            boolean isSkipMode
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
                            RecordCursor dedupWrapper = new DedupCursor(factory.getMetadata(), cursor1, previewCursor, upsertKeyNames, isSkipMode);
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

    private void createEmptyTable(String tableName, String dedupOption, boolean createAsParquet) throws SqlException {
        String format = createAsParquet ? "FORMAT PARQUET " : "";
        execute("create table " + tableName + " (ts timestamp, commit int) timestamp(ts) partition by DAY " + format + "WAL " + dedupOption
                , sqlExecutionContext);
    }

    // Adds rows that repeat the timestamp, the dedup key, of an insert with random values in the other columns.
    // UPSERT keeps the row written last, so a duplicate goes before its original: earlier in the same transaction
    // or into the previous insert transaction. SKIP keeps the row written first, so a duplicate goes after its
    // original: later in the same transaction or into the next insert transaction. Either way, the table must end
    // up with the original rows.
    private ObjList<FuzzTransaction> duplicateInserts(ObjList<FuzzTransaction> transactions, Rnd rnd, boolean isSkipMode) {
        ObjList<FuzzTransaction> result = new ObjList<>();
        FuzzTransaction prevInsertTrans = null;
        ObjList<FuzzTransactionOperation> nextInsertDuplicates = new ObjList<>();

        for (int i = 0; i < transactions.size(); i++) {
            FuzzTransaction transaction = transactions.getQuick(i);

            if (!transaction.rollback && transaction.operationList.size() > 1) {
                int size = transaction.operationList.size();
                FuzzTransaction duplicateTrans = new FuzzTransaction();
                // SKIP: the originals of these duplicates are in the previous insert transaction
                duplicateTrans.operationList.addAll(nextInsertDuplicates);
                nextInsertDuplicates.clear();
                for (int op = 0; op < size; op++) {
                    int dupStep = Math.max(2, transaction.operationList.size() / (1 + rnd.nextInt(10)));
                    FuzzTransactionOperation operation = transaction.operationList.getQuick(op);
                    if (operation instanceof FuzzInsertOperation insertOperation && op % dupStep == 1) {
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
                        if (isSkipMode) {
                            duplicateTrans.operationList.add(operation);
                            if (rnd.nextBoolean()) {
                                nextInsertDuplicates.add(duplicate);
                            } else {
                                duplicateTrans.operationList.add(duplicate);
                            }
                            continue;
                        }
                        if (prevInsertTrans != null && rnd.nextBoolean()) {
                            prevInsertTrans.operationList.add(duplicate);
                        } else {
                            duplicateTrans.operationList.add(duplicate);
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
                        // the operation removes the originals, their duplicates must not land in an empty range
                        prevInsertTrans = null;
                        Misc.freeObjListAndClear(nextInsertDuplicates);
                    }
                }
                result.add(transaction);
            }
        }

        if (nextInsertDuplicates.size() > 0) {
            // SKIP: the duplicates of the last insert transaction go into a commit of their own
            FuzzTransaction duplicateTrans = new FuzzTransaction();
            duplicateTrans.operationList.addAll(nextInsertDuplicates);
            result.add(duplicateTrans);
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

    private void maybeConvertToParquet(String tableName, boolean convertToParquet) {
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

    // Returns a commit that repeats the dedup keys of some rows of the transaction, or null when it picks none.
    private FuzzTransaction newDuplicateCommit(
            FuzzTransaction transaction,
            Rnd rnd,
            IntHashSet upsertKeyIndexesMap,
            boolean isSkipMode
    ) {
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

                // Generate only 1 non-identical record, compares will have work harder to find 1 record only.
                // SKIP keeps the row written first, so there every duplicate differs from its original outside
                // the dedup keys and the table must keep the original values.
                boolean generatedNonIdentical = isSkipMode || (generateNonIdenticalCount > 0 && rnd.nextBoolean());
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
            return duplicateTrans;
        }
        return null;
    }

    private CharSequence readStrValue(Record rec, int colType) {
        return switch (colType) {
            case ColumnType.SYMBOL -> rec.getSymA(2);
            case ColumnType.STRING -> rec.getStrA(2);
            case ColumnType.VARCHAR -> Utf8s.toString(rec.getVarcharA(2));
            default -> throw new IllegalArgumentException();
        };
    }

    // Copies rows of the reference table, which has every written row, into the dedup table. UPSERT keeps the
    // last copy of a key, which is the row the table already has. SKIP drops every copy, including those that
    // differ from the kept row outside the dedup keys. Either way, no native partition gets rewritten, and with
    // SKIP no Parquet partition either.
    private void reinsertWrittenRows(
            Rnd rnd,
            String tableNameDedup,
            String tableNameReference,
            String timestampColumnName,
            boolean isSkipMode
    ) throws SqlException {
        execute("ALTER TABLE \"" + tableNameDedup + "\" SQUASH PARTITIONS", sqlExecutionContext);
        drainWalQueue();

        LOG.info().$("Re-inserting written rows into table ").$(tableNameDedup).$();
        TableToken tt = engine.verifyTableName(tableNameDedup);
        final boolean isUnchangedParquet = isSkipMode || hasNoSentinelColumn(tt);
        String partitions = readPartitionsToString(tt, isUnchangedParquet);
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
            // maxTs > minTs condition prevents arithmetic exception thrown from rnd.nextLong() when maxTs = minTs
            long fromTs = minTs + (maxTs > minTs ? rnd.nextLong(maxTs - minTs) : 0);
            long toTs = (long) (fromTs + Micros.DAY_MICROS * Math.pow(1.2, rnd.nextDouble()));

            // no LIMIT: it could cut the rows of one key and leave UPSERT with a row that is not the last
            String insertSql = "insert into " + tableNameDedup +
                    " select * from " + tableNameReference +
                    " where " + timestampColumnName + " >= " + fromTs + " and ts < " + toTs;

            execute(insertSql);
        }
        drainWalQueue();

        Assert.assertFalse("table should not be suspended", engine.getTableSequencerAPI().getTxnTracker(tt).isSuspended());
        String partitionsAfter = readPartitionsToString(tt, isUnchangedParquet);
        Assert.assertEquals("partitions should be not rewritten", partitions, partitionsAfter);
    }

    // Switches the table and its native twin to UPSERT and resends stored rows unchanged, as an at-least-once
    // client does after a reconnect: first rows of a random interval, which every partition keeps as it is, then
    // the rows of a day up to its last row together with a new row 1us after it, which appends to the partition of
    // that day. The rows of the first batch hold NULL in s and b, as their column tops read.
    private void replayStoredRows(Rnd rnd, String tableName, String twinTableName) throws SqlException {
        for (String t : new String[]{tableName, twinTableName}) {
            execute("alter table " + t + " dedup upsert keys(ts)", sqlExecutionContext);
        }
        drainWalQueue();
        final TableToken tableToken = engine.verifyTableName(tableName);
        final TableToken twinToken = engine.verifyTableName(twinTableName);
        final long minTs, maxTs;
        try (TableReader reader = getReader(tableName)) {
            if (reader.size() == 0) {
                return;
            }
            minTs = reader.getMinTimestamp();
            maxTs = reader.getMaxTimestamp();
        }

        final long replayLo = minTs + (maxTs > minTs ? rnd.nextLong(maxTs - minTs) : 0);
        final long replayHi = replayLo + rnd.nextLong(Micros.DAY_MICROS);
        LOG.info().$("replaying stored rows [table=").$(tableName).$(", lo=").$ts(replayLo).$(", hi=").$ts(replayHi).I$();
        final String partitions = readTxnToString(tableToken, false, true, true, true);
        final String twinPartitions = readTxnToString(twinToken, false, true, true, true);
        for (String t : new String[]{tableName, twinTableName}) {
            execute("insert into " + t + " select * from " + t + " where ts >= " + replayLo + " and ts <= " + replayHi);
        }
        drainWalQueue();
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(twinToken));
        Assert.assertEquals(twinPartitions, readTxnToString(twinToken, false, true, true, true));
        Assert.assertEquals(partitions, readTxnToString(tableToken, false, true, true, true));

        // the last row of the day of replayLo, the new row goes 1us after it when that is still the same day
        final long dayLo = Micros.floorDD(replayLo);
        long dayMax = Long.MIN_VALUE;
        try (
                RecordCursorFactory factory = select("select max(ts) from " + tableName + " where ts >= " + dayLo + " and ts < " + (dayLo + Micros.DAY_MICROS));
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            if (cursor.hasNext()) {
                dayMax = cursor.getRecord().getTimestamp(0);
            }
        }
        if (dayMax != Numbers.LONG_NULL && dayMax != Long.MIN_VALUE && dayMax + 1 < dayLo + Micros.DAY_MICROS) {
            final long appendLo = Math.max(dayLo, dayMax - rnd.nextLong(Micros.DAY_MICROS));
            LOG.info().$("replaying stored rows with an append [table=").$(tableName).$(", lo=").$ts(appendLo).$(", dayMax=").$ts(dayMax).I$();
            for (String t : new String[]{tableName, twinTableName}) {
                execute("insert into " + t + " select * from ("
                        + "select ts, commit, s, b from " + t + " where ts >= " + appendLo + " and ts <= " + dayMax
                        + " union all select ts + 1 ts, commit, s, b from " + t + " where ts = " + dayMax + ")");
            }
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(twinToken));
        }
    }

    private void runDedupWithShiftAndStep(
            Rnd rnd,
            String tableName,
            int colType,
            boolean convertToParquet,
            boolean isSkipMode
    ) throws SqlException {
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
            maybeConvertToParquet(tableName, convertToParquet);

            ObjHashSet<String> firstBatchKeys = collectDedupKeys(transactions, true);
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
            CommitOracle oracle = applyWithDedupModeSwitch(rnd, tableName, "ts, s", true, isSkipMode, transactions, firstBatchKeys);

            validateNoTimestampDuplicates(tableName, from, delta, count, symbols, 1, colType, oracle);
        } finally {
            Misc.freeObjListAndClear(transactions);
        }
    }

    private void runFuzzWithRandomColsDedup(
            Rnd rnd,
            int dedupKeys,
            boolean createAsParquet,
            boolean convertToParquet,
            boolean isSkipMode
    ) throws Exception {
        assertMemoryLeak(() -> {
            String tableNameBase = getTestName();
            String tableNameDedup = tableNameBase + "_wal";
            String tableNameWalNoDedup = tableNameBase + "_nodedup";

            // The reference table stays native, see DedupCursor
            setCreateWalAsParquet(false);
            fuzzer.createInitialTableWal(tableNameWalNoDedup);
            setCreateWalAsParquet(createAsParquet);
            fuzzer.createInitialTableWal(tableNameDedup);
            maybeConvertToParquet(tableNameDedup, convertToParquet);

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
                    // without an insert commit, the run writes no duplicates for the dedup to resolve
                    for (int attempt = 0; attempt < 10 && !hasInsertCommit(transactions); attempt++) {
                        Misc.freeObjListAndClear(transactions);
                        transactions = generateSet(rnd, sequencerMetadata, readerMetadata, start, end, tableNameWalNoDedup);
                    }
                    comaSeparatedUpsertCols = toCommaSeparatedString(readerMetadata, upsertKeyIndexes);
                }
                String alterStatement = String.format(
                        "alter table %s dedup %s keys(%s)",
                        tableNameDedup,
                        dedupModeSql(isSkipMode),
                        comaSeparatedUpsertCols
                );
                LOG.info().$("enabling dedup: ").$(alterStatement).$();
                execute(alterStatement);

                WorkerPoolUtils.setupWriterJobs(sharedWorkerPool, engine);
                sharedWorkerPool.start(LOG);

                try {
                    addDuplicates(transactions, rnd, upsertKeyIndexes, isSkipMode);
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
                            tableNameDedup,
                            isSkipMode
                    );

                    fuzzer.assertCounts(tableNameDedup, timestampColumnName);
                    fuzzer.assertStringColDensity(tableNameDedup);

                    // Re-insert every written row of random intervals, duplicates included, into the table with
                    // dedup enabled. The rows go in commit order, so the dedup must keep the same rows as before.
                    reinsertWrittenRows(
                            rnd,
                            tableNameDedup,
                            tableNameWalNoDedup,
                            timestampColumnName,
                            isSkipMode
                    );
                    assertSqlCursorsNoDups(
                            tableNameWalNoDedup,
                            upsertKeyNames,
                            tableNameDedup,
                            isSkipMode
                    );

                } finally {
                    sharedWorkerPool.halt();
                }
            } finally {
                Misc.freeObjListAndClear(transactions);
            }
        });
    }

    private void runFuzzWithRepeatDedup(Rnd rnd, boolean convertToParquet, boolean isSkipMode) throws Exception {
        assertMemoryLeak(() -> {
            String tableNameBase = getTestName();
            String tableNameDedup = tableNameBase + "_wal";
            String tableNameNoWal = tableNameBase + "_nonwal";

            TableToken dedupTt = fuzzer.createInitialTableWal(tableNameDedup);
            maybeConvertToParquet(tableNameDedup, convertToParquet);
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

                final int dedupMode = isSkipMode ? TableUtils.DEDUP_MODE_SKIP : TableUtils.DEDUP_MODE_UPSERT;
                transactions = uniqueInserts(transactions, initialMinTs, initialMaxTs, 1000000L, dedupMode);
                WorkerPoolUtils.setupWriterJobs(sharedWorkerPool, engine);
                sharedWorkerPool.start(LOG);

                try {
                    fuzzer.applyNonWal(transactions, tableNameNoWal, rnd);

                    ObjList<FuzzTransaction> transactionsWithDups = duplicateInserts(transactions, rnd, isSkipMode);
                    try {
                        LOG.info().$("enabling dedup [table=").$(tableNameDedup).$(", mode=").$(dedupModeSql(isSkipMode)).I$();
                        execute("alter table " + tableNameDedup + " dedup " + dedupModeSql(isSkipMode) + " keys(ts)", sqlExecutionContext);
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

    private void testDedupWithRandomShiftAndStepAndColumnTops(
            Rnd rnd,
            short columType,
            String tableName,
            boolean convertToParquet
    ) throws SqlException {
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
            // Parquet partitions miss the s column, which the dedup compares as a column top
            maybeConvertToParquet(tableName, convertToParquet);

            LOG.info().$("adding S column after ").$ts(maxTimestamp).$();
            execute("alter table " + tableName + " add column s " + ColumnType.nameOf(columType));
            boolean isSkipMode = rnd.nextBoolean();
            execute("alter table " + tableName + " dedup " + dedupModeSql(isSkipMode) + " keys(ts, s)");

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

            // the rows added before the s column have null in it
            ObjHashSet<String> firstBatchKeys = collectDedupKeys(transactions, true);
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
            CommitOracle oracle = applyWithDedupModeSwitch(rnd, tableName, "ts, s", true, isSkipMode, transactions, firstBatchKeys);
            validateNoTimestampDuplicates(tableName, from, delta, count, symbols, 1, columType, oracle);
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

    private ObjList<FuzzTransaction> uniqueInserts(
            ObjList<FuzzTransaction> transactions,
            long minTs,
            long maxTs,
            long step,
            int dedupMode
    ) {
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
                            ((FuzzDropCreateTableOperation) operation).setDedupEnable(true, dedupMode);
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
            int colType,
            CommitOracle oracle
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
        boolean[] foundSymbols = null;
        if (symbols != null) {
            for (int i = 0; i < symbols.length; i++) {
                if (symbols[i] != null) {
                    symbolSet.put(symbols[i], i);
                } else {
                    symbolSet.put(NULL_SYMBOL_VALUE, i);
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
                        CharSequence keySymbol = null;
                        if (symbols != null) {
                            if (timestamp > lastTimestamp && lastTimestamp >= fromTimestamp) {
                                // New timestamp, check all symbols were set
                                // and reset foundSymbols to false
                                assertAllSymbolsSet(foundSymbols, symbols, lastTimestamp);
                            }
                            CharSequence sym = readStrValue(rec, colType);
                            if (sym == null) {
                                sym = NULL_SYMBOL_VALUE;
                            }
                            keySymbol = sym;
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
                            // a SKIP commit must not overwrite a row that has the same dedup key and a different commit
                            String key = dedupKey(timestamp, keySymbol);
                            Assert.assertEquals(
                                    "expected commit at timestamp " + Micros.toUSecString(timestamp) + ", key " + key,
                                    oracle.expectedCommit(key),
                                    commit
                            );
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

    // Predicts the commit column of the row that the dedup keeps for a key. The first batch writes commit=1 rows,
    // the second batch writes commit=2 rows in two runs with a dedup mode switch between them. UPSERT replaces
    // the existing row, SKIP keeps it and drops the new one. So the row of a run in UPSERT mode that wrote the
    // key survives. Without one, the first batch row survives, and without that, the row the second batch wrote
    // first.
    private record CommitOracle(
            ObjHashSet<String> firstBatchKeys,
            ObjHashSet<String> beforeSwitchKeys,
            boolean isSkipBeforeSwitch,
            ObjHashSet<String> afterSwitchKeys,
            boolean isSkipAfterSwitch
    ) {
        int expectedCommit(String key) {
            if ((!isSkipAfterSwitch && afterSwitchKeys.contains(key)) || (!isSkipBeforeSwitch && beforeSwitchKeys.contains(key))) {
                return 2;
            }
            return firstBatchKeys.contains(key) ? 1 : 2;
        }
    }

    // Deduplicates the rows of a reference table that has no dedup: keeps the first (SKIP) or the last (UPSERT)
    // row of each dedup key, in the order the reference table returns the rows of one timestamp. That order is
    // the commit order only when the reference table is native. A Parquet O3 merge without dedup puts a new row
    // that lands on a row group boundary timestamp before the existing rows of the next row group.
    private static class DedupCursor implements RecordCursor {

        private final StringSink currentRecordKeys = new StringSink();
        // SKIP keeps the first row of each duplicate group, UPSERT keeps the last one
        private final boolean isKeepFirst;
        private final IntList keyColumns = new IntList();
        private final ObjHashSet<String> keyProcessed = new ObjHashSet<>();
        private final RecordMetadata metadata;
        private final RecordCursor nextRecCursor;
        private final StringSink nextRecordKeys = new StringSink();
        private final RecordCursor recordCursor;
        private final IntList skipRecords = new IntList();
        ObjList<String> recordKeys = new ObjList<>();
        private long lastTimestamp = -1;
        // records of the previous timestamp group, after the last record dispatched from it, which the
        // inner cursor still has to skip; it is non-zero only when the last record of a group is a dropped duplicate
        private int skipCarry;
        private int timestampColIndex = -1;

        public DedupCursor(
                RecordMetadata cursorMetadata,
                RecordCursor innerCursor,
                RecordCursor previewCursor,
                ObjList<CharSequence> keyColumnNames,
                boolean isKeepFirst
        ) {
            this.recordCursor = innerCursor;
            this.isKeepFirst = isKeepFirst;
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

                    // unleash the first or the last records for each key, in ascending position order
                    if (isKeepFirst) {
                        for (int i = 0, n = recordKeys.size(); i < n; i++) {
                            if (keyProcessed.add(recordKeys.get(i))) {
                                skipRecords.add(i);
                            }
                        }
                    } else {
                        for (int i = recordKeys.size() - 1; i > -1; i--) {
                            if (keyProcessed.add(recordKeys.get(i))) {
                                skipRecords.add(i);
                            }
                        }
                        reverse(skipRecords);
                    }
                    final int groupSize = recordKeys.size();
                    recordKeys.clear();
                    keyProcessed.clear();

                    // find differences between indexes in skipRecords, the inner cursor sits on the record
                    // before the group, minus the records left over from the previous group
                    int last = -1 - skipCarry;
                    for (int i = 0; i < skipRecords.size(); i++) {
                        int val = skipRecords.get(i);
                        skipRecords.set(i, val - last);
                        last = val;
                    }
                    skipCarry = groupSize - 1 - last;

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
            skipCarry = 0;
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
