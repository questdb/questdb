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

import io.questdb.cairo.TableReader;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.StringSink;
import io.questdb.test.fuzz.FuzzStableInsertOperation;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.fuzz.FuzzTransactionOperation;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class DedupInsertFuzzTest extends AbstractFuzzTest {
    @Test
    public void testDedupWithRandomShiftAndStep() throws SqlException {
        String tableName = testName.getMethodName();
        createEmptyTable(tableName);

        ObjList<FuzzTransaction> transactions = new ObjList<>();
        Rnd rnd = generateRandom(LOG);
        long initialDelta = Timestamps.MINUTE_MICROS * 15;
        int initialCount = 4 * 24 * 5;
        transactions.add(
                generateInsertsTransactions(
                        1,
                        "2020-02-24T04:30",
                        initialDelta,
                        initialCount,
                        1 + rnd.nextInt(1),
                        null,
                        rnd
                )
        );
        applyWal(transactions, tableName, 1, rnd);

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
        validateNoTimestampDuplicates(tableName, from, delta, count, null);
    }

    @Test
    public void testDedupWithRandomShiftAndStepAndSymbolKey() throws SqlException {
        String tableName = testName.getMethodName();
        compile(
                "create table " + tableName +
                        " (ts timestamp, commit int, s symbol) " +
                        " , index(s) timestamp(ts) partition by DAY WAL " +
                        " DEDUPLICATE(s)",
                sqlExecutionContext
        );

        ObjList<FuzzTransaction> transactions = new ObjList<>();
        Rnd rnd = generateRandom(LOG);
        long initialDelta = Timestamps.MINUTE_MICROS * 15;
        String[] symbols = generateSymbols(rnd, 1 + rnd.nextInt(10), 4, tableName);
        String[] initialSymbols = Arrays.copyOf(symbols, rnd.nextInt(symbols.length));
        transactions.add(
                generateInsertsTransactions(
                        1,
                        "2020-02-24T04:30",
                        initialDelta,
                        4 * 24 * 5,
                        1 + rnd.nextInt(1),
                        initialSymbols,
                        rnd
                )
        );

        applyWal(transactions, tableName, 1, rnd);

        transactions.clear();
        long shift = rnd.nextLong(4 * 24 * 5) * Timestamps.MINUTE_MICROS * 15 +
                rnd.nextLong(15) * Timestamps.MINUTE_MICROS;
        String from = Timestamps.toUSecString(parseFloorPartialTimestamp("2020-02-24") + shift);
        long delta = Timestamps.MINUTE_MICROS;
        int count = rnd.nextInt(48) * 60;
        int rowsWithSameTimestamp = 1 + rnd.nextInt(2);
        transactions.add(
                generateInsertsTransactions(
                        2,
                        from,
                        delta,
                        count,
                        rowsWithSameTimestamp,
                        symbols,
                        rnd
                )
        );

        applyWal(transactions, tableName, 1, rnd);
        validateNoTimestampDuplicates(tableName, from, delta, count, symbols);
    }

    private void assertAllSet(
            boolean[] foundSymbols,
            String[] symbols,
            long timestamp
    ) {
        for (int i = 0; i < foundSymbols.length; i++) {
            if (!foundSymbols[i]) {
                CharSequence symbol = symbols[i];
                Assert.fail("Symbol " + symbol + " not found for timestamp " + Timestamps.toUSecString(timestamp));
            }
            foundSymbols[i] = false;
        }
    }

    private void createEmptyTable(String tableName) throws SqlException {
        compile("create table " + tableName + " (ts timestamp, commit int) timestamp(ts) partition by DAY WAL DEDUP"
                , sqlExecutionContext);
    }

    private FuzzTransaction generateInsertsTransactions(
            int commit,
            String from,
            long delta,
            int count,
            int rowsWithSameTimestamp,
            String[] symbols,
            Rnd rnd
    ) {
        FuzzTransaction transaction = new FuzzTransaction();
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
        return transaction;
    }

    private long parseFloorPartialTimestamp(String from) {
        try {
            return IntervalUtils.parseFloorPartialTimestamp(from);
        } catch (NumericException e) {
            throw new RuntimeException(e);
        }
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

    private void validateNoTimestampDuplicates(String tableName, String from, long delta, long commit2Count, String[] symbols) {

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
            var cursor = rdr.getCursor();
            var rec = cursor.getRecord();
            AssertionError fail = null;
            while (cursor.hasNext()) {
                try {
                    long timestamp = rec.getTimestamp(0);
                    int commit = rec.getInt(1);
                    if (timestamp >= (fromTimestamp - Timestamps.MINUTE_MICROS * 5) || started) {
                        started = true;
                        sink.putISODate(timestamp).put(',').put(commit);
                        if (symbols != null) {
                            sink.put(',').put(rec.getSym(2));
                        }
                        sink.put('\n');
                    }

                    if (fail == null) {
                        if (timestamp == lastTimestamp) {
                            if (symbols == null) {
                                Assert.fail("Duplicate timestamp " + Timestamps.toUSecString(timestamp));
                            }
                            int symbolIndex = symbolSet.get(rec.getSym(2));
                            if (foundSymbols[symbolIndex]) {
                                Assert.fail("Duplicate timestamp " + Timestamps.toUSecString(timestamp) + " for symbol " + rec.getSym(2));
                            }
                            foundSymbols[symbolIndex] = true;
                        }

                        if (timestamp < lastTimestamp) {
                            Assert.fail("Out of order timestamp " +
                                    Timestamps.toUSecString(lastTimestamp) +
                                    " followed by " +
                                    Timestamps.toUSecString(timestamp)
                            );
                        }

                        if (timestamp > fromTimestamp
                                && (timestamp - fromTimestamp) % delta == 0
                                && timestamp < toTimestamp) {
                            Assert.assertEquals("expected commit at timestamp " + Timestamps.toUSecString(timestamp), 2, commit);
                        }

                        Assert.assertTrue("commit must be 1 or 2", commit > 0);
                        if (symbols != null) {
                            if (timestamp > lastTimestamp && lastTimestamp != Long.MIN_VALUE) {
                                assertAllSet(foundSymbols, symbols, lastTimestamp);
                            }
                        }
                    }
                    lastTimestamp = timestamp;

                    if (timestamp > (toTimestamp + Timestamps.MINUTE_MICROS * 5)) {
                        break;
                    }
                } catch (AssertionError e) {
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
