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
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.StringSink;
import io.questdb.test.fuzz.FuzzStableInsertOperation;
import io.questdb.test.fuzz.FuzzTransaction;
import org.junit.Assert;
import org.junit.Test;

public class DedupInsertTest extends AbstractFuzzTest {
    @Test
    public void testDedupWithRandomShiftAndStep() throws SqlException {
        String tableName = testName.getMethodName();
        createEmptyTable(tableName);

        ObjList<FuzzTransaction> transactions = new ObjList<>();
        Rnd rnd = generateRandom(LOG, 34097923623125L, 1687285677830L);
        long initialDelta = Timestamps.MINUTE_MICROS * 15;
        transactions.add(
                generateInsertsTransactions(
                        1,
                        "2020-02-24T04:30",
                        initialDelta,
                        4 * 24 * 5, 1
                )
        );
        applyWal(transactions, tableName, 1, rnd);

        transactions.clear();
        long shift = rnd.nextLong(4 * 24 * 5) * Timestamps.MINUTE_MICROS * 15 +
                rnd.nextLong(15) * Timestamps.MINUTE_MICROS;
        String from = Timestamps.toUSecString(parseFloorPartialTimestamp("2020-02-24") + shift);
        long delta = Timestamps.MINUTE_MICROS;
        int count = rnd.nextInt(48) * 60;
        transactions.add(
                generateInsertsTransactions(
                        2,
                        from,
                        delta,
                        count, rnd.nextInt(3)
                )
        );

        applyWal(transactions, tableName, 1, rnd);
        validateNoTimestampDuplicates(tableName, from, delta, initialDelta, count);
    }

    private void createEmptyTable(String tableName) throws SqlException {
        compile("create table " + tableName + " (ts timestamp, commit int) timestamp(ts) partition by DAY WAL"
                , sqlExecutionContext);
    }

    private FuzzTransaction generateInsertsTransactions(int commit, String from, long delta, int count, int rowsWithSameTimestamp) {
        FuzzTransaction transaction = new FuzzTransaction();
        long timestamp = parseFloorPartialTimestamp(from) - delta;
        for (int i = 0; i < count * rowsWithSameTimestamp; i++) {
            if (i % rowsWithSameTimestamp == 0) {
                timestamp += delta;
            }
            // Don't change timestamp sometimes with probabilityOfRowsSameTimestamp
            transaction.operationList.add(new FuzzStableInsertOperation(timestamp, commit));
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

    private void validateNoTimestampDuplicates(String tableName, String from, long delta, long initialDelta, long commit2Count) throws SqlException {

        LOG.info().$("Validating no timestamp duplicates [from=").$(from)
                .$(", delta=").$(delta)
                .$(", commit2Count=").$(commit2Count)
                .I$();
        long lastTimestamp = Long.MIN_VALUE;
        long fromTimestamp = parseFloorPartialTimestamp(from);
        long toTimestamp = fromTimestamp + delta * commit2Count;
        StringSink sink = new StringSink();
        boolean started = false;

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
                        sink.putISODate(timestamp).put(',').put(commit).put('\n');
                    }

                    if (fail == null) {
                        if (timestamp == lastTimestamp) {
                            Assert.fail("Duplicate timestamp " + Timestamps.toUSecString(timestamp));
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
                        lastTimestamp = timestamp;
                    }

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
