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

package io.questdb.test.griffin.engine.join;

import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.griffin.engine.join.AsyncAsOfJoinIndexState;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.LongList;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * The index mode state of Async AsOf Join, driven directly: lookups whose master timestamps mostly
 * ascend, sometimes fall back (a worker slot that takes an earlier frame next) and jump across
 * partitions, each against the latest row of its key at or below the timestamp by brute force.
 */
public class AsyncAsOfJoinIndexStateTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(AsyncAsOfJoinIndexStateTest.class);

    @Test
    public void testMatchesBruteForce() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        for (String index : new String[]{"INDEX", "INDEX TYPE POSTING"}) {
            assertMemoryLeak(() -> {
                execute("CREATE TABLE quotes (id LONG, ts TIMESTAMP, sym SYMBOL " + index + ") TIMESTAMP(ts) PARTITION BY DAY");
                final int rows = 5_000 + rnd.nextInt(5_000);
                // two to a timestamp over three days: ties
                final long quantum = 6L * 24 * 3_600_000_000L / rows;
                execute("INSERT INTO quotes SELECT x id, '2024-01-01'::TIMESTAMP + (x / 2) * " + quantum + "L ts, " +
                        "CASE WHEN x % 13 = 0 THEN NULL ELSE rnd_symbol('A', 'B', 'C', 'R') END sym FROM long_sequence(" + rows + ")");
                // brute force reference: every row's id, timestamp and symbol, in table order
                final LongList ids = new LongList();
                final LongList timestamps = new LongList();
                final LongList keys = new LongList();
                try (RecordCursorFactory factory = select("quotes"); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        ids.add(record.getLong(0));
                        timestamps.add(record.getTimestamp(1));
                        keys.add(record.getInt(2));
                    }
                }
                try (RecordCursorFactory factory = select("quotes"); TimeFrameCursor cursor = factory.getTimeFrameCursor(sqlExecutionContext)) {
                    final StaticSymbolTable symbolTable = cursor.getSymbolTable(2);
                    final int[] slaveKeys = {symbolTable.keyOf("A"), symbolTable.keyOf("R"), StaticSymbolTable.VALUE_IS_NULL};
                    final PageFrameMemoryRecord slaveRecord = (PageFrameMemoryRecord) cursor.getRecord();
                    try (AsyncAsOfJoinIndexState state = new AsyncAsOfJoinIndexState()) {
                        Assert.assertTrue(state.of(cursor, slaveRecord, 1, 2, 1, slaveKeys.length));
                        final long tsLo = timestamps.getQuick(0) - quantum;
                        final long tsHi = timestamps.getLast() + quantum;
                        long ts = tsLo;
                        for (int q = 0; q < 3_000; q++) {
                            final int choice = rnd.nextInt(20);
                            if (choice == 0) {
                                // back, as a slot that takes an earlier frame next
                                ts = tsLo + rnd.nextLong(Math.max(1, ts - tsLo + 1));
                            } else if (choice == 1) {
                                // a long jump forward, across partitions
                                ts = Math.min(tsHi, ts + rnd.nextLong(24L * 3_600_000_000L));
                            } else {
                                // mostly short steps, onto the next quote timestamp half of the time: ties
                                final long base = timestamps.getQuick(0);
                                ts = rnd.nextBoolean()
                                        ? Math.min(tsHi, base + Math.floorDiv(ts - base, quantum) * quantum + quantum)
                                        : Math.min(tsHi, ts + rnd.nextLong(3 * quantum));
                            }
                            final int slot = rnd.nextInt(slaveKeys.length);
                            final int slaveKey = slaveKeys[slot];
                            // a key's index key is its symbol key + 1, NULL's 0
                            final int indexKey = slaveKey == StaticSymbolTable.VALUE_IS_NULL ? 0 : slaveKey + 1;
                            final long match = state.match(slot, indexKey, ts, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
                            Assert.assertNotEquals(AsyncAsOfJoinIndexState.UNSUPPORTED, match);
                            long expectedId = -1;
                            for (int r = 0, n = ids.size(); r < n && timestamps.getQuick(r) <= ts; r++) {
                                if (keys.getQuick(r) == slaveKey) {
                                    expectedId = ids.getQuick(r);
                                }
                            }
                            long actualId = -1;
                            if (match != Long.MIN_VALUE) {
                                cursor.recordAt(slaveRecord, match);
                                actualId = slaveRecord.getLong(0);
                            }
                            Assert.assertEquals(index + ": lookup " + q + " of key " + slot + " at " + ts, expectedId, actualId);
                        }
                    }
                }
                execute("DROP TABLE quotes");
            });
        }
    }
}
