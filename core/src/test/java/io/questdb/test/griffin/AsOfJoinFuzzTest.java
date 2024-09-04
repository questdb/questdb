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

package io.questdb.test.griffin;

import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class AsOfJoinFuzzTest extends AbstractCairoTest {

    @Test
    public void testFuzzManyDuplicates() throws Exception {
        testFuzz(50);
    }

    @Test
    public void testFuzzNoDuplicates() throws Exception {
        testFuzz(0);
    }

    @Test
    public void testFuzzPartitionByNoneManyDuplicates() throws Exception {
        testFuzzPartitionByNone(50);
    }

    @Test
    public void testFuzzPartitionByNoneNoDuplicates() throws Exception {
        testFuzzPartitionByNone(0);
    }

    @Test
    public void testFuzzPartitionByNoneSomeDuplicates() throws Exception {
        testFuzzPartitionByNone(10);
    }

    @Test
    public void testFuzzSomeDuplicates() throws Exception {
        testFuzz(10);
    }

    @Test
    public void testInterleaved1() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE t1 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t1 values ('2022-10-05T08:15:00.000000Z', 0, 'a');");
            insert("INSERT INTO t1 values ('2022-10-05T08:17:00.000000Z', 1, 'a');");
            insert("INSERT INTO t1 values ('2022-10-05T08:21:00.000000Z', 2, 'a');");
            insert("INSERT INTO t1 values ('2022-10-05T08:21:00.000000Z', 2, 'b');");
            insert("INSERT INTO t1 values ('2022-10-10T01:01:00.000000Z', 3, 'a');");

            ddl("CREATE TABLE t2 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t2 values ('2022-10-05T08:18:00.000000Z', 4, 'a');");
            insert("INSERT INTO t2 values ('2022-10-05T08:19:00.000000Z', 5, 'a');");
            insert("INSERT INTO t2 values ('2023-10-05T09:00:00.000000Z', 6, 'a');");
            insert("INSERT INTO t2 values ('2023-10-06T01:00:00.000000Z', 7, 'a');");

            assertResultSetsMatch("t1", "t2");
        });
    }

    @Test
    public void testInterleaved2() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE t1 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t1 values ('2000-02-07T22:00:00.000000Z', 1, 'a');");
            insert("INSERT INTO t1 values ('2000-02-08T06:00:00.000000Z', 2, 'a');");
            insert("INSERT INTO t1 values ('2000-02-08T19:00:00.000000Z', 3, 'a');");
            insert("INSERT INTO t1 values ('2000-02-08T19:00:00.000000Z', 3, 'b');");
            insert("INSERT INTO t1 values ('2000-02-09T16:00:00.000000Z', 4, 'a');");
            insert("INSERT INTO t1 values ('2000-02-09T16:00:00.000000Z', 5, 'a');");
            insert("INSERT INTO t1 values ('2000-02-10T06:00:00.000000Z', 6, 'a');");
            insert("INSERT INTO t1 values ('2000-02-10T06:00:00.000000Z', 6, 'b');");
            insert("INSERT INTO t1 values ('2000-02-10T19:00:00.000000Z', 7, 'a');");

            ddl("CREATE TABLE t2 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t1 values ('2000-02-07T14:00:00.000000Z', 8, 'a');");
            insert("INSERT INTO t1 values ('2000-02-08T02:00:00.000000Z', 9, 'a');");
            insert("INSERT INTO t1 values ('2000-02-08T02:00:00.000000Z', 10, 'a');");
            insert("INSERT INTO t1 values ('2000-02-08T02:00:00.000000Z', 10, 'c');");
            insert("INSERT INTO t1 values ('2000-02-08T21:00:00.000000Z', 11, 'a');");
            insert("INSERT INTO t1 values ('2000-02-09T15:00:00.000000Z', 12, 'a');");
            insert("INSERT INTO t1 values ('2000-02-09T20:00:00.000000Z', 13, 'a');");
            insert("INSERT INTO t1 values ('2000-02-09T20:00:00.000000Z', 13, 'c');");
            insert("INSERT INTO t1 values ('2000-02-10T16:00:00.000000Z', 14, 'a');");

            assertResultSetsMatch("t1", "t2");
        });
    }

    @Test
    public void testRightHandAfter() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE t1 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t1 values ('2022-10-05T08:15:00.000000Z', 0, 'a');");
            insert("INSERT INTO t1 values ('2022-10-05T08:16:00.000000Z', 1, 'a');");
            insert("INSERT INTO t1 values ('2022-10-05T08:16:00.000000Z', 2, 'a');");
            insert("INSERT INTO t1 values ('2022-10-05T08:17:00.000000Z', 2, 'b');");

            ddl("CREATE TABLE t2 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t2 values ('2023-10-05T04:00:00.000000Z', 3, 'a');");
            insert("INSERT INTO t2 values ('2023-10-05T04:00:00.000000Z', 3, 'b');");

            assertResultSetsMatch("t1", "t2");
        });
    }

    @Test
    public void testRightHandBefore() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE t1 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t1 values ('2022-10-05T08:15:00.000000Z', 0, 'a');");
            insert("INSERT INTO t1 values ('2022-10-05T08:16:00.000000Z', 1, 'a');");
            insert("INSERT INTO t1 values ('2022-10-05T08:16:30.000000Z', 1, 'b');");
            insert("INSERT INTO t1 values ('2022-10-05T08:16:00.000000Z', 2, 'a');");

            ddl("CREATE TABLE t2 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t2 values ('2021-10-01T00:00:00.000000Z', 3, 'a');");
            insert("INSERT INTO t2 values ('2021-10-03T01:00:00.000000Z', 4, 'a');");
            insert("INSERT INTO t2 values ('2021-10-03T01:00:00.000000Z', 4, 'b');");
            insert("INSERT INTO t2 values ('2021-10-05T04:00:00.000000Z', 5, 'a');");

            assertResultSetsMatch("t1", "t2");
        });
    }

    @Test
    public void testRightHandDuplicate() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE t1 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t1 values ('2022-10-05T08:15:00.000000Z', 0, 'a');");
            insert("INSERT INTO t1 values ('2022-10-05T08:15:00.000000Z', 1, 'a');");
            insert("INSERT INTO t1 values ('2022-10-05T08:15:00.000000Z', 2, 'a');");

            ddl("CREATE TABLE t2 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t2 values ('2022-10-05T08:15:00.000000Z', 0, 'a');");
            insert("INSERT INTO t2 values ('2022-10-05T08:15:00.000000Z', 1, 'a');");
            insert("INSERT INTO t2 values ('2022-10-05T08:15:00.000000Z', 2, 'a');");

            assertResultSetsMatch("t1", "t2");
        });
    }

    @Test
    public void testRightHandEmpty() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE t1 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t1 values ('2022-10-05T08:15:00.000000Z', 0, 'a');");
            insert("INSERT INTO t1 values ('2022-10-05T08:16:00.000000Z', 1, 'a');");
            insert("INSERT INTO t1 values ('2022-10-05T08:16:00.000000Z', 2, 'a');");

            ddl("CREATE TABLE t2 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t2 values ('2023-10-05T04:00:00.000000Z', 3, 'a');");

            assertResultSetsMatch("t1", "t2");
        });
    }

    @Test
    public void testRightHandPartitionBoundary() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE t1 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t1 values ('2022-10-05T00:00:00.000000Z', 0, 'a');");
            insert("INSERT INTO t1 values ('2022-10-05T00:00:00.000000Z', 0, 'b');");

            ddl("CREATE TABLE t2 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t2 values ('2022-10-04T23:59:59.999999Z', 1, 'a');");
            insert("INSERT INTO t2 values ('2022-10-04T23:59:59.999999Z', 1, 'b');");
            insert("INSERT INTO t2 values ('2022-10-05T00:00:00.000000Z', 2, 'a');");

            assertResultSetsMatch("t1", "t2");
        });
    }

    @Test
    public void testRightHandSame() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE t1 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t1 values ('2022-10-05T08:15:00.000000Z', 0, 'a');");
            insert("INSERT INTO t1 values ('2022-10-05T08:16:00.000000Z', 1, 'a');");
            insert("INSERT INTO t1 values ('2022-10-05T08:16:00.000000Z', 1, 'b');");
            insert("INSERT INTO t1 values ('2022-10-07T08:16:00.000000Z', 2, 'a');");

            ddl("CREATE TABLE t2 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t2 values ('2022-10-05T08:15:00.000000Z', 0, 'a');");
            insert("INSERT INTO t2 values ('2022-10-05T08:15:00.000000Z', 0, 'c');");
            insert("INSERT INTO t2 values ('2022-10-05T08:16:00.000000Z', 1, 'a');");
            insert("INSERT INTO t2 values ('2022-10-07T08:16:00.000000Z', 2, 'a');");

            assertResultSetsMatch("t1", "t2");
        });
    }

    @Test
    public void testSelfJoin() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE t (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t values ('2022-10-05T00:00:00.000000Z', 0, 'a');");
            insert("INSERT INTO t values ('2022-10-05T08:16:00.000000Z', 1, 'a');");
            insert("INSERT INTO t values ('2022-10-05T08:16:00.000000Z', 3, 'a');");
            insert("INSERT INTO t values ('2022-10-05T23:59:59.999999Z', 4, 'a');");
            insert("INSERT INTO t values ('2022-10-05T23:59:59.999999Z', 4, 'b');");
            insert("INSERT INTO t values ('2022-10-06T00:00:00.000000Z', 5, 'a');");
            insert("INSERT INTO t values ('2022-10-06T00:01:00.000000Z', 6, 'a');");
            insert("INSERT INTO t values ('2022-10-06T00:01:00.000000Z', 6, 'c');");
            insert("INSERT INTO t values ('2022-10-06T00:02:00.000000Z', 7, 'a');");

            assertResultSetsMatch("t as t1", "t as t2");
        });
    }


    private void assertResultSetsMatch(String leftTable, String rightTable) throws Exception {
        final StringSink expectedSink = new StringSink();
        // equivalent of the below query, but uses slow factory
        printSql("select * from " + leftTable + " asof join (" + rightTable + " where i >= 0) on s", expectedSink);

        final StringSink actualSink = new StringSink();
        printSql("select * from " + leftTable + " asof join " + rightTable + " on s", actualSink);

        TestUtils.assertEquals(expectedSink, actualSink);
    }

    private void testFuzz(int tsDuplicatePercentage) throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            final int table1Size = rnd.nextPositiveInt() % 1000;
            final int table2Size = rnd.nextPositiveInt() % 1000;

            ddl("CREATE TABLE t1 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            long ts = TimestampFormatUtils.parseTimestamp("2000-01-01T00:00:00.000Z");
            ts += Timestamps.HOUR_MICROS * (rnd.nextLong() % 48);
            for (int i = 0; i < table1Size; i++) {
                if (rnd.nextInt(100) >= tsDuplicatePercentage) {
                    ts += Timestamps.HOUR_MICROS * rnd.nextLong(24);
                }
                String symbol = "s_" + rnd.nextInt(10);
                insert("INSERT INTO t1 values (" + ts + ", " + i + ", '" + symbol + "');");
            }

            ddl("CREATE TABLE t2 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            ts = TimestampFormatUtils.parseTimestamp("2000-01-01T00:00:00.000Z");
            ts += Timestamps.HOUR_MICROS * rnd.nextLong(48);
            for (int i = 0; i < table2Size; i++) {
                if (rnd.nextInt(100) >= tsDuplicatePercentage) {
                    ts += Timestamps.HOUR_MICROS * rnd.nextLong(24);
                }
                String symbol = "s_" + rnd.nextInt(10);
                insert("INSERT INTO t2 values (" + ts + ", " + i + ", '" + symbol + "');");
            }

            assertResultSetsMatch("t1", "t2");
        });
    }

    private void testFuzzPartitionByNone(int tsDuplicatePercentage) throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            final int table1Size = rnd.nextPositiveInt() % 1000;
            final int table2Size = rnd.nextPositiveInt() % 1000;

            ddl("CREATE TABLE t1 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts)");
            long ts = TimestampFormatUtils.parseTimestamp("2000-01-01T00:00:00.000Z");
            ts += Timestamps.HOUR_MICROS * (rnd.nextLong() % 48);
            for (int i = 0; i < table1Size; i++) {
                if (rnd.nextInt(100) >= tsDuplicatePercentage) {
                    ts += Timestamps.HOUR_MICROS * rnd.nextLong(24);
                }
                String symbol = "s_" + rnd.nextInt(10);
                insert("INSERT INTO t1 values (" + ts + ", " + i + ", '" + symbol + "');");
            }

            ddl("CREATE TABLE t2 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts)");
            ts = TimestampFormatUtils.parseTimestamp("2000-01-01T00:00:00.000Z");
            ts += Timestamps.HOUR_MICROS * rnd.nextLong(48);
            for (int i = 0; i < table2Size; i++) {
                if (rnd.nextInt(100) >= tsDuplicatePercentage) {
                    ts += Timestamps.HOUR_MICROS * rnd.nextLong(24);
                }
                String symbol = "s_" + rnd.nextInt(10);
                insert("INSERT INTO t2 values (" + ts + ", " + i + ", '" + symbol + "');");
            }

            assertResultSetsMatch("t1", "t2");
        });
    }
}
