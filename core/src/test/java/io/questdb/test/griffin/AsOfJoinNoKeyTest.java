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

package io.questdb.test.griffin;

import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

/**
 * Verifies correctness of both AsOfJoinNoKeyRecordCursorFactory and AsOfJoinFastNoKeyRecordCursorFactory factories.
 * The latter skips full scan of right hand table by lazy time frame navigation.
 */
public class AsOfJoinNoKeyTest extends AbstractCairoTest {

    @Test
    public void testFuzz() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            final int table1Size = rnd.nextPositiveInt() % 100;
            final int table2Size = rnd.nextPositiveInt() % 100;

            ddl("CREATE TABLE t1 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            long ts = TimestampFormatUtils.parseTimestamp("2000-01-01T00:00:00.000Z");
            ts += Timestamps.HOUR_MICROS * (rnd.nextLong() % 48);
            for (int i = 0; i < table1Size; i++) {
                ts += Timestamps.HOUR_MICROS * rnd.nextLong(24);
                insert("INSERT INTO t1 values (" + ts + ", " + i + ", 't1_" + i + "');");
            }

            ddl("CREATE TABLE t2 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            ts = TimestampFormatUtils.parseTimestamp("2000-01-01T00:00:00.000Z");
            ts += Timestamps.HOUR_MICROS * rnd.nextLong(48);
            for (int i = 0; i < table2Size; i++) {
                ts += Timestamps.HOUR_MICROS * rnd.nextLong(24);
                insert("INSERT INTO t2 values (" + ts + ", " + i + ", 't2_" + i + "');");
            }

            assertResultSetsMatch("t1", "t2");
        });
    }

    @Test
    public void testMixed() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE t1 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t1 values ('2022-10-05T08:15:00.000000Z', 0, 'a');");
            insert("INSERT INTO t1 values ('2022-10-05T08:16:00.000000Z', 1, 'b');");
            insert("INSERT INTO t1 values ('2022-10-05T08:17:00.000000Z', 2, 'c');");

            ddl("CREATE TABLE t2 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t2 values ('2023-10-01T01:00:00.000000Z', 3, 'd');");
            insert("INSERT INTO t2 values ('2022-10-05T08:16:00.000000Z', 4, 'e');");
            insert("INSERT INTO t2 values ('2023-10-05T09:00:00.000000Z', 5, 'f');");
            insert("INSERT INTO t2 values ('2023-10-06T01:00:00.000000Z', 6, 'g');");

            assertResultSetsMatch("t1", "t2");
        });
    }

    @Test
    public void testRightHandAfter() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE t1 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t1 values ('2022-10-05T08:15:00.000000Z', 0, 'a');");
            insert("INSERT INTO t1 values ('2022-10-05T08:16:00.000000Z', 1, 'b');");
            insert("INSERT INTO t1 values ('2022-10-05T08:16:00.000000Z', 2, 'c');");

            ddl("CREATE TABLE t2 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t2 values ('2023-10-05T04:00:00.000000Z', 3, 'd');");

            assertResultSetsMatch("t1", "t2");
        });
    }

    @Test
    public void testRightHandBefore() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE t1 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t1 values ('2022-10-05T08:15:00.000000Z', 0, 'a');");
            insert("INSERT INTO t1 values ('2022-10-05T08:16:00.000000Z', 1, 'b');");
            insert("INSERT INTO t1 values ('2022-10-05T08:16:00.000000Z', 2, 'c');");

            ddl("CREATE TABLE t2 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t2 values ('2021-10-05T04:00:00.000000Z', 3, 'd');");

            assertResultSetsMatch("t1", "t2");
        });
    }

    @Test
    public void testRightHandDuplicate() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE t1 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t1 values ('2022-10-05T08:15:00.000000Z', 0, 'a');");
            insert("INSERT INTO t1 values ('2022-10-05T08:15:00.000000Z', 1, 'b');");
            insert("INSERT INTO t1 values ('2022-10-05T08:15:00.000000Z', 2, 'c');");

            ddl("CREATE TABLE t2 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t2 values ('2022-10-05T08:15:00.000000Z', 0, 'a');");
            insert("INSERT INTO t2 values ('2022-10-05T08:15:00.000000Z', 1, 'b');");
            insert("INSERT INTO t2 values ('2022-10-05T08:15:00.000000Z', 2, 'c');");

            assertResultSetsMatch("t1", "t2");
        });
    }

    @Test
    public void testRightHandEmpty() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE t1 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t1 values ('2022-10-05T08:15:00.000000Z', 0, 'a');");
            insert("INSERT INTO t1 values ('2022-10-05T08:16:00.000000Z', 1, 'b');");
            insert("INSERT INTO t1 values ('2022-10-05T08:16:00.000000Z', 2, 'c');");

            ddl("CREATE TABLE t2 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t2 values ('2023-10-05T04:00:00.000000Z', 3, 'd');");

            assertResultSetsMatch("t1", "t2");
        });
    }

    @Test
    public void testRightHandPartitionBoundary() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE t1 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t1 values ('2022-10-05T00:00:00.000000Z', 0, 'a');");

            ddl("CREATE TABLE t2 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t2 values ('2022-10-04T23:59:59.999999Z', 1, 'b');");
            insert("INSERT INTO t2 values ('2022-10-05T00:00:00.000000Z', 2, 'c');");

            assertResultSetsMatch("t1", "t2");
        });
    }

    @Test
    public void testRightHandSame() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE t1 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t1 values ('2022-10-05T08:15:00.000000Z', 0, 'a');");
            insert("INSERT INTO t1 values ('2022-10-05T08:16:00.000000Z', 1, 'b');");
            insert("INSERT INTO t1 values ('2022-10-07T08:16:00.000000Z', 2, 'c');");

            ddl("CREATE TABLE t2 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t2 values ('2022-10-05T08:15:00.000000Z', 0, 'a');");
            insert("INSERT INTO t2 values ('2022-10-05T08:16:00.000000Z', 1, 'b');");
            insert("INSERT INTO t2 values ('2022-10-07T08:16:00.000000Z', 2, 'c');");

            assertResultSetsMatch("t1", "t2");
        });
    }

    @Test
    public void testSelfJoin() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE t (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t values ('2022-10-05T00:00:00.000000Z', 0, 'a');");
            insert("INSERT INTO t values ('2022-10-05T08:16:00.000000Z', 1, 'b');");
            insert("INSERT INTO t values ('2022-10-05T08:16:00.000000Z', 3, 'c');");
            insert("INSERT INTO t values ('2022-10-05T23:59:59.999999Z', 4, 'd');");
            insert("INSERT INTO t values ('2022-10-06T00:00:00.000000Z', 5, 'e');");
            insert("INSERT INTO t values ('2022-10-06T00:01:00.000000Z', 6, 'f');");
            insert("INSERT INTO t values ('2022-10-06T00:02:00.000000Z', 7, 'g');");

            assertResultSetsMatch("t as t1", "t as t2");
        });
    }

    private void assertResultSetsMatch(String leftTable, String rightTable) throws Exception {
        final StringSink expectedSink = new StringSink();
        // equivalent of the below query, but uses slow factory
        printSql("select * from " + leftTable + " asof join (" + rightTable + " where i >= 0)", expectedSink);

        final StringSink actualSink = new StringSink();
        printSql("select * from " + leftTable + " asof join " + rightTable, actualSink);

        TestUtils.assertEquals(expectedSink, actualSink);
    }
}
