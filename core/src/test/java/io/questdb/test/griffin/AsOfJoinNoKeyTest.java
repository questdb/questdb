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
    public void testAsOfJoinRightHandAfter() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE t1 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t1 values ('2022-10-05T08:15:00.000000Z', 0, 'a');");
            insert("INSERT INTO t1 values ('2022-10-05T08:16:00.000000Z', 1, 'b');");
            insert("INSERT INTO t1 values ('2022-10-05T08:16:00.000000Z', 2, 'c');");

            ddl("CREATE TABLE t2 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t2 values ('2023-10-05T04:00:00.000000Z', 3, 'd');");

            assertResultSetsMatch();
        });
    }

    @Test
    public void testAsOfJoinRightHandBefore() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE t1 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t1 values ('2022-10-05T08:15:00.000000Z', 0, 'a');");
            insert("INSERT INTO t1 values ('2022-10-05T08:16:00.000000Z', 1, 'b');");
            insert("INSERT INTO t1 values ('2022-10-05T08:16:00.000000Z', 2, 'c');");

            ddl("CREATE TABLE t2 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t2 values ('2021-10-05T04:00:00.000000Z', 3, 'd');");

            assertResultSetsMatch();
        });
    }

    @Test
    public void testAsOfJoinRightHandDuplicate() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE t1 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t1 values ('2022-10-05T08:15:00.000000Z', 0, 'a');");
            insert("INSERT INTO t1 values ('2022-10-05T08:15:00.000000Z', 1, 'b');");
            insert("INSERT INTO t1 values ('2022-10-05T08:15:00.000000Z', 2, 'c');");

            ddl("CREATE TABLE t2 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t2 values ('2022-10-05T08:15:00.000000Z', 0, 'a');");
            insert("INSERT INTO t2 values ('2022-10-05T08:15:00.000000Z', 1, 'b');");
            insert("INSERT INTO t2 values ('2022-10-05T08:15:00.000000Z', 2, 'c');");

            assertResultSetsMatch();
        });
    }

    @Test
    public void testAsOfJoinRightHandEmpty() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE t1 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t1 values ('2022-10-05T08:15:00.000000Z', 0, 'a');");
            insert("INSERT INTO t1 values ('2022-10-05T08:16:00.000000Z', 1, 'b');");
            insert("INSERT INTO t1 values ('2022-10-05T08:16:00.000000Z', 2, 'c');");

            ddl("CREATE TABLE t2 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t2 values ('2023-10-05T04:00:00.000000Z', 3, 'd');");

            assertResultSetsMatch();
        });
    }

    @Test
    public void testAsOfJoinRightHandPartitionBoundary() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE t1 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t1 values ('2022-10-05T00:00:00.000000Z', 0, 'a');");

            ddl("CREATE TABLE t2 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t2 values ('2022-10-04T23:59:59.999999Z', 1, 'b');");
            insert("INSERT INTO t2 values ('2022-10-05T00:00:00.000000Z', 2, 'c');");

            assertResultSetsMatch();
        });
    }

    @Test
    public void testAsOfJoinRightHandSame() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE t1 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t1 values ('2022-10-05T08:15:00.000000Z', 0, 'a');");
            insert("INSERT INTO t1 values ('2022-10-05T08:16:00.000000Z', 1, 'b');");
            insert("INSERT INTO t1 values ('2022-10-07T08:16:00.000000Z', 2, 'c');");

            ddl("CREATE TABLE t2 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            insert("INSERT INTO t2 values ('2022-10-05T08:15:00.000000Z', 0, 'a');");
            insert("INSERT INTO t2 values ('2022-10-05T08:16:00.000000Z', 1, 'b');");
            insert("INSERT INTO t2 values ('2022-10-07T08:16:00.000000Z', 2, 'c');");

            assertResultSetsMatch();
        });
    }

    private void assertResultSetsMatch() throws Exception {
        final StringSink expectedSink = new StringSink();
        // equivalent of the below query, but uses slow factory
        printSql("select * from t1 asof join (t2 where i >= 0)", expectedSink);

        final StringSink actualSink = new StringSink();
        printSql("select * from t1 asof join t2", actualSink);

        TestUtils.assertEquals(expectedSink, actualSink);
    }
}
