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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

/**
 * Tests for arg_max(timestamp, long) - returns timestamp value at max long key.
 */
public class ArgMaxTimestampLongGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testArgMaxAllNull() throws Exception {
        execute("create table tab (value timestamp, key long)");
        execute("insert into tab values (null, null)");
        execute("insert into tab values (null, null)");
        assertQuery("select arg_max(value, key) from tab")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("arg_max\n\n");
    }

    @Test
    public void testArgMaxParallelAllNullKeys() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, timestamp_sequence(0, 1000) value, cast(null as long) key from long_sequence(100000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, _, sqlExecutionContext) -> {
                String sql = "select sym, arg_max(value, key) from tab group by sym order by sym";
                TestUtils.assertSql(engine, sqlExecutionContext, sql, sink, "sym\targ_max\nA\t\nB\t\nC\t\nD\t\nE\t\n");
            }, configuration, LOG);
        }
    }

    @Test
    public void testArgMaxParallelChunky() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, timestamp_sequence(0, 1000) value, rnd_long() key from long_sequence(2000000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, _, sqlExecutionContext) -> {
                String sql = "select sym, arg_max(value, key) from tab group by sym order by sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testArgMaxParallelMergeNullDestValidSrc() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, timestamp_sequence(0, 1000) value, case when x <= 1000000 then null else rnd_long() end key from long_sequence(2000000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, _, sqlExecutionContext) -> {
                String sql = "select sym, arg_max(value, key) from tab group by sym order by sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testArgMaxParallelWithNullKeys() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, timestamp_sequence(0, 1000) value, case when x % 2 = 0 then null else rnd_long() end key from long_sequence(2000000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, _, sqlExecutionContext) -> {
                String sql = "select sym, arg_max(value, key) from tab group by sym order by sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testArgMaxSimple() throws Exception {
        execute("create table tab (value timestamp, key long)");
        execute("insert into tab values ('2023-01-01T00:00:00.000000Z', 1)");
        execute("insert into tab values ('2023-01-03T00:00:00.000000Z', 3)");
        execute("insert into tab values ('2023-01-02T00:00:00.000000Z', 2)");
        assertQuery("select arg_max(value, key) from tab")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("arg_max\n2023-01-03T00:00:00.000000Z\n");
    }

    @Test
    public void testArgMaxWithGroupBy() throws Exception {
        execute("create table tab (sym symbol, value timestamp, key long)");
        execute("insert into tab values ('A', '2023-01-01T00:00:00.000000Z', 1)");
        execute("insert into tab values ('A', '2023-01-03T00:00:00.000000Z', 3)");
        execute("insert into tab values ('B', '2023-01-05T00:00:00.000000Z', 5)");
        execute("insert into tab values ('B', '2023-01-04T00:00:00.000000Z', 4)");
        assertQuery("select sym, arg_max(value, key) from tab order by sym")
                .noLeakCheck()
                .expectSize()
                .returns("sym\targ_max\nA\t2023-01-03T00:00:00.000000Z\nB\t2023-01-05T00:00:00.000000Z\n");
    }

    @Test
    public void testArgMaxWithNullKey() throws Exception {
        execute("create table tab (value timestamp, key long)");
        execute("insert into tab values ('2023-01-01T00:00:00.000000Z', null)");
        execute("insert into tab values ('2023-01-03T00:00:00.000000Z', 3)");
        execute("insert into tab values ('2023-01-02T00:00:00.000000Z', 2)");
        assertQuery("select arg_max(value, key) from tab")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("arg_max\n2023-01-03T00:00:00.000000Z\n");
    }

    @Test
    public void testArgMaxWithNullValue() throws Exception {
        execute("create table tab (value timestamp, key long)");
        execute("insert into tab values (null, 5)");
        execute("insert into tab values ('2023-01-03T00:00:00.000000Z', 3)");
        assertQuery("select arg_max(value, key) from tab")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("arg_max\n\n");
    }

    @Test
    public void testArgMaxNanoTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (value timestamp_ns, key long)");
            execute("""
                    INSERT INTO tab VALUES
                    ('2023-01-01T00:00:00.123456789Z', 1),
                    ('2023-01-03T12:34:56.987654321Z', 3),
                    ('2023-01-02T05:43:21.111222333Z', 2)""");
            assertQueryNoLeakCheck(
                    """
                            arg_max
                            2023-01-03T12:34:56.987654321Z
                            """,
                    "select arg_max(value, key) from tab",
                    null,
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testArgMaxNanoTimestampReturnsNanoType() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (value timestamp_ns, key long)");
            execute("INSERT INTO tab VALUES ('2024-06-15T10:00:00.123456789Z', 5)");
            assertQueryNoLeakCheck(
                    """
                            column_type
                            TIMESTAMP_NS
                            """,
                    "select typeOf(arg_max(value, key)) AS column_type from tab",
                    null,
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testArgMaxNanoTimestampWithGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (sym symbol, value timestamp_ns, key long)");
            execute("""
                    INSERT INTO tab VALUES
                    ('A', '2023-01-01T00:00:00.111111111Z', 1),
                    ('A', '2023-01-03T00:00:00.333333333Z', 3),
                    ('B', '2023-01-05T00:00:00.555555555Z', 5),
                    ('B', '2023-01-04T00:00:00.444444444Z', 4)""");
            assertQueryNoLeakCheck(
                    """
                            sym\targ_max
                            A\t2023-01-03T00:00:00.333333333Z
                            B\t2023-01-05T00:00:00.555555555Z
                            """,
                    "select sym, arg_max(value, key) from tab order by sym",
                    null,
                    null,
                    true,
                    true
            );
        });
    }
}
