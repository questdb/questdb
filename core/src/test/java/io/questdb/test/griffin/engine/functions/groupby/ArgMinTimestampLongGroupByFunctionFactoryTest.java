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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.griffin.SqlException;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

/**
 * Tests for arg_min(timestamp, long) - returns timestamp value at min long key.
 */
public class ArgMinTimestampLongGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testArgMinAllNull() throws SqlException {
        execute("create table tab (value timestamp, key long)");
        execute("insert into tab values (null, null)");
        execute("insert into tab values (null, null)");
        assertSql("arg_min\n\n", "select arg_min(value, key) from tab");
    }

    @Test
    public void testArgMinParallelAllNullKeys() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, timestamp_sequence(0, 1000) value, cast(null as long) key from long_sequence(100000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                String sql = "select sym, arg_min(value, key) from tab group by sym order by sym";
                TestUtils.assertSql(engine, sqlExecutionContext, sql, sink, "sym\targ_min\nA\t\nB\t\nC\t\nD\t\nE\t\n");
            }, configuration, LOG);
        }
    }

    @Test
    public void testArgMinParallelChunky() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, timestamp_sequence(0, 1000) value, rnd_long() key from long_sequence(2000000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                String sql = "select sym, arg_min(value, key) from tab group by sym order by sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testArgMinParallelMergeNullDestValidSrc() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, timestamp_sequence(0, 1000) value, case when x <= 1000000 then null else rnd_long() end key from long_sequence(2000000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                String sql = "select sym, arg_min(value, key) from tab group by sym order by sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testArgMinParallelWithNullKeys() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, timestamp_sequence(0, 1000) value, case when x % 2 = 0 then null else rnd_long() end key from long_sequence(2000000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                String sql = "select sym, arg_min(value, key) from tab group by sym order by sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testArgMinSimple() throws SqlException {
        execute("create table tab (value timestamp, key long)");
        execute("insert into tab values ('2023-01-01T00:00:00.000000Z', 1)");
        execute("insert into tab values ('2023-01-03T00:00:00.000000Z', 3)");
        execute("insert into tab values ('2023-01-02T00:00:00.000000Z', 2)");
        assertSql("arg_min\n2023-01-01T00:00:00.000000Z\n", "select arg_min(value, key) from tab");
    }

    @Test
    public void testArgMinWithGroupBy() throws SqlException {
        execute("create table tab (sym symbol, value timestamp, key long)");
        execute("insert into tab values ('A', '2023-01-01T00:00:00.000000Z', 1)");
        execute("insert into tab values ('A', '2023-01-03T00:00:00.000000Z', 3)");
        execute("insert into tab values ('B', '2023-01-05T00:00:00.000000Z', 5)");
        execute("insert into tab values ('B', '2023-01-04T00:00:00.000000Z', 4)");
        assertSql("sym\targ_min\nA\t2023-01-01T00:00:00.000000Z\nB\t2023-01-04T00:00:00.000000Z\n", "select sym, arg_min(value, key) from tab order by sym");
    }

    @Test
    public void testArgMinWithNullKey() throws SqlException {
        execute("create table tab (value timestamp, key long)");
        execute("insert into tab values ('2023-01-01T00:00:00.000000Z', null)");
        execute("insert into tab values ('2023-01-03T00:00:00.000000Z', 3)");
        execute("insert into tab values ('2023-01-02T00:00:00.000000Z', 2)");
        assertSql("arg_min\n2023-01-02T00:00:00.000000Z\n", "select arg_min(value, key) from tab");
    }

    @Test
    public void testArgMinWithNullValue() throws SqlException {
        execute("create table tab (value timestamp, key long)");
        execute("insert into tab values (null, 1)");
        execute("insert into tab values ('2023-01-03T00:00:00.000000Z', 3)");
        assertSql("arg_min\n\n", "select arg_min(value, key) from tab");
    }
}
