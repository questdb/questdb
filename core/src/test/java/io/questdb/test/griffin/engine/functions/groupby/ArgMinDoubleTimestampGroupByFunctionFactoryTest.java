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
 * Tests for arg_min(double, timestamp) - returns double value at min timestamp key.
 */
public class ArgMinDoubleTimestampGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testArgMinAllNull() throws SqlException {
        execute("create table tab (value double, key timestamp)");
        execute("insert into tab values (null, null)");
        execute("insert into tab values (null, null)");
        assertSql("arg_min\nnull\n", "select arg_min(value, key) from tab");
    }

    @Test
    public void testArgMinParallelAllNullKeys() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, rnd_double() value, cast(null as timestamp) key from long_sequence(100000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                String sql = "select sym, arg_min(value, key) from tab group by sym order by sym";
                TestUtils.assertSql(engine, sqlExecutionContext, sql, sink, "sym\targ_min\nA\tnull\nB\tnull\nC\tnull\nD\tnull\nE\tnull\n");
            }, configuration, LOG);
        }
    }

    @Test
    public void testArgMinParallelChunky() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, rnd_double() value, timestamp_sequence(0, 1000) key from long_sequence(2000000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                String sql = "select sym, arg_min(value, key) from tab group by sym order by sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testArgMinParallelMergeNullDestValidSrc() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, rnd_double() value, case when x <= 1000000 then cast(null as timestamp) else timestamp_sequence(0, 1000) end key from long_sequence(2000000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                String sql = "select sym, arg_min(value, key) from tab group by sym order by sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testArgMinParallelWithNullKeys() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, rnd_double() value, case when x % 2 = 0 then cast(null as timestamp) else timestamp_sequence(0, 1000) end key from long_sequence(2000000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                String sql = "select sym, arg_min(value, key) from tab group by sym order by sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testArgMinSimple() throws SqlException {
        execute("create table tab (value double, key timestamp)");
        execute("insert into tab values (10.5, '2023-01-01T00:00:00.000000Z')");
        execute("insert into tab values (20.5, '2023-01-03T00:00:00.000000Z')");
        execute("insert into tab values (30.5, '2023-01-02T00:00:00.000000Z')");
        assertSql("arg_min\n10.5\n", "select arg_min(value, key) from tab");
    }

    @Test
    public void testArgMinWithGroupBy() throws SqlException {
        execute("create table tab (sym symbol, value double, key timestamp)");
        execute("insert into tab values ('A', 10.5, '2023-01-01T00:00:00.000000Z')");
        execute("insert into tab values ('A', 20.5, '2023-01-03T00:00:00.000000Z')");
        execute("insert into tab values ('B', 100.5, '2023-01-05T00:00:00.000000Z')");
        execute("insert into tab values ('B', 200.5, '2023-01-04T00:00:00.000000Z')");
        assertSql("sym\targ_min\nA\t10.5\nB\t200.5\n", "select sym, arg_min(value, key) from tab order by sym");
    }

    @Test
    public void testArgMinWithNullKey() throws SqlException {
        execute("create table tab (value double, key timestamp)");
        execute("insert into tab values (10.5, null)");
        execute("insert into tab values (20.5, '2023-01-03T00:00:00.000000Z')");
        execute("insert into tab values (30.5, '2023-01-02T00:00:00.000000Z')");
        assertSql("arg_min\n30.5\n", "select arg_min(value, key) from tab");
    }

    @Test
    public void testArgMinWithNullValue() throws SqlException {
        execute("create table tab (value double, key timestamp)");
        execute("insert into tab values (null, '2023-01-01T00:00:00.000000Z')");
        execute("insert into tab values (20.5, '2023-01-03T00:00:00.000000Z')");
        assertSql("arg_min\nnull\n", "select arg_min(value, key) from tab");
    }
}
