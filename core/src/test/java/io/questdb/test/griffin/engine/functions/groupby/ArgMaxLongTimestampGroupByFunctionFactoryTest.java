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
 * Tests for arg_max(long, timestamp) - returns long value at max timestamp key.
 */
public class ArgMaxLongTimestampGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testArgMaxAllNull() throws SqlException {
        execute("create table tab (value long, key timestamp)");
        execute("insert into tab values (null, null)");
        execute("insert into tab values (null, null)");
        assertSql("arg_max\nnull\n", "select arg_max(value, key) from tab");
    }

    @Test
    public void testArgMaxParallelAllNullKeys() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, rnd_long() value, cast(null as timestamp) key from long_sequence(100000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                String sql = "select sym, arg_max(value, key) from tab group by sym order by sym";
                TestUtils.assertSql(engine, sqlExecutionContext, sql, sink, "sym\targ_max\nA\tnull\nB\tnull\nC\tnull\nD\tnull\nE\tnull\n");
            }, configuration, LOG);
        }
    }

    @Test
    public void testArgMaxParallelChunky() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, rnd_long() value, timestamp_sequence(0, 1000) key from long_sequence(2000000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                String sql = "select sym, arg_max(value, key) from tab group by sym order by sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testArgMaxParallelMergeNullDestValidSrc() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, rnd_long() value, case when x <= 1000000 then cast(null as timestamp) else timestamp_sequence(0, 1000) end key from long_sequence(2000000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                String sql = "select sym, arg_max(value, key) from tab group by sym order by sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testArgMaxParallelWithNullKeys() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, rnd_long() value, case when x % 2 = 0 then cast(null as timestamp) else timestamp_sequence(0, 1000) end key from long_sequence(2000000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                String sql = "select sym, arg_max(value, key) from tab group by sym order by sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testArgMaxSimple() throws SqlException {
        execute("create table tab (value long, key timestamp)");
        execute("insert into tab values (10, '2023-01-01T00:00:00.000000Z')");
        execute("insert into tab values (20, '2023-01-03T00:00:00.000000Z')");
        execute("insert into tab values (30, '2023-01-02T00:00:00.000000Z')");
        assertSql("arg_max\n20\n", "select arg_max(value, key) from tab");
    }

    @Test
    public void testArgMaxWithGroupBy() throws SqlException {
        execute("create table tab (sym symbol, value long, key timestamp)");
        execute("insert into tab values ('A', 10, '2023-01-01T00:00:00.000000Z')");
        execute("insert into tab values ('A', 20, '2023-01-03T00:00:00.000000Z')");
        execute("insert into tab values ('B', 100, '2023-01-05T00:00:00.000000Z')");
        execute("insert into tab values ('B', 200, '2023-01-04T00:00:00.000000Z')");
        assertSql("sym\targ_max\nA\t20\nB\t100\n", "select sym, arg_max(value, key) from tab order by sym");
    }

    @Test
    public void testArgMaxWithNullKey() throws SqlException {
        execute("create table tab (value long, key timestamp)");
        execute("insert into tab values (10, null)");
        execute("insert into tab values (20, '2023-01-03T00:00:00.000000Z')");
        execute("insert into tab values (30, '2023-01-02T00:00:00.000000Z')");
        assertSql("arg_max\n20\n", "select arg_max(value, key) from tab");
    }

    @Test
    public void testArgMaxWithNullValue() throws SqlException {
        execute("create table tab (value long, key timestamp)");
        execute("insert into tab values (null, '2023-01-05T00:00:00.000000Z')");
        execute("insert into tab values (20, '2023-01-03T00:00:00.000000Z')");
        assertSql("arg_max\nnull\n", "select arg_max(value, key) from tab");
    }
}
