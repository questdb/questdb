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
 * Tests for arg_max(char, long) - returns char value at max long key.
 */
public class ArgMaxCharLongGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testArgMaxAllNull() throws SqlException {
        execute("create table tab (value char, key long)");
        execute("insert into tab values (null, null)");
        execute("insert into tab values (null, null)");
        assertSql("arg_max\n\n", "select arg_max(value, key) from tab");
    }

    @Test
    public void testArgMaxParallelAllNullKeys() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, rnd_char() value, cast(null as long) key from long_sequence(100000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                String sql = "select sym, arg_max(value, key) from tab group by sym order by sym";
                TestUtils.assertSql(engine, sqlExecutionContext, sql, sink, "sym\targ_max\nA\t\nB\t\nC\t\nD\t\nE\t\n");
            }, configuration, LOG);
        }
    }

    @Test
    public void testArgMaxParallelChunky() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, rnd_char() value, x key from long_sequence(2000000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                String sql = "select sym, arg_max(value, key) from tab group by sym order by sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testArgMaxParallelMergeNullDestValidSrc() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, rnd_char() value, case when x <= 1000000 then cast(null as long) else x end key from long_sequence(2000000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                String sql = "select sym, arg_max(value, key) from tab group by sym order by sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testArgMaxParallelWithNullKeys() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, rnd_char() value, case when x % 2 = 0 then cast(null as long) else x end key from long_sequence(2000000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                String sql = "select sym, arg_max(value, key) from tab group by sym order by sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testArgMaxSimple() throws SqlException {
        execute("create table tab (value char, key long)");
        execute("insert into tab values ('X', 100)");
        execute("insert into tab values ('Y', 300)");
        execute("insert into tab values ('Z', 200)");
        assertSql("arg_max\nY\n", "select arg_max(value, key) from tab");
    }

    @Test
    public void testArgMaxWithGroupBy() throws SqlException {
        execute("create table tab (sym symbol, value char, key long)");
        execute("insert into tab values ('A', 'X', 100)");
        execute("insert into tab values ('A', 'Y', 300)");
        execute("insert into tab values ('B', 'P', 500)");
        execute("insert into tab values ('B', 'Q', 400)");
        assertSql("sym\targ_max\nA\tY\nB\tP\n", "select sym, arg_max(value, key) from tab order by sym");
    }

    @Test
    public void testArgMaxWithNullKey() throws SqlException {
        execute("create table tab (value char, key long)");
        execute("insert into tab values ('X', null)");
        execute("insert into tab values ('Y', 300)");
        execute("insert into tab values ('Z', 200)");
        assertSql("arg_max\nY\n", "select arg_max(value, key) from tab");
    }

    @Test
    public void testArgMaxWithNullValue() throws SqlException {
        execute("create table tab (value char, key long)");
        execute("insert into tab values (null, 500)");
        execute("insert into tab values ('Y', 300)");
        assertSql("arg_max\n\n", "select arg_max(value, key) from tab");
    }
}
