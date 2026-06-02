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
 * Tests for arg_min(char, double) - returns char value at min double key.
 */
public class ArgMinCharDoubleGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testArgMinAllNull() throws Exception {
        execute("create table tab (value char, key double)");
        execute("insert into tab values (null, null)");
        execute("insert into tab values (null, null)");
        assertQuery("select arg_min(value, key) from tab")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("arg_min\n\n");
    }

    @Test
    public void testArgMinEmptyTable() throws Exception {
        execute("create table tab (value char, key double)");
        assertQuery("select arg_min(value, key) from tab")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("arg_min\n\n");
    }

    @Test
    public void testArgMinMixedNullValueAndNullKey() throws Exception {
        execute("create table tab (value char, key double)");
        execute("insert into tab values (null, 1.0)");
        execute("insert into tab values ('X', null)");
        execute("insert into tab values ('Y', 3.0)");
        assertQuery("select arg_min(value, key) from tab")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("arg_min\n\n");
    }

    @Test
    public void testArgMinParallelAllNullKeys() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, rnd_char() value, cast(null as double) key from long_sequence(100000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, _, sqlExecutionContext) -> {
                String sql = "select sym, arg_min(value, key) from tab group by sym order by sym";
                assertQuery(sql)
                        .withEngine(engine)
                        .withContext(sqlExecutionContext)
                        .noLeakCheck()
                        .returnsOnce("sym\targ_min\nA\t\nB\t\nC\t\nD\t\nE\t\n");
            }, configuration, LOG);
        }
    }

    @Test
    public void testArgMinParallelChunky() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, rnd_char() value, rnd_double() key from long_sequence(2000000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, _, sqlExecutionContext) -> {
                String sql = "select sym, arg_min(value, key) from tab group by sym order by sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testArgMinParallelMergeNullDestValidSrc() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, rnd_char() value, case when x <= 1000000 then cast(null as double) else rnd_double() end key from long_sequence(2000000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, _, sqlExecutionContext) -> {
                String sql = "select sym, arg_min(value, key) from tab group by sym order by sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testArgMinParallelWithNullKeys() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, rnd_char() value, case when x % 2 = 0 then cast(null as double) else rnd_double() end key from long_sequence(2000000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, _, sqlExecutionContext) -> {
                String sql = "select sym, arg_min(value, key) from tab group by sym order by sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testArgMinSimple() throws Exception {
        execute("create table tab (value char, key double)");
        execute("insert into tab values ('X', 1.0)");
        execute("insert into tab values ('Y', 3.0)");
        execute("insert into tab values ('Z', 2.0)");
        assertQuery("select arg_min(value, key) from tab")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("arg_min\nX\n");
    }

    @Test
    public void testArgMinTieBreaking() throws Exception {
        execute("create table tab (value char, key double)");
        execute("insert into tab values ('X', 5.0)");
        execute("insert into tab values ('Y', 1.0)");
        execute("insert into tab values ('Z', 1.0)");
        assertQuery("select arg_min(value, key) from tab")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("arg_min\nY\n");
    }

    @Test
    public void testArgMinWithGroupBy() throws Exception {
        execute("create table tab (sym symbol, value char, key double)");
        execute("insert into tab values ('A', 'X', 1.0)");
        execute("insert into tab values ('A', 'Y', 3.0)");
        execute("insert into tab values ('B', 'P', 5.0)");
        execute("insert into tab values ('B', 'Q', 4.0)");
        assertQuery("select sym, arg_min(value, key) from tab order by sym")
                .noLeakCheck()
                .expectSize()
                .returns("sym\targ_min\nA\tX\nB\tQ\n");
    }

    @Test
    public void testArgMinWithNullKey() throws Exception {
        execute("create table tab (value char, key double)");
        execute("insert into tab values ('X', null)");
        execute("insert into tab values ('Y', 3.0)");
        execute("insert into tab values ('Z', 2.0)");
        assertQuery("select arg_min(value, key) from tab")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("arg_min\nZ\n");
    }

    @Test
    public void testArgMinWithNullValue() throws Exception {
        execute("create table tab (value char, key double)");
        execute("insert into tab values (null, 1.0)");
        execute("insert into tab values ('Y', 3.0)");
        assertQuery("select arg_min(value, key) from tab")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("arg_min\n\n");
    }
}
