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

import io.questdb.griffin.SqlException;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

/**
 * Tests for arg_max(varchar, double) - returns varchar value at max double key.
 */
public class ArgMaxVarcharDoubleGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testArgMaxAllNull() throws SqlException {
        execute("CREATE TABLE tab (value varchar, key double)");
        execute("INSERT INTO tab VALUES (null, null), (null, null)");
        assertSql("arg_max\n\n", "SELECT arg_max(value, key) FROM tab");
    }

    @Test
    public void testArgMaxEmptyStringValue() throws SqlException {
        execute("CREATE TABLE tab (value varchar, key double)");
        execute("INSERT INTO tab VALUES ('', 5.0), ('beta', 3.0)");
        assertSql("arg_max\n\n", "SELECT arg_max(value, key) FROM tab");
    }

    @Test
    public void testArgMaxEmptyTable() throws SqlException {
        execute("CREATE TABLE tab (value varchar, key double)");
        assertSql("arg_max\n\n", "SELECT arg_max(value, key) FROM tab");
    }

    @Test
    public void testArgMaxMixedNullValueAndNullKey() throws SqlException {
        execute("CREATE TABLE tab (value varchar, key double)");
        execute("INSERT INTO tab VALUES (null, 5.0), ('beta', null)");
        assertSql("arg_max\n\n", "SELECT arg_max(value, key) FROM tab");
    }

    @Test
    public void testArgMaxParallel() throws Exception {
        execute("CREATE TABLE tab AS (" +
                "SELECT rnd_symbol('A','B','C') sym, " +
                "rnd_varchar('foo','bar','baz','qux') value, " +
                "rnd_double() key " +
                "FROM long_sequence(10000))");

        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                String sql = "SELECT sym, arg_max(value, key) FROM tab GROUP BY sym ORDER BY sym";

                // Verify the query plan shows parallel execution
                TestUtils.assertSql(
                        engine,
                        sqlExecutionContext,
                        "EXPLAIN " + sql,
                        sink,
                        """
                                QUERY PLAN
                                Encode sort light
                                  keys: [sym]
                                    Async Group By workers: 4
                                      keys: [sym]
                                      values: [arg_max(value,key)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: tab
                                """
                );
            }, configuration, LOG);
        }
    }

    @Test
    public void testArgMaxParallelAllNullKeys() throws Exception {
        execute("CREATE TABLE tab AS (SELECT rnd_symbol('A','B','C','D','E') sym, rnd_varchar('foo','bar','baz','qux') value, CAST(null AS double) key FROM long_sequence(100000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                String sql = "SELECT sym, arg_max(value, key) FROM tab GROUP BY sym ORDER BY sym";
                TestUtils.assertSql(engine, sqlExecutionContext, sql, sink, "sym\targ_max\nA\t\nB\t\nC\t\nD\t\nE\t\n");
            }, configuration, LOG);
        }
    }

    @Test
    public void testArgMaxParallelChunky() throws Exception {
        execute("CREATE TABLE tab AS (SELECT rnd_symbol('A','B','C','D','E') sym, rnd_varchar('foo','bar','baz','qux') value, rnd_double() key FROM long_sequence(2000000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                String sql = "SELECT sym, arg_max(value, key) FROM tab GROUP BY sym ORDER BY sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testArgMaxParallelMergeNullDestValidSrc() throws Exception {
        execute("CREATE TABLE tab AS (SELECT rnd_symbol('A','B','C','D','E') sym, rnd_varchar('foo','bar','baz','qux') value, CASE WHEN x <= 1000000 THEN CAST(null AS double) ELSE rnd_double() END key FROM long_sequence(2000000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                String sql = "SELECT sym, arg_max(value, key) FROM tab GROUP BY sym ORDER BY sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testArgMaxParallelWithNullKeys() throws Exception {
        execute("CREATE TABLE tab AS (SELECT rnd_symbol('A','B','C','D','E') sym, rnd_varchar('foo','bar','baz','qux') value, CASE WHEN x % 2 = 0 THEN CAST(null AS double) ELSE rnd_double() END key FROM long_sequence(2000000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                String sql = "SELECT sym, arg_max(value, key) FROM tab GROUP BY sym ORDER BY sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testArgMaxSimple() throws SqlException {
        execute("CREATE TABLE tab (value varchar, key double)");
        execute("INSERT INTO tab VALUES ('alpha', 1.0), ('beta', 3.0), ('gamma', 2.0)");
        assertSql("arg_max\nbeta\n", "SELECT arg_max(value, key) FROM tab");
    }

    @Test
    public void testArgMaxTieBreaking() throws SqlException {
        execute("CREATE TABLE tab (value varchar, key double)");
        execute("INSERT INTO tab VALUES ('alpha', 3.0), ('beta', 3.0), ('gamma', 1.0)");
        assertSql("arg_max\nalpha\n", "SELECT arg_max(value, key) FROM tab");
    }

    @Test
    public void testArgMaxWithGroupBy() throws SqlException {
        execute("CREATE TABLE tab (sym symbol, value varchar, key double)");
        execute("""
                INSERT INTO tab VALUES
                    ('A', 'alpha', 1.0),
                    ('A', 'beta', 3.0),
                    ('B', 'gamma', 5.0),
                    ('B', 'delta', 4.0)
                """);
        assertSql("sym\targ_max\nA\tbeta\nB\tgamma\n", "SELECT sym, arg_max(value, key) FROM tab ORDER BY sym");
    }

    @Test
    public void testArgMaxWithNullKey() throws SqlException {
        execute("CREATE TABLE tab (value varchar, key double)");
        execute("INSERT INTO tab VALUES ('alpha', null), ('beta', 3.0), ('gamma', 2.0)");
        assertSql("arg_max\nbeta\n", "SELECT arg_max(value, key) FROM tab");
    }

    @Test
    public void testArgMaxWithNullValue() throws SqlException {
        execute("CREATE TABLE tab (value varchar, key double)");
        execute("INSERT INTO tab VALUES (null, 5.0), ('beta', 3.0)");
        assertSql("arg_max\n\n", "SELECT arg_max(value, key) FROM tab");
    }

    @Test
    public void testArgMaxWithNullValueNotAtMax() throws SqlException {
        execute("CREATE TABLE tab (value varchar, key double)");
        execute("INSERT INTO tab VALUES (null, 1.0), ('beta', 3.0)");
        assertSql("arg_max\nbeta\n", "SELECT arg_max(value, key) FROM tab");
    }
}
