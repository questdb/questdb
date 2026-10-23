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
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

/**
 * Tests for arg_min(varchar, double) - returns varchar value at min double key.
 */
public class ArgMinVarcharDoubleGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testArgMinAllNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (value VARCHAR, key DOUBLE)");
            execute("INSERT INTO tab VALUES (null, null), (null, null)");
            assertQuery("SELECT arg_min(value, key) FROM tab").noLeakCheck().noRandomAccess().expectSize().returns("arg_min\n\n");
        });
    }

    @Test
    public void testArgMinEmptyStringValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (value VARCHAR, key DOUBLE)");
            execute("INSERT INTO tab VALUES ('', 1.0), ('beta', 3.0)");
            assertQuery("SELECT arg_min(value, key), length(arg_min(value, key)) len FROM tab")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("arg_min\tlen\n\t0\n");
        });
    }

    @Test
    public void testArgMinEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (value VARCHAR, key DOUBLE)");
            assertQuery("SELECT arg_min(value, key) FROM tab").noLeakCheck().noRandomAccess().expectSize().returns("arg_min\n\n");
            assertQuery("SELECT value, arg_min(value, key) FROM tab").noLeakCheck().expectSize().returns("value\targ_min\n");
        });
    }

    @Test
    public void testArgMinMixedNullValueAndNullKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (value VARCHAR, key DOUBLE)");
            execute("INSERT INTO tab VALUES (null, 1.0), ('beta', null)");
            assertQuery("SELECT arg_min(value, key) FROM tab").noLeakCheck().noRandomAccess().expectSize().returns("arg_min\n\n");
        });
    }

    @Test
    public void testArgMinParallel() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE tab AS (
                        SELECT rnd_symbol('A','B','C') sym, rnd_varchar('foo','bar','baz','qux') value, rnd_double() key
                        FROM long_sequence(10_000)
                    )""");
            try (WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)))) {
                TestUtils.execute(pool, (engine, _, sqlExecutionContext) -> assertQuery("SELECT sym, arg_min(value, key) FROM tab GROUP BY sym ORDER BY sym")
                        .withEngine(engine)
                        .withContext(sqlExecutionContext)
                        .noLeakCheck()
                        .assertsPlan(
                                """
                                        Encode sort light
                                          keys: [sym]
                                            Async Group By workers: 4
                                              keys: [sym]
                                              values: [arg_min(value,key)]
                                              filter: null
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: tab
                                        """
                        ), configuration, LOG);
            }
        });
    }

    @Test
    public void testArgMinParallelAllNullKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (SELECT rnd_symbol('A','B','C','D','E') sym, rnd_varchar('foo','bar','baz','qux') value, null::DOUBLE key FROM long_sequence(100_000))");
            try (WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)))) {
                TestUtils.execute(pool, (engine, _, sqlExecutionContext) -> assertQuery("SELECT sym, arg_min(value, key) FROM tab GROUP BY sym ORDER BY sym")
                        .withEngine(engine)
                        .withContext(sqlExecutionContext)
                        .noLeakCheck()
                        .expectSize()
                        .returns("sym\targ_min\nA\t\nB\t\nC\t\nD\t\nE\t\n"), configuration, LOG);
            }
        });
    }

    @Test
    public void testArgMinParallelChunky() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (SELECT rnd_symbol('A','B','C','D','E') sym, rnd_varchar('foo','bar','baz','qux') value, rnd_double() key FROM long_sequence(2_000_000))");
            try (WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)))) {
                TestUtils.execute(pool, (engine, _, sqlExecutionContext) -> {
                    String sql = "SELECT sym, arg_min(value, key) FROM tab GROUP BY sym ORDER BY sym";
                    TestUtils.assertSqlCursors(engine, sqlExecutionContext, ArgMinMaxTestUtils.ARG_MIN_ORACLE, sql, LOG);
                }, configuration, LOG);
            }
        });
    }

    @Test
    public void testArgMinParallelKeyedAndNotKeyed() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE tab AS (
                        SELECT
                            (x % 5)::SYMBOL sym,
                            ('s' || (x % 100))::SYMBOL sym_value,
                            ('v' || x)::STRING str_value,
                            ('v' || x)::VARCHAR varchar_value,
                            CASE WHEN x <= 1_000_000 THEN null ELSE (2_000_001 - x)::DOUBLE END key
                        FROM long_sequence(2_000_000)
                    )""");
            try (WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)))) {
                TestUtils.execute(pool, (engine, _, sqlExecutionContext) -> {
                    assertQuery("""
                            SELECT sym, arg_min(sym_value, key) k, arg_min(str_value, key) s, arg_min(varchar_value, key) v
                            FROM tab
                            GROUP BY sym
                            ORDER BY sym
                            """)
                            .withEngine(engine)
                            .withContext(sqlExecutionContext)
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    sym\tk\ts\tv
                                    0\ts0\tv2000000\tv2000000
                                    1\ts96\tv1999996\tv1999996
                                    2\ts97\tv1999997\tv1999997
                                    3\ts98\tv1999998\tv1999998
                                    4\ts99\tv1999999\tv1999999
                                    """);
                    assertQuery("SELECT arg_min(sym_value, key) k, arg_min(str_value, key) s, arg_min(varchar_value, key) v FROM tab")
                            .withEngine(engine)
                            .withContext(sqlExecutionContext)
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .returns("k\ts\tv\ns0\tv2000000\tv2000000\n");
                }, configuration, LOG);
            }
        });
    }

    @Test
    public void testArgMinParallelMergeNullDestValidSrc() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (SELECT rnd_symbol('A','B','C','D','E') sym, rnd_varchar('foo','bar','baz','qux') value, CASE WHEN x <= 1_000_000 THEN null::DOUBLE ELSE rnd_double() END key FROM long_sequence(2_000_000))");
            try (WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)))) {
                TestUtils.execute(pool, (engine, _, sqlExecutionContext) -> {
                    String sql = "SELECT sym, arg_min(value, key) FROM tab GROUP BY sym ORDER BY sym";
                    TestUtils.assertSqlCursors(engine, sqlExecutionContext, ArgMinMaxTestUtils.ARG_MIN_ORACLE, sql, LOG);
                }, configuration, LOG);
            }
        });
    }

    @Test
    public void testArgMinParallelWithNullKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (SELECT rnd_symbol('A','B','C','D','E') sym, rnd_varchar('foo','bar','baz','qux') value, CASE WHEN x % 2 = 0 THEN null::DOUBLE ELSE rnd_double() END key FROM long_sequence(2_000_000))");
            try (WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)))) {
                TestUtils.execute(pool, (engine, _, sqlExecutionContext) -> {
                    String sql = "SELECT sym, arg_min(value, key) FROM tab GROUP BY sym ORDER BY sym";
                    TestUtils.assertSqlCursors(engine, sqlExecutionContext, ArgMinMaxTestUtils.ARG_MIN_ORACLE, sql, LOG);
                }, configuration, LOG);
            }
        });
    }

    @Test
    public void testArgMinSimple() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (value VARCHAR, key DOUBLE)");
            execute("INSERT INTO tab VALUES ('alpha', 2.0), ('beta', 1.0), ('gamma', 3.0)");
            assertQuery("SELECT arg_min(value, key) FROM tab").noLeakCheck().noRandomAccess().expectSize().returns("arg_min\nbeta\n");
        });
    }

    @Test
    public void testArgMinSymbolAndStringValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (sym_value SYMBOL, str_value STRING, key DOUBLE)");
            execute("INSERT INTO tab VALUES ('alpha', 'one', 2.0), ('beta', 'two', 1.0), ('gamma', 'three', 3.0)");
            assertQuery("""
                    SELECT
                        arg_min(sym_value, key) k, arg_min(str_value, key) s,
                        typeOf(arg_min(sym_value, key)) k_type, typeOf(arg_min(str_value, key)) s_type,
                        typeOf(arg_max(sym_value, key)) k_max_type, typeOf(arg_max(str_value, key)) s_max_type
                    FROM tab
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            k\ts\tk_type\ts_type\tk_max_type\ts_max_type
                            beta\ttwo\tVARCHAR\tVARCHAR\tVARCHAR\tVARCHAR
                            """);
        });
    }

    @Test
    public void testArgMinTieBreaking() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (value VARCHAR, key DOUBLE)");
            execute("INSERT INTO tab VALUES ('alpha', 1.0), ('beta', 1.0), ('gamma', 3.0)");
            assertQuery("SELECT arg_min(value, key) FROM tab").noLeakCheck().noRandomAccess().expectSize().returns("arg_min\nalpha\n");
        });
    }

    @Test
    public void testArgMinWithGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (sym SYMBOL, value VARCHAR, key DOUBLE)");
            execute("""
                    INSERT INTO tab VALUES
                        ('A', 'alpha', 1.0),
                        ('A', 'beta', 3.0),
                        ('B', 'gamma', 5.0),
                        ('B', 'delta', 4.0),
                        ('C', null, 2.0),
                        ('C', 'epsilon', null)
                    """);
            assertQuery("SELECT sym, arg_min(value, key) FROM tab ORDER BY sym")
                    .noLeakCheck()
                    .expectSize()
                    .returns("sym\targ_min\nA\talpha\nB\tdelta\nC\t\n");
        });
    }

    @Test
    public void testArgMinWithNullKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (value VARCHAR, key DOUBLE)");
            execute("INSERT INTO tab VALUES ('alpha', null), ('beta', 3.0), ('gamma', 2.0)");
            assertQuery("SELECT arg_min(value, key) FROM tab").noLeakCheck().noRandomAccess().expectSize().returns("arg_min\ngamma\n");
        });
    }

    @Test
    public void testArgMinWithNullValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (value VARCHAR, key DOUBLE)");
            execute("INSERT INTO tab VALUES (null, 1.0), ('beta', 3.0)");
            assertQuery("SELECT arg_min(value, key), length(arg_min(value, key)) len FROM tab")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("arg_min\tlen\n\t-1\n");
        });
    }

    @Test
    public void testArgMinWithNullValueNotAtMin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (value VARCHAR, key DOUBLE)");
            execute("INSERT INTO tab VALUES (null, 3.0), ('beta', 1.0)");
            assertQuery("SELECT arg_min(value, key) FROM tab").noLeakCheck().noRandomAccess().expectSize().returns("arg_min\nbeta\n");
        });
    }
}
