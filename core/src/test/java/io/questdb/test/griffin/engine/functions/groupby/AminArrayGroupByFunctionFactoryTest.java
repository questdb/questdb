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

import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class AminArrayGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            execute("INSERT INTO tab VALUES (null)");
            execute("INSERT INTO tab VALUES (null)");
            assertQueryNoLeakCheck(
                    """
                            arr
                            null
                            """,
                    "SELECT amin(arr) arr FROM tab",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testKeyed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (grp INT, arr DOUBLE[])");
            execute("INSERT INTO tab VALUES (1, ARRAY[10.0, 11.0])");
            execute("INSERT INTO tab VALUES (1, ARRAY[20.0, 21.0])");
            execute("INSERT INTO tab VALUES (1, ARRAY[30.0, 31.0])");
            execute("INSERT INTO tab VALUES (2, ARRAY[40.0, 41.0])");
            execute("INSERT INTO tab VALUES (2, ARRAY[50.0, 51.0])");
            assertQueryNoLeakCheck(
                    """
                            grp\tarr
                            1\t[10.0,11.0]
                            2\t[40.0,41.0]
                            """,
                    "SELECT grp, amin(arr) arr FROM tab ORDER BY grp",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testNanElements() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            execute("INSERT INTO tab VALUES (ARRAY[1.0, 2.0, 1.0])");
            execute("INSERT INTO tab VALUES (ARRAY[3.0, null, 5.0])");
            assertQueryNoLeakCheck(
                    """
                            arr
                            [1.0,2.0,1.0]
                            """,
                    "SELECT amin(arr) arr FROM tab",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testNotKeyed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            execute("INSERT INTO tab VALUES (ARRAY[1.0, 2.0])");
            execute("INSERT INTO tab VALUES (ARRAY[3.0, 4.0])");
            assertQueryNoLeakCheck(
                    """
                            arr
                            [1.0,2.0]
                            """,
                    "SELECT amin(arr) arr FROM tab",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testNullArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            execute("INSERT INTO tab VALUES (null)");
            execute("INSERT INTO tab VALUES (ARRAY[1.0, 2.0])");
            assertQueryNoLeakCheck(
                    """
                            arr
                            [1.0,2.0]
                            """,
                    "SELECT amin(arr) arr FROM tab",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, arr DOUBLE[]) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tab VALUES ('2024-01-01T00:00:00', ARRAY[1.0, 2.0])");
            execute("INSERT INTO tab VALUES ('2024-01-01T00:30:00', ARRAY[3.0, 4.0])");
            execute("INSERT INTO tab VALUES ('2024-01-01T01:00:00', ARRAY[10.0, 20.0])");
            assertQueryNoLeakCheck(
                    """
                            ts\tarr
                            2024-01-01T00:00:00.000000Z\t[1.0,2.0]
                            2024-01-01T01:00:00.000000Z\t[10.0,20.0]
                            """,
                    "SELECT ts, amin(arr) arr FROM tab SAMPLE BY 1h",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testSingleRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            execute("INSERT INTO tab VALUES (ARRAY[1.0, 2.0])");
            assertQueryNoLeakCheck(
                    """
                            arr
                            [1.0,2.0]
                            """,
                    "SELECT amin(arr) arr FROM tab",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testParallel() throws Exception {
        execute("CREATE TABLE tab AS (" +
                "SELECT rnd_symbol('A','B','C','D','E') sym, " +
                "rnd_double_array(1, 0, 0, 5) arr " +
                "FROM long_sequence(100_000))");

        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        String sql = "SELECT sym, amin(arr) FROM tab GROUP BY sym ORDER BY sym";
                        TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
                    },
                    configuration,
                    LOG
            );
        }
    }

    @Test
    public void testParallelAllNulls() throws Exception {
        execute("CREATE TABLE tab AS (" +
                "SELECT rnd_symbol('A','B','C','D','E') sym, " +
                "CAST(null AS DOUBLE[]) arr " +
                "FROM long_sequence(100_000))");

        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        String sql = "SELECT sym, amin(arr) FROM tab GROUP BY sym ORDER BY sym";
                        TestUtils.assertSql(
                                engine,
                                sqlExecutionContext,
                                sql,
                                sink,
                                """
                                        sym\tamin
                                        A\tnull
                                        B\tnull
                                        C\tnull
                                        D\tnull
                                        E\tnull
                                        """
                        );
                    },
                    configuration,
                    LOG
            );
        }
    }

    @Test
    public void testParallelWithNulls() throws Exception {
        execute("CREATE TABLE tab AS (" +
                "SELECT rnd_symbol('A','B','C','D','E') sym, " +
                "CASE WHEN x % 3 = 0 THEN CAST(null AS DOUBLE[]) ELSE rnd_double_array(1, 0, 0, 5) END arr " +
                "FROM long_sequence(100_000))");

        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        String sql = "SELECT sym, amin(arr) FROM tab GROUP BY sym ORDER BY sym";
                        TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
                    },
                    configuration,
                    LOG
            );
        }
    }

    @Test
    public void testVariableLengthArrays() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            execute("INSERT INTO tab VALUES (ARRAY[1.0, 2.0])");
            execute("INSERT INTO tab VALUES (ARRAY[3.0, 4.0, 5.0])");
            assertQueryNoLeakCheck(
                    """
                            arr
                            [1.0,2.0,5.0]
                            """,
                    "SELECT amin(arr) arr FROM tab",
                    null,
                    false,
                    true
            );
        });
    }
}
