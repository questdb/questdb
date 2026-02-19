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

public class AavgArrayGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testAllNanAtPosition() throws Exception {
        // Both rows have NaN at position 0 → result should be NaN there
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            execute("INSERT INTO tab VALUES (ARRAY[null, 2.0])");
            execute("INSERT INTO tab VALUES (ARRAY[null, 4.0])");
            assertQueryNoLeakCheck(
                    """
                            arr
                            [null,3.0]
                            """,
                    "SELECT aavg(arr) arr FROM tab",
                    null,
                    false,
                    true
            );
        });
    }

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
                    "SELECT aavg(arr) arr FROM tab",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testEmptyArray() throws Exception {
        // Empty arrays should be skipped, like nulls
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            execute("INSERT INTO tab VALUES (ARRAY[])");
            execute("INSERT INTO tab VALUES (ARRAY[1.0, 2.0])");
            assertQueryNoLeakCheck(
                    """
                            arr
                            [1.0,2.0]
                            """,
                    "SELECT aavg(arr) arr FROM tab",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testFirstRowAllNan() throws Exception {
        // First row has all NaN → starts in variable mode; subsequent rows fill in
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            execute("INSERT INTO tab VALUES (ARRAY[null, null])");
            execute("INSERT INTO tab VALUES (ARRAY[4.0, 6.0])");
            assertQueryNoLeakCheck(
                    """
                            arr
                            [4.0,6.0]
                            """,
                    "SELECT aavg(arr) arr FROM tab",
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
            execute("INSERT INTO tab VALUES (1, ARRAY[10.0, 20.0])");
            execute("INSERT INTO tab VALUES (1, ARRAY[30.0, 40.0])");
            execute("INSERT INTO tab VALUES (2, ARRAY[100.0, 200.0])");
            assertQueryNoLeakCheck(
                    """
                            grp\tarr
                            1\t[20.0,30.0]
                            2\t[100.0,200.0]
                            """,
                    "SELECT grp, aavg(arr) arr FROM tab ORDER BY grp",
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
            execute("INSERT INTO tab VALUES (ARRAY[3.0, null, 6.0])");
            execute("INSERT INTO tab VALUES (ARRAY[1.0, 2.0, 2.0])");
            assertQueryNoLeakCheck(
                    """
                            arr
                            [2.0,2.0,4.0]
                            """,
                    "SELECT aavg(arr) arr FROM tab",
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
                            [2.0,3.0]
                            """,
                    "SELECT aavg(arr) arr FROM tab",
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
                    "SELECT aavg(arr) arr FROM tab",
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
                            2024-01-01T00:00:00.000000Z\t[2.0,3.0]
                            2024-01-01T01:00:00.000000Z\t[10.0,20.0]
                            """,
                    "SELECT ts, aavg(arr) arr FROM tab SAMPLE BY 1h",
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
                    "SELECT aavg(arr) arr FROM tab",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testShorterInputTriggersVariableMode() throws Exception {
        // First row length=3, second row length=2 → variable mode via inputLen < accLen
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            execute("INSERT INTO tab VALUES (ARRAY[6.0, 4.0, 9.0])");
            execute("INSERT INTO tab VALUES (ARRAY[2.0, 8.0])");
            // pos 0: (6+2)/2=4.0, pos 1: (4+8)/2=6.0, pos 2: 9/1=9.0
            assertQueryNoLeakCheck(
                    """
                            arr
                            [4.0,6.0,9.0]
                            """,
                    "SELECT aavg(arr) arr FROM tab",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testUniformManyRows() throws Exception {
        // Many rows, all same length, all finite → stays uniform mode
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            execute("""
                    INSERT INTO tab VALUES
                    (ARRAY[10.0, 20.0]),
                    (ARRAY[20.0, 40.0]),
                    (ARRAY[30.0, 60.0]),
                    (ARRAY[40.0, 80.0])
                    """);
            // pos 0: (10+20+30+40)/4=25.0, pos 1: (20+40+60+80)/4=50.0
            assertQueryNoLeakCheck(
                    """
                            arr
                            [25.0,50.0]
                            """,
                    "SELECT aavg(arr) arr FROM tab",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testUniformToVariableTransition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            execute("INSERT INTO tab VALUES (ARRAY[1.0, 2.0])");
            execute("INSERT INTO tab VALUES (ARRAY[3.0, 4.0])");
            execute("INSERT INTO tab VALUES (ARRAY[null, 6.0])");
            assertQueryNoLeakCheck(
                    """
                            arr
                            [2.0,4.0]
                            """,
                    "SELECT aavg(arr) arr FROM tab",
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
                        String sql = "SELECT sym, aavg(arr) FROM tab GROUP BY sym ORDER BY sym";
                        // Verify parallel execution plan
                        TestUtils.assertSql(
                                engine,
                                sqlExecutionContext,
                                "EXPLAIN " + sql,
                                sink,
                                """
                                        QUERY PLAN
                                        Sort light
                                          keys: [sym]
                                            Async Group By workers: 4
                                              keys: [sym]
                                              values: [aavg(arr)]
                                              filter: null
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: tab
                                        """
                        );
                        // Exact comparison skipped due to floating-point non-associativity
                        TestUtils.printSql(engine, sqlExecutionContext, sql, sink);
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
                        String sql = "SELECT sym, aavg(arr) FROM tab GROUP BY sym ORDER BY sym";
                        TestUtils.assertSql(
                                engine,
                                sqlExecutionContext,
                                sql,
                                sink,
                                """
                                        sym\taavg
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
                        String sql = "SELECT sym, aavg(arr) FROM tab GROUP BY sym ORDER BY sym";
                        // Exact comparison skipped due to floating-point non-associativity
                        TestUtils.printSql(engine, sqlExecutionContext, sql, sink);
                    },
                    configuration,
                    LOG
            );
        }
    }

    @Test
    public void testParallelMergeNullDest() throws Exception {
        // Group 'Z' has null arrays in the first half and non-null arrays in the
        // second half. With 4 workers, early workers see only nulls for 'Z' (ptr=0).
        // When a later worker's non-null 'Z' is merged into the dest that was copied
        // from an all-null worker, merge() hits the destPtr==0 branch.
        execute("""
                CREATE TABLE tab AS (
                SELECT
                    CASE WHEN x % 5000 = 0 THEN 'Z'::symbol ELSE rnd_symbol('A','B','C','D','E') END sym,
                    CASE WHEN x % 5000 = 0 AND x < 50_000 THEN CAST(null AS DOUBLE[])
                         ELSE rnd_double_array(1, 0, 0, 5) END arr
                FROM long_sequence(100_000))
                """);

        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        String sql = "SELECT sym, aavg(arr) FROM tab GROUP BY sym ORDER BY sym";
                        TestUtils.printSql(engine, sqlExecutionContext, sql, sink);
                    },
                    configuration,
                    LOG
            );
        }
    }

    @Test
    public void testParallelVariableLength() throws Exception {
        // Partitioned table where all groups have short arrays (length 2) in the
        // first half and long arrays (length 5) in the second half. Multiple
        // partitions ensure page frames are distributed across workers, so early
        // workers build short accumulators and later workers have longer ones,
        // exercising the srcLen > destLen growth branch in merge().
        execute("""
                CREATE TABLE tab (ts TIMESTAMP, sym SYMBOL, arr DOUBLE[]) TIMESTAMP(ts) PARTITION BY HOUR
                """);
        execute("""
                INSERT INTO tab SELECT
                    '2024-01-01'::timestamp + x * 1_000_000L ts,
                    rnd_symbol('A','B','C','D','E') sym,
                    CASE WHEN x <= 50_000 THEN rnd_double_array(1, 0, 0, 2)
                         ELSE rnd_double_array(1, 0, 0, 5) END arr
                FROM long_sequence(100_000)
                """);

        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        String sql = "SELECT sym, aavg(arr) FROM tab GROUP BY sym ORDER BY sym";
                        TestUtils.printSql(engine, sqlExecutionContext, sql, sink);
                    },
                    configuration,
                    LOG
            );
        }
    }

    @Test
    public void testVariableGrowthWhileAlreadyVariable() throws Exception {
        // Row 1: [1,null] → variable mode (NaN at pos 1)
        // Row 2: [3,4,5]  → growth while already in variable mode
        // pos 0: (1+3)/2=2.0, pos 1: 4/1=4.0, pos 2: 5/1=5.0
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            execute("INSERT INTO tab VALUES (ARRAY[1.0, null])");
            execute("INSERT INTO tab VALUES (ARRAY[3.0, 4.0, 5.0])");
            assertQueryNoLeakCheck(
                    """
                            arr
                            [2.0,4.0,5.0]
                            """,
                    "SELECT aavg(arr) arr FROM tab",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testVariableLengthArrays() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            execute("INSERT INTO tab VALUES (ARRAY[1.0, 2.0])");
            execute("INSERT INTO tab VALUES (ARRAY[3.0, 4.0, 5.0])");
            // pos 0: (1+3)/2=2.0, pos 1: (2+4)/2=3.0, pos 2: 5/1=5.0
            assertQueryNoLeakCheck(
                    """
                            arr
                            [2.0,3.0,5.0]
                            """,
                    "SELECT aavg(arr) arr FROM tab",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testVariableLengthKeyed() throws Exception {
        // Groups with different array sizes exercise keyed + variable mode
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (grp INT, arr DOUBLE[])");
            execute("""
                    INSERT INTO tab VALUES
                    (1, ARRAY[2.0, 4.0]),
                    (1, ARRAY[6.0, 8.0, 10.0]),
                    (2, ARRAY[10.0]),
                    (2, ARRAY[20.0, 30.0])
                    """);
            // grp 1 pos 0: (2+6)/2=4.0, pos 1: (4+8)/2=6.0, pos 2: 10/1=10.0
            // grp 2 pos 0: (10+20)/2=15.0, pos 1: 30/1=30.0
            assertQueryNoLeakCheck(
                    """
                            grp\tarr
                            1\t[4.0,6.0,10.0]
                            2\t[15.0,30.0]
                            """,
                    "SELECT grp, aavg(arr) arr FROM tab ORDER BY grp",
                    null,
                    true,
                    true
            );
        });
    }
}
