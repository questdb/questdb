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

package io.questdb.test.griffin.engine.orderby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class OrderBySortKeyMaterializationTest extends AbstractCairoTest {

    @Test
    public void testCorrectnessBinaryExpression() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (3, 10, '2024-01-01T00:00:00.000000Z'),
                    (1, 20, '2024-01-01T00:00:01.000000Z'),
                    (2, 30, '2024-01-01T00:00:02.000000Z'),
                    (5, 5, '2024-01-01T00:00:03.000000Z'),
                    (4, 15, '2024-01-01T00:00:04.000000Z')
                    """);
            assertQueryNoLeakCheck(
                    """
                            x
                            10
                            13
                            19
                            21
                            32
                            """,
                    "SELECT a + b AS x FROM t ORDER BY x"
            );
        });
    }

    @Test
    public void testCorrectnessDescending() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (3, 10, '2024-01-01T00:00:00.000000Z'),
                    (1, 20, '2024-01-01T00:00:01.000000Z'),
                    (2, 30, '2024-01-01T00:00:02.000000Z')
                    """);
            assertQueryNoLeakCheck(
                    """
                            x
                            32
                            21
                            13
                            """,
                    "SELECT a + b AS x FROM t ORDER BY x DESC"
            );
        });
    }

    @Test
    public void testCorrectnessEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, ts TIMESTAMP) TIMESTAMP(ts)");
            assertQueryNoLeakCheck(
                    "x\n",
                    "SELECT a + b AS x FROM t ORDER BY x"
            );
        });
    }

    @Test
    public void testCorrectnessMultipleSortKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, c INT, d INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (1, 2, 10, 5, '2024-01-01T00:00:00.000000Z'),
                    (1, 2, 3, 4, '2024-01-01T00:00:01.000000Z'),
                    (3, 4, 10, 5, '2024-01-01T00:00:02.000000Z'),
                    (3, 4, 3, 4, '2024-01-01T00:00:03.000000Z')
                    """);
            assertQueryNoLeakCheck(
                    """
                            x\ty
                            3\t7
                            3\t15
                            7\t7
                            7\t15
                            """,
                    "SELECT a + b AS x, c + d AS y FROM t ORDER BY x, y"
            );
        });
    }

    @Test
    public void testCorrectnessSingleRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (3, 10, '2024-01-01T00:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            x
                            13
                            """,
                    "SELECT a + b AS x FROM t ORDER BY x"
            );
        });
    }

    @Test
    public void testCorrectnessWithDoubleType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (3.5, 10.1, '2024-01-01T00:00:00.000000Z'),
                    (1.2, 20.3, '2024-01-01T00:00:01.000000Z'),
                    (2.7, 30.8, '2024-01-01T00:00:02.000000Z')
                    """);
            assertQueryNoLeakCheck(
                    """
                            x
                            13.6
                            21.5
                            33.5
                            """,
                    "SELECT a + b AS x FROM t ORDER BY x"
            );
        });
    }

    @Test
    public void testCorrectnessWithFloatType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a FLOAT, b FLOAT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (3.5, 10.0, '2024-01-01T00:00:00.000000Z'),
                    (1.0, 20.0, '2024-01-01T00:00:01.000000Z'),
                    (2.0, 30.0, '2024-01-01T00:00:02.000000Z')
                    """);
            assertQueryNoLeakCheck(
                    """
                            x
                            13.5
                            21.0
                            32.0
                            """,
                    "SELECT a + b AS x FROM t ORDER BY x"
            );
        });
    }

    @Test
    public void testCorrectnessWithLongArithmetic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a LONG, b LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (3, 10, '2024-01-01T00:00:00.000000Z'),
                    (1, 20, '2024-01-01T00:00:01.000000Z'),
                    (2, 30, '2024-01-01T00:00:02.000000Z')
                    """);
            assertQueryNoLeakCheck(
                    """
                            x
                            13
                            21
                            32
                            """,
                    "SELECT a + b AS x FROM t ORDER BY x"
            );
        });
    }

    @Test
    public void testCorrectnessWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (3, 10, '2024-01-01T00:00:00.000000Z'),
                    (NULL, 20, '2024-01-01T00:00:01.000000Z'),
                    (2, 30, '2024-01-01T00:00:02.000000Z')
                    """);
            assertQueryNoLeakCheck(
                    """
                            x
                            null
                            13
                            32
                            """,
                    "SELECT a + b AS x FROM t ORDER BY x"
            );
        });
    }

    @Test
    public void testExplainNoMaterializeForSimpleExpression() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            // a + b has complexity 2, below default threshold (3)
            // Should NOT show "Materialize sort keys"
            assertPlanNoLeakCheck(
                    "SELECT a + b AS x FROM t ORDER BY x",
                    """
                            Sort light
                              keys: [x]
                                VirtualRecord
                                  functions: [a+b]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                            """
            );
        });
    }

    @Test
    public void testExplainShowsMaterializeForComplexExpression() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            // (a + b) * (c + d) has complexity 4, which exceeds default threshold (3)
            assertPlanNoLeakCheck(
                    "SELECT (a + b) * (c + d) AS x FROM t ORDER BY x",
                    """
                            Sort light
                              keys: [x]
                                Materialize sort keys
                                    VirtualRecord
                                      functions: [a+b*c+d]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                            """
            );
        });
    }

    @Test
    public void testMixedMaterializedAndNonMaterialized() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, c INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (3, 10, 1, '2024-01-01T00:00:00.000000Z'),
                    (1, 20, 2, '2024-01-01T00:00:01.000000Z'),
                    (3, 10, 3, '2024-01-01T00:00:02.000000Z')
                    """);
            assertQueryNoLeakCheck(
                    """
                            x\tc
                            13\t1
                            13\t3
                            21\t2
                            """,
                    "SELECT a + b AS x, c FROM t ORDER BY x, c"
            );
        });
    }

    @Test
    public void testVariableLengthTypeNotMaterialized() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a VARCHAR, b VARCHAR, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    ('c', 'c', '2024-01-01T00:00:00.000000Z'),
                    ('a', 'a', '2024-01-01T00:00:01.000000Z'),
                    ('b', 'b', '2024-01-01T00:00:02.000000Z')
                    """);
            assertQueryNoLeakCheck(
                    """
                            x
                            aa
                            bb
                            cc
                            """,
                    "SELECT concat(a, b) AS x FROM t ORDER BY x"
            );
        });
    }
}
