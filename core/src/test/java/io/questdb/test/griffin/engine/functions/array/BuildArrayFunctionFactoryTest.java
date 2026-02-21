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

package io.questdb.test.griffin.engine.functions.array;

import io.questdb.cairo.CairoException;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class BuildArrayFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void test1dArrayFillerCopy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT ARRAY[1.0, 2.0, 3.0] AS arr FROM long_sequence(1))");
            assertSqlWithTypes(
                    """
                            array_build
                            [1.0,2.0,3.0]:DOUBLE[]
                            """,
                    "SELECT array_build(1, arr, arr) FROM t"
            );
        });
    }

    @Test
    public void test1dArrayFillerNaNPad() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT ARRAY[1.0, 2.0, 3.0] AS arr FROM long_sequence(1))");
            assertSqlWithTypes(
                    """
                            array_build
                            [1.0,2.0,3.0,null,null]:DOUBLE[]
                            """,
                    "SELECT array_build(1, 5, arr) FROM t"
            );
        });
    }

    @Test
    public void test1dArrayFillerTruncate() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT ARRAY[1.0, 2.0, 3.0, 4.0, 5.0] AS arr FROM long_sequence(1))");
            assertSqlWithTypes(
                    """
                            array_build
                            [1.0,2.0]:DOUBLE[]
                            """,
                    "SELECT array_build(1, 2, arr) FROM t"
            );
        });
    }

    @Test
    public void test1dArraySizeFromArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT ARRAY[1.0, 2.0, 3.0] AS arr FROM long_sequence(1))");
            assertSqlWithTypes(
                    """
                            array_build
                            [42.0,42.0,42.0]:DOUBLE[]
                            """,
                    "SELECT array_build(1, arr, 42.0) FROM t"
            );
        });
    }

    @Test
    public void test1dScalarFill() throws Exception {
        assertMemoryLeak(() -> assertSqlWithTypes(
                """
                        array_build
                        [0.0,0.0,0.0]:DOUBLE[]
                        """,
                "SELECT array_build(1, 3, 0)"
        ));
    }

    @Test
    public void test1dScalarFillWithNaN() throws Exception {
        assertMemoryLeak(() -> assertSqlWithTypes(
                """
                        array_build
                        [null,null]:DOUBLE[]
                        """,
                "SELECT array_build(1, 2, NaN)"
        ));
    }

    @Test
    public void test1dScalarFillWithValue() throws Exception {
        assertMemoryLeak(() -> assertSqlWithTypes(
                """
                        array_build
                        [1.5,1.5,1.5,1.5]:DOUBLE[]
                        """,
                "SELECT array_build(1, 4, 1.5)"
        ));
    }

    @Test
    public void test1dVariableSize() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT x::INT AS sz FROM long_sequence(3))");
            assertSql(
                    """
                            array_build
                            [1.0]
                            [1.0,1.0]
                            [1.0,1.0,1.0]
                            """,
                    "SELECT array_build(1, sz, 1.0) FROM t"
            );
        });
    }

    @Test
    public void test1dZeroSize() throws Exception {
        assertMemoryLeak(() -> assertSqlWithTypes(
                """
                        array_build
                        []:DOUBLE[]
                        """,
                "SELECT array_build(1, 0, 1.0)"
        ));
    }

    @Test
    public void test2dArrayFillerNaNPad() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT ARRAY[1.0, 2.0, 3.0] AS arr FROM long_sequence(1))");
            assertSqlWithTypes(
                    """
                            array_build
                            [[1.0,2.0,3.0,null,null],[4.0,4.0,4.0,4.0,4.0]]:DOUBLE[][]
                            """,
                    "SELECT array_build(2, 5, arr, 4.0) FROM t"
            );
        });
    }

    @Test
    public void test2dArrayFillerTruncate() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT ARRAY[1.0, 2.0, 3.0, 4.0, 5.0] AS arr FROM long_sequence(1))");
            assertSqlWithTypes(
                    """
                            array_build
                            [[1.0,2.0],[7.0,7.0]]:DOUBLE[][]
                            """,
                    "SELECT array_build(2, 2, arr, 7.0) FROM t"
            );
        });
    }

    @Test
    public void test2dArrayFillers() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT ARRAY[1.0, 2.0, 3.0] AS a, ARRAY[4.0, 5.0, 6.0] AS b FROM long_sequence(1))");
            assertSqlWithTypes(
                    """
                            array_build
                            [[1.0,2.0,3.0],[4.0,5.0,6.0]]:DOUBLE[][]
                            """,
                    "SELECT array_build(2, a, a, b) FROM t"
            );
        });
    }

    @Test
    public void test2dMixedFillers() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT ARRAY[1.0, 2.0, 3.0] AS arr FROM long_sequence(1))");
            assertSqlWithTypes(
                    """
                            array_build
                            [[1.0,2.0,3.0],[5.0,5.0,5.0]]:DOUBLE[][]
                            """,
                    "SELECT array_build(2, arr, arr, 5.0) FROM t"
            );
        });
    }

    @Test
    public void test2dScalarFillers() throws Exception {
        assertMemoryLeak(() -> assertSqlWithTypes(
                """
                        array_build
                        [[1.0,1.0,1.0],[2.0,2.0,2.0]]:DOUBLE[][]
                        """,
                "SELECT array_build(2, 3, 1.0, 2.0)"
        ));
    }

    @Test
    public void test2dVariableSize() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT x::INT AS sz FROM long_sequence(3))");
            assertSql(
                    """
                            array_build
                            [[1.0],[2.0]]
                            [[1.0,1.0],[2.0,2.0]]
                            [[1.0,1.0,1.0],[2.0,2.0,2.0]]
                            """,
                    "SELECT array_build(2, sz, 1.0, 2.0) FROM t"
            );
        });
    }

    @Test
    public void test2dZeroSize() throws Exception {
        assertMemoryLeak(() -> assertSqlWithTypes(
                """
                        array_build
                        []:DOUBLE[][]
                        """,
                "SELECT array_build(2, 0, 1.0, 2.0)"
        ));
    }

    @Test
    public void test2dZeroSizeArrayFiller() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT ARRAY[1.0, 2.0, 3.0] AS arr FROM long_sequence(1))");
            assertSqlWithTypes(
                    """
                            array_build
                            []:DOUBLE[][]
                            """,
                    "SELECT array_build(2, 0, arr, 7.0) FROM t"
            );
        });
    }

    @Test
    public void testBindVariableAsSize() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.getBindVariableService().setInt(0, 5);
            assertSqlWithTypes(
                    """
                            array_build
                            [0.0,0.0,0.0,0.0,0.0]:DOUBLE[]
                            """,
                    "SELECT array_build(1, $1, 0)"
            );
        });
    }

    @Test
    public void testComputedScalarFiller() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT ARRAY[10.0, 20.0, 30.0] AS arr FROM long_sequence(1))");
            assertSqlWithTypes(
                    """
                            array_build
                            [30.0,30.0,30.0]:DOUBLE[]
                            """,
                    "SELECT array_build(1, 3, array_max(arr)) FROM t"
            );
        });
    }

    @Test
    public void testError2dElementCountExceedsMax() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertSqlWithTypes("ignored\n", "SELECT array_build(2, 5_500_000, 1.0, 2.0)");
                Assert.fail("expected CairoException");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "array element count exceeds max");
            }
        });
    }

    @Test
    public void testErrorMultiDimArrayFiller() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT rnd_double_array(2, 0, 0, 3, 3) AS arr2d FROM long_sequence(1))");
            try {
                assertSqlWithTypes("ignored\n", "SELECT array_build(1, 3, arr2d) FROM t");
                Assert.fail("expected SqlException for DOUBLE[][] filler");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "filler array argument must be DOUBLE[]");
            }
        });
    }

    @Test
    public void testErrorMultiDimArraySize() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT rnd_double_array(2, 0, 0, 3, 3) AS arr2d FROM long_sequence(1))");
            try {
                assertSqlWithTypes("ignored\n", "SELECT array_build(1, arr2d, 0) FROM t");
                Assert.fail("expected SqlException for DOUBLE[][] size");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "size array argument must be DOUBLE[]");
            }
        });
    }

    @Test
    public void testErrorNArraysNegative() throws Exception {
        assertException(
                "SELECT array_build(-1, 3, 0)",
                19,
                "nArrays out of range"
        );
    }

    @Test
    public void testErrorNArraysOverflow() throws Exception {
        assertException(
                "SELECT array_build(3_000_000_000, 3, 0)",
                19,
                "nArrays out of range"
        );
    }

    @Test
    public void testErrorNArraysTooManyForArgCount() throws Exception {
        assertException(
                "SELECT array_build(33, 3, 0)",
                7,
                "array_build with nArrays=33 requires 35 arguments, got 3"
        );
    }

    @Test
    public void testErrorNArraysZero() throws Exception {
        assertException(
                "SELECT array_build(0, 3, 0)",
                19,
                "nArrays out of range"
        );
    }

    @Test
    public void testErrorNegativeSize() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertSqlWithTypes("ignored\n", "SELECT array_build(1, -1, 0)");
                Assert.fail("expected CairoException");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "size must not be negative");
            }
        });
    }

    @Test
    public void testErrorNonConstantNArrays() throws Exception {
        // bind variables are rejected by the function resolver before reaching our factory
        assertMemoryLeak(() -> {
            sqlExecutionContext.getBindVariableService().setLong(0, 1);
            try {
                assertSqlWithTypes("ignored\n", "SELECT array_build($1, 3, 0)");
                Assert.fail("expected SqlException for non-constant nArrays");
            } catch (SqlException ignored) {
            }
        });
    }

    @Test
    public void testErrorNotEnoughFillers() throws Exception {
        assertException(
                "SELECT array_build(2, 3, 1.0)",
                7,
                "array_build with nArrays=2 requires 4 arguments, got 3"
        );
    }

    @Test
    public void testErrorSizeExceedsMax() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertSqlWithTypes("ignored\n", "SELECT array_build(1, 268_435_456, 0)");
                Assert.fail("expected CairoException");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "size exceeds maximum");
            }
        });
    }

    @Test
    public void testErrorTooFewArgs() throws Exception {
        assertException(
                "SELECT array_build(1, 3)",
                7,
                "array_build requires at least 3 arguments"
        );
    }

    @Test
    public void testErrorTooManyFillers() throws Exception {
        assertException(
                "SELECT array_build(1, 3, 1.0, 2.0)",
                7,
                "array_build with nArrays=1 requires 3 arguments, got 4"
        );
    }

    @Test
    public void testLargeArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT x AS id FROM long_sequence(1))");
            assertSql(
                    """
                            cnt
                            1000
                            """,
                    "SELECT array_count(array_build(1, 1_000, 42.0)) AS cnt FROM t"
            );
        });
    }

    @Test
    public void testLargeArray2d() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT x AS id FROM long_sequence(1))");
            assertSql(
                    """
                            cnt
                            2000
                            """,
                    "SELECT array_count(array_build(2, 1_000, 1.0, 2.0)) AS cnt FROM t"
            );
        });
    }

    @Test
    public void testMultipleRowsWithArrayColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t AS (
                        SELECT
                            ARRAY[x::DOUBLE, (x * 10)::DOUBLE] AS arr
                        FROM long_sequence(3)
                    )
                    """);
            assertSql(
                    """
                            array_build
                            [1.0,10.0]
                            [2.0,20.0]
                            [3.0,30.0]
                            """,
                    "SELECT array_build(1, arr, arr) FROM t"
            );
        });
    }

    @Test
    public void testNArraysThreeScalarFillers() throws Exception {
        assertMemoryLeak(() -> assertSqlWithTypes(
                """
                        array_build
                        [[1.0,1.0,1.0],[2.0,2.0,2.0],[3.0,3.0,3.0]]:DOUBLE[][]
                        """,
                "SELECT array_build(3, 3, 1.0, 2.0, 3.0)"
        ));
    }

    @Test
    public void testNullArrayFiller() throws Exception {
        assertMemoryLeak(() -> assertSqlWithTypes(
                """
                        array_build
                        [null,null,null]:DOUBLE[]
                        """,
                "SELECT array_build(1, 3, null::DOUBLE[])"
        ));
    }

    @Test
    public void testNullArrayFillerFromColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t AS (
                        SELECT CASE WHEN x = 2 THEN null::DOUBLE[]
                                    ELSE ARRAY[x::DOUBLE, (x * 10)::DOUBLE, (x * 100)::DOUBLE]
                               END AS arr
                        FROM long_sequence(3)
                    )
                    """);
            assertSql(
                    """
                            array_build
                            [1.0,10.0,100.0]
                            [null,null,null]
                            [3.0,30.0,300.0]
                            """,
                    "SELECT array_build(1, 3, arr) FROM t"
            );
        });
    }

    @Test
    public void testNullArraySize() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT null::DOUBLE[] AS arr FROM long_sequence(1))");
            assertSqlWithTypes(
                    """
                            array_build
                            null:DOUBLE[]
                            """,
                    "SELECT array_build(1, arr, 1.0) FROM t"
            );
        });
    }

    @Test
    public void testNullScalarSizeFromColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t AS (
                        SELECT CASE WHEN x = 2 THEN null::INT ELSE x::INT END AS sz
                        FROM long_sequence(3)
                    )
                    """);
            assertSqlWithTypes(
                    """
                            array_build
                            [1.0]:DOUBLE[]
                            null:DOUBLE[]
                            [1.0,1.0,1.0]:DOUBLE[]
                            """,
                    "SELECT array_build(1, sz, 1.0) FROM t"
            );
        });
    }

    @Test
    public void testNullScalarSizeReturnsNull() throws Exception {
        assertMemoryLeak(() -> assertSqlWithTypes(
                """
                        array_build
                        null:DOUBLE[]
                        """,
                "SELECT array_build(1, null::INT, 0)"
        ));
    }

    @Test
    public void testOrderBookStyle() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE book AS (
                        SELECT ARRAY[100.0, 101.0, 102.0] AS ask_price,
                               ARRAY[10.0, 20.0, 30.0] AS ask_size,
                               ARRAY[99.0, 98.0, 97.0] AS bid_price,
                               ARRAY[15.0, 25.0, 35.0] AS bid_size
                        FROM long_sequence(1)
                    )
                    """);
            assertSqlWithTypes(
                    """
                            array_build
                            [[100.0,101.0,102.0],[10.0,20.0,30.0],[99.0,98.0,97.0],[15.0,25.0,35.0]]:DOUBLE[][]
                            """,
                    "SELECT array_build(4, ask_price, ask_price, ask_size, bid_price, bid_size) FROM book"
            );
        });
    }
}
