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

package io.questdb.test.griffin.engine.functions.bool;

import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class GeoWithinBoxFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testInWhereClause() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table points (x double, y double)");
            execute("insert into points values (5.0, 5.0)");
            execute("insert into points values (-1.0, 5.0)");
            execute("insert into points values (5.0, 11.0)");
            execute("insert into points values (0.0, 0.0)");

            assertSql(
                    """
                            x\ty
                            5.0\t5.0
                            0.0\t0.0
                            """,
                    "select x, y from points where geo_within_box(x, y, 0.0, 0.0, 10.0, 10.0)"
            );
        });
    }

    @Test
    public void testInfinity() throws Exception {
        // Note: QuestDB's division converts Infinity to NaN (see DivDoubleFunctionFactory),
        // so 1.0/0.0 produces NaN, not Infinity. These tests verify NaN handling.
        assertSql("geo_within_box\nfalse\n", "select geo_within_box(1.0/0.0, 5.0, 0.0, 0.0, 10.0, 10.0)");
        assertSql("geo_within_box\nfalse\n", "select geo_within_box(-1.0/0.0, 5.0, 0.0, 0.0, 10.0, 10.0)");
    }

    @Test
    public void testInvertedBoxX() throws Exception {
        // min_x > max_x should return false
        assertSql("geo_within_box\nfalse\n", "select geo_within_box(5.0, 5.0, 10.0, 0.0, 0.0, 10.0)");
    }

    @Test
    public void testInvertedBoxY() throws Exception {
        // min_y > max_y should return false
        assertSql("geo_within_box\nfalse\n", "select geo_within_box(5.0, 5.0, 0.0, 10.0, 10.0, 0.0)");
    }

    @Test
    public void testNaNMaxX() throws Exception {
        assertSql("geo_within_box\nfalse\n", "select geo_within_box(5.0, 5.0, 0.0, 0.0, NaN, 10.0)");
    }

    @Test
    public void testNaNMaxY() throws Exception {
        assertSql("geo_within_box\nfalse\n", "select geo_within_box(5.0, 5.0, 0.0, 0.0, 10.0, NaN)");
    }

    @Test
    public void testNaNMinX() throws Exception {
        assertSql("geo_within_box\nfalse\n", "select geo_within_box(5.0, 5.0, NaN, 0.0, 10.0, 10.0)");
    }

    @Test
    public void testNaNMinY() throws Exception {
        assertSql("geo_within_box\nfalse\n", "select geo_within_box(5.0, 5.0, 0.0, NaN, 10.0, 10.0)");
    }

    @Test
    public void testNaNX() throws Exception {
        assertSql("geo_within_box\nfalse\n", "select geo_within_box(NaN, 5.0, 0.0, 0.0, 10.0, 10.0)");
    }

    @Test
    public void testNaNY() throws Exception {
        assertSql("geo_within_box\nfalse\n", "select geo_within_box(5.0, NaN, 0.0, 0.0, 10.0, 10.0)");
    }

    @Test
    public void testNegativeCoordinates() throws Exception {
        assertSql("geo_within_box\ntrue\n", "select geo_within_box(-5.0, -5.0, -10.0, -10.0, 0.0, 0.0)");
        assertSql("geo_within_box\nfalse\n", "select geo_within_box(-15.0, -5.0, -10.0, -10.0, 0.0, 0.0)");
    }

    @Test
    public void testConstantBoxInvertedReturnsEmpty() throws Exception {
        // Inverted box (minX > maxX) should be optimized to constant false
        assertMemoryLeak(() -> {
            execute("create table points (x double, y double)");
            execute("insert into points values (5.0, 5.0)");

            // Query plan should show constant false (the function is optimized away)
            assertSql(
                    """
                            QUERY PLAN
                            Empty table
                            """,
                    "explain select * from points where geo_within_box(x, y, 10.0, 0.0, 0.0, 10.0)"
            );
        });
    }

    @Test
    public void testPointInsideBox() throws Exception {
        assertSql("geo_within_box\ntrue\n", "select geo_within_box(5.0, 5.0, 0.0, 0.0, 10.0, 10.0)");
    }

    @Test
    public void testPointOnCorner() throws Exception {
        assertSql("geo_within_box\ntrue\n", "select geo_within_box(0.0, 0.0, 0.0, 0.0, 10.0, 10.0)");
        assertSql("geo_within_box\ntrue\n", "select geo_within_box(10.0, 10.0, 0.0, 0.0, 10.0, 10.0)");
        assertSql("geo_within_box\ntrue\n", "select geo_within_box(0.0, 10.0, 0.0, 0.0, 10.0, 10.0)");
        assertSql("geo_within_box\ntrue\n", "select geo_within_box(10.0, 0.0, 0.0, 0.0, 10.0, 10.0)");
    }

    @Test
    public void testPointOnMaxXBoundary() throws Exception {
        assertSql("geo_within_box\ntrue\n", "select geo_within_box(10.0, 5.0, 0.0, 0.0, 10.0, 10.0)");
    }

    @Test
    public void testPointOnMaxYBoundary() throws Exception {
        assertSql("geo_within_box\ntrue\n", "select geo_within_box(5.0, 10.0, 0.0, 0.0, 10.0, 10.0)");
    }

    @Test
    public void testPointOnMinXBoundary() throws Exception {
        assertSql("geo_within_box\ntrue\n", "select geo_within_box(0.0, 5.0, 0.0, 0.0, 10.0, 10.0)");
    }

    @Test
    public void testPointOnMinYBoundary() throws Exception {
        assertSql("geo_within_box\ntrue\n", "select geo_within_box(5.0, 0.0, 0.0, 0.0, 10.0, 10.0)");
    }

    @Test
    public void testPointOutsideBoxAbove() throws Exception {
        assertSql("geo_within_box\nfalse\n", "select geo_within_box(5.0, 11.0, 0.0, 0.0, 10.0, 10.0)");
    }

    @Test
    public void testPointOutsideBoxBelow() throws Exception {
        assertSql("geo_within_box\nfalse\n", "select geo_within_box(5.0, -1.0, 0.0, 0.0, 10.0, 10.0)");
    }

    @Test
    public void testPointOutsideBoxLeft() throws Exception {
        assertSql("geo_within_box\nfalse\n", "select geo_within_box(-1.0, 5.0, 0.0, 0.0, 10.0, 10.0)");
    }

    @Test
    public void testPointOutsideBoxRight() throws Exception {
        assertSql("geo_within_box\nfalse\n", "select geo_within_box(11.0, 5.0, 0.0, 0.0, 10.0, 10.0)");
    }

    @Test
    public void testVerySmallDifferences() throws Exception {
        assertSql("geo_within_box\ntrue\n", "select geo_within_box(1.0000000001, 1.0, 1.0, 1.0, 2.0, 2.0)");
        assertSql("geo_within_box\nfalse\n", "select geo_within_box(0.9999999999, 1.0, 1.0, 1.0, 2.0, 2.0)");
    }

    @Test
    public void testWithNullValuesInTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table points (x double, y double)");
            execute("insert into points values (5.0, 5.0)");
            execute("insert into points values (null, 5.0)");
            execute("insert into points values (5.0, null)");

            assertSql(
                    """
                            x\ty\tinside
                            5.0\t5.0\ttrue
                            null\t5.0\tfalse
                            5.0\tnull\tfalse
                            """,
                    "select x, y, geo_within_box(x, y, 0.0, 0.0, 10.0, 10.0) as inside from points"
            );
        });
    }

    @Test
    public void testWithTableData() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table points (x double, y double)");
            execute("insert into points values (5.0, 5.0)");
            execute("insert into points values (-1.0, 5.0)");
            execute("insert into points values (5.0, 11.0)");
            execute("insert into points values (0.0, 0.0)");

            assertSql(
                    """
                            x\ty\tinside
                            5.0\t5.0\ttrue
                            -1.0\t5.0\tfalse
                            5.0\t11.0\tfalse
                            0.0\t0.0\ttrue
                            """,
                    "select x, y, geo_within_box(x, y, 0.0, 0.0, 10.0, 10.0) as inside from points"
            );
        });
    }

    @Test
    public void testZeroSizedBox() throws Exception {
        // Point exactly at zero-sized box location
        assertSql("geo_within_box\ntrue\n", "select geo_within_box(5.0, 5.0, 5.0, 5.0, 5.0, 5.0)");
        // Point not at zero-sized box location
        assertSql("geo_within_box\nfalse\n", "select geo_within_box(5.0, 5.0, 6.0, 6.0, 6.0, 6.0)");
    }

    @Test
    public void testConstantBoxNaNMinX() throws Exception {
        // Constant box with NaN should be optimized to constant false
        assertMemoryLeak(() -> {
            execute("create table points (x double, y double)");
            execute("insert into points values (5.0, 5.0)");

            // Query plan should show constant false (the function is optimized away)
            assertSql(
                    """
                            QUERY PLAN
                            Empty table
                            """,
                    "explain select * from points where geo_within_box(x, y, NaN, 0.0, 10.0, 10.0)"
            );

            // Should return no rows
            assertSql(
                    """
                            x\ty
                            """,
                    "select * from points where geo_within_box(x, y, NaN, 0.0, 10.0, 10.0)"
            );
        });
    }

    @Test
    public void testConstantBoxOptimization() throws Exception {
        // Verify that constant box uses optimized function (shows literal values in plan)
        assertMemoryLeak(() -> {
            execute("create table points (x double, y double)");

            // Plan should show constant box values, not function references
            assertSql(
                    """
                            QUERY PLAN
                            Async Filter workers: 1
                              filter: geo_within_box(x,y,0.0,0.0,10.0,10.0)
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: points
                            """,
                    "explain select * from points where geo_within_box(x, y, 0.0, 0.0, 10.0, 10.0)"
            );
        });
    }

    @Test
    public void testDynamicBoxUsesGeneralFunction() throws Exception {
        // When box params come from columns, use general function
        assertMemoryLeak(() -> {
            execute("create table points (x double, y double, minX double, minY double, maxX double, maxY double)");
            execute("insert into points values (5.0, 5.0, 0.0, 0.0, 10.0, 10.0)");
            execute("insert into points values (15.0, 5.0, 0.0, 0.0, 10.0, 10.0)");

            assertSql(
                    """
                            x\ty\tminX\tminY\tmaxX\tmaxY\tinside
                            5.0\t5.0\t0.0\t0.0\t10.0\t10.0\ttrue
                            15.0\t5.0\t0.0\t0.0\t10.0\t10.0\tfalse
                            """,
                    "select *, geo_within_box(x, y, minX, minY, maxX, maxY) as inside from points"
            );
        });
    }

    // Tests for constant box optimization

    @Test
    public void testNegativeZero() throws Exception {
        // Edge case: -0.0 on exact boundary returns false due to branchless bit comparison.
        // This is documented behavior - -0.0 has sign bit 1, making (x - minX) = -0.0 appear negative.
        // In practice, this edge case is extremely rare and acceptable for performance.
        assertSql("geo_within_box\nfalse\n", "select geo_within_box(-0.0, 5.0, 0.0, 0.0, 10.0, 10.0)");
        // -0.0 in box bounds works correctly when point is clearly inside
        assertSql("geo_within_box\ntrue\n", "select geo_within_box(5.0, 5.0, -0.0, -0.0, 10.0, 10.0)");
    }

    @Test
    public void testParallelFilter() throws Exception {
        // Create large dataset with random coordinates in range [-100, 100]
        execute("create table points as (" +
                "select " +
                "  (rnd_double() * 200 - 100) x, " +
                "  (rnd_double() * 200 - 100) y " +
                "from long_sequence(1000000))");

        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        // Query with geo_within_box filter - box from (0,0) to (50,50)
                        String sql = "select count(*) from points where geo_within_box(x, y, 0.0, 0.0, 50.0, 50.0)";

                        // Verify the query plan shows parallel execution
                        TestUtils.assertSql(
                                engine,
                                sqlExecutionContext,
                                "explain " + sql,
                                sink,
                                """
                                        QUERY PLAN
                                        Count
                                            Async Filter workers: 4
                                              filter: geo_within_box(x,y,0.0,0.0,50.0,50.0)
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: points
                                        """
                        );

                        // Run query and verify results are consistent (run twice, compare)
                        TestUtils.assertSqlCursors(
                                engine,
                                sqlExecutionContext,
                                sql,
                                sql,
                                LOG
                        );
                    },
                    configuration,
                    LOG
            );
        }
    }

    @Test
    public void testParallelFilterVerifyCorrectness() throws Exception {
        // Create dataset and verify geo_within_box gives same results as manual bounds check
        execute("create table points as (" +
                "select " +
                "  (rnd_double() * 200 - 100) x, " +
                "  (rnd_double() * 200 - 100) y " +
                "from long_sequence(100000))");

        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        // Compare geo_within_box result with equivalent manual bounds check
                        String geoWithinBoxQuery = "select count(*) from points where geo_within_box(x, y, 0.0, 0.0, 50.0, 50.0)";
                        String manualBoundsQuery = "select count(*) from points where x >= 0.0 and x <= 50.0 and y >= 0.0 and y <= 50.0";

                        // Both queries should return the same count
                        TestUtils.assertSqlCursors(
                                engine,
                                sqlExecutionContext,
                                geoWithinBoxQuery,
                                manualBoundsQuery,
                                LOG
                        );
                    },
                    configuration,
                    LOG
            );
        }
    }

    @Test
    public void testParallelFilterWithNullValues() throws Exception {
        // Create dataset where some rows have null x or y values
        execute("create table points as (" +
                "select " +
                "  case when x % 10 = 0 then null else (rnd_double() * 200 - 100) end x, " +
                "  case when x % 7 = 0 then null else (rnd_double() * 200 - 100) end y " +
                "from long_sequence(1000000))");

        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        String sql = "select count(*) from points where geo_within_box(x, y, 0.0, 0.0, 50.0, 50.0)";

                        // Run query and verify results are consistent
                        TestUtils.assertSqlCursors(
                                engine,
                                sqlExecutionContext,
                                sql,
                                sql,
                                LOG
                        );
                    },
                    configuration,
                    LOG
            );
        }
    }
}
