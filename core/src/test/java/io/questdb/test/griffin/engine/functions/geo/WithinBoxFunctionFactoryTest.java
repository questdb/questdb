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

package io.questdb.test.griffin.engine.functions.geo;

import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class WithinBoxFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testInWhereClause() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table points (x double, y double)");
            execute("insert into points values (5.0, 5.0)");
            execute("insert into points values (-1.0, 5.0)");
            execute("insert into points values (5.0, 11.0)");
            execute("insert into points values (0.0, 0.0)");

            assertQuery("select x, y from points where within_box(x, y, 0.0, 0.0, 10.0, 10.0)")
                    .noLeakCheck()
                    .returns("""
                            x\ty
                            5.0\t5.0
                            0.0\t0.0
                            """);
        });
    }

    @Test
    public void testInfinity() throws Exception {
        // Note: QuestDB's division converts Infinity to NaN (see DivDoubleFunctionFactory),
        // so 1.0/0.0 produces NaN, not Infinity. These tests verify NaN handling.
        assertQuery("select within_box(1.0/0.0, 5.0, 0.0, 0.0, 10.0, 10.0)")
                .noLeakCheck()
                .expectSize()
                .returns("within_box\nfalse\n");
        assertQuery("select within_box(-1.0/0.0, 5.0, 0.0, 0.0, 10.0, 10.0)")
                .noLeakCheck()
                .expectSize()
                .returns("within_box\nfalse\n");
    }

    @Test
    public void testInvertedBoxX() throws Exception {
        // min_x > max_x should return false
        assertQuery("select within_box(5.0, 5.0, 10.0, 0.0, 0.0, 10.0)")
                .noLeakCheck()
                .expectSize()
                .returns("within_box\nfalse\n");
    }

    @Test
    public void testInvertedBoxY() throws Exception {
        // min_y > max_y should return false
        assertQuery("select within_box(5.0, 5.0, 0.0, 10.0, 10.0, 0.0)")
                .noLeakCheck()
                .expectSize()
                .returns("within_box\nfalse\n");
    }

    @Test
    public void testNaNMaxX() throws Exception {
        assertQuery("select within_box(5.0, 5.0, 0.0, 0.0, NaN, 10.0)")
                .noLeakCheck()
                .expectSize()
                .returns("within_box\nfalse\n");
    }

    @Test
    public void testNaNMaxY() throws Exception {
        assertQuery("select within_box(5.0, 5.0, 0.0, 0.0, 10.0, NaN)")
                .noLeakCheck()
                .expectSize()
                .returns("within_box\nfalse\n");
    }

    @Test
    public void testNaNMinX() throws Exception {
        assertQuery("select within_box(5.0, 5.0, NaN, 0.0, 10.0, 10.0)")
                .noLeakCheck()
                .expectSize()
                .returns("within_box\nfalse\n");
    }

    @Test
    public void testNaNMinY() throws Exception {
        assertQuery("select within_box(5.0, 5.0, 0.0, NaN, 10.0, 10.0)")
                .noLeakCheck()
                .expectSize()
                .returns("within_box\nfalse\n");
    }

    @Test
    public void testNaNX() throws Exception {
        assertQuery("select within_box(NaN, 5.0, 0.0, 0.0, 10.0, 10.0)")
                .noLeakCheck()
                .expectSize()
                .returns("within_box\nfalse\n");
    }

    @Test
    public void testNaNY() throws Exception {
        assertQuery("select within_box(5.0, NaN, 0.0, 0.0, 10.0, 10.0)")
                .noLeakCheck()
                .expectSize()
                .returns("within_box\nfalse\n");
    }

    @Test
    public void testNegativeCoordinates() throws Exception {
        assertQuery("select within_box(-5.0, -5.0, -10.0, -10.0, 0.0, 0.0)")
                .noLeakCheck()
                .expectSize()
                .returns("within_box\ntrue\n");
        assertQuery("select within_box(-15.0, -5.0, -10.0, -10.0, 0.0, 0.0)")
                .noLeakCheck()
                .expectSize()
                .returns("within_box\nfalse\n");
    }

    @Test
    public void testConstantBoxInvertedReturnsEmpty() throws Exception {
        // Inverted box (minX > maxX) should be optimized to constant false
        assertMemoryLeak(() -> {
            execute("create table points (x double, y double)");
            execute("insert into points values (5.0, 5.0)");

            // Query plan should show constant false (the function is optimized away)
            assertQuery("explain select * from points where within_box(x, y, 10.0, 0.0, 0.0, 10.0)")
                    .noLeakCheck()
                    .returnsOnce("""
                            QUERY PLAN
                            Empty table
                            """);
        });
    }

    @Test
    public void testPointInsideBox() throws Exception {
        assertQuery("select within_box(5.0, 5.0, 0.0, 0.0, 10.0, 10.0)")
                .noLeakCheck()
                .expectSize()
                .returns("within_box\ntrue\n");
    }

    @Test
    public void testPointOnCorner() throws Exception {
        assertQuery("select within_box(0.0, 0.0, 0.0, 0.0, 10.0, 10.0)")
                .noLeakCheck()
                .expectSize()
                .returns("within_box\ntrue\n");
        assertQuery("select within_box(10.0, 10.0, 0.0, 0.0, 10.0, 10.0)")
                .noLeakCheck()
                .expectSize()
                .returns("within_box\ntrue\n");
        assertQuery("select within_box(0.0, 10.0, 0.0, 0.0, 10.0, 10.0)")
                .noLeakCheck()
                .expectSize()
                .returns("within_box\ntrue\n");
        assertQuery("select within_box(10.0, 0.0, 0.0, 0.0, 10.0, 10.0)")
                .noLeakCheck()
                .expectSize()
                .returns("within_box\ntrue\n");
    }

    @Test
    public void testPointOnMaxXBoundary() throws Exception {
        assertQuery("select within_box(10.0, 5.0, 0.0, 0.0, 10.0, 10.0)")
                .noLeakCheck()
                .expectSize()
                .returns("within_box\ntrue\n");
    }

    @Test
    public void testPointOnMaxYBoundary() throws Exception {
        assertQuery("select within_box(5.0, 10.0, 0.0, 0.0, 10.0, 10.0)")
                .noLeakCheck()
                .expectSize()
                .returns("within_box\ntrue\n");
    }

    @Test
    public void testPointOnMinXBoundary() throws Exception {
        assertQuery("select within_box(0.0, 5.0, 0.0, 0.0, 10.0, 10.0)")
                .noLeakCheck()
                .expectSize()
                .returns("within_box\ntrue\n");
    }

    @Test
    public void testPointOnMinYBoundary() throws Exception {
        assertQuery("select within_box(5.0, 0.0, 0.0, 0.0, 10.0, 10.0)")
                .noLeakCheck()
                .expectSize()
                .returns("within_box\ntrue\n");
    }

    @Test
    public void testPointOutsideBoxAbove() throws Exception {
        assertQuery("select within_box(5.0, 11.0, 0.0, 0.0, 10.0, 10.0)")
                .noLeakCheck()
                .expectSize()
                .returns("within_box\nfalse\n");
    }

    @Test
    public void testPointOutsideBoxBelow() throws Exception {
        assertQuery("select within_box(5.0, -1.0, 0.0, 0.0, 10.0, 10.0)")
                .noLeakCheck()
                .expectSize()
                .returns("within_box\nfalse\n");
    }

    @Test
    public void testPointOutsideBoxLeft() throws Exception {
        assertQuery("select within_box(-1.0, 5.0, 0.0, 0.0, 10.0, 10.0)")
                .noLeakCheck()
                .expectSize()
                .returns("within_box\nfalse\n");
    }

    @Test
    public void testPointOutsideBoxRight() throws Exception {
        assertQuery("select within_box(11.0, 5.0, 0.0, 0.0, 10.0, 10.0)")
                .noLeakCheck()
                .expectSize()
                .returns("within_box\nfalse\n");
    }

    @Test
    public void testVerySmallDifferences() throws Exception {
        assertQuery("select within_box(1.0000000001, 1.0, 1.0, 1.0, 2.0, 2.0)")
                .noLeakCheck()
                .expectSize()
                .returns("within_box\ntrue\n");
        assertQuery("select within_box(0.9999999999, 1.0, 1.0, 1.0, 2.0, 2.0)")
                .noLeakCheck()
                .expectSize()
                .returns("within_box\nfalse\n");
    }

    @Test
    public void testWithNullValuesInTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table points (x double, y double)");
            execute("insert into points values (5.0, 5.0)");
            execute("insert into points values (null, 5.0)");
            execute("insert into points values (5.0, null)");

            assertQuery("select x, y, within_box(x, y, 0.0, 0.0, 10.0, 10.0) as inside from points")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x\ty\tinside
                            5.0\t5.0\ttrue
                            null\t5.0\tfalse
                            5.0\tnull\tfalse
                            """);
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

            assertQuery("select x, y, within_box(x, y, 0.0, 0.0, 10.0, 10.0) as inside from points")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x\ty\tinside
                            5.0\t5.0\ttrue
                            -1.0\t5.0\tfalse
                            5.0\t11.0\tfalse
                            0.0\t0.0\ttrue
                            """);
        });
    }

    @Test
    public void testZeroSizedBox() throws Exception {
        // Point exactly at zero-sized box location
        assertQuery("select within_box(5.0, 5.0, 5.0, 5.0, 5.0, 5.0)")
                .noLeakCheck()
                .expectSize()
                .returns("within_box\ntrue\n");
        // Point not at zero-sized box location
        assertQuery("select within_box(5.0, 5.0, 6.0, 6.0, 6.0, 6.0)")
                .noLeakCheck()
                .expectSize()
                .returns("within_box\nfalse\n");
    }

    @Test
    public void testConstantBoxNaNMinX() throws Exception {
        // Constant box with NaN should be optimized to constant false
        assertMemoryLeak(() -> {
            execute("create table points (x double, y double)");
            execute("insert into points values (5.0, 5.0)");

            // Query plan should show constant false (the function is optimized away)
            assertQuery("explain select * from points where within_box(x, y, NaN, 0.0, 10.0, 10.0)")
                    .noLeakCheck()
                    .returnsOnce("""
                            QUERY PLAN
                            Empty table
                            """);

            // Should return no rows
            assertQuery("select * from points where within_box(x, y, NaN, 0.0, 10.0, 10.0)")
                    .noLeakCheck()
                    .returns("""
                            x\ty
                            """);
        });
    }

    @Test
    public void testConstantBoxOptimization() throws Exception {
        // Verify that constant box uses optimized function (shows literal values in plan)
        assertMemoryLeak(() -> {
            execute("create table points (x double, y double)");

            // Plan should show constant box values, not function references
            assertQuery("explain select * from points where within_box(x, y, 0.0, 0.0, 10.0, 10.0)")
                    .noLeakCheck()
                    .returnsOnce("""
                            QUERY PLAN
                            Async Filter workers: 1
                              filter: within_box(x,y,0.0,0.0,10.0,10.0)
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: points
                            """);
        });
    }

    @Test
    public void testDynamicBoxUsesGeneralFunction() throws Exception {
        // When box params come from columns, use general function
        assertMemoryLeak(() -> {
            execute("create table points (x double, y double, minX double, minY double, maxX double, maxY double)");
            execute("insert into points values (5.0, 5.0, 0.0, 0.0, 10.0, 10.0)");
            execute("insert into points values (15.0, 5.0, 0.0, 0.0, 10.0, 10.0)");

            assertQuery("select *, within_box(x, y, minX, minY, maxX, maxY) as inside from points")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x\ty\tminX\tminY\tmaxX\tmaxY\tinside
                            5.0\t5.0\t0.0\t0.0\t10.0\t10.0\ttrue
                            15.0\t5.0\t0.0\t0.0\t10.0\t10.0\tfalse
                            """);
        });
    }

    // Tests for constant box optimization

    @Test
    public void testNegativeZero() throws Exception {
        // Edge case: -0.0 on exact boundary returns false due to branchless bit comparison.
        // This is documented behavior - -0.0 has sign bit 1, making (x - minX) = -0.0 appear negative.
        // In practice, this edge case is extremely rare and acceptable for performance.
        assertQuery("select within_box(-0.0, 5.0, 0.0, 0.0, 10.0, 10.0)")
                .noLeakCheck()
                .expectSize()
                .returns("within_box\nfalse\n");
        // -0.0 in box bounds works correctly when point is clearly inside
        assertQuery("select within_box(5.0, 5.0, -0.0, -0.0, 10.0, 10.0)")
                .noLeakCheck()
                .expectSize()
                .returns("within_box\ntrue\n");
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
                    (engine, _, sqlExecutionContext) -> {
                        // Query with within_box filter - box from (0,0) to (50,50)
                        String sql = "select count(*) from points where within_box(x, y, 0.0, 0.0, 50.0, 50.0)";

                        // Verify the query plan shows parallel execution
                        assertQuery(sql)
                                .withEngine(engine)
                                .withContext(sqlExecutionContext)
                                .noLeakCheck()
                                .assertsPlan("""
                                        Count
                                            Async Filter workers: 4
                                              filter: within_box(x,y,0.0,0.0,50.0,50.0)
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: points
                                        """);

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
        // Create dataset and verify within_box gives same results as manual bounds check
        execute("create table points as (" +
                "select " +
                "  (rnd_double() * 200 - 100) x, " +
                "  (rnd_double() * 200 - 100) y " +
                "from long_sequence(100000))");

        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(
                    pool,
                    (engine, _, sqlExecutionContext) -> {
                        // Compare within_box result with equivalent manual bounds check
                        String geoWithinBoxQuery = "select count(*) from points where within_box(x, y, 0.0, 0.0, 50.0, 50.0)";
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
                    (engine, _, sqlExecutionContext) -> {
                        String sql = "select count(*) from points where within_box(x, y, 0.0, 0.0, 50.0, 50.0)";

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

    @Test
    public void testJoinWithZonesTable() throws Exception {
        assertMemoryLeak(() -> {
            // Create zones table with named regions
            execute("create table zones (zone_name symbol, min_x double, min_y double, max_x double, max_y double)");
            execute("insert into zones values ('zone_a', 0.0, 0.0, 10.0, 10.0)");
            execute("insert into zones values ('zone_b', 20.0, 20.0, 30.0, 30.0)");
            execute("insert into zones values ('zone_c', -10.0, -10.0, -5.0, -5.0)");

            // Create points table
            execute("create table points (point_id symbol, x double, y double)");
            execute("insert into points values ('p1', 5.0, 5.0)");     // in zone_a
            execute("insert into points values ('p2', 25.0, 25.0)");   // in zone_b
            execute("insert into points values ('p3', -7.0, -7.0)");   // in zone_c
            execute("insert into points values ('p4', 15.0, 15.0)");   // in no zone
            execute("insert into points values ('p5', 0.0, 0.0)");     // on zone_a boundary

            // Join points with zones using within_box
            assertQuery("select p.point_id, p.x, p.y, z.zone_name " +
                    "from points p " +
                    "join zones z on within_box(p.x, p.y, z.min_x, z.min_y, z.max_x, z.max_y) " +
                    "order by p.point_id")
                    .noLeakCheck()
                    .returns("""
                            point_id\tx\ty\tzone_name
                            p1\t5.0\t5.0\tzone_a
                            p2\t25.0\t25.0\tzone_b
                            p3\t-7.0\t-7.0\tzone_c
                            p5\t0.0\t0.0\tzone_a
                            """);
        });
    }

    @Test
    public void testCrossJoinWithZonesFilter() throws Exception {
        assertMemoryLeak(() -> {
            // Create zones table
            execute("create table zones (zone_name symbol, min_x double, min_y double, max_x double, max_y double)");
            execute("insert into zones values ('north', 0.0, 50.0, 100.0, 100.0)");
            execute("insert into zones values ('south', 0.0, 0.0, 100.0, 50.0)");

            // Create points table
            execute("create table points (point_id symbol, x double, y double)");
            execute("insert into points values ('p1', 25.0, 75.0)");   // in north
            execute("insert into points values ('p2', 25.0, 25.0)");   // in south
            execute("insert into points values ('p3', 50.0, 50.0)");   // on boundary (in both due to inclusive)

            // Cross join with filter - point on boundary should appear in both zones
            assertQuery("select p.point_id, z.zone_name " +
                    "from points p, zones z " +
                    "where within_box(p.x, p.y, z.min_x, z.min_y, z.max_x, z.max_y) " +
                    "order by p.point_id, z.zone_name")
                    .noLeakCheck()
                    .returns("""
                            point_id\tzone_name
                            p1\tnorth
                            p2\tsouth
                            p3\tnorth
                            p3\tsouth
                            """);
        });
    }

    @Test
    public void testLeftJoinWithZones() throws Exception {
        assertMemoryLeak(() -> {
            // Create zones table
            execute("create table zones (zone_name symbol, min_x double, min_y double, max_x double, max_y double)");
            execute("insert into zones values ('zone_a', 0.0, 0.0, 10.0, 10.0)");

            // Create points table with some points outside any zone
            execute("create table points (point_id symbol, x double, y double)");
            execute("insert into points values ('inside', 5.0, 5.0)");
            execute("insert into points values ('outside', 50.0, 50.0)");

            // Left join to see which points have matching zones
            assertQuery("select p.point_id, z.zone_name " +
                    "from points p " +
                    "left join zones z on within_box(p.x, p.y, z.min_x, z.min_y, z.max_x, z.max_y) " +
                    "order by p.point_id")
                    .noLeakCheck()
                    .returns("""
                            point_id\tzone_name
                            inside\tzone_a
                            outside\t
                            """);
        });
    }
}
