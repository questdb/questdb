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

package io.questdb.test.griffin.engine.functions.geo;

import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class WithinRadiusFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testConstantNegativeRadius() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table points (x double, y double)");
            execute("insert into points values (0.0, 0.0)");

            // Constant negative radius should be optimized to constant false
            assertSql(
                    """
                            QUERY PLAN
                            Empty table
                            """,
                    "explain select * from points where within_radius(x, y, 0.0, 0.0, -5.0)"
            );
        });
    }

    @Test
    public void testConstantRadiusNaN() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table points (x double, y double)");
            execute("insert into points values (0.0, 0.0)");

            // Constant NaN radius should be optimized to constant false
            assertSql(
                    """
                            QUERY PLAN
                            Empty table
                            """,
                    "explain select * from points where within_radius(x, y, 0.0, 0.0, NaN)"
            );
        });
    }

    @Test
    public void testConstantRadiusOptimization() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table points (x double, y double)");

            // Plan should show constant center and radius values
            assertSql(
                    """
                            QUERY PLAN
                            Async Filter workers: 1
                              filter: within_radius(x,y,0.0,0.0,10.0)
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: points
                            """,
                    "explain select * from points where within_radius(x, y, 0.0, 0.0, 10.0)"
            );
        });
    }

    @Test
    public void testDynamicCenter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table points (x double, y double, cx double, cy double)");
            execute("insert into points values (5.0, 5.0, 0.0, 0.0)");   // distance ~7.07, inside radius 10
            execute("insert into points values (5.0, 5.0, 4.0, 4.0)");   // distance ~1.41, inside radius 10
            execute("insert into points values (5.0, 5.0, 20.0, 20.0)"); // distance ~21.2, outside radius 10

            assertSql(
                    """
                            x\ty\tcx\tcy\tinside
                            5.0\t5.0\t0.0\t0.0\ttrue
                            5.0\t5.0\t4.0\t4.0\ttrue
                            5.0\t5.0\t20.0\t20.0\tfalse
                            """,
                    "select x, y, cx, cy, within_radius(x, y, cx, cy, 10.0) as inside from points"
            );
        });
    }

    @Test
    public void testDynamicRadius() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table points (x double, y double, radius double)");
            execute("insert into points values (3.0, 4.0, 5.0)");   // distance 5, radius 5 -> on boundary
            execute("insert into points values (3.0, 4.0, 4.0)");   // distance 5, radius 4 -> outside
            execute("insert into points values (3.0, 4.0, 10.0)");  // distance 5, radius 10 -> inside

            assertSql(
                    """
                            x\ty\tradius\tinside
                            3.0\t4.0\t5.0\ttrue
                            3.0\t4.0\t4.0\tfalse
                            3.0\t4.0\t10.0\ttrue
                            """,
                    "select x, y, radius, within_radius(x, y, 0.0, 0.0, radius) as inside from points"
            );
        });
    }

    @Test
    public void testInWhereClause() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table points (x double, y double)");
            execute("insert into points values (3.0, 4.0)");   // distance 5, inside
            execute("insert into points values (10.0, 10.0)"); // distance ~14.14, outside
            execute("insert into points values (0.0, 0.0)");   // at center, inside
            execute("insert into points values (6.0, 8.0)");   // distance 10, on boundary

            assertSql(
                    """
                            x\ty
                            3.0\t4.0
                            0.0\t0.0
                            6.0\t8.0
                            """,
                    "select x, y from points where within_radius(x, y, 0.0, 0.0, 10.0)"
            );
        });
    }

    @Test
    public void testLargeRadius() throws Exception {
        assertSql("within_radius\ntrue\n", "select within_radius(1000000.0, 1000000.0, 0.0, 0.0, 2000000.0)");
    }

    @Test
    public void testNaNCenterX() throws Exception {
        assertSql("within_radius\nfalse\n", "select within_radius(0.0, 0.0, NaN, 0.0, 10.0)");
    }

    @Test
    public void testNaNCenterY() throws Exception {
        assertSql("within_radius\nfalse\n", "select within_radius(0.0, 0.0, 0.0, NaN, 10.0)");
    }

    @Test
    public void testNaNRadius() throws Exception {
        assertSql("within_radius\nfalse\n", "select within_radius(0.0, 0.0, 0.0, 0.0, NaN)");
    }

    @Test
    public void testNaNX() throws Exception {
        assertSql("within_radius\nfalse\n", "select within_radius(NaN, 0.0, 0.0, 0.0, 10.0)");
    }

    @Test
    public void testNaNY() throws Exception {
        assertSql("within_radius\nfalse\n", "select within_radius(0.0, NaN, 0.0, 0.0, 10.0)");
    }

    @Test
    public void testNegativeCoordinates() throws Exception {
        // Point (-3, -4) is distance 5 from origin
        assertSql("within_radius\ntrue\n", "select within_radius(-3.0, -4.0, 0.0, 0.0, 5.0)");
        assertSql("within_radius\nfalse\n", "select within_radius(-3.0, -4.0, 0.0, 0.0, 4.0)");
    }

    @Test
    public void testNegativeRadius() throws Exception {
        // Negative radius should return false
        assertSql("within_radius\nfalse\n", "select within_radius(0.0, 0.0, 0.0, 0.0, -5.0)");
    }

    @Test
    public void testNonOriginCenter() throws Exception {
        // Point (13, 14) is distance 5 from center (10, 10)
        assertSql("within_radius\ntrue\n", "select within_radius(13.0, 14.0, 10.0, 10.0, 5.0)");
        assertSql("within_radius\nfalse\n", "select within_radius(13.0, 14.0, 10.0, 10.0, 4.0)");
    }

    @Test
    public void testParallelFilter() throws Exception {
        execute("create table points as (" +
                "select " +
                "  (rnd_double() * 200 - 100) x, " +
                "  (rnd_double() * 200 - 100) y " +
                "from long_sequence(1000000))");

        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        String sql = "select count(*) from points where within_radius(x, y, 0.0, 0.0, 50.0)";

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
                                              filter: within_radius(x,y,0.0,0.0,50.0)
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: points
                                        """
                        );

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
    public void testParallelFilterVerifyCorrectness() throws Exception {
        execute("create table points as (" +
                "select " +
                "  (rnd_double() * 200 - 100) x, " +
                "  (rnd_double() * 200 - 100) y " +
                "from long_sequence(100000))");

        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        // Compare within_radius result with equivalent manual distance check
                        // radius 50, so radius^2 = 2500
                        String geoWithinRadiusQuery = "select count(*) from points where within_radius(x, y, 0.0, 0.0, 50.0)";
                        String manualDistanceQuery = "select count(*) from points where (x * x + y * y) <= 2500.0";

                        // Both queries should return the same count
                        TestUtils.assertSqlCursors(
                                engine,
                                sqlExecutionContext,
                                geoWithinRadiusQuery,
                                manualDistanceQuery,
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
        execute("create table points as (" +
                "select " +
                "  case when x % 10 = 0 then null else (rnd_double() * 200 - 100) end x, " +
                "  case when x % 7 = 0 then null else (rnd_double() * 200 - 100) end y " +
                "from long_sequence(1000000))");

        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        String sql = "select count(*) from points where within_radius(x, y, 0.0, 0.0, 50.0)";

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
    public void testPointAtCenter() throws Exception {
        assertSql("within_radius\ntrue\n", "select within_radius(5.0, 5.0, 5.0, 5.0, 10.0)");
    }

    @Test
    public void testPointExactlyOnBoundary() throws Exception {
        // Point (3, 4) is exactly distance 5 from origin
        assertSql("within_radius\ntrue\n", "select within_radius(3.0, 4.0, 0.0, 0.0, 5.0)");
    }

    // Tests for constant radius optimization

    @Test
    public void testPointInsideRadius() throws Exception {
        // Point (3, 4) is distance 5 from origin, radius is 10
        assertSql("within_radius\ntrue\n", "select within_radius(3.0, 4.0, 0.0, 0.0, 10.0)");
    }

    @Test
    public void testPointOutsideRadius() throws Exception {
        // Point (10, 10) is distance ~14.14 from origin, radius is 10
        assertSql("within_radius\nfalse\n", "select within_radius(10.0, 10.0, 0.0, 0.0, 10.0)");
    }

    @Test
    public void testVerySmallRadius() throws Exception {
        assertSql("within_radius\ntrue\n", "select within_radius(0.0, 0.0, 0.0, 0.0, 0.0000001)");
        assertSql("within_radius\nfalse\n", "select within_radius(0.0001, 0.0, 0.0, 0.0, 0.0000001)");
    }

    @Test
    public void testWithNullValuesInTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table points (x double, y double)");
            execute("insert into points values (3.0, 4.0)");
            execute("insert into points values (null, 4.0)");
            execute("insert into points values (3.0, null)");

            assertSql(
                    """
                            x\ty\tinside
                            3.0\t4.0\ttrue
                            null\t4.0\tfalse
                            3.0\tnull\tfalse
                            """,
                    "select x, y, within_radius(x, y, 0.0, 0.0, 10.0) as inside from points"
            );
        });
    }

    @Test
    public void testWithTableData() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table points (x double, y double)");
            execute("insert into points values (3.0, 4.0)");   // distance 5 from origin
            execute("insert into points values (6.0, 8.0)");   // distance 10 from origin
            execute("insert into points values (10.0, 10.0)"); // distance ~14.14 from origin
            execute("insert into points values (0.0, 0.0)");   // at origin

            assertSql(
                    """
                            x\ty\tinside
                            3.0\t4.0\ttrue
                            6.0\t8.0\ttrue
                            10.0\t10.0\tfalse
                            0.0\t0.0\ttrue
                            """,
                    "select x, y, within_radius(x, y, 0.0, 0.0, 10.0) as inside from points"
            );
        });
    }

    @Test
    public void testZeroRadius() throws Exception {
        // Only the center point should be inside
        assertSql("within_radius\ntrue\n", "select within_radius(5.0, 5.0, 5.0, 5.0, 0.0)");
        assertSql("within_radius\nfalse\n", "select within_radius(5.1, 5.0, 5.0, 5.0, 0.0)");
    }

    @Test
    public void testJoinWithSensorsTable() throws Exception {
        assertMemoryLeak(() -> {
            // Create sensors table with detection zones (center + radius)
            execute("create table sensors (sensor_id symbol, center_x double, center_y double, range double)");
            execute("insert into sensors values ('s1', 0.0, 0.0, 10.0)");
            execute("insert into sensors values ('s2', 20.0, 20.0, 5.0)");
            execute("insert into sensors values ('s3', 50.0, 50.0, 15.0)");

            // Create detections table with points
            execute("create table detections (detection_id symbol, x double, y double)");
            execute("insert into detections values ('d1', 3.0, 4.0)");    // distance 5 from s1, in range
            execute("insert into detections values ('d2', 22.0, 22.0)");  // distance ~2.83 from s2, in range
            execute("insert into detections values ('d3', 100.0, 100.0)");// far from all sensors
            execute("insert into detections values ('d4', 10.0, 0.0)");   // exactly on s1 boundary

            // Join detections with sensors to find which sensor detected each point
            assertSql(
                    """
                            detection_id\tsensor_id
                            d1\ts1
                            d2\ts2
                            d4\ts1
                            """,
                    "select d.detection_id, s.sensor_id " +
                            "from detections d " +
                            "join sensors s on within_radius(d.x, d.y, s.center_x, s.center_y, s.range) " +
                            "order by d.detection_id, s.sensor_id"
            );
        });
    }

    @Test
    public void testCrossJoinWithOverlappingSensors() throws Exception {
        assertMemoryLeak(() -> {
            // Create overlapping sensor zones
            execute("create table sensors (sensor_id symbol, center_x double, center_y double, range double)");
            execute("insert into sensors values ('s1', 0.0, 0.0, 15.0)");
            execute("insert into sensors values ('s2', 10.0, 0.0, 15.0)");  // overlaps with s1

            // Point in overlap region
            execute("create table detections (detection_id symbol, x double, y double)");
            execute("insert into detections values ('d1', 5.0, 0.0)");  // distance 5 from both

            // Should match both sensors
            assertSql(
                    """
                            detection_id\tsensor_id
                            d1\ts1
                            d1\ts2
                            """,
                    "select d.detection_id, s.sensor_id " +
                            "from detections d, sensors s " +
                            "where within_radius(d.x, d.y, s.center_x, s.center_y, s.range) " +
                            "order by d.detection_id, s.sensor_id"
            );
        });
    }
}
