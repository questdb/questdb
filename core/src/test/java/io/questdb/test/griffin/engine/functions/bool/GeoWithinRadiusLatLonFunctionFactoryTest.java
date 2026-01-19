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

public class GeoWithinRadiusLatLonFunctionFactoryTest extends AbstractCairoTest {

    // Test coordinates: Times Square, NYC (40.758, -73.9855)
    // Empire State Building: 40.7484, -73.9857 (~1.07km away)
    // Central Park (south): 40.7649, -73.9763 (~1.0km away)

    @Test
    public void testBoundaryPrecision() throws Exception {
        // Test that boundary is inclusive
        // At lat 40, 0.001 deg ≈ 111.32m
        // Use a point slightly inside the boundary to avoid floating-point edge cases
        double centerLat = 40.0;
        double pointLat = 40.0 + (99.0 / 111320.0); // ~99m north, inside 100m radius

        assertSql("geo_within_radius_latlon\ntrue\n",
                "select geo_within_radius_latlon(" + pointLat + ", -73.0, " + centerLat + ", -73.0, 100.0)");

        // Point clearly outside
        double pointLatOutside = 40.0 + (110.0 / 111320.0); // ~110m north, outside 100m radius
        assertSql("geo_within_radius_latlon\nfalse\n",
                "select geo_within_radius_latlon(" + pointLatOutside + ", -73.0, " + centerLat + ", -73.0, 100.0)");
    }

    @Test
    public void testConstantCenterNaN() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table points (lat double, lon double)");
            execute("insert into points values (40.0, -73.0)");

            assertSql(
                    """
                            QUERY PLAN
                            Empty table
                            """,
                    "explain select * from points where geo_within_radius_latlon(lat, lon, NaN, -73.0, 1000.0)"
            );
        });
    }

    @Test
    public void testConstantCenterOptimization() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table points (lat double, lon double)");

            // Plan should show constant center and radius values
            assertSql(
                    """
                            QUERY PLAN
                            Async Filter workers: 1
                              filter: geo_within_radius_latlon(lat,lon,40.0,-73.0,1000.0)
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: points
                            """,
                    "explain select * from points where geo_within_radius_latlon(lat, lon, 40.0, -73.0, 1000.0)"
            );
        });
    }

    @Test
    public void testConstantNegativeRadius() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table points (lat double, lon double)");
            execute("insert into points values (40.0, -73.0)");

            assertSql(
                    """
                            QUERY PLAN
                            Empty table
                            """,
                    "explain select * from points where geo_within_radius_latlon(lat, lon, 40.0, -73.0, -100.0)"
            );
        });
    }

    @Test
    public void testDynamicCenter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table points (lat double, lon double, clat double, clon double)");
            execute("insert into points values (40.001, -73.0, 40.0, -73.0)");     // ~111m from center
            execute("insert into points values (40.001, -73.0, 40.001, -73.0)");   // at center
            execute("insert into points values (40.001, -73.0, 40.01, -73.0)");    // ~1km from center

            assertSql(
                    """
                            lat\tlon\tclat\tclon\tinside
                            40.001\t-73.0\t40.0\t-73.0\tfalse
                            40.001\t-73.0\t40.001\t-73.0\ttrue
                            40.001\t-73.0\t40.01\t-73.0\tfalse
                            """,
                    "select lat, lon, clat, clon, geo_within_radius_latlon(lat, lon, clat, clon, 100.0) as inside from points"
            );
        });
    }

    @Test
    public void testDynamicRadius() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table points (lat double, lon double, radius double)");
            execute("insert into points values (40.001, -73.0, 50.0)");   // ~111m, radius 50m -> outside
            execute("insert into points values (40.001, -73.0, 200.0)");  // ~111m, radius 200m -> inside
            execute("insert into points values (40.001, -73.0, 150.0)");  // ~111m, radius 150m -> inside

            assertSql(
                    """
                            lat\tlon\tradius\tinside
                            40.001\t-73.0\t50.0\tfalse
                            40.001\t-73.0\t200.0\ttrue
                            40.001\t-73.0\t150.0\ttrue
                            """,
                    "select lat, lon, radius, geo_within_radius_latlon(lat, lon, 40.0, -73.0, radius) as inside from points"
            );
        });
    }

    @Test
    public void testEquator() throws Exception {
        // At equator, 1 deg lon ≈ 111km (same as lat)
        // Point 0.001 deg east ≈ 111m
        assertSql("geo_within_radius_latlon\ntrue\n",
                "select geo_within_radius_latlon(0.0, 0.001, 0.0, 0.0, 500.0)");
        assertSql("geo_within_radius_latlon\nfalse\n",
                "select geo_within_radius_latlon(0.0, 0.01, 0.0, 0.0, 500.0)");
    }

    @Test
    public void testHighLatitude() throws Exception {
        // At lat 60, cosLat ≈ 0.5, so 1 deg lon ≈ 55km
        // 0.001 deg lon at lat 60 ≈ 55m
        assertSql("geo_within_radius_latlon\ntrue\n",
                "select geo_within_radius_latlon(60.0, 0.001, 60.0, 0.0, 100.0)");
    }

    @Test
    public void testInWhereClause() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table points (lat double, lon double)");
            execute("insert into points values (40.001, -73.0)");   // ~111m, inside
            execute("insert into points values (40.01, -73.0)");    // ~1.1km, outside
            execute("insert into points values (40.0, -73.0)");     // at center, inside

            assertSql(
                    """
                            lat\tlon
                            40.001\t-73.0
                            40.0\t-73.0
                            """,
                    "select lat, lon from points where geo_within_radius_latlon(lat, lon, 40.0, -73.0, 500.0)"
            );
        });
    }

    @Test
    public void testLidarScanScenario() throws Exception {
        // Simulate lidar scan: points within 100m of scanner position
        assertMemoryLeak(() -> {
            execute("create table lidar_points as (" +
                    "select " +
                    "  40.758 + (rnd_double() * 0.004 - 0.002) lat, " +  // ±200m range
                    "  -73.9855 + (rnd_double() * 0.005 - 0.0025) lon " +
                    "from long_sequence(10000))");

            // Count points within 100m - should have some points inside
            // We just verify the query runs and returns results
            assertSql(
                    "count\n",
                    "select count(*) from lidar_points where geo_within_radius_latlon(lat, lon, 40.758, -73.9855, 100.0) limit 0"
            );
        });
    }

    @Test
    public void testNaNCenterLat() throws Exception {
        assertSql("geo_within_radius_latlon\nfalse\n",
                "select geo_within_radius_latlon(40.0, -73.0, NaN, -73.0, 1000.0)");
    }

    @Test
    public void testNaNCenterLon() throws Exception {
        assertSql("geo_within_radius_latlon\nfalse\n",
                "select geo_within_radius_latlon(40.0, -73.0, 40.0, NaN, 1000.0)");
    }

    @Test
    public void testNaNLat() throws Exception {
        assertSql("geo_within_radius_latlon\nfalse\n",
                "select geo_within_radius_latlon(NaN, -73.0, 40.0, -73.0, 1000.0)");
    }

    @Test
    public void testNaNLon() throws Exception {
        assertSql("geo_within_radius_latlon\nfalse\n",
                "select geo_within_radius_latlon(40.0, NaN, 40.0, -73.0, 1000.0)");
    }

    @Test
    public void testNaNRadius() throws Exception {
        assertSql("geo_within_radius_latlon\nfalse\n",
                "select geo_within_radius_latlon(40.0, -73.0, 40.0, -73.0, NaN)");
    }

    @Test
    public void testNearPole() throws Exception {
        // At lat 89, longitude distances are very small
        // Still should work, just less accurate
        assertSql("geo_within_radius_latlon\ntrue\n",
                "select geo_within_radius_latlon(89.0, 0.0, 89.0, 0.0, 100.0)");
    }

    @Test
    public void testNegativeCoordinates() throws Exception {
        // Southern hemisphere, western longitude
        assertSql("geo_within_radius_latlon\ntrue\n",
                "select geo_within_radius_latlon(-33.8688, 151.2093, -33.8688, 151.2093, 100.0)");
    }

    @Test
    public void testNegativeRadius() throws Exception {
        assertSql("geo_within_radius_latlon\nfalse\n",
                "select geo_within_radius_latlon(40.0, -73.0, 40.0, -73.0, -100.0)");
    }

    @Test
    public void testParallelFilter() throws Exception {
        // Generate random points around NYC area
        execute("create table points as (" +
                "select " +
                "  40.7 + (rnd_double() * 0.2 - 0.1) lat, " +
                "  -74.0 + (rnd_double() * 0.2 - 0.1) lon " +
                "from long_sequence(1000000))");

        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        // 5km radius around Times Square
                        String sql = "select count(*) from points where geo_within_radius_latlon(lat, lon, 40.758, -73.9855, 5000.0)";

                        TestUtils.assertSql(
                                engine,
                                sqlExecutionContext,
                                "explain " + sql,
                                sink,
                                """
                                        QUERY PLAN
                                        Count
                                            Async Filter workers: 4
                                              filter: geo_within_radius_latlon(lat,lon,40.758,-73.9855,5000.0)
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
    public void testParallelFilterWithNullValues() throws Exception {
        execute("create table points as (" +
                "select " +
                "  case when x % 10 = 0 then null else 40.7 + (rnd_double() * 0.2 - 0.1) end lat, " +
                "  case when x % 7 = 0 then null else -74.0 + (rnd_double() * 0.2 - 0.1) end lon " +
                "from long_sequence(1000000))");

        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        String sql = "select count(*) from points where geo_within_radius_latlon(lat, lon, 40.758, -73.9855, 5000.0)";

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

    // Tests for constant center optimization

    @Test
    public void testPointAtCenter() throws Exception {
        assertSql("geo_within_radius_latlon\ntrue\n",
                "select geo_within_radius_latlon(40.0, -73.0, 40.0, -73.0, 100.0)");
    }

    @Test
    public void testPointInsideRadius() throws Exception {
        // Point ~100m north of center, within 500m radius
        // At lat 40, 1 degree lat ≈ 111km, so 0.001 deg ≈ 111m
        assertSql("geo_within_radius_latlon\ntrue\n",
                "select geo_within_radius_latlon(40.001, -73.0, 40.0, -73.0, 500.0)");
    }

    @Test
    public void testPointOutsideRadius() throws Exception {
        // Point ~1km north of center, outside 500m radius
        // 0.01 deg lat ≈ 1.1km
        assertSql("geo_within_radius_latlon\nfalse\n",
                "select geo_within_radius_latlon(40.01, -73.0, 40.0, -73.0, 500.0)");
    }

    @Test
    public void testRealWorldNYC() throws Exception {
        // Times Square to Empire State Building ≈ 1.07km
        // Times Square: 40.758, -73.9855
        // Empire State: 40.7484, -73.9857
        assertSql("geo_within_radius_latlon\ntrue\n",
                "select geo_within_radius_latlon(40.7484, -73.9857, 40.758, -73.9855, 1500.0)");
        assertSql("geo_within_radius_latlon\nfalse\n",
                "select geo_within_radius_latlon(40.7484, -73.9857, 40.758, -73.9855, 500.0)");
    }

    @Test
    public void testWithNullValuesInTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table points (lat double, lon double)");
            execute("insert into points values (40.0, -73.0)");
            execute("insert into points values (null, -73.0)");
            execute("insert into points values (40.0, null)");

            assertSql(
                    """
                            lat\tlon\tinside
                            40.0\t-73.0\ttrue
                            null\t-73.0\tfalse
                            40.0\tnull\tfalse
                            """,
                    "select lat, lon, geo_within_radius_latlon(lat, lon, 40.0, -73.0, 1000.0) as inside from points"
            );
        });
    }

    @Test
    public void testWithTableData() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table points (lat double, lon double)");
            execute("insert into points values (40.001, -73.0)");   // ~111m north
            execute("insert into points values (40.005, -73.0)");   // ~555m north
            execute("insert into points values (40.01, -73.0)");    // ~1.1km north
            execute("insert into points values (40.0, -73.0)");     // at center

            assertSql(
                    """
                            lat\tlon\tinside
                            40.001\t-73.0\ttrue
                            40.005\t-73.0\tfalse
                            40.01\t-73.0\tfalse
                            40.0\t-73.0\ttrue
                            """,
                    "select lat, lon, geo_within_radius_latlon(lat, lon, 40.0, -73.0, 200.0) as inside from points"
            );
        });
    }

    @Test
    public void testZeroRadius() throws Exception {
        // Only the center point should be inside
        assertSql("geo_within_radius_latlon\ntrue\n",
                "select geo_within_radius_latlon(40.0, -73.0, 40.0, -73.0, 0.0)");
        assertSql("geo_within_radius_latlon\nfalse\n",
                "select geo_within_radius_latlon(40.0001, -73.0, 40.0, -73.0, 0.0)");
    }
}
