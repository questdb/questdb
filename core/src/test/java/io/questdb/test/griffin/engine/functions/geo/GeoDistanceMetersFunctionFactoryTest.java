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

public class GeoDistanceMetersFunctionFactoryTest extends AbstractCairoTest {

    // Reference: 1 degree of latitude ≈ 111,320 meters
    // At equator, 1 degree of longitude ≈ 111,320 meters
    // At lat 40, 1 degree of longitude ≈ 85,280 meters (111320 * cos(40°))

    @Test
    public void testSamePoint() throws Exception {
        assertSql("geo_distance_meters\n0.0\n",
                "select geo_distance_meters(40.0, -73.0, 40.0, -73.0)");
    }

    @Test
    public void testLatitudeOnlyDifference() throws Exception {
        // 1 degree latitude difference = ~111,320 meters
        assertSql("geo_distance_meters\n111320.0\n",
                "select geo_distance_meters(40.0, -73.0, 41.0, -73.0)");
    }

    @Test
    public void testLongitudeOnlyDifferenceAtEquator() throws Exception {
        // At equator, 1 degree longitude = ~111,320 meters
        // Using midpoint latitude (0), so cosLat = 1
        assertSql("geo_distance_meters\n111320.0\n",
                "select geo_distance_meters(0.0, 0.0, 0.0, 1.0)");
    }

    @Test
    public void testNaNLat1() throws Exception {
        assertSql("geo_distance_meters\nnull\n",
                "select geo_distance_meters(NaN, -73.0, 40.0, -73.0)");
    }

    @Test
    public void testNaNLon1() throws Exception {
        assertSql("geo_distance_meters\nnull\n",
                "select geo_distance_meters(40.0, NaN, 40.0, -73.0)");
    }

    @Test
    public void testNaNLat2() throws Exception {
        assertSql("geo_distance_meters\nnull\n",
                "select geo_distance_meters(40.0, -73.0, NaN, -73.0)");
    }

    @Test
    public void testNaNLon2() throws Exception {
        assertSql("geo_distance_meters\nnull\n",
                "select geo_distance_meters(40.0, -73.0, 40.0, NaN)");
    }

    @Test
    public void testConstantOptimization() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table points (lat double, lon double)");

            // Plan should show constant lat1/lon1 values
            assertSql(
                    """
                            QUERY PLAN
                            VirtualRecord
                              functions: [lat,lon,geo_distance_meters(40.0,-73.0,lat,lon)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: points
                            """,
                    "explain select lat, lon, geo_distance_meters(40.0, -73.0, lat, lon) from points"
            );
        });
    }

    @Test
    public void testNegativeCoordinates() throws Exception {
        // Southern hemisphere
        assertSql("geo_distance_meters\n111320.0\n",
                "select geo_distance_meters(-40.0, 151.0, -41.0, 151.0)");
    }

    // Validation tests

    @Test
    public void testInvalidLat1Above90() throws Exception {
        assertException(
                "select geo_distance_meters(91.0, -73.0, 40.0, -73.0)",
                27,
                "latitude must be between -90 and 90"
        );
    }

    @Test
    public void testInvalidLat1Below90() throws Exception {
        assertException(
                "select geo_distance_meters(-91.0, -73.0, 40.0, -73.0)",
                27,
                "latitude must be between -90 and 90"
        );
    }

    @Test
    public void testInvalidLon1Above180() throws Exception {
        assertException(
                "select geo_distance_meters(40.0, 181.0, 40.0, -73.0)",
                33,
                "longitude must be between -180 and 180"
        );
    }

    @Test
    public void testInvalidLon1Below180() throws Exception {
        assertException(
                "select geo_distance_meters(40.0, -181.0, 40.0, -73.0)",
                33,
                "longitude must be between -180 and 180"
        );
    }

    @Test
    public void testInvalidLat2Above90() throws Exception {
        assertException(
                "select geo_distance_meters(40.0, -73.0, 91.0, -73.0)",
                40,
                "latitude must be between -90 and 90"
        );
    }

    @Test
    public void testInvalidLat2Below90() throws Exception {
        assertException(
                "select geo_distance_meters(40.0, -73.0, -91.0, -73.0)",
                40,
                "latitude must be between -90 and 90"
        );
    }

    @Test
    public void testInvalidLon2Above180() throws Exception {
        assertException(
                "select geo_distance_meters(40.0, -73.0, 40.0, 181.0)",
                46,
                "longitude must be between -180 and 180"
        );
    }

    @Test
    public void testInvalidLon2Below180() throws Exception {
        assertException(
                "select geo_distance_meters(40.0, -73.0, 40.0, -181.0)",
                46,
                "longitude must be between -180 and 180"
        );
    }

    @Test
    public void testInvalidLatInTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table points (lat double, lon double)");
            execute("insert into points values (91.0, -73.0)");

            assertException(
                    "select geo_distance_meters(40.0, -73.0, lat, lon) from points",
                    40,
                    "latitude must be between -90 and 90"
            );
        });
    }

    @Test
    public void testInvalidLonInTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table points (lat double, lon double)");
            execute("insert into points values (40.0, 181.0)");

            assertException(
                    "select geo_distance_meters(40.0, -73.0, lat, lon) from points",
                    45,
                    "longitude must be between -180 and 180"
            );
        });
    }

    @Test
    public void testBoundaryLatitudes() throws Exception {
        // Exactly at +90 and -90 should be valid
        // 180 degrees of latitude = 180 * 111320 = 20,037,600 meters
        assertSql("geo_distance_meters\n2.00376E7\n",
                "select geo_distance_meters(90.0, 0.0, -90.0, 0.0)");
    }

    @Test
    public void testBoundaryLongitudes() throws Exception {
        // Exactly at +180 and -180 should be valid
        // 360 degrees of longitude at equator = 360 * 111320 = 40,075,200 meters
        assertSql("geo_distance_meters\n4.00752E7\n",
                "select geo_distance_meters(0.0, 180.0, 0.0, -180.0)");
    }

    @Test
    public void testInWhereClause() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table points (lat double, lon double)");
            execute("insert into points values (40.001, -73.0)");   // ~111m
            execute("insert into points values (40.005, -73.0)");   // ~555m
            execute("insert into points values (40.01, -73.0)");    // ~1113m

            assertSql(
                    """
                            lat\tlon
                            40.001\t-73.0
                            40.005\t-73.0
                            """,
                    "select lat, lon from points where geo_distance_meters(40.0, -73.0, lat, lon) < 1000.0"
            );
        });
    }

    @Test
    public void testOrderByDistance() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table points (name symbol, lat double, lon double)");
            execute("insert into points values ('far', 40.01, -73.0)");
            execute("insert into points values ('near', 40.001, -73.0)");
            execute("insert into points values ('mid', 40.005, -73.0)");

            assertSql(
                    """
                            name\tlat\tlon
                            near\t40.001\t-73.0
                            mid\t40.005\t-73.0
                            far\t40.01\t-73.0
                            """,
                    "select name, lat, lon from points order by geo_distance_meters(40.0, -73.0, lat, lon)"
            );
        });
    }

    @Test
    public void testParallelFilter() throws Exception {
        execute("create table points as (" +
                "select " +
                "  (rnd_double() * 0.1 + 40.0) lat, " +  // lat between 40.0 and 40.1
                "  (rnd_double() * 0.1 - 73.05) lon " +  // lon between -73.05 and -72.95
                "from long_sequence(1000000))");

        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        // Filter points within ~5km of reference point (40.0, -73.0)
                        String sql = "select count(*) from points where geo_distance_meters(40.0, -73.0, lat, lon) < 5000.0";

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
                                              filter: geo_distance_meters(40.0,-73.0,lat,lon)<5000.0
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
                "  case when x % 10 = 0 then null else (rnd_double() * 0.1 + 40.0) end lat, " +
                "  case when x % 7 = 0 then null else (rnd_double() * 0.1 - 73.05) end lon " +
                "from long_sequence(1000000))");

        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        String sql = "select count(*) from points where geo_distance_meters(40.0, -73.0, lat, lon) < 5000.0";

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
    public void testOrderByDistanceLargeDataset() throws Exception {
        execute("create table points as (" +
                "select " +
                "  x id, " +
                "  (rnd_double() * 0.02 + 40.0) lat, " +   // lat between 40.0 and 40.02
                "  (rnd_double() * 0.02 - 73.01) lon " +   // lon between -73.01 and -72.99
                "from long_sequence(10000))");

        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        // Order by distance and take top 10 closest points
                        String sql = "select id, lat, lon from points order by geo_distance_meters(40.0, -73.0, lat, lon) limit 10";

                        // Run query twice and verify results are consistent
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
    public void testJoinOnDistanceLessThanThreshold() throws Exception {
        assertMemoryLeak(() -> {
            // Create warehouses table
            execute("create table warehouses (warehouse_id symbol, lat double, lon double, max_delivery_km double)");
            execute("insert into warehouses values ('w1', 40.7580, -73.9855, 0.5)");   // Times Square, 500m
            execute("insert into warehouses values ('w2', 40.7484, -73.9857, 0.5)");   // Empire State, 500m (~1070m from w1)

            // Create orders table
            // Note: 0.001 degree lat ≈ 111m
            execute("create table orders (order_id symbol, lat double, lon double)");
            execute("insert into orders values ('o1', 40.7582, -73.9855)");  // ~22m from w1, ~1090m from w2
            execute("insert into orders values ('o2', 40.7486, -73.9857)");  // ~1048m from w1, ~22m from w2
            execute("insert into orders values ('o3', 40.7200, -73.9900)");  // far from both

            // Join orders with warehouses where distance < max_delivery (converted to meters)
            // o1 should only match w1, o2 should only match w2, o3 matches nothing
            assertSql(
                    """
                            order_id\twarehouse_id
                            o1\tw1
                            o2\tw2
                            """,
                    "select o.order_id, w.warehouse_id " +
                            "from orders o " +
                            "join warehouses w on geo_distance_meters(o.lat, o.lon, w.lat, w.lon) < w.max_delivery_km * 1000.0 " +
                            "order by o.order_id, w.warehouse_id"
            );
        });
    }

    @Test
    public void testJoinWithDistanceInSelect() throws Exception {
        assertMemoryLeak(() -> {
            // Create locations table
            execute("create table locations (name symbol, lat double, lon double)");
            execute("insert into locations values ('times_square', 40.7580, -73.9855)");
            execute("insert into locations values ('empire_state', 40.7484, -73.9857)");

            // Create points of interest
            execute("create table pois (poi_name symbol, lat double, lon double)");
            execute("insert into pois values ('central_park', 40.7829, -73.9654)");
            execute("insert into pois values ('bryant_park', 40.7536, -73.9832)");

            // Cross join to get distance matrix - verify distances are in expected ranges
            // Bryant Park is ~500-650m from both, Central Park is ~3000-4300m
            assertSql(
                    """
                            name\tpoi_name\tin_range
                            empire_state\tbryant_park\ttrue
                            empire_state\tcentral_park\ttrue
                            times_square\tbryant_park\ttrue
                            times_square\tcentral_park\ttrue
                            """,
                    "select l.name, p.poi_name, " +
                            "case " +
                            "  when p.poi_name = 'bryant_park' then geo_distance_meters(l.lat, l.lon, p.lat, p.lon) >= 500.0 and geo_distance_meters(l.lat, l.lon, p.lat, p.lon) <= 650.0 " +
                            "  else geo_distance_meters(l.lat, l.lon, p.lat, p.lon) >= 3000.0 and geo_distance_meters(l.lat, l.lon, p.lat, p.lon) <= 4300.0 " +
                            "end as in_range " +
                            "from locations l, pois p " +
                            "order by l.name, p.poi_name"
            );
        });
    }

    @Test
    public void testFindNearestWithJoin() throws Exception {
        assertMemoryLeak(() -> {
            // Create reference points
            execute("create table refs (ref_id symbol, lat double, lon double)");
            execute("insert into refs values ('r1', 40.7580, -73.9855)");

            // Create candidates
            execute("create table candidates (candidate_id symbol, lat double, lon double)");
            execute("insert into candidates values ('c1', 40.7590, -73.9855)");  // ~111m north
            execute("insert into candidates values ('c2', 40.7600, -73.9855)");  // ~222m north
            execute("insert into candidates values ('c3', 40.7570, -73.9855)");  // ~111m south

            // Find closest candidate to each reference point
            assertSql(
                    """
                            ref_id\tcandidate_id
                            r1\tc1
                            r1\tc3
                            """,
                    "select r.ref_id, c.candidate_id " +
                            "from refs r " +
                            "join candidates c on geo_distance_meters(r.lat, r.lon, c.lat, c.lon) < 150.0 " +
                            "order by r.ref_id, c.candidate_id"
            );
        });
    }
}
