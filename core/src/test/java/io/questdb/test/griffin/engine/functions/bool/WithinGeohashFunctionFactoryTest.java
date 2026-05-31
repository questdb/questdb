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

package io.questdb.test.griffin.engine.functions.bool;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class WithinGeohashFunctionFactoryTest extends AbstractCairoTest {

    public static String ddlTrips = """
            CREATE TABLE trips (
            pickup_datetime TIMESTAMP,
            pickup_geohash GEOHASH(12c)
            ) timestamp(pickup_datetime)
            """;

    public static String dmlTrips = """
            INSERT INTO trips (pickup_datetime, pickup_geohash) VALUES\s
                ('2009-01-01T00:00:00.000000Z', #dr5rsjutvshf),
                ('2009-01-01T00:00:00.000000Z', #dr5ruy5ttnw1),
                ('2009-01-01T00:00:02.000000Z', #dr5ruvkrr2fe),
                ('2009-01-01T00:00:04.000000Z', #dr5reff6hu5e),
                ('2009-01-01T00:00:07.000000Z', #dr5rshd6guzu),
                ('2009-01-01T00:00:09.000000Z', #dr5rugk21jew),
                ('2009-01-01T00:00:10.000000Z', #dr5ru7bhxxr7),
                ('2009-01-01T00:00:13.000000Z', #dr5ruhv90nt3),
                ('2009-01-01T00:00:15.000000Z', #dr5ru43qt65b),
                ('2009-01-01T00:00:16.000000Z', #dr5rt6qwr9s8),
                ('2009-01-01T00:00:17.000000Z', #dr5rvn6ccrhq),
                ('2009-01-01T00:00:21.000000Z', #dr5rgcj30u1n),
                ('2009-01-01T00:00:23.000000Z', #dr5rgbk02dxk),
                ('2009-01-01T00:00:25.000000Z', #000000000000),
                ('2009-01-01T00:00:25.000000Z', #s00000000000),
                ('2009-01-01T00:00:27.000000Z', #dr5rutj2ty38),
                ('2009-01-01T00:00:27.000000Z', #dr5rsncg1he5),
                ('2009-01-01T00:00:27.000000Z', #dr5x1p1t86gx),
                ('2009-01-01T00:00:28.000000Z', #dr5ruswebtne),
                ('2009-01-01T00:00:29.000000Z', #dr5rsqhc82jq),
                ('2009-01-01T00:00:29.000000Z', #dr5rzjybb9g4),
                ('2009-01-01T00:00:29.000000Z', #dr7grdqzppp0),
                ('2009-01-01T00:00:29.000000Z', #dr5rgbw8fxz6),
                ('2009-01-01T00:00:30.000000Z', #dr5rsqqnccnz),
                ('2009-01-01T00:00:30.000000Z', #dr5rtnj79c20),
                ('2009-01-01T00:00:33.000000Z', #dr5rzjy9r094),
                ('2009-01-01T00:00:36.000000Z', #dr5rvhfgw67v),
                ('2009-01-01T00:00:37.000000Z', #dr5rsnpw997n),
                ('2009-01-01T00:00:39.000000Z', #dr72h8hkt556),
                ('2009-01-01T00:00:43.000000Z', #000000000000)
            """;


    @Test
    public void testBindVariables() throws Exception {
        bindVariableService.clear();
        long hash1 = GeoHashes.fromString("dr5");
        long hash2 = GeoHashes.fromString("dr7");
        bindVariableService.setGeoHash(0, hash1, ColumnType.getGeoHashTypeWithBits(15));
        bindVariableService.setGeoHash(1, hash2, ColumnType.getGeoHashTypeWithBits(15));

        assertQueryAndPlan(
                "pickup_datetime\tpickup_geohash\n",
                "trips WHERE pickup_geohash WITHIN ($1, $2);",
                ddlTrips,
                "pickup_datetime",
                dmlTrips,
                """
                        pickup_datetime\tpickup_geohash
                        2009-01-01T00:00:00.000000Z\tdr5rsjutvshf
                        2009-01-01T00:00:00.000000Z\tdr5ruy5ttnw1
                        2009-01-01T00:00:02.000000Z\tdr5ruvkrr2fe
                        2009-01-01T00:00:04.000000Z\tdr5reff6hu5e
                        2009-01-01T00:00:07.000000Z\tdr5rshd6guzu
                        2009-01-01T00:00:09.000000Z\tdr5rugk21jew
                        2009-01-01T00:00:10.000000Z\tdr5ru7bhxxr7
                        2009-01-01T00:00:13.000000Z\tdr5ruhv90nt3
                        2009-01-01T00:00:15.000000Z\tdr5ru43qt65b
                        2009-01-01T00:00:16.000000Z\tdr5rt6qwr9s8
                        2009-01-01T00:00:17.000000Z\tdr5rvn6ccrhq
                        2009-01-01T00:00:21.000000Z\tdr5rgcj30u1n
                        2009-01-01T00:00:23.000000Z\tdr5rgbk02dxk
                        2009-01-01T00:00:27.000000Z\tdr5rutj2ty38
                        2009-01-01T00:00:27.000000Z\tdr5rsncg1he5
                        2009-01-01T00:00:27.000000Z\tdr5x1p1t86gx
                        2009-01-01T00:00:28.000000Z\tdr5ruswebtne
                        2009-01-01T00:00:29.000000Z\tdr5rsqhc82jq
                        2009-01-01T00:00:29.000000Z\tdr5rzjybb9g4
                        2009-01-01T00:00:29.000000Z\tdr7grdqzppp0
                        2009-01-01T00:00:29.000000Z\tdr5rgbw8fxz6
                        2009-01-01T00:00:30.000000Z\tdr5rsqqnccnz
                        2009-01-01T00:00:30.000000Z\tdr5rtnj79c20
                        2009-01-01T00:00:33.000000Z\tdr5rzjy9r094
                        2009-01-01T00:00:36.000000Z\tdr5rvhfgw67v
                        2009-01-01T00:00:37.000000Z\tdr5rsnpw997n
                        2009-01-01T00:00:39.000000Z\tdr72h8hkt556
                        """,
                true,
                false,
                false,
                """
                        Async Filter workers: 1
                          filter: pickup_geohash in [$0::geohash(3c),$1::geohash(3c)]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: trips
                        """
        );
    }

    @Test
    public void testConstConstFunc() throws Exception {
        assertQueryAndPlan(
                """
                        column
                        true
                        """,
                "SELECT #dr72 WITHIN (#dr5, #dr7);",
                ddlTrips,
                null,
                dmlTrips,
                """
                        column
                        true
                        """,
                true,
                true,
                false,
                """
                        VirtualRecord
                          functions: [true]
                            long_sequence count: 1
                        """
        );
    }

    @Test
    public void testConstVarFilter() throws Exception {
        assertQueryAndPlan(
                "pickup_datetime\tpickup_geohash\n",
                "trips WHERE #dr5reff6hu5e WITHIN (pickup_geohash);",
                ddlTrips,
                "pickup_datetime",
                dmlTrips,
                """
                        pickup_datetime\tpickup_geohash
                        2009-01-01T00:00:04.000000Z\tdr5reff6hu5e
                        """,
                true,
                false,
                false,
                """
                        Async Filter workers: 1
                          filter: "011001011100101101110110101110011100011010000110100010101101" in [pickup_geohash]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: trips
                        """
        );
    }

    @Test
    public void testNoArgs() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlTrips);
            drainWalQueue();
            assertQuery("trips WHERE pickup_geohash WITHIN ();")
                    .fails(27, "too few arguments");
        });
    }

    @Test
    public void testNullAllArgs() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlTrips);
            drainWalQueue();
            assertQuery("trips WHERE null WITHIN (null, null);")
                    .fails(25, "cannot compare GEOHASH");
        });
    }

    @Test
    public void testNullRhs() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlTrips);
            drainWalQueue();
            assertQuery("trips WHERE pickup_geohash WITHIN (null, null);")
                    .fails(35, "cannot compare GEOHASH");
        });
    }

    @Test
    public void testVarConstFilter() throws Exception {
        assertQueryAndPlan(
                "pickup_datetime\tpickup_geohash\n",
                "trips WHERE pickup_geohash WITHIN (#dr5, #dr7);",
                ddlTrips,
                "pickup_datetime",
                dmlTrips,
                """
                        pickup_datetime\tpickup_geohash
                        2009-01-01T00:00:00.000000Z\tdr5rsjutvshf
                        2009-01-01T00:00:00.000000Z\tdr5ruy5ttnw1
                        2009-01-01T00:00:02.000000Z\tdr5ruvkrr2fe
                        2009-01-01T00:00:04.000000Z\tdr5reff6hu5e
                        2009-01-01T00:00:07.000000Z\tdr5rshd6guzu
                        2009-01-01T00:00:09.000000Z\tdr5rugk21jew
                        2009-01-01T00:00:10.000000Z\tdr5ru7bhxxr7
                        2009-01-01T00:00:13.000000Z\tdr5ruhv90nt3
                        2009-01-01T00:00:15.000000Z\tdr5ru43qt65b
                        2009-01-01T00:00:16.000000Z\tdr5rt6qwr9s8
                        2009-01-01T00:00:17.000000Z\tdr5rvn6ccrhq
                        2009-01-01T00:00:21.000000Z\tdr5rgcj30u1n
                        2009-01-01T00:00:23.000000Z\tdr5rgbk02dxk
                        2009-01-01T00:00:27.000000Z\tdr5rutj2ty38
                        2009-01-01T00:00:27.000000Z\tdr5rsncg1he5
                        2009-01-01T00:00:27.000000Z\tdr5x1p1t86gx
                        2009-01-01T00:00:28.000000Z\tdr5ruswebtne
                        2009-01-01T00:00:29.000000Z\tdr5rsqhc82jq
                        2009-01-01T00:00:29.000000Z\tdr5rzjybb9g4
                        2009-01-01T00:00:29.000000Z\tdr7grdqzppp0
                        2009-01-01T00:00:29.000000Z\tdr5rgbw8fxz6
                        2009-01-01T00:00:30.000000Z\tdr5rsqqnccnz
                        2009-01-01T00:00:30.000000Z\tdr5rtnj79c20
                        2009-01-01T00:00:33.000000Z\tdr5rzjy9r094
                        2009-01-01T00:00:36.000000Z\tdr5rvhfgw67v
                        2009-01-01T00:00:37.000000Z\tdr5rsnpw997n
                        2009-01-01T00:00:39.000000Z\tdr72h8hkt556
                        """,
                true,
                false,
                false,
                """
                        Async Filter workers: 1
                          filter: pickup_geohash in ["011001011100101","011001011100111"]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: trips
                        """
        );
    }


    @Test
    public void testWithinWorksWithLatestByAndNoIndexNoSymbolData() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlTrips);
            execute(dmlTrips);
            execute("ALTER TABLE trips ADD COLUMN sym SYMBOL;");
            String query = "trips WHERE pickup_geohash WITHIN (#dr5) LATEST ON pickup_datetime PARTITION BY sym;";

            // without index
            assertPlanNoLeakCheck(query,
                    """
                            LatestByDeferredListValuesFiltered
                              filter: pickup_geohash in ["011001011100101"]
                                Frame backward scan on: trips
                            """);

            assertQuery(query)
                    .ddl(null)
                    .timestamp("pickup_datetime")
                    .expectSize()
                    .returns("""
                            pickup_datetime\tpickup_geohash\tsym
                            2009-01-01T00:00:37.000000Z\tdr5rsnpw997n\t
                            """);

            execute("ALTER TABLE trips ALTER COLUMN sym ADD INDEX;");
            drainWalQueue();

            // with index
            assertPlanNoLeakCheck(query,
                    """
                            LatestByDeferredListValuesFiltered
                              filter: pickup_geohash in ["011001011100101"]
                                Frame backward scan on: trips
                            """);

            assertQuery(query)
                    .ddl(null)
                    .timestamp("pickup_datetime")
                    .expectSize()
                    .returns("""
                            pickup_datetime\tpickup_geohash\tsym
                            2009-01-01T00:00:37.000000Z\tdr5rsnpw997n\t
                            """);

            // now override
            configOverrideUseWithinLatestByOptimisation();

            assertPlanNoLeakCheck(query,
                    """
                            LatestByAllIndexed
                                Async index backward scan on: sym workers: 2
                                  filter: pickup_geohash within("011001011100101000000000000000000000000000000000000000000000")
                                Frame backward scan on: trips
                            """);

            assertQuery(query)
                    .ddl(null)
                    .timestamp("pickup_datetime")
                    .expectSize()
                    .returns("pickup_datetime\tpickup_geohash\tsym\n");
        });
    }

    @Test
    public void testWithinWorksWithLatestByAndNoIndexWithSymbolData() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlTrips);
            execute(dmlTrips);
            execute("ALTER TABLE trips ADD COLUMN sym SYMBOL;");
            execute("UPDATE trips SET sym = 'ABC';");
            String query = "trips WHERE pickup_geohash WITHIN (#dr5) LATEST ON pickup_datetime PARTITION BY sym;";

            // without index
            assertPlanNoLeakCheck(query,
                    """
                            LatestByDeferredListValuesFiltered
                              filter: pickup_geohash in ["011001011100101"]
                                Frame backward scan on: trips
                            """);

            assertQuery(query)
                    .ddl(null)
                    .timestamp("pickup_datetime")
                    .expectSize()
                    .returns("""
                            pickup_datetime\tpickup_geohash\tsym
                            2009-01-01T00:00:37.000000Z\tdr5rsnpw997n\tABC
                            """);

            execute("ALTER TABLE trips ALTER COLUMN sym ADD INDEX;");
            drainWalQueue();

            // with index, it won't be used by default, since latest by optimisation is disabled
            assertPlanNoLeakCheck(query,
                    """
                            LatestByDeferredListValuesFiltered
                              filter: pickup_geohash in ["011001011100101"]
                                Frame backward scan on: trips
                            """);

            assertQuery(query)
                    .ddl(null)
                    .timestamp("pickup_datetime")
                    .expectSize()
                    .returns("""
                            pickup_datetime\tpickup_geohash\tsym
                            2009-01-01T00:00:37.000000Z\tdr5rsnpw997n\tABC
                            """);

            // now override
            configOverrideUseWithinLatestByOptimisation();

            assertPlanNoLeakCheck(query,
                    """
                            LatestByAllIndexed
                                Async index backward scan on: sym workers: 2
                                  filter: pickup_geohash within("011001011100101000000000000000000000000000000000000000000000")
                                Frame backward scan on: trips
                            """);

            // no result expected, because this special case factory execute LATEST BY before WHERE.
            assertQuery(query)
                    .ddl(null)
                    .timestamp("pickup_datetime")
                    .expectSize()
                    .returns("pickup_datetime\tpickup_geohash\tsym\n");
        });
    }
}
