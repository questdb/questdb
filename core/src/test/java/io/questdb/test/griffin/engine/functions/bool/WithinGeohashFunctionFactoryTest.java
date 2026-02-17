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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class WithinGeohashFunctionFactoryTest extends AbstractCairoTest {

    public static String ddlTrips = "CREATE TABLE trips (\n" +
            "pickup_datetime TIMESTAMP,\n" +
            "pickup_geohash GEOHASH(12c)\n" +
            ") timestamp(pickup_datetime)\n";

    public static String dmlTrips = "INSERT INTO trips (pickup_datetime, pickup_geohash) VALUES \n" +
            "    ('2009-01-01T00:00:00.000000Z', #dr5rsjutvshf),\n" +
            "    ('2009-01-01T00:00:00.000000Z', #dr5ruy5ttnw1),\n" +
            "    ('2009-01-01T00:00:02.000000Z', #dr5ruvkrr2fe),\n" +
            "    ('2009-01-01T00:00:04.000000Z', #dr5reff6hu5e),\n" +
            "    ('2009-01-01T00:00:07.000000Z', #dr5rshd6guzu),\n" +
            "    ('2009-01-01T00:00:09.000000Z', #dr5rugk21jew),\n" +
            "    ('2009-01-01T00:00:10.000000Z', #dr5ru7bhxxr7),\n" +
            "    ('2009-01-01T00:00:13.000000Z', #dr5ruhv90nt3),\n" +
            "    ('2009-01-01T00:00:15.000000Z', #dr5ru43qt65b),\n" +
            "    ('2009-01-01T00:00:16.000000Z', #dr5rt6qwr9s8),\n" +
            "    ('2009-01-01T00:00:17.000000Z', #dr5rvn6ccrhq),\n" +
            "    ('2009-01-01T00:00:21.000000Z', #dr5rgcj30u1n),\n" +
            "    ('2009-01-01T00:00:23.000000Z', #dr5rgbk02dxk),\n" +
            "    ('2009-01-01T00:00:25.000000Z', #000000000000),\n" +
            "    ('2009-01-01T00:00:25.000000Z', #s00000000000),\n" +
            "    ('2009-01-01T00:00:27.000000Z', #dr5rutj2ty38),\n" +
            "    ('2009-01-01T00:00:27.000000Z', #dr5rsncg1he5),\n" +
            "    ('2009-01-01T00:00:27.000000Z', #dr5x1p1t86gx),\n" +
            "    ('2009-01-01T00:00:28.000000Z', #dr5ruswebtne),\n" +
            "    ('2009-01-01T00:00:29.000000Z', #dr5rsqhc82jq),\n" +
            "    ('2009-01-01T00:00:29.000000Z', #dr5rzjybb9g4),\n" +
            "    ('2009-01-01T00:00:29.000000Z', #dr7grdqzppp0),\n" +
            "    ('2009-01-01T00:00:29.000000Z', #dr5rgbw8fxz6),\n" +
            "    ('2009-01-01T00:00:30.000000Z', #dr5rsqqnccnz),\n" +
            "    ('2009-01-01T00:00:30.000000Z', #dr5rtnj79c20),\n" +
            "    ('2009-01-01T00:00:33.000000Z', #dr5rzjy9r094),\n" +
            "    ('2009-01-01T00:00:36.000000Z', #dr5rvhfgw67v),\n" +
            "    ('2009-01-01T00:00:37.000000Z', #dr5rsnpw997n),\n" +
            "    ('2009-01-01T00:00:39.000000Z', #dr72h8hkt556),\n" +
            "    ('2009-01-01T00:00:43.000000Z', #000000000000)\n";


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
                "pickup_datetime\tpickup_geohash\n" +
                        "2009-01-01T00:00:00.000000Z\tdr5rsjutvshf\n" +
                        "2009-01-01T00:00:00.000000Z\tdr5ruy5ttnw1\n" +
                        "2009-01-01T00:00:02.000000Z\tdr5ruvkrr2fe\n" +
                        "2009-01-01T00:00:04.000000Z\tdr5reff6hu5e\n" +
                        "2009-01-01T00:00:07.000000Z\tdr5rshd6guzu\n" +
                        "2009-01-01T00:00:09.000000Z\tdr5rugk21jew\n" +
                        "2009-01-01T00:00:10.000000Z\tdr5ru7bhxxr7\n" +
                        "2009-01-01T00:00:13.000000Z\tdr5ruhv90nt3\n" +
                        "2009-01-01T00:00:15.000000Z\tdr5ru43qt65b\n" +
                        "2009-01-01T00:00:16.000000Z\tdr5rt6qwr9s8\n" +
                        "2009-01-01T00:00:17.000000Z\tdr5rvn6ccrhq\n" +
                        "2009-01-01T00:00:21.000000Z\tdr5rgcj30u1n\n" +
                        "2009-01-01T00:00:23.000000Z\tdr5rgbk02dxk\n" +
                        "2009-01-01T00:00:27.000000Z\tdr5rutj2ty38\n" +
                        "2009-01-01T00:00:27.000000Z\tdr5rsncg1he5\n" +
                        "2009-01-01T00:00:27.000000Z\tdr5x1p1t86gx\n" +
                        "2009-01-01T00:00:28.000000Z\tdr5ruswebtne\n" +
                        "2009-01-01T00:00:29.000000Z\tdr5rsqhc82jq\n" +
                        "2009-01-01T00:00:29.000000Z\tdr5rzjybb9g4\n" +
                        "2009-01-01T00:00:29.000000Z\tdr7grdqzppp0\n" +
                        "2009-01-01T00:00:29.000000Z\tdr5rgbw8fxz6\n" +
                        "2009-01-01T00:00:30.000000Z\tdr5rsqqnccnz\n" +
                        "2009-01-01T00:00:30.000000Z\tdr5rtnj79c20\n" +
                        "2009-01-01T00:00:33.000000Z\tdr5rzjy9r094\n" +
                        "2009-01-01T00:00:36.000000Z\tdr5rvhfgw67v\n" +
                        "2009-01-01T00:00:37.000000Z\tdr5rsnpw997n\n" +
                        "2009-01-01T00:00:39.000000Z\tdr72h8hkt556\n",
                true,
                false,
                false,
                "Async Filter workers: 1\n" +
                        "  filter: pickup_geohash in [$0::geohash(3c),$1::geohash(3c)]\n" +
                        "    PageFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: trips\n"
        );
    }

    @Test
    public void testConstConstFunc() throws Exception {
        assertQueryAndPlan(
                "column\n" +
                        "true\n",
                "SELECT #dr72 WITHIN (#dr5, #dr7);",
                ddlTrips,
                null,
                dmlTrips,
                "column\n" +
                        "true\n",
                true,
                true,
                false,
                "VirtualRecord\n" +
                        "  functions: [true]\n" +
                        "    long_sequence count: 1\n"
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
                "pickup_datetime\tpickup_geohash\n" +
                        "2009-01-01T00:00:04.000000Z\tdr5reff6hu5e\n",
                true,
                false,
                false,
                "Async Filter workers: 1\n" +
                        "  filter: \"011001011100101101110110101110011100011010000110100010101101\" in [pickup_geohash]\n" +
                        "    PageFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: trips\n"
        );
    }

    @Test
    public void testNoArgs() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlTrips);
            drainWalQueue();
            assertException("trips WHERE pickup_geohash WITHIN ();", 27, "too few arguments");
        });
    }

    @Test
    public void testNullAllArgs() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlTrips);
            drainWalQueue();
            assertException("trips WHERE null WITHIN (null, null);", 25, "cannot compare GEOHASH");
        });
    }

    @Test
    public void testNullRhs() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlTrips);
            drainWalQueue();
            assertException("trips WHERE pickup_geohash WITHIN (null, null);", 35, "cannot compare GEOHASH");
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
                "pickup_datetime\tpickup_geohash\n" +
                        "2009-01-01T00:00:00.000000Z\tdr5rsjutvshf\n" +
                        "2009-01-01T00:00:00.000000Z\tdr5ruy5ttnw1\n" +
                        "2009-01-01T00:00:02.000000Z\tdr5ruvkrr2fe\n" +
                        "2009-01-01T00:00:04.000000Z\tdr5reff6hu5e\n" +
                        "2009-01-01T00:00:07.000000Z\tdr5rshd6guzu\n" +
                        "2009-01-01T00:00:09.000000Z\tdr5rugk21jew\n" +
                        "2009-01-01T00:00:10.000000Z\tdr5ru7bhxxr7\n" +
                        "2009-01-01T00:00:13.000000Z\tdr5ruhv90nt3\n" +
                        "2009-01-01T00:00:15.000000Z\tdr5ru43qt65b\n" +
                        "2009-01-01T00:00:16.000000Z\tdr5rt6qwr9s8\n" +
                        "2009-01-01T00:00:17.000000Z\tdr5rvn6ccrhq\n" +
                        "2009-01-01T00:00:21.000000Z\tdr5rgcj30u1n\n" +
                        "2009-01-01T00:00:23.000000Z\tdr5rgbk02dxk\n" +
                        "2009-01-01T00:00:27.000000Z\tdr5rutj2ty38\n" +
                        "2009-01-01T00:00:27.000000Z\tdr5rsncg1he5\n" +
                        "2009-01-01T00:00:27.000000Z\tdr5x1p1t86gx\n" +
                        "2009-01-01T00:00:28.000000Z\tdr5ruswebtne\n" +
                        "2009-01-01T00:00:29.000000Z\tdr5rsqhc82jq\n" +
                        "2009-01-01T00:00:29.000000Z\tdr5rzjybb9g4\n" +
                        "2009-01-01T00:00:29.000000Z\tdr7grdqzppp0\n" +
                        "2009-01-01T00:00:29.000000Z\tdr5rgbw8fxz6\n" +
                        "2009-01-01T00:00:30.000000Z\tdr5rsqqnccnz\n" +
                        "2009-01-01T00:00:30.000000Z\tdr5rtnj79c20\n" +
                        "2009-01-01T00:00:33.000000Z\tdr5rzjy9r094\n" +
                        "2009-01-01T00:00:36.000000Z\tdr5rvhfgw67v\n" +
                        "2009-01-01T00:00:37.000000Z\tdr5rsnpw997n\n" +
                        "2009-01-01T00:00:39.000000Z\tdr72h8hkt556\n",
                true,
                false,
                false,
                "Async Filter workers: 1\n" +
                        "  filter: pickup_geohash in [\"011001011100101\",\"011001011100111\"]\n" +
                        "    PageFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: trips\n"
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
                    "LatestByDeferredListValuesFiltered\n" +
                            "  filter: pickup_geohash in [\"011001011100101\"]\n" +
                            "    Frame backward scan on: trips\n");

            assertQuery("pickup_datetime\tpickup_geohash\tsym\n" +
                            "2009-01-01T00:00:37.000000Z\tdr5rsnpw997n\t\n",
                    query,
                    null,
                    "pickup_datetime",
                    true,
                    true);

            execute("ALTER TABLE trips ALTER COLUMN sym ADD INDEX;");
            drainWalQueue();

            // with index
            assertPlanNoLeakCheck(query,
                    "LatestByDeferredListValuesFiltered\n" +
                            "  filter: pickup_geohash in [\"011001011100101\"]\n" +
                            "    Frame backward scan on: trips\n");

            assertQuery("pickup_datetime\tpickup_geohash\tsym\n" +
                            "2009-01-01T00:00:37.000000Z\tdr5rsnpw997n\t\n",
                    query,
                    null,
                    "pickup_datetime",
                    true,
                    true);

            // now override
            configOverrideUseWithinLatestByOptimisation();

            assertPlanNoLeakCheck(query,
                    "LatestByAllIndexed\n" +
                            "    Async index backward scan on: sym workers: 2\n" +
                            "      filter: pickup_geohash within(\"011001011100101000000000000000000000000000000000000000000000\")\n" +
                            "    Frame backward scan on: trips\n");

            assertQuery("pickup_datetime\tpickup_geohash\tsym\n",
                    query,
                    null,
                    "pickup_datetime",
                    true,
                    true);
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
                    "LatestByDeferredListValuesFiltered\n" +
                            "  filter: pickup_geohash in [\"011001011100101\"]\n" +
                            "    Frame backward scan on: trips\n");

            assertQuery("pickup_datetime\tpickup_geohash\tsym\n" +
                            "2009-01-01T00:00:37.000000Z\tdr5rsnpw997n\tABC\n",
                    query,
                    null,
                    "pickup_datetime",
                    true,
                    true);

            execute("ALTER TABLE trips ALTER COLUMN sym ADD INDEX;");
            drainWalQueue();

            // with index, it won't be used by default, since latest by optimisation is disabled
            assertPlanNoLeakCheck(query,
                    "LatestByDeferredListValuesFiltered\n" +
                            "  filter: pickup_geohash in [\"011001011100101\"]\n" +
                            "    Frame backward scan on: trips\n");

            assertQuery("pickup_datetime\tpickup_geohash\tsym\n" +
                            "2009-01-01T00:00:37.000000Z\tdr5rsnpw997n\tABC\n",
                    query,
                    null,
                    "pickup_datetime",
                    true,
                    true);

            // now override
            configOverrideUseWithinLatestByOptimisation();

            assertPlanNoLeakCheck(query,
                    "LatestByAllIndexed\n" +
                            "    Async index backward scan on: sym workers: 2\n" +
                            "      filter: pickup_geohash within(\"011001011100101000000000000000000000000000000000000000000000\")\n" +
                            "    Frame backward scan on: trips\n");

            // no result expected, because this special case factory execute LATEST BY before WHERE.
            assertQuery("pickup_datetime\tpickup_geohash\tsym\n",
                    query,
                    null,
                    "pickup_datetime",
                    true,
                    true);
        });
    }
}
