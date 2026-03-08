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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Tests group by with geohash column(s)
 */
public class GroupByGeoHashTest extends AbstractCairoTest {

    @Test
    public void testGroupByGeoByte() throws Exception {
        assertQuery("geo\tminx\tmaxx\n" +
                        "0\t1\t9\n" +
                        "1\t10\t19\n" +
                        "2\t20\t29\n",
                "select geo, min(x) as minx, max(x) as maxx from geotest group by geo order by geo",
                "create table geotest as ( select cast(x/10 as geohash(1c)) as geo, x from long_sequence(29)) ",
                null, true, true);
    }

    @Test
    public void testGroupByGeoByteAndGeoInt() throws Exception {
        assertQuery("g1c\tg4c\tminx\tmaxx\n" +
                        "0\t00z8\t1001\t1009\n" +
                        "1\t00z9\t1010\t1019\n" +
                        "2\t00zb\t1020\t1029\n",
                "select g1c, g4c, min(x) as minx, max(x) as maxx from geotest group by g1c,g4c order by g1c, g4c",
                "create table geotest as ( select cast(x/10 as geohash(1c)) as g1c, cast( 1000+x/10 as geohash(4c)) as g4c, 1000+x as x from long_sequence(29)) ",
                null, true, true);
    }

    @Test
    public void testGroupByGeoByteAndOtherColumns() throws Exception {
        assertQuery(
                "geo\tx15\tminy\n" +
                        "0\t0\t1\n" +
                        "1\t0\t10\n" +
                        "1\t1\t15\n" +
                        "2\t1\t20\n",
                "select geo, (x/15) as x15, min(y) as miny from geotest group by geo, (x/15) order by 1, 2",
                "create table geotest as ( select cast(x/10 as geohash(1c)) as geo, x, x as y from long_sequence(29)) ",
                null,
                true,
                true
        );
    }

    @Test
    public void testGroupByGeoInt() throws Exception {
        assertQuery("geo\tminx\tmaxx\n" +
                        "00z8\t1001\t1009\n" +
                        "00z9\t1010\t1019\n" +
                        "00zb\t1020\t1029\n",
                "select geo, min(x) as minx, max(x) as maxx from geotest group by geo order by geo",
                "create table geotest as ( select cast( 1000+x/10 as geohash(4c)) as geo, 1000+x as x from long_sequence(29)) ",
                null, true, true);
    }

    @Test
    public void testGroupByGeoLong() throws Exception {
        assertQuery(
                "geo\tminx\tmaxx\n" +
                        "02te0rh0\t1001\t1009\n" +
                        "02te0rh1\t1010\t1019\n" +
                        "02te0rh2\t1020\t1029\n",
                "select geo, min(x) as minx, max(x) as maxx from geotest group by geo order by geo",
                "create table geotest as ( select cast( 3000000000+x/10 as geohash(8c)) as geo, 1000+x as x from long_sequence(29)) ",
                null,
                true,
                true
        );
    }

    @Test
    public void testGroupByGeoShort() throws Exception {
        assertQuery("geo\tminx\tmaxx\n" +
                        "0b\t101\t109\n" +
                        "0c\t110\t119\n" +
                        "0d\t120\t129\n",
                "select geo, min(x) as minx, max(x) as maxx from geotest group by geo order by geo",
                "create table geotest as ( select cast( 10+x/10 as geohash(2c)) as geo, 100+x as x from long_sequence(29)) ",
                null, true, true);
    }

}
