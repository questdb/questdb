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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Tests group by with geohash column(s)
 */
public class GroupByGeoHashTest extends AbstractCairoTest {

    @Test
    public void testGroupByGeoByte() throws Exception {
        assertQuery("select geo, min(x) as minx, max(x) as maxx from geotest group by geo order by geo")
                .ddl("create table geotest as ( select cast(x/10 as geohash(1c)) as geo, x from long_sequence(29)) ")
                .expectSize()
                .returns("""
                        geo\tminx\tmaxx
                        0\t1\t9
                        1\t10\t19
                        2\t20\t29
                        """);
    }

    @Test
    public void testGroupByGeoByteAndGeoInt() throws Exception {
        assertQuery("select g1c, g4c, min(x) as minx, max(x) as maxx from geotest group by g1c,g4c order by g1c, g4c")
                .ddl("create table geotest as ( select cast(x/10 as geohash(1c)) as g1c, cast( 1000+x/10 as geohash(4c)) as g4c, 1000+x as x from long_sequence(29)) ")
                .expectSize()
                .returns("""
                        g1c\tg4c\tminx\tmaxx
                        0\t00z8\t1001\t1009
                        1\t00z9\t1010\t1019
                        2\t00zb\t1020\t1029
                        """);
    }

    @Test
    public void testGroupByGeoByteAndOtherColumns() throws Exception {
        assertQuery("select geo, (x/15) as x15, min(y) as miny from geotest group by geo, (x/15) order by 1, 2")
                .ddl("create table geotest as ( select cast(x/10 as geohash(1c)) as geo, x, x as y from long_sequence(29)) ")
                .expectSize()
                .returns("""
                        geo\tx15\tminy
                        0\t0\t1
                        1\t0\t10
                        1\t1\t15
                        2\t1\t20
                        """);
    }

    @Test
    public void testGroupByGeoInt() throws Exception {
        assertQuery("select geo, min(x) as minx, max(x) as maxx from geotest group by geo order by geo")
                .ddl("create table geotest as ( select cast( 1000+x/10 as geohash(4c)) as geo, 1000+x as x from long_sequence(29)) ")
                .expectSize()
                .returns("""
                        geo\tminx\tmaxx
                        00z8\t1001\t1009
                        00z9\t1010\t1019
                        00zb\t1020\t1029
                        """);
    }

    @Test
    public void testGroupByGeoLong() throws Exception {
        assertQuery("select geo, min(x) as minx, max(x) as maxx from geotest group by geo order by geo")
                .ddl("create table geotest as ( select cast( 3000000000+x/10 as geohash(8c)) as geo, 1000+x as x from long_sequence(29)) ")
                .expectSize()
                .returns("""
                        geo\tminx\tmaxx
                        02te0rh0\t1001\t1009
                        02te0rh1\t1010\t1019
                        02te0rh2\t1020\t1029
                        """);
    }

    @Test
    public void testGroupByGeoShort() throws Exception {
        assertQuery("select geo, min(x) as minx, max(x) as maxx from geotest group by geo order by geo")
                .ddl("create table geotest as ( select cast( 10+x/10 as geohash(2c)) as geo, 100+x as x from long_sequence(29)) ")
                .expectSize()
                .returns("""
                        geo\tminx\tmaxx
                        0b\t101\t109
                        0c\t110\t119
                        0d\t120\t129
                        """);
    }

}
