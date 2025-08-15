/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.griffin.engine.join;

import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;


public class HashJoinTest extends AbstractCairoTest {

    /**
     * Check that hash join factory doesn't allocate substantial amounts of memory prior to- and after cursor execution.
     * This is tricky because:
     * - memory allocation is delayed (so malloc() doesn't really allocate)
     * - most objects malloc but don't touch memory
     * - rss/wss can jump up and down due to gc, os, etc.
     */
    @Test
    public void testHashJoinDoesNotAllocateMemoryPriorToCursorOpenAndAfterCursorCloseForNonEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table weather_data_historical (\n" +
                    "  sensor_time timestamp not null,\n" +
                    "  sensor_day symbol,\n" +
                    "  min_temperature_out float,\n" +
                    "  max_temperature_out float,\n" +
                    "  avg_temperature_out float,\n" +
                    "  snow_height float,\n" +
                    "  rain_acc_24h float,\n" +
                    "  max_wind_gust_speed float\n" +
                    ")\n" +
                    "TIMESTAMP(sensor_time);", sqlExecutionContext);

            execute("insert into weather_data_historical \n" +
                    "select cast(x*1000000000 as timestamp), to_str( cast(x*1000000000 as timestamp), 'MM-dd'), \n" +
                    "       rnd_float()*100, rnd_float()*100, rnd_float()*100, rnd_float()*200, rnd_float()*100, rnd_float()*300\n" +
                    "from long_sequence(1000);");

            // allocate readers eagerly (at least one for each join) so that final getMem() doesn't report them as diff
            TableReader[] readers = new TableReader[10];
            try {
                for (int i = 0; i < readers.length; i++) {
                    readers[i] = getReader("weather_data_historical");
                }
            } finally {
                Misc.free(readers);
            }

            long tagBeforeFactory = getMemUsedByFactories();
            System.gc();

            try (
                    final RecordCursorFactory factory = select(
                            "  select a1.sensor_day, \n" +
                                    "  warmest_day, to_str(a2.sensor_time, 'yyyy') as warmest_day_year, \n" +
                                    "  coldest_day, to_str(a3.sensor_time, 'yyyy') as coldest_day_year,\n" +
                                    "  warmest_night, to_str(a4.sensor_time, 'yyyy') as warmest_night_year,\n" +
                                    "  coldest_night, to_str(a5.sensor_time, 'yyyy') as coldest_night_year,\n" +
                                    "  max_snow_height, to_str(a6.sensor_time, 'yyyy') as max_snow_height_year,\n" +
                                    "  max_wind_gust_overall, to_str(a7.sensor_time, 'yyyy') as max_wind_gust_year,\n" +
                                    "  avg_temperature\n" +
                                    "  from\n" +
                                    "  (\n" +
                                    "    select sensor_day, \n" +
                                    "    max(max_temperature_out) as warmest_day, min(max_temperature_out) as coldest_day, \n" +
                                    "    max(min_temperature_out) as warmest_night, min(min_temperature_out) as coldest_night, \n" +
                                    "    max(rain_acc_24h) as max_rain, max(snow_height) as max_snow_height, \n" +
                                    "    max(max_wind_gust_speed) as max_wind_gust_overall, \n" +
                                    "    avg(avg_temperature_out) as avg_temperature\n" +
                                    "    from weather_data_historical \n" +
                                    "    group by sensor_day\n" +
                                    "  ) a1\n" +
                                    "  left join weather_data_historical a2 on (a1.sensor_day = a2.sensor_day and warmest_day = a2.max_temperature_out)\n" +
                                    "  left join weather_data_historical a3 on (a1.sensor_day = a3.sensor_day and coldest_day = a3.max_temperature_out)\n" +
                                    "  left join weather_data_historical a4 on (a1.sensor_day = a4.sensor_day and warmest_night = a4.min_temperature_out)\n" +
                                    "  left join weather_data_historical a5 on (a1.sensor_day = a5.sensor_day and coldest_night = a5.min_temperature_out)\n" +
                                    "  left join weather_data_historical a6 on (a1.sensor_day = a6.sensor_day and max_snow_height = a6.snow_height and a6.snow_height > 0)\n" +
                                    "  left join weather_data_historical a7 on (a1.sensor_day = a7.sensor_day and max_wind_gust_overall = a7.max_wind_gust_speed)"
                    )
            ) {
                long freeCount;
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    TestUtils.drainCursor(cursor);
                    freeCount = Unsafe.getFreeCount();
                }

                Assert.assertTrue(freeCount < Unsafe.getFreeCount());
                Assert.assertTrue(getMemUsedByFactories() < tagBeforeFactory + 1024 * 1024);
            }
        });
    }

    @Test
    public void testHashOuterLeftJoinWithFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table taba (i long, locale_name symbol )");
            execute("create table tabb (i long, state symbol, city symbol)");
            execute("insert into taba values (1, 'pl')");
            execute("insert into tabb values (1, 'a', 'pl')");
            execute("insert into tabb values (1, 'b', 'b')");

            assertQueryNoLeakCheck("i\tlocale_name\ti1\tstate\tcity\n" +
                    "1\tpl\t1\ta\tpl\n", "select * from taba left join tabb on taba.i = tabb.i and (locale_name = state OR locale_name=city)", null);
        });
    }

}
