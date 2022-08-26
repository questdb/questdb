/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;

import static org.hamcrest.MatcherAssert.assertThat;

import static org.hamcrest.Matchers.*;

import org.junit.Test;

/**
 * Check that hash join factory doesn't allocate substantial amounts of memory prior to- and after cursor execution.
 * This is tricky because:
 * - memory allocation is delayed (so malloc() doesn't really allocate)
 * - most objects malloc but don't touch memory
 * - rss/wss can jump up and down due to gc, os, etc.
 */
public class HashJoinTest extends AbstractGriffinTest {

    @Test
    public void testHashJoinDoesntAllocateMemoryPriorToCursorOpenAndAfterCursorCloseForNonEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table weather_data_historical (\n" +
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

            compile("insert into weather_data_historical \n" +
                    "select cast(x*1000000000 as timestamp), to_str( cast(x*1000000000 as timestamp), 'MM-dd') ), \n" +
                    "       rnd_float()*100, rnd_float()*100, rnd_float()*100, rnd_float()*200, rnd_float()*100, rnd_float()*300\n" +
                    "from long_sequence(1000);");

            //allocate readers eagerly (at least one for each join) so that final getMem() doesn't report them as diff
            TableReader[] readers = new TableReader[10];
            for (int i = 0; i < readers.length; i++) {
                readers[i] = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), "weather_data_historical");
            }
            for (int i = 0; i < readers.length; i++) {
                readers[i].close();
            }

            long rssBeforeFactory = Os.getRss();
            long tagBeforeFactory = getMemUsedExceptMmap();
            System.gc();

            try (final RecordCursorFactory factory = compiler.compile("  select a1.sensor_day, \n" +
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
                    "  left join weather_data_historical a7 on (a1.sensor_day = a7.sensor_day and max_wind_gust_overall = a7.max_wind_gust_speed)", sqlExecutionContext).getRecordCursorFactory()) {

                long rssBeforeCursor = Os.getRss();
                long virtCursorMem = getMemUsedExceptMmap() - tagBeforeFactory;
                assertThat(rssBeforeCursor - rssBeforeFactory, is(lessThan(virtCursorMem)));

                long freeCount;
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    while (cursor.hasNext()) {
                    }

                    long rssDuringCursor = Os.getRss();
                    assertThat(rssDuringCursor - rssBeforeCursor, is(lessThan(virtCursorMem)));
                    freeCount = Unsafe.getFreeCount();
                }

                assertThat(freeCount, is(lessThan(Unsafe.getFreeCount())));
                assertThat(getMemUsedExceptMmap(), lessThan(tagBeforeFactory + 1024 * 1024));
            }
        });
    }

}
