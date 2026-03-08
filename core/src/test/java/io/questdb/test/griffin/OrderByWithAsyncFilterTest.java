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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static io.questdb.PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT;

public class OrderByWithAsyncFilterTest extends AbstractCairoTest {

    private static final String DDL = "create table weather_data as \n" +
            "(select  dateadd( 'm' , cast(x-1000 as int), to_timestamp('2022-08-01:00:00:00', 'yyyy-MM-dd:HH:mm:ss') ) sensor_time, " +
            "1000-x temperature_out \n" +
            "from long_sequence(1000)) timestamp(sensor_time) partition by year;";
    private static final int PAGE_FRAME_COUNT = 4;
    private static final int PAGE_FRAME_MAX_ROWS = 100;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, PAGE_FRAME_MAX_ROWS);
        // We intentionally use small values for shard count and reduce
        // queue capacity to exhibit various edge cases.
        setProperty(CAIRO_PAGE_FRAME_SHARD_COUNT, 2);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, PAGE_FRAME_COUNT);

        AbstractCairoTest.setUpStatic();
    }

    // tearDown() overrides settings set in setUpStatic()
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, PAGE_FRAME_MAX_ROWS);
        setProperty(CAIRO_PAGE_FRAME_SHARD_COUNT, 2);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, PAGE_FRAME_COUNT);
        super.setUp();
    }

    @Test
    public void testAsyncFilterWithNegativeLimitNoOrderBy() throws Exception {
        assertQuery("sensor_time\ttemperature_out\n" +
                        "2022-07-31T23:56:00.000000Z\t4\n" +
                        "2022-07-31T23:57:00.000000Z\t3\n" +
                        "2022-07-31T23:58:00.000000Z\t2\n" +
                        "2022-07-31T23:59:00.000000Z\t1\n" +
                        "2022-08-01T00:00:00.000000Z\t0\n",
                "select sensor_time, temperature_out\n" +
                        "from weather_data \n" +
                        "where sensor_time <= dateadd( 's', rnd_int(0,1,0), to_timestamp('2022-08-01:00:00:00', 'yyyy-MM-dd:HH:mm:ss')) \n" +
                        "limit -5; ",
                DDL,
                "sensor_time", true, true);
    }

    @Test
    public void testAsyncFilterWithNegativeLimitNoOrderByThenCount() throws Exception {
        assertQuery("count\n" +
                        "5\n",
                "select count(*) from ( " +
                        "select sensor_time, temperature_out " +
                        "from weather_data " +
                        "where sensor_time <= dateadd( 's', rnd_int(0,1,0), to_timestamp('2022-08-01:00:00:00', 'yyyy-MM-dd:HH:mm:ss')) " +
                        "limit -5 )",
                DDL,
                null, false, true);
    }

    @Test
    public void testAsyncFilterWithNegativeLimitOrderByAsc() throws Exception {
        assertQuery("sensor_time\ttemperature_out\n" +
                        "2022-07-31T23:56:00.000000Z\t4\n" +
                        "2022-07-31T23:57:00.000000Z\t3\n" +
                        "2022-07-31T23:58:00.000000Z\t2\n" +
                        "2022-07-31T23:59:00.000000Z\t1\n" +
                        "2022-08-01T00:00:00.000000Z\t0\n",
                "select sensor_time, temperature_out\n" +
                        "from weather_data \n" +
                        "where sensor_time <= dateadd( 's', rnd_int(0,1,0), to_timestamp('2022-08-01:00:00:00', 'yyyy-MM-dd:HH:mm:ss')) \n" +
                        "order by sensor_time  asc \n" +
                        "limit -5",
                DDL,
                "sensor_time", true, true);
    }

    @Test
    public void testAsyncFilterWithNegativeLimitOrderByDesc() throws Exception {
        assertQuery("sensor_time\ttemperature_out\n" +
                        "2022-07-31T07:25:00.000000Z\t995\n" +
                        "2022-07-31T07:24:00.000000Z\t996\n" +
                        "2022-07-31T07:23:00.000000Z\t997\n" +
                        "2022-07-31T07:22:00.000000Z\t998\n" +
                        "2022-07-31T07:21:00.000000Z\t999\n",
                "select sensor_time, temperature_out\n" +
                        "from weather_data \n" +
                        "where sensor_time <= dateadd( 's', rnd_int(0,1,0), to_timestamp('2022-08-01:00:00:00', 'yyyy-MM-dd:HH:mm:ss')) \n" +
                        "order by sensor_time  desc \n" +
                        "limit -5; ",
                DDL,
                "sensor_time###DESC", true, true);
    }

    @Test
    public void testAsyncFilterWithOrderByAsc() throws Exception {
        assertQuery("sensor_time\ttemperature_out\n" +
                        "2022-07-31T07:21:00.000000Z\t999\n" +
                        "2022-07-31T07:22:00.000000Z\t998\n" +
                        "2022-07-31T07:23:00.000000Z\t997\n" +
                        "2022-07-31T07:24:00.000000Z\t996\n" +
                        "2022-07-31T07:25:00.000000Z\t995\n",
                "select sensor_time, temperature_out\n" +
                        "from weather_data \n" +
                        "where sensor_time <= dateadd( 's', rnd_int(0,1,0), to_timestamp('2022-07-31:07:25:00', 'yyyy-MM-dd:HH:mm:ss')) \n" +
                        "order by sensor_time  asc \n",
                DDL,
                "sensor_time", true, false);
    }

    @Test
    public void testAsyncFilterWithOrderByDesc() throws Exception {
        assertQuery("sensor_time\ttemperature_out\n" +
                        "2022-08-01T00:00:00.000000Z\t0\n" +
                        "2022-07-31T23:59:00.000000Z\t1\n" +
                        "2022-07-31T23:58:00.000000Z\t2\n" +
                        "2022-07-31T23:57:00.000000Z\t3\n" +
                        "2022-07-31T23:56:00.000000Z\t4\n",
                "select sensor_time, temperature_out\n" +
                        "from weather_data \n" +
                        "where sensor_time >= dateadd( 's', rnd_int(0,1,0)*0, to_timestamp('2022-07-31:23:56:00', 'yyyy-MM-dd:HH:mm:ss'))  \n" +
                        "order by sensor_time  desc \n",
                DDL,
                "sensor_time###DESC", true, false);
    }

    @Test
    public void testAsyncFilterWithPositiveLimitNoOrderBy() throws Exception {
        assertQuery("sensor_time\ttemperature_out\n" +
                        "2022-07-31T07:21:00.000000Z\t999\n" +
                        "2022-07-31T07:22:00.000000Z\t998\n" +
                        "2022-07-31T07:23:00.000000Z\t997\n" +
                        "2022-07-31T07:24:00.000000Z\t996\n" +
                        "2022-07-31T07:25:00.000000Z\t995\n",
                "select sensor_time, temperature_out\n" +
                        "from weather_data \n" +
                        "where sensor_time <= dateadd( 's', rnd_int(0,1,0), to_timestamp('2022-08-01:00:00:00', 'yyyy-MM-dd:HH:mm:ss')) \n" +
                        "limit 5; ",
                DDL,
                "sensor_time", true, false);
    }

    @Test
    public void testAsyncFilterWithPositiveLimitOrderByAsc() throws Exception {
        assertQuery("sensor_time\ttemperature_out\n" +
                        "2022-07-31T07:21:00.000000Z\t999\n" +
                        "2022-07-31T07:22:00.000000Z\t998\n" +
                        "2022-07-31T07:23:00.000000Z\t997\n" +
                        "2022-07-31T07:24:00.000000Z\t996\n" +
                        "2022-07-31T07:25:00.000000Z\t995\n",
                "select sensor_time, temperature_out\n" +
                        "from weather_data \n" +
                        "where sensor_time <= dateadd( 's', rnd_int(0,1,0), to_timestamp('2022-08-01:00:00:00', 'yyyy-MM-dd:HH:mm:ss')) \n" +
                        "order by sensor_time  asc \n" +
                        "limit 5; ",
                DDL,
                "sensor_time", true, false);
    }

    @Test
    public void testAsyncFilterWithPositiveLimitOrderByDesc() throws Exception {
        assertQuery("sensor_time\ttemperature_out\n" +
                        "2022-08-01T00:00:00.000000Z\t0\n" +
                        "2022-07-31T23:59:00.000000Z\t1\n" +
                        "2022-07-31T23:58:00.000000Z\t2\n" +
                        "2022-07-31T23:57:00.000000Z\t3\n" +
                        "2022-07-31T23:56:00.000000Z\t4\n",
                "select sensor_time, temperature_out\n" +
                        "from weather_data \n" +
                        "where sensor_time <= dateadd( 's', rnd_int(0,1,0), to_timestamp('2022-08-01:00:00:00', 'yyyy-MM-dd:HH:mm:ss')) \n" +
                        "order by sensor_time  desc \n" +
                        "limit 5; ",
                DDL,
                "sensor_time###DESC", true, false);
    }

    @Test
    public void testAsyncJitFilterWithNegativeLimitNoOrderBy() throws Exception {
        assertQuery("sensor_time\ttemperature_out\n" +
                        "2022-07-31T23:56:00.000000Z\t4\n" +
                        "2022-07-31T23:57:00.000000Z\t3\n" +
                        "2022-07-31T23:58:00.000000Z\t2\n" +
                        "2022-07-31T23:59:00.000000Z\t1\n" +
                        "2022-08-01T00:00:00.000000Z\t0\n",
                "select sensor_time, temperature_out\n" +
                        "from weather_data \n" +
                        "where temperature_out <= 10000 \n" +
                        "limit -5; ",
                DDL,
                "sensor_time", true, true);
    }

    @Test
    public void testAsyncJitFilterWithNegativeLimitOrderByAsc() throws Exception {
        assertQuery("sensor_time\ttemperature_out\n" +
                        "2022-07-31T23:56:00.000000Z\t4\n" +
                        "2022-07-31T23:57:00.000000Z\t3\n" +
                        "2022-07-31T23:58:00.000000Z\t2\n" +
                        "2022-07-31T23:59:00.000000Z\t1\n" +
                        "2022-08-01T00:00:00.000000Z\t0\n",
                "select sensor_time, temperature_out\n" +
                        "from weather_data \n" +
                        "where temperature_out <= 10000 \n" +
                        "order by sensor_time  asc \n" +
                        "limit -5; ",
                DDL,
                "sensor_time", true, true);
    }

    @Test
    public void testAsyncJitFilterWithNegativeLimitOrderByDesc() throws Exception {
        assertQuery("sensor_time\ttemperature_out\n" +
                        "2022-07-31T07:25:00.000000Z\t995\n" +
                        "2022-07-31T07:24:00.000000Z\t996\n" +
                        "2022-07-31T07:23:00.000000Z\t997\n" +
                        "2022-07-31T07:22:00.000000Z\t998\n" +
                        "2022-07-31T07:21:00.000000Z\t999\n",
                "select sensor_time, temperature_out\n" +
                        "from weather_data \n" +
                        "where temperature_out <= 10000 \n" +
                        "order by sensor_time  desc \n" +
                        "limit -5; ",
                DDL,
                "sensor_time###DESC", true, true);
    }

    @Test
    public void testAsyncJitFilterWithOrderByAsc() throws Exception {
        assertQuery("sensor_time\ttemperature_out\n" +
                        "2022-07-31T07:21:00.000000Z\t999\n" +
                        "2022-07-31T07:22:00.000000Z\t998\n" +
                        "2022-07-31T07:23:00.000000Z\t997\n" +
                        "2022-07-31T07:24:00.000000Z\t996\n" +
                        "2022-07-31T07:25:00.000000Z\t995\n" +
                        "2022-07-31T07:26:00.000000Z\t994\n" +
                        "2022-07-31T07:27:00.000000Z\t993\n" +
                        "2022-07-31T07:28:00.000000Z\t992\n" +
                        "2022-07-31T07:29:00.000000Z\t991\n",
                "select sensor_time, temperature_out\n" +
                        "from weather_data \n" +
                        "where temperature_out > 990 \n" +
                        "order by sensor_time  asc \n",
                DDL,
                "sensor_time", true, false);
    }

    @Test
    public void testAsyncJitFilterWithOrderByDesc() throws Exception {
        assertQuery("sensor_time\ttemperature_out\n" +
                        "2022-08-01T00:00:00.000000Z\t0\n" +
                        "2022-07-31T23:59:00.000000Z\t1\n" +
                        "2022-07-31T23:58:00.000000Z\t2\n" +
                        "2022-07-31T23:57:00.000000Z\t3\n" +
                        "2022-07-31T23:56:00.000000Z\t4\n" +
                        "2022-07-31T23:55:00.000000Z\t5\n" +
                        "2022-07-31T23:54:00.000000Z\t6\n" +
                        "2022-07-31T23:53:00.000000Z\t7\n" +
                        "2022-07-31T23:52:00.000000Z\t8\n" +
                        "2022-07-31T23:51:00.000000Z\t9\n",
                "select sensor_time, temperature_out\n" +
                        "from weather_data \n" +
                        "where temperature_out < 10 \n" +
                        "order by sensor_time desc \n",
                DDL,
                "sensor_time###DESC", true, false);
    }

    @Test
    public void testAsyncJitFilterWithPositiveLimitNoOrderBy() throws Exception {
        assertQuery("sensor_time\ttemperature_out\n" +
                        "2022-07-31T07:21:00.000000Z\t999\n" +
                        "2022-07-31T07:22:00.000000Z\t998\n" +
                        "2022-07-31T07:23:00.000000Z\t997\n" +
                        "2022-07-31T07:24:00.000000Z\t996\n" +
                        "2022-07-31T07:25:00.000000Z\t995\n",
                "select sensor_time, temperature_out\n" +
                        "from weather_data \n" +
                        "where temperature_out <= 10000 \n" +
                        "limit 5; ",
                DDL,
                "sensor_time", true, false);
    }

    @Test
    public void testAsyncJitFilterWithPositiveLimitOrderByAsc() throws Exception {
        assertQuery("sensor_time\ttemperature_out\n" +
                        "2022-07-31T07:21:00.000000Z\t999\n" +
                        "2022-07-31T07:22:00.000000Z\t998\n" +
                        "2022-07-31T07:23:00.000000Z\t997\n" +
                        "2022-07-31T07:24:00.000000Z\t996\n" +
                        "2022-07-31T07:25:00.000000Z\t995\n",
                "select sensor_time, temperature_out\n" +
                        "from weather_data \n" +
                        "where temperature_out <= 10000 \n" +
                        "order by sensor_time  asc \n" +
                        "limit 5; ",
                DDL,
                "sensor_time", true, false);
    }

    @Test
    public void testAsyncJitFilterWithPositiveLimitOrderByDesc() throws Exception {
        assertQuery("sensor_time\ttemperature_out\n" +
                        "2022-08-01T00:00:00.000000Z\t0\n" +
                        "2022-07-31T23:59:00.000000Z\t1\n" +
                        "2022-07-31T23:58:00.000000Z\t2\n" +
                        "2022-07-31T23:57:00.000000Z\t3\n" +
                        "2022-07-31T23:56:00.000000Z\t4\n",
                "select sensor_time, temperature_out\n" +
                        "from weather_data \n" +
                        "where temperature_out <= 100000000 \n" +
                        "order by sensor_time desc \n" +
                        "limit 5; ",
                DDL,
                "sensor_time###DESC", true, false);
    }
}
