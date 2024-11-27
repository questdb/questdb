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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class AvgDoubleGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testAll() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "max\tavg\tsum\tstddev_samp\n" +
                        "10\t5.5\t55\t3.0276503540974917\n", "select max(x), avg(x), sum(x), stddev_samp(x) from long_sequence(10)"
        ));
    }

    @Test
    public void testAllWithInfinity() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test2 as(select case  when rnd_double() > 0.6 then 1.0   else 0.0  end val from long_sequence(100));");
            assertSql(
                    "sum\tavg\tmax\tmin\tksum\tnsum\tstddev_samp\n" +
                            "44.0\t1.0\tnull\t1.0\t44.0\t44.0\t0.0\n", "select sum(1/val) , avg(1/val), max(1/val), min(1/val), ksum(1/val), nsum(1/val), stddev_samp(1/val) from test2"
            );
        });
    }

    @Test
    public void testAvgWithInfinity() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test2 as(select case  when rnd_double() > 0.6 then 1.0   else 0.0  end val from long_sequence(100));");
            assertSql(
                    "avg\n1.0\n", "select avg(1/val) from test2"
            );
        });
    }

    @Test
    public void testInterpolatedAvg() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table fill_options(ts timestamp, price int) timestamp(ts);");
            execute("insert into fill_options values(to_timestamp('2020-01-01:10:00:00', 'yyyy-MM-dd:HH:mm:ss'), 1);");
            execute("insert into fill_options values(to_timestamp('2020-01-01:11:00:00', 'yyyy-MM-dd:HH:mm:ss'), 2);");
            execute("insert into fill_options values(to_timestamp('2020-01-01:12:00:00', 'yyyy-MM-dd:HH:mm:ss'), 3);");
            execute("insert into fill_options values(to_timestamp('2020-01-01:14:00:00', 'yyyy-MM-dd:HH:mm:ss'), 5);");

            assertQuery("ts\tmin\tmax\tavg\tstddev_samp\n" +
                            "2020-01-01T10:00:00.000000Z\t1\t1\t1.0\tnull\n" +
                            "2020-01-01T11:00:00.000000Z\t2\t2\t2.0\tnull\n" +
                            "2020-01-01T12:00:00.000000Z\t3\t3\t3.0\tnull\n" +
                            "2020-01-01T13:00:00.000000Z\t4\t4\t4.0\tnull\n" +
                            "2020-01-01T14:00:00.000000Z\t5\t5\t5.0\tnull\n",
                    "select ts, min(price) min, max(price) max, avg(price) avg, stddev_samp(price) stddev_samp\n" +
                            "from fill_options\n" +
                            "sample by 1h\n" +
                            "fill(linear);",
                    "ts",
                    true,
                    true
            );
        });
    }
}