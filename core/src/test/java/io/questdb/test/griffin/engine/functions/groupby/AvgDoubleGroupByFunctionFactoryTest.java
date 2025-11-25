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
                """
                        max	avg	weighted_avg	sum	stddev_samp	weighted_stddev_samp
                        10	5.5	7.0	55	3.0276503540974917	2.6220221204253784
                        """, """
                        SELECT
                            max(x),
                            avg(x),
                            weighted_avg(x, x),
                            sum(x),
                            stddev_samp(x),
                            weighted_stddev_samp(x, x)
                        FROM long_sequence(10)
                        """
        ));
    }

    @Test
    public void testAllWithInfinity() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test2 as(select case  when rnd_double() > 0.6 then 1.0   else 0.0  end val from long_sequence(100));");
            assertSql(
                    """
                            sum\tavg\tmax\tmin\tksum\tnsum\tstddev_samp
                            44.0\t1.0\t1.0\t1.0\t44.0\t44.0\t0.0
                            """,
                    "select sum(1/val) , avg(1/val), max(1/val), min(1/val), ksum(1/val), nsum(1/val), stddev_samp(1/val) from test2"
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

            assertQuery("""
                            ts\tmin\tmax\tavg\tstddev_samp
                            2020-01-01T10:00:00.000000Z\t1\t1\t1.0\tnull
                            2020-01-01T11:00:00.000000Z\t2\t2\t2.0\tnull
                            2020-01-01T12:00:00.000000Z\t3\t3\t3.0\tnull
                            2020-01-01T13:00:00.000000Z\t4\t4\t4.0\tnull
                            2020-01-01T14:00:00.000000Z\t5\t5\t5.0\tnull
                            """,
                    """
                            select ts, min(price) min, max(price) max, avg(price) avg, stddev_samp(price) stddev_samp
                            from fill_options
                            sample by 1h
                            fill(linear);""",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testWeightedAvg() throws Exception {
        assertMemoryLeak(() -> {
            assertSql(
                    """
                            weighted_avg	sum_over_sum
                            7.0	7
                            """, """
                            SELECT
                                weighted_avg(x, x),
                                sum(x * x) / sum(x) sum_over_sum
                            FROM long_sequence(10)
                            """
            );
            assertSql(
                    """
                            weighted_avg
                            4.999999999999999
                            """,
                    "SELECT weighted_avg(5.0, rnd_double()) FROM long_sequence(10)\n"
            );
        });
    }

    @Test
    public void testWeightedAvgZeroWeight() throws Exception {
        assertMemoryLeak(() -> {
            assertSql(
                    """
                            weighted_avg
                            null
                            """,
                    "SELECT weighted_avg(x, 0) FROM long_sequence(10)"
            );
            assertSql(
                    """
                            weighted_avg
                            7.0
                            """,
                    """
                            SELECT weighted_avg(
                                x,
                                CASE WHEN x = 7 THEN 1 ELSE 0 END
                            ) FROM long_sequence(10)
                            """
            );
            assertSql(
                    """
                            weighted_avg
                            null
                            """,
                    "SELECT weighted_avg(x, x - 6) FROM long_sequence(11)"
            );
        });
    }

    @Test
    public void testWeightedStddevZero() throws Exception {
        assertMemoryLeak(() -> {
            assertSql(
                    """
                            weighted_stddev_samp
                            0.0
                            """, "SELECT weighted_stddev_samp(1, 1) FROM long_sequence(10)"
            );
            assertSql(
                    """
                            weighted_stddev_samp
                            0.0
                            """, "SELECT weighted_stddev_samp(1, 1) FROM long_sequence(10)"
            );
        });
    }
}
