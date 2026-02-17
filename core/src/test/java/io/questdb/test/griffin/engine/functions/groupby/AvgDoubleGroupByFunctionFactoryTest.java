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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class AvgDoubleGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testAll() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        max	avg	weighted_avg	sum	stddev_samp	weighted_stddev
                        10	5.5	7.0	55	3.0276503540974917	2.6220221204253784
                        """, """
                        SELECT
                            max(x),
                            avg(x),
                            weighted_avg(x, x),
                            sum(x),
                            stddev_samp(x),
                            weighted_stddev(x, x)
                        FROM long_sequence(10)
                        """
        ));
    }

    @Test
    public void testAllWithEmptyAndNulls() throws Exception {
        assertMemoryLeak(() -> {
            String expected = """
                    sum	avg	weighted_avg	max	min	ksum	nsum	stddev_samp	weighted_stddev_rel	weighted_stddev_freq
                    null	null	null			null	null	null	null	null
                    """;
            String sql = """
                    SELECT
                        sum(NULL),
                        avg(NULL),
                        weighted_avg(NULL, x),
                        max(NULL),
                        min(NULL),
                        ksum(NULL),
                        nsum(NULL),
                        stddev_samp(NULL),
                        weighted_stddev_rel(NULL, x),
                        weighted_stddev_freq(NULL, x)
                    FROM long_sequence(""";
            assertSql(expected, sql + "0)");
            assertSql(expected, sql + "1)");
            assertSql(expected, sql + "2)");
        });
    }

    @Test
    public void testAllWithInfinity() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE test2 AS (
                        SELECT CASE
                            WHEN rnd_double() > 0.6 then 1.0
                            ELSE 0.0
                        END val FROM long_sequence(100)
                    )""");
            assertSql(
                    """
                            sum	avg	weighted_avg	max	min	ksum	nsum	stddev_samp	weighted_stddev
                            44.0	1.0	1.0	1.0	1.0	44.0	44.0	0.0	0.0
                            """,
                    """
                            SELECT
                                sum(1/val),
                                avg(1/val),
                                weighted_avg(1/val, val),
                                max(1/val),
                                min(1/val),
                                ksum(1/val),
                                nsum(1/val),
                                stddev_samp(1/val),
                                weighted_stddev(1/val, val)
                            FROM test2
                            """
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
                            7.0	7.0
                            """, """
                            SELECT
                                weighted_avg(x, x),
                                sum(x * x) / sum(x)::double sum_over_sum
                            FROM long_sequence(10)
                            """
            );
            assertSql(
                    """
                            weighted_avg
                            5.0
                            """,
                    "SELECT round(weighted_avg(5.0, rnd_double()), 10) weighted_avg FROM long_sequence(10)\n"
            );
        });
    }

    @Test
    public void testWeightedAvgSampleByFill() throws Exception {
        assertMemoryLeak(() -> {
            // Create table with gaps: data at hour 0 and hour 2, gap at hour 1
            execute("CREATE TABLE test_fill (value DOUBLE, weight DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            // Hour 0: values 1,2,3 with weight 1 -> avg == 2
            execute("""
                    INSERT INTO test_fill VALUES
                        (1.0, 1.0, '1970-01-01T00:00'),
                        (2.0, 1.0, '1970-01-01T00:30'),
                        (3.0, 1.0, '1970-01-01T00:45')
                    """);
            // Hour 1: no data (gap to be filled)
            // Hour 2: values 1,2,3,4,5 with weight 1 -> avg == 3
            execute("""
                    INSERT INTO test_fill VALUES
                        (1.0, 1.0, '1970-01-01T02:00'),
                        (2.0, 1.0, '1970-01-01T02:15'),
                        (3.0, 1.0, '1970-01-01T02:30'),
                        (4.0, 1.0, '1970-01-01T02:45'),
                        (5.0, 1.0, '1970-01-01T02:50')
                    """);

            // Get the actual avg values for hour 0 and hour 2
            String hour0Avg = queryResult("SELECT round(weighted_avg(value, weight), 8) FROM test_fill WHERE ts < '1970-01-01T01'");
            String hour2Avg = queryResult("SELECT round(weighted_avg(value, weight), 8) FROM test_fill WHERE ts >= '1970-01-01T02'");

            double avg0 = Double.parseDouble(hour0Avg.trim().split("\n")[1]);
            double avg2 = Double.parseDouble(hour2Avg.trim().split("\n")[1]);

            assertSql(
                    "ts\tround\n" +
                            "1970-01-01T00:00:00.000000Z\t" + avg0 + "\n" +
                            "1970-01-01T01:00:00.000000Z\t" + avg0 + "\n" +
                            "1970-01-01T02:00:00.000000Z\t" + avg2 + "\n",
                    "SELECT ts, round(weighted_avg(value, weight), 8) FROM test_fill SAMPLE BY 1h FILL(PREV)"
            );
            assertSql(
                    "ts\tround\n" +
                            "1970-01-01T00:00:00.000000Z\t" + avg0 + "\n" +
                            "1970-01-01T01:00:00.000000Z\tnull\n" +
                            "1970-01-01T02:00:00.000000Z\t" + avg2 + "\n",
                    "SELECT ts, round(weighted_avg(value, weight), 8) FROM test_fill SAMPLE BY 1h FILL(NULL)"
            );
        });
    }

    @Test
    public void testWeightedAvgZeroAndNegativeWeight() throws Exception {
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
            // Weights sum to zero
            assertSql(
                    """
                            weighted_avg
                            null
                            """,
                    "SELECT weighted_avg(x, x - 6) FROM long_sequence(11)"
            );
            // Weights all negative
            assertSql(
                    """
                            weighted_avg
                            4.333333333333333
                            """,
                    "SELECT weighted_avg(x, x - 12) FROM long_sequence(11)"
            );
        });
    }

    @Test
    public void testWeightedStddevAgainstFormula() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (sym SYMBOL, value DOUBLE, weight DOUBLE)");
            execute("""
                    INSERT INTO tango SELECT
                        rnd_symbol('A', 'B', 'C', 'D'),
                        8 + 4 * rnd_double(),
                        rnd_double()
                    FROM long_sequence(1000)
                    """);
            assertSql("""
                    sym	function	formula
                    A	1.1832	1.1832
                    D	1.1332	1.1332
                    B	1.1528	1.1528
                    C	1.2291	1.2291
                    """, """
                    SELECT
                        sym,
                        round(weighted_stddev_rel(value, weight), 4) AS function,
                        round(sqrt(
                          (
                            sum(weight * value * value)
                            - (sum(weight * value) * sum(weight * value) / sum(weight))
                          )
                          / (sum(weight) - sum(weight * weight) / sum(weight))
                        ), 4) AS formula
                    FROM tango
                    """);
            assertSql("""
                    sym	function	formula
                    A	1.1845	1.1845
                    D	1.1346	1.1346
                    B	1.1544	1.1544
                    C	1.2309	1.2309
                    """, """
                    SELECT
                        sym,
                        round(weighted_stddev_freq(value, weight), 4) AS function,
                        round(sqrt(
                          (
                            sum(weight * value * value)
                            - (sum(weight * value) * sum(weight * value) / sum(weight))
                          )
                          / (sum(weight) - 1)
                        ), 4) AS formula
                    FROM tango
                    """);
        });
    }

    @Test
    public void testWeightedStddevEqualSamples() throws Exception {
        assertMemoryLeak(() -> {
            assertSql(
                    """
                            weighted_stddev_freq
                            0.0
                            """, "SELECT weighted_stddev_freq(1, 1) FROM long_sequence(10)"
            );
            assertSql(
                    """
                            weighted_stddev_rel
                            0.0
                            """, "SELECT weighted_stddev_rel(1, 1) FROM long_sequence(10)"
            );
        });
    }

    private String queryResult(String query) throws Exception {
        StringSink sink = new StringSink();
        TestUtils.printSql(engine, sqlExecutionContext, query, sink);
        return sink.toString();
    }
}
