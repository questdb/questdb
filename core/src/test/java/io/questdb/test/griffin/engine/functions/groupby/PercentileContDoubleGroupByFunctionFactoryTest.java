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

public class PercentileContDoubleGroupByFunctionFactoryTest extends AbstractCairoTest {

    private static final String txDdl = """
            CREATE TABLE tx_traffic (timestamp TIMESTAMP, value DOUBLE);
            """;

    private static final String txDml = """
            INSERT INTO tx_traffic (timestamp, value) VALUES
            ('2022-05-02T14:51:28Z', '10.978'),
            ('2022-05-02T14:51:58Z', '85.063'),
            ('2022-05-02T14:52:28Z', '148.83'),
            ('2022-05-02T14:52:58Z', '135.881'),
            ('2022-05-02T14:53:28Z', '47.674'),
            ('2022-05-02T14:53:58Z', '61.336'),
            ('2022-05-02T14:54:28Z', '9.651'),
            ('2022-05-02T14:54:58Z', '63.451'),
            ('2022-05-02T14:55:28Z', '8.618'),
            ('2022-05-02T14:55:58Z', '61.774'),
            ('2022-05-02T14:56:28Z', '8.1'),
            ('2022-05-02T14:56:58Z', '177.434'),
            ('2022-05-02T14:57:28Z', '12.955'),
            ('2022-05-02T14:57:58Z', '62.164'),
            ('2022-05-02T14:58:28Z', '161.288'),
            ('2022-05-02T14:58:58Z', '63.363'),
            ('2022-05-02T14:59:28Z', '10.228'),
            ('2022-05-02T14:59:58Z', '60.801'),
            ('2022-05-02T15:00:28Z', '14.053'),
            ('2022-05-02T15:00:58Z', '200.455'),
            ('2022-05-02T15:01:28Z', '406.977'),
            ('2022-05-02T15:01:58Z', '849.035'),
            ('2022-05-02T15:02:28Z', '14.674'),
            ('2022-05-02T15:02:58Z', '126.668'),
            ('2022-05-02T15:03:28Z', '217.782'),
            ('2022-05-02T15:03:58Z', '66.75'),
            ('2022-05-02T15:04:28Z', '39.856'),
            ('2022-05-02T15:04:58Z', '289.615'),
            ('2022-05-02T15:05:28Z', '182.917'),
            ('2022-05-02T15:05:58Z', '219.222'),
            ('2022-05-02T15:06:28Z', '125.86'),
            ('2022-05-02T15:06:58Z', '87.316'),
            ('2022-05-02T15:07:28Z', '191.085'),
            ('2022-05-02T15:07:58Z', '242.04'),
            ('2022-05-02T15:08:28Z', '224.195'),
            ('2022-05-02T15:08:58Z', '240.752'),
            ('2022-05-02T15:09:28Z', '70.349'),
            ('2022-05-02T15:09:58Z', '177.227'),
            ('2022-05-02T15:10:28Z', '184.439'),
            ('2022-05-02T15:10:58Z', '118.329'),
            ('2022-05-02T15:11:28Z', '8.316'),
            ('2022-05-02T15:11:58Z', '63.863'),
            ('2022-05-02T15:12:28Z', '13.639'),
            ('2022-05-02T15:12:58Z', '60.102'),
            ('2022-05-02T15:13:28Z', '47.509'),
            ('2022-05-02T15:13:58Z', '62.541'),
            ('2022-05-02T15:14:28Z', '11.976'),
            ('2022-05-02T15:14:58Z', '59.689'),
            ('2022-05-02T15:15:28Z', '8.939'),
            ('2022-05-02T15:15:58Z', '65.81');
            """;

    @Test
    public void test0thPercentileDoubleValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) x from long_sequence(100))");
            assertSql(
                    "percentile_cont\n1.0\n",
                    "select percentile_cont(x, 0) from test"
            );
        });
    }

    @Test
    public void test100thPercentileDoubleValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) x from long_sequence(100))");
            assertSql(
                    "percentile_cont\n100.0\n",
                    "select percentile_cont(x, 1.0) from test"
            );
        });
    }

    @Test
    public void test25thPercentileInterpolation() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x double)");
            execute("insert into test values (1.0), (2.0), (3.0), (4.0)");
            assertSql(
                    "percentile_cont\n1.75\n",
                    "select percentile_cont(x, 0.25) from test"
            );
        });
    }

    @Test
    public void test50thPercentileDoubleValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) x from long_sequence(100))");
            assertSql(
                    "percentile_cont\n50.5\n",
                    "select percentile_cont(x, 0.5) from test"
            );
        });
    }

    @Test
    public void test50thPercentileFloatValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as float) x from long_sequence(100))");
            assertSql(
                    "percentile_cont\n50.5\n",
                    "select percentile_cont(x, 0.5) from test"
            );
        });
    }

    @Test
    public void test50thPercentileWithPrecision() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) x from long_sequence(1000))");
            assertSql(
                    "percentile_cont\n500.5\n",
                    "select percentile_cont(x, 0.5) from test"
            );
        });
    }

    @Test
    public void test75thPercentileInterpolation() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x double)");
            execute("insert into test values (1.0), (2.0), (3.0), (4.0)");
            assertSql(
                    "percentile_cont\n" +
                            "3.25\n",
                    "select percentile_cont(x, 0.75) from test"
            );
        });
    }

    @Test
    public void testCompareDiscVsCont() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x double)");
            execute("insert into test values (1.0), (2.0), (3.0), (4.0), (5.0)");
            assertSql(
                    "percentile_cont\tpercentile_disc\n" +
                            "3.0\t3.0\n",
                    "select percentile_cont(x, 0.5), percentile_disc(x, 0.5) from test"
            );
        });
    }

    @Test
    public void testCompareDiscVsContInterpolation() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x double)");
            execute("insert into test values (1.0), (2.0), (3.0), (4.0)");
            // For 4 values (indices 0-3):
            // percentile_cont(0.5): position = 0.5 * 3 = 1.5, interpolates between index 1 (2.0) and 2 (3.0) = 2.5
            // percentile_disc(0.5): ceil(4 * 0.5) - 1 = 1, returns value at index 1 = 2.0
            assertSql(
                    "percentile_cont\tpercentile_disc\n" +
                            "2.5\t2.0\n",
                    "select percentile_cont(x, 0.5), percentile_disc(x, 0.5) from test"
            );
        });
    }

    @Test
    public void testInterpolationWithTwoValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x double)");
            execute("insert into test values (10.0), (20.0)");
            assertSql(
                    "percentile_cont\n15.0\n",
                    "select percentile_cont(x, 0.5) from test"
            );
        });
    }

    @Test
    public void testInvalidPercentile1() throws Exception {
        assertException(
                "select percentile_cont(x::double, 1.1) from long_sequence(1)",
                34,
                "invalid percentile"
        );
    }

    @Test
    public void testInvalidPercentile2() throws Exception {
        assertException(
                "select percentile_cont(x::double, -1.1) from long_sequence(1)",
                34,
                "invalid percentile"
        );
    }


    @Test
    public void testNegativeValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x double)");
            execute("insert into test values (1.0), (-1.0)");
            assertSql(
                    "percentile_cont\n" +
                            "0.0\n",
                    "select percentile_cont(x, 0.5) from test"
            );
        });
    }

    @Test
    public void testPercentileAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x long)");
            execute("insert into test values (null), (null), (null)");
            assertSql(
                    "percentile_cont\n" +
                            "null\n",
                    "select percentile_cont(x, 0.5) from test"
            );
        });
    }

    @Test
    public void testPercentileAllSameValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select 5.0 x from long_sequence(100))");
            assertSql(
                    "percentile_cont\n5.0\n",
                    "select percentile_cont(x, 0.5) from test"
            );
        });
    }

    @Test
    public void testPercentileContGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (" +
                    "select x % 2 as category, cast(x as double) as value from long_sequence(10)" +
                    ")");
            assertSql(
                    "category\tpercentile_cont\n" +
                            "0\t6.0\n" +
                            "1\t5.0\n",
                    "select category, percentile_cont(value, 0.5) from test group by category order by category"
            );
        });
    }

    @Test
    public void testPercentileContGroupByWithInterpolation() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (" +
                    "select x % 2 as category, cast(x as double) as value from long_sequence(10)" +
                    ")");
            // cat=0: 2, 4, 6, 8, 10 → 75th percentile position = 0.75 * 4 = 3.0 → index 3 → 8.0
            // cat=1: 1, 3, 5, 7, 9 → 75th percentile position = 0.75 * 4 = 3.0 → index 3 → 7.0
            assertSql(
                    "category\tpercentile_cont\n" +
                            "0\t8.0\n" +
                            "1\t7.0\n",
                    "select category, percentile_cont(value, 0.75) from test group by category order by category"
            );
        });
    }


    @Test
    public void testPercentileEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x double)");
            assertSql(
                    "percentile_cont\n" +
                            "null\n",
                    "select percentile_cont(x, 0.5) from test"
            );
        });
    }

    @Test
    public void testPercentileSomeNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x double)");
            execute("insert into test values (1.0), (null), (null), (null)");
            assertSql(
                    "percentile_cont\n" +
                            "1.0\n",
                    "select percentile_cont(x, 0.5) from test"
            );
        });
    }

    @Test
    public void testPercentileWithPercentileBindVariable() throws Exception {
        bindVariableService.setDouble(0, 0.5);
        assertMemoryLeak(() -> {
            execute("create table test as (select 5.0 x from long_sequence(100))");
            assertSql(
                    "percentile_cont\n5.0\n",
                    "select percentile_cont(x, $1) from test"
            );
        });
    }

    @Test
    public void testSingleValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x double)");
            execute("insert into test values (42.0)");
            assertSql(
                    "percentile_cont\n42.0\n",
                    "select percentile_cont(x, 0.5) from test"
            );
        });
    }

    @Test
    public void testWithKnownData() throws Exception {
        assertMemoryLeak(() -> {
            execute(txDdl);
            execute(txDml);
            assertSql("percentile_cont\n" +
                            "268.20624999999984\n",
                    "select percentile_cont(value, 0.95) from tx_traffic");
        });
    }

    @Test
    public void testNegativePercentile() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) x from long_sequence(100))");
            // -0.95 should be equivalent to 1 - 0.95 = 0.05 (5th percentile)
            // position = 0.05 * (100 - 1) = 4.95, interpolating between indices 4 and 5 (values 5 and 6)
            assertSql(
                    "percentile_cont\n5.950000000000005\n",
                    "select percentile_cont(x, -0.95) from test"
            );
        });
    }

    @Test
    public void testNegativePercentileEqualsPositive() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) x from long_sequence(100))");
            // -0.5 should be equivalent to 1 - 0.5 = 0.5
            assertSql(
                    "percentile_cont\n50.5\n",
                    "select percentile_cont(x, -0.5) from test"
            );
        });
    }

    @Test
    public void testNegativeOnePercentile() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) x from long_sequence(100))");
            // -1.0 should be equivalent to 1 - 1.0 = 0.0 (0th percentile = minimum)
            assertSql(
                    "percentile_cont\n1.0\n",
                    "select percentile_cont(x, -1.0) from test"
            );
        });
    }


}
