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

public class MultiplePercentileContDoubleGroupByFunctionFactoryTest extends AbstractCairoTest {

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
    public void testInvalidPercentileInArray1() throws Exception {
        assertException(
                "select percentile_cont(x::double, ARRAY[0.98, 1.1]) from long_sequence(1)",
                39,
                "invalid percentile"
        );
    }

    @Test
    public void testInvalidPercentileInArray2() throws Exception {
        assertException(
                "select percentile_cont(x::double, ARRAY[0.98, -1]) from long_sequence(1)",
                39,
                "invalid percentile"
        );
    }

    @Test
    public void testInvalidPercentileInArrayMixed() throws Exception {
        assertException(
                "select percentile_cont(x::double, ARRAY[0.5, 0.95, 1.5, 0.98]) from long_sequence(1)",
                39,
                "invalid percentile"
        );
    }

    @Test
    public void testMultiplePercentilesAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x long)");
            execute("insert into test values (null), (null), (null)");
            assertSql(
                    "percentile_cont\n" +
                            "null\n",
                    "select percentile_cont(x, ARRAY[0.98, 0.95, 0.90, 0.50]) from test"
            );
        });
    }

    @Test
    public void testMultiplePercentilesAllSameValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select 5.0 x from long_sequence(100))");
            assertSql(
                    "percentile_cont\n[5.0,5.0,5.0,5.0]\n",
                    "select percentile_cont(x, ARRAY[0.98, 0.95, 0.90, 0.50]) from test"
            );
        });
    }

    @Test
    public void testMultiplePercentilesAllZeros() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select 0.0 x from long_sequence(100))");
            assertSql(
                    "percentile_cont\n[0.0,0.0,0.0,0.0]\n",
                    "select percentile_cont(x, ARRAY[0.98, 0.95, 0.90, 0.50]) from test"
            );
        });
    }

    @Test
    public void testMultiplePercentilesDoubleValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) x from long_sequence(100))");
            // For 100 values (indices 0-99):
            // 0.98: position = 0.98 * 99 = 97.02, interpolate between 98.0 and 99.0
            // 0.95: position = 0.95 * 99 = 94.05, interpolate between 95.0 and 96.0
            // 0.90: position = 0.90 * 99 = 89.1, interpolate between 90.0 and 91.0
            // 0.50: position = 0.50 * 99 = 49.5, interpolate between 50.0 and 51.0
            assertSql(
                    "percentile_cont\n[98.02,95.05,90.10000000000001,50.5]\n",
                    "select percentile_cont(x, ARRAY[0.98, 0.95, 0.90, 0.50]) from test"
            );
        });
    }

    @Test
    public void testMultiplePercentilesEmptyArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) x from long_sequence(100))");
            assertSql(
                    "percentile_cont\n[]\n",
                    "select percentile_cont(x, ARRAY[]) from test"
            );
        });
    }

    @Test
    public void testMultiplePercentilesEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x double)");
            assertSql(
                    "percentile_cont\n" +
                            "null\n",
                    "select percentile_cont(x, ARRAY[0.98, 0.95, 0.90, 0.50]) from test"
            );
        });
    }

    @Test
    public void testMultiplePercentilesFloatValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as float) x from long_sequence(100))");
            // Same as double values test
            assertSql(
                    "percentile_cont\n[98.02,95.05,90.10000000000001,50.5]\n",
                    "select percentile_cont(x, ARRAY[0.98, 0.95, 0.90, 0.50]) from test"
            );
        });
    }

    @Test
    public void testMultiplePercentilesIncludingEdgeCases() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) x from long_sequence(100))");
            // 1.0 -> position = 99, returns 100.0
            // 0.98 -> position = 97.02, interpolates
            // 0.0 -> position = 0, returns 1.0
            // 0.50 -> position = 49.5, interpolates
            assertSql(
                    "percentile_cont\n[100.0,98.02,1.0,50.5]\n",
                    "select percentile_cont(x, ARRAY[1.0, 0.98, 0.0, 0.50]) from test"
            );
        });
    }

    @Test
    public void testMultiplePercentilesLargeDataset() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) x from long_sequence(10000))");
            // For 10000 values (indices 0-9999):
            // 0.98: position = 0.98 * 9999 = 9799.02
            // 0.95: position = 0.95 * 9999 = 9499.05
            // 0.90: position = 0.90 * 9999 = 8999.1
            // 0.50: position = 0.50 * 9999 = 4999.5
            assertSql(
                    "percentile_cont\n[9800.02,9500.05,9000.1,5000.5]\n",
                    "select percentile_cont(x, ARRAY[0.98, 0.95, 0.90, 0.50]) from test"
            );
        });
    }

    @Test
    public void testMultiplePercentilesOrdering() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x double)");
            execute("insert into test values (5.0), (1.0), (10.0), (3.0), (7.0)");
            // Sorted: 1, 3, 5, 7, 10
            // For 5 values (indices 0-4):
            // 0.98: position = 0.98 * 4 = 3.92, interpolate between 7.0 and 10.0 = 7.0 + 0.92*3 = 9.76
            // 0.95: position = 0.95 * 4 = 3.8, interpolate between 7.0 and 10.0 = 7.0 + 0.8*3 = 9.4
            // 0.90: position = 0.90 * 4 = 3.6, interpolate between 7.0 and 10.0 = 7.0 + 0.6*3 = 8.8
            // 0.50: position = 0.50 * 4 = 2.0, returns 5.0
            assertSql(
                    "percentile_cont\n" +
                            "[9.76,9.399999999999999,8.8,5.0]\n",
                    "select percentile_cont(x, ARRAY[0.98, 0.95, 0.90, 0.50]) from test"
            );
        });
    }

    @Test
    public void testMultiplePercentilesSinglePercentile() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) x from long_sequence(100))");
            assertSql(
                    "percentile_cont\n[50.5]\n",
                    "select percentile_cont(x, ARRAY[0.50]) from test"
            );
        });
    }

    @Test
    public void testMultiplePercentilesSingleValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x double)");
            execute("insert into test values (42.0)");
            assertSql(
                    "percentile_cont\n" +
                            "[42.0,42.0,42.0,42.0]\n",
                    "select percentile_cont(x, ARRAY[0.98, 0.95, 0.90, 0.50]) from test"
            );
        });
    }

    @Test
    public void testMultiplePercentilesSomeNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x double)");
            execute("insert into test values (1.0), (null), (null), (null)");
            assertSql(
                    "percentile_cont\n" +
                            "[1.0,1.0,1.0,1.0]\n",
                    "select percentile_cont(x, ARRAY[0.98, 0.95, 0.90, 0.50]) from test"
            );
        });
    }

    @Test
    public void testMultiplePercentilesTwoValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x double)");
            execute("insert into test values (10.0), (20.0)");
            // Sorted: 10.0, 20.0
            // For 2 values (indices 0-1):
            // 0.98: position = 0.98 * 1 = 0.98, interpolate 10 + 0.98*10 = 19.8
            // 0.95: position = 0.95 * 1 = 0.95, interpolate 10 + 0.95*10 = 19.5
            // 0.90: position = 0.90 * 1 = 0.90, interpolate 10 + 0.90*10 = 19.0
            // 0.50: position = 0.50 * 1 = 0.50, interpolate 10 + 0.50*10 = 15.0
            assertSql(
                    "percentile_cont\n" +
                            "[19.8,19.5,19.0,15.0]\n",
                    "select percentile_cont(x, ARRAY[0.98, 0.95, 0.90, 0.50]) from test"
            );
        });
    }

    @Test
    public void testMultiplePercentilesWithDuplicates() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x double)");
            execute("insert into test values (1.0), (1.0), (2.0), (2.0), (3.0), (3.0)");
            // Sorted: 1.0, 1.0, 2.0, 2.0, 3.0, 3.0
            // For 6 values (indices 0-5):
            // 0.98: position = 0.98 * 5 = 4.9, interpolate between 3.0 and 3.0 = 3.0
            // 0.95: position = 0.95 * 5 = 4.75, interpolate between 3.0 and 3.0 = 3.0
            // 0.90: position = 0.90 * 5 = 4.5, interpolate between 3.0 and 3.0 = 3.0
            // 0.50: position = 0.50 * 5 = 2.5, interpolate between 2.0 and 2.0 = 2.0
            assertSql(
                    "percentile_cont\n" +
                            "[3.0,3.0,3.0,2.0]\n",
                    "select percentile_cont(x, ARRAY[0.98, 0.95, 0.90, 0.50]) from test"
            );
        });
    }

    @Test
    public void testMultiplePercentilesWithGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (category symbol, value double)");
            execute("insert into test values ('A', 1.0), ('A', 2.0), ('A', 3.0), ('B', 10.0), ('B', 20.0), ('B', 30.0)");
            // For each group with 3 values (indices 0-2):
            // 0.98: position = 0.98 * 2 = 1.96
            // 0.95: position = 0.95 * 2 = 1.9
            // 0.90: position = 0.90 * 2 = 1.8
            // 0.50: position = 0.50 * 2 = 1.0
            // Group A: interpolate between 2.0 and 3.0
            // Group B: interpolate between 20.0 and 30.0
            assertSql(
                    "category\tpercentile_cont\n" +
                            "A\t[2.96,2.9,2.8,2.0]\n" +
                            "B\t[29.6,29.0,28.0,20.0]\n",
                    "select category, percentile_cont(value, ARRAY[0.98, 0.95, 0.90, 0.50]) from test order by category"
            );
        });
    }

    @Test
    public void testMultiplePercentilesWithKnownData() throws Exception {
        assertMemoryLeak(() -> {
            execute(txDdl);
            execute(txDml);
            // For 50 values (indices 0-49):
            // 0.98: position = 48.02, interpolate between indices 48 and 49
            // 0.95: position = 46.55, interpolate between indices 46 and 47
            // 0.90: position = 44.10, interpolate between indices 44 and 45
            // 0.50: position = 24.50, interpolate between indices 24 and 25
            assertSql("percentile_cont\n" +
                            "[415.8181599999982,268.20624999999984,225.85070000000002,64.8365]\n",
                    "select percentile_cont(value, ARRAY[0.98, 0.95, 0.90, 0.50]) from tx_traffic");
        });
    }

    @Test
    public void testMultiplePercentilesWithNegativeValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x double)");
            execute("insert into test values (1.0), (-1.0), (5.0), (-5.0), (10.0), (-10.0)");
            // Sorted: -10.0, -5.0, -1.0, 1.0, 5.0, 10.0
            // For 6 values (indices 0-5):
            // 0.98: position = 0.98 * 5 = 4.9, interpolate between 5.0 and 10.0 = 5 + 0.9*5 = 9.5
            // 0.95: position = 0.95 * 5 = 4.75, interpolate between 5.0 and 10.0 = 5 + 0.75*5 = 8.75
            // 0.90: position = 0.90 * 5 = 4.5, interpolate between 5.0 and 10.0 = 5 + 0.5*5 = 7.5
            // 0.50: position = 0.50 * 5 = 2.5, interpolate between -1.0 and 1.0 = -1 + 0.5*2 = 0.0
            assertSql(
                    "percentile_cont\n" +
                            "[9.500000000000002,8.75,7.5,0.0]\n",
                    "select percentile_cont(x, ARRAY[0.98, 0.95, 0.90, 0.50]) from test"
            );
        });
    }

    @Test
    public void testMultiplePercentilesWithPrecision() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) x from long_sequence(1000))");
            // For 1000 values (indices 0-999):
            // 0.98: position = 0.98 * 999 = 979.02
            // 0.95: position = 0.95 * 999 = 949.05
            // 0.90: position = 0.90 * 999 = 899.1
            // 0.50: position = 0.50 * 999 = 499.5
            assertSql(
                    "percentile_cont\n[980.02,950.05,900.1,500.5]\n",
                    "select percentile_cont(x, ARRAY[0.98, 0.95, 0.90, 0.50]) from test"
            );
        });
    }

    @Test
    public void testCompareDiscVsCont() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x double)");
            execute("insert into test values (1.0), (2.0), (3.0), (4.0)");
            // For 4 values:
            // percentile_cont at 0.5: position = 1.5, interpolates to 2.5
            // percentile_disc at 0.5: ceil(4*0.5)-1 = 1, returns 2.0
            assertSql(
                    "percentile_cont\tpercentile_disc\n" +
                            "[2.5,2.5]\t[2.0,2.0]\n",
                    "select percentile_cont(x, ARRAY[0.5, 0.5]), percentile_disc(x, ARRAY[0.5, 0.5]) from test"
            );
        });
    }

    @Test
    public void testWithKnownData() throws Exception {
        assertMemoryLeak(() -> {
            execute(txDdl);
            execute(txDml);
            assertSql("percentile_cont\n" +
                            "[415.8181599999982,268.20624999999984,225.85070000000002,64.8365]\n",
                    "select percentile_cont(value, ARRAY[0.98, 0.95, 0.90, 0.50]) from tx_traffic");
        });
    }
}
