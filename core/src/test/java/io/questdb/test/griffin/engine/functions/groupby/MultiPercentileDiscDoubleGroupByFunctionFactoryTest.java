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

public class MultiPercentileDiscDoubleGroupByFunctionFactoryTest extends AbstractCairoTest {

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
                "select percentile_disc(x::double, ARRAY[0.98, 1.1]) from long_sequence(1)",
                39,
                "invalid percentile"
        );
    }

    @Test
    public void testInvalidPercentileInArray2() throws Exception {
        assertException(
                "select percentile_disc(x::double, ARRAY[0.98, -1]) from long_sequence(1)",
                39,
                "invalid percentile"
        );
    }

    @Test
    public void testInvalidPercentileInArrayMixed() throws Exception {
        assertException(
                "select percentile_disc(x::double, ARRAY[0.5, 0.95, 1.5, 0.98]) from long_sequence(1)",
                39,
                "invalid percentile"
        );
    }

    @Test
    public void testMultiPercentileDiscGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (" +
                    "select x % 2 as category, cast(x as double) as value from long_sequence(10)" +
                    ")");
            assertSql(
                    "category\tpercentile_disc\n" +
                            "0\t[4.0,6.0,8.0]\n" +
                            "1\t[3.0,5.0,7.0]\n",
                    "select category, percentile_disc(value, ARRAY[0.25, 0.5, 0.75]) from test group by category order by category"
            );
        });
    }

    @Test
    public void testMultiPercentileDiscGroupByWithAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (" +
                    "select 1 as category, null::double as value from long_sequence(5)" +
                    ")");
            assertSql(
                    "category\tpercentile_disc\n" +
                            "1\tnull\n",
                    "select category, percentile_disc(value, ARRAY[0.25, 0.5, 0.75]) from test group by category"
            );
        });
    }

    @Test
    public void testMultiPercentileDiscGroupByWithExtremePercentiles() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (" +
                    "select x % 2 as category, cast(x as double) as value from long_sequence(10)" +
                    ")");
            // cat=0: 2, 4, 6, 8, 10 → [0, 0.5, 1.0] → [2.0, 6.0, 10.0]
            // cat=1: 1, 3, 5, 7, 9 → [0, 0.5, 1.0] → [1.0, 5.0, 9.0]
            assertSql(
                    "category\tpercentile_disc\n" +
                            "0\t[2.0,6.0,10.0]\n" +
                            "1\t[1.0,5.0,9.0]\n",
                    "select category, percentile_disc(value, ARRAY[0.0, 0.5, 1.0]) from test group by category order by category"
            );
        });
    }

    @Test
    public void testMultiPercentileDiscGroupByWithMultipleColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (" +
                    "select x % 2 as cat1, x % 3 as cat2, cast(x as double) as value " +
                    "from long_sequence(12)" +
                    ")");
            // cat1=0, cat2=0: values 6, 12 (2 values)
            //   25th: ceil(2*0.25)-1 = 0 → 6.0
            //   50th: ceil(2*0.50)-1 = 0 → 6.0
            //   75th: ceil(2*0.75)-1 = 1 → 12.0
            // cat1=0, cat2=1: values 4, 10 (2 values)
            //   25th: ceil(2*0.25)-1 = 0 → 4.0
            //   50th: ceil(2*0.50)-1 = 0 → 4.0
            //   75th: ceil(2*0.75)-1 = 1 → 10.0
            // cat1=0, cat2=2: values 2, 8 (2 values)
            //   25th: ceil(2*0.25)-1 = 0 → 2.0
            //   50th: ceil(2*0.50)-1 = 0 → 2.0
            //   75th: ceil(2*0.75)-1 = 1 → 8.0
            // cat1=1, cat2=0: values 3, 9 (2 values)
            //   25th: ceil(2*0.25)-1 = 0 → 3.0
            //   50th: ceil(2*0.50)-1 = 0 → 3.0
            //   75th: ceil(2*0.75)-1 = 1 → 9.0
            // cat1=1, cat2=1: values 1, 7 (2 values)
            //   25th: ceil(2*0.25)-1 = 0 → 1.0
            //   50th: ceil(2*0.50)-1 = 0 → 1.0
            //   75th: ceil(2*0.75)-1 = 1 → 7.0
            // cat1=1, cat2=2: values 5, 11 (2 values)
            //   25th: ceil(2*0.25)-1 = 0 → 5.0
            //   50th: ceil(2*0.50)-1 = 0 → 5.0
            //   75th: ceil(2*0.75)-1 = 1 → 11.0
            assertSql(
                    "cat1\tcat2\tpercentile_disc\n" +
                            "0\t0\t[6.0,6.0,12.0]\n" +
                            "0\t1\t[4.0,4.0,10.0]\n" +
                            "0\t2\t[2.0,2.0,8.0]\n" +
                            "1\t0\t[3.0,3.0,9.0]\n" +
                            "1\t1\t[1.0,1.0,7.0]\n" +
                            "1\t2\t[5.0,5.0,11.0]\n",
                    "select cat1, cat2, percentile_disc(value, ARRAY[0.25, 0.5, 0.75]) " +
                            "from test group by cat1, cat2 order by cat1, cat2"
            );
        });
    }

    @Test
    public void testMultiPercentileDiscGroupByWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (" +
                    "select x % 2 as category, " +
                    "case when x % 4 = 0 then null else cast(x as double) end as value " +
                    "from long_sequence(10)" +
                    ")");
            // cat=0: 2, null, 6, null, 10 → non-null: 2, 6, 10 (3 values)
            //   25th: ceil(3*0.25)-1 = 0 → 2.0
            //   50th: ceil(3*0.50)-1 = 1 → 6.0
            //   75th: ceil(3*0.75)-1 = 2 → 10.0
            // cat=1: 1, 3, 5, 7, 9 (5 values)
            //   25th: ceil(5*0.25)-1 = 1 → 3.0
            //   50th: ceil(5*0.50)-1 = 2 → 5.0
            //   75th: ceil(5*0.75)-1 = 3 → 7.0
            assertSql(
                    "category\tpercentile_disc\n" +
                            "0\t[2.0,6.0,10.0]\n" +
                            "1\t[3.0,5.0,7.0]\n",
                    "select category, percentile_disc(value, ARRAY[0.25, 0.5, 0.75]) from test group by category order by category"
            );
        });
    }

    @Test
    public void testMultiPercentileDiscGroupByWithSingleElement() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (" +
                    "select x, cast(x as double) as value from long_sequence(5)" +
                    ")");
            // Each group has one value, all percentiles should return that value
            assertSql(
                    "x\tpercentile_disc\n" +
                            "1\t[1.0,1.0,1.0]\n" +
                            "2\t[2.0,2.0,2.0]\n" +
                            "3\t[3.0,3.0,3.0]\n" +
                            "4\t[4.0,4.0,4.0]\n" +
                            "5\t[5.0,5.0,5.0]\n",
                    "select x, percentile_disc(value, ARRAY[0.25, 0.5, 0.75]) from test group by x order by x"
            );
        });
    }

    @Test
    public void testMultiPercentileDiscWithFineGrainedPercentiles() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) value from long_sequence(100))");
            // Test with percentiles that have more decimal places
            assertSql(
                    "count\n" +
                            "100\n",
                    "select count(*) from (select value, percentile_disc(value, ARRAY[0.1, 0.25, 0.333, 0.5, 0.667, 0.75, 0.9]) over () from test)"
            );
        });
    }

    @Test
    public void testMultiPercentileDiscWithNegativeValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x - 6 as double) value from long_sequence(10))");
            // Values: -5, -4, -3, -2, -1, 0, 1, 2, 3, 4
            // 25th: ceil(10*0.25)-1 = 2 → -3.0
            // 50th: ceil(10*0.50)-1 = 4 → -1.0
            // 75th: ceil(10*0.75)-1 = 7 → 2.0
            assertSql(
                    "value\tpercentile_disc\n" +
                            "-5.0\t[-3.0,-1.0,2.0]\n" +
                            "-4.0\t[-3.0,-1.0,2.0]\n" +
                            "-3.0\t[-3.0,-1.0,2.0]\n" +
                            "-2.0\t[-3.0,-1.0,2.0]\n" +
                            "-1.0\t[-3.0,-1.0,2.0]\n" +
                            "0.0\t[-3.0,-1.0,2.0]\n" +
                            "1.0\t[-3.0,-1.0,2.0]\n" +
                            "2.0\t[-3.0,-1.0,2.0]\n" +
                            "3.0\t[-3.0,-1.0,2.0]\n" +
                            "4.0\t[-3.0,-1.0,2.0]\n",
                    "select value, percentile_disc(value, ARRAY[0.25, 0.5, 0.75]) over () from test"
            );
        });
    }

    @Test
    public void testMultiPercentileDiscWithSinglePercentileInArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) value from long_sequence(10))");
            // Single element array should work like scalar version but return array
            assertSql(
                    "value\tpercentile_disc\n" +
                            "1.0\t[5.0]\n" +
                            "2.0\t[5.0]\n" +
                            "3.0\t[5.0]\n" +
                            "4.0\t[5.0]\n" +
                            "5.0\t[5.0]\n" +
                            "6.0\t[5.0]\n" +
                            "7.0\t[5.0]\n" +
                            "8.0\t[5.0]\n" +
                            "9.0\t[5.0]\n" +
                            "10.0\t[5.0]\n",
                    "select value, percentile_disc(value, ARRAY[0.5]) over () from test"
            );
        });
    }

    @Test
    public void testMultiPercentileDiscWithVeryLargeArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) value from long_sequence(10))");
            // Test with 20 percentiles
            assertSql(
                    "count\n" +
                            "10\n",
                    "select count(*) from (" +
                            "select value, percentile_disc(value, ARRAY[" +
                            "0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, " +
                            "0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1.0" +
                            "]) over () from test)"
            );
        });
    }

    @Test
    public void testMultiplePercentilesAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x long)");
            execute("insert into test values (null), (null), (null)");
            assertSql(
                    "percentile_disc\n" +
                            "null\n",
                    "select percentile_disc(x, ARRAY[0.98, 0.95, 0.90, 0.50]) from test"
            );
        });
    }

    @Test
    public void testMultiplePercentilesAllSameValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select 5.0 x from long_sequence(100))");
            assertSql(
                    "percentile_disc\n[5.0,5.0,5.0,5.0]\n",
                    "select percentile_disc(x, ARRAY[0.98, 0.95, 0.90, 0.50]) from test"
            );
        });
    }

    @Test
    public void testMultiplePercentilesAllZeros() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select 0.0 x from long_sequence(100))");
            assertSql(
                    "percentile_disc\n[0.0,0.0,0.0,0.0]\n",
                    "select percentile_disc(x, ARRAY[0.98, 0.95, 0.90, 0.50]) from test"
            );
        });
    }

    @Test
    public void testMultiplePercentilesDoubleValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) x from long_sequence(100))");
            assertSql(
                    "percentile_disc\n[98.0,95.0,90.0,50.0]\n",
                    "select percentile_disc(x, ARRAY[0.98, 0.95, 0.90, 0.50]) from test"
            );
        });
    }

    @Test
    public void testMultiplePercentilesEmptyArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) x from long_sequence(100))");
            assertSql(
                    "percentile_disc\n[]\n",
                    "select percentile_disc(x, ARRAY[]) from test"
            );
        });
    }

    @Test
    public void testMultiplePercentilesEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x double)");
            assertSql(
                    "percentile_disc\n" +
                            "null\n",
                    "select percentile_disc(x, ARRAY[0.98, 0.95, 0.90, 0.50]) from test"
            );
        });
    }

    @Test
    public void testMultiplePercentilesFloatValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as float) x from long_sequence(100))");
            assertSql(
                    "percentile_disc\n[98.0,95.0,90.0,50.0]\n",
                    "select percentile_disc(x, ARRAY[0.98, 0.95, 0.90, 0.50]) from test"
            );
        });
    }

    @Test
    public void testMultiplePercentilesIncludingEdgeCases() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) x from long_sequence(100))");
            assertSql(
                    "percentile_disc\n[100.0,98.0,1.0,50.0]\n",
                    "select percentile_disc(x, ARRAY[1.0, 0.98, 0.0, 0.50]) from test"
            );
        });
    }

    @Test
    public void testMultiplePercentilesLargeDataset() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) x from long_sequence(10000))");
            assertSql(
                    "percentile_disc\n[9800.0,9500.0,9000.0,5000.0]\n",
                    "select percentile_disc(x, ARRAY[0.98, 0.95, 0.90, 0.50]) from test"
            );
        });
    }

    @Test
    public void testMultiplePercentilesOrdering() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x double)");
            execute("insert into test values (5.0), (1.0), (10.0), (3.0), (7.0)");
            // Sorted: 1, 3, 5, 7, 10
            // 0.98 -> ceil(5 * 0.98) - 1 = 4 -> 10.0
            // 0.95 -> ceil(5 * 0.95) - 1 = 4 -> 10.0
            // 0.90 -> ceil(5 * 0.90) - 1 = 4 -> 10.0
            // 0.50 -> ceil(5 * 0.50) - 1 = 2 -> 5.0
            assertSql(
                    "percentile_disc\n" +
                            "[10.0,10.0,10.0,5.0]\n",
                    "select percentile_disc(x, ARRAY[0.98, 0.95, 0.90, 0.50]) from test"
            );
        });
    }

    @Test
    public void testMultiplePercentilesSinglePercentile() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) x from long_sequence(100))");
            assertSql(
                    "percentile_disc\n[50.0]\n",
                    "select percentile_disc(x, ARRAY[0.50]) from test"
            );
        });
    }

    @Test
    public void testMultiplePercentilesSingleValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x double)");
            execute("insert into test values (42.0)");
            assertSql(
                    "percentile_disc\n" +
                            "[42.0,42.0,42.0,42.0]\n",
                    "select percentile_disc(x, ARRAY[0.98, 0.95, 0.90, 0.50]) from test"
            );
        });
    }

    @Test
    public void testMultiplePercentilesSomeNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x double)");
            execute("insert into test values (1.0), (null), (null), (null)");
            assertSql(
                    "percentile_disc\n" +
                            "[1.0,1.0,1.0,1.0]\n",
                    "select percentile_disc(x, ARRAY[0.98, 0.95, 0.90, 0.50]) from test"
            );
        });
    }

    @Test
    public void testMultiplePercentilesTwoValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x double)");
            execute("insert into test values (10.0), (20.0)");
            assertSql(
                    "percentile_disc\n" +
                            "[20.0,20.0,20.0,10.0]\n",
                    "select percentile_disc(x, ARRAY[0.98, 0.95, 0.90, 0.50]) from test"
            );
        });
    }

    @Test
    public void testMultiplePercentilesWithDuplicates() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x double)");
            execute("insert into test values (1.0), (1.0), (2.0), (2.0), (3.0), (3.0)");
            assertSql(
                    "percentile_disc\n" +
                            "[3.0,3.0,3.0,2.0]\n",
                    "select percentile_disc(x, ARRAY[0.98, 0.95, 0.90, 0.50]) from test"
            );
        });
    }

    @Test
    public void testMultiplePercentilesWithGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (category symbol, value double)");
            execute("insert into test values ('A', 1.0), ('A', 2.0), ('A', 3.0), ('B', 10.0), ('B', 20.0), ('B', 30.0)");
            assertSql(
                    "category\tpercentile_disc\n" +
                            "A\t[3.0,3.0,3.0,2.0]\n" +
                            "B\t[30.0,30.0,30.0,20.0]\n",
                    "select category, percentile_disc(value, ARRAY[0.98, 0.95, 0.90, 0.50]) from test order by category"
            );
        });
    }

    @Test
    public void testMultiplePercentilesWithKnownData() throws Exception {
        assertMemoryLeak(() -> {
            execute(txDdl);
            execute(txDml);
            assertSql("percentile_disc\n" +
                            "[406.977,289.615,224.195,63.863]\n",
                    "select percentile_disc(value, ARRAY[0.98, 0.95, 0.90, 0.50]) from tx_traffic");
        });
    }

    @Test
    public void testMultiplePercentilesWithNegativeValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x double)");
            execute("insert into test values (1.0), (-1.0), (5.0), (-5.0), (10.0), (-10.0)");
            assertSql(
                    "percentile_disc\n" +
                            "[10.0,10.0,10.0,-1.0]\n",
                    "select percentile_disc(x, ARRAY[0.98, 0.95, 0.90, 0.50]) from test"
            );
        });
    }

    @Test
    public void testMultiplePercentilesWithPrecision() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) x from long_sequence(1000))");
            assertSql(
                    "percentile_disc\n[980.0,950.0,900.0,500.0]\n",
                    "select percentile_disc(x, ARRAY[0.98, 0.95, 0.90, 0.50]) from test"
            );
        });
    }

    @Test
    public void testWithKnownData() throws Exception {
        assertMemoryLeak(() -> {
            execute(txDdl);
            execute(txDml);
            assertSql("percentile_disc\n" +
                            "[406.977,289.615,224.195,63.863]\n",
                    "select percentile_disc(value, ARRAY[0.98, 0.95, 0.90, 0.50]) from tx_traffic");
        });
    }
}
