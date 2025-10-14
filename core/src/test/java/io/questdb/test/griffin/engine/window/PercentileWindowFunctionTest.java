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

package io.questdb.test.griffin.engine.window;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class PercentileWindowFunctionTest extends AbstractCairoTest {

    @Test
    public void testPercentileDiscOverWholeResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) value from long_sequence(10))");
            assertSql(
                    "value\tpercentile_disc\n" +
                            "1.0\t5.0\n" +
                            "2.0\t5.0\n" +
                            "3.0\t5.0\n" +
                            "4.0\t5.0\n" +
                            "5.0\t5.0\n" +
                            "6.0\t5.0\n" +
                            "7.0\t5.0\n" +
                            "8.0\t5.0\n" +
                            "9.0\t5.0\n" +
                            "10.0\t5.0\n",
                    "select value, percentile_disc(value, 0.5) over () from test"
            );
        });
    }

    @Test
    public void testPercentileContOverWholeResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) value from long_sequence(10))");
            assertSql(
                    "value\tpercentile_cont\n" +
                            "1.0\t5.5\n" +
                            "2.0\t5.5\n" +
                            "3.0\t5.5\n" +
                            "4.0\t5.5\n" +
                            "5.0\t5.5\n" +
                            "6.0\t5.5\n" +
                            "7.0\t5.5\n" +
                            "8.0\t5.5\n" +
                            "9.0\t5.5\n" +
                            "10.0\t5.5\n",
                    "select value, percentile_cont(value, 0.5) over () from test"
            );
        });
    }

    @Test
    public void testPercentileDiscOverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (" +
                    "select x % 2 as category, cast(x as double) as value from long_sequence(10)" +
                    ")");
            assertSql(
                    "category\tvalue\tpercentile_disc\n" +
                            "1\t1.0\t5.0\n" +
                            "0\t2.0\t6.0\n" +
                            "1\t3.0\t5.0\n" +
                            "0\t4.0\t6.0\n" +
                            "1\t5.0\t5.0\n" +
                            "0\t6.0\t6.0\n" +
                            "1\t7.0\t5.0\n" +
                            "0\t8.0\t6.0\n" +
                            "1\t9.0\t5.0\n" +
                            "0\t10.0\t6.0\n",
                    "select category, value, percentile_disc(value, 0.5) over (partition by category) from test"
            );
        });
    }

    @Test
    public void testPercentileContOverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (" +
                    "select x % 2 as category, cast(x as double) as value from long_sequence(10)" +
                    ")");
            assertSql(
                    "category\tvalue\tpercentile_cont\n" +
                            "1\t1.0\t5.0\n" +
                            "0\t2.0\t6.0\n" +
                            "1\t3.0\t5.0\n" +
                            "0\t4.0\t6.0\n" +
                            "1\t5.0\t5.0\n" +
                            "0\t6.0\t6.0\n" +
                            "1\t7.0\t5.0\n" +
                            "0\t8.0\t6.0\n" +
                            "1\t9.0\t5.0\n" +
                            "0\t10.0\t6.0\n",
                    "select category, value, percentile_cont(value, 0.5) over (partition by category) from test"
            );
        });
    }

    @Test
    public void testPercentileDisc0thPercentile() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) value from long_sequence(10))");
            assertSql(
                    "percentile_disc\n" +
                            "1.0\n" +
                            "1.0\n" +
                            "1.0\n" +
                            "1.0\n" +
                            "1.0\n" +
                            "1.0\n" +
                            "1.0\n" +
                            "1.0\n" +
                            "1.0\n" +
                            "1.0\n",
                    "select percentile_disc(value, 0) over () from test"
            );
        });
    }

    @Test
    public void testPercentileDisc100thPercentile() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) value from long_sequence(10))");
            assertSql(
                    "percentile_disc\n" +
                            "10.0\n" +
                            "10.0\n" +
                            "10.0\n" +
                            "10.0\n" +
                            "10.0\n" +
                            "10.0\n" +
                            "10.0\n" +
                            "10.0\n" +
                            "10.0\n" +
                            "10.0\n",
                    "select percentile_disc(value, 1.0) over () from test"
            );
        });
    }

    @Test
    public void testPercentileDiscWith75thPercentile() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) value from long_sequence(10))");
            assertSql(
                    "percentile_disc\n" +
                            "8.0\n" +
                            "8.0\n" +
                            "8.0\n" +
                            "8.0\n" +
                            "8.0\n" +
                            "8.0\n" +
                            "8.0\n" +
                            "8.0\n" +
                            "8.0\n" +
                            "8.0\n",
                    "select percentile_disc(value, 0.75) over () from test"
            );
        });
    }

    @Test
    public void testPercentileContWith75thPercentile() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) value from long_sequence(10))");
            assertSql(
                    "percentile_cont\n" +
                            "7.75\n" +
                            "7.75\n" +
                            "7.75\n" +
                            "7.75\n" +
                            "7.75\n" +
                            "7.75\n" +
                            "7.75\n" +
                            "7.75\n" +
                            "7.75\n" +
                            "7.75\n",
                    "select percentile_cont(value, 0.75) over () from test"
            );
        });
    }

    @Test
    public void testPercentileDiscWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (value double)");
            execute("insert into test values (1.0), (2.0), (null), (4.0), (5.0)");
            // For 4 non-null values: [1.0, 2.0, 4.0, 5.0], 50th percentile index = ceil(4 * 0.5) - 1 = 1
            // After sorting: [1.0, 2.0, 4.0, 5.0], index 1 = 2.0
            assertSql(
                    "percentile_disc\n" +
                            "2.0\n" +
                            "2.0\n" +
                            "2.0\n" +
                            "2.0\n" +
                            "2.0\n",
                    "select percentile_disc(value, 0.5) over () from test"
            );
        });
    }

    @Test
    public void testPercentileContWithInterpolation() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) value from long_sequence(5))");
            // For 5 values (1,2,3,4,5), 25th percentile at position 0.25*(5-1) = 1.0
            // This is exactly at index 1, so result should be the value at index 1 after sorting = 2.0
            assertSql(
                    "percentile_cont\n" +
                            "2.0\n" +
                            "2.0\n" +
                            "2.0\n" +
                            "2.0\n" +
                            "2.0\n",
                    "select percentile_cont(value, 0.25) over () from test"
            );
        });
    }

    @Test
    public void testPercentileDiscRejectsNonWholePartitionFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) value, x % 2 as category from long_sequence(10))");
            assertException(
                    "select percentile_disc(value, 0.5) over (partition by category rows between 1 preceding and current row) from test",
                    7,
                    "percentile_disc window function only supports whole partition frames"
            );
        });
    }

    @Test
    public void testPercentileContRejectsNonWholePartitionFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) value, x % 2 as category from long_sequence(10))");
            assertException(
                    "select percentile_cont(value, 0.5) over (partition by category rows between 1 preceding and current row) from test",
                    7,
                    "percentile_cont window function only supports whole partition frames"
            );
        });
    }

    @Test
    public void testPercentileDiscRejectsOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) value from long_sequence(10))");
            assertException(
                    "select percentile_disc(value, 0.5) over (order by value) from test",
                    7,
                    "percentile_disc window function only supports whole partition frames"
            );
        });
    }

    @Test
    public void testPercentileContRejectsOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) value from long_sequence(10))");
            assertException(
                    "select percentile_cont(value, 0.5) over (order by value) from test",
                    7,
                    "percentile_cont window function only supports whole partition frames"
            );
        });
    }

    @Test
    public void testPercentileDiscMultiplePartitions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (" +
                    "select x % 2 as cat1, cast(x as double) as value " +
                    "from long_sequence(10)" +
                    ")");
            // cat1=0 (even): values 2, 4, 6, 8, 10 → 50th percentile index = ceil(5*0.5)-1 = 2 → value 6.0
            // cat1=1 (odd):  values 1, 3, 5, 7, 9  → 50th percentile index = ceil(5*0.5)-1 = 2 → value 5.0
            assertSql(
                    "cat1\tvalue\tpercentile_disc\n" +
                            "1\t1.0\t5.0\n" +
                            "0\t2.0\t6.0\n" +
                            "1\t3.0\t5.0\n" +
                            "0\t4.0\t6.0\n" +
                            "1\t5.0\t5.0\n" +
                            "0\t6.0\t6.0\n" +
                            "1\t7.0\t5.0\n" +
                            "0\t8.0\t6.0\n" +
                            "1\t9.0\t5.0\n" +
                            "0\t10.0\t6.0\n",
                    "select cat1, value, percentile_disc(value, 0.5) over (partition by cat1) from test"
            );
        });
    }

    @Test
    public void testMultiPercentileDiscOverWholeResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) value from long_sequence(10))");
            assertSql(
                    "value\tpercentile_disc\n" +
                            "1.0\t[3.0,5.0,8.0]\n" +
                            "2.0\t[3.0,5.0,8.0]\n" +
                            "3.0\t[3.0,5.0,8.0]\n" +
                            "4.0\t[3.0,5.0,8.0]\n" +
                            "5.0\t[3.0,5.0,8.0]\n" +
                            "6.0\t[3.0,5.0,8.0]\n" +
                            "7.0\t[3.0,5.0,8.0]\n" +
                            "8.0\t[3.0,5.0,8.0]\n" +
                            "9.0\t[3.0,5.0,8.0]\n" +
                            "10.0\t[3.0,5.0,8.0]\n",
                    "select value, percentile_disc(value, ARRAY[0.25, 0.5, 0.75]) over () from test"
            );
        });
    }

    @Test
    public void testMultiPercentileContOverWholeResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) value from long_sequence(10))");
            assertSql(
                    "value\tpercentile_cont\n" +
                            "1.0\t[3.25,5.5,7.75]\n" +
                            "2.0\t[3.25,5.5,7.75]\n" +
                            "3.0\t[3.25,5.5,7.75]\n" +
                            "4.0\t[3.25,5.5,7.75]\n" +
                            "5.0\t[3.25,5.5,7.75]\n" +
                            "6.0\t[3.25,5.5,7.75]\n" +
                            "7.0\t[3.25,5.5,7.75]\n" +
                            "8.0\t[3.25,5.5,7.75]\n" +
                            "9.0\t[3.25,5.5,7.75]\n" +
                            "10.0\t[3.25,5.5,7.75]\n",
                    "select value, percentile_cont(value, ARRAY[0.25, 0.5, 0.75]) over () from test"
            );
        });
    }

    @Test
    public void testMultiPercentileDiscOverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (" +
                    "select x % 2 as category, cast(x as double) as value from long_sequence(10)" +
                    ")");
            // cat=0 (even): values 2, 4, 6, 8, 10
            //   25th percentile index = ceil(5*0.25)-1 = 1 → 4.0
            //   50th percentile index = ceil(5*0.50)-1 = 2 → 6.0
            //   75th percentile index = ceil(5*0.75)-1 = 3 → 8.0
            // cat=1 (odd): values 1, 3, 5, 7, 9
            //   25th percentile index = ceil(5*0.25)-1 = 1 → 3.0
            //   50th percentile index = ceil(5*0.50)-1 = 2 → 5.0
            //   75th percentile index = ceil(5*0.75)-1 = 3 → 7.0
            assertSql(
                    "category\tvalue\tpercentile_disc\n" +
                            "1\t1.0\t[3.0,5.0,7.0]\n" +
                            "0\t2.0\t[4.0,6.0,8.0]\n" +
                            "1\t3.0\t[3.0,5.0,7.0]\n" +
                            "0\t4.0\t[4.0,6.0,8.0]\n" +
                            "1\t5.0\t[3.0,5.0,7.0]\n" +
                            "0\t6.0\t[4.0,6.0,8.0]\n" +
                            "1\t7.0\t[3.0,5.0,7.0]\n" +
                            "0\t8.0\t[4.0,6.0,8.0]\n" +
                            "1\t9.0\t[3.0,5.0,7.0]\n" +
                            "0\t10.0\t[4.0,6.0,8.0]\n",
                    "select category, value, percentile_disc(value, ARRAY[0.25, 0.5, 0.75]) over (partition by category) from test"
            );
        });
    }

    @Test
    public void testMultiPercentileContOverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (" +
                    "select x % 2 as category, cast(x as double) as value from long_sequence(10)" +
                    ")");
            // cat=0 (even): values 2, 4, 6, 8, 10
            //   position(0.25) = 0.25*(5-1) = 1.0 → value at index 1 = 4.0
            //   position(0.50) = 0.50*(5-1) = 2.0 → value at index 2 = 6.0
            //   position(0.75) = 0.75*(5-1) = 3.0 → value at index 3 = 8.0
            // cat=1 (odd): values 1, 3, 5, 7, 9
            //   position(0.25) = 0.25*(5-1) = 1.0 → value at index 1 = 3.0
            //   position(0.50) = 0.50*(5-1) = 2.0 → value at index 2 = 5.0
            //   position(0.75) = 0.75*(5-1) = 3.0 → value at index 3 = 7.0
            assertSql(
                    "category\tvalue\tpercentile_cont\n" +
                            "1\t1.0\t[3.0,5.0,7.0]\n" +
                            "0\t2.0\t[4.0,6.0,8.0]\n" +
                            "1\t3.0\t[3.0,5.0,7.0]\n" +
                            "0\t4.0\t[4.0,6.0,8.0]\n" +
                            "1\t5.0\t[3.0,5.0,7.0]\n" +
                            "0\t6.0\t[4.0,6.0,8.0]\n" +
                            "1\t7.0\t[3.0,5.0,7.0]\n" +
                            "0\t8.0\t[4.0,6.0,8.0]\n" +
                            "1\t9.0\t[3.0,5.0,7.0]\n" +
                            "0\t10.0\t[4.0,6.0,8.0]\n",
                    "select category, value, percentile_cont(value, ARRAY[0.25, 0.5, 0.75]) over (partition by category) from test"
            );
        });
    }

    @Test
    public void testMultiPercentileDiscWithSingleValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) value from long_sequence(10))");
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
}
