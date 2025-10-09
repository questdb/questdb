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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Tests group by with decimal column(s)
 */
public class GroupByDecimalTest extends AbstractCairoTest {

    @Test
    public void testGroupByDecimal128() throws Exception {
        execute("create table dec128test (bigval decimal(38, 6))");
        execute("insert into dec128test values " +
                "(23450.000000m), (23450.000000m), (23450.000000m), " +
                "(23451.000000m), (23451.000000m), (23451.000000m), " +
                "(23452.000000m), (23452.000000m), (23452.000000m)");

        assertQuery("bigval\tcount\n" +
                        "23450.000000\t3\n" +
                        "23451.000000\t3\n" +
                        "23452.000000\t3\n",
                "select bigval, count(*) from dec128test group by bigval order by bigval",
                null, null, true, true);
    }

    @Test
    public void testGroupByDecimal16() throws Exception {
        execute("create table dec16test (amount decimal(4, 2))");
        execute("insert into dec16test values " +
                "(10.00m), (10.00m), (10.00m), (10.00m), (10.00m), " +
                "(10.00m), (10.00m), (10.00m), (10.00m), (10.00m), " +
                "(11.00m), (11.00m), (11.00m), (11.00m), (11.00m), " +
                "(11.00m), (11.00m), (11.00m), (11.00m), (11.00m)");

        assertQuery("amount\tcount\n" +
                        "10.00\t10\n" +
                        "11.00\t10\n",
                "select amount, count(*) from dec16test group by amount order by amount",
                null, null, true, true);
    }

    @Test
    public void testGroupByDecimal256() throws Exception {
        execute("create table dec256test (hugeval decimal(60, 0))");
        execute("insert into dec256test values " +
                "(23450m), (23450m), (23450m), " +
                "(23451m), (23451m), (23451m), " +
                "(23452m), (23452m), (23452m)");

        assertQuery("hugeval\tcount\n" +
                        "23450\t3\n" +
                        "23451\t3\n" +
                        "23452\t3\n",
                "select hugeval, count(*) from dec256test group by hugeval order by hugeval",
                null, null, true, true);
    }

    @Test
    public void testGroupByDecimal32() throws Exception {
        execute("create table dec32test (value decimal(9, 3), x int)");
        execute("insert into dec32test values " +
                "(23450.000m, 1), (23450.000m, 2), (23450.000m, 3), " +
                "(23451.000m, 4), (23451.000m, 5), (23451.000m, 6), " +
                "(23452.000m, 7), (23452.000m, 8), (23452.000m, 9)");

        assertQuery("value\tminx\tmaxx\n" +
                        "23450.000\t1\t3\n" +
                        "23451.000\t4\t6\n" +
                        "23452.000\t7\t9\n",
                "select value, min(x) as minx, max(x) as maxx from dec32test group by value order by value",
                null, null, true, true);
    }

    @Test
    public void testGroupByDecimal64() throws Exception {
        execute("create table dec64test (balance decimal(18, 6), id long)");
        execute("insert into dec64test values " +
                "(23450.000000m, 1), (23450.000000m, 2), (23450.000000m, 3), " +
                "(23451.000000m, 4), (23451.000000m, 5), (23451.000000m, 6), " +
                "(23452.000000m, 7), (23452.000000m, 8), (23452.000000m, 9)");

        assertQuery("balance\tcount\tsum_id\n" +
                        "23450.000000\t3\t6\n" +
                        "23451.000000\t3\t15\n" +
                        "23452.000000\t3\t24\n",
                "select balance, count(*), sum(id) as sum_id from dec64test group by balance order by balance",
                null, null, true, true);
    }

    @Test
    public void testGroupByDecimal8() throws Exception {
        execute("create table dec8test (price decimal(2, 1))");
        execute("insert into dec8test values " +
                "(1.0m), (1.0m), (1.0m), (1.0m), (1.0m), " +
                "(1.0m), (1.0m), (1.0m), (1.0m), (1.0m), " +
                "(2.0m), (2.0m), (2.0m), (2.0m), (2.0m), " +
                "(2.0m), (2.0m), (2.0m), (2.0m), (2.0m)");

        assertQuery("price\tcount\n" +
                        "1.0\t10\n" +
                        "2.0\t10\n",
                "select price, count(*) from dec8test group by price order by price",
                null, null, true, true);
    }

    @Test
    public void testGroupByDecimalAndOtherTypes() throws Exception {
        execute("create table mixedtest (category string, price decimal(3, 1), qty double)");
        execute("insert into mixedtest values " +
                "('A', 10.0m, 0.5), ('B', 10.0m, 1.0), " +
                "('A', 10.0m, 1.5), ('B', 10.0m, 2.0), " +
                "('A', 20.0m, 2.5), ('B', 20.0m, 3.0), " +
                "('A', 20.0m, 3.5), ('B', 20.0m, 4.0)");

        assertQuery("category\tprice\ttotal\n" +
                        "A\t10.0\t2.0\n" +
                        "A\t20.0\t6.0\n" +
                        "B\t10.0\t3.0\n" +
                        "B\t20.0\t7.0\n",
                "select category, price, sum(qty) as total from mixedtest group by category, price order by category, price",
                null, null, true, true);
    }

    @Test
    public void testGroupByDecimalCastFromLong() throws Exception {
        execute("create table casttest (dec_val decimal(3, 2))");
        execute("insert into casttest values " +
                "(1.00m), (1.00m), " +
                "(2.00m), (2.00m), " +
                "(3.00m), (3.00m), " +
                "(4.00m), (4.00m), " +
                "(5.00m), (5.00m)");

        assertQuery("dec_val\tcount\n" +
                        "1.00\t2\n" +
                        "2.00\t2\n" +
                        "3.00\t2\n" +
                        "4.00\t2\n" +
                        "5.00\t2\n",
                "select dec_val, count(*) from casttest group by dec_val order by dec_val",
                null, null, true, true);
    }

    @Test
    public void testGroupByDecimalDistinct() throws Exception {
        execute("create table distincttest (price decimal(5, 2))");
        execute("insert into distincttest values " +
                "(9.00m), (9.00m), (9.00m), " +
                "(39.00m), (39.00m), (39.00m), " +
                "(69.00m), (69.00m), (69.00m), " +
                "(99.00m), (99.00m), (99.00m)");

        assertQuery("price\tcount\n" +
                        "9.00\t3\n" +
                        "39.00\t3\n" +
                        "69.00\t3\n" +
                        "99.00\t3\n",
                "select price, count(*) from distincttest group by price order by price",
                null, null, true, true);
    }

    @Test
    public void testGroupByDecimalNegativeAndPositive() throws Exception {
        execute("create table boundaries (amount decimal(5, 2), id long)");
        execute("insert into boundaries values " +
                "(99.99m, 1), " +
                "(-99.99m, 2), " +
                "(0.00m, 3), " +
                "(0.01m, 4), " +
                "(-0.01m, 5), " +
                "(1.00m, 6), " +
                "(1.00m, 7), " +
                "(1.00m, 8), " +
                "(1.00m, 9), " +
                "(1.00m, 10)");

        assertQuery("amount\tcount\tid_sum\n" +
                        "-99.99\t1\t2\n" +
                        "-0.01\t1\t5\n" +
                        "0.00\t1\t3\n" +
                        "0.01\t1\t4\n" +
                        "1.00\t5\t40\n" +
                        "99.99\t1\t1\n",
                "select amount, count(*), sum(id) as id_sum from boundaries group by amount order by amount",
                null, null, true, true);
    }

    @Test
    public void testGroupByDecimalPrecisionBoundaries() throws Exception {
        execute("create table boundtest (d2 decimal(2, 0), d4 decimal(4, 0), d9 decimal(9, 0), d18 decimal(18, 0), d38 decimal(38, 0))");
        execute("insert into boundtest values " +
                "(99m, 9999m, 999999999m, 999999999999999999m, 99999999999999999999999999999999999999m), " +
                "(99m, 9999m, 999999999m, 999999999999999999m, 99999999999999999999999999999999999999m)");

        assertQuery("d2\td4\td9\td18\td38\tcount\n" +
                        "99\t9999\t999999999\t999999999999999999\t99999999999999999999999999999999999999\t2\n",
                "select d2, d4, d9, d18, d38, count(*) from boundtest group by d2, d4, d9, d18, d38",
                null, null, true, true);
    }

    @Test
    public void testGroupByDecimalTimeSeries() throws Exception {
        execute("create table timeseries (ts timestamp, price decimal(10, 2), volume long) timestamp(ts)");
        execute("insert into timeseries values " +
                "('2024-01-01T00:00:00.000000Z', 100.50m, 1000), " +
                "('2024-01-01T01:00:00.000000Z', 100.50m, 2000), " +
                "('2024-01-01T02:00:00.000000Z', 101.25m, 1500), " +
                "('2024-01-01T03:00:00.000000Z', 101.25m, 3000), " +
                "('2024-01-01T04:00:00.000000Z', 102.00m, 2500)");

        assertQuery("price\ttotal_volume\thours\n" +
                        "100.50\t3000\t2\n" +
                        "101.25\t4500\t2\n" +
                        "102.00\t2500\t1\n",
                "select price, sum(volume) as total_volume, count(*) as hours " +
                        "from timeseries group by price order by price",
                null, null, true, true);
    }

    @Test
    public void testGroupByDecimalWithDifferentScales() throws Exception {
        execute("create table scaletest (grp decimal(8, 3), x long)");
        execute("insert into scaletest values " +
                "(1.000m, 1), (1.000m, 2), (1.000m, 3), " +
                "(2.000m, 4), (2.000m, 5), (2.000m, 6), " +
                "(3.000m, 7), (3.000m, 8), (3.000m, 9)");

        assertQuery("grp\tcount\tsum_x\n" +
                        "1.000\t3\t6\n" +
                        "2.000\t3\t15\n" +
                        "3.000\t3\t24\n",
                "select grp, count(*), sum(x) as sum_x from scaletest group by grp order by grp",
                null, null, true, true);
    }

    @Test
    public void testGroupByDecimalWithLimits() throws Exception {
        execute("create table ordertest (price decimal(6, 2))");
        execute("insert into ordertest values " +
                "(100.00m), (100.00m), " +
                "(110.50m), (110.50m), " +
                "(121.00m), (121.00m), " +
                "(131.50m), (131.50m), " +
                "(142.00m), (142.00m)");

        assertQuery("price\tcount\n" +
                        "100.00\t2\n" +
                        "110.50\t2\n",
                "select price, count(*) from ordertest group by price order by price limit 2",
                null, null, true, true);
    }

    @Test
    public void testGroupByDecimalWithNull() throws Exception {
        execute("create table nulltest (amount decimal(3, 1), id long)");
        execute("insert into nulltest values " +
                "(1.0m, 1), (2.0m, 2), (3.0m, 3), (null, 4), " +
                "(1.0m, 5), (2.0m, 6), (3.0m, 7), (null, 8), " +
                "(1.0m, 9), (2.0m, 10), (3.0m, 11), (null, 12)");

        assertQuery("amount\tcount\n" +
                        "\t3\n" +
                        "1.0\t3\n" +
                        "2.0\t3\n" +
                        "3.0\t3\n",
                "select amount, count(*) from nulltest group by amount order by amount",
                null, null, true, true);
    }

    @Test
    public void testGroupByDecimalWithOrderBy() throws Exception {
        execute("create table ordertest (price decimal(6, 2))");
        execute("insert into ordertest values " +
                "(100.00m), (110.50m), (121.00m), (131.50m), (142.00m), " +
                "(100.00m), (110.50m), (121.00m), (131.50m), (142.00m)");

        assertQuery("price\tcount\n" +
                        "100.00\t2\n" +
                        "110.50\t2\n" +
                        "121.00\t2\n" +
                        "131.50\t2\n" +
                        "142.00\t2\n",
                "select price, count(*) from ordertest group by price order by price",
                null, null, true, true);
    }

    @Test
    public void testGroupByMultipleDecimals() throws Exception {
        execute("create table multidectest (price decimal(3, 1), qty decimal(5, 2))");
        execute("insert into multidectest values " +
                "(10.5m, 100.00m), (10.5m, 100.00m), " +
                "(20.5m, 100.00m), (20.5m, 100.00m), " +
                "(10.5m, 200.00m), (10.5m, 200.00m), " +
                "(20.5m, 200.00m), (20.5m, 200.00m)");

        assertQuery("price\tqty\tcount\n" +
                        "10.5\t100.00\t2\n" +
                        "10.5\t200.00\t2\n" +
                        "20.5\t100.00\t2\n" +
                        "20.5\t200.00\t2\n",
                "select price, qty, count(*) from multidectest group by price, qty order by price, qty",
                null, null, true, true);
    }
}
