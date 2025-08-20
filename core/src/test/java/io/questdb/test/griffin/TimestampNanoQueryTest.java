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

package io.questdb.test.griffin;

import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TimestampNanoQueryTest extends AbstractCairoTest {

    @Test
    public void testAlterColumnTypeToMicros() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table xyz(id int, nano_time timestamp_ns, time timestamp_ns) timestamp(time) partition by DAY");
            execute("INSERT INTO xyz VALUES(1, '2021-01-01T00:00:00.123456789Z'::timestamp_ns, '2021-01-01T00:00:00.000000100Z'::timestamp_ns)");
            execute("INSERT INTO xyz VALUES(2, '2021-01-01T00:00:00.987654321Z'::timestamp_ns, '2021-01-01T00:00:00.000000200Z'::timestamp_ns)");

            String beforeExpected = "id\tnano_time\ttime\n" +
                    "1\t2021-01-01T00:00:00.123456789Z\t2021-01-01T00:00:00.000000100Z\n" +
                    "2\t2021-01-01T00:00:00.987654321Z\t2021-01-01T00:00:00.000000200Z\n";
            String beforeQuery = "select id, nano_time, time from xyz order by time";
            assertQuery(beforeExpected, beforeQuery, "time", true, true);

            execute("ALTER TABLE xyz ALTER COLUMN nano_time TYPE timestamp");

            String afterExpected = "id\tnano_time\ttime\n" +
                    "1\t2021-01-01T00:00:00.123456Z\t2021-01-01T00:00:00.000000100Z\n" +
                    "2\t2021-01-01T00:00:00.987654Z\t2021-01-01T00:00:00.000000200Z\n";
            String afterQuery = "select id, nano_time, time from xyz order by time";
            assertQuery(afterExpected, afterQuery, "time", true, true);
        });
    }

    @Test
    public void testAlterColumnTypeToNanos() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table xyz(id int, micro_time timestamp, time timestamp_ns) timestamp(time) partition by DAY");
            execute("INSERT INTO xyz VALUES(1, '2021-01-01T00:00:00.123456Z'::timestamp, '2021-01-01T00:00:00.000000100Z'::timestamp_ns)");
            execute("INSERT INTO xyz VALUES(2, '2021-01-01T00:00:00.789123Z'::timestamp, '2021-01-01T00:00:00.000000200Z'::timestamp_ns)");

            String beforeExpected = "id\tmicro_time\ttime\n" +
                    "1\t2021-01-01T00:00:00.123456Z\t2021-01-01T00:00:00.000000100Z\n" +
                    "2\t2021-01-01T00:00:00.789123Z\t2021-01-01T00:00:00.000000200Z\n";
            String beforeQuery = "select id, micro_time, time from xyz order by time";
            assertQuery(beforeExpected, beforeQuery, "time", true, true);

            execute("ALTER TABLE xyz ALTER COLUMN micro_time TYPE timestamp_ns");

            String afterExpected = "id\tmicro_time\ttime\n" +
                    "1\t2021-01-01T00:00:00.123456000Z\t2021-01-01T00:00:00.000000100Z\n" +
                    "2\t2021-01-01T00:00:00.789123000Z\t2021-01-01T00:00:00.000000200Z\n";
            String afterQuery = "select id, micro_time, time from xyz order by time";
            assertQuery(afterExpected, afterQuery, "time", true, true);
        });
    }

    @Test
    public void testAlterTableAddColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table xyz(id int, time timestamp_ns) timestamp(time) partition by DAY");
            execute("INSERT INTO xyz VALUES(1, '2021-01-01T00:00:00.000000100Z'::timestamp_ns)");

            execute("ALTER TABLE xyz ADD COLUMN new_time timestamp_ns");
            execute("UPDATE xyz SET new_time = '2021-01-01T00:00:00.000000200Z'::timestamp_ns WHERE id = 1");

            String expected = "id\ttime\tnew_time\n" +
                    "1\t2021-01-01T00:00:00.000000100Z\t2021-01-01T00:00:00.000000200Z\n";
            String query = "select id, time, new_time from xyz";
            assertQuery(expected, query, "time", true, true);
        });
    }

    @Test
    public void testAlterTableDropColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table xyz(id int, time timestamp_ns, temp_time timestamp_ns) timestamp(time) partition by DAY");
            execute("INSERT INTO xyz VALUES(1, '2021-01-01T00:00:00.000000100Z'::timestamp_ns, '2021-01-01T00:00:00.000000200Z'::timestamp_ns)");

            execute("ALTER TABLE xyz DROP COLUMN temp_time");

            String expected = "id\ttime\n" +
                    "1\t2021-01-01T00:00:00.000000100Z\n";
            String query = "select id, time from xyz";
            assertQuery(expected, query, "time", true, true);
        });
    }

    @Test
    public void testArithmetic() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table xyz(id int, time timestamp_ns) timestamp(time) partition by DAY");
            execute("INSERT INTO xyz VALUES(1, '2021-01-01T00:00:00.000000100Z'::timestamp_ns)");

            String expected = "id\ttime\tadd_nanos\tsub_nanos\n" +
                    "1\t2021-01-01T00:00:00.000000100Z\t2021-01-01T00:00:00.000000200Z\t2021-01-01T00:00:00.000000000Z\n";
            String query = "select id, time, time + 100L add_nanos, time - 100L sub_nanos from xyz";
            assertQuery(expected, query, "time", true, true);
        });
    }

    @Test
    public void testBasicComparison() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table xyz(id int, time timestamp_ns) timestamp(time) partition by DAY");
            execute("INSERT INTO xyz VALUES(1, '2021-01-01T00:00:00.000000123Z'::timestamp_ns)");
            execute("INSERT INTO xyz VALUES(2, '2021-01-01T00:00:00.000000456Z'::timestamp_ns)");
            execute("INSERT INTO xyz VALUES(3, '2021-01-01T00:00:00.000000789Z'::timestamp_ns)");

            String expected = "id\ttime\n" +
                    "2\t2021-01-01T00:00:00.000000456Z\n" +
                    "3\t2021-01-01T00:00:00.000000789Z\n";
            String query = "select id, time from xyz where time > '2021-01-01T00:00:00.000000123Z'::timestamp_ns order by time";
            assertQuery(expected, query, "time", true, false);
        });
    }

    @Test
    public void testBasicEquality() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table xyz(id int, time timestamp_ns) timestamp(time) partition by DAY");
            execute("INSERT INTO xyz VALUES(1, '2021-01-01T00:00:00.000000123Z'::timestamp_ns)");
            execute("INSERT INTO xyz VALUES(2, '2021-01-01T00:00:00.000000123Z'::timestamp_ns)");
            execute("INSERT INTO xyz VALUES(3, '2021-01-01T00:00:00.000000456Z'::timestamp_ns)");

            String expected = "id\ttime\n" +
                    "1\t2021-01-01T00:00:00.000000123Z\n" +
                    "2\t2021-01-01T00:00:00.000000123Z\n";
            String query = "select id, time from xyz where time = '2021-01-01T00:00:00.000000123Z'::timestamp_ns order by id";
            assertQuery(expected, query, null, true, false);
        });
    }

    @Test
    public void testBasicInsertAndSelect() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table xyz(id int, time timestamp_ns) timestamp(time) partition by DAY");
            execute("INSERT INTO xyz VALUES(1, '2021-01-01T00:00:00.000000123Z'::timestamp_ns)");
            execute("INSERT INTO xyz VALUES(2, '2021-01-01T00:00:00.000000456Z'::timestamp_ns)");

            String expected = "id\ttime\n" +
                    "1\t2021-01-01T00:00:00.000000123Z\n" +
                    "2\t2021-01-01T00:00:00.000000456Z\n";
            String query = "select id, time from xyz order by time";
            assertQuery(expected, query, "time", true, true);
        });
    }

    @Test
    public void testCTE() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table trades(id int, symbol symbol, price double, time timestamp_ns) timestamp(time) partition by DAY");
            execute("INSERT INTO trades VALUES(1, 'AAPL', 150.1, '2021-01-01T09:00:00.000000100Z'::timestamp_ns)");
            execute("INSERT INTO trades VALUES(2, 'AAPL', 150.5, '2021-01-01T09:00:00.000000300Z'::timestamp_ns)");
            execute("INSERT INTO trades VALUES(3, 'MSFT', 250.2, '2021-01-01T09:00:00.000000200Z'::timestamp_ns)");
            execute("INSERT INTO trades VALUES(4, 'MSFT', 250.8, '2021-01-01T09:00:00.000000400Z'::timestamp_ns)");

            String expected = "symbol\tavg_price\tmin_time\tmax_time\n" +
                    "AAPL\t150.3\t2021-01-01T09:00:00.000000100Z\t2021-01-01T09:00:00.000000300Z\n" +
                    "MSFT\t250.5\t2021-01-01T09:00:00.000000200Z\t2021-01-01T09:00:00.000000400Z\n";
            String query = "with symbol_stats as (" +
                    "  select symbol, avg(price) avg_price, min(time) min_time, max(time) max_time " +
                    "  from trades " +
                    "  group by symbol" +
                    ") " +
                    "select symbol, avg_price, min_time, max_time from symbol_stats order by symbol";
            assertQuery(expected, query, null, true, true);
        });
    }

    @Test
    public void testCTEWithTimeFiltering() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table trades(id int, symbol symbol, price double, time timestamp_ns) timestamp(time) partition by DAY");
            execute("INSERT INTO trades VALUES(1, 'AAPL', 150.1, '2021-01-01T09:00:00.000000100Z'::timestamp_ns)");
            execute("INSERT INTO trades VALUES(2, 'AAPL', 150.5, '2021-01-01T09:00:00.000000300Z'::timestamp_ns)");
            execute("INSERT INTO trades VALUES(3, 'MSFT', 250.2, '2021-01-01T09:00:00.000000200Z'::timestamp_ns)");
            execute("INSERT INTO trades VALUES(4, 'MSFT', 250.8, '2021-01-01T09:00:00.000000400Z'::timestamp_ns)");

            String expected = "symbol\tavg_price\ttrade_count\n" +
                    "AAPL\t150.5\t1\n" +
                    "MSFT\t250.8\t1\n";
            String query = "with recent_trades as (" +
                    "  select * from trades " +
                    "  where time >= '2021-01-01T09:00:00.000000250Z'::timestamp_ns" +
                    ") " +
                    "select symbol, avg(price) avg_price, count(*) trade_count " +
                    "from recent_trades group by symbol order by symbol";
            assertQuery(expected, query, null, true, true);
        });
    }

    @Test
    public void testComplexArithmetic() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table math_test(id int, nano_ts timestamp_ns, base_value long) timestamp(nano_ts) partition by DAY");
            execute("INSERT INTO math_test VALUES(1, '2021-01-01T12:00:00.000000100Z'::timestamp_ns, 1000)");
            execute("INSERT INTO math_test VALUES(2, '2021-01-01T12:00:00.000000250Z'::timestamp_ns, 2000)");
            execute("INSERT INTO math_test VALUES(3, '2021-01-01T12:00:00.000000375Z'::timestamp_ns, 1500)");
            execute("INSERT INTO math_test VALUES(4, '2021-01-01T12:00:00.000000500Z'::timestamp_ns, 3000)");

            String expected = "id\tnano_ts\tbase_value\tmodulo_result\tcyclic_pattern\ttime_ratio\n" +
                    "1\t2021-01-01T12:00:00.000000100Z\t1000\t100\t100\t100.0\n" +
                    "2\t2021-01-01T12:00:00.000000250Z\t2000\t250\t50\t125.0\n" +
                    "3\t2021-01-01T12:00:00.000000375Z\t1500\t375\t175\t250.0\n" +
                    "4\t2021-01-01T12:00:00.000000500Z\t3000\t500\t0\t166.66666666666666\n";
            String query = "select id, nano_ts, base_value, " +
                    "nano_ts % 1000 modulo_result, " +
                    "(nano_ts % 500) % 200 cyclic_pattern, " +
                    "(nano_ts % 1000) / base_value::double * 1000 time_ratio " +
                    "from math_test order by nano_ts";
            assertQuery(expected, query, "nano_ts", true, true);
        });
    }

    @Test
    public void testComplexTimeSeriesAnalysis() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table prices(symbol symbol, price double, volume long, time timestamp_ns) timestamp(time) partition by DAY");

            execute("INSERT INTO prices VALUES('AAPL', 150.10, 1000, '2021-01-01T09:00:00.000000100Z'::timestamp_ns)");
            execute("INSERT INTO prices VALUES('AAPL', 150.15, 1500, '2021-01-01T09:00:00.000000200Z'::timestamp_ns)");
            execute("INSERT INTO prices VALUES('AAPL', 150.20, 2000, '2021-01-01T09:00:00.000000300Z'::timestamp_ns)");

            String expected = "symbol\ttime\tprice\tvolume\ttotal_volume\n" +
                    "AAPL\t2021-01-01T09:00:00.000000100Z\t150.1\t1000\t1000.0\n" +
                    "AAPL\t2021-01-01T09:00:00.000000200Z\t150.15\t1500\t2500.0\n" +
                    "AAPL\t2021-01-01T09:00:00.000000300Z\t150.2\t2000\t4500.0\n";

            String query = "select symbol, time, price, volume, " +
                    "sum(volume) over (partition by symbol order by time rows unbounded preceding) total_volume " +
                    "from prices order by symbol, time";
            assertQuery(expected, query, null, true, false);
        });
    }

    @Test
    public void testConversionFromMicros() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table micro_table(id int, time timestamp) timestamp(time) partition by DAY");
            execute("INSERT INTO micro_table VALUES(1, '2021-01-01T00:00:00.123456Z'::timestamp)");

            String expected = "id\ttime\ttime_ns\n" +
                    "1\t2021-01-01T00:00:00.123456Z\t2021-01-01T00:00:00.123456000Z\n";
            String query = "select id, time, cast(time as timestamp_ns) time_ns from micro_table";
            assertQuery(expected, query, "time", true, true);
        });
    }

    @Test
    public void testConversionToMicros() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table nano_table(id int, time timestamp_ns) timestamp(time) partition by DAY");
            execute("INSERT INTO nano_table VALUES(1, '2021-01-01T00:00:00.123456789Z'::timestamp_ns)");

            String expected = "id\ttime\ttime_micro\n" +
                    "1\t2021-01-01T00:00:00.123456789Z\t2021-01-01T00:00:00.123456Z\n";
            String query = "select id, time, cast(time as timestamp) time_micro from nano_table";
            assertQuery(expected, query, "time", true, true);
        });
    }

    @Test
    public void testCrossPrecisionAsofJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table micro_events(id int, micro_time timestamp, price double) timestamp(micro_time) partition by DAY");
            execute("create table nano_trades(trade_id int, nano_time timestamp_ns, quantity long) timestamp(nano_time) partition by DAY");

            execute("INSERT INTO micro_events VALUES(1, '2021-01-01T09:00:00.123456Z'::timestamp, 100.5)");
            execute("INSERT INTO micro_events VALUES(2, '2021-01-01T09:00:00.125000Z'::timestamp, 101.2)");
            execute("INSERT INTO micro_events VALUES(3, '2021-01-01T09:00:00.127500Z'::timestamp, 99.8)");

            execute("INSERT INTO nano_trades VALUES(101, '2021-01-01T09:00:00.123456100Z'::timestamp_ns, 1000)");
            execute("INSERT INTO nano_trades VALUES(102, '2021-01-01T09:00:00.125000500Z'::timestamp_ns, 2000)");
            execute("INSERT INTO nano_trades VALUES(103, '2021-01-01T09:00:00.126000200Z'::timestamp_ns, 1500)");

            String expected = "trade_id\tnano_time\tquantity\tid\tmicro_time\tprice\n" +
                    "101\t2021-01-01T09:00:00.123456100Z\t1000\t1\t2021-01-01T09:00:00.123456Z\t100.5\n" +
                    "102\t2021-01-01T09:00:00.125000500Z\t2000\t2\t2021-01-01T09:00:00.125000Z\t101.2\n" +
                    "103\t2021-01-01T09:00:00.126000200Z\t1500\t2\t2021-01-01T09:00:00.125000Z\t101.2\n";
            String query = "select t.trade_id, t.nano_time, t.quantity, e.id, e.micro_time, e.price " +
                    "from nano_trades t asof join micro_events e " +
                    "order by t.nano_time";
            assertQuery(expected, query, "nano_time", false, true);
        });
    }

    @Test
    public void testDistinctOperations() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table xyz(id int, time timestamp_ns) timestamp(time) partition by DAY");
            execute("INSERT INTO xyz VALUES(1, '2021-01-01T00:00:00.000000123Z'::timestamp_ns)");
            execute("INSERT INTO xyz VALUES(2, '2021-01-01T00:00:00.000000123Z'::timestamp_ns)");
            execute("INSERT INTO xyz VALUES(3, '2021-01-01T00:00:00.000000456Z'::timestamp_ns)");
            execute("INSERT INTO xyz VALUES(4, '2021-01-01T00:00:00.000000456Z'::timestamp_ns)");
            execute("INSERT INTO xyz VALUES(5, '2021-01-01T00:00:00.000000789Z'::timestamp_ns)");

            String expected = "time\n" +
                    "2021-01-01T00:00:00.000000123Z\n" +
                    "2021-01-01T00:00:00.000000456Z\n" +
                    "2021-01-01T00:00:00.000000789Z\n";
            String query = "select distinct time from xyz order by time";
            assertQuery(expected, query, "time", true, true);
        });
    }

    @Test
    public void testGroupByOperations() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table xyz(symbol symbol, value double, time timestamp_ns) timestamp(time) partition by DAY");
            execute("INSERT INTO xyz VALUES('A', 10.5, '2021-01-01T00:00:00.000000100Z'::timestamp_ns)");
            execute("INSERT INTO xyz VALUES('A', 20.3, '2021-01-01T00:00:00.000000200Z'::timestamp_ns)");
            execute("INSERT INTO xyz VALUES('B', 30.7, '2021-01-01T00:00:00.000000150Z'::timestamp_ns)");
            execute("INSERT INTO xyz VALUES('B', 40.1, '2021-01-01T00:00:00.000000250Z'::timestamp_ns)");

            String expected = "symbol\tsum_value\tmin_time\tmax_time\tcount\n" +
                    "A\t30.8\t2021-01-01T00:00:00.000000100Z\t2021-01-01T00:00:00.000000200Z\t2\n" +
                    "B\t70.8\t2021-01-01T00:00:00.000000150Z\t2021-01-01T00:00:00.000000250Z\t2\n";
            String query = "select symbol, sum(value) sum_value, min(time) min_time, max(time) max_time, count(*) from xyz group by symbol order by symbol";
            assertQuery(expected, query, null, true, true);
        });
    }

    @Test
    public void testInOperations() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table xyz(id int, time timestamp_ns) timestamp(time) partition by DAY");
            execute("INSERT INTO xyz VALUES(1, '2021-01-01T00:00:00.000000100Z'::timestamp_ns)");
            execute("INSERT INTO xyz VALUES(2, '2021-01-01T00:00:00.000000200Z'::timestamp_ns)");
            execute("INSERT INTO xyz VALUES(3, '2021-01-01T00:00:00.000000300Z'::timestamp_ns)");
            execute("INSERT INTO xyz VALUES(4, '2021-01-01T00:00:00.000000400Z'::timestamp_ns)");

            String expected = "id\ttime\n" +
                    "2\t2021-01-01T00:00:00.000000200Z\n" +
                    "4\t2021-01-01T00:00:00.000000400Z\n";
            String query = "select id, time from xyz where time in ('2021-01-01T00:00:00.000000200Z'::timestamp_ns, '2021-01-01T00:00:00.000000400Z'::timestamp_ns) order by time";
            assertQuery(expected, query, "time", true, false);
        });
    }

    @Test
    public void testIndexOperations() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table xyz(symbol symbol index, id int, time timestamp_ns) timestamp(time) partition by DAY");
            execute("INSERT INTO xyz VALUES('A', 1, '2021-01-01T00:00:00.000000100Z'::timestamp_ns)");
            execute("INSERT INTO xyz VALUES('B', 2, '2021-01-01T00:00:00.000000200Z'::timestamp_ns)");
            execute("INSERT INTO xyz VALUES('A', 3, '2021-01-01T00:00:00.000000300Z'::timestamp_ns)");
            execute("INSERT INTO xyz VALUES('C', 4, '2021-01-01T00:00:00.000000400Z'::timestamp_ns)");
            execute("INSERT INTO xyz VALUES('B', 5, '2021-01-01T00:00:00.000000500Z'::timestamp_ns)");

            String expected = "symbol\tid\ttime\n" +
                    "A\t1\t2021-01-01T00:00:00.000000100Z\n" +
                    "A\t3\t2021-01-01T00:00:00.000000300Z\n";
            String query = "select symbol, id, time from xyz where symbol = 'A' order by time";
            assertQuery(expected, query, "time", true, false);
        });
    }

    @Test
    public void testInvalidTimestampFormat() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table xyz(id int, time timestamp_ns) timestamp(time) partition by DAY");

            try {
                execute("INSERT INTO xyz VALUES(1, 'invalid-timestamp'::timestamp_ns)");
                Assert.fail("Expected SqlException");
            } catch (Exception e) {
                TestUtils.assertContains(e.getMessage().toLowerCase(), "cannot be null");
            }
        });
    }

    @Test
    public void testMinMaxAggregation() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table xyz(id int, time timestamp_ns) timestamp(time) partition by DAY");
            execute("INSERT INTO xyz VALUES(1, '2021-01-01T00:00:00.000000100Z'::timestamp_ns)");
            execute("INSERT INTO xyz VALUES(2, '2021-01-01T00:00:00.000000500Z'::timestamp_ns)");
            execute("INSERT INTO xyz VALUES(3, '2021-01-01T00:00:00.000000300Z'::timestamp_ns)");

            String expected = "min_time\tmax_time\tcount\n" +
                    "2021-01-01T00:00:00.000000100Z\t2021-01-01T00:00:00.000000500Z\t3\n";
            String query = "select min(time) min_time, max(time) max_time, count(*) from xyz";
            assertQuery(expected, query, null, false, true);
        });
    }

    @Test
    public void testMixedPrecision() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table events(event_id int, micro_ts timestamp, nano_ts timestamp_ns, value double) timestamp(nano_ts) partition by DAY");
            String t0us = "2021-01-01T10:00:00.123456Z";
            String t1us = "2021-01-01T10:00:01.123456Z";
            String t2us = "2021-01-01T10:00:02.123456Z";

            String t0ns = "2021-01-01T10:00:00.123456789Z";
            String t1ns = "2021-01-01T10:00:01.123456800Z";
            String t2ns = "2021-01-01T10:00:02.123456900Z";

            execute("INSERT INTO events VALUES(1, '" + t0us + "'::timestamp, '" + t0ns + "'::timestamp_ns, 100.0)");
            execute("INSERT INTO events VALUES(2, '" + t1us + "'::timestamp, '" + t1ns + "'::timestamp_ns, 200.0)");
            execute("INSERT INTO events VALUES(3, '" + t2us + "'::timestamp, '" + t2ns + "'::timestamp_ns, 150.0)");

            String expected = "event_id\tmicro_ts\tnano_ts\tvalue\tround_nanos\tdiff_nano_to_micro\n" +
                    "1\t" + t0us + "\t" + t0ns + "\t100.0\t789\t789\n" +
                    "2\t" + t1us + "\t" + t1ns + "\t200.0\t800\t800\n" +
                    "3\t" + t2us + "\t" + t2ns + "\t150.0\t900\t900\n";
            String query = "select event_id, micro_ts, nano_ts, value, " +
                    "nano_ts % 1000 round_nanos, " +
                    "(nano_ts - micro_ts::timestamp_ns) diff_nano_to_micro " +
                    "from events order by nano_ts";
            assertQuery(expected, query, "nano_ts", true, true);
        });
    }

    @Test
    public void testNullHandling() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table xyz(id int, time timestamp_ns, other_time timestamp_ns) timestamp(time) partition by DAY");
            execute("INSERT INTO xyz VALUES(1, '2021-01-01T00:00:00.000000100Z'::timestamp_ns, null)");
            execute("INSERT INTO xyz VALUES(2, '2021-01-01T00:00:00.000000123Z'::timestamp_ns, '2021-01-01T00:00:00.000000456Z'::timestamp_ns)");

            String expected = "id\ttime\tother_time\n" +
                    "1\t2021-01-01T00:00:00.000000100Z\t\n" +
                    "2\t2021-01-01T00:00:00.000000123Z\t2021-01-01T00:00:00.000000456Z\n";
            String query = "select id, time, other_time from xyz order by time";
            assertQuery(expected, query, "time", true, true);
        });
    }

    @Test
    public void testOrdering() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table xyz(id int, time timestamp_ns) timestamp(time) partition by DAY");
            execute("INSERT INTO xyz VALUES" +
                    "  (5, '2021-01-01T00:00:00.000000500Z'::timestamp_ns)" +
                    ", (1, '2021-01-01T00:00:00.000000100Z'::timestamp_ns)" +
                    ", (3, '2021-01-01T00:00:00.000000300Z'::timestamp_ns)" +
                    ", (2, '2021-01-01T00:00:00.000000200Z'::timestamp_ns)" +
                    ", (4, '2021-01-01T00:00:00.000000400Z'::timestamp_ns)");

            String expectedAsc = "id\ttime\n" +
                    "1\t2021-01-01T00:00:00.000000100Z\n" +
                    "2\t2021-01-01T00:00:00.000000200Z\n" +
                    "3\t2021-01-01T00:00:00.000000300Z\n" +
                    "4\t2021-01-01T00:00:00.000000400Z\n" +
                    "5\t2021-01-01T00:00:00.000000500Z\n";
            assertQuery(expectedAsc, "select id, time from xyz order by time", "time", true, true);

            String expectedDesc = "id\ttime\n" +
                    "5\t2021-01-01T00:00:00.000000500Z\n" +
                    "4\t2021-01-01T00:00:00.000000400Z\n" +
                    "3\t2021-01-01T00:00:00.000000300Z\n" +
                    "2\t2021-01-01T00:00:00.000000200Z\n" +
                    "1\t2021-01-01T00:00:00.000000100Z\n";
            assertQuery(expectedDesc, "select id, time from xyz order by time desc",
                    "time###desc", true, true);
        });
    }

    @Test
    public void testPartitionByDay() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table xyz(id int, time timestamp_ns) timestamp(time) partition by DAY");
            execute("INSERT INTO xyz VALUES(1, '2021-01-01T23:59:59.999999999Z'::timestamp_ns)");
            execute("INSERT INTO xyz VALUES(2, '2021-01-02T00:00:00.000000001Z'::timestamp_ns)");
            execute("INSERT INTO xyz VALUES(3, '2021-01-02T12:00:00.000000000Z'::timestamp_ns)");
            execute("INSERT INTO xyz VALUES(4, '2021-01-03T00:00:00.000000000Z'::timestamp_ns)");

            String expected = "id\ttime\n" +
                    "1\t2021-01-01T23:59:59.999999999Z\n" +
                    "2\t2021-01-02T00:00:00.000000001Z\n" +
                    "3\t2021-01-02T12:00:00.000000000Z\n" +
                    "4\t2021-01-03T00:00:00.000000000Z\n";
            String query = "select id, time from xyz order by time";
            assertQuery(expected, query, "time", true, true);
        });
    }

    @Test
    public void testPartitionByHour() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table xyz(id int, time timestamp_ns) timestamp(time) partition by HOUR");
            execute("INSERT INTO xyz VALUES(1, '2021-01-01T00:59:59.999999999Z'::timestamp_ns)");
            execute("INSERT INTO xyz VALUES(2, '2021-01-01T01:00:00.000000001Z'::timestamp_ns)");
            execute("INSERT INTO xyz VALUES(3, '2021-01-01T01:30:00.000000000Z'::timestamp_ns)");
            execute("INSERT INTO xyz VALUES(4, '2021-01-01T02:00:00.000000000Z'::timestamp_ns)");

            String expected = "id\ttime\n" +
                    "1\t2021-01-01T00:59:59.999999999Z\n" +
                    "2\t2021-01-01T01:00:00.000000001Z\n" +
                    "3\t2021-01-01T01:30:00.000000000Z\n" +
                    "4\t2021-01-01T02:00:00.000000000Z\n";
            String query = "select id, time from xyz order by time";
            assertQuery(expected, query, "time", true, true);
        });
    }

    @Test
    public void testPrecisionBoundaries() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table xyz(id int, time timestamp_ns) timestamp(time) partition by DAY");
            execute("INSERT INTO xyz VALUES(1, '1970-01-01T00:00:00.000000000Z'::timestamp_ns)");
            execute("INSERT INTO xyz VALUES(2, '1970-01-01T00:00:00.000000001Z'::timestamp_ns)");
            execute("INSERT INTO xyz VALUES(3, '1970-01-01T00:00:00.000000999Z'::timestamp_ns)");
            execute("INSERT INTO xyz VALUES(4, '1970-01-01T00:00:00.000001000Z'::timestamp_ns)");
            execute("INSERT INTO xyz VALUES(5, '1970-01-01T00:00:00.999999999Z'::timestamp_ns)");

            String expected = "id\ttime\n" +
                    "1\t1970-01-01T00:00:00.000000000Z\n" +
                    "2\t1970-01-01T00:00:00.000000001Z\n" +
                    "3\t1970-01-01T00:00:00.000000999Z\n" +
                    "4\t1970-01-01T00:00:00.000001000Z\n" +
                    "5\t1970-01-01T00:00:00.999999999Z\n";
            String query = "select id, time from xyz order by time";
            assertQuery(expected, query, "time", true, true);
        });
    }

    @Test
    public void testSampleByNanosecondIntervals() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table high_freq_data(sensor_id int, reading double, nano_ts timestamp_ns) timestamp(nano_ts) partition by DAY");
            execute("INSERT INTO high_freq_data VALUES(1, 25.1, '2021-01-01T15:00:00.000000100Z'::timestamp_ns)");
            execute("INSERT INTO high_freq_data VALUES(1, 25.3, '2021-01-01T15:00:00.000000150Z'::timestamp_ns)");
            execute("INSERT INTO high_freq_data VALUES(1, 25.2, '2021-01-01T15:00:00.000000200Z'::timestamp_ns)");
            execute("INSERT INTO high_freq_data VALUES(1, 25.4, '2021-01-01T15:00:00.000000250Z'::timestamp_ns)");
            execute("INSERT INTO high_freq_data VALUES(1, 25.0, '2021-01-01T15:00:00.000000300Z'::timestamp_ns)");
            execute("INSERT INTO high_freq_data VALUES(1, 25.6, '2021-01-01T15:00:00.000000350Z'::timestamp_ns)");

            String expected = "nano_ts\tavg_reading\tmax_reading\tmin_reading\tcount\n" +
                    "2021-01-01T15:00:00.000000100Z\t25.2\t25.3\t25.1\t2\n" +
                    "2021-01-01T15:00:00.000000200Z\t25.3\t25.4\t25.2\t2\n" +
                    "2021-01-01T15:00:00.000000300Z\t25.3\t25.6\t25.0\t2\n";
            String query = "select nano_ts, round(avg(reading), 2) avg_reading, max(reading) max_reading, min(reading) min_reading, count(*) " +
                    "from high_freq_data sample by 100n fill(none)";
            assertQuery(expected, query, "nano_ts", true, true);
        });
    }

    @Test
    public void testUpdateOperations() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table xyz(id int, time timestamp_ns, value double) timestamp(time) partition by DAY");
            execute("INSERT INTO xyz VALUES(1, '2021-01-01T00:00:00.000000100Z'::timestamp_ns, 10.5)");
            execute("INSERT INTO xyz VALUES(2, '2021-01-01T00:00:00.000000200Z'::timestamp_ns, 20.3)");
            execute("INSERT INTO xyz VALUES(3, '2021-01-01T00:00:00.000000300Z'::timestamp_ns, 30.7)");

            execute("UPDATE xyz SET value = 99.9 WHERE time = '2021-01-01T00:00:00.000000200Z'::timestamp_ns");

            String expected = "id\ttime\tvalue\n" +
                    "1\t2021-01-01T00:00:00.000000100Z\t10.5\n" +
                    "2\t2021-01-01T00:00:00.000000200Z\t99.9\n" +
                    "3\t2021-01-01T00:00:00.000000300Z\t30.7\n";
            String query = "select id, time, value from xyz order by time";
            assertQuery(expected, query, "time", true, true);
        });
    }

    @Test
    public void testUpdateWithTimeRange() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table xyz(id int, time timestamp_ns, value double) timestamp(time) partition by DAY");
            execute("INSERT INTO xyz VALUES(1, '2021-01-01T00:00:00.000000100Z'::timestamp_ns, 10.5)");
            execute("INSERT INTO xyz VALUES(2, '2021-01-01T00:00:00.000000200Z'::timestamp_ns, 20.3)");
            execute("INSERT INTO xyz VALUES(3, '2021-01-01T00:00:00.000000300Z'::timestamp_ns, 30.7)");
            execute("INSERT INTO xyz VALUES(4, '2021-01-01T00:00:00.000000400Z'::timestamp_ns, 40.1)");

            execute("UPDATE xyz SET value = value * 2 WHERE time >= '2021-01-01T00:00:00.000000200Z'::timestamp_ns AND time <= '2021-01-01T00:00:00.000000300Z'::timestamp_ns");

            String expected = "id\ttime\tvalue\n" +
                    "1\t2021-01-01T00:00:00.000000100Z\t10.5\n" +
                    "2\t2021-01-01T00:00:00.000000200Z\t40.6\n" +
                    "3\t2021-01-01T00:00:00.000000300Z\t61.4\n" +
                    "4\t2021-01-01T00:00:00.000000400Z\t40.1\n";
            String query = "select id, time, value from xyz order by time";
            assertQuery(expected, query, "time", true, true);
        });
    }

    @Test
    public void testWindowRowCountBoundaryFunctions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table time_series(id int, value double, nano_time timestamp_ns) timestamp(nano_time) partition by DAY");
            execute("INSERT INTO time_series VALUES(1, 10.5, '2021-01-01T09:00:00.000000100Z'::timestamp_ns)");
            execute("INSERT INTO time_series VALUES(2, 15.2, '2021-01-01T09:00:00.000000200Z'::timestamp_ns)");
            execute("INSERT INTO time_series VALUES(3, 12.8, '2021-01-01T09:00:00.000000300Z'::timestamp_ns)");
            execute("INSERT INTO time_series VALUES(4, 18.1, '2021-01-01T09:00:00.000000400Z'::timestamp_ns)");
            execute("INSERT INTO time_series VALUES(5, 14.7, '2021-01-01T09:00:00.000000500Z'::timestamp_ns)");

            String expected = "id\tvalue\tnano_time\tfirst_val\tlast_val\tlag_val\tlead_val\n" +
                    "1\t10.5\t2021-01-01T09:00:00.000000100Z\t10.5\t10.5\tnull\t15.2\n" +
                    "2\t15.2\t2021-01-01T09:00:00.000000200Z\t10.5\t15.2\t10.5\t12.8\n" +
                    "3\t12.8\t2021-01-01T09:00:00.000000300Z\t15.2\t12.8\t15.2\t18.1\n" +
                    "4\t18.1\t2021-01-01T09:00:00.000000400Z\t12.8\t18.1\t12.8\t14.7\n" +
                    "5\t14.7\t2021-01-01T09:00:00.000000500Z\t18.1\t14.7\t18.1\tnull\n";
            String query = "select id, value, nano_time, " +
                    "first_value(value) over (order by nano_time rows between 1 preceding and current row) first_val, " +
                    "last_value(value) over (order by nano_time rows between 1 preceding and current row) last_val, " +
                    "lag(value, 1) over (order by nano_time) lag_val, " +
                    "lead(value, 1) over (order by nano_time) lead_val " +
                    "from time_series order by nano_time";
            assertQuery(expected, query, "nano_time", true, false);
        });
    }
}
