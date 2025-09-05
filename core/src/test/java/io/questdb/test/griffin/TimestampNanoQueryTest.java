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
    public void testAlterColumnTypeFromLongToNanos() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango(id INT, long_time LONG, time TIMESTAMP_NS) TIMESTAMP(time) PARTITION BY DAY");
            execute("INSERT INTO tango VALUES(1, 123456789, '2021-01-01T00:00:00.000000100Z'::TIMESTAMP_NS)");
            execute("INSERT INTO tango VALUES(2, 789123456, '2021-01-01T00:00:00.000000200Z'::TIMESTAMP_NS)");

            String beforeExpected = "id\tlong_time\ttime\n" +
                    "1\t123456789\t2021-01-01T00:00:00.000000100Z\n" +
                    "2\t789123456\t2021-01-01T00:00:00.000000200Z\n";
            String beforeQuery = "SELECT id, long_time, time FROM tango";
            assertQuery(beforeExpected, beforeQuery, "time", true, true);

            execute("ALTER TABLE tango ALTER COLUMN long_time TYPE TIMESTAMP_NS");

            String afterExpected = "id\tlong_time\ttime\n" +
                    "1\t1970-01-01T00:00:00.123456789Z\t2021-01-01T00:00:00.000000100Z\n" +
                    "2\t1970-01-01T00:00:00.789123456Z\t2021-01-01T00:00:00.000000200Z\n";
            String afterQuery = "SELECT id, long_time, time FROM tango";
            assertQuery(afterExpected, afterQuery, "time", true, true);
        });
    }

    @Test
    public void testAlterColumnTypeToMicros() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango(id INT, nano_time TIMESTAMP_NS, time TIMESTAMP_NS) TIMESTAMP(time) PARTITION BY DAY");
            execute("INSERT INTO tango VALUES(1, '2021-01-01T00:00:00.123456789Z'::TIMESTAMP_NS, '2021-01-01T00:00:00.000000100Z'::TIMESTAMP_NS)");
            execute("INSERT INTO tango VALUES(2, '2021-01-01T00:00:00.987654321Z'::TIMESTAMP_NS, '2021-01-01T00:00:00.000000200Z'::TIMESTAMP_NS)");

            String beforeExpected = "id\tnano_time\ttime\n" +
                    "1\t2021-01-01T00:00:00.123456789Z\t2021-01-01T00:00:00.000000100Z\n" +
                    "2\t2021-01-01T00:00:00.987654321Z\t2021-01-01T00:00:00.000000200Z\n";
            String beforeQuery = "SELECT id, nano_time, time FROM tango ORDER BY time";
            assertQuery(beforeExpected, beforeQuery, "time", true, true);

            execute("ALTER TABLE tango ALTER COLUMN nano_time TYPE TIMESTAMP");

            String afterExpected = "id\tnano_time\ttime\n" +
                    "1\t2021-01-01T00:00:00.123456Z\t2021-01-01T00:00:00.000000100Z\n" +
                    "2\t2021-01-01T00:00:00.987654Z\t2021-01-01T00:00:00.000000200Z\n";
            String afterQuery = "SELECT id, nano_time, time FROM tango ORDER BY time";
            assertQuery(afterExpected, afterQuery, "time", true, true);
        });
    }

    @Test
    public void testAlterColumnTypeToNanos() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango(id INT, micro_time TIMESTAMP, time TIMESTAMP_NS) TIMESTAMP(time) PARTITION BY DAY");
            execute("INSERT INTO tango VALUES(1, '2021-01-01T00:00:00.123456Z'::TIMESTAMP, '2021-01-01T00:00:00.000000100Z'::TIMESTAMP_NS)");
            execute("INSERT INTO tango VALUES(2, '2021-01-01T00:00:00.789123Z'::TIMESTAMP, '2021-01-01T00:00:00.000000200Z'::TIMESTAMP_NS)");

            String beforeExpected = "id\tmicro_time\ttime\n" +
                    "1\t2021-01-01T00:00:00.123456Z\t2021-01-01T00:00:00.000000100Z\n" +
                    "2\t2021-01-01T00:00:00.789123Z\t2021-01-01T00:00:00.000000200Z\n";
            String beforeQuery = "SELECT id, micro_time, time FROM tango ORDER BY time";
            assertQuery(beforeExpected, beforeQuery, "time", true, true);

            execute("ALTER TABLE tango ALTER COLUMN micro_time TYPE TIMESTAMP_NS");

            String afterExpected = "id\tmicro_time\ttime\n" +
                    "1\t2021-01-01T00:00:00.123456000Z\t2021-01-01T00:00:00.000000100Z\n" +
                    "2\t2021-01-01T00:00:00.789123000Z\t2021-01-01T00:00:00.000000200Z\n";
            String afterQuery = "SELECT id, micro_time, time FROM tango ORDER BY time";
            assertQuery(afterExpected, afterQuery, "time", true, true);
        });
    }

    @Test
    public void testAlterTableAddColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango(id INT, time TIMESTAMP_NS) TIMESTAMP(time) PARTITION BY DAY");
            execute("INSERT INTO tango VALUES(1, '2021-01-01T00:00:00.000000100Z'::TIMESTAMP_NS)");

            execute("ALTER TABLE tango ADD COLUMN new_time TIMESTAMP_NS");
            execute("UPDATE tango SET new_time = 200::TIMESTAMP_NS WHERE id = 1");

            String expected = "id\ttime\tnew_time\n" +
                    "1\t2021-01-01T00:00:00.000000100Z\t1970-01-01T00:00:00.000000200Z\n";
            String query = "SELECT id, time, new_time FROM tango";
            assertQuery(expected, query, "time", true, true);
        });
    }

    @Test
    public void testAlterTableDropColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango(id INT, time TIMESTAMP_NS, temp_time TIMESTAMP_NS) TIMESTAMP(time) PARTITION BY DAY");
            execute("INSERT INTO tango VALUES(1, '2021-01-01T00:00:00.000000100Z'::TIMESTAMP_NS, '2021-01-01T00:00:00.000000200Z'::TIMESTAMP_NS)");

            execute("ALTER TABLE tango DROP COLUMN temp_time");

            String expected = "id\ttime\n" +
                    "1\t2021-01-01T00:00:00.000000100Z\n";
            String query = "SELECT id, time FROM tango";
            assertQuery(expected, query, "time", true, true);
        });
    }

    @Test
    public void testArithmetic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango(id INT, time TIMESTAMP_NS) TIMESTAMP(time) PARTITION BY DAY");
            execute("INSERT INTO tango VALUES(1, '2021-01-01T00:00:00.000000100Z'::TIMESTAMP_NS)");

            String expected = "id\ttime\tadd_nanos\tsub_nanos\n" +
                    "1\t2021-01-01T00:00:00.000000100Z\t2021-01-01T00:00:00.000000200Z\t2021-01-01T00:00:00.000000000Z\n";
            String query = "SELECT id, time, time + 100L add_nanos, time - 100L sub_nanos FROM tango";
            assertQuery(expected, query, "time", true, true);
        });
    }

    @Test
    public void testBasicComparison() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango(id INT, time TIMESTAMP_NS) TIMESTAMP(time) PARTITION BY DAY");
            execute("INSERT INTO tango VALUES(1, '2021-01-01T00:00:00.000000123Z'::TIMESTAMP_NS)");
            execute("INSERT INTO tango VALUES(2, '2021-01-01T00:00:00.000000456Z'::TIMESTAMP_NS)");
            execute("INSERT INTO tango VALUES(3, '2021-01-01T00:00:00.000000789Z'::TIMESTAMP_NS)");

            String expected = "id\ttime\n" +
                    "2\t2021-01-01T00:00:00.000000456Z\n" +
                    "3\t2021-01-01T00:00:00.000000789Z\n";
            String query = "SELECT id, time FROM tango where time > '2021-01-01T00:00:00.000000123Z'::TIMESTAMP_NS ORDER BY time";
            assertQuery(expected, query, "time", true, false);
        });
    }

    @Test
    public void testBasicEquality() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango(id INT, time TIMESTAMP_NS) TIMESTAMP(time) PARTITION BY DAY");
            execute("INSERT INTO tango VALUES(1, '2021-01-01T00:00:00.000000123Z'::TIMESTAMP_NS)");
            execute("INSERT INTO tango VALUES(2, '2021-01-01T00:00:00.000000123Z'::TIMESTAMP_NS)");
            execute("INSERT INTO tango VALUES(3, '2021-01-01T00:00:00.000000456Z'::TIMESTAMP_NS)");

            String expected = "id\ttime\n" +
                    "1\t2021-01-01T00:00:00.000000123Z\n" +
                    "2\t2021-01-01T00:00:00.000000123Z\n";
            String query = "SELECT id, time FROM tango where time = '2021-01-01T00:00:00.000000123Z'::TIMESTAMP_NS ORDER BY id";
            assertQuery(expected, query, null, true, false);
        });
    }

    @Test
    public void testBasicInsertAndSelect() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango(id INT, time TIMESTAMP_NS) TIMESTAMP(time) PARTITION BY DAY");
            execute("INSERT INTO tango VALUES(1, '2021-01-01T00:00:00.000000123Z'::TIMESTAMP_NS)");
            execute("INSERT INTO tango VALUES(2, '2021-01-01T00:00:00.000000456Z'::TIMESTAMP_NS)");

            String expected = "id\ttime\n" +
                    "1\t2021-01-01T00:00:00.000000123Z\n" +
                    "2\t2021-01-01T00:00:00.000000456Z\n";
            String query = "SELECT id, time FROM tango ORDER BY time";
            assertQuery(expected, query, "time", true, true);
        });
    }

    @Test
    public void testBetweenWithMixedTimestampPrecisions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE mixed_timestamps(id INT, arg_ts TIMESTAMP_NS, from_ts TIMESTAMP, to_ts TIMESTAMP)");

            execute("INSERT INTO mixed_timestamps VALUES(1, '1970-01-01T10:00:00.123456789Z', '1970-01-01T10:00:00.120000Z', '1970-01-01T10:00:00.125000Z')");
            execute("INSERT INTO mixed_timestamps VALUES(2, '1970-01-01T10:00:00.150000000Z', '1970-01-01T10:00:00.149999Z', '1970-01-01T10:00:00.150000Z')");
            execute("INSERT INTO mixed_timestamps VALUES(3, '1970-01-01T10:00:00.200000000Z', '1970-01-01T10:00:00.180000Z', '1970-01-01T10:00:00.199999Z')");
            execute("INSERT INTO mixed_timestamps VALUES(4, '1970-01-01T10:00:00.250000000Z', '1970-01-01T10:00:00.250001Z', '1970-01-01T10:00:00.260000Z')");
            execute("INSERT INTO mixed_timestamps VALUES(4, null, '1970-01-01T10:00:00.250001Z', '1970-01-01T10:00:00.260000Z')");
            execute("INSERT INTO mixed_timestamps VALUES(4, '1970-01-01T10:00:00.250000000Z', null, '1970-01-01T10:00:00.260000Z')");
            execute("INSERT INTO mixed_timestamps VALUES(4, '1970-01-01T10:00:00.250000000Z', '1970-01-01T10:00:00.250001Z', null)");

            String expected = "id\targ_ts\tfrom_ts\tto_ts\tin_range\n" +
                    "1\t1970-01-01T10:00:00.123456789Z\t1970-01-01T10:00:00.120000Z\t1970-01-01T10:00:00.125000Z\ttrue\n" +
                    "2\t1970-01-01T10:00:00.150000000Z\t1970-01-01T10:00:00.149999Z\t1970-01-01T10:00:00.150000Z\ttrue\n" +
                    "3\t1970-01-01T10:00:00.200000000Z\t1970-01-01T10:00:00.180000Z\t1970-01-01T10:00:00.199999Z\tfalse\n" +
                    "4\t1970-01-01T10:00:00.250000000Z\t1970-01-01T10:00:00.250001Z\t1970-01-01T10:00:00.260000Z\tfalse\n" +
                    "4\t\t1970-01-01T10:00:00.250001Z\t1970-01-01T10:00:00.260000Z\tfalse\n" +
                    "4\t1970-01-01T10:00:00.250000000Z\t\t1970-01-01T10:00:00.260000Z\tfalse\n" +
                    "4\t1970-01-01T10:00:00.250000000Z\t1970-01-01T10:00:00.250001Z\t\tfalse\n";

            String query = "SELECT id, arg_ts, from_ts, to_ts, " +
                    "arg_ts between from_ts AND to_ts in_range " +
                    "FROM mixed_timestamps";
            assertQuery(expected, query, null, true, true);

            String expectedReverse = "id\targ_ts\tfrom_ts\tto_ts\tin_range_rev\n" +
                    "1\t1970-01-01T10:00:00.123456789Z\t1970-01-01T10:00:00.120000Z\t1970-01-01T10:00:00.125000Z\ttrue\n" +
                    "2\t1970-01-01T10:00:00.150000000Z\t1970-01-01T10:00:00.149999Z\t1970-01-01T10:00:00.150000Z\ttrue\n" +
                    "3\t1970-01-01T10:00:00.200000000Z\t1970-01-01T10:00:00.180000Z\t1970-01-01T10:00:00.199999Z\tfalse\n" +
                    "4\t1970-01-01T10:00:00.250000000Z\t1970-01-01T10:00:00.250001Z\t1970-01-01T10:00:00.260000Z\tfalse\n" +
                    "4\t\t1970-01-01T10:00:00.250001Z\t1970-01-01T10:00:00.260000Z\tfalse\n" +
                    "4\t1970-01-01T10:00:00.250000000Z\t\t1970-01-01T10:00:00.260000Z\tfalse\n" +
                    "4\t1970-01-01T10:00:00.250000000Z\t1970-01-01T10:00:00.250001Z\t\tfalse\n";

            String queryReverse = "SELECT id, arg_ts, from_ts, to_ts, " +
                    "arg_ts between to_ts AND from_ts in_range_rev " +
                    "FROM mixed_timestamps";
            assertQuery(expectedReverse, queryReverse, null, true, true);
        });
    }

    @Test
    public void testCTE() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades(id INT, symbol symbol, price DOUBLE, time TIMESTAMP_NS) TIMESTAMP(time) PARTITION BY DAY");
            execute("INSERT INTO trades VALUES(1, 'AAPL', 150.1, '2021-01-01T09:00:00.000000100Z'::TIMESTAMP_NS)");
            execute("INSERT INTO trades VALUES(2, 'AAPL', 150.5, '2021-01-01T09:00:00.000000300Z'::TIMESTAMP_NS)");
            execute("INSERT INTO trades VALUES(3, 'MSFT', 250.2, '2021-01-01T09:00:00.000000200Z'::TIMESTAMP_NS)");
            execute("INSERT INTO trades VALUES(4, 'MSFT', 250.8, '2021-01-01T09:00:00.000000400Z'::TIMESTAMP_NS)");

            String expected = "symbol\tavg_price\tmin_time\tmax_time\n" +
                    "AAPL\t150.3\t2021-01-01T09:00:00.000000100Z\t2021-01-01T09:00:00.000000300Z\n" +
                    "MSFT\t250.5\t2021-01-01T09:00:00.000000200Z\t2021-01-01T09:00:00.000000400Z\n";
            String query = "with symbol_stats as (" +
                    "  SELECT symbol, avg(price) avg_price, min(time) min_time, max(time) max_time " +
                    "  FROM trades " +
                    "  GROUP BY symbol" +
                    ") " +
                    "SELECT symbol, avg_price, min_time, max_time FROM symbol_stats ORDER BY symbol";
            assertQuery(expected, query, null, true, true);
        });
    }

    @Test
    public void testCTEWithTimeFiltering() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades(id INT, symbol symbol, price DOUBLE, time TIMESTAMP_NS) TIMESTAMP(time) PARTITION BY DAY");
            execute("INSERT INTO trades VALUES(1, 'AAPL', 150.1, '2021-01-01T09:00:00.000000100Z'::TIMESTAMP_NS)");
            execute("INSERT INTO trades VALUES(2, 'AAPL', 150.5, '2021-01-01T09:00:00.000000300Z'::TIMESTAMP_NS)");
            execute("INSERT INTO trades VALUES(3, 'MSFT', 250.2, '2021-01-01T09:00:00.000000200Z'::TIMESTAMP_NS)");
            execute("INSERT INTO trades VALUES(4, 'MSFT', 250.8, '2021-01-01T09:00:00.000000400Z'::TIMESTAMP_NS)");

            String expected = "symbol\tavg_price\ttrade_count\n" +
                    "AAPL\t150.5\t1\n" +
                    "MSFT\t250.8\t1\n";
            String query = "with recent_trades as (" +
                    "  SELECT * FROM trades " +
                    "  where time >= '2021-01-01T09:00:00.000000250Z'::TIMESTAMP_NS" +
                    ") " +
                    "SELECT symbol, avg(price) avg_price, count(*) trade_count " +
                    "FROM recent_trades GROUP BY symbol ORDER BY symbol";
            assertQuery(expected, query, null, true, true);
        });
    }

    @Test
    public void testComplexArithmetic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE math_test(id INT, nano_ts TIMESTAMP_NS, base_value LONG) TIMESTAMP(nano_ts) PARTITION BY DAY");
            execute("INSERT INTO math_test VALUES(1, '2021-01-01T12:00:00.000000100Z'::TIMESTAMP_NS, 1000)");
            execute("INSERT INTO math_test VALUES(2, '2021-01-01T12:00:00.000000250Z'::TIMESTAMP_NS, 2000)");
            execute("INSERT INTO math_test VALUES(3, '2021-01-01T12:00:00.000000375Z'::TIMESTAMP_NS, 1500)");
            execute("INSERT INTO math_test VALUES(4, '2021-01-01T12:00:00.000000500Z'::TIMESTAMP_NS, 3000)");

            String expected = "id\tnano_ts\tbase_value\tmodulo_result\tcyclic_pattern\ttime_ratio\n" +
                    "1\t2021-01-01T12:00:00.000000100Z\t1000\t100\t100\t100.0\n" +
                    "2\t2021-01-01T12:00:00.000000250Z\t2000\t250\t50\t125.0\n" +
                    "3\t2021-01-01T12:00:00.000000375Z\t1500\t375\t175\t250.0\n" +
                    "4\t2021-01-01T12:00:00.000000500Z\t3000\t500\t0\t166.66666666666666\n";
            String query = "SELECT id, nano_ts, base_value, " +
                    "nano_ts % 1000 modulo_result, " +
                    "(nano_ts % 500) % 200 cyclic_pattern, " +
                    "(nano_ts % 1000) / base_value::DOUBLE * 1000 time_ratio " +
                    "FROM math_test ORDER BY nano_ts";
            assertQuery(expected, query, "nano_ts", true, true);
        });
    }

    @Test
    public void testComplexTimeSeriesAnalysis() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE prices(symbol symbol, price DOUBLE, volume LONG, time TIMESTAMP_NS) TIMESTAMP(time) PARTITION BY DAY");

            execute("INSERT INTO prices VALUES('AAPL', 150.10, 1000, '2021-01-01T09:00:00.000000100Z'::TIMESTAMP_NS)");
            execute("INSERT INTO prices VALUES('AAPL', 150.15, 1500, '2021-01-01T09:00:00.000000200Z'::TIMESTAMP_NS)");
            execute("INSERT INTO prices VALUES('AAPL', 150.20, 2000, '2021-01-01T09:00:00.000000300Z'::TIMESTAMP_NS)");

            String expected = "symbol\ttime\tprice\tvolume\ttotal_volume\n" +
                    "AAPL\t2021-01-01T09:00:00.000000100Z\t150.1\t1000\t1000.0\n" +
                    "AAPL\t2021-01-01T09:00:00.000000200Z\t150.15\t1500\t2500.0\n" +
                    "AAPL\t2021-01-01T09:00:00.000000300Z\t150.2\t2000\t4500.0\n";

            String query = "SELECT symbol, time, price, volume, " +
                    "sum(volume) over (PARTITION BY symbol ORDER BY time rows unbounded preceding) total_volume " +
                    "FROM prices ORDER BY symbol, time";
            assertQuery(expected, query, null, true, false);
        });
    }

    @Test
    public void testConversionFromMicros() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE micro_table(id INT, time TIMESTAMP) TIMESTAMP(time) PARTITION BY DAY");
            execute("INSERT INTO micro_table VALUES(1, '2021-01-01T00:00:00.123456Z'::TIMESTAMP)");

            String expected = "id\ttime\ttime_ns\n" +
                    "1\t2021-01-01T00:00:00.123456Z\t2021-01-01T00:00:00.123456000Z\n";
            String query = "SELECT id, time, cast(time as TIMESTAMP_NS) time_ns FROM micro_table";
            assertQuery(expected, query, "time", true, true);
        });
    }

    @Test
    public void testConversionToMicros() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE nano_table(id INT, time TIMESTAMP_NS) TIMESTAMP(time) PARTITION BY DAY");
            execute("INSERT INTO nano_table VALUES(1, '2021-01-01T00:00:00.123456789Z'::TIMESTAMP_NS)");

            String expected = "id\ttime\ttime_micro\n" +
                    "1\t2021-01-01T00:00:00.123456789Z\t2021-01-01T00:00:00.123456Z\n";
            String query = "SELECT id, time, cast(time as TIMESTAMP) time_micro FROM nano_table";
            assertQuery(expected, query, "time", true, true);
        });
    }

    @Test
    public void testCrossPrecisionAsofJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE micro_events(id INT, micro_time TIMESTAMP, price DOUBLE) TIMESTAMP(micro_time) PARTITION BY DAY");
            execute("CREATE TABLE nano_trades(trade_id INT, nano_time TIMESTAMP_NS, quantity LONG) TIMESTAMP(nano_time) PARTITION BY DAY");

            execute("INSERT INTO micro_events VALUES(1, '2021-01-01T09:00:00.123456Z'::TIMESTAMP, 100.5)");
            execute("INSERT INTO micro_events VALUES(2, '2021-01-01T09:00:00.125000Z'::TIMESTAMP, 101.2)");
            execute("INSERT INTO micro_events VALUES(3, '2021-01-01T09:00:00.127500Z'::TIMESTAMP, 99.8)");

            execute("INSERT INTO nano_trades VALUES(101, '2021-01-01T09:00:00.123456100Z'::TIMESTAMP_NS, 1000)");
            execute("INSERT INTO nano_trades VALUES(102, '2021-01-01T09:00:00.125000500Z'::TIMESTAMP_NS, 2000)");
            execute("INSERT INTO nano_trades VALUES(103, '2021-01-01T09:00:00.126000200Z'::TIMESTAMP_NS, 1500)");

            String expected = "trade_id\tnano_time\tquantity\tid\tmicro_time\tprice\n" +
                    "101\t2021-01-01T09:00:00.123456100Z\t1000\t1\t2021-01-01T09:00:00.123456Z\t100.5\n" +
                    "102\t2021-01-01T09:00:00.125000500Z\t2000\t2\t2021-01-01T09:00:00.125000Z\t101.2\n" +
                    "103\t2021-01-01T09:00:00.126000200Z\t1500\t2\t2021-01-01T09:00:00.125000Z\t101.2\n";
            String query = "SELECT t.trade_id, t.nano_time, t.quantity, e.id, e.micro_time, e.price " +
                    "FROM nano_trades t ASOF JOIN micro_events e " +
                    "ORDER BY t.nano_time";
            assertQuery(expected, query, "nano_time", false, true);
        });
    }

    @Test
    public void testDistinctOperations() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango(id INT, time TIMESTAMP_NS) TIMESTAMP(time) PARTITION BY DAY");
            execute("INSERT INTO tango VALUES(1, '2021-01-01T00:00:00.000000123Z'::TIMESTAMP_NS)");
            execute("INSERT INTO tango VALUES(2, '2021-01-01T00:00:00.000000123Z'::TIMESTAMP_NS)");
            execute("INSERT INTO tango VALUES(3, '2021-01-01T00:00:00.000000456Z'::TIMESTAMP_NS)");
            execute("INSERT INTO tango VALUES(4, '2021-01-01T00:00:00.000000456Z'::TIMESTAMP_NS)");
            execute("INSERT INTO tango VALUES(5, '2021-01-01T00:00:00.000000789Z'::TIMESTAMP_NS)");

            String expected = "time\n" +
                    "2021-01-01T00:00:00.000000123Z\n" +
                    "2021-01-01T00:00:00.000000456Z\n" +
                    "2021-01-01T00:00:00.000000789Z\n";
            String query = "SELECT distinct time FROM tango ORDER BY time";
            assertQuery(expected, query, "time", true, true);
        });
    }

    @Test
    public void testGroupByOperations() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango(symbol symbol, value DOUBLE, time TIMESTAMP_NS) TIMESTAMP(time) PARTITION BY DAY");
            execute("INSERT INTO tango VALUES('A', 10.5, '2021-01-01T00:00:00.000000100Z'::TIMESTAMP_NS)");
            execute("INSERT INTO tango VALUES('A', 20.3, '2021-01-01T00:00:00.000000200Z'::TIMESTAMP_NS)");
            execute("INSERT INTO tango VALUES('B', 30.7, '2021-01-01T00:00:00.000000150Z'::TIMESTAMP_NS)");
            execute("INSERT INTO tango VALUES('B', 40.1, '2021-01-01T00:00:00.000000250Z'::TIMESTAMP_NS)");

            String expected = "symbol\tsum_value\tmin_time\tmax_time\tcount\n" +
                    "A\t30.8\t2021-01-01T00:00:00.000000100Z\t2021-01-01T00:00:00.000000200Z\t2\n" +
                    "B\t70.8\t2021-01-01T00:00:00.000000150Z\t2021-01-01T00:00:00.000000250Z\t2\n";
            String query = "SELECT symbol, sum(value) sum_value, min(time) min_time, max(time) max_time, count(*) FROM tango GROUP BY symbol ORDER BY symbol";
            assertQuery(expected, query, null, true, true);
        });
    }

    @Test
    public void testInOperations() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango(id INT, time TIMESTAMP_NS) TIMESTAMP(time) PARTITION BY DAY");
            execute("INSERT INTO tango VALUES(1, '2021-01-01T00:00:00.000000100Z'::TIMESTAMP_NS)");
            execute("INSERT INTO tango VALUES(2, '2021-01-01T00:00:00.000000200Z'::TIMESTAMP_NS)");
            execute("INSERT INTO tango VALUES(3, '2021-01-01T00:00:00.000000300Z'::TIMESTAMP_NS)");
            execute("INSERT INTO tango VALUES(4, '2021-01-01T00:00:00.000000400Z'::TIMESTAMP_NS)");

            String expected = "id\ttime\n" +
                    "2\t2021-01-01T00:00:00.000000200Z\n" +
                    "4\t2021-01-01T00:00:00.000000400Z\n";
            String query = "SELECT id, time FROM tango where time in ('2021-01-01T00:00:00.000000200Z'::TIMESTAMP_NS, '2021-01-01T00:00:00.000000400Z'::TIMESTAMP_NS) ORDER BY time";
            assertQuery(expected, query, "time", true, false);
        });
    }

    @Test
    public void testIndexOperations() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango(symbol symbol index, id INT, time TIMESTAMP_NS) TIMESTAMP(time) PARTITION BY DAY");
            execute("INSERT INTO tango VALUES('A', 1, '2021-01-01T00:00:00.000000100Z'::TIMESTAMP_NS)");
            execute("INSERT INTO tango VALUES('B', 2, '2021-01-01T00:00:00.000000200Z'::TIMESTAMP_NS)");
            execute("INSERT INTO tango VALUES('A', 3, '2021-01-01T00:00:00.000000300Z'::TIMESTAMP_NS)");
            execute("INSERT INTO tango VALUES('C', 4, '2021-01-01T00:00:00.000000400Z'::TIMESTAMP_NS)");
            execute("INSERT INTO tango VALUES('B', 5, '2021-01-01T00:00:00.000000500Z'::TIMESTAMP_NS)");

            String expected = "symbol\tid\ttime\n" +
                    "A\t1\t2021-01-01T00:00:00.000000100Z\n" +
                    "A\t3\t2021-01-01T00:00:00.000000300Z\n";
            String query = "SELECT symbol, id, time FROM tango where symbol = 'A' ORDER BY time";
            assertQuery(expected, query, "time", true, false);
        });
    }

    @Test
    public void testIntervalEquality() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE interval_test(id INT, nano_time TIMESTAMP_NS) TIMESTAMP(nano_time) PARTITION BY DAY");
            execute("INSERT INTO interval_test VALUES(1, '2021-01-01T09:00:00.000000100Z'::TIMESTAMP_NS)");
            execute("INSERT INTO interval_test VALUES(2, '2021-01-01T10:00:00.000000200Z'::TIMESTAMP_NS)");
            execute("INSERT INTO interval_test VALUES(3, '2021-01-02T09:00:00.000000300Z'::TIMESTAMP_NS)");

            // Test ConstCheckFunc - comparing interval function to constant interval
            String expected = "id\tnano_time\tis_constant_interval\n" +
                    "1\t2021-01-01T09:00:00.000000100Z\ttrue\n" +
                    "2\t2021-01-01T10:00:00.000000200Z\ttrue\n" +
                    "3\t2021-01-02T09:00:00.000000300Z\tfalse\n";
            String query = "SELECT id, nano_time, " +
                    "interval(date_trunc('day', nano_time), dateadd('d', 1, date_trunc('day', nano_time))) = " +
                    "interval('2021-01-01T00:00:00.000000000Z'::TIMESTAMP_NS, '2021-01-02T00:00:00.000000000Z'::TIMESTAMP_NS) as is_constant_interval " +
                    "FROM interval_test ORDER BY nano_time";
            assertQuery(expected, query, "nano_time", true, true);

            String expected2 = "id\tnano_time\tis_not_constant_interval\n" +
                    "1\t2021-01-01T09:00:00.000000100Z\tfalse\n" +
                    "2\t2021-01-01T10:00:00.000000200Z\tfalse\n" +
                    "3\t2021-01-02T09:00:00.000000300Z\ttrue\n";
            String query2 = "SELECT id, nano_time, " +
                    "interval(date_trunc('day', nano_time), dateadd('d', 1, date_trunc('day', nano_time))) != " +
                    "interval('2021-01-01T00:00:00.000000000Z'::TIMESTAMP_NS, '2021-01-02T00:00:00.000000000Z'::TIMESTAMP_NS) as is_not_constant_interval " +
                    "FROM interval_test ORDER BY nano_time";
            assertQuery(expected2, query2, "nano_time", true, true);
        });
    }

    @Test
    public void testInvalidTimestampFormat() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango(id INT, time TIMESTAMP_NS) TIMESTAMP(time) PARTITION BY DAY");

            try {
                execute("INSERT INTO tango VALUES(1, 'invalid-timestamp'::TIMESTAMP_NS)");
                Assert.fail("Expected SqlException");
            } catch (Exception e) {
                TestUtils.assertContains(e.getMessage().toLowerCase(), "cannot be null");
            }
        });
    }

    @Test
    public void testMinMaxAggregation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango(id INT, time TIMESTAMP_NS) TIMESTAMP(time) PARTITION BY DAY");
            execute("INSERT INTO tango VALUES(1, '2021-01-01T00:00:00.000000100Z'::TIMESTAMP_NS)");
            execute("INSERT INTO tango VALUES(2, '2021-01-01T00:00:00.000000500Z'::TIMESTAMP_NS)");
            execute("INSERT INTO tango VALUES(3, '2021-01-01T00:00:00.000000300Z'::TIMESTAMP_NS)");

            String expected = "min_time\tmax_time\tcount\n" +
                    "2021-01-01T00:00:00.000000100Z\t2021-01-01T00:00:00.000000500Z\t3\n";
            String query = "SELECT min(time) min_time, max(time) max_time, count(*) FROM tango";
            assertQuery(expected, query, null, false, true);
        });
    }

    @Test
    public void testMixedPrecision() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE events(event_id INT, micro_ts TIMESTAMP, nano_ts TIMESTAMP_NS, value DOUBLE) TIMESTAMP(nano_ts) PARTITION BY DAY");
            String t0us = "2021-01-01T10:00:00.123456Z";
            String t1us = "2021-01-01T10:00:01.123456Z";
            String t2us = "2021-01-01T10:00:02.123456Z";

            String t0ns = "2021-01-01T10:00:00.123456789Z";
            String t1ns = "2021-01-01T10:00:01.123456800Z";
            String t2ns = "2021-01-01T10:00:02.123456900Z";

            execute("INSERT INTO events VALUES(1, '" + t0us + "'::TIMESTAMP, '" + t0ns + "'::TIMESTAMP_NS, 100.0)");
            execute("INSERT INTO events VALUES(2, '" + t1us + "'::TIMESTAMP, '" + t1ns + "'::TIMESTAMP_NS, 200.0)");
            execute("INSERT INTO events VALUES(3, '" + t2us + "'::TIMESTAMP, '" + t2ns + "'::TIMESTAMP_NS, 150.0)");

            String expected = "event_id\tmicro_ts\tnano_ts\tvalue\tround_nanos\tdiff_nano_to_micro\n" +
                    "1\t" + t0us + "\t" + t0ns + "\t100.0\t789\t789\n" +
                    "2\t" + t1us + "\t" + t1ns + "\t200.0\t800\t800\n" +
                    "3\t" + t2us + "\t" + t2ns + "\t150.0\t900\t900\n";
            String query = "SELECT event_id, micro_ts, nano_ts, value, " +
                    "nano_ts % 1000 round_nanos, " +
                    "(nano_ts - micro_ts::TIMESTAMP_NS) diff_nano_to_micro " +
                    "FROM events ORDER BY nano_ts";
            assertQuery(expected, query, "nano_ts", true, true);
        });
    }

    @Test
    public void testNullHandling() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango(id INT, time TIMESTAMP_NS, other_time TIMESTAMP_NS) TIMESTAMP(time) PARTITION BY DAY");
            execute("INSERT INTO tango VALUES(1, '2021-01-01T00:00:00.000000100Z'::TIMESTAMP_NS, null)");
            execute("INSERT INTO tango VALUES(2, '2021-01-01T00:00:00.000000123Z'::TIMESTAMP_NS, '2021-01-01T00:00:00.000000456Z'::TIMESTAMP_NS)");

            String expected = "id\ttime\tother_time\n" +
                    "1\t2021-01-01T00:00:00.000000100Z\t\n" +
                    "2\t2021-01-01T00:00:00.000000123Z\t2021-01-01T00:00:00.000000456Z\n";
            String query = "SELECT id, time, other_time FROM tango ORDER BY time";
            assertQuery(expected, query, "time", true, true);
        });
    }

    @Test
    public void testOrdering() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango(id INT, time TIMESTAMP_NS) TIMESTAMP(time) PARTITION BY DAY");
            execute("INSERT INTO tango VALUES" +
                    "  (5, '2021-01-01T00:00:00.000000500Z'::TIMESTAMP_NS)" +
                    ", (1, '2021-01-01T00:00:00.000000100Z'::TIMESTAMP_NS)" +
                    ", (3, '2021-01-01T00:00:00.000000300Z'::TIMESTAMP_NS)" +
                    ", (2, '2021-01-01T00:00:00.000000200Z'::TIMESTAMP_NS)" +
                    ", (4, '2021-01-01T00:00:00.000000400Z'::TIMESTAMP_NS)");

            String expectedAsc = "id\ttime\n" +
                    "1\t2021-01-01T00:00:00.000000100Z\n" +
                    "2\t2021-01-01T00:00:00.000000200Z\n" +
                    "3\t2021-01-01T00:00:00.000000300Z\n" +
                    "4\t2021-01-01T00:00:00.000000400Z\n" +
                    "5\t2021-01-01T00:00:00.000000500Z\n";
            assertQuery(expectedAsc, "SELECT id, time FROM tango ORDER BY time", "time", true, true);

            String expectedDesc = "id\ttime\n" +
                    "5\t2021-01-01T00:00:00.000000500Z\n" +
                    "4\t2021-01-01T00:00:00.000000400Z\n" +
                    "3\t2021-01-01T00:00:00.000000300Z\n" +
                    "2\t2021-01-01T00:00:00.000000200Z\n" +
                    "1\t2021-01-01T00:00:00.000000100Z\n";
            assertQuery(expectedDesc, "SELECT id, time FROM tango ORDER BY time desc",
                    "time###desc", true, true);
        });
    }

    @Test
    public void testPartitionByDay() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango(id INT, time TIMESTAMP_NS) TIMESTAMP(time) PARTITION BY DAY");
            execute("INSERT INTO tango VALUES(1, '2021-01-01T23:59:59.999999999Z'::TIMESTAMP_NS)");
            execute("INSERT INTO tango VALUES(2, '2021-01-02T00:00:00.000000001Z'::TIMESTAMP_NS)");
            execute("INSERT INTO tango VALUES(3, '2021-01-02T12:00:00.000000000Z'::TIMESTAMP_NS)");
            execute("INSERT INTO tango VALUES(4, '2021-01-03T00:00:00.000000000Z'::TIMESTAMP_NS)");

            String expected = "id\ttime\n" +
                    "1\t2021-01-01T23:59:59.999999999Z\n" +
                    "2\t2021-01-02T00:00:00.000000001Z\n" +
                    "3\t2021-01-02T12:00:00.000000000Z\n" +
                    "4\t2021-01-03T00:00:00.000000000Z\n";
            String query = "SELECT id, time FROM tango ORDER BY time";
            assertQuery(expected, query, "time", true, true);
        });
    }

    @Test
    public void testPartitionByHour() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango(id INT, time TIMESTAMP_NS) TIMESTAMP(time) PARTITION BY HOUR");
            execute("INSERT INTO tango VALUES(1, '2021-01-01T00:59:59.999999999Z'::TIMESTAMP_NS)");
            execute("INSERT INTO tango VALUES(2, '2021-01-01T01:00:00.000000001Z'::TIMESTAMP_NS)");
            execute("INSERT INTO tango VALUES(3, '2021-01-01T01:30:00.000000000Z'::TIMESTAMP_NS)");
            execute("INSERT INTO tango VALUES(4, '2021-01-01T02:00:00.000000000Z'::TIMESTAMP_NS)");

            String expected = "id\ttime\n" +
                    "1\t2021-01-01T00:59:59.999999999Z\n" +
                    "2\t2021-01-01T01:00:00.000000001Z\n" +
                    "3\t2021-01-01T01:30:00.000000000Z\n" +
                    "4\t2021-01-01T02:00:00.000000000Z\n";
            String query = "SELECT id, time FROM tango ORDER BY time";
            assertQuery(expected, query, "time", true, true);
        });
    }

    @Test
    public void testPrecisionBoundaries() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango(id INT, time TIMESTAMP_NS) TIMESTAMP(time) PARTITION BY DAY");
            execute("INSERT INTO tango VALUES(1, '1970-01-01T00:00:00.000000000Z'::TIMESTAMP_NS)");
            execute("INSERT INTO tango VALUES(2, '1970-01-01T00:00:00.000000001Z'::TIMESTAMP_NS)");
            execute("INSERT INTO tango VALUES(3, '1970-01-01T00:00:00.000000999Z'::TIMESTAMP_NS)");
            execute("INSERT INTO tango VALUES(4, '1970-01-01T00:00:00.000001000Z'::TIMESTAMP_NS)");
            execute("INSERT INTO tango VALUES(5, '1970-01-01T00:00:00.999999999Z'::TIMESTAMP_NS)");

            String expected = "id\ttime\n" +
                    "1\t1970-01-01T00:00:00.000000000Z\n" +
                    "2\t1970-01-01T00:00:00.000000001Z\n" +
                    "3\t1970-01-01T00:00:00.000000999Z\n" +
                    "4\t1970-01-01T00:00:00.000001000Z\n" +
                    "5\t1970-01-01T00:00:00.999999999Z\n";
            String query = "SELECT id, time FROM tango ORDER BY time";
            assertQuery(expected, query, "time", true, true);
        });
    }

    @Test
    public void testSampleByNanosecondIntervals() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE high_freq_data(sensor_id INT, reading DOUBLE, nano_ts TIMESTAMP_NS) TIMESTAMP(nano_ts) PARTITION BY DAY");
            execute("INSERT INTO high_freq_data VALUES(1, 25.1, '2021-01-01T15:00:00.000000100Z'::TIMESTAMP_NS)");
            execute("INSERT INTO high_freq_data VALUES(1, 25.3, '2021-01-01T15:00:00.000000150Z'::TIMESTAMP_NS)");
            execute("INSERT INTO high_freq_data VALUES(1, 25.2, '2021-01-01T15:00:00.000000200Z'::TIMESTAMP_NS)");
            execute("INSERT INTO high_freq_data VALUES(1, 25.4, '2021-01-01T15:00:00.000000250Z'::TIMESTAMP_NS)");
            execute("INSERT INTO high_freq_data VALUES(1, 25.0, '2021-01-01T15:00:00.000000300Z'::TIMESTAMP_NS)");
            execute("INSERT INTO high_freq_data VALUES(1, 25.6, '2021-01-01T15:00:00.000000350Z'::TIMESTAMP_NS)");

            String expected = "nano_ts\tavg_reading\tmax_reading\tmin_reading\tcount\n" +
                    "2021-01-01T15:00:00.000000100Z\t25.2\t25.3\t25.1\t2\n" +
                    "2021-01-01T15:00:00.000000200Z\t25.3\t25.4\t25.2\t2\n" +
                    "2021-01-01T15:00:00.000000300Z\t25.3\t25.6\t25.0\t2\n";
            String query = "SELECT nano_ts, round(avg(reading), 2) avg_reading, max(reading) max_reading, min(reading) min_reading, count(*) " +
                    "FROM high_freq_data sample by 100n fill(none)";
            assertQuery(expected, query, "nano_ts", true, true);
        });
    }

    @Test
    public void testTimestampEqualityWithBindVariables() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE mixed_ts(id INT, nano_time TIMESTAMP_NS, micro_time TIMESTAMP) TIMESTAMP(nano_time) PARTITION BY DAY");
            execute("INSERT INTO mixed_ts VALUES(1, '2021-01-01T09:00:00.000100000Z', '2021-01-01T09:00:00.000100Z')");
            execute("INSERT INTO mixed_ts VALUES(2, '2021-01-01T10:00:00.000200000Z', '2021-01-01T10:00:00.000200Z')");
            execute("INSERT INTO mixed_ts VALUES(3, '2021-01-02T09:00:00.000300000Z', '2021-01-02T09:00:00.000300Z')");
            final long tsLong = 1_609_491_600_000_100L; // '2021-01-01T09:00:00.000100Z' in microseconds

            // Test RightRunTimeConstFunc - TIMESTAMP_NS column = :timestamp_micro_bind_variable
            // This triggers RightRunTimeConstFunc because:
            // - left: nano_time (TIMESTAMP_NS) - higher precision
            // - right: bind variable (TIMESTAMP - microsecond) - lower precision, runtime constant
            bindVariableService.clear();
            bindVariableService.setTimestamp("micro_bind", tsLong);

            String expected = "id\tnano_time\tmicro_time\tequals\n" +
                    "1\t2021-01-01T09:00:00.000100000Z\t2021-01-01T09:00:00.000100Z\ttrue\n" +
                    "2\t2021-01-01T10:00:00.000200000Z\t2021-01-01T10:00:00.000200Z\tfalse\n" +
                    "3\t2021-01-02T09:00:00.000300000Z\t2021-01-02T09:00:00.000300Z\tfalse\n";
            String query = "SELECT id, nano_time, micro_time, nano_time = :micro_bind equals FROM mixed_ts";
            assertQuery(expected, query, "nano_time", true, true);

            // Test inequality as well to cover the negated path
            bindVariableService.clear();
            bindVariableService.setTimestamp("micro_bind", tsLong);

            String expected2 = "id\tnano_time\tmicro_time\tequals\n" +
                    "1\t2021-01-01T09:00:00.000100000Z\t2021-01-01T09:00:00.000100Z\tfalse\n" +
                    "2\t2021-01-01T10:00:00.000200000Z\t2021-01-01T10:00:00.000200Z\ttrue\n" +
                    "3\t2021-01-02T09:00:00.000300000Z\t2021-01-02T09:00:00.000300Z\ttrue\n";
            String query2 = "SELECT id, nano_time, micro_time, nano_time != :micro_bind equals FROM mixed_ts";
            assertQuery(expected2, query2, "nano_time", true, true);
        });
    }

    @Test
    public void testUpdateOperations() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango(id INT, time TIMESTAMP_NS, value DOUBLE) TIMESTAMP(time) PARTITION BY DAY");
            execute("INSERT INTO tango VALUES(1, '2021-01-01T00:00:00.000000100Z'::TIMESTAMP_NS, 10.5)");
            execute("INSERT INTO tango VALUES(2, '2021-01-01T00:00:00.000000200Z'::TIMESTAMP_NS, 20.3)");
            execute("INSERT INTO tango VALUES(3, '2021-01-01T00:00:00.000000300Z'::TIMESTAMP_NS, 30.7)");

            execute("UPDATE tango SET value = 99.9 WHERE time = '2021-01-01T00:00:00.000000200Z'::TIMESTAMP_NS");

            String expected = "id\ttime\tvalue\n" +
                    "1\t2021-01-01T00:00:00.000000100Z\t10.5\n" +
                    "2\t2021-01-01T00:00:00.000000200Z\t99.9\n" +
                    "3\t2021-01-01T00:00:00.000000300Z\t30.7\n";
            String query = "SELECT id, time, value FROM tango ORDER BY time";
            assertQuery(expected, query, "time", true, true);
        });
    }

    @Test
    public void testUpdateWithTimeRange() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango(id INT, time TIMESTAMP_NS, value DOUBLE) TIMESTAMP(time) PARTITION BY DAY");
            execute("INSERT INTO tango VALUES(1, '2021-01-01T00:00:00.000000100Z'::TIMESTAMP_NS, 10.5)");
            execute("INSERT INTO tango VALUES(2, '2021-01-01T00:00:00.000000200Z'::TIMESTAMP_NS, 20.3)");
            execute("INSERT INTO tango VALUES(3, '2021-01-01T00:00:00.000000300Z'::TIMESTAMP_NS, 30.7)");
            execute("INSERT INTO tango VALUES(4, '2021-01-01T00:00:00.000000400Z'::TIMESTAMP_NS, 40.1)");

            execute("UPDATE tango SET value = value * 2 WHERE time >= '2021-01-01T00:00:00.000000200Z'::TIMESTAMP_NS AND time <= '2021-01-01T00:00:00.000000300Z'::TIMESTAMP_NS");

            String expected = "id\ttime\tvalue\n" +
                    "1\t2021-01-01T00:00:00.000000100Z\t10.5\n" +
                    "2\t2021-01-01T00:00:00.000000200Z\t40.6\n" +
                    "3\t2021-01-01T00:00:00.000000300Z\t61.4\n" +
                    "4\t2021-01-01T00:00:00.000000400Z\t40.1\n";
            String query = "SELECT id, time, value FROM tango ORDER BY time";
            assertQuery(expected, query, "time", true, true);
        });
    }

    @Test
    public void testWindowRowCountBoundaryFunctions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE time_series(id INT, value DOUBLE, nano_time TIMESTAMP_NS) TIMESTAMP(nano_time) PARTITION BY DAY");
            execute("INSERT INTO time_series VALUES(1, 10.5, '2021-01-01T09:00:00.000000100Z'::TIMESTAMP_NS)");
            execute("INSERT INTO time_series VALUES(2, 15.2, '2021-01-01T09:00:00.000000200Z'::TIMESTAMP_NS)");
            execute("INSERT INTO time_series VALUES(3, 12.8, '2021-01-01T09:00:00.000000300Z'::TIMESTAMP_NS)");
            execute("INSERT INTO time_series VALUES(4, 18.1, '2021-01-01T09:00:00.000000400Z'::TIMESTAMP_NS)");
            execute("INSERT INTO time_series VALUES(5, 14.7, '2021-01-01T09:00:00.000000500Z'::TIMESTAMP_NS)");

            String expected = "id\tvalue\tnano_time\tfirst_val\tlast_val\tlag_val\tlead_val\n" +
                    "1\t10.5\t2021-01-01T09:00:00.000000100Z\t10.5\t10.5\tnull\t15.2\n" +
                    "2\t15.2\t2021-01-01T09:00:00.000000200Z\t10.5\t15.2\t10.5\t12.8\n" +
                    "3\t12.8\t2021-01-01T09:00:00.000000300Z\t15.2\t12.8\t15.2\t18.1\n" +
                    "4\t18.1\t2021-01-01T09:00:00.000000400Z\t12.8\t18.1\t12.8\t14.7\n" +
                    "5\t14.7\t2021-01-01T09:00:00.000000500Z\t18.1\t14.7\t18.1\tnull\n";
            String query = "SELECT id, value, nano_time, " +
                    "first_value(value) over (ORDER BY nano_time rows between 1 preceding and current row) first_val, " +
                    "last_value(value) over (ORDER BY nano_time rows between 1 preceding and current row) last_val, " +
                    "lag(value, 1) over (ORDER BY nano_time) lag_val, " +
                    "lead(value, 1) over (ORDER BY nano_time) lead_val " +
                    "FROM time_series ORDER BY nano_time";
            assertQuery(expected, query, "nano_time", true, false);
        });
    }
}
