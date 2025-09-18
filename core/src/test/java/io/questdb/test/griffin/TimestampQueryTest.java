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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import org.junit.Assert;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.function.LongPredicate;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class TimestampQueryTest extends AbstractCairoTest {

    @Test
    public void testCast2AsValidColumnNameTouchFunction() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table xyz(time timestamp, cast2 geohash(8c)) timestamp(time) partition by DAY;");
            execute("INSERT INTO xyz VALUES(1609459199000000, #u33d8b12)");
            String expected = "touch\n{\"data_pages\": 2, \"index_key_pages\":0, \"index_values_pages\": 0}\n";
            String query = "select touch(select time, cast2 from xyz);";
            assertSql(expected, query);
        });
    }

    @Test
    public void testCastAsValidColumnNameSelectTest() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table xyz(time timestamp, \"cast\" geohash(8c)) timestamp(time) partition by DAY;");
            execute("INSERT INTO xyz VALUES(1609459199000000, #u33d8b12)");
            String expected = "time\tcast\n" +
                    "2020-12-31T23:59:59.000000Z\tu33d8b12\n";
            String query = "select time, \"cast\" from xyz;";
            assertSql(expected, query);
        });
    }

    @Test
    public void testConstantFunctionExtractionNanos() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab(i int, ts timestamp_ns) timestamp(ts) partition by DAY;");
            execute("INSERT INTO tab VALUES(0, '2000-01-01T00:00:00.000000000Z')");


            String expected = "i\tts\n" +
                    "0\t2000-01-01T00:00:00.000000000Z\n";
            // constant function
            String query = "select * from tab where ts = 946684800000000000 + 0;";

            assertQueryNoLeakCheck(expected, query, "ts", true, false);
        });
    }

    @Test
    public void testDesignatedTimestampOpSymbolColumns() throws Exception {
        assertQuery(
                "a\tdk\tk\n" +
                        "1970-01-01T00:00:00.040000Z\t1970-01-01T00:00:00.030000Z\t1970-01-01T00:00:00.030000Z\n" +
                        "1970-01-01T00:00:00.050000Z\t1970-01-01T00:00:00.040000Z\t1970-01-01T00:00:00.040000Z\n",
                "select a, dk, k from x where dk < cast(a as timestamp)",
                "create table x as (select cast(concat('1970-01-01T00:00:00.0', (case when x > 3 then x else x - 1 end), '0000Z') as symbol) a, timestamp_sequence(0, 10000) dk, timestamp_sequence(0, 10000) k from long_sequence(5)) timestamp(k)",
                "k",
                null,
                null,
                true,
                false,
                false
        );
    }

    @Test
    public void testEqualityTimestampFormatYearAndMonthNegativeTest() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY");
            execute("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            assertSql(expected, query);
            // test where ts ='2021-01'
            expected = "symbol\tme_seq_num\ttimestamp\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp ='2021-01'";
            assertSql(expected, query);
            // test where ts ='2020-11'
            expected = "symbol\tme_seq_num\ttimestamp\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp ='2020-11'";
            assertSql(expected, query);
        });
    }

    @Test
    public void testEqualityTimestampFormatYearAndMonthPositiveTest() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY");
            execute("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            assertSql(expected, query);
            // test where ts ='2020-12'
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp IN '2020-12'";
            assertSql(expected, query);
        });
    }

    @Test
    public void testEqualityTimestampFormatYearOnlyNegativeTest() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY");
            execute("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            assertSql(expected, query);
            // test where ts ='2021'
            expected = "symbol\tme_seq_num\ttimestamp\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp ='2021'";
            assertSql(expected, query);
        });
    }

    @Test
    public void testEqualityTimestampFormatYearOnlyPositiveTest() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY");
            execute("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test where ts ='2020'
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp IN '2020'";
            printSqlResult(expected, query, "timestamp", true, false);
        });
    }

    @Test
    public void testEqualsToTimestampFormatYearMonthDay() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY");
            execute("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp IN '2020-12-31'";
            printSqlResult(expected, query, "timestamp", true, false);
        });
    }

    @Test
    public void testEqualsToTimestampFormatYearMonthDayHour() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY");
            execute("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp IN '2020-12-31T23'";
            printSqlResult(expected, query, "timestamp", true, false);
        });
    }

    @Test
    public void testEqualsToTimestampFormatYearMonthDayHourMinute() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY");
            execute("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp IN '2020-12-31T23:59'";
            printSqlResult(expected, query, "timestamp", true, false);
        });
    }

    @Test
    public void testEqualsToTimestampFormatYearMonthDayHourMinuteSecond() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY");
            execute("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp = '2020-12-31T23:59:59'";
            printSqlResult(expected, query, "timestamp", true, false);
        });
    }

    @Test
    public void testEqualsToTimestampWithMicrosecond() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY");
            execute("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000001)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000001Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000001Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp = '2020-12-31T23:59:59.000001Z'";
            printSqlResult(expected, query, "timestamp", true, false);
        });
    }

    @Test
    public void testInsertAsSelectTimestampVarcharCast() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (l long, t timestamp) timestamp(t) partition by DAY");
            execute("insert into x select 1, '2024-02-27T00:00:00'::varchar");
            assertSql("l\tt\n1\t2024-02-27T00:00:00.000000Z\n", "select * from x");
        });
    }

    @Test
    public void testIntervalEquality() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE interval_test_micro(id INT, micro_time TIMESTAMP) TIMESTAMP(micro_time) PARTITION BY DAY");
            execute("INSERT INTO interval_test_micro VALUES(1, '2021-01-01T09:00:00.000100Z'::TIMESTAMP)");
            execute("INSERT INTO interval_test_micro VALUES(2, '2021-01-01T10:00:00.000200Z'::TIMESTAMP)");
            execute("INSERT INTO interval_test_micro VALUES(3, '2021-01-02T09:00:00.000300Z'::TIMESTAMP)");

            // Test ConstCheckFunc with microsecond intervals - comparing interval function to constant interval
            String expected = "id\tmicro_time\tis_constant_interval\n" +
                    "1\t2021-01-01T09:00:00.000100Z\ttrue\n" +
                    "2\t2021-01-01T10:00:00.000200Z\ttrue\n" +
                    "3\t2021-01-02T09:00:00.000300Z\tfalse\n";
            String query = "SELECT id, micro_time, " +
                    "interval(date_trunc('day', micro_time), dateadd('d', 1, date_trunc('day', micro_time))) = " +
                    "interval('2021-01-01T00:00:00.000000Z'::TIMESTAMP, '2021-01-02T00:00:00.000000Z'::TIMESTAMP) as is_constant_interval " +
                    "FROM interval_test_micro ORDER BY micro_time";
            assertQuery(expected, query, "micro_time", true, true);

            // Test NullCheckFunc with microsecond intervals - comparing interval function to null interval
            String expected2 = "id\tmicro_time\tis_null_interval\n" +
                    "1\t2021-01-01T09:00:00.000100Z\tfalse\n" +
                    "2\t2021-01-01T10:00:00.000200Z\tfalse\n" +
                    "3\t2021-01-02T09:00:00.000300Z\tfalse\n";
            String query2 = "SELECT id, micro_time, " +
                    "interval(date_trunc('day', micro_time), dateadd('d', 1, date_trunc('day', micro_time))) = " +
                    "null::interval as is_null_interval " +
                    "FROM interval_test_micro ORDER BY micro_time";
            assertQuery(expected2, query2, "micro_time", true, true);

            String expected3 = "id\tmicro_time\tis_not_null_interval\n" +
                    "1\t2021-01-01T09:00:00.000100Z\ttrue\n" +
                    "2\t2021-01-01T10:00:00.000200Z\ttrue\n" +
                    "3\t2021-01-02T09:00:00.000300Z\ttrue\n";
            String query3 = "SELECT id, micro_time, " +
                    "interval(date_trunc('day', micro_time), dateadd('d', 1, date_trunc('day', micro_time))) != " +
                    "null::interval as is_not_null_interval " +
                    "FROM interval_test_micro ORDER BY micro_time";
            assertQuery(expected3, query3, "micro_time", true, true);
        });
    }

    @Test
    public void testLMoreThanOrEqualsToTimestampFormatYearOnlyPositiveTest1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY");
            execute("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp >= '2020'";
            printSqlResult(expected, query, "timestamp", true, false);
        });
    }

    @Test
    public void testLMoreThanTimestampFormatYearOnlyPositiveTest1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY");
            execute("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp > '2019'";
            printSqlResult(expected, query, "timestamp", true, false);
        });
    }

    @Test
    public void testLessThanOrEqualsToTimestampFormatYearOnlyNegativeTest1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY");
            execute("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp <= '2019'";
            printSqlResult(expected, query, "timestamp", true, false);
        });
    }

    @Test
    public void testLessThanOrEqualsToTimestampFormatYearOnlyNegativeTest2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY");
            execute("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n";
            query = "SELECT * FROM ob_mem_snapshot where '2021' <=  timestamp";
            printSqlResult(expected, query, "timestamp", true, false);
        });
    }

    @Test
    public void testLessThanOrEqualsToTimestampFormatYearOnlyPositiveTest1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY");
            execute("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp <= '2021'";
            printSqlResult(expected, query, "timestamp", true, false);
        });
    }

    @Test
    public void testLessThanOrEqualsToTimestampFormatYearOnlyPositiveTest2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY");
            execute("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where '2020' <=  timestamp";
            printSqlResult(expected, query, "timestamp", true, false);
        });
    }

    @Test
    public void testLessThanTimestampFormatYearOnlyNegativeTest1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY");
            execute("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp <'2020'";
            printSqlResult(expected, query, "timestamp", true, false);
        });
    }

    @Test
    public void testLessThanTimestampFormatYearOnlyNegativeTest2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY");
            execute("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n";
            query = "SELECT * FROM ob_mem_snapshot where '2021' <  timestamp";
            printSqlResult(expected, query, "timestamp", true, false);
        });
    }

    @Test
    public void testLessThanTimestampFormatYearOnlyPositiveTest1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY");
            execute("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp <'2021'";
            printSqlResult(expected, query, "timestamp", true, false);
        });
    }

    @Test
    public void testLessThanTimestampFormatYearOnlyPositiveTest2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY");
            execute("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where '2019' <  timestamp";
            printSqlResult(expected, query, "timestamp", true, false);
        });
    }

    @Test
    public void testMicrosecondVsNanosecondTimestampAsOfJoin() throws Exception {
        assertMemoryLeak(() -> {
            // Create tables for AS OF JOIN test
            execute("create table micro_events (id int, ts_micro timestamp, value int) timestamp(ts_micro)");
            execute("create table nano_events (id int, ts_nano timestamp_ns, price double) timestamp(ts_nano)");

            // Insert test data with various timestamp precisions
            long baseMicros = 1_577_836_800_123_456L; // 2020-01-01T00:00:00.123456
            long baseNanos = baseMicros * 1000; // 2020-01-01T00:00:00.123456000

            execute("insert into micro_events values (1, " + baseMicros + ", 100)");
            execute("insert into micro_events values (2, " + (baseMicros + 1000) + ", 200)"); // +1ms

            execute("insert into nano_events values (1, " + baseNanos + ", 10.5)");
            execute("insert into nano_events values (2, " + (baseNanos + 500_000) + ", 20.5)"); // +500µs
            execute("insert into nano_events values (3, " + (baseNanos + 1_000_123) + ", 30.5)"); // +1ms+123ns

            // Test AS OF JOIN with mixed timestamp precisions
            assertSql("id\tts_micro\tvalue\tid1\tts_nano\tprice\n" +
                            "1\t2020-01-01T00:00:00.123456Z\t100\t1\t2020-01-01T00:00:00.123456000Z\t10.5\n" + // Connect rows 1 to 1 (same timestamp)
                            "2\t2020-01-01T00:00:00.124456Z\t200\t2\t2020-01-01T00:00:00.123956000Z\t20.5\n", // Connect 2 to 2 (ts_nano in 3 is beyond ts_micro in 2)
                    "select * from micro_events m asof join nano_events n");
        });
    }

    @Test
    public void testMicrosecondVsNanosecondTimestampJoin() throws Exception {
        assertMemoryLeak(() -> {
            // Create tables for JOIN test
            execute("create table micro_table (id int, ts_micro timestamp) timestamp(ts_micro)");
            execute("create table nano_table (id int, ts_nano timestamp_ns) timestamp(ts_nano)");

            // Insert test data: one microsecond value, two nanosecond values (one matching, one with extra precision)
            long micros = 1577836800123456L; // 2020-01-01T00:00:00.123456
            long nanosMatching = micros * 1000; // 2020-01-01T00:00:00.123456000 (matching)
            long nanosWithExtra = nanosMatching + 789; // 2020-01-01T00:00:00.123456789 (extra precision)

            execute("insert into micro_table values (1, " + micros + ")");
            execute("insert into nano_table values (1, " + nanosMatching + "), (2, " + nanosWithExtra + ")");

            // Test 1: JOIN with explicit nanos->micros cast (should work with both nano rows)
            assertSql("id\tts_micro\tid1\tts_nano\n" +
                            "1\t2020-01-01T00:00:00.123456Z\t1\t2020-01-01T00:00:00.123456000Z\n" +
                            "1\t2020-01-01T00:00:00.123456Z\t2\t2020-01-01T00:00:00.123456789Z\n",
                    "select * from micro_table m join nano_table n on m.ts_micro = CAST(n.ts_nano as timestamp)");

            // Test 2: JOIN without explicit cast (should succeed with implicit casting)
            // Only the matching precision row should join
            assertSql("id\tts_micro\tid1\tts_nano\n" +
                            "1\t2020-01-01T00:00:00.123456Z\t1\t2020-01-01T00:00:00.123456000Z\n",
                    "select * from micro_table m join nano_table n on m.ts_micro = n.ts_nano");
        });
    }

    @Test
    public void testMicrosecondVsNanosecondTimestampLtJoin() throws Exception {
        assertMemoryLeak(() -> {
            // Create tables for LT JOIN test
            execute("create table micro_orders (order_id int, ts_micro timestamp, amount int) timestamp(ts_micro)");
            execute("create table nano_trades (trade_id int, ts_nano timestamp_ns, quantity double) timestamp(ts_nano)");

            // Insert test data
            long orderTime = 1_577_836_800_123_456L; // 2020-01-01T00:00:00.123456
            long tradeTime1 = orderTime * 1000 - 1_000_000; // 1ms before order
            long tradeTime2 = orderTime * 1000 + 500_000; // 500µs after order

            execute("insert into micro_orders values (1, " + orderTime + ", 1000)");
            execute("insert into nano_trades values (1, " + tradeTime1 + ", 100.0)");
            execute("insert into nano_trades values (2, " + tradeTime2 + ", 200.0)");

            // Test LT JOIN with mixed timestamp precisions
            assertSql("order_id\tts_micro\tamount\ttrade_id\tts_nano\tquantity\n" +
                            "1\t2020-01-01T00:00:00.123456Z\t1000\t1\t2020-01-01T00:00:00.122456000Z\t100.0\n",
                    "select * from micro_orders m lt join nano_trades t");
        });
    }

    @Test
    public void testMicrosecondVsNanosecondTimestampSpliceJoin() throws Exception {
        assertMemoryLeak(() -> {
            // Create tables for SPLICE JOIN test
            execute("create table micro_base (id int, ts_micro timestamp, status symbol) timestamp(ts_micro)");
            execute("create table nano_updates (id int, ts_nano timestamp_ns, flag symbol) timestamp(ts_nano)");

            // Insert test data with correct timestamp arithmetic
            long baseTime = 1_577_836_800_000_000L; // 2020-01-01T00:00:00.000000 (microseconds)
            execute("insert into micro_base values (1, " + baseTime + ", 'A')");
            execute("insert into micro_base values (2, " + (baseTime + 2_000) + ", 'B')"); // +2ms

            execute("insert into nano_updates values (1, " + (baseTime * 1000 + 1_000_000) + ", 'X')"); // +1ms
            execute("insert into nano_updates values (2, " + (baseTime * 1000 + 3_000_000) + ", 'Y')"); // +3ms

            // Test SPLICE JOIN with mixed timestamp precisions (no ON clause - true SPLICE JOIN semantics)
            // SPLICE JOIN returns all records from both tables with prevailing records
            // Expected: 4 rows total (2 from each table with their prevailing counterparts)
            assertSql("id\tts_micro\tstatus\tid1\tts_nano\tflag\n" +
                            "1\t2020-01-01T00:00:00.000000Z\tA\tnull\t\t\n" + // micro record A: no prevailing nano record
                            "1\t2020-01-01T00:00:00.000000Z\tA\t1\t2020-01-01T00:00:00.001000000Z\tX\n" + // nano record X, A is prevailing micro
                            "2\t2020-01-01T00:00:00.002000Z\tB\t1\t2020-01-01T00:00:00.001000000Z\tX\n" + // micro record B: X is prevailing nano
                            "2\t2020-01-01T00:00:00.002000Z\tB\t2\t2020-01-01T00:00:00.003000000Z\tY\n", // nano record Y: B is prevailing micro
                    "select * from micro_base m splice join nano_updates n");
        });
    }

    @Test
    public void testMicrosecondVsNanosecondTimestampWhereClause() throws Exception {
        assertMemoryLeak(() -> {
            // Create table for WHERE clause test
            execute("create table ts_table (id int, ts_micro timestamp, ts_nano timestamp_ns)");

            // Insert test data: one microsecond value, two nanosecond values (one matching, one with extra precision)
            long micros = 1577836800123456L; // 2020-01-01T00:00:00.123456
            long nanosMatching = micros * 1000; // 2020-01-01T00:00:00.123456000 (matching)
            long nanosWithExtra = nanosMatching + 789; // 2020-01-01T00:00:00.123456789 (extra precision)

            execute("insert into ts_table values (1, " + micros + ", " + nanosMatching + ")");
            execute("insert into ts_table values (2, " + micros + ", " + nanosWithExtra + ")");

            // Test 1: WHERE clause with explicit nanos->micros cast (should work with both rows)
            assertSql("id\tts_micro\tts_nano\n" +
                            "1\t2020-01-01T00:00:00.123456Z\t2020-01-01T00:00:00.123456000Z\n" +
                            "2\t2020-01-01T00:00:00.123456Z\t2020-01-01T00:00:00.123456789Z\n",
                    "select * from ts_table where ts_micro = CAST(ts_nano as timestamp)");

            // Test 2: WHERE clause without explicit cast
            // Row 1 has matching precision: microseconds convert to same nanoseconds
            assertSql("id\tts_micro\tts_nano\n" +
                            "1\t2020-01-01T00:00:00.123456Z\t2020-01-01T00:00:00.123456000Z\n",
                    "select * from ts_table where ts_micro = ts_nano");
        });
    }

    @Test
    public void testMinOnTimestampEmptyResutlSetIsNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tt (dts timestamp, nts timestamp) timestamp(dts)");
            // insert same values to dts (designated) as nts (non-designated) timestamp
            execute("insert into tt " +
                    "select timestamp_sequence(1577836800000000L, 60*60*1000000L), timestamp_sequence(1577836800000000L, 60*60*1000000L) " +
                    "from long_sequence(48L)");

            String expected = "min\tmax\tcount\n\t\t0\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts), count() from tt where nts < '2020-01-01'");
            assertTimestampTtQuery(expected, "select min(nts), max(nts), count() from tt where '2020-01-01' > nts");
        });
    }

    @Test
    public void testMoreThanOrEqualsToTimestampFormatYearOnlyNegativeTest1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY");
            execute("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp >= '2021'";
            printSqlResult(expected, query, "timestamp", true, false);
        });
    }

    @Test
    public void testMoreThanOrEqualsToTimestampFormatYearOnlyNegativeTest2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY");
            execute("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n";
            query = "SELECT * FROM ob_mem_snapshot where '2019' >=  timestamp";
            printSqlResult(expected, query, "timestamp", true, false);
        });
    }

    @Test
    public void testMoreThanOrEqualsToTimestampFormatYearOnlyPositiveTest2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY");
            execute("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where '2021-01-01' >=  timestamp";
            printSqlResult(expected, query, "timestamp", true, false);
        });
    }

    @Test
    public void testMoreThanTimestampFormatYearOnlyNegativeTest1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY");
            execute("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp >= '2021'";
            printSqlResult(expected, query, "timestamp", true, false);
        });
    }

    @Test
    public void testMoreThanTimestampFormatYearOnlyNegativeTest2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY");
            execute("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n";
            query = "SELECT * FROM ob_mem_snapshot where '2020' > timestamp";
            printSqlResult(expected, query, "timestamp", true, false);
        });
    }

    @Test
    public void testMoreThanTimestampFormatYearOnlyPositiveTest2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY");
            //insert
            execute("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where '2021' >  timestamp";
            printSqlResult(expected, query, "timestamp", true, false);
        });
    }

    @Test
    public void testNonContinuousPartitions() throws Exception {
        setCurrentMicros(0);
        assertMemoryLeak(() -> {
            // Create table
            // One-hour step timestamps from epoch for 32 then skip 48 etc for 10 iterations
            final int count = 32;
            final int skip = 48;
            final int iterations = 10;
            final long hour = Micros.HOUR_MICROS;

            String createStmt = "create table xts (ts Timestamp) timestamp(ts) partition by DAY";
            execute(createStmt);
            long start = 0;
            List<Object[]> datesArr = new ArrayList<>();
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'.000000Z'");
            formatter.setTimeZone(TimeZone.getTimeZone("UTC"));

            for (int i = 0; i < iterations; i++) {
                String insert = "insert into xts " +
                        "select timestamp_sequence(" + start + "L, 3600L * 1000 * 1000) ts from long_sequence(" + count + ")";
                execute(insert);
                for (long ts = 0; ts < count; ts++) {
                    long nextTs = start + ts * hour;
                    datesArr.add(new Object[]{nextTs, formatter.format(nextTs / 1000L)});
                }
                start += (count + skip) * hour;
            }
            final long end = start;

            // Search with 3-hour window every 22 hours
            int min = Integer.MAX_VALUE;
            int max = Integer.MIN_VALUE;
            for (long micros = 0; micros < end; micros += 22 * hour) {
                setCurrentMicros(micros);
                int results = compareNowRange(
                        "select ts FROM xts WHERE ts <= dateadd('h', 2, now()) and ts >= dateadd('h', -1, now())",
                        datesArr,
                        ts -> ts >= (currentMicros - hour) && (ts <= currentMicros + 2 * hour)
                );
                min = Math.min(min, results);
                max = Math.max(max, results);
            }

            Assert.assertEquals(0, min);
            Assert.assertEquals(4, max);
        });
    }

    @Test
    public void testNowIsSameForAllQueryParts() throws Exception {
        setCurrentMicros(0);
        assertMemoryLeak(() -> {
            execute("create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY");
            execute("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "now1\tnow2\tsymbol\ttimestamp\n" +
                    "1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:00.000000Z\t1\t2020-12-31T23:59:59.000000Z\n";

            String query1 = "select now() as now1, now() as now2, symbol, timestamp FROM ob_mem_snapshot WHERE now() = now()";
            printSqlResult(expected, query1, "timestamp", true, false);

            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot where timestamp > now()";
            printSqlResult(expected, query, "timestamp", true, false);
        });
    }

    @Test
    public void testNowPerformsBinarySearchOnTimestamp() throws Exception {
        setCurrentMicros(0);
        assertMemoryLeak(() -> {
            // One-hour step timestamps from epoch for 2000 steps
            final int count = 200;
            String createStmt = "create table xts as (select timestamp_sequence(0, 3600L * 1000 * 1000) ts from long_sequence(" + count + ")) timestamp(ts) partition by DAY";
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'.000000Z'");
            execute(createStmt);

            formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
            Stream<Object[]> dates = LongStream.rangeClosed(0, count - 1)
                    .map(i -> i * 3600L * 1000)
                    .mapToObj(ts -> new Object[]{ts * 1000L, formatter.format(new Date(ts))});

            List<Object[]> datesArr = dates.collect(Collectors.toList());

            final long hour = Micros.HOUR_MICROS;
            final long day = 24 * hour;
            compareNowRange("select * FROM xts WHERE ts >= '1970' and ts <= '2021'", datesArr, ts -> true);

            // Scroll now to the end
            setCurrentMicros(200L * hour);
            compareNowRange("select ts FROM xts WHERE ts >= now() - 3600 * 1000 * 1000L", datesArr, ts -> ts >= currentMicros - hour);
            compareNowRange("select ts FROM xts WHERE ts >= now() + 3600 * 1000 * 1000L", datesArr, ts -> ts >= currentMicros + hour);

            for (long micros = hour; micros < count * hour; micros += day) {
                setCurrentMicros(micros);
                compareNowRange("select ts FROM xts WHERE ts < now()", datesArr, ts -> ts < currentMicros);
            }

            for (long micros = hour; micros < count * hour; micros += 12 * hour) {
                setCurrentMicros(micros);
                compareNowRange("select ts FROM xts WHERE ts >= now()", datesArr, ts -> ts >= currentMicros);
            }

            for (long micros = 0; micros < count * hour + 4 * day; micros += 5 * hour) {
                setCurrentMicros(micros);
                compareNowRange(
                        "select ts FROM xts WHERE ts <= dateadd('d', -1, now()) and ts >= dateadd('d', -2, now())",
                        datesArr,
                        ts -> ts >= (currentMicros - 2 * day) && (ts <= currentMicros - day)
                );
            }

            setCurrentMicros(100L * hour);
            compareNowRange("WITH temp AS (SELECT ts FROM xts WHERE ts > dateadd('y', -1, now())) " +
                    "SELECT ts FROM temp WHERE ts < now()", datesArr, ts -> ts < currentMicros);
        });
    }

    @Test
    public void testTimestampConversion() throws Exception {
        assertMemoryLeak(() -> {
            TableModel m = new TableModel(configuration, "tt", PartitionBy.DAY);
            m.timestamp("dts")
                    .col("ts", ColumnType.TIMESTAMP);
            createPopulateTable(m, 31, "2021-03-14", 31);
            String expected = "dts\tts\n" +
                    "2021-04-02T23:59:59.354820Z\t2021-04-02T23:59:59.354820Z\n";

            assertQuery(
                    expected,
                    "tt where dts > '2021-04-02T13:45:49.207Z' and dts < '2021-04-03 13:45:49.207'",
                    "dts",
                    true,
                    false
            );

            assertQuery(
                    expected,
                    "tt where ts > '2021-04-02T13:45:49.207Z' and ts < '2021-04-03 13:45:49.207'",
                    "dts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testTimestampDifferentThanFixedValue() throws Exception {
        assertQuery(
                "t\n" +
                        "1970-01-01T00:00:01.000000Z\n" +
                        "1970-01-01T00:00:02.000000Z\n",
                "select t from x where t != to_timestamp('1970-01-01:00:00:00', 'yyyy-MM-dd:HH:mm:ss') ",
                "create table x as (select timestamp_sequence(0, 1000000) t from long_sequence(3)) timestamp(t)",
                "t",
                null,
                null,
                true,
                false,
                false
        );
    }

    @Test
    public void testTimestampDifferentThanNonFixedValue() throws Exception {
        assertQuery(
                "t\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-01T00:00:01.000000Z\n",
                "select t from x where t != to_timestamp('201' || rnd_long(0,9,0),'yyyy')",
                "create table x as (select timestamp_sequence(0, 1000000) t from long_sequence(2)) timestamp(t)",
                "t",
                null,
                null,
                true,
                false,
                false
        );
    }

    @Test
    public void testTimestampInDay1orDay2() throws Exception {
        assertQuery(
                "min\tmax\n\t\n",
                "select min(nts), max(nts) from tt where nts IN '2020-01-01' or nts IN '2020-01-02'",
                "create table tt (dts timestamp, nts timestamp) timestamp(dts)",
                null,
                "insert into tt " +
                        "select timestamp_sequence(1577836800000000L, 60*60*1000000L), timestamp_sequence(1577836800000000L, 60*60*1000000L) " +
                        "from long_sequence(48L)",
                "min\tmax\n" +
                        "2020-01-01T00:00:00.000000Z\t2020-01-02T23:00:00.000000Z\n",
                false,
                true,
                false
        );
    }

    @Test
    public void testTimestampIntervalPartitionDay() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table interval_test(seq_num long, timestamp timestamp) timestamp(timestamp) partition by DAY");
            execute("insert into interval_test select x, timestamp_sequence(" +
                    "'2022-11-19T00:00:00', " +
                    Micros.DAY_MICROS + ") FROM long_sequence(5)");
            String expected = "seq_num\ttimestamp\n" +
                    "1\t2022-11-19T00:00:00.000000Z\n" +
                    "2\t2022-11-20T00:00:00.000000Z\n" +
                    "3\t2022-11-21T00:00:00.000000Z\n" +
                    "4\t2022-11-22T00:00:00.000000Z\n" +
                    "5\t2022-11-23T00:00:00.000000Z\n";
            String query = "select * from interval_test";
            assertSql(expected, query);
            // test mid-case
            expected = "seq_num\ttimestamp\n" +
                    "3\t2022-11-21T00:00:00.000000Z\n";
            query = "SELECT * FROM interval_test where timestamp IN '2022-11-21'";
            assertSql(expected, query);
        });
    }

    @Test
    public void testTimestampIntervalPartitionMonth() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table interval_test(seq_num long, timestamp timestamp) timestamp(timestamp) partition by MONTH");
            execute("insert into interval_test select x, timestamp_sequence(" +
                    "'2022-11-19T00:00:00', " +
                    Micros.DAY_MICROS * 30 + ") FROM long_sequence(5)");
            String expected = "seq_num\ttimestamp\n" +
                    "1\t2022-11-19T00:00:00.000000Z\n" +
                    "2\t2022-12-19T00:00:00.000000Z\n" +
                    "3\t2023-01-18T00:00:00.000000Z\n" +
                    "4\t2023-02-17T00:00:00.000000Z\n" +
                    "5\t2023-03-19T00:00:00.000000Z\n";
            String query = "select * from interval_test";
            assertSql(expected, query);
            // test mid-case
            expected = "seq_num\ttimestamp\n" +
                    "3\t2023-01-18T00:00:00.000000Z\n";
            query = "SELECT * FROM interval_test where timestamp IN '2023-01'";
            assertSql(expected, query);
        });
    }

    @Test
    public void testTimestampIntervalPartitionWeek() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table interval_test(seq_num long, timestamp timestamp) timestamp(timestamp) partition by WEEK");
            execute("insert into interval_test select x, timestamp_sequence(" +
                    "'2022-11-19T00:00:00', " +
                    Micros.WEEK_MICROS + ") FROM long_sequence(5)");
            String expected = "seq_num\ttimestamp\n" +
                    "1\t2022-11-19T00:00:00.000000Z\n" +
                    "2\t2022-11-26T00:00:00.000000Z\n" +
                    "3\t2022-12-03T00:00:00.000000Z\n" +
                    "4\t2022-12-10T00:00:00.000000Z\n" +
                    "5\t2022-12-17T00:00:00.000000Z\n";
            String query = "select * from interval_test";
            assertSql(expected, query);
            // test mid-case
            expected = "seq_num\ttimestamp\n" +
                    "3\t2022-12-03T00:00:00.000000Z\n";
            query = "SELECT * FROM interval_test where timestamp IN '2022-12-03'";
            assertSql(expected, query);
        });
    }

    @Test
    public void testTimestampIntervalPartitionYear() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table interval_test(seq_num long, timestamp timestamp) timestamp(timestamp) partition by YEAR");
            execute("insert into interval_test select x, timestamp_sequence(" +
                    "'2022-11-19T00:00:00', " +
                    Micros.DAY_MICROS * 365 + ") FROM long_sequence(5)");
            String expected = "seq_num\ttimestamp\n" +
                    "1\t2022-11-19T00:00:00.000000Z\n" +
                    "2\t2023-11-19T00:00:00.000000Z\n" +
                    "3\t2024-11-18T00:00:00.000000Z\n" +
                    "4\t2025-11-18T00:00:00.000000Z\n" +
                    "5\t2026-11-18T00:00:00.000000Z\n";
            String query = "select * from interval_test";
            assertSql(expected, query);
            // test mid-case
            expected = "seq_num\ttimestamp\n" +
                    "3\t2024-11-18T00:00:00.000000Z\n";
            query = "SELECT * FROM interval_test where timestamp IN '2024'";
            assertSql(expected, query);
        });
    }

    @Test
    public void testTimestampMin() throws Exception {
        assertQuery(
                "nts\tmin\n" +
                        "nts\t\n",
                "select 'nts', min(nts) from tt where nts > '2020-01-01T00:00:00.000000Z'",
                "create table tt (dts timestamp, nts timestamp) timestamp(dts)",
                null,
                "insert into tt " +
                        "select timestamp_sequence(1577836800000000L, 10L), timestamp_sequence(1577836800000000L, 10L) " +
                        "from long_sequence(2L)",
                "nts\tmin\n" +
                        "nts\t2020-01-01T00:00:00.000010Z\n",
                false,
                true,
                false
        );
    }

    @Test
    public void testTimestampNanoWithTimezone() throws Exception {
        // with constant
        assertSql("ts\n" +
                        "2020-01-01T00:00:00.000000001Z\n",
                "select '2020-01-01T00:00:00.000000001Z'::timestamp_ns with time zone ts");

        // with function
        assertSql("ts\n" +
                        "2020-01-01T01:02:03.123456789Z\n",
                "select concat('2020-01-01T','01:02:03.123456789Z')::timestamp_ns with time zone ts");

        // with column
        assertMemoryLeak(() -> {
            execute("create table tt (vch varchar, ts timestamp_ns)");
            execute("insert into tt values ('2020-01-01T00:00:00.000000123Z', '2020-01-01T00:00:00.000000123Z'::timestamp_ns with time zone)");

            assertSql("ts\n" +
                            "2020-01-01T00:00:00.000000123Z\n",
                    "select vch::timestamp_ns with time zone ts from tt");
        });
    }

    @Test
    public void testTimestampOpSymbolColumns() throws Exception {
        assertQuery(
                "a\tk\n" +
                        "1970-01-01T00:00:00.040000Z\t1970-01-01T00:00:00.030000Z\n" +
                        "1970-01-01T00:00:00.050000Z\t1970-01-01T00:00:00.040000Z\n",
                "select a, k from x where k < cast(a as timestamp)",
                "create table x as (select cast(concat('1970-01-01T00:00:00.0', (case when x > 3 then x else x - 1 end), '0000Z') as symbol) a, timestamp_sequence(0, 10000) k from long_sequence(5)) timestamp(k)",
                "k",
                null,
                null,
                true,
                false,
                false
        );
    }

    @Test
    public void testTimestampParseWithYearMonthDayTHourMinuteSecondAndIncompleteMillisTimeZone() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY");
            execute("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // 2 ms characters
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp IN '2020-12-31T23:59:59.00Z'";
            printSqlResult(expected, query, "timestamp", true, false);
            // 1 ms character
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp IN '2020-12-31T23:59:59.0Z'";
            printSqlResult(expected, query, "timestamp", true, false);
            printSqlResult(expected, query, "timestamp", true, false);
        });
    }

    @Test
    public void testTimestampParseWithYearMonthDayTHourMinuteSecondTimeZone() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY");
            execute("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp ='2020-12-31T23:59:59Z'";
            printSqlResult(expected, query, "timestamp", true, false);
        });
    }

    @Test
    public void testTimestampStringComparison() throws Exception {
        assertQuery(
                "min\tmax\n\t\n",
                "select min(nts), max(nts) from tt where nts = '2020-01-01'",
                "create table tt (dts timestamp, nts timestamp) timestamp(dts)",
                null,
                "insert into tt " +
                        "select timestamp_sequence(1577836800000000L, 60*60*1000000L), timestamp_sequence(1577836800000000L, 60*60*1000000L) " +
                        "from long_sequence(48L)",
                "min\tmax\n" +
                        "2020-01-01T00:00:00.000000Z\t2020-01-01T00:00:00.000000Z\n",
                false,
                true,
                false
        );
    }

    @Test
    public void testTimestampStringComparisonBetween() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tt (dts timestamp, nts timestamp) timestamp(dts)");
            // insert same values to dts (designated) as nts (non-designated) timestamp
            execute("insert into tt " +
                    "select timestamp_sequence(1577836800000000L, 60*60*1000000L), timestamp_sequence(1577836800000000L, 60*60*1000000L) " +
                    "from long_sequence(48L)");

            String expected;
            // between constants
            expected = "min\tmax\n" +
                    "2020-01-01T00:00:00.000000Z\t2020-01-02T00:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts between '2020-01-01' and '2020-01-02' ");
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts between '2020-01-02' and '2020-01-01' ");

            // Between non-constants
            expected = "min\tmax\n" +
                    "2020-01-01T12:00:00.000000Z\t2020-01-02T00:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts between '2020-01-02' and dateadd('d', -1, '2020-01-01') and nts >= '2020-01-01T12:00'");

            // NOT between constants
            expected = "min\tmax\n" +
                    "2020-01-02T01:00:00.000000Z\t2020-01-02T23:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts not between '2020-01-01' and '2020-01-02' ");

            // NOT between non-constants
            expected = "min\tmax\n" +
                    "2020-01-01T01:00:00.000000Z\t2020-01-02T23:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts not between dateadd('d', -1, '2020-01-01') and '2020-01-01'");

            // Non constant
            expected = "min\tmax\n" +
                    "2020-01-01T00:00:00.000000Z\t2020-01-02T00:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts between '2020-01-' || '02' and dateadd('d', -1, nts)");

            // Runtime constant TernaryFunction
            expected = "min\tmax\n" +
                    "2020-01-02T00:00:00.000000Z\t2020-01-02T23:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts between '2020-01-02' and dateadd(CAST(rnd_str('s', 's') as CHAR), rnd_short(0, 1), now())");

            // NOT between Non constant
            expected = "min\tmax\n" +
                    "2020-01-02T01:00:00.000000Z\t2020-01-02T23:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts not between '2020-01-' || '02' and dateadd('d', -1, nts)");

            // NOT between in case
            expected = "sum\n" +
                    "0\n";
            assertTimestampTtQuery(expected, "select sum(case when nts not between now() and '2020-01-01' then 1 else 0 end) from tt");

            // Between runtime constant inside case
            expected = "min\tmax\n" +
                    "2020-01-02T00:00:00.000000Z\t2020-01-02T23:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts between '2020-01-02' and case when 1=1 then now() else now() end");

            // Between with NULL and NULL
            expected = "dts\tnts\n";
            assertTimestampTtQuery(expected, "select * from tt where nts between NULL and NULL");

            // Not between with NULL and NULL
            expected = "dts\tnts\n" +
                    "2020-01-01T00:00:00.000000Z\t2020-01-01T00:00:00.000000Z\n" +
                    "2020-01-01T01:00:00.000000Z\t2020-01-01T01:00:00.000000Z\n" +
                    "2020-01-01T02:00:00.000000Z\t2020-01-01T02:00:00.000000Z\n" +
                    "2020-01-01T03:00:00.000000Z\t2020-01-01T03:00:00.000000Z\n" +
                    "2020-01-01T04:00:00.000000Z\t2020-01-01T04:00:00.000000Z\n" +
                    "2020-01-01T05:00:00.000000Z\t2020-01-01T05:00:00.000000Z\n" +
                    "2020-01-01T06:00:00.000000Z\t2020-01-01T06:00:00.000000Z\n" +
                    "2020-01-01T07:00:00.000000Z\t2020-01-01T07:00:00.000000Z\n" +
                    "2020-01-01T08:00:00.000000Z\t2020-01-01T08:00:00.000000Z\n" +
                    "2020-01-01T09:00:00.000000Z\t2020-01-01T09:00:00.000000Z\n" +
                    "2020-01-01T10:00:00.000000Z\t2020-01-01T10:00:00.000000Z\n" +
                    "2020-01-01T11:00:00.000000Z\t2020-01-01T11:00:00.000000Z\n" +
                    "2020-01-01T12:00:00.000000Z\t2020-01-01T12:00:00.000000Z\n" +
                    "2020-01-01T13:00:00.000000Z\t2020-01-01T13:00:00.000000Z\n" +
                    "2020-01-01T14:00:00.000000Z\t2020-01-01T14:00:00.000000Z\n" +
                    "2020-01-01T15:00:00.000000Z\t2020-01-01T15:00:00.000000Z\n" +
                    "2020-01-01T16:00:00.000000Z\t2020-01-01T16:00:00.000000Z\n" +
                    "2020-01-01T17:00:00.000000Z\t2020-01-01T17:00:00.000000Z\n" +
                    "2020-01-01T18:00:00.000000Z\t2020-01-01T18:00:00.000000Z\n" +
                    "2020-01-01T19:00:00.000000Z\t2020-01-01T19:00:00.000000Z\n" +
                    "2020-01-01T20:00:00.000000Z\t2020-01-01T20:00:00.000000Z\n" +
                    "2020-01-01T21:00:00.000000Z\t2020-01-01T21:00:00.000000Z\n" +
                    "2020-01-01T22:00:00.000000Z\t2020-01-01T22:00:00.000000Z\n" +
                    "2020-01-01T23:00:00.000000Z\t2020-01-01T23:00:00.000000Z\n" +
                    "2020-01-02T00:00:00.000000Z\t2020-01-02T00:00:00.000000Z\n" +
                    "2020-01-02T01:00:00.000000Z\t2020-01-02T01:00:00.000000Z\n" +
                    "2020-01-02T02:00:00.000000Z\t2020-01-02T02:00:00.000000Z\n" +
                    "2020-01-02T03:00:00.000000Z\t2020-01-02T03:00:00.000000Z\n" +
                    "2020-01-02T04:00:00.000000Z\t2020-01-02T04:00:00.000000Z\n" +
                    "2020-01-02T05:00:00.000000Z\t2020-01-02T05:00:00.000000Z\n" +
                    "2020-01-02T06:00:00.000000Z\t2020-01-02T06:00:00.000000Z\n" +
                    "2020-01-02T07:00:00.000000Z\t2020-01-02T07:00:00.000000Z\n" +
                    "2020-01-02T08:00:00.000000Z\t2020-01-02T08:00:00.000000Z\n" +
                    "2020-01-02T09:00:00.000000Z\t2020-01-02T09:00:00.000000Z\n" +
                    "2020-01-02T10:00:00.000000Z\t2020-01-02T10:00:00.000000Z\n" +
                    "2020-01-02T11:00:00.000000Z\t2020-01-02T11:00:00.000000Z\n" +
                    "2020-01-02T12:00:00.000000Z\t2020-01-02T12:00:00.000000Z\n" +
                    "2020-01-02T13:00:00.000000Z\t2020-01-02T13:00:00.000000Z\n" +
                    "2020-01-02T14:00:00.000000Z\t2020-01-02T14:00:00.000000Z\n" +
                    "2020-01-02T15:00:00.000000Z\t2020-01-02T15:00:00.000000Z\n" +
                    "2020-01-02T16:00:00.000000Z\t2020-01-02T16:00:00.000000Z\n" +
                    "2020-01-02T17:00:00.000000Z\t2020-01-02T17:00:00.000000Z\n" +
                    "2020-01-02T18:00:00.000000Z\t2020-01-02T18:00:00.000000Z\n" +
                    "2020-01-02T19:00:00.000000Z\t2020-01-02T19:00:00.000000Z\n" +
                    "2020-01-02T20:00:00.000000Z\t2020-01-02T20:00:00.000000Z\n" +
                    "2020-01-02T21:00:00.000000Z\t2020-01-02T21:00:00.000000Z\n" +
                    "2020-01-02T22:00:00.000000Z\t2020-01-02T22:00:00.000000Z\n" +
                    "2020-01-02T23:00:00.000000Z\t2020-01-02T23:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select * from tt where nts not between NULL and NULL");

            // Between with NULL
            expected = "dts\tnts\n";
            assertTimestampTtQuery(expected, "select * from tt where nts between CAST(NULL as TIMESTAMP) and '2020-01-01'");
            assertTimestampTtQuery(expected, "select * from tt where nts between NULL and '2020-01-01'");
            assertTimestampTtQuery(expected, "select * from tt where nts between '2020-01-01' and NULL");

            // NOT Between with NULL
            expected = "min\tmax\n" +
                    "2020-01-01T00:00:00.000000Z\t2020-01-02T23:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts not between CAST(NULL as TIMESTAMP) and '2020-01-01'");
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts not between NULL and '2020-01-01'");
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts not between '2020-01-01' and NULL");

            // Between with NULL and now()
            expected = "dts\tnts\n";
            assertTimestampTtQuery(expected, "select * from tt where nts between CAST(NULL as TIMESTAMP) and now()");
            assertTimestampTtQuery(expected, "select * from tt where nts between NULL and now()");

            // Between with now() and NULL
            expected = "dts\tnts\n";
            assertTimestampTtQuery(expected, "select * from tt where nts between now() and CAST(NULL as TIMESTAMP)");
            assertTimestampTtQuery(expected, "select * from tt where nts between now() and NULL");

            // NOT Between with NULL and now()
            expected = "min\tmax\n" +
                    "2020-01-01T00:00:00.000000Z\t2020-01-02T23:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts not between CAST(NULL as TIMESTAMP) and now()");
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts not between NULL and now()");

            // NOT Between with now() and NULL
            expected = "min\tmax\n" +
                    "2020-01-01T00:00:00.000000Z\t2020-01-02T23:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts not between now() and CAST(NULL as TIMESTAMP)");
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts not between now() and NULL");

            // Between runtime const evaluating to NULL
            expected = "dts\tnts\n";
            assertTimestampTtQuery(expected, "select * from tt where nts between (now() + CAST(NULL AS LONG)) and now()");

            // NOT Between runtime const evaluating to NULL
            expected = "min\tmax\n" +
                    "2020-01-01T00:00:00.000000Z\t2020-01-02T23:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts not between (now() + CAST(NULL AS LONG)) and now()");

            // NOT Between runtime const evaluating to invalid string
            expected = "min\tmax\n" +
                    "2020-01-01T00:00:00.000000Z\t2020-01-02T23:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts not between cast((to_str(now(), 'yyyy-MM-dd') || '-222') as timestamp) and now()");

            // Between columns
            expected = "min\tmax\n" +
                    "2020-01-01T00:00:00.000000Z\t2020-01-02T23:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts between nts and dts");
        });
    }

    @Test
    public void testTimestampStringComparisonBetweenInvalidValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tt (dts timestamp, nts timestamp) timestamp(dts)");
            // insert same values to dts (designated) as nts (non-designated) timestamp
            execute("insert into tt " +
                    "select timestamp_sequence(1577836800000000L, 60*60*1000000L), timestamp_sequence(1577836800000000L, 60*60*1000000L) " +
                    "from long_sequence(48L)");

            assertTimestampTtFailedQuery("select min(nts), max(nts) from tt where nts between 'invalid' and '2020-01-01'", 52, "Invalid date");
            assertTimestampTtFailedQuery("select min(nts), max(nts) from tt where nts between '2020-01-01' and 'invalid'", 69, "Invalid date");
            assertTimestampTtFailedQuery("select min(nts), max(nts) from tt where nts between '2020-01-01' and 'invalid' || 'dd'", 79, "Invalid date");
            assertTimestampTtFailedQuery("select min(nts), max(nts) from tt where invalidCol not between '2020-01-01' and '2020-01-02'", 40, "Invalid column: invalidCol");
            assertTimestampTtFailedQuery("select min(nts), max(nts) from tt where nts in ('2020-01-01', 'invalid')", 62, "Invalid date");
            assertTimestampTtFailedQuery("select min(nts), max(nts) from tt where nts in (select nts from tt)", 48, "cannot compare TIMESTAMP with type CURSOR");
        });
    }

    @Test
    public void testTimestampStringComparisonInString() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tt (dts timestamp, nts timestamp) timestamp(dts)");
            // insert same values to dts (designated) as nts (non-designated) timestamp
            execute("insert into tt " +
                    "select timestamp_sequence(1577836800000000L, 60*60*1000000L), timestamp_sequence(1577836800000000L, 60*60*1000000L) " +
                    "from long_sequence(48L)");

            String expected;
            // not in period
            expected = "min\tmax\n" +
                    "2020-01-02T00:00:00.000000Z\t2020-01-02T23:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts not in '2020-01-01'");

            expected = "min\tmax\n" +
                    "2020-01-01T00:00:00.000000Z\t2020-01-01T23:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts in '2020-01-01'");

            expected = "min\tmax\n" +
                    "2020-01-01T00:00:00.000000Z\t2020-01-01T23:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts not in ('2020-01' || '-02')");

            expected = "min\tmax\n" +
                    "2020-01-01T00:00:00.000000Z\t2020-01-01T23:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts in ('2020-01-' || rnd_str('01', '01'))");

            expected = "min\tmax\n" +
                    "2020-01-01T00:00:00.000000Z\t2020-01-01T12:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts in ('2020-01-01T12:00', '2020-01-01')");

            expected = "min\tmax\n" +
                    "2020-01-01T00:00:00.000000Z\t2020-01-01T00:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where dts in ('2020-01-01', '2020-01-03') ");

            expected = "min\tmax\n" +
                    "2020-01-02T00:00:00.000000Z\t2020-01-02T23:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts in '2020-01' || '-02'");

            expected = "dts\tnts\n" +
                    "2020-01-02T00:00:00.000000Z\t2020-01-02T00:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select * from tt where nts in (NULL, cast('2020-01-05' as TIMESTAMP), '2020-01-02', NULL)");

            expected = "min\tmax\n" +
                    "2020-01-01T00:00:00.000000Z\t2020-01-02T23:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts in (now(),'2020-01-01',1234567,1234567L,CAST('2020-01-01' as TIMESTAMP),NULL,nts)");

            expected = "min\tmax\n" +
                    "2020-01-01T00:00:00.000000Z\t2020-01-01T00:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts in (now(),'2020-01-01')");

            expected = "dts\tnts\n";
            assertTimestampTtQuery(expected, "select * from tt where CAST(NULL as TIMESTAMP) in ('2020-01-02', now())");

            expected = "dts\tnts\n";
            assertTimestampTtQuery(expected, "select * from tt where CAST(NULL as TIMESTAMP) in ('2020-01-02', '2020-01-01')");

            expected = "dts\tnts\n";
            assertTimestampTtQuery(expected, "select * from tt where nts in now()");
        });
    }

    @Test
    public void testTimestampStringComparisonInVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tt (dts timestamp, nts timestamp) timestamp(dts)");
            // insert same values to dts (designated) as nts (non-designated) timestamp
            execute("insert into tt " +
                    "select timestamp_sequence(1577836800000000L, 60*60*1000000L), timestamp_sequence(1577836800000000L, 60*60*1000000L) " +
                    "from long_sequence(48L)");

            String expected;
            // not in period
            expected = "min\tmax\n" +
                    "2020-01-02T00:00:00.000000Z\t2020-01-02T23:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts not in '2020-01-01'::varchar");

            expected = "min\tmax\n" +
                    "2020-01-01T00:00:00.000000Z\t2020-01-01T23:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts in '2020-01-01'::varchar");

            expected = "min\tmax\n" +
                    "2020-01-01T00:00:00.000000Z\t2020-01-01T23:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts not in cast(('2020-01' || '-02') as varchar)");

            expected = "min\tmax\n" +
                    "2020-01-01T00:00:00.000000Z\t2020-01-01T23:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts in cast(('2020-01-' || rnd_str('01', '01')) as varchar)");

            expected = "min\tmax\n" +
                    "2020-01-01T00:00:00.000000Z\t2020-01-01T12:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts in ('2020-01-01T12:00'::varchar, '2020-01-01'::varchar)");

            expected = "min\tmax\n" +
                    "2020-01-01T00:00:00.000000Z\t2020-01-01T00:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where dts in ('2020-01-01'::varchar, '2020-01-03'::varchar) ");

            expected = "min\tmax\n" +
                    "2020-01-02T00:00:00.000000Z\t2020-01-02T23:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts in cast('2020-01' || '-02' as varchar)");

            expected = "dts\tnts\n" +
                    "2020-01-02T00:00:00.000000Z\t2020-01-02T00:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select * from tt where nts in (NULL, cast('2020-01-05' as TIMESTAMP), '2020-01-02'::varchar, NULL)");

            expected = "min\tmax\n" +
                    "2020-01-01T00:00:00.000000Z\t2020-01-02T23:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts in (now(),'2020-01-01'::varchar,1234567,1234567L,CAST('2020-01-01' as TIMESTAMP),NULL::varchar,nts)");

            expected = "min\tmax\n" +
                    "2020-01-01T00:00:00.000000Z\t2020-01-01T00:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts in (now(),'2020-01-01'::varchar)");

            expected = "dts\tnts\n";
            assertTimestampTtQuery(expected, "select * from tt where CAST(NULL as TIMESTAMP) in ('2020-01-02'::varchar, now())");

            expected = "dts\tnts\n";
            assertTimestampTtQuery(expected, "select * from tt where CAST(NULL as TIMESTAMP) in ('2020-01-02'::varchar, '2020-01-01'::varchar)");
        });
    }

    @Test
    public void testTimestampStringComparisonInvalidValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tt (dts timestamp, nts timestamp) timestamp(dts)");
            // insert same values to dts (designated) as nts (non-designated) timestamp
            execute("insert into tt " +
                    "select timestamp_sequence(1577836800000000L, 60*60*1000000L), timestamp_sequence(1577836800000000L, 60*60*1000000L) " +
                    "from long_sequence(48L)");

            assertTimestampTtFailedQuery("select min(nts), max(nts) from tt where nts > 'invalid'", 46, "Invalid date");
            assertTimestampTtFailedQuery("select min(nts), max(nts) from tt where '2020-01-01' in (0.34)", 57, "cannot compare STRING with type DOUBLE");
        });
    }

    @Test
    public void testTimestampStringComparisonNonConst() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tt (dts timestamp, nts timestamp) timestamp(dts)");
            // insert same values to dts (designated) as nts (non-designated) timestamp
            execute("insert into tt " +
                    "select timestamp_sequence(1577836800000000L, 60*60*1000000L), timestamp_sequence(1577836800000000L, 60*60*1000000L) " +
                    "from long_sequence(48L)");

            String expected;
            // between constants
            expected = "min\tmax\n" +
                    "2020-01-01T00:00:00.000000Z\t2020-01-02T00:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts = cast( to_str(nts,'yyyy-MM-dd') as timestamp)");
        });
    }

    @Test
    public void testTimestampStringComparisonWithString() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tt (dts timestamp, nts timestamp) timestamp(dts)");
            // insert same values to dts (designated) as nts (non-designated) timestamp
            execute("insert into tt " +
                    "select timestamp_sequence(1577836800000000L, 60*60*1000000L), timestamp_sequence(1577836800000000L, 60*60*1000000L) " +
                    "from long_sequence(48L)");

            String expected;
            // >
            expected = "min\tmax\n" +
                    "2020-01-01T01:00:00.000000Z\t2020-01-02T23:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts > '2020-01-01'");
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where '2020-01-01' < nts");

            // >=
            expected = "min\tmax\n" +
                    "2020-01-01T00:00:00.000000Z\t2020-01-02T23:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts >= '2020-01-01'");
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where '2020-01-01' <= nts");

            // <
            expected = "min\tmax\n" +
                    "2020-01-01T00:00:00.000000Z\t2020-01-01T00:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts < '2020-01-01T01:00:00'");
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where '2020-01-01T01:00:00' > nts");

            // <=
            expected = "min\tmax\n" +
                    "2020-01-01T00:00:00.000000Z\t2020-01-01T01:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts <= '2020-01-01T01:00:00'");
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where '2020-01-01T01:00:00' >= nts");

            expected = "min\tmax\n" +
                    "2020-01-01T00:00:00.000000Z\t2020-01-01T11:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts < dateadd('d',-1, '2020-01-02T12:00:00')");
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where dateadd('d', -1, '2020-01-02T12:00:00') > nts");

            expected = "dts\tnts\n";
            assertTimestampTtQuery(expected, "select * from tt where nts > (case when 1=1 THEN dts else now() end)");
        });
    }

    @Test
    public void testTimestampStringDateAdd() throws Exception {
        assertQuery(
                "dateadd\n" +
                        "2020-01-02T00:00:00.000000Z\n",
                "select dateadd('d', 1, '2020-01-01')",
                null,
                null,
                null,
                null,
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampSymbolComparison() throws Exception {
        assertQuery(
                "min\tmax\n\t\n",
                "select min(nts), max(nts) from tt where nts = cast('2020-01-01' as symbol)",
                "create table tt (dts timestamp, nts timestamp) timestamp(dts)",
                null,
                "insert into tt " +
                        "select timestamp_sequence(1577836800000000L, 60*60*1000000L), timestamp_sequence(1577836800000000L, 60*60*1000000L) " +
                        "from long_sequence(48L)",
                "min\tmax\n" +
                        "2020-01-01T00:00:00.000000Z\t2020-01-01T00:00:00.000000Z\n",
                false,
                true,
                false
        );
    }

    @Test
    public void testTimestampSymbolComparisonBetweenInvalidValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tt (dts timestamp, nts timestamp) timestamp(dts)");
            // insert same values to dts (designated) as nts (non-designated) timestamp
            execute("insert into tt " +
                    "select timestamp_sequence(1577836800000000L, 60*60*1000000L), timestamp_sequence(1577836800000000L, 60*60*1000000L) " +
                    "from long_sequence(48L)");

            assertTimestampTtFailedQuery("select min(nts), max(nts) from tt where nts between cast('invalid' as symbol) and cast('2020-01-01' as symbol)", 52, "Invalid date");
            assertTimestampTtFailedQuery("select min(nts), max(nts) from tt where nts between cast('2020-01-01' as symbol) and cast('invalid' as symbol)", 85, "Invalid date");
            assertTimestampTtFailedQuery("select min(nts), max(nts) from tt where nts between cast('2020-01-01' as symbol) and cast('invalid' as symbol) || cast('dd' as symbol)", 111, "Invalid date");
            assertTimestampTtFailedQuery("select min(nts), max(nts) from tt where invalidCol not between cast('2020-01-01' as symbol) and cast('2020-01-02' as symbol)", 40, "Invalid column: invalidCol");
            assertTimestampTtFailedQuery("select min(nts), max(nts) from tt where nts in (cast('2020-01-01' as symbol), cast('invalid' as symbol))", 78, "Invalid date");
            assertTimestampTtFailedQuery("select min(nts), max(nts) from tt where nts in (select nts from tt)", 48, "cannot compare TIMESTAMP with type CURSOR");
        });
    }

    @Test
    public void testTimestampSymbolComparisonInvalidValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tt (dts timestamp, nts timestamp) timestamp(dts)");
            // insert same values to dts (designated) as nts (non-designated) timestamp
            execute("insert into tt " +
                    "select timestamp_sequence(1577836800000000L, 60*60*1000000L), timestamp_sequence(1577836800000000L, 60*60*1000000L) " +
                    "from long_sequence(48L)");

            assertTimestampTtFailedQuery("select min(nts), max(nts) from tt where nts > cast('invalid' as symbol)", 46, "Invalid date");
            assertTimestampTtFailedQuery("select min(nts), max(nts) from tt where cast('2020-01-01' as symbol) in (3.14)", 73, "STRING constant expected");
        });
    }

    @Test
    public void testTimestampSymbolConversion() throws Exception {
        assertMemoryLeak(() -> {
            TableModel m = new TableModel(configuration, "tt", PartitionBy.DAY)
                    .timestamp("dts")
                    .col("ts", ColumnType.TIMESTAMP);

            createPopulateTable(m, 31, "2021-03-14", 31);
            String expected = "dts\tts\n" +
                    "2021-04-02T23:59:59.354820Z\t2021-04-02T23:59:59.354820Z\n";

            assertQueryNoLeakCheck(
                    expected,
                    "tt where dts > cast('2021-04-02T13:45:49.207Z' as symbol) and dts < cast('2021-04-03 13:45:49.207' as symbol)",
                    "dts",
                    true,
                    false
            );

            assertQueryNoLeakCheck(
                    expected,
                    "tt where ts > cast('2021-04-02T13:45:49.207Z' as symbol) and ts < cast('2021-04-03 13:45:49.207' as symbol)",
                    "dts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testTimestampSymbolDateAdd() throws Exception {
        assertQuery(
                "dateadd\n" +
                        "2020-01-02T00:00:00.000000Z\n",
                "select dateadd('d', 1, cast('2020-01-01' as symbol))",
                null,
                null,
                null,
                null,
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampWithTimezone() throws Exception {
        // with constant
        assertSql("ts\n" +
                        "2020-01-01T00:00:00.000000Z\n",
                "select '2020-01-01T00:00:00.000000Z'::timestamp with time zone ts");

        // with function
        assertSql("ts\n" +
                        "2020-01-01T01:02:03.123456Z\n",
                "select concat('2020-01-01T','01:02:03.123456Z')::timestamp with time zone ts");

        // with column
        assertMemoryLeak(() -> {
            execute("create table tt (vch varchar, ts timestamp)");
            execute("insert into tt values ('2020-01-01T00:00:00.000000Z', '2020-01-01T00:00:00.000000Z'::timestamp with time zone)");

            assertSql("ts\n" +
                            "2020-01-01T00:00:00.000000Z\n",
                    "select vch::timestamp with time zone ts from tt");
        });
    }

    private void assertQueryWithConditions(String query, String expected, String columnName) throws SqlException {
        assertSql(expected, query);

        String joining = query.indexOf("where") > 0 ? " and " : " where ";

        // Non-impacting additions to WHERE
        assertSql(expected, query + joining + columnName + " not between now() and CAST(NULL as TIMESTAMP)");
        assertSql(expected, query + joining + columnName + " between '2200-01-01' and dateadd('y', -10000, now())");
        assertSql(expected, query + joining + columnName + " > dateadd('y', -1000, now())");
        assertSql(expected, query + joining + columnName + " <= dateadd('y', 1000, now())");
        assertSql(expected, query + joining + columnName + " not in '1970-01-01'");
    }

    private void assertTimestampTtFailedQuery(String sql, int errorPos, String expectedError) throws Exception {
        assertTimestampTtFailedQuery0(sql, errorPos, expectedError);
        String dtsQuery = sql.replace("nts", "dts");
        assertTimestampTtFailedQuery0(dtsQuery, errorPos, expectedError);
    }

    private void assertTimestampTtFailedQuery0(String sql, int errorPos, String contains) throws Exception {
        assertExceptionNoLeakCheck(sql, errorPos, contains);
    }

    private void assertTimestampTtQuery(String expected, String query) throws SqlException {
        assertQueryWithConditions(query, expected, "nts");
        String dtsQuery = query.replace("nts", "dts");
        assertQueryWithConditions(dtsQuery, expected, "dts");
    }

    private int compareNowRange(String query, List<Object[]> dates, LongPredicate filter) throws SqlException {
        String queryPlan = "Interval forward scan on: xts";
        StringSink text = getPlanSink(query).getSink();
        Assert.assertTrue(text.toString(), Chars.contains(text, queryPlan));

        long expectedCount = dates.stream().filter(arr -> filter.test((long) arr[0])).count();
        String expected = "ts\n"
                + dates.stream().filter(arr -> filter.test((long) arr[0]))
                .map(arr -> arr[1] + "\n")
                .collect(Collectors.joining());
        printSqlResult(expected, query, "ts", true, false);
        return (int) expectedCount;
    }
}
