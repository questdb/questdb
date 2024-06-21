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
import io.questdb.std.datetime.microtime.Timestamps;
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
            //create table
            String createStmt = "create table xyz(time timestamp, cast2 geohash(8c)) timestamp(time) partition by DAY;";
            ddl(createStmt);
            //insert
            insert("INSERT INTO xyz VALUES(1609459199000000, #u33d8b12)");
            String expected = "touch\n{\"data_pages\": 2, \"index_key_pages\":0, \"index_values_pages\": 0}\n";
            String query = "select touch(select time, cast2 from xyz);";
            assertSql(expected, query);
        });
    }


    @Test
    public void testCastAsValidColumnNameSelectTest() throws Exception {
        assertMemoryLeak(() -> {
            //create table
            String createStmt = "create table xyz(time timestamp, \"cast\" geohash(8c)) timestamp(time) partition by DAY;";
            ddl(createStmt);
            //insert
            insert("INSERT INTO xyz VALUES(1609459199000000, #u33d8b12)");
            String expected = "time\tcast\n" +
                    "2020-12-31T23:59:59.000000Z\tu33d8b12\n";
            String query = "select time, \"cast\" from xyz;";
            assertSql(expected, query);
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
            //create table
            String createStmt = "create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY";
            ddl(createStmt);
            //insert
            insert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
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
            //create table
            String createStmt = "create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY";
            ddl(createStmt);
            //insert
            insert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
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
            //create table
            String createStmt = "create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY";
            ddl(createStmt);
            //insert
            insert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
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
            //create table
            String createStmt = "create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY";
            ddl(createStmt);
            //insert
            insert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test where ts ='2020'
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp IN '2020'";
            printSqlResult(expected, query, "timestamp", true, true);
        });
    }

    @Test
    public void testEqualsToTimestampFormatYearMonthDay() throws Exception {
        assertMemoryLeak(() -> {
            //create table
            String createStmt = "create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY";
            ddl(createStmt);
            //insert
            insert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp IN '2020-12-31'";
            printSqlResult(expected, query, "timestamp", true, true);
        });
    }

    @Test
    public void testEqualsToTimestampFormatYearMonthDayHour() throws Exception {
        assertMemoryLeak(() -> {
            //create table
            String createStmt = "create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY";
            ddl(createStmt);
            //insert
            insert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp IN '2020-12-31T23'";
            printSqlResult(expected, query, "timestamp", true, true);
        });
    }

    @Test
    public void testEqualsToTimestampFormatYearMonthDayHourMinute() throws Exception {
        assertMemoryLeak(() -> {
            //create table
            String createStmt = "create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY";
            ddl(createStmt);
            //insert
            insert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp IN '2020-12-31T23:59'";
            printSqlResult(expected, query, "timestamp", true, true);
        });
    }

    @Test
    public void testEqualsToTimestampFormatYearMonthDayHourMinuteSecond() throws Exception {
        assertMemoryLeak(() -> {
            //create table
            String createStmt = "create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY";
            ddl(createStmt);
            //insert
            insert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp = '2020-12-31T23:59:59'";
            printSqlResult(expected, query, "timestamp", true, true);
        });
    }

    @Test
    public void testEqualsToTimestampWithMicrosecond() throws Exception {
        assertMemoryLeak(() -> {
            //create table
            String createStmt = "create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY";
            ddl(createStmt);
            //insert
            insert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000001)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000001Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000001Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp = '2020-12-31T23:59:59.000001Z'";
            printSqlResult(expected, query, "timestamp", true, true);
        });
    }

    @Test
    public void testInsertAsSelectTimestampVarcharCast() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x (l long, t timestamp) timestamp(t) partition by DAY");
            insert("insert into x select 1, '2024-02-27T00:00:00'::varchar");
            assertSql("l\tt\n1\t2024-02-27T00:00:00.000000Z\n", "select * from x");
        });
    }

    @Test
    public void testLMoreThanOrEqualsToTimestampFormatYearOnlyPositiveTest1() throws Exception {
        assertMemoryLeak(() -> {
            //create table
            String createStmt = "create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY";
            ddl(createStmt);
            //insert
            insert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp >= '2020'";
            printSqlResult(expected, query, "timestamp", true, true);
        });
    }

    @Test
    public void testLMoreThanTimestampFormatYearOnlyPositiveTest1() throws Exception {
        assertMemoryLeak(() -> {
            //create table
            String createStmt = "create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY";
            ddl(createStmt);
            //insert
            insert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp > '2019'";
            printSqlResult(expected, query, "timestamp", true, true);
        });
    }

    @Test
    public void testLessThanOrEqualsToTimestampFormatYearOnlyNegativeTest1() throws Exception {
        assertMemoryLeak(() -> {
            //create table
            String createStmt = "create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY";
            ddl(createStmt);
            //insert
            insert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp <= '2019'";
            printSqlResult(expected, query, "timestamp", true, true);
        });
    }

    @Test
    public void testLessThanOrEqualsToTimestampFormatYearOnlyNegativeTest2() throws Exception {
        assertMemoryLeak(() -> {
            //create table
            String createStmt = "create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY";
            ddl(createStmt);
            //insert
            insert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n";
            query = "SELECT * FROM ob_mem_snapshot where '2021' <=  timestamp";
            printSqlResult(expected, query, "timestamp", true, true);
        });
    }

    @Test
    public void testLessThanOrEqualsToTimestampFormatYearOnlyPositiveTest1() throws Exception {
        assertMemoryLeak(() -> {
            //create table
            String createStmt = "create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY";
            ddl(createStmt);
            //insert
            insert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp <= '2021'";
            printSqlResult(expected, query, "timestamp", true, true);
        });
    }

    @Test
    public void testLessThanOrEqualsToTimestampFormatYearOnlyPositiveTest2() throws Exception {
        assertMemoryLeak(() -> {
            //create table
            String createStmt = "create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY";
            ddl(createStmt);
            //insert
            insert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where '2020' <=  timestamp";
            printSqlResult(expected, query, "timestamp", true, true);
        });
    }

    @Test
    public void testLessThanTimestampFormatYearOnlyNegativeTest1() throws Exception {
        assertMemoryLeak(() -> {
            //create table
            String createStmt = "create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY";
            ddl(createStmt);
            //insert
            insert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp <'2020'";
            printSqlResult(expected, query, "timestamp", true, true);
        });
    }

    @Test
    public void testLessThanTimestampFormatYearOnlyNegativeTest2() throws Exception {
        assertMemoryLeak(() -> {
            //create table
            String createStmt = "create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY";
            ddl(createStmt);
            //insert
            insert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n";
            query = "SELECT * FROM ob_mem_snapshot where '2021' <  timestamp";
            printSqlResult(expected, query, "timestamp", true, true);
        });
    }

    @Test
    public void testLessThanTimestampFormatYearOnlyPositiveTest1() throws Exception {
        assertMemoryLeak(() -> {
            //create table
            String createStmt = "create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY";
            ddl(createStmt);
            //insert
            insert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp <'2021'";
            printSqlResult(expected, query, "timestamp", true, true);
        });
    }

    @Test
    public void testLessThanTimestampFormatYearOnlyPositiveTest2() throws Exception {
        assertMemoryLeak(() -> {
            //create table
            String createStmt = "create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY";
            ddl(createStmt);
            //insert
            insert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where '2019' <  timestamp";
            printSqlResult(expected, query, "timestamp", true, true);
        });
    }

    @Test
    public void testMinOnTimestampEmptyResutlSetIsNull() throws Exception {
        assertMemoryLeak(() -> {
            // create table
            String createStmt = "create table tt (dts timestamp, nts timestamp) timestamp(dts)";
            ddl(createStmt);

            // insert same values to dts (designated) as nts (non-designated) timestamp
            insert("insert into tt " +
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
            //create table
            String createStmt = "create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY";
            ddl(createStmt);
            //insert
            insert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp >= '2021'";
            printSqlResult(expected, query, "timestamp", true, true);
        });
    }

    @Test
    public void testMoreThanOrEqualsToTimestampFormatYearOnlyNegativeTest2() throws Exception {
        assertMemoryLeak(() -> {
            //create table
            String createStmt = "create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY";
            ddl(createStmt);
            //insert
            insert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n";
            query = "SELECT * FROM ob_mem_snapshot where '2019' >=  timestamp";
            printSqlResult(expected, query, "timestamp", true, true);
        });
    }

    @Test
    public void testMoreThanOrEqualsToTimestampFormatYearOnlyPositiveTest2() throws Exception {
        assertMemoryLeak(() -> {
            //create table
            String createStmt = "create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY";
            ddl(createStmt);
            //insert
            insert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where '2021-01-01' >=  timestamp";
            printSqlResult(expected, query, "timestamp", true, true);
        });
    }

    @Test
    public void testMoreThanTimestampFormatYearOnlyNegativeTest1() throws Exception {
        assertMemoryLeak(() -> {
            //create table
            String createStmt = "create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY";
            ddl(createStmt);
            //insert
            insert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp >= '2021'";
            printSqlResult(expected, query, "timestamp", true, true);
        });
    }

    @Test
    public void testMoreThanTimestampFormatYearOnlyNegativeTest2() throws Exception {
        assertMemoryLeak(() -> {
            //create table
            String createStmt = "create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY";
            ddl(createStmt);
            //insert
            insert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n";
            query = "SELECT * FROM ob_mem_snapshot where '2020' > timestamp";
            printSqlResult(expected, query, "timestamp", true, true);
        });
    }

    @Test
    public void testMoreThanTimestampFormatYearOnlyPositiveTest2() throws Exception {
        assertMemoryLeak(() -> {
            //create table
            String createStmt = "create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY";
            ddl(createStmt);
            //insert
            insert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where '2021' >  timestamp";
            printSqlResult(expected, query, "timestamp", true, true);
        });
    }

    @Test
    public void testNonContinuousPartitions() throws Exception {
        currentMicros = 0;
        assertMemoryLeak(() -> {
            // Create table
            // One hour step timestamps from epoch for 32 then skip 48 etc for 10 iterations
            final int count = 32;
            final int skip = 48;
            final int iterations = 10;
            final long hour = Timestamps.HOUR_MICROS;

            String createStmt = "create table xts (ts Timestamp) timestamp(ts) partition by DAY";
            ddl(createStmt);
            long start = 0;
            List<Object[]> datesArr = new ArrayList<>();
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'.000000Z'");
            formatter.setTimeZone(TimeZone.getTimeZone("UTC"));

            for (int i = 0; i < iterations; i++) {
                String insert = "insert into xts " +
                        "select timestamp_sequence(" + start + "L, 3600L * 1000 * 1000) ts from long_sequence(" + count + ")";
                insert(insert);
                for (long ts = 0; ts < count; ts++) {
                    long nextTs = start + ts * hour;
                    datesArr.add(new Object[]{nextTs, formatter.format(nextTs / 1000L)});
                }
                start += (count + skip) * hour;
            }
            final long end = start;

            // Search with 3 hour window every 22 hours
            int min = Integer.MAX_VALUE;
            int max = Integer.MIN_VALUE;
            for (currentMicros = 0; currentMicros < end; currentMicros += 22 * hour) {
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
        currentMicros = 0;
        assertMemoryLeak(() -> {
            //create table
            String createStmt = "create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY";
            ddl(createStmt);
            //insert
            insert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "now1\tnow2\tsymbol\ttimestamp\n" +
                    "1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:00.000000Z\t1\t2020-12-31T23:59:59.000000Z\n";

            String query1 = "select now() as now1, now() as now2, symbol, timestamp FROM ob_mem_snapshot WHERE now() = now()";
            printSqlResult(expected, query1, "timestamp", true, false);

            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot where timestamp > now()";
            printSqlResult(expected, query, "timestamp", true, true);
        });
    }

    @Test
    public void testNowPerformsBinarySearchOnTimestamp() throws Exception {
        currentMicros = 0;
        assertMemoryLeak(() -> {
            //create table
            // One hour step timestamps from epoch for 2000 steps
            final int count = 200;
            String createStmt = "create table xts as (select timestamp_sequence(0, 3600L * 1000 * 1000) ts from long_sequence(" + count + ")) timestamp(ts) partition by DAY";
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'.000000Z'");
            ddl(createStmt);

            formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
            Stream<Object[]> dates = LongStream.rangeClosed(0, count - 1)
                    .map(i -> i * 3600L * 1000)
                    .mapToObj(ts -> new Object[]{ts * 1000L, formatter.format(new Date(ts))});

            List<Object[]> datesArr = dates.collect(Collectors.toList());

            final long hour = Timestamps.HOUR_MICROS;
            final long day = 24 * hour;
            compareNowRange("select * FROM xts WHERE ts >= '1970' and ts <= '2021'", datesArr, ts -> true);

            // Scroll now to the end
            currentMicros = 200L * hour;
            compareNowRange("select ts FROM xts WHERE ts >= now() - 3600 * 1000 * 1000L", datesArr, ts -> ts >= currentMicros - hour);
            compareNowRange("select ts FROM xts WHERE ts >= now() + 3600 * 1000 * 1000L", datesArr, ts -> ts >= currentMicros + hour);

            for (currentMicros = hour; currentMicros < count * hour; currentMicros += day) {
                compareNowRange("select ts FROM xts WHERE ts < now()", datesArr, ts -> ts < currentMicros);
            }

            for (currentMicros = hour; currentMicros < count * hour; currentMicros += 12 * hour) {
                compareNowRange("select ts FROM xts WHERE ts >= now()", datesArr, ts -> ts >= currentMicros);
            }

            for (currentMicros = 0; currentMicros < count * hour + 4 * day; currentMicros += 5 * hour) {
                compareNowRange(
                        "select ts FROM xts WHERE ts <= dateadd('d', -1, now()) and ts >= dateadd('d', -2, now())",
                        datesArr,
                        ts -> ts >= (currentMicros - 2 * day) && (ts <= currentMicros - day)
                );
            }

            currentMicros = 100L * hour;
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
                    true
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
        assertQuery("t\n" +
                "1970-01-01T00:00:01.000000Z\n" +
                "1970-01-01T00:00:02.000000Z\n", "select t from x where t != to_timestamp('1970-01-01:00:00:00', 'yyyy-MM-dd:HH:mm:ss') ", "create table x as (select timestamp_sequence(0, 1000000) t from long_sequence(3)) timestamp(t)", "t", null, null, true, true, false);
    }

    @Test
    public void testTimestampDifferentThanNonFixedValue() throws Exception {
        assertQuery("t\n" +
                "1970-01-01T00:00:00.000000Z\n" +
                "1970-01-01T00:00:01.000000Z\n", "select t from x where t != to_timestamp('201' || rnd_long(0,9,0),'yyyy')", "create table x as (select timestamp_sequence(0, 1000000) t from long_sequence(2)) timestamp(t)", "t", null, null, true, false, false);
    }

    @Test
    public void testTimestampInDay1orDay2() throws Exception {
        assertQuery("min\tmax\n\t\n", "select min(nts), max(nts) from tt where nts IN '2020-01-01' or nts IN '2020-01-02'", "create table tt (dts timestamp, nts timestamp) timestamp(dts)", null, "insert into tt " +
                "select timestamp_sequence(1577836800000000L, 60*60*1000000L), timestamp_sequence(1577836800000000L, 60*60*1000000L) " +
                "from long_sequence(48L)", "min\tmax\n" +
                "2020-01-01T00:00:00.000000Z\t2020-01-02T23:00:00.000000Z\n", false, true, false);
    }

    @Test
    public void testTimestampIntervalPartitionDay() throws Exception {
        assertMemoryLeak(() -> {
            //create table
            String createStmt = "create table interval_test(seq_num long, timestamp timestamp) timestamp(timestamp) partition by DAY";
            ddl(createStmt);
            //insert as select
            insert("insert into interval_test select x, timestamp_sequence(" +
                    "'2022-11-19T00:00:00', " +
                    Timestamps.DAY_MICROS + ") FROM long_sequence(5)");
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
            //create table
            String createStmt = "create table interval_test(seq_num long, timestamp timestamp) timestamp(timestamp) partition by MONTH";
            ddl(createStmt);
            //insert as select
            insert("insert into interval_test select x, timestamp_sequence(" +
                    "'2022-11-19T00:00:00', " +
                    Timestamps.DAY_MICROS * 30 + ") FROM long_sequence(5)");
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
            //create table
            String createStmt = "create table interval_test(seq_num long, timestamp timestamp) timestamp(timestamp) partition by WEEK";
            ddl(createStmt);
            //insert as select
            insert("insert into interval_test select x, timestamp_sequence(" +
                    "'2022-11-19T00:00:00', " +
                    Timestamps.WEEK_MICROS + ") FROM long_sequence(5)");
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
            //create table
            String createStmt = "create table interval_test(seq_num long, timestamp timestamp) timestamp(timestamp) partition by YEAR";
            ddl(createStmt);
            //insert as select
            insert("insert into interval_test select x, timestamp_sequence(" +
                    "'2022-11-19T00:00:00', " +
                    Timestamps.DAY_MICROS * 365 + ") FROM long_sequence(5)");
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
        assertQuery("nts\tmin\n" +
                "nts\t\n", "select 'nts', min(nts) from tt where nts > '2020-01-01T00:00:00.000000Z'", "create table tt (dts timestamp, nts timestamp) timestamp(dts)", null, "insert into tt " +
                "select timestamp_sequence(1577836800000000L, 10L), timestamp_sequence(1577836800000000L, 10L) " +
                "from long_sequence(2L)", "nts\tmin\n" +
                "nts\t2020-01-01T00:00:00.000010Z\n", false, true, false);
    }

    @Test
    public void testTimestampOpSymbolColumns() throws Exception {
        assertQuery("a\tk\n" +
                "1970-01-01T00:00:00.040000Z\t1970-01-01T00:00:00.030000Z\n" +
                "1970-01-01T00:00:00.050000Z\t1970-01-01T00:00:00.040000Z\n", "select a, k from x where k < cast(a as timestamp)", "create table x as (select cast(concat('1970-01-01T00:00:00.0', (case when x > 3 then x else x - 1 end), '0000Z') as symbol) a, timestamp_sequence(0, 10000) k from long_sequence(5)) timestamp(k)", "k", null, null, true, false, false);
    }

    @Test
    public void testTimestampParseWithYearMonthDayTHourMinuteSecondAndIncompleteMillisTimeZone() throws Exception {
        assertMemoryLeak(() -> {
            //create table
            String createStmt = "create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY";
            ddl(createStmt);
            //insert
            insert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            //2 millisec characters
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp IN '2020-12-31T23:59:59.00Z'";
            printSqlResult(expected, query, "timestamp", true, true);
            //1 millisec character
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp IN '2020-12-31T23:59:59.0Z'";
            printSqlResult(expected, query, "timestamp", true, true);
            printSqlResult(expected, query, "timestamp", true, true);
        });
    }

    @Test
    public void testTimestampParseWithYearMonthDayTHourMinuteSecondTimeZone() throws Exception {
        assertMemoryLeak(() -> {
            //create table
            String createStmt = "create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY";
            ddl(createStmt);
            //insert
            insert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", true, true);
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp ='2020-12-31T23:59:59Z'";
            printSqlResult(expected, query, "timestamp", true, true);
        });
    }

    @Test
    public void testTimestampStringComparison() throws Exception {
        assertQuery("min\tmax\n\t\n", "select min(nts), max(nts) from tt where nts = '2020-01-01'", "create table tt (dts timestamp, nts timestamp) timestamp(dts)", null, "insert into tt " +
                "select timestamp_sequence(1577836800000000L, 60*60*1000000L), timestamp_sequence(1577836800000000L, 60*60*1000000L) " +
                "from long_sequence(48L)", "min\tmax\n" +
                "2020-01-01T00:00:00.000000Z\t2020-01-01T00:00:00.000000Z\n", false, true, false);
    }

    @Test
    public void testTimestampStringComparisonBetween() throws Exception {
        assertMemoryLeak(() -> {
            // create table
            String createStmt = "create table tt (dts timestamp, nts timestamp) timestamp(dts)";
            ddl(createStmt);

            // insert same values to dts (designated) as nts (non-designated) timestamp
            insert("insert into tt " +
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
            // create table
            String createStmt = "create table tt (dts timestamp, nts timestamp) timestamp(dts)";
            ddl(createStmt);

            // insert same values to dts (designated) as nts (non-designated) timestamp
            insert("insert into tt " +
                    "select timestamp_sequence(1577836800000000L, 60*60*1000000L), timestamp_sequence(1577836800000000L, 60*60*1000000L) " +
                    "from long_sequence(48L)");

            assertTimestampTtFailedQuery("Invalid date", "select min(nts), max(nts) from tt where nts between 'invalid' and '2020-01-01'");
            assertTimestampTtFailedQuery("Invalid date", "select min(nts), max(nts) from tt where nts between '2020-01-01' and 'invalid'");
            assertTimestampTtFailedQuery("Invalid date", "select min(nts), max(nts) from tt where nts between '2020-01-01' and 'invalid' || 'dd'");
            assertTimestampTtFailedQuery("Invalid column: invalidCol", "select min(nts), max(nts) from tt where invalidCol not between '2020-01-01' and '2020-01-02'");
            assertTimestampTtFailedQuery("Invalid date", "select min(nts), max(nts) from tt where nts in ('2020-01-01', 'invalid')");
            assertTimestampTtFailedQuery("cannot compare TIMESTAMP with type CURSOR", "select min(nts), max(nts) from tt where nts in (select nts from tt)");
        });
    }

    @Test
    public void testTimestampStringComparisonInString() throws Exception {
        assertMemoryLeak(() -> {
            // create table
            String createStmt = "create table tt (dts timestamp, nts timestamp) timestamp(dts)";
            ddl(createStmt);

            // insert same values to dts (designated) as nts (non-designated) timestamp
            insert("insert into tt " +
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

            expected = "dts\tnts\n";
            assertTimestampTtQuery(expected, "select * from tt where nts in (now() || 'invalid')");

            expected = "min\tmax\n" +
                    "2020-01-01T00:00:00.000000Z\t2020-01-02T23:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where  nts not in (now() || 'invalid')");
        });
    }

    @Test
    public void testTimestampStringComparisonInVarchar() throws Exception {
        assertMemoryLeak(() -> {
            // create table
            String createStmt = "create table tt (dts timestamp, nts timestamp) timestamp(dts)";
            ddl(createStmt);

            // insert same values to dts (designated) as nts (non-designated) timestamp
            insert("insert into tt " +
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

            expected = "dts\tnts\n";
            assertTimestampTtQuery(expected, "select * from tt where nts in (now() || 'invalid'::varchar)");

            expected = "min\tmax\n" +
                    "2020-01-01T00:00:00.000000Z\t2020-01-02T23:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where  nts not in (now() || 'invalid'::varchar)");
        });
    }

    @Test
    public void testTimestampStringComparisonInvalidValue() throws Exception {
        assertMemoryLeak(() -> {
            // create table
            String createStmt = "create table tt (dts timestamp, nts timestamp) timestamp(dts)";
            ddl(createStmt);

            // insert same values to dts (designated) as nts (non-designated) timestamp
            insert("insert into tt " +
                    "select timestamp_sequence(1577836800000000L, 60*60*1000000L), timestamp_sequence(1577836800000000L, 60*60*1000000L) " +
                    "from long_sequence(48L)");

            assertTimestampTtFailedQuery("Invalid date", "select min(nts), max(nts) from tt where nts > 'invalid'");
            assertTimestampTtFailedQuery("cannot compare STRING with type DOUBLE", "select min(nts), max(nts) from tt where '2020-01-01' in ( NaN)");
        });
    }

    @Test
    public void testTimestampStringComparisonNonConst() throws Exception {
        assertMemoryLeak(() -> {
            // create table
            String createStmt = "create table tt (dts timestamp, nts timestamp) timestamp(dts)";
            ddl(createStmt);

            // insert same values to dts (designated) as nts (non-designated) timestamp
            insert("insert into tt " +
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
            // create table
            String createStmt = "create table tt (dts timestamp, nts timestamp) timestamp(dts)";
            ddl(createStmt);

            // insert same values to dts (designated) as nts (non-designated) timestamp
            insert("insert into tt " +
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
        assertQuery("dateadd\n" +
                "2020-01-02T00:00:00.000000Z\n", "select dateadd('d', 1, '2020-01-01')", null, null, null, null, true, true, false);
    }

    @Test
    public void testTimestampSymbolComparison() throws Exception {
        assertQuery("min\tmax\n\t\n", "select min(nts), max(nts) from tt where nts = cast('2020-01-01' as symbol)", "create table tt (dts timestamp, nts timestamp) timestamp(dts)", null, "insert into tt " +
                "select timestamp_sequence(1577836800000000L, 60*60*1000000L), timestamp_sequence(1577836800000000L, 60*60*1000000L) " +
                "from long_sequence(48L)", "min\tmax\n" +
                "2020-01-01T00:00:00.000000Z\t2020-01-01T00:00:00.000000Z\n", false, true, false);
    }

    @Test
    public void testTimestampSymbolComparisonBetweenInvalidValue() throws Exception {
        assertMemoryLeak(() -> {
            // create table
            String createStmt = "create table tt (dts timestamp, nts timestamp) timestamp(dts)";
            ddl(createStmt);

            // insert same values to dts (designated) as nts (non-designated) timestamp
            insert("insert into tt " +
                    "select timestamp_sequence(1577836800000000L, 60*60*1000000L), timestamp_sequence(1577836800000000L, 60*60*1000000L) " +
                    "from long_sequence(48L)");

            assertTimestampTtFailedQuery("Invalid date", "select min(nts), max(nts) from tt where nts between cast('invalid' as symbol) and cast('2020-01-01' as symbol)");
            assertTimestampTtFailedQuery("Invalid date", "select min(nts), max(nts) from tt where nts between cast('2020-01-01' as symbol) and cast('invalid' as symbol)");
            assertTimestampTtFailedQuery("Invalid date", "select min(nts), max(nts) from tt where nts between cast('2020-01-01' as symbol) and cast('invalid' as symbol) || cast('dd' as symbol)");
            assertTimestampTtFailedQuery("Invalid column: invalidCol", "select min(nts), max(nts) from tt where invalidCol not between cast('2020-01-01' as symbol) and cast('2020-01-02' as symbol)");
            assertTimestampTtFailedQuery("Invalid date", "select min(nts), max(nts) from tt where nts in (cast('2020-01-01' as symbol), cast('invalid' as symbol))");
            assertTimestampTtFailedQuery("cannot compare TIMESTAMP with type CURSOR", "select min(nts), max(nts) from tt where nts in (select nts from tt)");
        });
    }

    @Test
    public void testTimestampSymbolComparisonInvalidValue() throws Exception {
        assertMemoryLeak(() -> {
            // create table
            String createStmt = "create table tt (dts timestamp, nts timestamp) timestamp(dts)";
            ddl(createStmt);

            // insert same values to dts (designated) as nts (non-designated) timestamp
            insert("insert into tt " +
                    "select timestamp_sequence(1577836800000000L, 60*60*1000000L), timestamp_sequence(1577836800000000L, 60*60*1000000L) " +
                    "from long_sequence(48L)");

            assertTimestampTtFailedQuery("Invalid date", "select min(nts), max(nts) from tt where nts > cast('invalid' as symbol)");
            assertTimestampTtFailedQuery("STRING constant expected", "select min(nts), max(nts) from tt where cast('2020-01-01' as symbol) in (NaN)");
        });
    }

    @Test
    public void testTimestampSymbolConversion() throws Exception {
        assertMemoryLeak(() -> {
            TableModel m = new TableModel(configuration, "tt", PartitionBy.DAY);
            m.timestamp("dts")
                    .col("ts", ColumnType.TIMESTAMP);
            createPopulateTable(m, 31, "2021-03-14", 31);
            String expected = "dts\tts\n" +
                    "2021-04-02T23:59:59.354820Z\t2021-04-02T23:59:59.354820Z\n";

            assertQuery(
                    expected,
                    "tt where dts > cast('2021-04-02T13:45:49.207Z' as symbol) and dts < cast('2021-04-03 13:45:49.207' as symbol)",
                    "dts",
                    true,
                    true
            );

            assertQuery(
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
        assertQuery("dateadd\n" +
                "2020-01-02T00:00:00.000000Z\n", "select dateadd('d', 1, cast('2020-01-01' as symbol))", null, null, null, null, true, true, false);
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

    private void assertTimestampTtFailedQuery(String expectedError, String sql) throws Exception {
        assertTimestampTtFailedQuery0(sql, expectedError);
        String dtsQuery = sql.replace("nts", "dts");
        assertTimestampTtFailedQuery0(dtsQuery, expectedError);
    }

    private void assertTimestampTtFailedQuery0(String sql, String contains) throws Exception {
        assertException(sql, -1, contains);
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
        printSqlResult(expected, query, "ts", true, true);
        return (int) expectedCount;
    }
}
