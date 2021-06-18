/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableModel;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
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

public class TimestampQueryTest extends AbstractGriffinTest {

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testEqualityTimestampFormatYearAndMonthNegativeTest() throws Exception {
        assertMemoryLeak(() -> {
            //create table
            String createStmt = "create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY";
            compiler.compile(createStmt, sqlExecutionContext);
            //insert
            executeInsert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            assertSql(query, expected);
            // test where ts ='2021-01'
            expected = "symbol\tme_seq_num\ttimestamp\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp ='2021-01'";
            assertSql(query, expected);
            // test where ts ='2020-11'
            expected = "symbol\tme_seq_num\ttimestamp\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp ='2020-11'";
            assertSql(query, expected);
        });
    }

    @Test
    public void testEqualityTimestampFormatYearAndMonthPositiveTest() throws Exception {
        assertMemoryLeak(() -> {
            //create table
            String createStmt = "create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY";
            compiler.compile(createStmt, sqlExecutionContext);
            //insert
            executeInsert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            assertSql(query, expected);
            // test where ts ='2020-12'
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp IN '2020-12'";
            assertSql(query, expected);
        });
    }

    @Test
    public void testEqualityTimestampFormatYearOnlyNegativeTest() throws Exception {
        assertMemoryLeak(() -> {
            //create table
            String createStmt = "create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY";
            compiler.compile(createStmt, sqlExecutionContext);
            //insert
            executeInsert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            assertSql(query, expected);
            // test where ts ='2021'
            expected = "symbol\tme_seq_num\ttimestamp\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp ='2021'";
            assertSql(query, expected);
        });
    }

    @Test
    public void testEqualityTimestampFormatYearOnlyPositiveTest() throws Exception {
        assertMemoryLeak(() -> {
            //create table
            String createStmt = "create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY";
            compiler.compile(createStmt, sqlExecutionContext);
            //insert
            executeInsert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
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
            compiler.compile(createStmt, sqlExecutionContext);
            //insert
            executeInsert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
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
            compiler.compile(createStmt, sqlExecutionContext);
            //insert
            executeInsert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
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
            compiler.compile(createStmt, sqlExecutionContext);
            //insert
            executeInsert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
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
            compiler.compile(createStmt, sqlExecutionContext);
            //insert
            executeInsert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
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
            compiler.compile(createStmt, sqlExecutionContext);
            //insert
            executeInsert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000001)");
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
    public void testLMoreThanOrEqualsToTimestampFormatYearOnlyPositiveTest1() throws Exception {
        assertMemoryLeak(() -> {
            //create table
            String createStmt = "create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY";
            compiler.compile(createStmt, sqlExecutionContext);
            //insert
            executeInsert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
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
            compiler.compile(createStmt, sqlExecutionContext);
            //insert
            executeInsert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
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
            compiler.compile(createStmt, sqlExecutionContext);
            //insert
            executeInsert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
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
            compiler.compile(createStmt, sqlExecutionContext);
            //insert
            executeInsert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
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
            compiler.compile(createStmt, sqlExecutionContext);
            //insert
            executeInsert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
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
            compiler.compile(createStmt, sqlExecutionContext);
            //insert
            executeInsert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
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
            compiler.compile(createStmt, sqlExecutionContext);
            //insert
            executeInsert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
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
            compiler.compile(createStmt, sqlExecutionContext);
            //insert
            executeInsert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
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
            compiler.compile(createStmt, sqlExecutionContext);
            //insert
            executeInsert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
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
            compiler.compile(createStmt, sqlExecutionContext);
            //insert
            executeInsert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
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
    public void testMoreThanOrEqualsToTimestampFormatYearOnlyNegativeTest1() throws Exception {
        assertMemoryLeak(() -> {
            //create table
            String createStmt = "create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY";
            compiler.compile(createStmt, sqlExecutionContext);
            //insert
            executeInsert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
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
            compiler.compile(createStmt, sqlExecutionContext);
            //insert
            executeInsert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
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
            compiler.compile(createStmt, sqlExecutionContext);
            //insert
            executeInsert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
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
            compiler.compile(createStmt, sqlExecutionContext);
            //insert
            executeInsert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
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
            compiler.compile(createStmt, sqlExecutionContext);
            //insert
            executeInsert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
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
            compiler.compile(createStmt, sqlExecutionContext);
            //insert
            executeInsert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
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
    public void testNowIsSameForAllQueryParts() throws Exception {
        try {
            currentMicros = 0;
            assertMemoryLeak(() -> {
                //create table
                String createStmt = "create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY";
                compiler.compile(createStmt, sqlExecutionContext);
                //insert
                executeInsert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
                String expected = "now1\tnow2\tsymbol\ttimestamp\n" +
                        "1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:00.000000Z\t1\t2020-12-31T23:59:59.000000Z\n";

                String query1 = "select now() as now1, now() as now2, symbol, timestamp FROM ob_mem_snapshot WHERE now() = now()";
                printSqlResult(expected, query1, "timestamp", true, false);

                expected = "symbol\tme_seq_num\ttimestamp\n" +
                        "1\t1\t2020-12-31T23:59:59.000000Z\n";
                String query = "select * from ob_mem_snapshot where timestamp > now()";
                printSqlResult(expected, query, "timestamp", true, true);
            });
        } finally {
            currentMicros = -1;
        }
    }

    @Test
    public void testNowPerformsBinarySearchOnTimestamp() throws Exception {
        try {
            currentMicros = 0;
            assertMemoryLeak(() -> {
                //create table
                // One hour step timestamps from epoch for 2000 steps
                final int count = 200;
                String createStmt = "create table xts as (select timestamp_sequence(0, 3600L * 1000 * 1000) ts from long_sequence(" + count + ")) timestamp(ts) partition by DAY";
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'.000000Z'");
                compiler.compile(createStmt, sqlExecutionContext);

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
                    compareNowRange("select ts FROM xts WHERE ts <= dateadd('d', -1, now()) and ts >= dateadd('d', -2, now())",
                            datesArr,
                            ts -> ts >= (currentMicros - 2 * day) && (ts <= currentMicros - day));
                }

                currentMicros = 100L * hour;
                compareNowRange("WITH temp AS (SELECT ts FROM xts WHERE ts > dateadd('y', -1, now())) " +
                        "SELECT ts FROM temp WHERE ts < now()", datesArr, ts -> ts < currentMicros);
            });
        } finally {
            currentMicros = -1;
        }
    }

    @Test
    public void testNonContinuousPartitions() throws Exception {
        try {
            currentMicros = 0;
            assertMemoryLeak(() -> {
                // Create table
                // One hour step timestamps from epoch for 32 then skip 48 etc for 10 iterations
                final int count = 32;
                final int skip = 48;
                final int iterations = 10;
                final long hour = Timestamps.HOUR_MICROS;

                String createStmt = "create table xts (ts Timestamp) timestamp(ts) partition by DAY";
                compiler.compile(createStmt, sqlExecutionContext);
                long start = 0;
                List<Object[]> datesArr = new ArrayList<>();
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'.000000Z'");
                formatter.setTimeZone(TimeZone.getTimeZone("UTC"));

                for (int i = 0; i < iterations; i++) {
                    String insert = "insert into xts " +
                            "select timestamp_sequence(" + start + "L, 3600L * 1000 * 1000) ts from long_sequence(" + count + ")";
                    compiler.compile(insert, sqlExecutionContext);
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
                    int results = compareNowRange("select ts FROM xts WHERE ts <= dateadd('h', 2, now()) and ts >= dateadd('h', -1, now())",
                            datesArr,
                            ts -> ts >= (currentMicros - hour) && (ts <= currentMicros + 2 * hour));
                    min = Math.min(min, results);
                    max = Math.max(max, results);
                }

                Assert.assertEquals(0, min);
                Assert.assertEquals(4, max);
            });
        } finally {
            currentMicros = -1;
        }
    }

    @Test
    public void testTimestampParseWithYearMonthDayTHourMinuteSecondAndIncompleteMillisTimeZone() throws Exception {
        assertMemoryLeak(() -> {
            //create table
            String createStmt = "create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY";
            compiler.compile(createStmt, sqlExecutionContext);
            //insert
            executeInsert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
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
            compiler.compile(createStmt, sqlExecutionContext);
            //insert
            executeInsert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
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
    public void testTimestampConversion() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel m = new TableModel(configuration, "tt", PartitionBy.DAY)) {
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
                        true);

                assertQuery(
                        expected,
                        "tt where ts > '2021-04-02T13:45:49.207Z' and ts < '2021-04-03 13:45:49.207'",
                        "dts",
                        true,
                        false);
            }
        });
    }

    @Test
    public void testTimestampSymbolConversion() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel m = new TableModel(configuration, "tt", PartitionBy.DAY)) {
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
                        true);

                assertQuery(
                        expected,
                        "tt where ts > cast('2021-04-02T13:45:49.207Z' as symbol) and ts < cast('2021-04-03 13:45:49.207' as symbol)",
                        "dts",
                        true,
                        false);
            }
        });
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
                false,
                true
        );
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
                false,
                true
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
                false,
                true
        );
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
                false,
                true
        );
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
                false,
                true
        );
    }

    @Test
    public void testTimestampStringComparisonWithString() throws Exception {
        assertMemoryLeak(() -> {
            // create table
            String createStmt = "create table tt (dts timestamp, nts timestamp) timestamp(dts)";
            compiler.compile(createStmt, sqlExecutionContext);

            // insert same values to dts (designated) as nts (non-designated) timestamp
            compiler.compile("insert into tt " +
                    "select timestamp_sequence(1577836800000000L, 60*60*1000000L), timestamp_sequence(1577836800000000L, 60*60*1000000L) " +
                    "from long_sequence(48L)", sqlExecutionContext);

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
    public void testTimestampStringComparisonInString() throws Exception {
        assertMemoryLeak(() -> {
            // create table
            String createStmt = "create table tt (dts timestamp, nts timestamp) timestamp(dts)";
            compiler.compile(createStmt, sqlExecutionContext);

            // insert same values to dts (designated) as nts (non-designated) timestamp
            compiler.compile("insert into tt " +
                    "select timestamp_sequence(1577836800000000L, 60*60*1000000L), timestamp_sequence(1577836800000000L, 60*60*1000000L) " +
                    "from long_sequence(48L)", sqlExecutionContext);

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
    public void testTimestampStringComparisonBetween() throws Exception {
        assertMemoryLeak(() -> {
            // create table
            String createStmt = "create table tt (dts timestamp, nts timestamp) timestamp(dts)";
            compiler.compile(createStmt, sqlExecutionContext);

            // insert same values to dts (designated) as nts (non-designated) timestamp
            compiler.compile("insert into tt " +
                    "select timestamp_sequence(1577836800000000L, 60*60*1000000L), timestamp_sequence(1577836800000000L, 60*60*1000000L) " +
                    "from long_sequence(48L)", sqlExecutionContext);

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

            // Between with NULL
            expected = "dts\tnts\n";
            assertTimestampTtQuery(expected, "select * from tt where nts between CAST(NULL as TIMESTAMP) and '2020-01-01'");

            // NOT Between with NULL
            expected = "min\tmax\n" +
                    "2020-01-01T00:00:00.000000Z\t2020-01-02T23:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts not between CAST(NULL as TIMESTAMP) and '2020-01-01'");

            // Between with NULL and now()
            expected = "dts\tnts\n";
            assertTimestampTtQuery(expected, "select * from tt where nts between CAST(NULL as TIMESTAMP) and now()");

            // NOT Between with NULL and now()
            expected = "min\tmax\n" +
                    "2020-01-01T00:00:00.000000Z\t2020-01-02T23:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts not between CAST(NULL as TIMESTAMP) and now()");

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
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts not between to_str(now(), 'yyyy-MM-dd') || '-222' and now()");

            // Between columns
            expected = "min\tmax\n" +
                    "2020-01-01T00:00:00.000000Z\t2020-01-02T23:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts between nts and dts");
        });
    }

    @Test
    public void testTimestampStringComparisonNonConst() throws Exception {
        assertMemoryLeak(() -> {
            // create table
            String createStmt = "create table tt (dts timestamp, nts timestamp) timestamp(dts)";
            compiler.compile(createStmt, sqlExecutionContext);

            // insert same values to dts (designated) as nts (non-designated) timestamp
            compiler.compile("insert into tt " +
                    "select timestamp_sequence(1577836800000000L, 60*60*1000000L), timestamp_sequence(1577836800000000L, 60*60*1000000L) " +
                    "from long_sequence(48L)", sqlExecutionContext);

            String expected;
            // between constants
            expected = "min\tmax\n" +
                    "2020-01-01T00:00:00.000000Z\t2020-01-02T00:00:00.000000Z\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts) from tt where nts = to_str(nts,'yyyy-MM-dd')");
        });
    }

    @Test
    public void testTimestampStringComparisonInvalidValue() throws Exception {
        assertMemoryLeak(() -> {
            // create table
            String createStmt = "create table tt (dts timestamp, nts timestamp) timestamp(dts)";
            compiler.compile(createStmt, sqlExecutionContext);

            // insert same values to dts (designated) as nts (non-designated) timestamp
            compiler.compile("insert into tt " +
                    "select timestamp_sequence(1577836800000000L, 60*60*1000000L), timestamp_sequence(1577836800000000L, 60*60*1000000L) " +
                    "from long_sequence(48L)", sqlExecutionContext);

            assertTimestampTtFailedQuery("Invalid date", "select min(nts), max(nts) from tt where nts > 'invalid'");
            assertTimestampTtFailedQuery("cannot compare TIMESTAMP with type DOUBLE", "select min(nts), max(nts) from tt in ('2020-01-01', NaN)");
        });
    }

    @Test
    public void testTimestampSymbolComparisonInvalidValue() throws Exception {
        assertMemoryLeak(() -> {
            // create table
            String createStmt = "create table tt (dts timestamp, nts timestamp) timestamp(dts)";
            compiler.compile(createStmt, sqlExecutionContext);

            // insert same values to dts (designated) as nts (non-designated) timestamp
            compiler.compile("insert into tt " +
                    "select timestamp_sequence(1577836800000000L, 60*60*1000000L), timestamp_sequence(1577836800000000L, 60*60*1000000L) " +
                    "from long_sequence(48L)", sqlExecutionContext);

            assertTimestampTtFailedQuery("Invalid date", "select min(nts), max(nts) from tt where nts > cast('invalid' as symbol)");
            assertTimestampTtFailedQuery("cannot compare TIMESTAMP with type DOUBLE", "select min(nts), max(nts) from tt in (cast('2020-01-01' as symbol), NaN)");
        });
    }

    @Test
    public void testTimestampStringComparisonBetweenInvalidValue() throws Exception {
        assertMemoryLeak(() -> {
            // create table
            String createStmt = "create table tt (dts timestamp, nts timestamp) timestamp(dts)";
            compiler.compile(createStmt, sqlExecutionContext);

            // insert same values to dts (designated) as nts (non-designated) timestamp
            compiler.compile("insert into tt " +
                    "select timestamp_sequence(1577836800000000L, 60*60*1000000L), timestamp_sequence(1577836800000000L, 60*60*1000000L) " +
                    "from long_sequence(48L)", sqlExecutionContext);

            assertTimestampTtFailedQuery("Invalid date", "select min(nts), max(nts) from tt where nts between 'invalid' and '2020-01-01'");
            assertTimestampTtFailedQuery("Invalid date", "select min(nts), max(nts) from tt where nts between '2020-01-01' and 'invalid'");
            assertTimestampTtFailedQuery("Invalid date", "select min(nts), max(nts) from tt where nts between '2020-01-01' and 'invalid' || 'dd'");
            assertTimestampTtFailedQuery("Invalid column: invalidCol", "select min(nts), max(nts) from tt where invalidCol not between '2020-01-01' and '2020-01-02'");
            assertTimestampTtFailedQuery("Invalid date", "select min(nts), max(nts) from tt where nts in ('2020-01-01', 'invalid')");
            assertTimestampTtFailedQuery("cannot compare TIMESTAMP with type CURSOR", "select min(nts), max(nts) from tt where nts in (select nts from tt)");
        });
    }

    @Test
    public void testTimestampSymbolComparisonBetweenInvalidValue() throws Exception {
        assertMemoryLeak(() -> {
            // create table
            String createStmt = "create table tt (dts timestamp, nts timestamp) timestamp(dts)";
            compiler.compile(createStmt, sqlExecutionContext);

            // insert same values to dts (designated) as nts (non-designated) timestamp
            compiler.compile("insert into tt " +
                    "select timestamp_sequence(1577836800000000L, 60*60*1000000L), timestamp_sequence(1577836800000000L, 60*60*1000000L) " +
                    "from long_sequence(48L)", sqlExecutionContext);

            assertTimestampTtFailedQuery("Invalid date", "select min(nts), max(nts) from tt where nts between cast('invalid' as symbol) and cast('2020-01-01' as symbol)");
            assertTimestampTtFailedQuery("Invalid date", "select min(nts), max(nts) from tt where nts between cast('2020-01-01' as symbol) and cast('invalid' as symbol)");
            assertTimestampTtFailedQuery("Invalid date", "select min(nts), max(nts) from tt where nts between cast('2020-01-01' as symbol) and cast('invalid' as symbol) || cast('dd' as symbol)");
            assertTimestampTtFailedQuery("Invalid column: invalidCol", "select min(nts), max(nts) from tt where invalidCol not between cast('2020-01-01' as symbol) and cast('2020-01-02' as symbol)");
            assertTimestampTtFailedQuery("Invalid date", "select min(nts), max(nts) from tt where nts in (cast('2020-01-01' as symbol), cast('invalid' as symbol))");
            assertTimestampTtFailedQuery("cannot compare TIMESTAMP with type CURSOR", "select min(nts), max(nts) from tt where nts in (select nts from tt)");
        });
    }

    @Test
    public void testTimestampOpSymbolColumns() throws Exception {
        assertQuery(
                "a\tk\n" +
                        "1970-01-01T00:00:00.040000Z\t1970-01-01T00:00:00.030000Z\n" +
                        "1970-01-01T00:00:00.050000Z\t1970-01-01T00:00:00.040000Z\n",
                "select a, k from x where k < a",
                "create table x as (select cast(concat('1970-01-01T00:00:00.0', (case when x > 3 then x else x - 1 end), '0000Z') as symbol) a, timestamp_sequence(0, 10000) k from long_sequence(5)) timestamp(k)",
                "k",
                null,
                null,
                true,
                true,
                false
        );
    }

    @Test
    public void testDesignatedTimestampOpSymbolColumns() throws Exception {
        assertQuery(
                "a\tdk\tk\n" +
                        "1970-01-01T00:00:00.040000Z\t1970-01-01T00:00:00.030000Z\t1970-01-01T00:00:00.030000Z\n" +
                        "1970-01-01T00:00:00.050000Z\t1970-01-01T00:00:00.040000Z\t1970-01-01T00:00:00.040000Z\n",
                "select a, dk, k from x where dk < a",
                "create table x as (select cast(concat('1970-01-01T00:00:00.0', (case when x > 3 then x else x - 1 end), '0000Z') as symbol) a, timestamp_sequence(0, 10000) dk, timestamp_sequence(0, 10000) k from long_sequence(5)) timestamp(k)",
                "k",
                null,
                null,
                true,
                true,
                false
        );
    }
    private void assertTimestampTtFailedQuery(String expectedError, String query) {
        assertTimestampTtFailedQuery0(expectedError, query);
        String dtsQuery = query.replace("nts", "dts");
        assertTimestampTtFailedQuery0(expectedError, dtsQuery);
    }

    private void assertTimestampTtFailedQuery0(String expectedError, String query) {
        try {
            compiler.compile(query, sqlExecutionContext);
            Assert.fail();
        } catch (SqlException ex) {
            TestUtils.assertContains(ex.getFlyweightMessage(), expectedError);
        }
    }

    private void assertTimestampTtQuery(String expected, String query) throws SqlException {
        assertQueryWithConditions(query, expected, "nts");
        String dtsQuery = query.replace("nts", "dts");
        assertQueryWithConditions(dtsQuery, expected, "dts");
    }

    private void assertQueryWithConditions(String query, String expected, String columnName) throws SqlException {
        assertSql(query, expected);

        String joining = query.indexOf("where") > 0 ? " and " : " where ";

        // Non-impacting additions to WHERE
        assertSql(query + joining + columnName + " not between now() and CAST(NULL as TIMESTAMP)", expected);
        assertSql(query + joining + columnName + " between '2200-01-01' and dateadd('y', -10000, now())", expected);
        assertSql(query + joining + columnName + " > dateadd('y', -1000, now())", expected);
        assertSql(query + joining + columnName + " <= dateadd('y', 1000, now())", expected);
        assertSql(query + joining + columnName + " not in '1970-01-01'", expected);
    }

    @Test
    public void testMinOnTimestampEmptyResutlSetIsNull() throws Exception {
        assertMemoryLeak(() -> {
            // create table
            String createStmt = "create table tt (dts timestamp, nts timestamp) timestamp(dts)";
            compiler.compile(createStmt, sqlExecutionContext);

            // insert same values to dts (designated) as nts (non-designated) timestamp
            compiler.compile("insert into tt " +
                    "select timestamp_sequence(1577836800000000L, 60*60*1000000L), timestamp_sequence(1577836800000000L, 60*60*1000000L) " +
                    "from long_sequence(48L)", sqlExecutionContext);

            String expected = "min\tmax\tcount\n\t\t0\n";
            assertTimestampTtQuery(expected, "select min(nts), max(nts), count() from tt where nts < '2020-01-01'");
            assertTimestampTtQuery(expected, "select min(nts), max(nts), count() from tt where '2020-01-01' > nts");
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
                false,
                true
        );
    }

    private int compareNowRange(String query, List<Object[]> dates, LongPredicate filter) throws SqlException {
        String queryPlan = "{\"name\":\"DataFrameRecordCursorFactory\", \"cursorFactory\":{\"name\":\"IntervalFwdDataFrameCursorFactory\", \"table\":\"xts\"}}";
        long expectedCount = dates.stream().filter(arr -> filter.test((long) arr[0])).count();
        String expected = "ts\n"
                + dates.stream().filter(arr -> filter.test((long) arr[0]))
                .map(arr -> arr[1] + "\n")
                .collect(Collectors.joining());
        printSqlResult(expected, query, "ts", null, null, true, true, true, false, queryPlan);
        return (int) expectedCount;
    }
}
