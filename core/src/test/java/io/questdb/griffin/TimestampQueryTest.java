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

import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.Rnd;
import org.junit.Before;
import org.junit.Test;

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
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
            // test where ts ='2021-01'
            expected = "symbol\tme_seq_num\ttimestamp\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp ='2021-01'";
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
            // test where ts ='2020-11'
            expected = "symbol\tme_seq_num\ttimestamp\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp ='2020-11'";
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
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
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
            // test where ts ='2020-12'
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp ='2020-12'";
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
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
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
            // test where ts ='2021'
            expected = "symbol\tme_seq_num\ttimestamp\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp ='2021'";
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
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
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
            // test where ts ='2020'
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp ='2020'";
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
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
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp = '2020-12-31'";
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
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
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp = '2020-12-31T23'";
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
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
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp = '2020-12-31T23:59'";
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
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
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp = '2020-12-31T23:59:59'";
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
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
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000001Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp = '2020-12-31T23:59:59.000001Z'";
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
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
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp >= '2020'";
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
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
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp > '2019'";
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
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
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp <= '2019'";
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
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
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n";
            query = "SELECT * FROM ob_mem_snapshot where '2021' <=  timestamp";
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
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
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp <= '2020'";
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
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
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where '2020' <=  timestamp";
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
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
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp <'2020'";
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
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
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n";
            query = "SELECT * FROM ob_mem_snapshot where '2021' <  timestamp";
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
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
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp <'2021'";
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
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
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where '2019' <  timestamp";
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
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
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp >= '2021'";
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
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
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n";
            query = "SELECT * FROM ob_mem_snapshot where '2019' >=  timestamp";
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
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
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where '2020' >=  timestamp";
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
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
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp > '2020'";
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
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
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n";
            query = "SELECT * FROM ob_mem_snapshot where '2020' > timestamp";
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
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
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
            // test
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where '2021' >  timestamp";
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
        });
    }

    @Test
    public void testTimestampParseWithYearMonthDayTHourMinuteSecondTimeZone() throws Exception {
        //yyyy-MM-ddTHH:mm:ssz
        assertMemoryLeak(() -> {
            //create table
            String createStmt = "create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY";
            compiler.compile(createStmt, sqlExecutionContext);
            //insert
            executeInsert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp ='2020-12-31T23:59:59Z'";
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
        });
    }

    @Test
    public void testTimestampParseWithYearMonthDayTHourMinuteSecondAndIncompleteMillisTimeZone() throws Exception {
        //yyyy-MM-ddTHH:mm:ssz
        assertMemoryLeak(() -> {
            //create table
            String createStmt = "create table ob_mem_snapshot (symbol int,  me_seq_num long,  timestamp timestamp) timestamp(timestamp) partition by DAY";
            compiler.compile(createStmt, sqlExecutionContext);
            //insert
            executeInsert("INSERT INTO ob_mem_snapshot  VALUES(1, 1, 1609459199000000)");
            String expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            String query = "select * from ob_mem_snapshot";
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
            //2 millisec characters
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp = '2020-12-31T23:59:59.00Z'";
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
            //1 millisec character
            expected = "symbol\tme_seq_num\ttimestamp\n" +
                    "1\t1\t2020-12-31T23:59:59.000000Z\n";
            query = "SELECT * FROM ob_mem_snapshot where timestamp = '2020-12-31T23:59:59.0Z'";
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
            printSqlResult(expected, query, "timestamp", null, null, true, true, true);
        });
    }

    @Test
    public void testNowIsSameForAllQueryParts() throws Exception {
        //yyyy-MM-ddTHH:mm:ssz
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
                printSqlResult(expected, query1, "timestamp", null, null, true, true, true);

                expected = "symbol\tme_seq_num\ttimestamp\n" +
                        "1\t1\t2020-12-31T23:59:59.000000Z\n";
                String query = "select * from ob_mem_snapshot where timestamp > now()";
                printSqlResult(expected, query, "timestamp", null, null, true, true, false);
            });
        } finally {
            currentMicros = -1;
        }
    }

//    @Test
//    public void testNowPerformsBinarySearchOnTimestamp() throws Exception {
//        //yyyy-MM-ddTHH:mm:ssz
//        try {
//            currentMicros = 0;
//            assertMemoryLeak(() -> {
//                int hoursCount = 2000;
//
//                //create table
//                // One hour step timestamps from epoch for 2000 steps
//                String createStmt = "create table x as (select x * 3600 * 1000 * 1000 timestamp from long_sequence(2000)) timestamp(timestamp) partition by DAY";
//                compiler.compile(createStmt, sqlExecutionContext);
//
//                String query1 = "select now() as now1, now() as now2, symbol, timestamp FROM ob_mem_snapshot WHERE now() = now()";
//                printSqlResult(expected, query1, "timestamp", null, null, true, true, true);
//
//                expected = "symbol\tme_seq_num\ttimestamp\n" +
//                        "1\t1\t2020-12-31T23:59:59.000000Z\n";
//                String query = "select * from ob_mem_snapshot where timestamp > now()";
//                printSqlResult(expected, query, "timestamp", null, null, true, true, false);
//            });
//        } finally {
//            currentMicros = -1;
//        }
//    }

}
