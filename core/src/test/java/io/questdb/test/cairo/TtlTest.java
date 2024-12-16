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

package io.questdb.test.cairo;

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TtlTest extends AbstractCairoTest {

    @Test
    public void testAlterTableSetTtl() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR");
        execute("INSERT INTO tango VALUES (0), (3_600_000_000), (7_200_000_001)");
        assertQuery("ts\n" +
                        "1970-01-01T00:00:00.000000Z\n" +  // with TTL of 1 hour, this row would be evictable
                        "1970-01-01T01:00:00.000000Z\n" +
                        "1970-01-01T02:00:00.000001Z\n",
                "tango", "ts", true, true);
        execute("ALTER TABLE tango SET TTL 1H");
        execute("INSERT INTO tango VALUES (7_200_000_002)"); // insert something just to trigger commit
        assertQuery("ts\n" +
                        "1970-01-01T01:00:00.000000Z\n" +
                        "1970-01-01T02:00:00.000001Z\n" +
                        "1970-01-01T02:00:00.000002Z\n",
                "tango", "ts", true, true);
    }

    @Test
    public void testDayExactlyAtTtl() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 DAY");
        execute("INSERT INTO tango VALUES ('1970-01-01T00:00:00'), ('1970-01-01T23:00:00'), ('1970-01-02T00:59:59.999999')");
        assertQuery("ts\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-01T23:00:00.000000Z\n" +
                        "1970-01-02T00:59:59.999999Z\n",
                "tango", "ts", true, true);
    }

    @Test
    public void testDayOneMicrosBeyondTtl() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1D");
        execute("INSERT INTO tango VALUES ('1970-01-01T00:00:00'), ('1970-01-01T23:00:00'), ('1970-01-02T01:00:00')");
        assertQuery("ts\n" +
                        "1970-01-01T23:00:00.000000Z\n" +
                        "1970-01-02T01:00:00.000000Z\n",
                "tango", "ts", true, true);
    }

    @Test
    public void testHourExactlyAtTtl() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 HOUR");
        execute("INSERT INTO tango VALUES ('1970-01-01T00:00:00'), ('1970-01-01T01:00:00'), ('1970-01-01T01:59:59.999999')");
        assertQuery("ts\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-01T01:00:00.000000Z\n" +
                        "1970-01-01T01:59:59.999999Z\n",
                "tango", "ts", true, true);
    }

    @Test
    public void testHourOneMicrosBeyondTtl() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1H");
        execute("INSERT INTO tango VALUES ('1970-01-01T00:00:00'), ('1970-01-01T01:00:00'), ('1970-01-01T02:00:00')");
        assertQuery("ts\n" +
                        "1970-01-01T01:00:00.000000Z\n" +
                        "1970-01-01T02:00:00.000000Z\n",
                "tango", "ts", true, true);
    }

    @Test
    public void testMonthExactlyAtTtl() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 MONTH");
        execute("INSERT INTO tango VALUES ('1970-02-01T04:20:00.0Z'), ('1970-02-10T04:20:00.0Z'), ('1970-03-01T04:59:59.999999Z')");
        assertQuery("ts\n" +
                        "1970-02-01T04:20:00.000000Z\n" +
                        "1970-02-10T04:20:00.000000Z\n" +
                        "1970-03-01T04:59:59.999999Z\n",
                "tango", "ts", true, true);
    }

    @Test
    public void testMonthOneMicrosBeyondTtl() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1M");
        execute("INSERT INTO tango VALUES ('1970-02-01T04:20:00.0Z'), ('1970-02-10T04:20:00.0Z'), ('1970-03-01T05:00:00')");
        assertQuery("ts\n" +
                        "1970-02-10T04:20:00.000000Z\n" +
                        "1970-03-01T05:00:00.000000Z\n",
                "tango", "ts", true, true);
    }

    @Test
    public void testSyntaxInvalid() {
        try {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL");
            fail("Invalid syntax accepted");
        } catch (SqlException e) {
            assertEquals("[69] missing argument, should be TTL <number> <unit> or <number_with_unit>", e.getMessage());
        }
        try {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL X");
            fail("Invalid syntax accepted");
        } catch (SqlException e) {
            assertEquals("[70] invalid syntax, should be TTL <number> <unit> but was TTL X", e.getMessage());
        }
        try {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 12");
            fail("Invalid syntax accepted");
        } catch (SqlException e) {
            assertEquals("[72] missing unit, 'HOUR(S)', 'DAY(S)', 'WEEK(S)', 'MONTH(S)' or 'YEAR(S)' expected", e.getMessage());
        }
        try {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 NONE");
            fail("Invalid syntax accepted");
        } catch (SqlException e) {
            assertEquals("[72] invalid unit, expected 'HOUR(S)', 'DAY(S)', 'WEEK(S)', 'MONTH(S)' or 'YEAR(S)', but was 'NONE'",
                    e.getMessage());
        }
        try {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1G");
            fail("Invalid syntax accepted");
        } catch (SqlException e) {
            assertEquals("[71] invalid time unit, expecting 'H', 'D', 'W', 'M' or 'Y', but was 'G'",
                    e.getMessage());
        }
    }

    @Test
    public void testSyntaxJustWithinRange() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 2_147_483_647 HOURS");
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 89_478_485 DAYS");
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 12_782_640 WEEKS");
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 244_978 MONTHS");
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 244_978 YEARS");
    }

    @Test
    public void testSyntaxOutOfRange() {
        try {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL -1 HOURS");
            fail("Invalid syntax accepted");
        } catch (SqlException e) {
            assertEquals("[70] invalid syntax, should be TTL <number> <unit> but was TTL -",
                    e.getMessage());
        }
        try {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 2_147_483_648 HOURS");
            fail("Invalid syntax accepted");
        } catch (SqlException e) {
            assertEquals("[70] TTL value out of range: 2147483648. Max value: 2147483647",
                    e.getMessage());
        }
        try {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 89_478_486 DAYS");
            fail("Invalid syntax accepted");
        } catch (SqlException e) {
            assertEquals("[70] value out of range: 89478486 days. Max value: 89478485 days",
                    e.getMessage());
        }
        try {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 12_782_641 WEEKS");
            fail("Invalid syntax accepted");
        } catch (SqlException e) {
            assertEquals("[70] value out of range: 12782641 weeks. Max value: 12782640 weeks",
                    e.getMessage());
        }
        try {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 2_147_483_648 MONTHS");
            fail("Invalid syntax accepted");
        } catch (SqlException e) {
            assertEquals("[70] TTL value out of range: 2147483648. Max value: 2147483647",
                    e.getMessage());
        }
        try {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 178_956_971 YEARS");
            fail("Invalid syntax accepted");
        } catch (SqlException e) {
            assertEquals("[70] value out of range: 178956971 years. Max value: 178956970 years",
                    e.getMessage());
        }
    }

    @Test
    public void testSyntaxValid() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1H");
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 HOUR");
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 HOURS");
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1D");
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 DAY");
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 DAYS");
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1W");
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 WEEK");
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 WEEKS");
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1M");
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 MONTH");
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 MONTHS");
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1Y");
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 YEAR");
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 YEARS");
    }

    @Test
    public void testWeekExactlyAtTtl() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 WEEK");
        execute("INSERT INTO tango VALUES ('1970-01-01'), ('1970-01-03'), ('1970-01-08T00:59:59.999999')");
        assertQuery("ts\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-03T00:00:00.000000Z\n" +
                        "1970-01-08T00:59:59.999999Z\n",
                "tango", "ts", true, true);
    }

    @Test
    public void testWeekOneMicrosBeyondTtl() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1W");
        execute("INSERT INTO tango VALUES ('1970-01-01'), ('1970-01-03'), ('1970-01-08T01:00:00')");
        assertQuery("ts\n" +
                        "1970-01-03T00:00:00.000000Z\n" +
                        "1970-01-08T01:00:00.000000Z\n",
                "tango", "ts", true, true);
    }

    @Test
    public void testYearExactlyAtTtl() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 YEAR");
        execute("INSERT INTO tango VALUES ('1970-01-01T04:20:00.0Z'), ('1970-12-01'), ('1971-01-01T04:59:59.999999')");
        assertQuery("ts\n" +
                        "1970-01-01T04:20:00.000000Z\n" +
                        "1970-12-01T00:00:00.000000Z\n" +
                        "1971-01-01T04:59:59.999999Z\n",
                "tango", "ts", true, true);
    }

    @Test
    public void testYearOneMicrosBeyondTtl() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1Y");
        execute("INSERT INTO tango VALUES ('1970-01-01T04:20:00.0Z'), ('1970-12-01'), ('1971-01-01T05:00:00')");
        assertQuery("ts\n" +
                        "1970-12-01T00:00:00.000000Z\n" +
                        "1971-01-01T05:00:00.000000Z\n",
                "tango", "ts", true, true);
    }
}
