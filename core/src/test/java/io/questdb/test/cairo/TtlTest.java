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
    public void testDayExactlyAtTtl() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 DAY");
        execute("INSERT INTO tango VALUES (0), (86_400_000_000), (172_800_000_000)");
        assertQuery("ts\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-02T00:00:00.000000Z\n" +
                        "1970-01-03T00:00:00.000000Z\n",
                "tango", "ts", true, true);
    }

    @Test
    public void testDayOneMicrosBeyondTtl() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 DAY");
        execute("INSERT INTO tango VALUES (0), (86_400_000_000), (172_800_000_001)");
        assertQuery("ts\n" +
                        "1970-01-02T00:00:00.000000Z\n" +
                        "1970-01-03T00:00:00.000001Z\n",
                "tango", "ts", true, true);
    }

    @Test
    public void testHourExactlyAtTtl() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 HOUR");
        execute("INSERT INTO tango VALUES (0), (3_600_000_000), (7_200_000_000)");
        assertQuery("ts\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-01T01:00:00.000000Z\n" +
                        "1970-01-01T02:00:00.000000Z\n",
                "tango", "ts", true, true);
    }

    @Test
    public void testHourOneMicrosBeyondTtl() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 HOUR");
        execute("INSERT INTO tango VALUES (0), (3_600_000_000), (7_200_000_001)");
        assertQuery("ts\n" +
                        "1970-01-01T01:00:00.000000Z\n" +
                        "1970-01-01T02:00:00.000001Z\n",
                "tango", "ts", true, true);
    }

    @Test
    public void testMonthExactlyAtTtl() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 MONTH");
        execute("INSERT INTO tango VALUES (0), (2_628_000_000_000), (5_256_000_000_000)");
        assertQuery("ts\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-31T10:00:00.000000Z\n" +
                        "1970-03-02T20:00:00.000000Z\n",
                "tango", "ts", true, true);
    }

    @Test
    public void testMonthOneMicrosBeyondTtl() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 MONTH");
        execute("INSERT INTO tango VALUES (0), (2_628_000_000_000), (5_256_000_000_001)");
        assertQuery("ts\n" +
                        "1970-01-31T10:00:00.000000Z\n" +
                        "1970-03-02T20:00:00.000001Z\n",
                "tango", "ts", true, true);
    }

    @Test
    public void testSyntaxInvalid() {
        try {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL");
            fail("Invalid syntax accepted");
        } catch (SqlException e) {
            assertEquals("[69] missing argument, should be TTL <number> <unit>", e.getMessage());
        }
        try {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL X");
            fail("Invalid syntax accepted");
        } catch (SqlException e) {
            assertEquals("[70] invalid syntax, should be TTL <number> <unit> but was TTL X", e.getMessage());
        }
        try {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1");
            fail("Invalid syntax accepted");
        } catch (SqlException e) {
            assertEquals("[71] missing unit, 'HOUR(S)', 'DAY(S)', 'MONTH(S)' or 'YEAR(S)' expected", e.getMessage());
        }
        try {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 GROKS");
            fail("Invalid syntax accepted");
        } catch (SqlException e) {
            assertEquals("[77] invalid unit, expected 'HOUR(S)', 'DAY(S)', 'MONTH(S)' or 'YEAR(S)', but was 'GROKS'",
                    e.getMessage());
        }
        try {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 NONES");
            fail("Invalid syntax accepted");
        } catch (SqlException e) {
            assertEquals("[77] invalid unit, expected 'HOUR(S)', 'DAY(S)', 'MONTH(S)' or 'YEAR(S)', but was 'NONES'",
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
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 244_979 MONTHS");
            fail("Invalid syntax accepted");
        } catch (SqlException e) {
            assertEquals("[70] value out of range: 244979 months. Max value: 244978 months",
                    e.getMessage());
        }
        try {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 244_979 YEARS");
            fail("Invalid syntax accepted");
        } catch (SqlException e) {
            assertEquals("[70] value out of range: 244979 years. Max value: 244978 years",
                    e.getMessage());
        }
    }

    @Test
    public void testSyntaxValid() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 DAY");
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 DAYS");
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 WEEK");
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 WEEKS");
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 MONTH");
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 MONTHS");
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 YEAR");
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 YEARS");
    }

    @Test
    public void testWeekExactlyAtTtl() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 WEEK");
        execute("INSERT INTO tango VALUES (0), (604_800_000_000), (1_209_600_000_000)");
        assertQuery("ts\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-08T00:00:00.000000Z\n" +
                        "1970-01-15T00:00:00.000000Z\n",
                "tango", "ts", true, true);
    }

    @Test
    public void testWeekOneMicrosBeyondTtl() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 WEEK");
        execute("INSERT INTO tango VALUES (0), (604_800_000_000), (1_209_600_000_001)");
        assertQuery("ts\n" +
                        "1970-01-08T00:00:00.000000Z\n" +
                        "1970-01-15T00:00:00.000001Z\n",
                "tango", "ts", true, true);
    }
}
