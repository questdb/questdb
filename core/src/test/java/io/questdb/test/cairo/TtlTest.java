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
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class TtlTest extends AbstractCairoTest {

    private final String wal;

    public TtlTest(WalMode walMode) {
        this.wal = walMode == WalMode.WITH_WAL ? " WAL" : " BYPASS WAL";
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {WalMode.WITH_WAL}, {WalMode.NO_WAL}
        });
    }

    @Test
    public void testAlterSyntaxInvalid() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR" + wal);
        try {
            execute("ALTER TABLE tango SET TTL");
            fail("Invalid syntax accepted");
        } catch (SqlException e) {
            assertEquals("[25] missing argument, should be TTL <number> <unit> or <number_with_unit>", e.getMessage());
        }
        try {
            execute("ALTER TABLE tango SET TTL X");
            fail("Invalid syntax accepted");
        } catch (SqlException e) {
            assertEquals("[26] invalid syntax, should be TTL <number> <unit> but was TTL X", e.getMessage());
        }
        try {
            execute("ALTER TABLE tango SET TTL 12");
            fail("Invalid syntax accepted");
        } catch (SqlException e) {
            assertEquals("[28] missing unit, 'HOUR(S)', 'DAY(S)', 'WEEK(S)', 'MONTH(S)' or 'YEAR(S)' expected", e.getMessage());
        }
        try {
            execute("ALTER TABLE tango SET TTL 1 NONE");
            fail("Invalid syntax accepted");
        } catch (SqlException e) {
            assertEquals("[28] invalid unit, expected 'HOUR(S)', 'DAY(S)', 'WEEK(S)', 'MONTH(S)' or 'YEAR(S)', but was 'NONE'",
                    e.getMessage());
        }
        try {
            execute("ALTER TABLE tango SET TTL HOURS");
            fail("Invalid syntax accepted");
        } catch (SqlException e) {
            assertEquals("[26] invalid argument, should be TTL <number> <unit> or <number_with_unit>",
                    e.getMessage());
        }
        try {
            execute("ALTER TABLE tango SET TTL H");
            fail("Invalid syntax accepted");
        } catch (SqlException e) {
            assertEquals("[26] invalid syntax, should be TTL <number> <unit> but was TTL H",
                    e.getMessage());
        }
        try {
            execute("ALTER TABLE tango SET TTL 1G");
            fail("Invalid syntax accepted");
        } catch (SqlException e) {
            assertEquals("[27] invalid time unit, expecting 'H', 'D', 'W', 'M' or 'Y', but was 'G'",
                    e.getMessage());
        }
    }

    @Test
    public void testAlterTableNotPartitioned() throws Exception {
        Assume.assumeTrue(wal.equals("BYPASS WAL"));
        execute("CREATE TABLE tango (n LONG)");
        execute("ALTER TABLE tango SET TTL 0H"); // zero TTL is acceptable
        try {
            execute("ALTER TABLE tango SET TTL 1H");
            fail("Accepted TTL on a non-partitioned table");
        } catch (SqlException e) {
            assertEquals("[26] cannot set TTL on a non-partitioned table", e.getMessage());
        }
    }

    @Test
    public void testAlterTableSetTtl() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR" + wal);
        execute("INSERT INTO tango VALUES (0), (3_600_000_000), (7_200_000_001)");
        drainWalQueue();
        assertSql("ts\n" +
                        "1970-01-01T00:00:00.000000Z\n" +  // with TTL of 1 hour, this row would be evictable
                        "1970-01-01T01:00:00.000000Z\n" +
                        "1970-01-01T02:00:00.000001Z\n",
                "tango");
        execute("ALTER TABLE tango SET TTL 1H");
        drainWalQueue();
        assertSql("ts\n" +
                        "1970-01-01T01:00:00.000000Z\n" +
                        "1970-01-01T02:00:00.000001Z\n",
                "tango");
    }

    @Test
    public void testCreateSyntaxInvalid() {
        Assume.assumeTrue(wal.equals("BYPASS WAL"));
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
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL HOURS");
            fail("Invalid syntax accepted");
        } catch (SqlException e) {
            assertEquals("[70] invalid argument, should be TTL <number> <unit> or <number_with_unit>",
                    e.getMessage());
        }
        try {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL H");
            fail("Invalid syntax accepted");
        } catch (SqlException e) {
            assertEquals("[70] invalid syntax, should be TTL <number> <unit> but was TTL H",
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
    public void testCreateTableLike() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 2 HOURS" + wal);
        execute("CREATE TABLE samba (LIKE tango)");
        assertSql("ddl\n" +
                        "CREATE TABLE 'samba' ( \n\tts TIMESTAMP\n) timestamp(ts) PARTITION BY HOUR TTL 2 HOURS" + wal
                        + "\nWITH maxUncommittedRows=1000, o3MaxLag=300000000us;\n",
                "SHOW CREATE TABLE samba");
    }

    @Test
    public void testDayExactlyAtTtl() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 DAY" + wal);
        execute("INSERT INTO tango VALUES ('1970-01-01T00:00:00'), ('1970-01-01T23:00:00'), ('1970-01-02T00:59:59.999999')");
        drainWalQueue();
        assertSql("ts\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-01T23:00:00.000000Z\n" +
                        "1970-01-02T00:59:59.999999Z\n",
                "tango");
    }

    @Test
    public void testDayOneMicrosBeyondTtl() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1D");
        execute("INSERT INTO tango VALUES ('1970-01-01T00:00:00'), ('1970-01-01T23:00:00'), ('1970-01-02T01:00:00')");
        assertSql("ts\n" +
                        "1970-01-01T23:00:00.000000Z\n" +
                        "1970-01-02T01:00:00.000000Z\n",
                "tango");
    }

    @Test
    public void testGranularityInvalidAlter() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        try {
            execute("ALTER TABLE tango SET TTL 1 HOUR");
            fail("Accepted a TTL that's too fine-grained for partition size");
        } catch (SqlException e) {
            assertEquals("[26] TTL value must be an integer multiple of partition size", e.getMessage());
        }
        try {
            execute("ALTER TABLE tango SET TTL 25 HOUR");
            fail("Accepted a TTL that's too fine-grained for partition size");
        } catch (SqlException e) {
            assertEquals("[26] TTL value must be an integer multiple of partition size", e.getMessage());
        }
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY WEEK");
        try {
            execute("ALTER TABLE tango SET TTL 1 HOUR");
            fail("Accepted a TTL that's too fine-grained for partition size");
        } catch (SqlException e) {
            assertEquals("[26] TTL value must be an integer multiple of partition size", e.getMessage());
        }
        try {
            execute("ALTER TABLE tango SET TTL 1 DAY");
            fail("Accepted a TTL that's too fine-grained for partition size");
        } catch (SqlException e) {
            assertEquals("[26] TTL value must be an integer multiple of partition size", e.getMessage());
        }
        try {
            execute("ALTER TABLE tango SET TTL 12 DAY");
            fail("Accepted a TTL that's too fine-grained for partition size");
        } catch (SqlException e) {
            assertEquals("[26] TTL value must be an integer multiple of partition size", e.getMessage());
        }
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY MONTH");
        try {
            execute("ALTER TABLE tango SET TTL 1 HOUR");
            fail("Accepted a TTL that's too fine-grained for partition size");
        } catch (SqlException e) {
            assertEquals("[26] TTL value must be an integer multiple of partition size", e.getMessage());
        }
        try {
            execute("ALTER TABLE tango SET TTL 30 DAY");
            fail("Accepted a TTL that's too fine-grained for partition size");
        } catch (SqlException e) {
            assertEquals("[26] TTL value must be an integer multiple of partition size", e.getMessage());
        }
        try {
            execute("ALTER TABLE tango SET TTL 4 WEEK");
            fail("Accepted a TTL that's too fine-grained for partition size");
        } catch (SqlException e) {
            assertEquals("[26] TTL value must be an integer multiple of partition size", e.getMessage());
        }
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY YEAR");
        try {
            execute("ALTER TABLE tango SET TTL 1000 HOUR");
            fail("Accepted a TTL that's too fine-grained for partition size");
        } catch (SqlException e) {
            assertEquals("[26] TTL value must be an integer multiple of partition size", e.getMessage());
        }
        try {
            execute("ALTER TABLE tango SET TTL 365 DAY");
            fail("Accepted a TTL that's too fine-grained for partition size");
        } catch (SqlException e) {
            assertEquals("[26] TTL value must be an integer multiple of partition size", e.getMessage());
        }
        try {
            execute("ALTER TABLE tango SET TTL 52 WEEK");
            fail("Accepted a TTL that's too fine-grained for partition size");
        } catch (SqlException e) {
            assertEquals("[26] TTL value must be an integer multiple of partition size", e.getMessage());
        }
        try {
            execute("ALTER TABLE tango SET TTL 13 MONTH");
            fail("Accepted a TTL that's too fine-grained for partition size");
        } catch (SqlException e) {
            assertEquals("[26] TTL value must be an integer multiple of partition size", e.getMessage());
        }
    }

    @Test
    public void testGranularityInvalidCreate() throws Exception {
        try {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY TTL 1 HOUR");
            fail("Accepted a TTL that's too fine-grained for partition size");
        } catch (SqlException e) {
            assertEquals("[69] TTL value must be an integer multiple of partition size", e.getMessage());
        }
        try {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY TTL 25 HOUR");
            fail("Accepted a TTL that's too fine-grained for partition size");
        } catch (SqlException e) {
            assertEquals("[69] TTL value must be an integer multiple of partition size", e.getMessage());
        }
        try {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY WEEK TTL 1 HOUR");
            fail("Accepted a TTL that's too fine-grained for partition size");
        } catch (SqlException e) {
            assertEquals("[70] TTL value must be an integer multiple of partition size", e.getMessage());
        }
        try {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY WEEK TTL 1 DAY");
            fail("Accepted a TTL that's too fine-grained for partition size");
        } catch (SqlException e) {
            assertEquals("[70] TTL value must be an integer multiple of partition size", e.getMessage());
        }
        try {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY WEEK TTL 12 DAY");
            fail("Accepted a TTL that's too fine-grained for partition size");
        } catch (SqlException e) {
            assertEquals("[70] TTL value must be an integer multiple of partition size", e.getMessage());
        }
        try {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY MONTH TTL 1 HOUR");
            fail("Accepted a TTL that's too fine-grained for partition size");
        } catch (SqlException e) {
            assertEquals("[71] TTL value must be an integer multiple of partition size", e.getMessage());
        }
        try {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY MONTH TTL 30 DAY");
            fail("Accepted a TTL that's too fine-grained for partition size");
        } catch (SqlException e) {
            assertEquals("[71] TTL value must be an integer multiple of partition size", e.getMessage());
        }
        try {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY MONTH TTL 4 WEEK");
            fail("Accepted a TTL that's too fine-grained for partition size");
        } catch (SqlException e) {
            assertEquals("[71] TTL value must be an integer multiple of partition size", e.getMessage());
        }
        try {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY YEAR TTL 1000 HOUR");
            fail("Accepted a TTL that's too fine-grained for partition size");
        } catch (SqlException e) {
            assertEquals("[70] TTL value must be an integer multiple of partition size", e.getMessage());
        }
        try {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY YEAR TTL 365 DAY");
            fail("Accepted a TTL that's too fine-grained for partition size");
        } catch (SqlException e) {
            assertEquals("[70] TTL value must be an integer multiple of partition size", e.getMessage());
        }
        try {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY YEAR TTL 52 WEEK");
            fail("Accepted a TTL that's too fine-grained for partition size");
        } catch (SqlException e) {
            assertEquals("[70] TTL value must be an integer multiple of partition size", e.getMessage());
        }
        try {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY YEAR TTL 13 MONTH");
            fail("Accepted a TTL that's too fine-grained for partition size");
        } catch (SqlException e) {
            assertEquals("[70] TTL value must be an integer multiple of partition size", e.getMessage());
        }
    }

    @Test
    public void testGranularityValidAlter() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("ALTER TABLE tango SET TTL 24 HOUR");
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY WEEK");
        execute("ALTER TABLE tango SET TTL 168 HOUR");
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY YEAR");
        execute("ALTER TABLE tango SET TTL 12 MONTH");
    }

    @Test
    public void testGranularityValidCreate() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY TTL 24 HOUR");
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY WEEK TTL 7 DAY");
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY WEEK TTL 168 HOUR");
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY YEAR TTL 12 MONTH");
    }

    @Test
    public void testHourExactlyAtTtl() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 HOUR");
        execute("INSERT INTO tango VALUES ('1970-01-01T00:00:00'), ('1970-01-01T01:00:00'), ('1970-01-01T01:59:59.999999')");
        assertSql("ts\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-01T01:00:00.000000Z\n" +
                        "1970-01-01T01:59:59.999999Z\n",
                "tango");
    }

    @Test
    public void testHourOneMicrosBeyondTtl() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1H" + wal);
        execute("INSERT INTO tango VALUES ('1970-01-01T00:00:00'), ('1970-01-01T01:00:00'), ('1970-01-01T02:00:00')");
        drainWalQueue();
        assertSql("ts\n" +
                        "1970-01-01T01:00:00.000000Z\n" +
                        "1970-01-01T02:00:00.000000Z\n",
                "tango");
    }

    @Test
    public void testManyPartitions() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 HOUR" + wal);
        execute("INSERT INTO tango SELECT (x*1_000_000*60*60)::TIMESTAMP ts FROM long_sequence(72);");
        drainWalQueue();
        assertSql("ts\n" +
                        "1970-01-03T23:00:00.000000Z\n" +
                        "1970-01-04T00:00:00.000000Z\n",
                "tango");
    }

    @Test
    public void testMonthExactlyAtTtl() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 MONTH" + wal);
        execute("INSERT INTO tango VALUES ('1970-02-01T04:20:00.0Z'), ('1970-02-10T04:20:00.0Z'), ('1970-03-01T04:59:59.999999Z')");
        drainWalQueue();
        assertSql("ts\n" +
                        "1970-02-01T04:20:00.000000Z\n" +
                        "1970-02-10T04:20:00.000000Z\n" +
                        "1970-03-01T04:59:59.999999Z\n",
                "tango");
    }

    @Test
    public void testMonthOneMicrosBeyondTtl() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1M" + wal);
        execute("INSERT INTO tango VALUES ('1970-02-01T04:20:00.0Z'), ('1970-02-10T04:20:00.0Z'), ('1970-03-01T05:00:00')");
        drainWalQueue();
        assertSql("ts\n" +
                        "1970-02-10T04:20:00.000000Z\n" +
                        "1970-03-01T05:00:00.000000Z\n",
                "tango");
    }

    @Test
    public void testRandomInsertion() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1D" + wal);
        execute("INSERT INTO tango SELECT rnd_timestamp('1970-01-01', '1970-12-01', 0) ts from long_sequence(2_000_000)");
        execute("INSERT INTO tango values ('1970-12-02')");
        drainWalQueue();
        assertQuery("name\n1970-12-02T00\n",
                "SELECT name FROM (SHOW PARTITIONS FROM tango)",
                "", false, true);
    }

    @Test
    public void testSyntaxJustWithinRange() throws Exception {
        Assume.assumeTrue(wal.equals("BYPASS WAL"));

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
        Assume.assumeTrue(wal.equals("BYPASS WAL"));

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
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1H" + wal);
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 HOUR" + wal);
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 HOURS" + wal);
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1D" + wal);
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 DAY" + wal);
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 DAYS" + wal);
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1W" + wal);
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 WEEK" + wal);
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 WEEKS" + wal);
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1M" + wal);
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 MONTH" + wal);
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 MONTHS" + wal);
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1Y" + wal);
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 YEAR" + wal);
        execute("DROP TABLE tango");
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 YEARS" + wal);
    }

    @Test
    public void testTablesFunction() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR" + wal);
        assertSql("ttlValue\tttlUnit\n0\tHOUR\n", "select ttlValue, ttlUnit from tables()");
        execute("ALTER TABLE tango SET TTL 2 HOURS");
        drainWalQueue();
        assertSql("ttlValue\tttlUnit\n2\tHOUR\n", "select ttlValue, ttlUnit from tables()");
        execute("ALTER TABLE tango SET TTL 2 DAYS");
        drainWalQueue();
        assertSql("ttlValue\tttlUnit\n2\tDAY\n", "select ttlValue, ttlUnit from tables()");
        execute("ALTER TABLE tango SET TTL 2 WEEKS");
        drainWalQueue();
        assertSql("ttlValue\tttlUnit\n2\tWEEK\n", "select ttlValue, ttlUnit from tables()");
        execute("ALTER TABLE tango SET TTL 2 MONTHS");
        drainWalQueue();
        assertSql("ttlValue\tttlUnit\n2\tMONTH\n", "select ttlValue, ttlUnit from tables()");
        execute("ALTER TABLE tango SET TTL 2 YEARS");
        drainWalQueue();
        assertSql("ttlValue\tttlUnit\n2\tYEAR\n", "select ttlValue, ttlUnit from tables()");
    }

    @Test
    public void testWeekExactlyAtTtl() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 WEEK" + wal);
        execute("INSERT INTO tango VALUES ('1970-01-01'), ('1970-01-03'), ('1970-01-08T00:59:59.999999')");
        drainWalQueue();
        assertSql("ts\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-03T00:00:00.000000Z\n" +
                        "1970-01-08T00:59:59.999999Z\n",
                "tango");
    }

    @Test
    public void testWeekOneMicrosBeyondTtl() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1W" + wal);
        execute("INSERT INTO tango VALUES ('1970-01-01'), ('1970-01-03'), ('1970-01-08T01:00:00')");
        drainWalQueue();
        assertSql("ts\n" +
                        "1970-01-03T00:00:00.000000Z\n" +
                        "1970-01-08T01:00:00.000000Z\n",
                "tango");
    }

    @Test
    public void testYearExactlyAtTtl() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 YEAR" + wal);
        execute("INSERT INTO tango VALUES ('1970-01-01T04:20:00.0Z'), ('1970-12-01'), ('1971-01-01T04:59:59.999999')");
        drainWalQueue();
        assertSql("ts\n" +
                        "1970-01-01T04:20:00.000000Z\n" +
                        "1970-12-01T00:00:00.000000Z\n" +
                        "1971-01-01T04:59:59.999999Z\n",
                "tango");
    }

    @Test
    public void testYearOneMicrosBeyondTtl() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1Y" + wal);
        execute("INSERT INTO tango VALUES ('1970-01-01T04:20:00.0Z'), ('1970-12-01'), ('1971-01-01T05:00:00')");
        drainWalQueue();
        assertSql("ts\n" +
                        "1970-12-01T00:00:00.000000Z\n" +
                        "1971-01-01T05:00:00.000000Z\n",
                "tango");
    }
}
