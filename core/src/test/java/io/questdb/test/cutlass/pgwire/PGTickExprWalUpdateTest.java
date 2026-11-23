/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.test.cutlass.pgwire;

import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * A prepared UPDATE on a WAL table hands its bind variables to the WAL, which runs the statement
 * after the client was told it succeeded. A tick expression in a bind variable that does not parse
 * acts as a null value there, rather than suspending the table, and a date variable in it resolves
 * against the commit time.
 */
public class PGTickExprWalUpdateTest extends BasePGTest {

    @Test
    public void testPreparedPointInTimeRangeWithMalformedDurationFails() throws Exception {
        // A range between two points in time ignores its duration, which still has to parse: such a
        // duration fails a prepared statement on every run, with the bind variable until it holds a
        // duration that parses, and with the literal. The bind variable selected the rows of the
        // range. The repeating interval of the parser, a duration, a period and a count, repeats
        // nothing after the range.
        assertWithPgServer(CONN_AWARE_EXTENDED, (connection, binary, mode, port) -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T10:00:00.000000Z"));
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE w (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
                statement.execute("""
                        INSERT INTO w VALUES
                        ('2024-01-02T09:30:00.000000Z', 1),
                        ('2024-01-02T10:00:00.000000Z', 1),
                        ('2024-01-02T10:30:00.000000Z', 1)
                        """);
            }
            drainWalQueue();
            try (PreparedStatement select = connection.prepareStatement("SELECT ts FROM w WHERE ts IN ?")) {
                select.setString(1, "$now - 1h..$now;1h#Tue");
                for (int i = 0; i < 2; i++) {
                    try (ResultSet ignore = select.executeQuery()) {
                        Assert.fail("the query should fail");
                    } catch (PSQLException e) {
                        TestUtils.assertContains(e.getMessage(), "Expected number before unit '#'");
                    }
                }
                // 09:00 to 10:00
                for (String tickExpr : new String[]{"$now - 1h..$now;1h", "$now - 1h..$now;1h;1d;3"}) {
                    select.setString(1, tickExpr);
                    try (ResultSet rs = select.executeQuery()) {
                        sink.clear();
                        assertResultSet(
                                """
                                        ts[TIMESTAMP]
                                        2024-01-02 09:30:00.0
                                        2024-01-02 10:00:00.0
                                        """,
                                sink,
                                rs
                        );
                    }
                }
            }
            for (String sql : new String[]{"SELECT ts FROM w WHERE ts IN '$now - 1h..$now;1x'", "UPDATE w SET v = 2 WHERE ts IN '$now - 1h..$now;1x'"}) {
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    for (int i = 0; i < 2; i++) {
                        try {
                            statement.execute();
                            Assert.fail("the statement should fail: " + sql);
                        } catch (PSQLException e) {
                            TestUtils.assertContains(e.getMessage(), "Invalid duration unit: x");
                        }
                    }
                }
            }
            drainWalQueue();
            try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery("SELECT v FROM w")) {
                sink.clear();
                assertResultSet(
                        """
                                v[INTEGER]
                                1
                                1
                                1
                                """,
                        sink,
                        rs
                );
            }
        });
    }

    @Test
    public void testPreparedStatementWithUnknownDurationUnitFails() throws Exception {
        // A duration unit that the timestamp drivers do not add fails a prepared statement on every
        // run, with the literal, and with the bind variable until it holds a unit that they add. The
        // literal selected every row before $today, and the UPDATE rewrote them.
        assertWithPgServer(CONN_AWARE_EXTENDED, (connection, binary, mode, port) -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T10:00:00.000000Z"));
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE w (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
                statement.execute("""
                        INSERT INTO w VALUES
                        ('2024-01-01T00:00:00.000000Z', 1),
                        ('2024-01-02T00:00:00.000000Z', 1),
                        ('2024-01-02T01:00:00.000000Z', 1)
                        """);
            }
            drainWalQueue();
            for (String sql : new String[]{"SELECT ts FROM w WHERE ts IN '$today;1x'", "UPDATE w SET v = 2 WHERE ts IN '$today;1x'"}) {
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    for (int i = 0; i < 2; i++) {
                        try {
                            statement.execute();
                            Assert.fail("the statement should fail: " + sql);
                        } catch (PSQLException e) {
                            TestUtils.assertContains(e.getMessage(), "Invalid duration unit: x");
                        }
                    }
                }
            }
            try (PreparedStatement select = connection.prepareStatement("SELECT ts FROM w WHERE ts IN ?")) {
                select.setString(1, "$today;1x");
                for (int i = 0; i < 2; i++) {
                    try (ResultSet ignore = select.executeQuery()) {
                        Assert.fail("the query should fail");
                    } catch (PSQLException e) {
                        TestUtils.assertContains(e.getMessage(), "Invalid duration unit: x");
                    }
                }
                // the first hour of $today
                select.setString(1, "$today;1h");
                try (ResultSet rs = select.executeQuery()) {
                    sink.clear();
                    assertResultSet(
                            """
                                    ts[TIMESTAMP]
                                    2024-01-02 00:00:00.0
                                    """,
                            sink,
                            rs
                    );
                }
            }
            drainWalQueue();
            try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery("SELECT v FROM w")) {
                sink.clear();
                assertResultSet(
                        """
                                v[INTEGER]
                                1
                                1
                                1
                                """,
                        sink,
                        rs
                );
            }
        });
    }

    @Test
    public void testPreparedWalUpdateWithTickExprBindVariable() throws Exception {
        assertWithPgServer(CONN_AWARE_EXTENDED, (connection, binary, mode, port) -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T12:00:00.000000Z"));
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE w (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
                statement.execute("""
                        INSERT INTO w VALUES
                        ('2024-01-02T08:00:00.000000Z', 1),
                        ('2024-01-02T10:00:00.000000Z', 1),
                        ('2024-01-03T08:00:00.000000Z', 1)
                        """);
            }
            drainWalQueue();
            try (PreparedStatement update = connection.prepareStatement("UPDATE w SET v = v + 1 WHERE ts IN ?")) {
                update.setString(1, "2024-01-02T08:00@Bogus/Zone");
                update.executeUpdate();
                // 09:30 to 12:00, at the commit time
                update.setString(1, "$today + 570m..$now");
                update.executeUpdate();
                update.setString(1, "2024-01-03");
                update.executeUpdate();
            }
            // the apply runs the next day, against the commit time
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-03T03:00:00.000000Z"));
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("w")));
            try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery("SELECT ts, v FROM w")) {
                sink.clear();
                assertResultSet(
                        """
                                ts[TIMESTAMP],v[INTEGER]
                                2024-01-02 08:00:00.0,1
                                2024-01-02 10:00:00.0,2
                                2024-01-03 08:00:00.0,2
                                """,
                        sink,
                        rs
                );
            }
            // a query that a user runs fails on the value
            try (PreparedStatement select = connection.prepareStatement("SELECT count() FROM w WHERE ts IN ?")) {
                select.setString(1, "2024-01-02T08:00@Bogus/Zone");
                try (ResultSet ignore = select.executeQuery()) {
                    Assert.fail("the query should fail");
                } catch (PSQLException e) {
                    TestUtils.assertContains(e.getMessage(), "invalid timezone: Bogus/Zone");
                }
            }
        });
    }
}
