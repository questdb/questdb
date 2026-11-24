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

import io.questdb.DefaultFactoryProvider;
import io.questdb.cairo.TickCalendarServiceFactory;
import io.questdb.jit.JitUtil;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.StringSink;
import io.questdb.test.cairo.TestTickCalendarService;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * The extended protocol compiles a statement at Parse, with the "now" of the last statement that
 * the connection ran, and sets "now" at the Execute that runs it. A tick expression whose date
 * variables resolve against that stale "now" fails nothing as the statement compiles, and the
 * statement runs against the "now" of the Execute.
 */
public class PGTickExprPreparedStatementTest extends BasePGTest {
    private static final long NOW_0300 = MicrosFormatUtils.parseTimestamp("2024-01-02T03:00:00.000000Z");
    private static final long NOW_1200 = MicrosFormatUtils.parseTimestamp("2024-01-02T12:00:00.000000Z");

    @Test
    public void testPreparedBareDateVariableTickCalendarDurationFollowsNowOfEachRun() throws Exception {
        // A statement prepared once with a bind variable that holds a bare date variable expression
        // with a tick calendar and a duration selects, on the day of each run, the minutes at 15:00
        // and 16:00, each an hour longer: the 121 minutes from 15:00 to 17:00 on Friday 2025-01-24
        // and on Monday 2025-01-27, which XNYS trades on from 14:30 to 21:00Z. Each mode of the server
        // sets the engine up anew, with the test tick calendars.
        assertWithPgServer(CONN_AWARE_EXTENDED, (connection, binary, mode, port) -> {
            final long friday = MicrosFormatUtils.parseTimestamp("2025-01-24T12:00:00.000000Z");
            final long monday = MicrosFormatUtils.parseTimestamp("2025-01-27T12:00:00.000000Z");
            setCurrentMicros(friday);
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE base (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
                // a row every minute of the four days from 2025-01-24 to 2025-01-27
                statement.execute("INSERT INTO base SELECT timestamp_sequence('2025-01-24T00:00:00.000000Z', 60_000_000L), 1 FROM long_sequence(5_760)");
            }
            drainWalQueue();
            try (PreparedStatement bind = connection.prepareStatement("SELECT count(), min(ts), max(ts) FROM base WHERE ts IN ?")) {
                bind.setString(1, "$todayT[15:00,16:00]#XNYS;1h");
                for (int i = 0; i < 2; i++) {
                    currentMicros = friday;
                    assertRows(bind, "count[BIGINT],min[TIMESTAMP],max[TIMESTAMP]\n121,2025-01-24 15:00:00.0,2025-01-24 17:00:00.0\n");
                    currentMicros = monday;
                    assertRows(bind, "count[BIGINT],min[TIMESTAMP],max[TIMESTAMP]\n121,2025-01-27 15:00:00.0,2025-01-27 17:00:00.0\n");
                }
            }
        }, () -> factoryProvider = new DefaultFactoryProvider() {
            @Override
            public @NotNull TickCalendarServiceFactory getTickCalendarServiceFactory() {
                return () -> TestTickCalendarService.INSTANCE;
            }
        });
    }

    @Test
    public void testPreparedPointInTimeRangeElementSuffixFollowsNowOfEachRun() throws Exception {
        // A statement prepared once applies the day filter of the element of a range between two
        // points in time on the day of each run, with the literal and with a bind variable: #Mon
        // holds no row on Sunday 2025-01-19, and the rows from 10:00 to 12:00 on Monday. A timezone
        // of the element before its day filter is an offset: 10:00 to 12:00 at UTC+05 is 05:00 to
        // 07:00Z.
        assertWithPgServer(CONN_AWARE_EXTENDED, (connection, binary, mode, port) -> {
            final long sunday = MicrosFormatUtils.parseTimestamp("2025-01-19T12:00:00.000000Z");
            final long monday = MicrosFormatUtils.parseTimestamp("2025-01-20T12:00:00.000000Z");
            setCurrentMicros(sunday);
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE base (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
                statement.execute("""
                        INSERT INTO base VALUES
                        ('2025-01-19T05:00:00.000000Z', 1),
                        ('2025-01-19T06:00:00.000000Z', 1),
                        ('2025-01-19T11:00:00.000000Z', 1),
                        ('2025-01-20T11:00:00.000000Z', 1)
                        """);
            }
            drainWalQueue();
            final String none = "ts[TIMESTAMP],v[INTEGER]\n";
            final String mondayRow = "ts[TIMESTAMP],v[INTEGER]\n2025-01-20 11:00:00.0,1\n";
            try (
                    PreparedStatement literal = connection.prepareStatement("SELECT ts, v FROM base WHERE ts IN '[$now - 2h..$now#Mon]'");
                    PreparedStatement bind = connection.prepareStatement("SELECT ts, v FROM base WHERE ts IN ?")
            ) {
                bind.setString(1, "[$now - 2h..$now#Mon]");
                for (int i = 0; i < 2; i++) {
                    currentMicros = sunday;
                    assertRows(literal, none);
                    assertRows(bind, none);
                    currentMicros = monday;
                    assertRows(literal, mondayRow);
                    assertRows(bind, mondayRow);
                }
                currentMicros = sunday;
                bind.setString(1, "[$now - 2h..$now@UTC+05#Sun]");
                for (int i = 0; i < 2; i++) {
                    assertRows(bind, "ts[TIMESTAMP],v[INTEGER]\n2025-01-19 05:00:00.0,1\n2025-01-19 06:00:00.0,1\n");
                }
            }
        });
    }

    @Test
    public void testStatementPreparedBeforeRangeStartRunsAfterIt() throws Exception {
        // At 03:00 the range from 09:30 to "now" starts after it ends, and from "now" to 1024 days
        // after today, 09:00 comes 1025 times, one more than the limit. A connection whose last
        // statement ran at 03:00 prepares statements with these at 12:00, which the JIT compiles,
        // opening their interval scans at 03:00. The statements run at 12:00, every time. At 03:00
        // on the next day, they fail when they run, and the connection keeps answering.
        sharedQueryWorkerCount = 2;
        try {
            assertWithPgServer(CONN_AWARE_EXTENDED, (connection, binary, mode, port) -> {
                setCurrentMicros(NOW_0300);
                try (Statement statement = connection.createStatement()) {
                    statement.execute("CREATE TABLE base (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    statement.execute("""
                            INSERT INTO base VALUES
                            ('2024-01-02T08:00:00.000000Z', 1),
                            ('2024-01-02T10:00:00.000000Z', 1),
                            ('2024-01-03T09:00:00.000000Z', 1),
                            ('2024-01-03T10:00:00.000000Z', 1)
                            """);
                }
                drainWalQueue();
                final String rangeSql = "SELECT ts, v FROM base WHERE ts IN '$today + 570m..$now' AND v > 0";
                final String limitSql = "SELECT count() FROM base WHERE ts IN '[$now..$today + 1024d]T09:00' AND v > 0";

                runLastStatementAt0300(connection);
                try (PreparedStatement range = connection.prepareStatement(rangeSql)) {
                    for (int i = 0; i < 2; i++) {
                        assertRows(range, "ts[TIMESTAMP],v[INTEGER]\n2024-01-02 10:00:00.0,1\n");
                    }
                    runLastStatementAt0300(connection);
                    try (PreparedStatement limit = connection.prepareStatement(limitSql)) {
                        // 09:00 on 2024-01-03
                        for (int i = 0; i < 2; i++) {
                            assertRows(limit, "count[BIGINT]\n1\n");
                        }

                        currentMicros = MicrosFormatUtils.parseTimestamp("2024-01-03T03:00:00.000000Z");
                        assertFails(range, "Invalid date range: start is after end", rangeSql.indexOf('\'') + 1);
                        assertFails(limit, "Date range expansion produces too many intervals (max 1024)", limitSql.indexOf('\'') + 1);
                    }
                    currentMicros = MicrosFormatUtils.parseTimestamp("2024-01-03T12:00:00.000000Z");
                    assertRows(range, "ts[TIMESTAMP],v[INTEGER]\n2024-01-03 10:00:00.0,1\n");
                }

                runLastStatementAt0300(connection);
                try (
                        PreparedStatement explain = connection.prepareStatement("EXPLAIN " + rangeSql);
                        ResultSet rs = explain.executeQuery()
                ) {
                    final StringSink plan = new StringSink();
                    while (rs.next()) {
                        plan.put(rs.getString(1)).put('\n');
                    }
                    TestUtils.assertContains(plan, JitUtil.isJitSupported() ? "Async JIT Filter" : "Async Filter");
                }
            });
        } finally {
            sharedQueryWorkerCount = 0;
        }
    }

    private static void assertFails(PreparedStatement statement, String error, int position) throws SQLException {
        try (ResultSet ignore = statement.executeQuery()) {
            Assert.fail("the query should fail");
        } catch (PSQLException e) {
            Assert.assertNotNull(e.getServerErrorMessage());
            TestUtils.assertEquals(error, e.getServerErrorMessage().getMessage());
            Assert.assertEquals(position, e.getServerErrorMessage().getPosition());
        }
    }

    private static void assertRows(PreparedStatement statement, String expected) throws SQLException {
        try (ResultSet rs = statement.executeQuery()) {
            sink.clear();
            assertResultSet(expected, sink, rs);
        }
    }

    /**
     * Runs a statement on the connection at 03:00, which sets the "now" that the connection compiles
     * its next statement at, then moves the clock to 12:00.
     */
    private static void runLastStatementAt0300(Connection connection) throws SQLException {
        currentMicros = NOW_0300;
        try (Statement statement = connection.createStatement()) {
            statement.execute("SELECT 1");
        }
        currentMicros = NOW_1200;
    }
}
