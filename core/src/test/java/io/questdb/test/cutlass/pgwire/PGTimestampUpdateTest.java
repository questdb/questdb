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

import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.function.Consumer;

@SuppressWarnings({"SqlNoDataSourceInspection"})
public class PGTimestampUpdateTest extends BasePGTest {

    @Test
    public void testUpdateMicrosColumnToNull() throws Exception {
        testUpdateTimestampColumnToNull("TIMESTAMP");
    }

    @Test
    public void testUpdateMicrosColumnToNullSentinel() throws Exception {
        testUpdateTimestampColumnToNullSentinel("TIMESTAMP");
    }

    @Test
    public void testUpdateMicrosColumnUsingSetLongAPI() throws Exception {
        testUpdateTimestampColumn("TIMESTAMP", stmt -> {
            try {
                stmt.setLong(2, 1764863925000008L);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }, "2025-12-04T15:58:45.000008");
    }

    @Test
    public void testUpdateMicrosColumnUsingSetStringAPI() throws Exception {
        testUpdateTimestampColumn("TIMESTAMP", stmt -> {
            try {
                stmt.setString(2, "2025-12-04T15:58:45.000002");
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }, "2025-12-04T15:58:45.000002");
    }

    @Test
    public void testUpdateMicrosColumnUsingSetTimestampAPI() throws Exception {
        testUpdateTimestampColumn("TIMESTAMP", stmt -> {
            try {
                stmt.setTimestamp(2, Timestamp.from(Instant.ofEpochSecond(1764863925L, 4242L)));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }, "2025-12-04T15:58:45.000004");
    }

    @Test
    public void testShortAndByteBindsOnDesignatedTimestamp() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, _, _, _) -> {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("CREATE TABLE s (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
                statement.executeUpdate("INSERT INTO s VALUES (3, 1), (3, 2), (5, 3), (7, 4)");
            }
            drainWalQueue();
            final StringSink sink = new StringSink();

            try (PreparedStatement select = connection.prepareStatement("SELECT ts, v FROM s WHERE ts = ?")) {
                select.setShort(1, (short) 3);
                try (ResultSet rs = select.executeQuery()) {
                    assertResultSet("""
                            ts[TIMESTAMP],v[INTEGER]
                            1970-01-01 00:00:00.000003,1
                            1970-01-01 00:00:00.000003,2
                            """, sink, rs);
                }
            }
            try (PreparedStatement select = connection.prepareStatement("SELECT ts, v FROM s WHERE ts = ? OR ts = ?")) {
                select.setShort(1, (short) 3);
                select.setByte(2, (byte) 7);
                try (ResultSet rs = select.executeQuery()) {
                    assertResultSet("""
                            ts[TIMESTAMP],v[INTEGER]
                            1970-01-01 00:00:00.000003,1
                            1970-01-01 00:00:00.000003,2
                            1970-01-01 00:00:00.000007,4
                            """, sink, rs);
                }
            }
            try (PreparedStatement select = connection.prepareStatement("SELECT ts, v FROM s WHERE ts > ?")) {
                select.setShort(1, (short) 3);
                select.executeQuery();
                Assert.fail("SHORT range bound must be rejected");
            } catch (SQLException e) {
                TestUtils.assertContains(e.getMessage(), "Invalid date");
            }
            try (PreparedStatement update = connection.prepareStatement("UPDATE s SET v = 0 WHERE ts > ?")) {
                update.setDouble(1, 3.5);
                update.executeUpdate();
                Assert.fail("DOUBLE range bound must be rejected before the UPDATE is sequenced");
            } catch (SQLException e) {
                TestUtils.assertContains(e.getMessage(), "Invalid date");
            }

            try (PreparedStatement update = connection.prepareStatement("UPDATE s SET v = 100 WHERE ts = ?")) {
                update.setShort(1, (short) 3);
                update.executeUpdate();
            }
            try (PreparedStatement update = connection.prepareStatement("UPDATE s SET v = 200 WHERE ts != ?")) {
                update.setByte(1, (byte) 3);
                update.executeUpdate();
            }
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("s")));
            assertQuery("s")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv
                            1970-01-01T00:00:00.000003Z\t100
                            1970-01-01T00:00:00.000003Z\t100
                            1970-01-01T00:00:00.000005Z\t200
                            1970-01-01T00:00:00.000007Z\t200
                            """);
        });
    }

    @Test
    public void testUpdateNanosColumnToNull() throws Exception {
        testUpdateTimestampColumnToNull("TIMESTAMP_NS");
    }

    @Test
    public void testUpdateNanosColumnToNullSentinel() throws Exception {
        testUpdateTimestampColumnToNullSentinel("TIMESTAMP_NS");
    }

    @Test
    public void testUpdateNanosColumnUsingSetLongAPI() throws Exception {
        testUpdateTimestampColumn("TIMESTAMP_NS", stmt -> {
            try {
                stmt.setLong(2, 1764863925000000042L);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }, "2025-12-04T15:58:45.000000042");
    }

    @Test
    public void testUpdateNanosColumnUsingSetStringAPI() throws Exception {
        testUpdateTimestampColumn("TIMESTAMP_NS", stmt -> {
            try {
                stmt.setString(2, "2025-12-04T15:58:45.000000042");
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }, "2025-12-04T15:58:45.000000042");
    }

    @Test
    public void testUpdateNanosColumnUsingSetTimestampAPI() throws Exception {
        testUpdateTimestampColumn("TIMESTAMP_NS", stmt -> {
            try {
                // transfers only micros precision
                stmt.setTimestamp(2, Timestamp.from(Instant.ofEpochSecond(1764863925L, 4242L)));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }, "2025-12-04T15:58:45.000004000");
    }

    @Test
    public void testUpdateWithInvalidTextBindFailsBeforeSequencing() throws Exception {
        assertWithPgServer(CONN_AWARE_EXTENDED, (connection, _, _, _) -> {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("CREATE TABLE t (ts TIMESTAMP, other TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
                statement.executeUpdate("INSERT INTO t VALUES ('2024-01-01', '2024-01-01', 0)");
            }
            drainWalQueue();
            for (String predicate : new String[]{"ts = ?::varchar", "ts > ?::varchar", "other = ?::string", "other < ?::varchar"}) {
                try (PreparedStatement update = connection.prepareStatement("UPDATE t SET x = 1 WHERE " + predicate)) {
                    update.setString(1, "garbage");
                    update.executeUpdate();
                    Assert.fail("invalid text must fail the UPDATE before the WAL records it");
                } catch (SQLException e) {
                    TestUtils.assertContains(e.getMessage(), "inconvertible value: `garbage` [");
                    TestUtils.assertContains(e.getMessage(), " -> TIMESTAMP]");
                }
            }
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));
            try (PreparedStatement update = connection.prepareStatement("UPDATE t SET x = 1 WHERE ts = ?::varchar")) {
                update.setString(1, "2024-01-01T00:00:00.000000Z");
                update.executeUpdate();
            }
            drainWalQueue();
            try (
                    Statement statement = connection.createStatement();
                    ResultSet rs = statement.executeQuery("SELECT ts, x FROM t")
            ) {
                assertResultSet("""
                        ts[TIMESTAMP],x[INTEGER]
                        2024-01-01 00:00:00.0,1
                        """, new StringSink(), rs);
            }
        });
    }

    private void testUpdateTimestampColumn(String tsType, Consumer<PreparedStatement> setTimestamp, String expectedTimestamp) throws Exception {
        final boolean isNanos = tsType.equals("TIMESTAMP_NS");

        assertWithPgServer(CONN_AWARE_ALL, (connection, _, _, _) -> {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("CREATE TABLE table1 (" +
                        "id LONG, " +
                        "status SYMBOL CAPACITY 32 CACHE INDEX CAPACITY 256, " +
                        "mts " + tsType + ", " +
                        "cts TIMESTAMP" +
                        ") timestamp(cts) PARTITION BY MONTH WAL");
            }

            try (PreparedStatement statement = connection.prepareStatement(
                    "insert into table1 values(20, 'HOHOHO', to_timestamp('2022-01-03', 'yyyy-MM-dd'), to_timestamp('2022-01-03', 'yyyy-MM-dd'))"
            )) {
                statement.execute();
            }
            drainWalQueue();
            assertQuery("table1")
                    .noLeakCheck()
                    .timestamp("cts")
                    .expectSize()
                    .returns("id\tstatus\tmts\tcts\n" +
                            "20\tHOHOHO\t2022-01-03T00:00:00.000000" + (isNanos ? "000" : "") + "Z\t2022-01-03T00:00:00.000000Z\n");

            try (PreparedStatement statement = connection.prepareStatement("update table1 set status = ?, mts = ? where id = ?")) {
                statement.setString(1, "HAHAHA");
                // used the lambda to set the timestamp field
                setTimestamp.accept(statement);
                statement.setLong(3, 20L);

                statement.executeUpdate();
            }
            drainWalQueue();
            assertQuery("table1")
                    .noLeakCheck()
                    .timestamp("cts")
                    .expectSize()
                    .returns("id\tstatus\tmts\tcts\n" +
                            "20\tHAHAHA\t" + expectedTimestamp + "Z\t2022-01-03T00:00:00.000000Z\n");
        });
    }

    private void testUpdateTimestampColumnToNull(String tsType) throws Exception {
        final boolean isNanos = tsType.equals("TIMESTAMP_NS");

        assertWithPgServer(CONN_AWARE_ALL, (connection, _, _, _) -> {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("CREATE TABLE table1 (" +
                        "id LONG, " +
                        "status SYMBOL CAPACITY 32 CACHE INDEX CAPACITY 256, " +
                        "mts " + tsType + ", " +
                        "cts TIMESTAMP" +
                        ") timestamp(cts) PARTITION BY MONTH WAL");
            }

            try (PreparedStatement statement = connection.prepareStatement(
                    "insert into table1 values(20, 'HOHOHO', to_timestamp('2022-01-03', 'yyyy-MM-dd'), to_timestamp('2022-01-03', 'yyyy-MM-dd'))"
            )) {
                statement.execute();
            }
            drainWalQueue();
            assertQuery("table1")
                    .noLeakCheck()
                    .timestamp("cts")
                    .expectSize()
                    .returns("id\tstatus\tmts\tcts\n" +
                            "20\tHOHOHO\t2022-01-03T00:00:00.000000" + (isNanos ? "000" : "") + "Z\t2022-01-03T00:00:00.000000Z\n");

            try (PreparedStatement statement = connection.prepareStatement("update table1 set status = ?, mts = ? where id = ?")) {
                statement.setString(1, "HAHAHA");
                statement.setNull(2, Types.TIMESTAMP);
                statement.setLong(3, 20L);

                statement.executeUpdate();
            }
            drainWalQueue();
            assertQuery("table1")
                    .noLeakCheck()
                    .timestamp("cts")
                    .expectSize()
                    .returns("""
                            id\tstatus\tmts\tcts
                            20\tHAHAHA\t\t2022-01-03T00:00:00.000000Z
                            """);
        });
    }

    private void testUpdateTimestampColumnToNullSentinel(String tsType) throws Exception {
        final boolean isNanos = tsType.equals("TIMESTAMP_NS");

        assertWithPgServer(CONN_AWARE_ALL, (connection, _, _, _) -> {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("CREATE TABLE table1 (" +
                        "id LONG, " +
                        "status SYMBOL CAPACITY 32 CACHE INDEX CAPACITY 256, " +
                        "mts " + tsType + ", " +
                        "cts TIMESTAMP" +
                        ") timestamp(cts) PARTITION BY MONTH WAL");
            }

            try (PreparedStatement statement = connection.prepareStatement(
                    "insert into table1 values(20, 'HOHOHO', to_timestamp('2022-01-03', 'yyyy-MM-dd'), to_timestamp('2022-01-03', 'yyyy-MM-dd'))"
            )) {
                statement.execute();
            }
            drainWalQueue();
            assertQuery("table1")
                    .noLeakCheck()
                    .timestamp("cts")
                    .expectSize()
                    .returns("id\tstatus\tmts\tcts\n" +
                            "20\tHOHOHO\t2022-01-03T00:00:00.000000" + (isNanos ? "000" : "") + "Z\t2022-01-03T00:00:00.000000Z\n");

            try (PreparedStatement statement = connection.prepareStatement("update table1 set status = ?, mts = ? where id = ?")) {
                statement.setString(1, "HAHAHA");
                statement.setLong(2, Long.MIN_VALUE);
                statement.setLong(3, 20L);

                statement.executeUpdate();
            }
            drainWalQueue();
            assertQuery("table1")
                    .noLeakCheck()
                    .timestamp("cts")
                    .expectSize()
                    .returns("""
                            id\tstatus\tmts\tcts
                            20\tHAHAHA\t\t2022-01-03T00:00:00.000000Z
                            """);
        });
    }
}