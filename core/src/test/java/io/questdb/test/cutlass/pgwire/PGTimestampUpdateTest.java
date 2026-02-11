/*******************************************************************************
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

import org.junit.Test;

import java.sql.PreparedStatement;
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

    private void testUpdateTimestampColumn(String tsType, Consumer<PreparedStatement> setTimestamp, String expectedTimestamp) throws Exception {
        final boolean isNanos = tsType.equals("TIMESTAMP_NS");

        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
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
            assertQueryNoLeakCheck("id\tstatus\tmts\tcts\n" +
                            "20\tHOHOHO\t2022-01-03T00:00:00.000000" + (isNanos ? "000" : "") + "Z\t2022-01-03T00:00:00.000000Z\n",
                    "table1", null, "cts", null, null, true, true, false);

            try (PreparedStatement statement = connection.prepareStatement("update table1 set status = ?, mts = ? where id = ?")) {
                statement.setString(1, "HAHAHA");
                // used the lambda to set the timestamp field
                setTimestamp.accept(statement);
                statement.setLong(3, 20L);

                statement.executeUpdate();
            }
            drainWalQueue();
            assertQueryNoLeakCheck("id\tstatus\tmts\tcts\n" +
                            "20\tHAHAHA\t" + expectedTimestamp + "Z\t2022-01-03T00:00:00.000000Z\n",
                    "table1", null, "cts", null, null, true, true, false);
        });
    }

    private void testUpdateTimestampColumnToNull(String tsType) throws Exception {
        final boolean isNanos = tsType.equals("TIMESTAMP_NS");

        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
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
            assertQueryNoLeakCheck("id\tstatus\tmts\tcts\n" +
                            "20\tHOHOHO\t2022-01-03T00:00:00.000000" + (isNanos ? "000" : "") + "Z\t2022-01-03T00:00:00.000000Z\n",
                    "table1", null, "cts", null, null, true, true, false);

            try (PreparedStatement statement = connection.prepareStatement("update table1 set status = ?, mts = ? where id = ?")) {
                statement.setString(1, "HAHAHA");
                statement.setNull(2, Types.TIMESTAMP);
                statement.setLong(3, 20L);

                statement.executeUpdate();
            }
            drainWalQueue();
            assertQueryNoLeakCheck("""
                            id\tstatus\tmts\tcts
                            20\tHAHAHA\t\t2022-01-03T00:00:00.000000Z
                            """,
                    "table1", null, "cts", null, null, true, true, false);
        });
    }

    private void testUpdateTimestampColumnToNullSentinel(String tsType) throws Exception {
        final boolean isNanos = tsType.equals("TIMESTAMP_NS");

        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
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
            assertQueryNoLeakCheck("id\tstatus\tmts\tcts\n" +
                            "20\tHOHOHO\t2022-01-03T00:00:00.000000" + (isNanos ? "000" : "") + "Z\t2022-01-03T00:00:00.000000Z\n",
                    "table1", null, "cts", null, null, true, true, false);

            try (PreparedStatement statement = connection.prepareStatement("update table1 set status = ?, mts = ? where id = ?")) {
                statement.setString(1, "HAHAHA");
                statement.setLong(2, Long.MIN_VALUE);
                statement.setLong(3, 20L);

                statement.executeUpdate();
            }
            drainWalQueue();
            assertQueryNoLeakCheck("""
                            id\tstatus\tmts\tcts
                            20\tHAHAHA\t\t2022-01-03T00:00:00.000000Z
                            """,
                    "table1", null, "cts", null, null, true, true, false);
        });
    }
}