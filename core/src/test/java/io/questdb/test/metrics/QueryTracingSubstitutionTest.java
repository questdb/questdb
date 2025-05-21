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

package io.questdb.test.metrics;

import io.questdb.ServerMain;
import io.questdb.std.Misc;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import org.junit.Assert;
import org.junit.ComparisonFailure;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Properties;

import static io.questdb.test.cutlass.pgwire.BasePGTest.assertResultSet;
import static io.questdb.test.cutlass.pgwire.BasePGTest.printToSink;
import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

@SuppressWarnings("SqlDialectInspection")
public class QueryTracingSubstitutionTest extends AbstractBootstrapTest {

    @Test
    public void testSubstitutionOfBindVariables() throws Exception {
        assertMemoryLeak(() -> {
            createDummyConfiguration("pg.select.cache.enabled=true", "query.tracing.enabled=true", "query.tracing.bind.variable.substitution.enabled=true");
            try (final ServerMain serverMain = TestServerMain.createWithManualWalRun(getServerMainArgs())) {
                serverMain.start();

                try (Connection connection = getConnection(serverMain)) {
                    try (final PreparedStatement stmt = connection.prepareStatement("SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::ipv4, ?::varchar, ?, ?::long256, ?;")) {
                        stmt.setBoolean(1, true);
                        stmt.setByte(2, (byte) 111);
                        stmt.setDouble(3, 123.456);
                        stmt.setFloat(4, (float) 123.456);
                        stmt.setInt(5, 987654);
                        stmt.setLong(6, 987654L);
                        stmt.setShort(7, (short) 11111);
                        stmt.setString(8, "te'st");
                        stmt.setBytes(9, new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF});
                        stmt.setString(10, "123.123.123.123");
                        stmt.setString(11, "te'st2");
                        stmt.setDate(12, new java.sql.Date(0));
                        stmt.setString(13, "0xDEADBEEFDEADBEEFDEADBEEFDEADBEEF");
                        stmt.setTimestamp(14, Timestamp.from(Instant.EPOCH));


                        final ResultSet ignore = stmt.executeQuery();
                        ignore.close();
                    }

                    int sleepMillis = 100;
                    while (true) {
                        //noinspection BusyWait
                        Thread.sleep(sleepMillis);
                        try {
                            try (final PreparedStatement stmt = connection.prepareStatement("SELECT query_text FROM _query_trace;")) {
                                try (final ResultSet resultSet = stmt.executeQuery()) {
                                    StringSink sink = Misc.getThreadLocalSink();
                                    printToSink(sink, resultSet, null);
                                    String[] parts = sink.toString().split("\n");
                                    boolean matched = false;
                                    for (int i = 0, n = parts.length; i < n; i++) {
                                        try {
                                            Assert.assertEquals("SELECT true::boolean, 111::short, 123.456::double, 123.456::float, 987654::int, 987654::long, 11111::short, 'te''st'::string, '\\xDEADBEEF'::binary, '123.123.123.123'::string::ipv4, 'te''st2'::string::varchar, '1970-01-01 +01'::string, '0xDEADBEEFDEADBEEFDEADBEEFDEADBEEF'::string::long256, '1970-01-01 01:00:00+01'::string",
                                                    parts[i]);
                                            matched = true;
                                        } catch (ComparisonFailure ignored) {
                                        }
                                    }
                                    Assert.assertTrue(matched);
                                    break;
                                }
                            }
                        } catch (AssertionError e) {
                            if (sleepMillis >= 6400) {
                                throw e;
                            }
                            sleepMillis *= 2;
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testWeCanLogTimedOutQueries() throws Exception {
        assertMemoryLeak(() -> {
            createDummyConfiguration("pg.select.cache.enabled=true", "query.tracing.enabled=true", "query.tracing.bind.variable.substitution.enabled=true", "query.timeout=1");
            try (final ServerMain serverMain = TestServerMain.createWithManualWalRun(getServerMainArgs())) {
                serverMain.start();

                try (Connection connection = getConnection(serverMain)) {
                    try (final PreparedStatement stmt = connection.prepareStatement("CREATE TABLE foo AS (SELECT x FROM long_sequence(10_000_000));")) {
                        stmt.execute();
                    }

                    try (final PreparedStatement stmt = connection.prepareStatement("select avg(x) FROM foo;")) {
                        try (final ResultSet resultSet = stmt.executeQuery()) {
                            assertResultSet("abc",
                                    Misc.getThreadLocalSink(),
                                    resultSet);
                        } catch (PSQLException ex) {
                            Assert.assertTrue(ex.getMessage().contains("timeout, query aborted"));
                        }
                    }

                    int sleepMillis = 100;
                    while (true) {
                        //noinspection BusyWait
                        Thread.sleep(sleepMillis);
                        try {
                            try (final PreparedStatement stmt = connection.prepareStatement("_query_trace;")) {
                                try (final ResultSet resultSet = stmt.executeQuery()) {
                                    StringSink sink = Misc.getThreadLocalSink();
                                    printToSink(sink, resultSet, null);
                                    String output = sink.toString();
                                    Assert.assertTrue(output.contains("select avg(x) FROM foo"));
                                    Assert.assertTrue(output.contains("admin,timeout, query aborted"));
                                    return;
                                }
                            }
                        } catch (AssertionError e) {
                            if (sleepMillis >= 6400) {
                                throw e;
                            }
                            sleepMillis *= 2;
                        }
                    }
                }
            }
        });
    }

    private static Connection getConnection(ServerMain serverMain) throws SQLException {
        final int port = serverMain.getConfiguration().getPGWireConfiguration().getBindPort();
        final Properties properties = new Properties();
        properties.setProperty("user", "admin");
        properties.setProperty("password", "quest");
        final String url = String.format("jdbc:postgresql://127.0.0.1:%d/qdb", port);
        return DriverManager.getConnection(url, properties);
    }
}