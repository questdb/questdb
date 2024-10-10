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

package io.questdb.test.cutlass.pgwire;

import io.questdb.test.tools.TestUtils;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.postgresql.util.PSQLException;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
@SuppressWarnings("SqlNoDataSourceInspection")
public class PreparedStatementInvalidationTest extends BasePGTest {

    private final boolean walEnabled;

    public PreparedStatementInvalidationTest(LegacyMode legacyMode, WalMode walMode) {
        super(legacyMode);
        this.walEnabled = (walMode == WalMode.WITH_WAL);
    }

    @Parameterized.Parameters(name = "{0}, {1}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{
                {LegacyMode.MODERN, WalMode.WITH_WAL},
                {LegacyMode.MODERN, WalMode.NO_WAL},
                {LegacyMode.LEGACY, WalMode.WITH_WAL},
                {LegacyMode.LEGACY, WalMode.NO_WAL},
        });
    }

    @Test
    public void testSelectStarPreparedThenColNameChanges() throws Exception {
        Assume.assumeFalse(legacyMode); // Legacy code doesn't update result set shape
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (CallableStatement st1 = connection.prepareCall("create table y as (" +
                    "select timestamp_sequence(0, 1000000000) timestamp," +
                    " rnd_symbol('a','b',null) symbol1 " +
                    " from long_sequence(10)" +
                    ") timestamp (timestamp) partition by YEAR")) {
                st1.execute();
            }

            try (PreparedStatement select = connection.prepareStatement("select * from y")) {
                ResultSet rs0 = select.executeQuery();
                rs0.close();

                connection.prepareStatement("drop table y").execute();
                connection.prepareStatement("create table y as ( " +
                        " select " +
                        " timestamp_sequence('1970-01-01T02:30:00.000000Z', 1000000000L) timestamp, " +
                        " rnd_symbol('a','b',null) symbol2 " +
                        " from long_sequence(10)" +
                        ")").execute();

                mayDrainWalQueue();
                ResultSet rs1 = select.executeQuery();
                sink.clear();
                assertResultSet("timestamp[TIMESTAMP],symbol2[VARCHAR]\n" +
                        "1970-01-01 02:30:00.0,b\n" +
                        "1970-01-01 02:46:40.0,null\n" +
                        "1970-01-01 03:03:20.0,b\n" +
                        "1970-01-01 03:20:00.0,b\n" +
                        "1970-01-01 03:36:40.0,b\n" +
                        "1970-01-01 03:53:20.0,a\n" +
                        "1970-01-01 04:10:00.0,a\n" +
                        "1970-01-01 04:26:40.0,b\n" +
                        "1970-01-01 04:43:20.0,a\n" +
                        "1970-01-01 05:00:00.0,b\n", sink, rs1);
                rs1.close();
            }
        });
    }

    @Test
    public void testSelectStarPreparedThenColTypeChanges() throws Exception {
        Assume.assumeFalse(legacyMode); // Legacy code doesn't update result set shape
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (CallableStatement st1 = connection.prepareCall("create table y as (" +
                    "select timestamp_sequence(0, 1000000000) timestamp," +
                    " rnd_symbol('a','b',null) symbol1 " +
                    " from long_sequence(10)" +
                    ") timestamp (timestamp) partition by YEAR")) {
                st1.execute();
            }

            try (PreparedStatement select = connection.prepareStatement("select * from y")) {
                ResultSet rs0 = select.executeQuery();
                rs0.close();

                connection.prepareStatement("drop table y").execute();
                connection.prepareStatement("create table y as ( " +
                        " select " +
                        " timestamp_sequence('1970-01-01T02:30:00.000000Z', 1000000000L) timestamp, " +
                        " rnd_boolean symbol1" +
                        " from long_sequence(10)" +
                        ")").execute();

                mayDrainWalQueue();
                ResultSet rs1 = select.executeQuery();
                sink.clear();
                assertResultSet("timestamp[TIMESTAMP],symbol1[BIT]\n" +
                        "1970-01-01 02:30:00.0,false\n" +
                        "1970-01-01 02:46:40.0,false\n" +
                        "1970-01-01 03:03:20.0,false\n" +
                        "1970-01-01 03:20:00.0,true\n" +
                        "1970-01-01 03:36:40.0,true\n" +
                        "1970-01-01 03:53:20.0,true\n" +
                        "1970-01-01 04:10:00.0,true\n" +
                        "1970-01-01 04:26:40.0,false\n" +
                        "1970-01-01 04:43:20.0,false\n" +
                        "1970-01-01 05:00:00.0,false\n", sink, rs1);
                rs1.close();
            }
        });
    }

    @Test
    public void testSelectTwoColsPreparedThenColAdded() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (CallableStatement st1 = connection.prepareCall("create table y as (" +
                    "select timestamp_sequence(0, 1000000000) timestamp," +
                    " rnd_symbol('a','b',null) symbol1 " +
                    " from long_sequence(10)" +
                    ") timestamp (timestamp) partition by YEAR")) {
                st1.execute();
            }

            try (PreparedStatement select = connection.prepareStatement("select timestamp, symbol1 from y")) {
                ResultSet rs0 = select.executeQuery();
                rs0.close();

                connection.prepareStatement("drop table y").execute();
                connection.prepareStatement("create table y as ( " +
                        " select " +
                        " timestamp_sequence('1970-01-01T02:30:00.000000Z', 1000000000L) timestamp " +
                        " ,rnd_str('a','b','c', 'd', 'e', 'f',null) symbol2" +
                        " ,rnd_str('a','b',null) symbol1" +
                        " from long_sequence(10)" +
                        ")").execute();

                mayDrainWalQueue();
                ResultSet rs1 = select.executeQuery();
                sink.clear();
                assertResultSet("timestamp[TIMESTAMP],symbol1[VARCHAR]\n" +
                        "1970-01-01 02:30:00.0,null\n" +
                        "1970-01-01 02:46:40.0,b\n" +
                        "1970-01-01 03:03:20.0,a\n" +
                        "1970-01-01 03:20:00.0,b\n" +
                        "1970-01-01 03:36:40.0,b\n" +
                        "1970-01-01 03:53:20.0,a\n" +
                        "1970-01-01 04:10:00.0,null\n" +
                        "1970-01-01 04:26:40.0,b\n" +
                        "1970-01-01 04:43:20.0,b\n" +
                        "1970-01-01 05:00:00.0,a\n", sink, rs1);

                rs1.close();
            }
        });
    }

    @Test
    public void testUpdateAfterDropAndRecreate() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("create table update_after_drop(id long, val int, ts timestamp) timestamp(ts) partition by YEAR");
            }

            try (PreparedStatement statement = connection.prepareStatement("update update_after_drop set id = ?")) {
                statement.setLong(1, 42);
                statement.executeUpdate();
            }

            mayDrainWalQueue();

            try (Statement stmt = connection.createStatement()) {
                stmt.executeUpdate("drop table update_after_drop");
                stmt.executeUpdate("create table update_after_drop(id long, val int, ts timestamp) timestamp(ts) partition by YEAR");
            }

            mayDrainWalQueue();

            try (PreparedStatement stmt = connection.prepareStatement("update update_after_drop set id = ?")) {
                stmt.setLong(1, 42);
                stmt.executeUpdate();
            }
        });
    }

    @Test
    public void testUpdateAfterDroppingColumnNotUsedByTheUpdate() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("create table update_after_drop(id long, val int, ts timestamp) timestamp(ts) partition by YEAR");
            }

            try (PreparedStatement statement = connection.prepareStatement("update update_after_drop set id = ?")) {
                statement.setInt(1, 42);
                statement.executeUpdate();
            }

            mayDrainWalQueue();

            try (Statement stmt = connection.createStatement()) {
                stmt.executeUpdate("alter table update_after_drop drop column val");
            }

            mayDrainWalQueue();

            try (PreparedStatement stmt = connection.prepareStatement("update update_after_drop set id = ?")) {
                stmt.setLong(1, 42);
                stmt.executeUpdate();
            }

            mayDrainWalQueue();
        });
    }

    @Test
    public void testUpdateAfterDroppingColumnUsedByTheUpdate() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("create table update_after_drop(id long, val int, ts timestamp) timestamp(ts) partition by YEAR");
            }

            try (PreparedStatement statement = connection.prepareStatement("update update_after_drop set id = ?")) {
                statement.setLong(1, 42);
                statement.executeUpdate();
            }

            try (Statement stmt = connection.createStatement()) {
                stmt.executeUpdate("alter table update_after_drop drop column id");
            }

            try (PreparedStatement stmt = connection.prepareStatement("update update_after_drop set id = ?")) {
                stmt.setLong(1, 42);
                stmt.executeUpdate();
                assertExceptionNoLeakCheck("id column was dropped, the UPDATE should have failed");
            } catch (PSQLException e) {
                TestUtils.assertContains(e.getMessage(), "Invalid column: id");
            }
        });
    }

    private void mayDrainWalQueue() {
        if (walEnabled) {
            drainWalQueue();
        }
    }
}
