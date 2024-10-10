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

import io.questdb.PropertyKey;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.postgresql.util.PSQLException;

import java.sql.*;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@SuppressWarnings("SqlNoDataSourceInspection")
public class PreparedStatementInvalidationTest extends BasePGTest {

    private final boolean walEnabled;

    public PreparedStatementInvalidationTest(WalMode walMode) {
        super(LegacyMode.MODERN);
        this.walEnabled = (walMode == WalMode.WITH_WAL);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{{WalMode.WITH_WAL}, {WalMode.NO_WAL}});
    }

    @Before
    public void setUp() {
        node1.setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, walEnabled);
        node1.setProperty(PropertyKey.DEV_MODE_ENABLED, true);
    }

    @Test
    public void testInsertAfterDropAndRecreate() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
                statement.execute("create table insert_after_drop(id long, val int, ts timestamp) timestamp(ts) partition by YEAR");
            }
            mayDrainWalQueue();

            try (PreparedStatement insertStatement = connection.prepareStatement("insert into insert_after_drop values (?, 0, '1990-01-01')")) {
                insertStatement.setLong(1, 42);
                Assert.assertEquals(1, insertStatement.executeUpdate());
                mayDrainWalQueue();

                try (Statement stmt = connection.createStatement()) {
                    stmt.execute("drop table insert_after_drop");
                    stmt.execute("create table insert_after_drop(id long, val int, ts timestamp) timestamp(ts) partition by YEAR");
                }
                mayDrainWalQueue();

                insertStatement.setLong(1, 43);
                Assert.assertEquals(1, insertStatement.executeUpdate());
                mayDrainWalQueue();

                // assert it's actually written
                assertSql("id\tval\tts\n" +
                                "43\t0\t1990-01-01T00:00:00.000000Z\n",
                        "select * from insert_after_drop");
            }
        });
    }

    @Test
    public void testInsertAllAfterColDropped() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
                statement.execute("create table insert_after_drop(id long, val int, ts timestamp) timestamp(ts) partition by YEAR");
            }
            mayDrainWalQueue();

            try (PreparedStatement insertStatement = connection.prepareStatement("insert into insert_after_drop values (?, 0, '1990-01-01')")) {
                insertStatement.setLong(1, 42);
                Assert.assertEquals(1, insertStatement.executeUpdate());

                mayDrainWalQueue();

                try (Statement stmt = connection.createStatement()) {
                    stmt.execute("alter table insert_after_drop drop column val");
                }
                mayDrainWalQueue();

                insertStatement.setLong(1, 43);
                try {
                    insertStatement.executeUpdate();
                    Assert.fail("val column was dropped, the INSERT should have failed");
                } catch (SQLException e) {
                    TestUtils.assertContains(e.getMessage(), "row value count does not match column count [expected=2, actual=3, tuple=1]");
                }
            }
        });
    }

    @Test
    public void testInsertAllAfterColNameChange() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
                statement.execute("create table insert_after_drop(id long, val int, ts timestamp) timestamp(ts) partition by YEAR");
            }
            mayDrainWalQueue();

            try (PreparedStatement insertStatement = connection.prepareStatement("insert into insert_after_drop values (?, 0, '1990-01-01')")) {
                insertStatement.setLong(1, 42);
                Assert.assertEquals(1, insertStatement.executeUpdate());

                mayDrainWalQueue();

                try (Statement stmt = connection.createStatement()) {
                    stmt.execute("drop table insert_after_drop");
                    stmt.execute("create table insert_after_drop(id long, val2 int, ts timestamp) timestamp(ts) partition by YEAR");
                }
                mayDrainWalQueue();

                insertStatement.setLong(1, 43);
                Assert.assertEquals(1, insertStatement.executeUpdate());
                mayDrainWalQueue();

                // assert it's actually written
                assertSql("id\tval2\tts\n" +
                                "43\t0\t1990-01-01T00:00:00.000000Z\n",
                        "select * from insert_after_drop");
            }
        });
    }

    @Test
    public void testInsertSpecificAfterColDropped() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
                statement.execute("create table insert_after_drop(id long, val int, ts timestamp) timestamp(ts) partition by YEAR");
            }
            mayDrainWalQueue();

            try (PreparedStatement insertStatement = connection.prepareStatement("insert into insert_after_drop (id, ts) values (?, '1990-01-01')")) {
                insertStatement.setLong(1, 42);
                Assert.assertEquals(1, insertStatement.executeUpdate());

                mayDrainWalQueue();

                try (Statement stmt = connection.createStatement()) {
                    stmt.execute("alter table insert_after_drop drop column val");
                }
                mayDrainWalQueue();

                insertStatement.setLong(1, 43);
                Assert.assertEquals(1, insertStatement.executeUpdate());
                mayDrainWalQueue();

                // assert it's actually written
                assertSql("id\tts\n" +
                                "42\t1990-01-01T00:00:00.000000Z\n" +
                                "43\t1990-01-01T00:00:00.000000Z\n",
                        "select * from insert_after_drop");
            }
        });
    }

    @Test
    public void testPreparedStatement_selectScenario() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.prepareStatement("create table x as" +
                    " (select 2 id, 'foobar' str, timestamp_sequence(1,10000) ts from long_sequence(1))" +
                    " timestamp(ts) partition by hour"
            ).execute();
            drainWalQueue();
            try (PreparedStatement ps = connection.prepareStatement("x where id=?")) {
                ps.setInt(1, 2);
                try (ResultSet resultSet = ps.executeQuery()) {
                    sink.clear();
                    assertResultSet(
                            "id[INTEGER],str[VARCHAR],ts[TIMESTAMP]\n" +
                                    "2,foobar,1970-01-01 00:00:00.000001\n",
                            sink,
                            resultSet
                    );
                }

                //drop a column
                try (PreparedStatement stmt = connection.prepareStatement("alter table x drop column str;")) {
                    stmt.execute();
                }
                drainWalQueue();
                // Query the data once again - this time the schema is different,
                ps.setInt(1, 2);
                try (ResultSet rs = ps.executeQuery()) {
                    sink.clear();
                    assertResultSet(
                            "id[INTEGER],ts[TIMESTAMP]\n" +
                                    "2,1970-01-01 00:00:00.000001\n",
                            sink, rs
                    );
                }

                //add a column
                try (Statement stmt = connection.createStatement()) {
                    stmt.execute("alter table x add column str2 varchar");
                    stmt.execute("update x set str2 = id::varchar");
                }
                drainWalQueue();

                ps.setInt(1, 2);
                try (ResultSet rs = ps.executeQuery()) {
                    sink.clear();
                    assertResultSet(
                            "id[INTEGER],ts[TIMESTAMP],str2[VARCHAR]\n" +
                                    "2,1970-01-01 00:00:00.000001,2\n",
                            sink, rs
                    );
                }

                //add and remove a column
                try (Statement stmt = connection.createStatement()) {
                    stmt.execute("alter table x add column str3 varchar");
                    stmt.execute("update x set str3 = concat(str2, '_new')");
                    stmt.execute("alter table x drop column str2");
                }
                drainWalQueue();

                // check it does not use a stale column name
                ps.setInt(1, 2);
                try (ResultSet rs = ps.executeQuery()) {
                    sink.clear();
                    assertResultSet(
                            "id[INTEGER],ts[TIMESTAMP],str3[VARCHAR]\n" +
                                    "2,1970-01-01 00:00:00.000001,2_new\n",
                            sink, rs
                    );
                }
            }
        });
    }

    @Test
    public void testSelectPreparedStatement_columnWithBindVariableDropped() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.prepareStatement("create table x as" +
                    " (select 2 id, 'foobar' str, timestamp_sequence(1,10000) ts from long_sequence(1))" +
                    " timestamp(ts) partition by hour"
            ).execute();
            drainWalQueue();
            try (PreparedStatement ps = connection.prepareStatement("x where id=?")) {
                ps.setInt(1, 2);
                try (ResultSet resultSet = ps.executeQuery()) {
                    sink.clear();
                    assertResultSet(
                            "id[INTEGER],str[VARCHAR],ts[TIMESTAMP]\n" +
                                    "2,foobar,1970-01-01 00:00:00.000001\n",
                            sink,
                            resultSet
                    );
                }

                //drop a column
                try (PreparedStatement stmt = connection.prepareStatement("alter table x drop column id;")) {
                    stmt.execute();
                }
                drainWalQueue();

                ps.setInt(1, 2);
                try (ResultSet shouldNotBeCreated = ps.executeQuery()) {
                    Assert.fail("id column was dropped, the query should fail");
                } catch (SQLException e) {
                    TestUtils.assertContains(e.getMessage(), "Invalid column: id");
                }
            }
        });
    }

    @Test
    public void testSelectStarPreparedThenColDropped() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (CallableStatement st1 = connection.prepareCall("create table y as (" +
                    "select timestamp_sequence(0, 1000000000) timestamp," +
                    " rnd_symbol('a','b',null) symbol1 " +
                    " from long_sequence(10)" +
                    ") timestamp (timestamp) partition by YEAR")) {
                st1.execute();
            }

            try (PreparedStatement select = connection.prepareStatement("select * from y");
                 Statement statement = connection.createStatement()
            ) {
                ResultSet rs0 = select.executeQuery();
                rs0.close();

                statement.executeUpdate("alter table y drop column symbol1");

                mayDrainWalQueue();
                ResultSet rs1 = select.executeQuery();
                sink.clear();
                assertResultSet("timestamp[TIMESTAMP]\n" +
                        "1970-01-01 00:00:00.0\n" +
                        "1970-01-01 00:16:40.0\n" +
                        "1970-01-01 00:33:20.0\n" +
                        "1970-01-01 00:50:00.0\n" +
                        "1970-01-01 01:06:40.0\n" +
                        "1970-01-01 01:23:20.0\n" +
                        "1970-01-01 01:40:00.0\n" +
                        "1970-01-01 01:56:40.0\n" +
                        "1970-01-01 02:13:20.0\n" +
                        "1970-01-01 02:30:00.0\n", sink, rs1);
                rs1.close();
            }
        });
    }

    @Test
    public void testSelectStarPreparedThenColNameChanges() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (CallableStatement st1 = connection.prepareCall("create table y as (" +
                    "select timestamp_sequence(0, 1000000000) timestamp," +
                    " rnd_symbol('a','b',null) symbol1 " +
                    " from long_sequence(10)" +
                    ") timestamp (timestamp) partition by YEAR")) {
                st1.execute();
            }

            try (PreparedStatement select = connection.prepareStatement("select * from y");
                 Statement statement = connection.createStatement()
            ) {
                ResultSet rs0 = select.executeQuery();
                rs0.close();

                statement.executeUpdate("drop table y");
                statement.executeUpdate("create table y as ( " +
                        " select " +
                        " timestamp_sequence('1970-01-01T02:30:00.000000Z', 1000000000L) timestamp, " +
                        " rnd_symbol('a','b',null) symbol2 " +
                        " from long_sequence(10)" +
                        ")");

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
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (CallableStatement st1 = connection.prepareCall("create table y as (" +
                    "select timestamp_sequence(0, 1000000000) timestamp," +
                    " rnd_symbol('a','b',null) symbol1 " +
                    " from long_sequence(10)" +
                    ") timestamp (timestamp) partition by YEAR")) {
                st1.execute();
            }

            try (PreparedStatement select = connection.prepareStatement("select * from y");
                 Statement statement = connection.createStatement()
            ) {
                ResultSet rs0 = select.executeQuery();
                rs0.close();

                statement.executeUpdate("drop table y");
                statement.executeUpdate("create table y as ( " +
                        " select " +
                        " timestamp_sequence('1970-01-01T02:30:00.000000Z', 1000000000L) timestamp, " +
                        " rnd_boolean symbol1" +
                        " from long_sequence(10)" +
                        ")");

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

            try (PreparedStatement select = connection.prepareStatement("select timestamp, symbol1 from y");
                 Statement statement = connection.createStatement()
            ) {
                ResultSet rs0 = select.executeQuery();
                rs0.close();

                statement.executeUpdate("drop table y");
                statement.executeUpdate("create table y as ( " +
                        " select " +
                        " timestamp_sequence('1970-01-01T02:30:00.000000Z', 1000000000L) timestamp " +
                        " ,rnd_str('a','b','c', 'd', 'e', 'f',null) symbol2" +
                        " ,rnd_str('a','b',null) symbol1" +
                        " from long_sequence(10)" +
                        ")");
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
    public void testSelectTwoColsPreparedThenColDropped() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (CallableStatement st1 = connection.prepareCall("create table y as (" +
                    "select timestamp_sequence(0, 1000000000) timestamp," +
                    " rnd_symbol('a','b',null) symbol1 " +
                    " from long_sequence(10)" +
                    ") timestamp (timestamp) partition by YEAR")) {
                st1.execute();
            }

            try (PreparedStatement select = connection.prepareStatement("select timestamp, symbol1 from y");
                 Statement statement = connection.createStatement()
            ) {
                ResultSet rs0 = select.executeQuery();
                rs0.close();

                statement.executeUpdate("alter table y drop column symbol1");
                mayDrainWalQueue();
                try (ResultSet ignored = select.executeQuery()) {
                    Assert.fail();
                } catch (Exception e) {
                    assertMessageContains(e, "Invalid column: symbol1");
                }
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
                assertMessageContains(e, "Invalid column: id");
            }
        });
    }

    private void assertMessageContains(Exception e, String expectedSubstring) {
        String message = e.getMessage();
        assertTrue(
                String.format("Exception message doesn't contain '%s'. Actual message: '%s'", expectedSubstring, message),
                message.contains(expectedSubstring)
        );
    }

    private void mayDrainWalQueue() {
        if (walEnabled) {
            drainWalQueue();
        }
    }
}
