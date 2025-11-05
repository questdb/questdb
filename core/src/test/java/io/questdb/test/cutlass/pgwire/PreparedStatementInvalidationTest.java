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
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Pattern;

import static org.junit.Assert.assertTrue;

@SuppressWarnings("SqlNoDataSourceInspection")
public class PreparedStatementInvalidationTest extends BasePGTest {

    private final boolean walEnabled;

    public PreparedStatementInvalidationTest() {
        this.walEnabled = TestUtils.isWal();
    }

    @Before
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, walEnabled);
        node1.setProperty(PropertyKey.DEV_MODE_ENABLED, true);
    }

    @Test
    public void testChangeBindVariableType_insert() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
                statement.execute("create table change_var_type(id long, val int, ts timestamp) timestamp(ts) partition by YEAR");
            }
            mayDrainWalQueue();

            try (PreparedStatement insertStatement = connection.prepareStatement("insert into change_var_type (id, val, ts) values (?, 0, '1990-01-01')")) {
                insertStatement.setObject(1, 42);
                Assert.assertEquals(1, insertStatement.executeUpdate());
                mayDrainWalQueue();

                insertStatement.setObject(1, "bad, bad value");
                try {
                    insertStatement.executeUpdate();
                    Assert.fail("bad value was set, the INSERT should have failed");
                } catch (PSQLException e) {
                    assertMessageMatches(e, "inconvertible value");
                }
            }
        });
    }

    @Test
    public void testChangeBindVariableType_select() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
                statement.execute("create table change_var_type(id long, val int, ts timestamp) timestamp(ts) partition by YEAR");
                statement.execute("insert into change_var_type (id, val, ts) values (42, 0, '1990-01-01')");
            }
            mayDrainWalQueue();

            try (PreparedStatement selectStatement = connection.prepareStatement("select * from change_var_type where id = ?")) {
                selectStatement.setObject(1, 42L);
                try (ResultSet rs = selectStatement.executeQuery()) {
                    sink.clear();
                    assertResultSet(
                            """
                                    id[BIGINT],val[INTEGER],ts[TIMESTAMP]
                                    42,0,1990-01-01 00:00:00.0
                                    """,
                            sink,
                            rs
                    );
                }

                // TODO: funny behaviour when the string is too small
                //       it might indicate a bug
                selectStatement.setObject(1, "bad, bad value");
                try {
                    selectStatement.executeQuery();
                    Assert.fail("bad value was set, the SELECT should have failed");
                } catch (PSQLException e) {
                    assertMessageMatches(e, "inconvertible value");
                }
            }

        });
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
                assertSql("""
                                id\tval\tts
                                43\t0\t1990-01-01T00:00:00.000000Z
                                """,
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
                    assertMessageMatches(e, "row value count does not match column count \\[expected=2, actual=3, tuple=1\\]");
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
                assertSql("""
                                id\tval2\tts
                                43\t0\t1990-01-01T00:00:00.000000Z
                                """,
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
                assertSql("""
                                id\tts
                                42\t1990-01-01T00:00:00.000000Z
                                43\t1990-01-01T00:00:00.000000Z
                                """,
                        "select * from insert_after_drop");
            }
        });
    }

    @Test
    public void testInsertSpecificAfterColNameChange() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
                statement.execute("create table insert_after_drop(id long, val int, ts timestamp) timestamp(ts) partition by YEAR");
            }
            mayDrainWalQueue();

            try (PreparedStatement insertStatement = connection.prepareStatement("insert into insert_after_drop (id, val, ts) values (?, 0, '1990-01-01')")) {
                insertStatement.setLong(1, 42);
                Assert.assertEquals(1, insertStatement.executeUpdate());

                mayDrainWalQueue();

                try (Statement stmt = connection.createStatement()) {
                    stmt.execute("alter table insert_after_drop drop column val");
                    stmt.execute("alter table insert_after_drop add column val2 int");
                }
                mayDrainWalQueue();

                insertStatement.setLong(1, 43);
                try {
                    insertStatement.executeUpdate();
                    Assert.fail("val column was dropped, the INSERT should have failed");
                } catch (SQLException e) {
                    assertMessageMatches(e, "Invalid column: val");
                }
            }
        });
    }

    @Test
    public void testInsertWhileConcurrentlyAlteringTable_preparedStatement() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> executeStatementWhileConcurrentlyChangingSchema(
                connection,
                "ALTER TABLE tango RENAME COLUMN x TO y",
                "ALTER TABLE tango RENAME COLUMN y TO x",
                "insert rows",
                null, () -> {
                    try (PreparedStatement s = connection.prepareStatement("INSERT INTO tango VALUES (42)")) {
                        s.execute();
                    }
                }));
    }

    @Test
    public void testInsertWhileConcurrentlyAlteringTable_preparedStatementReused() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement s = connection.prepareStatement("INSERT INTO tango VALUES (42)")) {
                executeStatementWhileConcurrentlyChangingSchema(connection,
                        "ALTER TABLE tango RENAME COLUMN x TO y",
                        "ALTER TABLE tango RENAME COLUMN y TO x",
                        "insert rows", null, s::execute);
            }
        });
    }

    @Test
    public void testInsertWhileConcurrentlyAlteringTable_simpleStatement() throws Exception {
        assertWithPgServer(
                CONN_AWARE_ALL,
                (connection, binary, mode, port) ->
                        executeStatementWhileConcurrentlyChangingSchema(
                                connection,
                                "ALTER TABLE tango RENAME COLUMN x TO y",
                                "ALTER TABLE tango RENAME COLUMN y TO x",
                                "insert rows",
                                null, () -> {
                                    try (Statement s = connection.createStatement()) {
                                        s.executeUpdate("INSERT INTO tango VALUES (42)");
                                    }
                                }));
    }

    @Test
    public void testInsertWhileConcurrentlyRecreatingTable_preparedStatement() throws Exception {
        assertWithPgServer(
                CONN_AWARE_ALL,
                (connection, binary, mode, port) ->
                        executeStatementWhileConcurrentlyChangingSchema(
                                connection,
                                "DROP TABLE tango; CREATE TABLE tango AS (SELECT x AS y from long_sequence(10));",
                                "DROP TABLE tango; CREATE TABLE tango AS (SELECT x from long_sequence(10));",
                                "insert rows", "table does not exist \\[table=tango\\]",
                                () -> {
                                    try (PreparedStatement s = connection.prepareStatement("INSERT INTO tango VALUES (42)")) {
                                        s.execute();
                                    }
                                }));
    }

    @Test
    public void testInsertWhileConcurrentlyRecreatingTable_preparedStatementReused() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement s = connection.prepareStatement("INSERT INTO tango VALUES (42)")) {
                executeStatementWhileConcurrentlyChangingSchema(connection,
                        "DROP TABLE tango; CREATE TABLE tango AS (SELECT x AS y from long_sequence(10));",
                        "DROP TABLE tango; CREATE TABLE tango AS (SELECT x from long_sequence(10));",
                        "insert rows", "table does not exist \\[table=tango\\]",
                        s::execute);
            }
        });
    }

    @Test
    public void testInsertWhileConcurrentlyRecreatingTable_simpleStatement() throws Exception {
        assertWithPgServer(
                CONN_AWARE_ALL,
                (connection, binary, mode, port) ->
                        executeStatementWhileConcurrentlyChangingSchema(
                                connection,
                                "DROP TABLE tango; CREATE TABLE tango AS (SELECT x AS y from long_sequence(10));",
                                "DROP TABLE tango; CREATE TABLE tango AS (SELECT x from long_sequence(10));",
                                "insert rows", "table does not exist \\[table=tango\\]",
                                () -> {
                                    try (Statement s = connection.createStatement()) {
                                        s.executeUpdate("INSERT INTO tango VALUES (42)");
                                    }
                                }));
    }

    @Test
    public void testPreparedStatementErrorConsistency() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            execute("create table abc(x double, y double, t timestamp) timestamp(t)");
            try (PreparedStatement ps = connection.prepareStatement("select y from abc")) {
                for (int i = 0; i < 10; i++) {
                    ps.execute();
                }

                Statement statement = connection.createStatement();
                statement.execute("alter table abc drop column y");

                for (int i = 0; i < 10; i++) {
                    try {
                        ps.execute();
                        Assert.fail();
                    } catch (SQLException e) {
                        TestUtils.assertEquals(
                                "ERROR: Invalid column: y\n" +
                                        "  Position: 8",
                                e.getMessage()
                        );
                    }
                }
            }
        });
    }

    @Test
    @Ignore("Statement executed at parse time fails when captured as server-side prepared statement and reused")
    public void testRepeatedDropCreate() throws Exception {
        Assume.assumeFalse(walEnabled); // no partitioned tables here
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            String create = "CREATE TABLE tango AS (SELECT x FROM long_sequence(10))";
            String drop = "DROP TABLE tango";
            try (Statement s = connection.createStatement()) {
                execute(create);
                s.execute(drop);
                execute(create);
                s.execute(drop);
            }
        });
    }

    @Test
    public void testSelectAllAfterConcurrentColAddDrop() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
                statement.execute("create table select_after_drop(id long, val int, ts timestamp) timestamp(ts) partition by YEAR");
            }
            mayDrainWalQueue();

            try (PreparedStatement insertStatement = connection.prepareStatement("insert into select_after_drop values (?, 0, '1990-01-01')")) {
                insertStatement.setLong(1, 42);
                Assert.assertEquals(1, insertStatement.executeUpdate());
            }
            mayDrainWalQueue();

            CyclicBarrier barrier = new CyclicBarrier(2);
            AtomicReference<Exception> exception = new AtomicReference<>();
            new Thread(() -> {
                try {
                    while (barrier.getNumberWaiting() == 0) {
                        try (Statement stmt = connection.createStatement()) {
                            stmt.execute("alter table select_after_drop add column val2 int");
                        }
                        mayDrainWalQueue();
                        try (Statement stmt = connection.createStatement()) {
                            stmt.execute("alter table select_after_drop drop column val2");
                        }
                        mayDrainWalQueue();
                    }
                } catch (SQLException e) {
                    exception.set(e);
                } finally {
                    // We have to clear thread locals *before* awaiting on the barrier.
                    // Otherwise, the main test thread might be unblocked before we
                    // clean up the thread locals and we get a false memory leak failure.
                    Path.clearThreadLocals();
                    try {
                        barrier.await();
                    } catch (InterruptedException | BrokenBarrierException e) {
                        exception.compareAndSet(null, e);
                    }
                }
            }).start();

            long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
            try (PreparedStatement selectStatement = connection.prepareStatement("select * from select_after_drop")) {
                do {
                    try (ResultSet rs = selectStatement.executeQuery()) {
                        sink.clear();
                        ResultSetMetaData metaData = rs.getMetaData();
                        String expected = null;
                        if (metaData.getColumnCount() == 3) {
                            expected = """
                                    id[BIGINT],val[INTEGER],ts[TIMESTAMP]
                                    42,0,1990-01-01 00:00:00.0
                                    """;
                        } else if (metaData.getColumnCount() == 4) {
                            expected = """
                                    id[BIGINT],val[INTEGER],ts[TIMESTAMP],val2[INTEGER]
                                    42,0,1990-01-01 00:00:00.0,null
                                    """;
                        } else {
                            Assert.fail("Unexpected column count: " + metaData.getColumnCount());
                        }
                        assertResultSet(
                                expected,
                                sink,
                                rs
                        );
                    } catch (SQLException e) {
                        // ignore this error, JDBC driver will retry the query only once
                        if (!e.getMessage().contains("ERROR: cached plan must not change result type")) {
                            throw e;
                        }
                    }
                } while (System.nanoTime() < deadlineNanos);
            } finally {
                barrier.await();
            }

            final Exception ex = exception.get();
            if (ex != null) {
                throw ex;
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
                            """
                                    id[INTEGER],str[VARCHAR],ts[TIMESTAMP]
                                    2,foobar,1970-01-01 00:00:00.000001
                                    """,
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
                try (ResultSet ignored = ps.executeQuery()) {
                    Assert.fail("id column was dropped, the query should fail");
                } catch (SQLException e) {
                    assertMessageMatches(e, "Invalid column: id");
                }
            }
        });
    }

    @Test
    public void testSelectPreparedStatement_scenario() throws Exception {
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
                            """
                                    id[INTEGER],str[VARCHAR],ts[TIMESTAMP]
                                    2,foobar,1970-01-01 00:00:00.000001
                                    """,
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
                            """
                                    id[INTEGER],ts[TIMESTAMP]
                                    2,1970-01-01 00:00:00.000001
                                    """,
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
                            """
                                    id[INTEGER],ts[TIMESTAMP],str2[VARCHAR]
                                    2,1970-01-01 00:00:00.000001,2
                                    """,
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
                            """
                                    id[INTEGER],ts[TIMESTAMP],str3[VARCHAR]
                                    2,1970-01-01 00:00:00.000001,2_new
                                    """,
                            sink, rs
                    );
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
                assertResultSet("""
                        timestamp[TIMESTAMP]
                        1970-01-01 00:00:00.0
                        1970-01-01 00:16:40.0
                        1970-01-01 00:33:20.0
                        1970-01-01 00:50:00.0
                        1970-01-01 01:06:40.0
                        1970-01-01 01:23:20.0
                        1970-01-01 01:40:00.0
                        1970-01-01 01:56:40.0
                        1970-01-01 02:13:20.0
                        1970-01-01 02:30:00.0
                        """, sink, rs1);
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
                assertResultSet("""
                        timestamp[TIMESTAMP],symbol2[VARCHAR]
                        1970-01-01 02:30:00.0,b
                        1970-01-01 02:46:40.0,null
                        1970-01-01 03:03:20.0,b
                        1970-01-01 03:20:00.0,b
                        1970-01-01 03:36:40.0,b
                        1970-01-01 03:53:20.0,a
                        1970-01-01 04:10:00.0,a
                        1970-01-01 04:26:40.0,b
                        1970-01-01 04:43:20.0,a
                        1970-01-01 05:00:00.0,b
                        """, sink, rs1);
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
                assertResultSet("""
                        timestamp[TIMESTAMP],symbol1[BIT]
                        1970-01-01 02:30:00.0,false
                        1970-01-01 02:46:40.0,false
                        1970-01-01 03:03:20.0,false
                        1970-01-01 03:20:00.0,true
                        1970-01-01 03:36:40.0,true
                        1970-01-01 03:53:20.0,true
                        1970-01-01 04:10:00.0,true
                        1970-01-01 04:26:40.0,false
                        1970-01-01 04:43:20.0,false
                        1970-01-01 05:00:00.0,false
                        """, sink, rs1);
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
                assertResultSet("""
                        timestamp[TIMESTAMP],symbol1[VARCHAR]
                        1970-01-01 02:30:00.0,null
                        1970-01-01 02:46:40.0,b
                        1970-01-01 03:03:20.0,a
                        1970-01-01 03:20:00.0,b
                        1970-01-01 03:36:40.0,b
                        1970-01-01 03:53:20.0,a
                        1970-01-01 04:10:00.0,null
                        1970-01-01 04:26:40.0,b
                        1970-01-01 04:43:20.0,b
                        1970-01-01 05:00:00.0,a
                        """, sink, rs1);

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
                    assertMessageMatches(e, "Invalid column: symbol1");
                }
            }
        });
    }

    @Test
    public void testSelectWhileConcurrentlyAlteringTable_preparedStatement() throws Exception {
        assertWithPgServer(
                CONN_AWARE_ALL,
                (connection, binary, mode, port) ->
                        executeStatementWhileConcurrentlyChangingSchema(
                                connection,
                                "ALTER TABLE tango RENAME COLUMN x TO y",
                                "ALTER TABLE tango RENAME COLUMN y TO x",
                                "query table",
                                "Invalid column: y", () -> {
                                    try (PreparedStatement s = connection.prepareStatement("SELECT y FROM tango")) {
                                        ResultSet rs = s.executeQuery();
                                        int rowCount = 0;
                                        while (rs.next()) {
                                            rowCount++;
                                        }
                                        Assert.assertEquals(10, rowCount);
                                    }
                                }));
    }

    @Test
    public void testSelectWhileConcurrentlyAlteringTable_preparedStatementReused() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement s = connection.prepareStatement("SELECT y FROM tango")) {
                executeStatementWhileConcurrentlyChangingSchema(connection,
                        "ALTER TABLE tango RENAME COLUMN x TO y",
                        "ALTER TABLE tango RENAME COLUMN y TO x",
                        "query table",
                        "Invalid column: y",
                        () -> {
                            ResultSet rs = s.executeQuery();
                            int rowCount = 0;
                            while (rs.next()) {
                                rowCount++;
                            }
                            Assert.assertEquals(10, rowCount);
                        });
            }
        });
    }

    @Test
    public void testSelectWhileConcurrentlyAlteringTable_simpleStatement() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> executeStatementWhileConcurrentlyChangingSchema(
                connection,
                "ALTER TABLE tango RENAME COLUMN x TO y",
                "ALTER TABLE tango RENAME COLUMN y TO x",
                "query table",
                "Invalid column: y", () -> {
                    try (Statement s = connection.createStatement()) {
                        ResultSet rs = s.executeQuery("SELECT y FROM tango");
                        int rowCount = 0;
                        while (rs.next()) {
                            rowCount++;
                        }
                        Assert.assertEquals(10, rowCount);
                    }
                }));
    }

    @Test
    public void testSelectWhileConcurrentlyAlteringTable_simpleStatementReused() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement s = connection.prepareStatement("SELECT y FROM tango")) {
                executeStatementWhileConcurrentlyChangingSchema(connection,
                        "ALTER TABLE tango RENAME COLUMN x TO y",
                        "ALTER TABLE tango RENAME COLUMN y TO x",
                        "query table",
                        "Invalid column: y", () -> {
                            ResultSet rs = s.executeQuery();
                            int rowCount = 0;
                            while (rs.next()) {
                                rowCount++;
                            }
                            Assert.assertEquals(10, rowCount);
                        });
            }
        });
    }

    @Test
    public void testSelectWhileConcurrentlyRecreatingTable_preparedStatement() throws Exception {
        assertWithPgServer(
                CONN_AWARE_EXTENDED,
                (connection, binary, mode, port) -> {
                    try (PreparedStatement s = connection.prepareStatement("SELECT y FROM tango")) {
                        executeStatementWhileConcurrentlyChangingSchema(
                                connection,
                                "DROP TABLE tango; CREATE TABLE tango as (SELECT x as y FROM long_sequence(10))",
                                "DROP TABLE tango; CREATE TABLE tango as (SELECT x FROM long_sequence(10))",
                                "query table",
                                "Invalid column: y", () -> {
                                    try (ResultSet rs = s.executeQuery()) {
                                        int rowCount = 0;
                                        while (rs.next()) {
                                            rowCount++;
                                        }
                                        Assert.assertEquals(10, rowCount);
                                    }
                                });
                    }
                });
    }

    @Test
    public void testSelectWhileConcurrentlyRecreatingTable_preparedStatementReused() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement s = connection.prepareStatement("SELECT y FROM tango")) {
                executeStatementWhileConcurrentlyChangingSchema(connection,
                        "DROP TABLE tango; CREATE TABLE tango as (SELECT x as y FROM long_sequence(10))",
                        "DROP TABLE tango; CREATE TABLE tango as (SELECT x FROM long_sequence(10))",
                        "query table",
                        "Invalid column: y", () -> {
                            ResultSet rs = s.executeQuery();
                            int rowCount = 0;
                            while (rs.next()) {
                                rowCount++;
                            }
                            Assert.assertEquals(10, rowCount);
                        });
            }
        });
    }

    @Test
    public void testSelectWhileConcurrentlyRecreatingTable_simpleStatement() throws Exception {
        assertWithPgServer(
                CONN_AWARE_ALL,
                (connection, binary, mode, port) ->
                        executeStatementWhileConcurrentlyChangingSchema(
                                connection,
                                "DROP TABLE tango; CREATE TABLE tango as (SELECT x as y FROM long_sequence(10))",
                                "DROP TABLE tango; CREATE TABLE tango as (SELECT x FROM long_sequence(10))",
                                "query table",
                                "Invalid column: y", () -> {
                                    try (Statement s = connection.createStatement()) {
                                        ResultSet rs = s.executeQuery("SELECT y FROM tango");
                                        int rowCount = 0;
                                        while (rs.next()) {
                                            rowCount++;
                                        }
                                        Assert.assertEquals(10, rowCount);
                                    }
                                })
        );
    }

    @Test
    public void testTxInsertSpecificAfterColNameChange() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
                statement.execute("create table insert_after_drop(id long, val int, ts timestamp) timestamp(ts) partition by YEAR");
            }
            mayDrainWalQueue();

            try (PreparedStatement insertStatement = connection.prepareStatement("BEGIN; insert into insert_after_drop (id, val, ts) values (?, 0, '1990-01-01'); COMMIT;")) {
                insertStatement.setLong(1, 42);
                insertStatement.executeUpdate();
                mayDrainWalQueue();

                try (Statement stmt = connection.createStatement()) {
                    stmt.execute("alter table insert_after_drop drop column val");
                    stmt.execute("alter table insert_after_drop add column val2 int");
                }
                mayDrainWalQueue();

                insertStatement.setLong(1, 43);
                try {
                    insertStatement.executeUpdate();
                    Assert.fail("val column was dropped, the INSERT should have failed");
                } catch (SQLException e) {
                    assertMessageMatches(e, "Invalid column: val");
                }
            }
        });
    }

    @Test
    public void testTxInsertWhileConcurrentlyAlteringTable_simpleStatement() throws Exception {
        assertWithPgServer(
                CONN_AWARE_ALL & ~CONN_AWARE_QUIRKS,
                (connection, binary, mode, port) ->
                        executeStatementWhileConcurrentlyChangingSchema(
                                connection,
                                "ALTER TABLE tango RENAME COLUMN x TO y",
                                "ALTER TABLE tango RENAME COLUMN y TO x",
                                "insert rows",
                                null, () -> {
                                    try (Statement s = connection.createStatement()) {
                                        s.executeUpdate("BEGIN; INSERT INTO tango VALUES (42); COMMIT;");
                                    }
                                }));
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
                Assert.fail("id column was dropped, the UPDATE should have failed");
            } catch (PSQLException e) {
                assertMessageMatches(e, "Invalid column: id");
            }
        });
    }

    @Test
    public void testUpdateUnaffectedColWhileConcurrentlyAlteringTable_preparedStatement() throws Exception {
        assertWithPgServer(
                CONN_AWARE_ALL,
                (connection, binary, mode, port) ->
                        executeStatementWhileConcurrentlyChangingSchema(
                                connection,
                                "ALTER TABLE tango ADD COLUMN y INT",
                                "ALTER TABLE tango DROP COLUMN y",
                                "update column x",
                                null, () -> {
                                    try (PreparedStatement s = connection.prepareStatement("UPDATE tango SET x = 42")) {
                                        s.execute();
                                    }
                                }));
    }

    @Test
    public void testUpdateUnaffectedColWhileConcurrentlyAlteringTable_preparedStatementReused() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement s = connection.prepareStatement("UPDATE tango SET x = 42")) {
                executeStatementWhileConcurrentlyChangingSchema(connection,
                        "ALTER TABLE tango ADD COLUMN y INT",
                        "ALTER TABLE tango DROP COLUMN y",
                        "update column x",
                        null,
                        s::execute);
            }
        });
    }

    @Test
    public void testUpdateUnaffectedColWhileConcurrentlyAlteringTable_simpleStatement() throws Exception {
        assertWithPgServer(
                CONN_AWARE_ALL,
                (connection, binary, mode, port) ->
                        executeStatementWhileConcurrentlyChangingSchema(
                                connection,
                                "ALTER TABLE tango ADD COLUMN y INT",
                                "ALTER TABLE tango DROP COLUMN y",
                                "update column x",
                                null, () -> {
                                    try (Statement s = connection.createStatement()) {
                                        s.executeUpdate("UPDATE tango SET x = 42");
                                    }
                                }));
    }

    @Test
    public void testUpdateUnaffectedColWhileConcurrentlyAlteringTable_simpleStatementReused() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement s = connection.createStatement()) {
                executeStatementWhileConcurrentlyChangingSchema(connection,
                        "ALTER TABLE tango ADD COLUMN y INT",
                        "ALTER TABLE tango DROP COLUMN y",
                        "update column x",
                        null,
                        () -> s.executeUpdate("UPDATE tango SET x = 42"));
            }
        });
    }

    @Test
    public void testUpdateUnaffectedColWhileConcurrentlyRecreatingTable_preparedStatement() throws Exception {
        assertWithPgServer(
                CONN_AWARE_ALL,
                (connection, binary, mode, port) ->
                        executeStatementWhileConcurrentlyChangingSchema(
                                connection,
                                "DROP TABLE tango;\n" +
                                        "CREATE TABLE tango AS (SELECT x, 1 AS y FROM long_sequence(10))",
                                "DROP TABLE tango;\n" +
                                        "CREATE TABLE tango AS (SELECT x FROM long_sequence(10))",
                                "update column x",
                                "table does not exist \\[table=tango\\]", () -> {
                                    try (PreparedStatement s = connection.prepareStatement("UPDATE tango SET x = 42")) {
                                        s.execute();
                                    }
                                }));
    }

    @Test
    public void testUpdateUnaffectedColWhileConcurrentlyRecreatingTable_preparedStatementReused() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement s = connection.prepareStatement("UPDATE tango SET x = 42")) {
                executeStatementWhileConcurrentlyChangingSchema(connection,
                        "DROP TABLE tango;\n" +
                                "CREATE TABLE tango AS (SELECT x, 1 AS y FROM long_sequence(10))",
                        "DROP TABLE tango;\n" +
                                "CREATE TABLE tango AS (SELECT x FROM long_sequence(10))",
                        "update column x",
                        "table does not exist \\[table=tango\\]", s::execute);
            }
        });
    }

    @Test
    public void testUpdateUnaffectedColWhileConcurrentlyRecreatingTable_simpleStatement() throws Exception {
        assertWithPgServer(
                CONN_AWARE_ALL,
                (connection, binary, mode, port) ->
                        executeStatementWhileConcurrentlyChangingSchema(
                                connection,
                                "DROP TABLE tango;\n" +
                                        "CREATE TABLE tango AS (SELECT x, 1 AS y FROM long_sequence(10))",
                                "DROP TABLE tango;\n" +
                                        "CREATE TABLE tango AS (SELECT x FROM long_sequence(10))",
                                "update column x",
                                "table does not exist \\[table=tango\\]", () -> {
                                    try (Statement s = connection.createStatement()) {
                                        s.executeUpdate("UPDATE tango SET x = 42");
                                    }
                                }));
    }

    @Test
    public void testUpdateWhileConcurrentlyAlteringTable_preparedStatement() throws Exception {
        assertWithPgServer(
                CONN_AWARE_ALL,
                (connection, binary, mode, port) ->
                        executeStatementWhileConcurrentlyChangingSchema(
                                connection,
                                "ALTER TABLE tango ADD COLUMN y INT",
                                "ALTER TABLE tango DROP COLUMN y",
                                "update column y",
                                "Invalid column: y", () -> {
                                    try (PreparedStatement s = connection.prepareStatement("UPDATE tango SET y = 42")) {
                                        s.execute();
                                    }
                                }));
    }

    @Test
    public void testUpdateWhileConcurrentlyAlteringTable_preparedStatementReused() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement s = connection.prepareStatement("UPDATE tango SET y = 42")) {
                executeStatementWhileConcurrentlyChangingSchema(connection,
                        "ALTER TABLE tango ADD COLUMN y INT",
                        "ALTER TABLE tango DROP COLUMN y",
                        "update column y",
                        "Invalid column: y", s::execute);
            }
        });
    }

    @Test
    public void testUpdateWhileConcurrentlyAlteringTable_simpleStatement() throws Exception {
        assertWithPgServer(
                CONN_AWARE_ALL,
                (connection, binary, mode, port) ->
                        executeStatementWhileConcurrentlyChangingSchema(
                                connection,
                                "ALTER TABLE tango ADD COLUMN y INT",
                                "ALTER TABLE tango DROP COLUMN y",
                                "update column y",
                                "Invalid column: y", () -> {
                                    try (Statement s = connection.createStatement()) {
                                        s.executeUpdate("UPDATE tango SET y = 42");
                                    }
                                }));
    }

    @Test
    public void testUpdateWhileConcurrentlyAlteringTable_simpleStatementReused() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement s = connection.createStatement()) {
                executeStatementWhileConcurrentlyChangingSchema(connection,
                        "ALTER TABLE tango ADD COLUMN y INT",
                        "ALTER TABLE tango DROP COLUMN y",
                        "update column y",
                        "Invalid column: y", () -> s.executeUpdate("UPDATE tango SET y = 42"));
            }
        });
    }

    @Test
    public void testUpdateWhileConcurrentlyRecreatingTable_preparedStatement() throws Exception {
        assertWithPgServer(
                CONN_AWARE_ALL,
                (connection, binary, mode, port) ->
                        executeStatementWhileConcurrentlyChangingSchema(
                                connection,
                                "DROP TABLE tango;\n" +
                                        "CREATE TABLE tango AS (SELECT x AS y FROM long_sequence(10))",
                                "DROP TABLE tango;\n" +
                                        "CREATE TABLE tango AS (SELECT x FROM long_sequence(10))",
                                "update column y",
                                "table does not exist \\[table=tango\\]|Invalid column: y",
                                () -> {
                                    try (PreparedStatement s = connection.prepareStatement("UPDATE tango SET y = 42")) {
                                        s.execute();
                                    }
                                }));
    }

    @Test
    public void testUpdateWhileConcurrentlyRecreatingTable_preparedStatementReused() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement s = connection.prepareStatement("UPDATE tango SET y = 42")) {
                executeStatementWhileConcurrentlyChangingSchema(connection,
                        "DROP TABLE tango;\n" +
                                "CREATE TABLE tango AS (SELECT x AS y FROM long_sequence(10))",
                        "DROP TABLE tango;\n" +
                                "CREATE TABLE tango AS (SELECT x FROM long_sequence(10))",
                        "update column y",
                        "table does not exist \\[table=tango\\]|Invalid column: y",
                        s::execute);
            }
        });
    }

    @Test
    public void testUpdateWhileConcurrentlyRecreatingTable_simpleStatement() throws Exception {
        assertWithPgServer(
                CONN_AWARE_ALL,
                (connection, binary, mode, port) ->
                        executeStatementWhileConcurrentlyChangingSchema(
                                connection,
                                "DROP TABLE tango;\n" +
                                        "CREATE TABLE tango AS (SELECT x AS y FROM long_sequence(10))",
                                "DROP TABLE tango;\n" +
                                        "CREATE TABLE tango AS (SELECT x FROM long_sequence(10))",
                                "update column y",
                                "table does not exist \\[table=tango\\]|Invalid column: y", () -> {
                                    try (Statement s = connection.createStatement()) {
                                        s.executeUpdate("UPDATE tango SET y = 42");
                                    }
                                }));
    }

    private void assertMessageMatches(Exception e, String expectedRegex) {
        String exceptionMessage = e.getMessage();
        if (!Pattern.compile(expectedRegex).matcher(exceptionMessage).find()) {
            String assertMessage = String.format("Exception message doesn't match regex '%s'. Actual message: '%s'", expectedRegex, exceptionMessage);
            throw new AssertionError(assertMessage, e);
        }
    }

    private void executeStatementWhileConcurrentlyChangingSchema(
            @NotNull Connection connection,
            @NotNull String backgroundDdl1,
            @NotNull String backgroundDdl2,
            @NotNull String whatMainLoopTriesToDo,
            @Nullable String acceptedErrorRegex,
            @NotNull MainLoopBody mainLoopBody
    ) throws Exception {
        execute("CREATE TABLE tango AS (SELECT x FROM long_sequence(10)) ");
        AtomicBoolean stop = new AtomicBoolean();
        AtomicReference<Exception> backgroundError = new AtomicReference<>();
        boolean hadForegroundError = false;
        CyclicBarrier barrier = new CyclicBarrier(2);
        Thread t = new Thread(() -> {
            try {
                TestUtils.await(barrier);
                try (
                        PreparedStatement s1 = connection.prepareStatement(backgroundDdl1);
                        PreparedStatement s2 = connection.prepareStatement(backgroundDdl2)
                ) {
                    while (!stop.get()) {
                        s1.execute();
                        mayDrainWalQueue();
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5));
                        s2.execute();
                    }
                }
            } catch (Exception e) {
                backgroundError.set(e);
            } finally {
                Path.clearThreadLocals();
            }
        });
        t.start();
        try {
            TestUtils.await(barrier);
            boolean hadSuccess = false;
            int minAttemptCount = 100;
            int minDurationMillis = 250;
            int maxAttemptCount = 1000;
            int maxDurationMillis = 5000;
            long minDeadline = Long.MAX_VALUE;
            long maxDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(maxDurationMillis);
            String failMsg = String.format(
                    "Failed to %s after at least %d milliseconds or at least %d attempts",
                    whatMainLoopTriesToDo, maxDurationMillis, maxAttemptCount);
            for (
                    int i = 0; backgroundError.get() == null && (
                    i < minAttemptCount || System.nanoTime() < minDeadline
                            || !hadSuccess && i < maxAttemptCount && System.nanoTime() < maxDeadline
            ); i++) {
                try {
                    mainLoopBody.run();
                    hadSuccess = true;
                    mayDrainWalQueue();
                    if (minDeadline == Long.MAX_VALUE) {
                        minDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(minDurationMillis);
                    }
                } catch (SQLException e) {
                    if (acceptedErrorRegex != null) {
                        assertMessageMatches(e, acceptedErrorRegex);
                    } else {
                        throw new AssertionError("Did not expect any failure", e);
                    }
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
            }
            assertTrue(failMsg, hadSuccess);
        } catch (Throwable e) {
            hadForegroundError = true;
            throw e;
        } finally {
            stop.set(true);
            t.join();
            Exception bgErr = backgroundError.get();
            if (bgErr != null && hadForegroundError) {
                LOG.error().$("Background task failed").$(bgErr).$();
            }
        }
        Exception bgErr = backgroundError.get();
        if (bgErr != null) {
            throw new Exception("Background task failed", bgErr);
        }
    }

    private void mayDrainWalQueue() {
        if (walEnabled) {
            drainWalQueue();
        }
    }

    @FunctionalInterface
    private interface MainLoopBody {
        void run() throws SQLException;
    }
}
