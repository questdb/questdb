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

import io.questdb.ServerMain;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.std.Misc;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import org.junit.Before;
import org.junit.Test;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static io.questdb.test.cutlass.pgwire.BasePGTest.assertResultSet;
import static io.questdb.test.tools.TestUtils.unchecked;

public class PGVarcharArrayBindVariablesTest extends AbstractBootstrapTest {
    @Before
    public void setUp() {
        super.setUp();
        unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testBindSingleElemVariable() throws Exception {
        assertMemoryLeak(() -> {
            createDummyConfiguration(
                    "pg.select.cache.enabled=true"
            );

            try (final ServerMain serverMain = TestServerMain.createWithManualWalRun(getServerMainArgs())) {
                serverMain.start();
                createTable(serverMain, "sym_col", "symbol");
                try (Connection connection = getConnection(serverMain)) {
                    try (final PreparedStatement stmt = connection.prepareStatement("SELECT * FROM tab where sym_col in (?)")) {
                        stmt.setString(1, "a");
                        try (final ResultSet resultSet = stmt.executeQuery()) {
                            assertResultSet(
                                    """
                                            ts[TIMESTAMP],sym_col[VARCHAR]
                                            2023-01-01 09:10:00.0,a
                                            2023-01-01 09:15:00.0,a
                                            2023-01-01 09:17:00.0,a
                                            2023-01-01 09:19:00.0,a
                                            """,
                                    new StringSink(),
                                    resultSet
                            );
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testBindStrListsVariable() throws Exception {
        assertMemoryLeak(() -> {
            createDummyConfiguration(
                    "pg.select.cache.enabled=true"
            );

            try (final ServerMain serverMain = TestServerMain.createWithManualWalRun(getServerMainArgs())) {
                serverMain.start();
                createTable(serverMain, "str_col", "string");
                try (Connection connection = getConnection(serverMain)) {
                    try (final PreparedStatement stmt = connection.prepareStatement("explain SELECT * FROM tab where str_col in (?)")) {
                        Array array = connection.createArrayOf("varchar", new String[]{"a"});
                        stmt.setArray(1, array);
                        try (final ResultSet resultSet = stmt.executeQuery()) {
                            assertResultSet(
                                    """
                                            QUERY PLAN[VARCHAR]
                                            Async Filter workers: 2
                                              filter: str_col in $0::varchar[]
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: tab
                                            """,
                                    Misc.getThreadLocalSink(),
                                    resultSet
                            );
                        }
                    }

                    try (final PreparedStatement stmt = connection.prepareStatement("SELECT * FROM tab where str_col in (?)")) {
                        Array array = connection.createArrayOf("varchar", new String[]{"a", "b"});
                        stmt.setArray(1, array);
                        try (final ResultSet resultSet = stmt.executeQuery()) {
                            assertResultSet(
                                    """
                                            ts[TIMESTAMP],str_col[VARCHAR]
                                            2023-01-01 09:10:00.0,a
                                            2023-01-01 09:13:00.0,b
                                            2023-01-01 09:14:00.0,b
                                            2023-01-01 09:15:00.0,a
                                            2023-01-01 09:17:00.0,a
                                            2023-01-01 09:18:00.0,b
                                            2023-01-01 09:19:00.0,a
                                            """,
                                    new StringSink(),
                                    resultSet
                            );
                        }

                        stmt.clearParameters();
                        Array array1 = connection.createArrayOf("text", new String[]{"a"});
                        stmt.setArray(1, array1);
                        try (final ResultSet resultSet = stmt.executeQuery()) {
                            assertResultSet(
                                    """
                                            ts[TIMESTAMP],str_col[VARCHAR]
                                            2023-01-01 09:10:00.0,a
                                            2023-01-01 09:15:00.0,a
                                            2023-01-01 09:17:00.0,a
                                            2023-01-01 09:19:00.0,a
                                            """,
                                    new StringSink(),
                                    resultSet
                            );
                        }

                        stmt.clearParameters();
                        Array array2 = connection.createArrayOf("text", new String[]{});
                        stmt.setArray(1, array2);
                        try (final ResultSet resultSet = stmt.executeQuery()) {
                            assertResultSet(
                                    """
                                            ts[TIMESTAMP],str_col[VARCHAR]
                                            """,
                                    new StringSink(),
                                    resultSet
                            );
                        }

                        stmt.clearParameters();
                        Array array3 = connection.createArrayOf("VARCHAR", new String[]{"b", null});
                        stmt.setArray(1, array3);
                        try (final ResultSet resultSet = stmt.executeQuery()) {
                            assertResultSet(
                                    """
                                            ts[TIMESTAMP],str_col[VARCHAR]
                                            2023-01-01 09:11:00.0,null
                                            2023-01-01 09:12:00.0,null
                                            2023-01-01 09:13:00.0,b
                                            2023-01-01 09:14:00.0,b
                                            2023-01-01 09:16:00.0,null
                                            2023-01-01 09:18:00.0,b
                                            """,
                                    new StringSink(),
                                    resultSet
                            );
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testBindSymbolListsVariable() throws Exception {
        assertMemoryLeak(() -> {
            createDummyConfiguration(
                    "pg.select.cache.enabled=true"
            );

            try (final ServerMain serverMain = TestServerMain.createWithManualWalRun(getServerMainArgs())) {
                serverMain.start();
                createTable(serverMain, "sym_col", "symbol");
                try (Connection connection = getConnection(serverMain)) {
                    try (final PreparedStatement stmt = connection.prepareStatement("explain SELECT * FROM tab where sym_col in (?)")) {
                        Array array = connection.createArrayOf("varchar", new String[]{"a"});
                        stmt.setArray(1, array);
                        try (final ResultSet resultSet = stmt.executeQuery()) {
                            assertResultSet(
                                    """
                                            QUERY PLAN[VARCHAR]
                                            Async Filter workers: 2
                                              filter: sym_col in $0::varchar[]
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: tab
                                            """,
                                    Misc.getThreadLocalSink(),
                                    resultSet
                            );
                        }
                    }

                    try (final PreparedStatement stmt = connection.prepareStatement("SELECT * FROM tab where sym_col in (?)")) {
                        Array array = connection.createArrayOf("varchar", new String[]{"a", "b"});
                        stmt.setArray(1, array);
                        try (final ResultSet resultSet = stmt.executeQuery()) {
                            assertResultSet(
                                    """
                                            ts[TIMESTAMP],sym_col[VARCHAR]
                                            2023-01-01 09:10:00.0,a
                                            2023-01-01 09:13:00.0,b
                                            2023-01-01 09:14:00.0,b
                                            2023-01-01 09:15:00.0,a
                                            2023-01-01 09:17:00.0,a
                                            2023-01-01 09:18:00.0,b
                                            2023-01-01 09:19:00.0,a
                                            """,
                                    new StringSink(),
                                    resultSet
                            );
                        }

                        stmt.clearParameters();
                        Array array1 = connection.createArrayOf("text", new String[]{"a"});
                        stmt.setArray(1, array1);
                        try (final ResultSet resultSet = stmt.executeQuery()) {
                            assertResultSet(
                                    """
                                            ts[TIMESTAMP],sym_col[VARCHAR]
                                            2023-01-01 09:10:00.0,a
                                            2023-01-01 09:15:00.0,a
                                            2023-01-01 09:17:00.0,a
                                            2023-01-01 09:19:00.0,a
                                            """,
                                    new StringSink(),
                                    resultSet
                            );
                        }

                        stmt.clearParameters();
                        Array array2 = connection.createArrayOf("text", new String[]{});
                        stmt.setArray(1, array2);
                        try (final ResultSet resultSet = stmt.executeQuery()) {
                            assertResultSet(
                                    """
                                            ts[TIMESTAMP],sym_col[VARCHAR]
                                            """,
                                    new StringSink(),
                                    resultSet
                            );
                        }

                        stmt.clearParameters();
                        Array array3 = connection.createArrayOf("VARCHAR", new String[]{"b", null});
                        stmt.setArray(1, array3);
                        try (final ResultSet resultSet = stmt.executeQuery()) {
                            assertResultSet(
                                    """
                                            ts[TIMESTAMP],sym_col[VARCHAR]
                                            2023-01-01 09:11:00.0,null
                                            2023-01-01 09:12:00.0,null
                                            2023-01-01 09:13:00.0,b
                                            2023-01-01 09:14:00.0,b
                                            2023-01-01 09:16:00.0,null
                                            2023-01-01 09:18:00.0,b
                                            """,
                                    new StringSink(),
                                    resultSet
                            );
                        }

                        stmt.clearParameters();
                        Array array4 = connection.createArrayOf("VARCHAR", new String[]{"a", "c"});
                        stmt.setArray(1, array4);
                        try (final ResultSet resultSet = stmt.executeQuery()) {
                            assertResultSet(
                                    """
                                            ts[TIMESTAMP],sym_col[VARCHAR]
                                            2023-01-01 09:10:00.0,a
                                            2023-01-01 09:15:00.0,a
                                            2023-01-01 09:17:00.0,a
                                            2023-01-01 09:19:00.0,a
                                            """,
                                    new StringSink(),
                                    resultSet
                            );
                        }

                        stmt.clearParameters();
                        stmt.setArray(1, null);
                        try (final ResultSet resultSet = stmt.executeQuery()) {
                            assertResultSet(
                                    """
                                            ts[TIMESTAMP],sym_col[VARCHAR]
                                            """,
                                    new StringSink(),
                                    resultSet
                            );
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testBindVarcharArrayWithSpecialCharacters() throws Exception {
        assertMemoryLeak(() -> {
            createDummyConfiguration(
                    "pg.select.cache.enabled=true"
            );

            try (final ServerMain serverMain = TestServerMain.createWithManualWalRun(getServerMainArgs())) {
                serverMain.start();
                final CairoEngine engine = serverMain.getEngine();
                try (
                        SqlExecutionContext executionContext = new SqlExecutionContextImpl(engine, 1)
                                .with(AllowAllSecurityContext.INSTANCE, new BindVariableServiceImpl(engine.getConfiguration()))
                ) {
                    engine.execute(
                            "create table special_chars (ts TIMESTAMP, val varchar) timestamp(ts) partition by day",
                            executionContext
                    );
                    engine.execute(
                            "insert into special_chars(ts, val) values " +
                                    "(cast('2023-01-01T09:10:00.000000Z' as TIMESTAMP), 'hello\\world')," +  // backslash
                                    "(cast('2023-01-01T09:11:00.000000Z' as TIMESTAMP), 'say\"hi')," +       // quote
                                    "(cast('2023-01-01T09:12:00.000000Z' as TIMESTAMP), 'a,b,c')," +         // commas
                                    "(cast('2023-01-01T09:13:00.000000Z' as TIMESTAMP), '{braces}')," +      // braces
                                    "(cast('2023-01-01T09:14:00.000000Z' as TIMESTAMP), 'normal')",
                            executionContext
                    );
                }
                drainWalQueue(engine);

                try (Connection connection = getConnection(serverMain)) {
                    // Test with backslash in array
                    try (final PreparedStatement stmt = connection.prepareStatement("SELECT * FROM special_chars WHERE val IN (?)")) {
                        Array array = connection.createArrayOf("varchar", new String[]{"hello\\world", "normal"});
                        stmt.setArray(1, array);
                        try (final ResultSet resultSet = stmt.executeQuery()) {
                            assertResultSet(
                                    """
                                            ts[TIMESTAMP],val[VARCHAR]
                                            2023-01-01 09:10:00.0,hello\\world
                                            2023-01-01 09:14:00.0,normal
                                            """,
                                    new StringSink(),
                                    resultSet
                            );
                        }
                    }

                    // Test with quote in array
                    try (final PreparedStatement stmt = connection.prepareStatement("SELECT * FROM special_chars WHERE val IN (?)")) {
                        Array array = connection.createArrayOf("varchar", new String[]{"say\"hi"});
                        stmt.setArray(1, array);
                        try (final ResultSet resultSet = stmt.executeQuery()) {
                            assertResultSet(
                                    """
                                            ts[TIMESTAMP],val[VARCHAR]
                                            2023-01-01 09:11:00.0,say"hi
                                            """,
                                    new StringSink(),
                                    resultSet
                            );
                        }
                    }

                    // Test with commas and braces
                    try (final PreparedStatement stmt = connection.prepareStatement("SELECT * FROM special_chars WHERE val IN (?)")) {
                        Array array = connection.createArrayOf("varchar", new String[]{"a,b,c", "{braces}"});
                        stmt.setArray(1, array);
                        try (final ResultSet resultSet = stmt.executeQuery()) {
                            assertResultSet(
                                    """
                                            ts[TIMESTAMP],val[VARCHAR]
                                            2023-01-01 09:12:00.0,a,b,c
                                            2023-01-01 09:13:00.0,{braces}
                                            """,
                                    new StringSink(),
                                    resultSet
                            );
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testBindVarcharListOutput() throws Exception {
        try (final ServerMain serverMain = TestServerMain.createWithManualWalRun(getServerMainArgs())) {
            serverMain.start();
            try (Connection connection = getConnection(serverMain)) {
                try (PreparedStatement stmt = connection.prepareStatement("select ? as arr")) {
                    Array arr = connection.createArrayOf("varchar", new String[]{"❤️hello"});
                    stmt.setArray(1, arr);
                    try (ResultSet rs = stmt.executeQuery()) {
                        assertResultSet("""
                                        arr[ARRAY]
                                        {❤️hello}
                                        """,
                                Misc.getThreadLocalSink(),
                                rs
                        );
                    }
                }
            }
        }
    }

    @Test
    public void testBindVarcharListsVariable() throws Exception {
        assertMemoryLeak(() -> {
            createDummyConfiguration(
                    "pg.select.cache.enabled=true"
            );

            try (final ServerMain serverMain = TestServerMain.createWithManualWalRun(getServerMainArgs())) {
                serverMain.start();
                createTable(serverMain, "varchar_col", "varchar");
                try (Connection connection = getConnection(serverMain)) {
                    try (final PreparedStatement stmt = connection.prepareStatement("explain SELECT * FROM tab where varchar_col in (?)")) {
                        Array array = connection.createArrayOf("varchar", new String[]{"a"});
                        stmt.setArray(1, array);
                        try (final ResultSet resultSet = stmt.executeQuery()) {
                            assertResultSet(
                                    """
                                            QUERY PLAN[VARCHAR]
                                            Async Filter workers: 2
                                              filter: varchar_col in $0::varchar[]
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: tab
                                            """,
                                    Misc.getThreadLocalSink(),
                                    resultSet
                            );
                        }
                    }

                    try (final PreparedStatement stmt = connection.prepareStatement("SELECT * FROM tab where varchar_col in (?)")) {
                        Array array = connection.createArrayOf("varchar", new String[]{"a", "b"});
                        stmt.setArray(1, array);
                        try (final ResultSet resultSet = stmt.executeQuery()) {
                            assertResultSet(
                                    """
                                            ts[TIMESTAMP],varchar_col[VARCHAR]
                                            2023-01-01 09:10:00.0,a
                                            2023-01-01 09:13:00.0,b
                                            2023-01-01 09:14:00.0,b
                                            2023-01-01 09:15:00.0,a
                                            2023-01-01 09:17:00.0,a
                                            2023-01-01 09:18:00.0,b
                                            2023-01-01 09:19:00.0,a
                                            """,
                                    new StringSink(),
                                    resultSet
                            );
                        }

                        stmt.clearParameters();
                        Array array1 = connection.createArrayOf("text", new String[]{"a"});
                        stmt.setArray(1, array1);
                        try (final ResultSet resultSet = stmt.executeQuery()) {
                            assertResultSet(
                                    """
                                            ts[TIMESTAMP],varchar_col[VARCHAR]
                                            2023-01-01 09:10:00.0,a
                                            2023-01-01 09:15:00.0,a
                                            2023-01-01 09:17:00.0,a
                                            2023-01-01 09:19:00.0,a
                                            """,
                                    new StringSink(),
                                    resultSet
                            );
                        }

                        stmt.clearParameters();
                        Array array2 = connection.createArrayOf("text", new String[]{});
                        stmt.setArray(1, array2);
                        try (final ResultSet resultSet = stmt.executeQuery()) {
                            assertResultSet(
                                    """
                                            ts[TIMESTAMP],varchar_col[VARCHAR]
                                            """,
                                    new StringSink(),
                                    resultSet
                            );
                        }

                        stmt.clearParameters();
                        Array array3 = connection.createArrayOf("VARCHAR", new String[]{"b", null});
                        stmt.setArray(1, array3);
                        try (final ResultSet resultSet = stmt.executeQuery()) {
                            assertResultSet(
                                    """
                                            ts[TIMESTAMP],varchar_col[VARCHAR]
                                            2023-01-01 09:11:00.0,null
                                            2023-01-01 09:12:00.0,null
                                            2023-01-01 09:13:00.0,b
                                            2023-01-01 09:14:00.0,b
                                            2023-01-01 09:16:00.0,null
                                            2023-01-01 09:18:00.0,b
                                            """,
                                    new StringSink(),
                                    resultSet
                            );
                        }
                    }
                }
            }
        });
    }

    private static void createTable(ServerMain serverMain, String colName, String colType) {
        final CairoEngine engine = serverMain.getEngine();
        try (
                SqlExecutionContext executionContext = new SqlExecutionContextImpl(engine, 1)
                        .with(AllowAllSecurityContext.INSTANCE, new BindVariableServiceImpl(engine.getConfiguration()))
        ) {
            engine.execute(
                    "create table tab (ts TIMESTAMP, " + colName + " " + colType + ") timestamp(ts) partition by day",
                    executionContext
            );
            engine.execute(
                    "insert into tab(ts, " + colName + ") values " +
                            "(cast('2023-01-01T09:10:00.000000Z' as TIMESTAMP), 'a')," +
                            "(cast('2023-01-01T09:11:00.000000Z' as TIMESTAMP), null)," +
                            "(cast('2023-01-01T09:12:00.000000Z' as TIMESTAMP), null)," +
                            "(cast('2023-01-01T09:13:00.000000Z' as TIMESTAMP), 'b')," +
                            "(cast('2023-01-01T09:14:00.000000Z' as TIMESTAMP), 'b')," +
                            "(cast('2023-01-01T09:15:00.000000Z' as TIMESTAMP), 'a')," +
                            "(cast('2023-01-01T09:16:00.000000Z' as TIMESTAMP), null)," +
                            "(cast('2023-01-01T09:17:00.000000Z' as TIMESTAMP), 'a')," +
                            "(cast('2023-01-01T09:18:00.000000Z' as TIMESTAMP), 'b')," +
                            "(cast('2023-01-01T09:19:00.000000Z' as TIMESTAMP), 'a')",
                    executionContext
            );
        } catch (SqlException e) {
            throw CairoException.critical(0).put("Could not create table: '").put(e.getFlyweightMessage());
        }
        drainWalQueue(engine);
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
