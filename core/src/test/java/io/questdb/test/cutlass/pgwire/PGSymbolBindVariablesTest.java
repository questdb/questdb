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

import io.questdb.ServerMain;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.std.Misc;
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

public class PGSymbolBindVariablesTest extends AbstractBootstrapTest {
    @Before
    public void setUp() {
        super.setUp();
        unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testBindSymbolListsVariable() throws Exception {
        assertMemoryLeak(() -> {
            createDummyConfiguration(
                    "pg.select.cache.enabled=true"
            );

            try (final ServerMain serverMain = TestServerMain.createWithManualWalRun(getServerMainArgs())) {
                serverMain.start();
                createTable(serverMain, 10);
                try (Connection connection = getConnection(serverMain)) {
                    try (final PreparedStatement stmt = connection.prepareStatement("SELECT * FROM tab where sym_col in (?)")) {
                        Array array = connection.createArrayOf("varchar", new String[]{"a", "b"});
                        stmt.setArray(1, array);
                        try (final ResultSet resultSet = stmt.executeQuery()) {
                            assertResultSet(
                                    "QUERY PLAN[VARCHAR]\n" +
                                            "Limit lo: $0::int[1] skip-over-rows: 0 limit: 1\n" +
                                            "    PageFrame\n" +
                                            "        Row forward scan\n" +
                                            "        Frame forward scan on: tab\n",
                                    Misc.getThreadLocalSink(),
                                    resultSet
                            );
                        }
                    }
                }
            }
        });
    }

    private static void createTable(ServerMain serverMain, int numOfRows) {
        final CairoEngine engine = serverMain.getEngine();
        try (
                SqlExecutionContext executionContext = new SqlExecutionContextImpl(engine, 1)
                        .with(AllowAllSecurityContext.INSTANCE, new BindVariableServiceImpl(engine.getConfiguration()))
        ) {
            engine.execute(
                    "create table tab as (select rnd_symbol('a', 'b', 'c', null) sym_col, timestamp_sequence(20000000, 100000) ts " +
                            "from long_sequence(" + numOfRows + ")) timestamp(ts) partition by day wal",
                    executionContext)
            ;
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
