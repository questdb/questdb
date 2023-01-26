/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin;

import io.questdb.AbstractBootstrapTest;
import io.questdb.Bootstrap;
import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.BeforeClass;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static io.questdb.griffin.AlterTableSetTypeTest.NON_WAL;
import static io.questdb.griffin.AlterTableSetTypeTest.WAL;
import static org.junit.Assert.*;

public class AlterTableSetTypeRestartTest extends AbstractBootstrapTest {
    private static final Log LOG = LogFactory.getLog(AlterTableSetTypeRestartTest.class);

    private final String tableName = "testtable";

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractBootstrapTest.setUpStatic();
        try {
            createDummyConfiguration(PropertyKey.CAIRO_WAL_SUPPORTED.getPropertyPath() + "=true");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testNonWalToWalWithDropTable() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain questdb = new ServerMain("-d", root.toString(), Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION)) {
                questdb.start();
                createTable(tableName, "BYPASS WAL");
                insertInto(tableName);

                final CairoEngine engine = questdb.getCairoEngine();
                final TableToken token = engine.getTableToken(tableName);

                // non-WAL table
                assertFalse(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 1);
                assertConvertFileDoesNotExist(engine, token);

                // schedule table conversion to WAL
                setType(tableName, "WAL");
                final Path path = assertConvertFileExists(engine, token);
                assertConvertFileContent(path, WAL);

                insertInto(tableName);
                assertFalse(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 2);

                // drop table
                dropTable(tableName);
            }
            validateShutdown();

            // restart
            try (final ServerMain questdb = new ServerMain("-d", root.toString(), Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION)) {
                questdb.start();

                final CairoEngine engine = questdb.getCairoEngine();
                try {
                    engine.getTableToken(tableName);
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "table does not exist [table=" + tableName + ']');
                }
            }
        });
    }

    @Test
    public void testSetType() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain questdb = new ServerMain("-d", root.toString(), Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION)) {
                questdb.start();
                createTable(tableName, "BYPASS WAL");
                insertInto(tableName);

                final CairoEngine engine = questdb.getCairoEngine();
                final TableToken token = engine.getTableToken(tableName);

                // non-WAL table
                assertFalse(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 1);
                assertConvertFileDoesNotExist(engine, token);

                // schedule table conversion to WAL
                setType(tableName, "WAL");
                final Path path = assertConvertFileExists(engine, token);
                assertConvertFileContent(path, WAL);

                insertInto(tableName);
                assertFalse(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 2);
            }
            validateShutdown();

            // restart
            try (final ServerMain questdb = new ServerMain("-d", root.toString(), Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION)) {
                questdb.start();

                final CairoEngine engine = questdb.getCairoEngine();
                final TableToken token = engine.getTableToken(tableName);

                // table has been converted to WAL
                assertTrue(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 2);
                assertConvertFileDoesNotExist(engine, token);

                insertInto(tableName);
                insertInto(tableName);
                drainWalQueue(engine);
                assertTrue(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 4);

                // schedule table conversion back to non-WAL
                setType(tableName, "BYPASS WAL");
                drainWalQueue(engine);
                final Path path = assertConvertFileExists(engine, token);
                assertConvertFileContent(path, NON_WAL);

                insertInto(tableName);
                drainWalQueue(engine);
                assertTrue(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 5);
            }
            validateShutdown();

            // restart
            try (final ServerMain questdb = new ServerMain("-d", root.toString(), Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION)) {
                questdb.start();

                final CairoEngine engine = questdb.getCairoEngine();
                final TableToken token = engine.getTableToken(tableName);

                // table has been converted to non-WAL
                assertFalse(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 5);
                assertConvertFileDoesNotExist(engine, token);

                insertInto(tableName);
                assertFalse(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 6);

                // schedule table conversion to non-WAL again
                setType(tableName, "BYPASS WAL");
                final Path path = assertConvertFileExists(engine, token);
                assertConvertFileContent(path, NON_WAL);
            }
            validateShutdown();

            // restart
            try (final ServerMain questdb = new ServerMain("-d", root.toString(), Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION)) {
                questdb.start();

                final CairoEngine engine = questdb.getCairoEngine();
                final TableToken token = engine.getTableToken(tableName);

                // no conversion happened, table was already non-WAL type
                assertFalse(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 6);
                assertConvertFileDoesNotExist(engine, token);

                // schedule table conversion to WAL
                setType(tableName, "WAL");
                final Path path = assertConvertFileExists(engine, token);
                assertConvertFileContent(path, WAL);
            }
            validateShutdown();

            // restart
            try (final ServerMain questdb = new ServerMain("-d", root.toString(), Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION)) {
                questdb.start();

                final CairoEngine engine = questdb.getCairoEngine();
                final TableToken token = engine.getTableToken(tableName);

                // table has been converted to WAL
                assertTrue(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 6);
                assertConvertFileDoesNotExist(engine, token);

                insertInto(tableName);
                insertInto(tableName);
                insertInto(tableName);
                drainWalQueue(engine);
                assertTrue(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 9);
            }
            validateShutdown();
        });
    }

    @Test
    public void testWalToNonWalWithDropTable() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain questdb = new ServerMain("-d", root.toString(), Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION)) {
                questdb.start();
                createTable(tableName, "WAL");

                final CairoEngine engine = questdb.getCairoEngine();
                final TableToken token = engine.getTableToken(tableName);

                insertInto(tableName);
                drainWalQueue(engine);

                // WAL table
                assertTrue(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 1);
                assertConvertFileDoesNotExist(engine, token);

                // schedule table conversion to non-WAL
                setType(tableName, "BYPASS WAL");
                drainWalQueue(engine);
                final Path path = assertConvertFileExists(engine, token);
                assertConvertFileContent(path, NON_WAL);

                insertInto(tableName);
                drainWalQueue(engine);
                assertTrue(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 2);

                // drop table
                dropTable(tableName);
                drainWalQueue(engine);
            }
            validateShutdown();

            // restart
            try (final ServerMain questdb = new ServerMain("-d", root.toString(), Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION)) {
                questdb.start();

                final CairoEngine engine = questdb.getCairoEngine();
                try {
                    engine.getTableToken(tableName);
                } catch (CairoException e) {
                    assertTrue(e.getMessage().contains("[-1] table does not exist [table=testtable]"));
                }
            }
        });
    }

    private static void assertConvertFileContent(Path convertFilePath, byte expected) throws IOException {
        final byte[] fileContent = java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(convertFilePath.toString()));
        assertEquals(1, fileContent.length);
        assertEquals(expected, fileContent[0]);
    }

    private static void assertConvertFileDoesNotExist(CairoEngine engine, TableToken token) {
        doesConvertFileExist(engine, token, false);
    }

    private static Path assertConvertFileExists(CairoEngine engine, TableToken token) {
        return doesConvertFileExist(engine, token, true);
    }

    private static void assertNumOfRows(CairoEngine engine, String tableName, int count) throws SqlException {
        try (
                final SqlExecutionContext context = createSqlExecutionContext(engine);
                final SqlCompiler compiler = new SqlCompiler(engine)
        ) {
            TestUtils.assertSql(
                    compiler,
                    context,
                    "SELECT count() FROM " + tableName,
                    Misc.getThreadLocalBuilder(),
                    "count\n" +
                            count + "\n"
            );
        }
    }

    private static SqlExecutionContext createSqlExecutionContext(CairoEngine engine) {
        return new SqlExecutionContextImpl(engine, 1).with(
                AllowAllCairoSecurityContext.INSTANCE,
                null,
                null,
                -1,
                null
        );
    }

    private static void createTable(String tableName, String walMode) throws SQLException {
        runSqlViaPG("create table " + tableName + " (ts timestamp, x long) timestamp(ts) PARTITION BY DAY " + walMode);
        LOG.info().$("created table: ").utf8(tableName).$();
    }

    private static Path doesConvertFileExist(CairoEngine engine, TableToken token, boolean doesExist) {
        final Path path = Path.PATH.get().of(engine.getConfiguration().getRoot()).concat(token).concat(WalUtils.CONVERT_FILE_NAME);
        MatcherAssert.assertThat(Chars.toString(path), Files.exists(path.$()), Matchers.is(doesExist));
        return doesExist ? path : null;
    }

    private static void dropTable(String tableName) throws SQLException {
        runSqlViaPG("drop table " + tableName);
        LOG.info().$("dropped table: ").utf8(tableName).$();
    }

    private static void insertInto(String tableName) throws SQLException {
        runSqlViaPG("insert into " + tableName + " values('2016-01-01T00:00:00.000Z', 1234)");
        LOG.info().$("inserted 1 row into table: ").utf8(tableName).$();
    }

    private static void runSqlViaPG(String sql) throws SQLException {
        try (
                final Connection connection = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                final PreparedStatement stmt = connection.prepareStatement(sql)
        ) {
            stmt.execute();
        }
    }

    private static void setType(String tableName, String walMode) throws SQLException {
        runSqlViaPG("alter table " + tableName + " set type " + walMode);
        LOG.info().$("scheduled table type conversion for table ").utf8(tableName).$(" to ").$(walMode).$();
    }

    private void validateShutdown() throws SQLException {
        try {
            insertInto(tableName);
            fail("Expected exception has not been thrown");
        } catch (PSQLException psqlException) {
            TestUtils.assertContains(psqlException.getMessage(), "Connection to 127.0.0.1:" + PG_PORT + " refused.");
        }
    }
}
