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

package io.questdb.test.griffin;

import io.questdb.PropServerConfiguration;
import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import java.util.HashMap;

import static io.questdb.test.griffin.AlterTableSetTypeTest.NON_WAL;
import static io.questdb.test.griffin.AlterTableSetTypeTest.WAL;
import static org.junit.Assert.*;

public class AlterTableSetTypeRestartTest extends AbstractAlterTableSetTypeRestartTest {

    @Override
    @Before
    public void setUp() {
        TestUtils.unchecked(() -> createDummyConfiguration(PropertyKey.CAIRO_WAL_SUPPORTED.getPropertyPath() + "=true"));
    }

    @Test
    @Ignore
    public void testConvertLoop2() throws Exception {
        final String tableName = testName.getMethodName();
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            }})) {
                serverMain.start();
                createTable(tableName, "WAL");

                final CairoEngine engine = serverMain.getEngine();
                final TableToken token = engine.verifyTableName(tableName);

                insertInto(tableName);
                insertInto(tableName);
                insertInto(tableName);
                drainWalQueue(engine);

                // WAL table
                assertTrue(engine.isWalTable(token));
                setType(tableName, "BYPASS WAL");
                assertNumOfRows(engine, tableName, 3);
            }
            validateShutdown(tableName);

            // restart
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            }})) {
                serverMain.start();

                final CairoEngine engine = serverMain.getEngine();
                final TableToken token = engine.verifyTableName(tableName);

                // table has been converted to non-WAL
                assertFalse(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 3);
                setType(tableName, "WAL");
            }
            validateShutdown(tableName);

            // restart
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            }})) {
                serverMain.start();

                final CairoEngine engine = serverMain.getEngine();
                final TableToken token = engine.verifyTableName(tableName);

                // table has been converted to WAL
                assertTrue(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 3);

                insertInto(tableName);
                drainWalQueue(engine);
                assertNumOfRows(engine, tableName, 4);

                dropTable(tableName);
            }
            validateShutdown(tableName);
        });
    }

    @Test
    public void testNonPartitionedToWal() throws Exception {
        final String tableName = testName.getMethodName();
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
                put(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED.getEnvVarName(), "false");
            }})) {
                serverMain.start();
                createNonPartitionedTable(tableName);
                insertInto(tableName);

                final CairoEngine engine = serverMain.getEngine();
                final TableToken token = engine.verifyTableName(tableName);

                // non-WAL table
                assertFalse(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 1);
                assertConvertFileDoesNotExist(engine, token);

                // table conversion to WAL is not allowed
                try {
                    setType(tableName, "WAL");
                    fail("Expected exception is missing");
                } catch (PSQLException e) {
                    TestUtils.assertContains(e.getMessage(), "Cannot convert non-partitioned table");
                }
            }
        });
    }

    @Test
    @Ignore
    public void testNonWalToWal() throws Exception {
        final String tableName = testName.getMethodName();
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            }})) {
                serverMain.start();
                createTable(tableName, "BYPASS WAL");
                insertInto(tableName);

                final CairoEngine engine = serverMain.getEngine();
                final TableToken token = engine.verifyTableName(tableName);

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
            validateShutdown(tableName);

            // restart
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            }})) {
                serverMain.start();

                final CairoEngine engine = serverMain.getEngine();
                final TableToken token = engine.verifyTableName(tableName);

                // WAL table
                assertTrue(engine.isWalTable(token));
                assertSeqTxn(engine, token, 0);
                assertNumOfRows(engine, tableName, 2);
                assertConvertFileDoesNotExist(engine, token);

                dropTable(tableName);
            }
        });
    }

    @Test
    @Ignore
    public void testNonWalToWalWithDropTable() throws Exception {
        final String tableName = testName.getMethodName();
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            }})) {
                serverMain.start();
                createTable(tableName, "BYPASS WAL");
                insertInto(tableName);

                final CairoEngine engine = serverMain.getEngine();
                final TableToken token = engine.verifyTableName(tableName);

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
            validateShutdown(tableName);

            // restart
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            }})) {
                serverMain.start();

                final CairoEngine engine = serverMain.getEngine();
                try {
                    engine.verifyTableName(tableName);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "table does not exist [table=" + tableName + ']');
                }
            }
        });
    }

    @Test
    @Ignore
    public void testNonWalToWalWithTxn() throws Exception {
        final String tableName = testName.getMethodName();
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            }})) {
                serverMain.start();
                createTable(tableName, "BYPASS WAL");

                insertInto(tableName);
                insertInto(tableName);
                insertInto(tableName);

                final CairoEngine engine = serverMain.getEngine();
                final TableToken token = engine.verifyTableName(tableName);
                setSeqTxn(engine, token);

                // non-WAL table
                assertFalse(engine.isWalTable(token));
                assertSeqTxn(engine, token, 12345L);
                assertNumOfRows(engine, tableName, 3);
                assertConvertFileDoesNotExist(engine, token);

                // schedule table conversion to WAL
                setType(tableName, "WAL");
                final Path path = assertConvertFileExists(engine, token);
                assertConvertFileContent(path, WAL);
            }
            validateShutdown(tableName);

            // restart
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            }})) {
                serverMain.start();
                insertInto(tableName);
                insertInto(tableName);
                insertInto(tableName);

                final CairoEngine engine = serverMain.getEngine();
                final TableToken token = engine.verifyTableName(tableName);
                assertSeqTxn(engine, token, 0L);

                drainWalQueue(engine);
                assertNumOfRows(engine, tableName, 6);
                assertSeqTxn(engine, token, 3L);
            }
        });
    }

    @Test
    @Ignore
    public void testSetType() throws Exception {
        final String tableName = testName.getMethodName();
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            }})) {
                serverMain.start();
                createTable(tableName, "BYPASS WAL");
                insertInto(tableName);

                final CairoEngine engine = serverMain.getEngine();
                final TableToken token = engine.verifyTableName(tableName);

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
            validateShutdown(tableName);

            // restart
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            }})) {
                serverMain.start();

                final CairoEngine engine = serverMain.getEngine();
                final TableToken token = engine.verifyTableName(tableName);

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
            validateShutdown(tableName);

            // restart
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            }})) {
                serverMain.start();

                final CairoEngine engine = serverMain.getEngine();
                final TableToken token = engine.verifyTableName(tableName);

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
            validateShutdown(tableName);

            // restart
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            }})) {
                serverMain.start();
                final CairoEngine engine = serverMain.getEngine();
                final TableToken token = engine.verifyTableName(tableName);

                // no conversion happened, table was already non-WAL type
                assertFalse(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 6);
                assertConvertFileDoesNotExist(engine, token);

                // schedule table conversion to WAL
                setType(tableName, "WAL");
                final Path path = assertConvertFileExists(engine, token);
                assertConvertFileContent(path, WAL);
            }
            validateShutdown(tableName);

            // restart
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            }})) {
                serverMain.start();

                final CairoEngine engine = serverMain.getEngine();
                final TableToken token = engine.verifyTableName(tableName);

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

                dropTable(tableName);
                drainWalQueue(engine);
            }
            validateShutdown(tableName);
        });
    }

    @Test
    @Ignore
    public void testWalToNonWal() throws Exception {
        final String tableName = testName.getMethodName();
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            }})) {
                serverMain.start();
                createTable(tableName, "WAL");

                final CairoEngine engine = serverMain.getEngine();
                final TableToken token = engine.verifyTableName(tableName);

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
            }
            validateShutdown(tableName);

            // restart
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            }})) {
                serverMain.start();

                final CairoEngine engine = serverMain.getEngine();
                final TableToken token = engine.verifyTableName(tableName);
                assertFalse(engine.isWalTable(token));
                assertSeqTxn(engine, token, 0);
                insertInto(tableName);
                assertNumOfRows(engine, tableName, 3);

                dropTable(tableName);
            }
        });
    }

    @Test
    @Ignore
    public void testWalToNonWalWithDropTable() throws Exception {
        final String tableName = testName.getMethodName();
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            }})) {
                serverMain.start();
                createTable(tableName, "WAL");

                final CairoEngine engine = serverMain.getEngine();
                final TableToken token = engine.verifyTableName(tableName);

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
            validateShutdown(tableName);

            // restart
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            }})) {
                serverMain.start();

                final CairoEngine engine = serverMain.getEngine();
                try {
                    engine.verifyTableName(tableName);
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "table does not exist [table=" + tableName + ']');
                }
            }
        });
    }
}
