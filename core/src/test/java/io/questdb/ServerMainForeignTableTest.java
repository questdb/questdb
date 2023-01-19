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

package io.questdb;

import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;

import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Files;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.*;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.*;

import static io.questdb.test.tools.TestUtils.*;
import static io.questdb.test.tools.TestUtils.assertSql;

public class ServerMainForeignTableTest extends AbstractBootstrapTest {

    private static final String TABLE_START_CONTENT = "min\tmax\tcount\n" +
            "2023-01-01T00:00:00.950399Z\t2023-01-01T23:59:59.822691Z\t90909\n" +
            "2023-01-02T00:00:00.773090Z\t2023-01-02T23:59:59.645382Z\t90909\n" +
            "2023-01-03T00:00:00.595781Z\t2023-01-03T23:59:59.468073Z\t90909\n" +
            "2023-01-04T00:00:00.418472Z\t2023-01-04T23:59:59.290764Z\t90909\n" +
            "2023-01-05T00:00:00.241163Z\t2023-01-05T23:59:59.113455Z\t90909\n" +
            "2023-01-06T00:00:00.063854Z\t2023-01-06T23:59:59.886545Z\t90910\n" +
            "2023-01-07T00:00:00.836944Z\t2023-01-07T23:59:59.709236Z\t90909\n" +
            "2023-01-08T00:00:00.659635Z\t2023-01-08T23:59:59.531927Z\t90909\n" +
            "2023-01-09T00:00:00.482326Z\t2023-01-09T23:59:59.354618Z\t90909\n" +
            "2023-01-10T00:00:00.305017Z\t2023-01-10T23:59:59.177309Z\t90909\n" +
            "2023-01-11T00:00:00.127708Z\t2023-01-11T23:59:59.000000Z\t90909\n";

    private static final String firstPartitionName = "2023-01-01";
    private static final int partitionCount = 11;
    private static final int pgPort = PG_PORT + 10;
    private static String otherVolume;
    private static String otherVolumeAlias = "SECONDARY_VOLUME";

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractBootstrapTest.setUpStatic();
        try (Path path = new Path().of(root).concat("db")) {
            int pathLen = path.length();
            Files.remove(path.concat("sys.column_versions_purge_log.lock").$());
            Files.remove(path.trimTo(pathLen).concat("telemetry_config.lock").$());
            otherVolume = AbstractBootstrapTest.temp.newFolder("path", "to", "wherever").getAbsolutePath();
            createDummyConfiguration(
                    HTTP_PORT + 10,
                    HTTP_MIN_PORT + 10,
                    pgPort,
                    ILP_PORT + 10,
                    PropertyKey.CAIRO_CREATE_ALLOWED_VOLUME_PATHS.getPropertyPath() + "=" + otherVolumeAlias + "->" + otherVolume);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @AfterClass
    public static void tearDownStatic() throws Exception {
        deleteFolder(otherVolume);
        AbstractBootstrapTest.tearDownStatic();
    }

    @Test
    public void testServerMainCreateTableIfNotExistsInVolumeTableExists() throws Exception {
        Assume.assumeFalse(Os.isWindows()); // Windows requires special privileges to create soft links
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            try (
                    ServerMain qdb = new ServerMain("-d", root.toString(), Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION);
                    SqlCompiler compiler = new SqlCompiler(qdb.getCairoEngine());
                    SqlExecutionContext context = executionContext(qdb.getCairoEngine())
            ) {
                qdb.start();
                createTableInVolume(qdb.getConfiguration().getCairoConfiguration(), compiler, context, tableName);
                createTableInVolumeIfNotExists(qdb.getConfiguration().getCairoConfiguration(), compiler, context, tableName);
                dropTable(compiler, context, tableName, true);
            }
        });
    }

    @Test
    public void testServerMainCreateTableInVolume() throws Exception {
        Assume.assumeFalse(Os.isWindows()); // Windows requires special privileges to create soft links
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            try (
                    ServerMain qdb = new ServerMain("-d", root.toString(), Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION);
                    SqlCompiler compiler = new SqlCompiler(qdb.getCairoEngine());
                    SqlExecutionContext context = executionContext(qdb.getCairoEngine())
            ) {
                qdb.start();
                createTableInVolume(qdb.getConfiguration().getCairoConfiguration(), compiler, context, tableName);
                assertSql(
                        compiler,
                        context,
                        "SELECT min(ts), max(ts), count() FROM " + tableName + " SAMPLE BY 1d ALIGN TO CALENDAR",
                        new StringSink(),
                        TABLE_START_CONTENT);
                assertTable(tableName);
                dropTable(compiler, context, tableName, true);
            }
        });
    }

    @Test
    public void testServerMainCreateTableInVolumeTableExists0() throws Exception {
        Assume.assumeFalse(Os.isWindows()); // Windows requires special privileges to create soft links
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            try (
                    ServerMain qdb = new ServerMain("-d", root.toString(), Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION);
                    SqlCompiler compiler = new SqlCompiler(qdb.getCairoEngine());
                    SqlExecutionContext context = executionContext(qdb.getCairoEngine())
            ) {
                qdb.start();
                createTable(qdb.getConfiguration().getCairoConfiguration(), compiler, context, tableName);
                try {
                    createTableInVolume(qdb.getConfiguration().getCairoConfiguration(), compiler, context, tableName);
                    Assert.fail();
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "table already exists");
                }
                dropTable(compiler, context, tableName, false);
            }
        });
    }

    @Test
    public void testServerMainCreateTableInVolumeTableExists1() throws Exception {
        Assume.assumeFalse(Os.isWindows()); // Windows requires special privileges to create soft links
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            try (
                    ServerMain qdb = new ServerMain("-d", root.toString(), Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION);
                    SqlCompiler compiler = new SqlCompiler(qdb.getCairoEngine());
                    SqlExecutionContext context = executionContext(qdb.getCairoEngine())
            ) {
                qdb.start();
                createTableInVolume(qdb.getConfiguration().getCairoConfiguration(), compiler, context, tableName);
                try {
                    createTable(qdb.getConfiguration().getCairoConfiguration(), compiler, context, tableName);
                    Assert.fail();
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "table already exists");
                }
                dropTable(compiler, context, tableName, true);
            }
        });
    }

    @Test
    public void testServerMainCreateTableInVolumeThenDrop() throws Exception {
        Assume.assumeFalse(Os.isWindows()); // Windows requires special privileges to create soft links
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            try (
                    ServerMain qdb = new ServerMain("-d", root.toString(), Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION);
                    SqlCompiler compiler = new SqlCompiler(qdb.getCairoEngine());
                    SqlExecutionContext context = executionContext(qdb.getCairoEngine())
            ) {
                qdb.start();
                for (int i = 0; i < 5; i++) {
                    createTableInVolume(qdb.getConfiguration().getCairoConfiguration(), compiler, context, tableName);
                    assertSql(
                            compiler,
                            context,
                            "SELECT min(ts), max(ts), count() FROM " + tableName + " SAMPLE BY 1d ALIGN TO CALENDAR",
                            new StringSink(),
                            TABLE_START_CONTENT);
                    assertTable(tableName);
                    dropTable(compiler, context, tableName, true);
                }
            }
        });
    }

    @Test
    public void testServerMainCreateTableMoveItsFolderAwayAndSoftLinkIt() throws Exception {
        Assume.assumeFalse(Os.isWindows()); // Windows requires special privileges to create soft links
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            // create table with some data
            try (
                    ServerMain qdb = new ServerMain("-d", root.toString(), Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION);
                    SqlCompiler compiler = new SqlCompiler(qdb.getCairoEngine());
                    SqlExecutionContext context = executionContext(qdb.getCairoEngine())
            ) {
                qdb.start();
                createTableInVolume(qdb.getConfiguration().getCairoConfiguration(), compiler, context, tableName);
                assertSql(
                        compiler,
                        context,
                        "SELECT min(ts), max(ts), count() FROM " + tableName + " SAMPLE BY 1d ALIGN TO CALENDAR",
                        new StringSink(),
                        TABLE_START_CONTENT);
                assertTable(tableName);
            }

            // copy the table to a foreign location, remove it, then symlink it
            try (
                    Path tablePath = new Path().of(root).concat(PropServerConfiguration.DB_DIRECTORY).concat(tableName).$();
                    Path filePath = new Path().of(root).concat(PropServerConfiguration.DB_DIRECTORY).concat(TableUtils.TAB_INDEX_FILE_NAME).$();
                    Path fakeTablePath = new Path().of(root).concat(PropServerConfiguration.DB_DIRECTORY).concat("coconut").$();
                    Path foreignPath = new Path().of(root).concat("banana").concat(tableName).slash$()
            ) {
                if (!Files.exists(foreignPath)) {
                    Assert.assertEquals(0, Files.mkdirs(foreignPath, 509));
                }
                Assert.assertTrue(Files.exists(foreignPath));
                TestUtils.copyDirectory(tablePath, foreignPath, 509);

                String tablePathStr = tablePath.toString();
                String foreignPathStr = foreignPath.toString();
                deleteFolder(tablePathStr);
                Assert.assertFalse(Files.exists(tablePath));
                createSoftLink(foreignPathStr, tablePathStr);
                Assert.assertTrue(Files.exists(tablePath));

                if (!Files.exists(fakeTablePath)) {
                    createSoftLink(filePath.toString(), fakeTablePath.toString());
                }
                Assert.assertTrue(Files.exists(fakeTablePath));
            }

            // check content of table after sym-linking it
            try (
                    ServerMain qdb = new ServerMain("-d", root.toString(), Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION);
                    SqlCompiler compiler = new SqlCompiler(qdb.getCairoEngine());
                    SqlExecutionContext context = executionContext(qdb.getCairoEngine())
            ) {
                qdb.start();
                assertSql(
                        compiler,
                        context,
                        "SELECT min(ts), max(ts), count() FROM " + tableName + " SAMPLE BY 1d ALIGN TO CALENDAR",
                        new StringSink(),
                        TABLE_START_CONTENT);
                assertTable(tableName);
                dropTable(compiler, context, tableName, true);
            }
        });
    }

    @Test
    public void testServerMainCreateTableWhileConcurrentCreateTable() throws Exception {
        Assume.assumeFalse(Os.isWindows()); // Windows requires special privileges to create soft links
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            try (
                    ServerMain qdb = new ServerMain("-d", root.toString(), Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION);
                    CairoEngine engine = qdb.getCairoEngine();
                    SqlCompiler compiler0 = new SqlCompiler(engine);
                    SqlCompiler compiler1 = new SqlCompiler(engine);
                    SqlExecutionContext context0 = executionContext(engine);
                    SqlExecutionContext context1 = executionContext(engine)
            ) {
                qdb.start();

                CairoConfiguration cairoConfig = qdb.getConfiguration().getCairoConfiguration();

                SOCountDownLatch startLatch = new SOCountDownLatch();
                SOCountDownLatch haltLatch = new SOCountDownLatch();

                for (int i = 0; i < 10; i++) {
                    startLatch.setCount(3);
                    haltLatch.setCount(2);
                    concurrentTableCreator(
                            "createTable",
                            cairoConfig,
                            compiler0,
                            context0,
                            startLatch,
                            haltLatch,
                            tableName,
                            false,
                            false
                    ).start();
                    concurrentTableCreator(
                            "createTableInVolume",
                            cairoConfig,
                            compiler1,
                            context1,
                            startLatch,
                            haltLatch,
                            tableName,
                            false,
                            true
                    ).start();
                    startLatch.countDown();
                    haltLatch.await();
                }
            }
        });
    }

    @Test
    public void testServerMainCreateWalTableIfNotExistsInVolumeTableExists() throws Exception {
        Assume.assumeFalse(Os.isWindows()); // Windows requires special privileges to create soft links
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            try (
                    ServerMain qdb = new ServerMain("-d", root.toString(), Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION);
                    SqlCompiler compiler = new SqlCompiler(qdb.getCairoEngine());
                    SqlExecutionContext context = executionContext(qdb.getCairoEngine())
            ) {
                qdb.start();
                createWalTableInVolume(qdb.getConfiguration().getCairoConfiguration(), compiler, context, tableName);
                createWalTableInVolumeIfNotExists(qdb.getConfiguration().getCairoConfiguration(), compiler, context, tableName);
                dropTable(compiler, context, tableName, true);
            }
        });
    }

    @Test
    public void testServerMainCreateWalTableInVolume() throws Exception {
        Assume.assumeFalse(Os.isWindows()); // Windows requires special privileges to create soft links
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            try (
                    ServerMain qdb = new ServerMain("-d", root.toString(), Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION);
                    SqlCompiler compiler = new SqlCompiler(qdb.getCairoEngine());
                    SqlExecutionContext context = executionContext(qdb.getCairoEngine())
            ) {
                qdb.start();
                createWalTableInVolume(qdb.getConfiguration().getCairoConfiguration(), compiler, context, tableName);
                assertSql(
                        compiler,
                        context,
                        "SELECT min(ts), max(ts), count() FROM " + tableName + " SAMPLE BY 1d ALIGN TO CALENDAR",
                        new StringSink(),
                        TABLE_START_CONTENT);
                assertTable(tableName);
                dropTable(compiler, context, tableName, true);
            }
        });
    }

    @Test
    public void testServerMainCreateWalTableInVolumeTableExists0() throws Exception {
        Assume.assumeFalse(Os.isWindows()); // Windows requires special privileges to create soft links
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            try (
                    ServerMain qdb = new ServerMain("-d", root.toString(), Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION);
                    SqlCompiler compiler = new SqlCompiler(qdb.getCairoEngine());
                    SqlExecutionContext context = executionContext(qdb.getCairoEngine())
            ) {
                qdb.start();
                createWalTable(qdb.getConfiguration().getCairoConfiguration(), compiler, context, tableName);
                try {
                    createWalTableInVolume(qdb.getConfiguration().getCairoConfiguration(), compiler, context, tableName);
                    Assert.fail();
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "table already exists");
                }
                dropTable(compiler, context, tableName, false);
            }
        });
    }

    @Test
    public void testServerMainCreateWalTableInVolumeTableExists1() throws Exception {
        Assume.assumeFalse(Os.isWindows()); // Windows requires special privileges to create soft links
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            try (
                    ServerMain qdb = new ServerMain("-d", root.toString(), Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION);
                    SqlCompiler compiler = new SqlCompiler(qdb.getCairoEngine());
                    SqlExecutionContext context = executionContext(qdb.getCairoEngine())
            ) {
                qdb.start();
                createWalTableInVolume(qdb.getConfiguration().getCairoConfiguration(), compiler, context, tableName);
                try {
                    createWalTable(qdb.getConfiguration().getCairoConfiguration(), compiler, context, tableName);
                    Assert.fail();
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "table already exists");
                }
                dropTable(compiler, context, tableName, true);
            }
        });
    }

    @Test
    public void testServerMainCreateWalTableInVolumeThenDrop() throws Exception {
        Assume.assumeFalse(Os.isWindows()); // Windows requires special privileges to create soft links
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            try (
                    ServerMain qdb = new ServerMain("-d", root.toString(), Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION);
                    SqlCompiler compiler = new SqlCompiler(qdb.getCairoEngine());
                    SqlExecutionContext context = executionContext(qdb.getCairoEngine())
            ) {
                qdb.start();
                for (int i = 0; i < 5; i++) {
                    createWalTableInVolume(qdb.getConfiguration().getCairoConfiguration(), compiler, context, tableName);
                    assertSql(
                            compiler,
                            context,
                            "SELECT min(ts), max(ts), count() FROM " + tableName + " SAMPLE BY 1d ALIGN TO CALENDAR",
                            new StringSink(),
                            TABLE_START_CONTENT);
                    assertTable(tableName);
                    dropTable(compiler, context, tableName, true);
                }
            }
        });
    }

    @Test
    public void testServerMainCreateWalTableWhileConcurrentCreateWalTable() throws Exception {
        Assume.assumeFalse(Os.isWindows()); // Windows requires special privileges to create soft links
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            try (
                    ServerMain qdb = new ServerMain("-d", root.toString(), Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION);
                    CairoEngine engine = qdb.getCairoEngine();
                    SqlCompiler compiler0 = new SqlCompiler(engine);
                    SqlCompiler compiler1 = new SqlCompiler(engine);
                    SqlExecutionContext context0 = executionContext(engine);
                    SqlExecutionContext context1 = executionContext(engine)
            ) {
                qdb.start();

                CairoConfiguration cairoConfig = qdb.getConfiguration().getCairoConfiguration();

                SOCountDownLatch startLatch = new SOCountDownLatch();
                SOCountDownLatch haltLatch = new SOCountDownLatch();

                for (int i = 0; i < 10; i++) {
                    startLatch.setCount(3);
                    haltLatch.setCount(2);
                    concurrentTableCreator(
                            "createWalTable",
                            cairoConfig,
                            compiler0,
                            context0,
                            startLatch,
                            haltLatch,
                            tableName,
                            true,
                            false
                    ).start();
                    concurrentTableCreator(
                            "createWalTableInVolume",
                            cairoConfig,
                            compiler1,
                            context1,
                            startLatch,
                            haltLatch,
                            tableName,
                            true,
                            true
                    ).start();
                    startLatch.countDown();
                    haltLatch.await();
                }
            }
        });
    }

    private static void assertTable(String tableName) throws Exception {
        StringSink resultSink = new StringSink();
        try (
                Connection conn = DriverManager.getConnection(getPgConnectionUri(pgPort), PG_CONNECTION_PROPERTIES);
                PreparedStatement stmt = conn.prepareStatement("select name, designatedTimestamp, partitionBy, walEnabled from tables()");
                ResultSet result = stmt.executeQuery()
        ) {
            ResultSetMetaData meta = result.getMetaData();
            int colCount = meta.getColumnCount();
            Assert.assertEquals(4, colCount);
            while (result.next()) {
                for (int i = 1; i <= colCount; i++) {
                    switch (meta.getColumnType(i)) {
                        case Types.BIT:
                            resultSink.put(result.getBoolean(i));
                            break;
                        case Types.INTEGER:
                            resultSink.put(result.getInt(i));
                            break;
                        case Types.BIGINT:
                            resultSink.put(result.getLong(i));
                            break;
                        case Types.VARCHAR:
                            resultSink.put(result.getString(i));
                            break;
                        default:
                            Assert.fail("unexpected type: " + meta.getColumnType(i));
                    }
                    resultSink.put('\t');
                }
                resultSink.clear(resultSink.length() - 1);
            }
        }
        TestUtils.assertContains(resultSink.toString(), tableName + "\tts\tDAY\tfalse");
    }

    private static void createSoftLink(String foreignPath, String tablePath) throws IOException {
        java.nio.file.Files.createSymbolicLink(Paths.get(tablePath), Paths.get(foreignPath));
    }

    private static String createTableStmt(String tableName, boolean addIfNotExists) {
        StringSink sink = new StringSink();
        sink.put("CREATE TABLE ");
        if (addIfNotExists) {
            sink.put("IF NOT EXISTS ");
        }
        sink.put(tableName).put('(');
        sink.put(" investmentMill LONG,");
        sink.put(" ticketThous INT,");
        sink.put(" broker SYMBOL INDEX CAPACITY 32,");
        sink.put(" ts TIMESTAMP");
        sink.put(") TIMESTAMP(ts) PARTITION BY DAY;");
        return sink.toString();
    }

    private static void deleteFolder(String folderName) throws IOException {
        java.nio.file.Path directory = Paths.get(folderName);
        if (directory.toFile().exists()) {
            java.nio.file.Files.walkFileTree(directory, new SimpleFileVisitor<java.nio.file.Path>() {
                @Override
                public FileVisitResult postVisitDirectory(java.nio.file.Path dir, IOException exc) throws IOException {
                    java.nio.file.Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(java.nio.file.Path file, BasicFileAttributes attrs) throws IOException {
                    java.nio.file.Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }
            });
        }
    }

    private static SqlExecutionContext executionContext(CairoEngine engine) {
        return new SqlExecutionContextImpl(engine, 1).with(
                AllowAllCairoSecurityContext.INSTANCE,
                null,
                null,
                -1,
                null);
    }

    private Thread concurrentTableCreator(
            String threadName,
            CairoConfiguration cairoConfig,
            SqlCompiler compiler,
            SqlExecutionContext context,
            SOCountDownLatch startLatch,
            SOCountDownLatch haltLatch,
            String tableName,
            boolean isWal,
            boolean isInVolume
    ) {
        return new Thread(() -> {
            try {
                startLatch.countDown();
                startLatch.await();
                Os.pause();
                if (isInVolume) {
                    if (isWal) {
                        createWalTableInVolume(cairoConfig, compiler, context, tableName);
                    } else {
                        createTableInVolume(cairoConfig, compiler, context, tableName);
                    }
                } else {
                    if (isWal) {
                        createWalTable(cairoConfig, compiler, context, tableName);
                    } else {
                        createTable(cairoConfig, compiler, context, tableName);
                    }
                }
                assertTable(tableName);
            } catch (Throwable thr) {
                TestUtils.assertContains(thr.getMessage(), "[13] table already exists");
                long startTs = System.currentTimeMillis();
                boolean tableAsserted = false;
                while (System.currentTimeMillis() - startTs < 500L) {
                    try {
                        assertTable(tableName);
                        tableAsserted = true;
                        break;
                    } catch (Throwable ignore) {
                        // no-op
                    }
                }
                Assert.assertTrue(tableAsserted);
                try {
                    dropTable(compiler, context, tableName, !isInVolume);
                } catch (Throwable unexpected) {
                    Assert.fail("unexpected: " + unexpected.getMessage());
                }
            } finally {
                haltLatch.countDown();
                Path.clearThreadLocals();
            }
        }, threadName);
    }

    private void createPopulateTable(
            CairoConfiguration cairoConfig,
            SqlCompiler compiler,
            SqlExecutionContext context,
            String tableName,
            boolean isWal,
            boolean inVolume,
            boolean addIfNotExists
    ) throws Exception {
        String createStmt = createTableStmt(tableName, addIfNotExists);
        if (isWal) {
            createStmt += " WAL";
        }
        if (inVolume) {
            createStmt += " IN VOLUME '" + otherVolumeAlias + '\'';
        }
        try (OperationFuture op = compiler.compile(createStmt, context).execute(null)) {
            op.await();
        }
        try (
                TableModel tableModel = new TableModel(cairoConfig, tableName, PartitionBy.DAY)
                        .col("investmentMill", ColumnType.LONG)
                        .col("ticketThous", ColumnType.INT)
                        .col("broker", ColumnType.SYMBOL).symbolCapacity(32)
                        .timestamp("ts");
                OperationFuture op = compiler.compile(insertFromSelectPopulateTableStmt(tableModel, 1000000, firstPartitionName, partitionCount), context).execute(null)
        ) {
            op.await();
        }
    }

    private void createTable(CairoConfiguration cairoConfig, SqlCompiler compiler, SqlExecutionContext context, String tableName) throws Exception {
        createPopulateTable(cairoConfig, compiler, context, tableName, false, false, false);
    }

    private void createTableInVolume(CairoConfiguration cairoConfig, SqlCompiler compiler, SqlExecutionContext context, String tableName) throws Exception {
        createPopulateTable(cairoConfig, compiler, context, tableName, false, true, false);
    }

    private void createTableInVolumeIfNotExists(CairoConfiguration cairoConfig, SqlCompiler compiler, SqlExecutionContext context, String tableName) throws Exception {
        createPopulateTable(cairoConfig, compiler, context, tableName, false, true, true);
    }

    private void createWalTable(CairoConfiguration cairoConfig, SqlCompiler compiler, SqlExecutionContext context, String tableName) throws Exception {
        createPopulateTable(cairoConfig, compiler, context, tableName, true, false, false);
    }

    private void createWalTableInVolume(CairoConfiguration cairoConfig, SqlCompiler compiler, SqlExecutionContext context, String tableName) throws Exception {
        createPopulateTable(cairoConfig, compiler, context, tableName, true, true, false);
    }

    private void createWalTableInVolumeIfNotExists(CairoConfiguration cairoConfig, SqlCompiler compiler, SqlExecutionContext context, String tableName) throws Exception {
        createPopulateTable(cairoConfig, compiler, context, tableName, true, true, true);
    }

    private void dropTable(SqlCompiler compiler, SqlExecutionContext context, String tableName, boolean isInVolume) throws Exception {
        try (OperationFuture op = compiler.compile("DROP TABLE " + tableName, context).execute(null)) {
            op.await();
        }
        if (isInVolume) {
            // drop simply unlinks, the folder remains, it is a feature as the requirements need further refinement
            deleteFolder(otherVolume + Files.SEPARATOR + tableName); // delete the table's folder in the other volume
        }
    }

    static {
        // log is needed to greedily allocate logger infra and
        // exclude it from leak detector
        LogFactory.getLog(ServerMainForeignTableTest.class);
    }
}
