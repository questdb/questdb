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
import io.questdb.std.Files;
import io.questdb.std.Misc;
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

    private String OTHER_VOLUME;

    @Before
    public void setUp() throws IOException {
        try (Path path = new Path().of(root).concat("db")) {
            int plen = path.length();
            Files.remove(path.concat("sys.column_versions_purge_log.lock").$());
            Files.remove(path.trimTo(plen).concat("telemetry_config.lock").$());
            OTHER_VOLUME = AbstractBootstrapTest.temp.newFolder("path", "to", "wherever").getAbsolutePath();
            createDummyConfiguration(PropertyKey.CAIRO_CREATE_ALLOWED_VOLUME_PATHS.getPropertyPath() + "=" + OTHER_VOLUME);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @After
    public void tearDown() throws IOException {
        deleteFolder(OTHER_VOLUME);
    }

    @Test
    public void testServerMainCreateTableInAllowedVolume() throws Exception {
        Assume.assumeFalse(Os.isWindows()); // Windows requires special privileges to create soft links
        String tableName = "oscar";
        assertMemoryLeak(() -> {
            try (
                    ServerMain qdb = new ServerMain("-d", root.toString(), Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION);
                    SqlCompiler compiler = new SqlCompiler(qdb.getCairoEngine());
                    SqlExecutionContext context = executionContext(qdb.getCairoEngine())
            ) {
                qdb.start();
                PropServerConfiguration.setCreateTableInVolumeAllowed(true);
                createTableInVolume(qdb.getConfiguration().getCairoConfiguration(), compiler, context, tableName);
                assertSql(
                        compiler,
                        context,
                        "SELECT min(ts), max(ts), count() FROM " + tableName + " SAMPLE BY 1d ALIGN TO CALENDAR",
                        Misc.getThreadLocalBuilder(),
                        TABLE_START_CONTENT);
                assertTables(tableName);
                dropTable(compiler, context, tableName, true);
            }
        });
    }

    @Test
    public void testServerMainCreateTableInAllowedVolumeNotAllowedByDefault() throws Exception {
        Assume.assumeFalse(Os.isWindows()); // Windows requires special privileges to create soft links
        String tableName = "charlie";
        assertMemoryLeak(() -> {
            try (
                    ServerMain qdb = new ServerMain("-d", root.toString(), Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION);
                    SqlCompiler compiler = new SqlCompiler(qdb.getCairoEngine());
                    SqlExecutionContext context = executionContext(qdb.getCairoEngine())
            ) {
                qdb.start();
                try {
                    createTableInVolume(qdb.getConfiguration().getCairoConfiguration(), compiler, context, tableName);
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "volume path is not allowed [path=" + OTHER_VOLUME + ']');
                }
            }
        });
    }

    @Test
    public void testServerMainCreateTableInAllowedVolumeTableExists0() throws Exception {
        Assume.assumeFalse(Os.isWindows()); // Windows requires special privileges to create soft links
        String tableName = "albert";
        assertMemoryLeak(() -> {
            try (
                    ServerMain qdb = new ServerMain("-d", root.toString(), Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION);
                    SqlCompiler compiler = new SqlCompiler(qdb.getCairoEngine());
                    SqlExecutionContext context = executionContext(qdb.getCairoEngine())
            ) {
                qdb.start();
                PropServerConfiguration.setCreateTableInVolumeAllowed(true);
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
    public void testServerMainCreateTableInAllowedVolumeTableExists1() throws Exception {
        Assume.assumeFalse(Os.isWindows()); // Windows requires special privileges to create soft links
        String tableName = "lusitania";
        assertMemoryLeak(() -> {
            try (
                    ServerMain qdb = new ServerMain("-d", root.toString(), Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION);
                    SqlCompiler compiler = new SqlCompiler(qdb.getCairoEngine());
                    SqlExecutionContext context = executionContext(qdb.getCairoEngine())
            ) {
                qdb.start();
                PropServerConfiguration.setCreateTableInVolumeAllowed(true);
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
    public void testServerMainCreateTableInAllowedVolumeThenDrop() throws Exception {
        Assume.assumeFalse(Os.isWindows()); // Windows requires special privileges to create soft links
        String tableName = "evil_hear";
        assertMemoryLeak(() -> {
            try (
                    ServerMain qdb = new ServerMain("-d", root.toString(), Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION);
                    SqlCompiler compiler = new SqlCompiler(qdb.getCairoEngine());
                    SqlExecutionContext context = executionContext(qdb.getCairoEngine())
            ) {
                qdb.start();
                PropServerConfiguration.setCreateTableInVolumeAllowed(true);
                for (int i = 0; i < 5; i++) {
                    createTableInVolume(qdb.getConfiguration().getCairoConfiguration(), compiler, context, tableName);
                    assertSql(
                            compiler,
                            context,
                            "SELECT min(ts), max(ts), count() FROM " + tableName + " SAMPLE BY 1d ALIGN TO CALENDAR",
                            Misc.getThreadLocalBuilder(),
                            TABLE_START_CONTENT);
                    assertTables(tableName);
                    dropTable(compiler, context, tableName, true);
                }
            }
        });
    }

    @Test
    public void testServerMainCreateTableMoveItsFolderAwayAndSoftLinkIt() throws Exception {
        Assume.assumeFalse(Os.isWindows()); // Windows requires special privileges to create soft links
        String tableName = "sponsors";
        assertMemoryLeak(() -> {
            // create table with some data
            try (
                    ServerMain qdb = new ServerMain("-d", root.toString(), Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION);
                    SqlCompiler compiler = new SqlCompiler(qdb.getCairoEngine());
                    SqlExecutionContext context = executionContext(qdb.getCairoEngine())
            ) {
                qdb.start();
                PropServerConfiguration.setCreateTableInVolumeAllowed(true);
                createTableInVolume(qdb.getConfiguration().getCairoConfiguration(), compiler, context, tableName);
                assertSql(
                        compiler,
                        context,
                        "SELECT min(ts), max(ts), count() FROM " + tableName + " SAMPLE BY 1d ALIGN TO CALENDAR",
                        Misc.getThreadLocalBuilder(),
                        TABLE_START_CONTENT);
                assertTables(tableName);
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
                        Misc.getThreadLocalBuilder(),
                        TABLE_START_CONTENT);
                assertTables(tableName);
                dropTable(compiler, context, tableName, true);
            }
        });
    }

    private static void assertTables(String tableName) throws Exception {
        StringSink sink = new StringSink();
        try (
                Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                PreparedStatement stmt = conn.prepareStatement("tables()");
                ResultSet result = stmt.executeQuery()
        ) {
            ResultSetMetaData meta = result.getMetaData();
            int colCount = meta.getColumnCount();
            while (result.next()) {
                // ignore table id
                for (int i = 2; i <= colCount; i++) {
                    switch (meta.getColumnType(i)) {
                        case Types.BIT:
                            sink.put(result.getBoolean(i));
                            break;
                        case Types.INTEGER:
                            sink.put(result.getInt(i));
                            break;
                        case Types.BIGINT:
                            sink.put(result.getLong(i));
                            break;
                        case Types.VARCHAR:
                            sink.put(result.getString(i));
                            break;
                        default:
                            throw new IllegalStateException("unexpected type: " + meta.getColumnType(i));

                    }
                    sink.put('\t');
                }
                sink.clear(sink.length() - 1);
            }
        }
        TestUtils.assertEquals(tableName + "\tts\tDAY\t500000\t600000000\tfalse\t" + tableName, sink.toString());
    }

    private static void createSoftLink(String foreignPath, String tablePath) throws IOException {
        java.nio.file.Files.createSymbolicLink(Paths.get(tablePath), Paths.get(foreignPath));
    }

    private static String createTableStmt(String tableName) {
        return "CREATE TABLE " + tableName + '(' +
                " investmentMill LONG," +
                " ticketThous INT," +
                " broker SYMBOL INDEX CAPACITY 32," +
                " ts TIMESTAMP"
                + ") TIMESTAMP(ts) PARTITION BY DAY";
    }

    private static void deleteFolder(String folderName) throws IOException {
        java.nio.file.Path directory = Paths.get(folderName);
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

    private static SqlExecutionContext executionContext(CairoEngine engine) {
        return new SqlExecutionContextImpl(engine, 1).with(
                AllowAllCairoSecurityContext.INSTANCE,
                null,
                null,
                -1,
                null);
    }

    private void createPopulateTable(CairoConfiguration cairoConfig, SqlCompiler compiler, SqlExecutionContext context, String tableName, boolean inVolume) throws Exception {
        String createStmt = createTableStmt(tableName);
        if (inVolume) {
            createStmt += " IN VOLUME '" + OTHER_VOLUME + '\'';
        }
        try (OperationFuture op = compiler.compile(createStmt, context).execute(null)) {
            op.await();
        }
        try (TableModel tableModel = new TableModel(cairoConfig, tableName, PartitionBy.DAY)
                .col("investmentMill", ColumnType.LONG)
                .col("ticketThous", ColumnType.INT)
                .col("broker", ColumnType.SYMBOL).symbolCapacity(32)
                .timestamp("ts")) {
            compiler.compile(insertFromSelectPopulateTableStmt(tableModel, 1000000, firstPartitionName, partitionCount), context);
        }
    }

    private void createTable(CairoConfiguration cairoConfig, SqlCompiler compiler, SqlExecutionContext context, String tableName) throws Exception {
        createPopulateTable(cairoConfig, compiler, context, tableName, false);
    }

    private void createTableInVolume(CairoConfiguration cairoConfig, SqlCompiler compiler, SqlExecutionContext context, String tableName) throws Exception {
        createPopulateTable(cairoConfig, compiler, context, tableName, true);
    }

    private void dropTable(SqlCompiler compiler, SqlExecutionContext context, String tableName, boolean isInVolume) throws Exception {
        try (OperationFuture op = compiler.compile("DROP TABLE " + tableName, context).execute(null)) {
            op.await();
        }
        if (isInVolume) {
            // drop simply unlinks, the folder remains, it is a feature as the requirements need further refinement
            deleteFolder(OTHER_VOLUME + Files.SEPARATOR + tableName); // delete the table's folder in the other volume
        }
    }

    static {
        // log is needed to greedily allocate logger infra and
        // exclude it from leak detector
        LogFactory.getLog(ServerMainForeignTableTest.class);
    }
}
