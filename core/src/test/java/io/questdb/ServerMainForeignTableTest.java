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
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.griffin.SqlCompiler;
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

import static io.questdb.cairo.TableUtils.createTable;
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

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractBootstrapTest.setUpStatic();
        try {
            createDummyConfiguration();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Before
    public void setUp() {
        try (Path path = new Path().of(root).concat("db")) {
            int plen = path.length();
            Files.remove(path.concat("sys.column_versions_purge_log.lock").$());
            Files.remove(path.trimTo(plen).concat("telemetry_config.lock").$());
        }
    }

    @Test
    public void testServerMainCreateTableMoveItsFolderAwayAndSoftLinkIt() throws Exception {

        Assume.assumeTrue(!Os.isWindows()); // windows requires special privileges to create soft links

        String tableName = "sponsors";
        String firstPartitionName = "2023-01-01";
        int partitionCount = 11;

        assertMemoryLeak(() -> {

            // create table with some data
            try (
                    ServerMain qdb = new ServerMain("-d", root.toString(), Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION);
                    SqlCompiler compiler = new SqlCompiler(qdb.getCairoEngine());
                    SqlExecutionContext context = executionContext(qdb.getCairoEngine())
            ) {
                qdb.start();
                CairoConfiguration cairoConfig = qdb.getConfiguration().getCairoConfiguration();
                try (
                        TableModel tableModel = new TableModel(cairoConfig, tableName, PartitionBy.DAY)
                                .col("investmentMill", ColumnType.LONG)
                                .col("ticketThous", ColumnType.INT)
                                .col("broker", ColumnType.SYMBOL).symbolCapacity(32)
                                .timestamp("ts");
                        MemoryMARW mem = Vm.getMARWInstance();
                        Path path = new Path().of(cairoConfig.getRoot()).concat(tableName)
                ) {
                    createTable(cairoConfig, mem, path, tableModel, 1);
                    compiler.compile(insertFromSelectPopulateTableStmt(tableModel, 1000000, firstPartitionName, partitionCount), context);
                }
                StringSink sink = Misc.getThreadLocalBuilder();
                assertSql(
                        compiler,
                        context,
                        "SELECT min(ts), max(ts), count() FROM " + tableName + " SAMPLE BY 1d ALIGN TO CALENDAR",
                        sink,
                        TABLE_START_CONTENT);
                assertSql(compiler, context, "tables()", sink, "ts1\tsponsors\tts\tDAY\t500000\t600000000\tfalse\n");
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
                StringSink sink = Misc.getThreadLocalBuilder();
                assertSql(
                        compiler,
                        context,
                        "SELECT min(ts), max(ts), count() FROM " + tableName + " SAMPLE BY 1d ALIGN TO CALENDAR",
                        sink,
                        TABLE_START_CONTENT);
                assertSql(compiler, context, "tables()", sink, "ts1\tsponsors\tts\tDAY\t500000\t600000000\tfalse\n");
            }
        });
    }

    private static void createSoftLink(String foreignPath, String tablePath) throws IOException {
        java.nio.file.Files.createSymbolicLink(Paths.get(tablePath), Paths.get(foreignPath));
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

    static {
        // log is needed to greedily allocate logger infra and
        // exclude it from leak detector
        LogFactory.getLog(ServerMainForeignTableTest.class);
    }
}
