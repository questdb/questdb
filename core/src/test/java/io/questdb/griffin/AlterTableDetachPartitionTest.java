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

import io.questdb.cairo.*;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.std.*;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.*;

import java.util.function.Function;

import static io.questdb.cairo.TableUtils.DETACHED_DIR_MARKER;
import static io.questdb.cairo.TableUtils.META_FILE_NAME;


public class AlterTableDetachPartitionTest extends AbstractGriffinTest {

    private static Path path;
    private static Path brokenMetaPath;
    private static int pathRootLen;


    @BeforeClass
    public static void setUpStatic() {
        AbstractGriffinTest.setUpStatic();
        path = new Path().of(configuration.getRoot());
        brokenMetaPath = new Path().of(configuration.getRoot());
        pathRootLen = path.length();
    }

    @AfterClass
    public static void tearDownStatic() {
        AbstractGriffinTest.tearDownStatic();
        Misc.free(path);
        Misc.free(brokenMetaPath);
    }

    @Test
    public void testDetachSyntaxErrorPartitionMissing() throws Exception {
        try (TableModel tableModel = new TableModel(configuration, "tab", PartitionBy.DAY).timestamp()) {
            AbstractSqlParserTest.assertSyntaxError(
                    "ALTER TABLE tab DETACH '2022-06-27'",
                    23,
                    "'partition' expected",
                    tableModel
            );
        }
    }

    @Test
    public void testDetachSyntaxErrorPartitionNameExpected() throws Exception {
        try (TableModel tableModel = new TableModel(configuration, "tab", PartitionBy.DAY).timestamp()) {
            AbstractSqlParserTest.assertSyntaxError(
                    "ALTER TABLE tab DETACH PARTITION",
                    32,
                    "'list' or 'where' expected",
                    tableModel
            );
        }
    }

    @Test
    public void testDetachSyntaxError2() throws Exception {
        try (TableModel tableModel = new TableModel(configuration, "tab", PartitionBy.DAY).timestamp()) {
            AbstractSqlParserTest.assertSyntaxError(
                    "ALTER TABLE tab DETACH PARTITION LIST",
                    37,
                    "partition name expected",
                    tableModel
            );
        }
    }

    @Test
    public void testDetachNotPartitioned0() throws Exception {
        try (TableModel tableModel = new TableModel(configuration, "tab", PartitionBy.NONE).timestamp()) {
            AbstractSqlParserTest.assertSyntaxError(
                    "ALTER TABLE tab DETACH PARTITION LIST '2022-06-27'",
                    23,
                    "table is not partitioned",
                    tableModel
            );
        }
    }

    @Test
    public void testDetachNotPartitioned1() throws Exception {
        assertFailedDetachOperation(
                "ALTER TABLE tab DETACH PARTITION LIST '2022-06-27'",
                "could not detach [statusCode=PARTITION_EMPTY, table=tab, partition='2022-06-27']"
        );
    }

    @Test
    public void testDetachNoDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel tab = new TableModel(configuration, "tab", PartitionBy.DAY)) {
                CairoTestUtils.create(tab
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG)
                );
                try {
                    compile("ALTER TABLE tab DETACH PARTITION LIST '2022-06-27'", sqlExecutionContext);
                    Assert.fail();
                } catch (AssertionError e) {
                    Assert.assertEquals(-1, tab.getTimestampIndex());
                }
            }
        });
    }

    @Test
    public void testDetachPartitionEmpty() throws Exception {
        assertFailedDetachOperation(
                "ALTER TABLE tab DETACH PARTITION LIST '2022-06-06'",
                "could not detach [statusCode=PARTITION_EMPTY, table=tab, partition='2022-06-06']"
        );
    }


    @Test
    public void testCannotDetachActivePartition() throws Exception {
        assertFailedDetachOperation(
                "ALTER TABLE tab DETACH PARTITION LIST '2022-06-05'",
                "could not detach [statusCode=PARTITION_IS_ACTIVE, table=tab, partition='2022-06-05']"
        );
    }

    @Test
    public void testAlreadyDetached() throws Exception {
        assertFailedDetachOperation(
                "ALTER TABLE tab DETACH PARTITION LIST '2022-06-03', '2022-06-03'",
                "could not detach [statusCode=PARTITION_ALREADY_DETACHED, table=tab, partition='2022-06-03']"
        );
    }

    @Test
    public void testCannotRenameFolder() throws Exception {
        FilesFacade ff = new FilesFacadeImpl() {
            @Override
            public boolean rename(LPSZ from, LPSZ to) {
                return false;
            }
        };
        assertFailedDetachOperation(
                ff,
                "ALTER TABLE tab DETACH PARTITION LIST '2022-06-03'",
                "could not detach [statusCode=PARTITION_FOLDER_CANNOT_RENAME, table=tab, partition='2022-06-03']"
        );
    }

    @Test
    public void testCannotCopyMeta() throws Exception {
        FilesFacade ff = new FilesFacadeImpl() {
            @Override
            public int copy(LPSZ from, LPSZ to) {
                return -1;
            }
        };
        assertFailedDetachOperation(
                ff,
                "ALTER TABLE tab DETACH PARTITION LIST '2022-06-03'",
                "could not detach [statusCode=PARTITION_CANNOT_COPY_META, table=tab, partition='2022-06-03']"
        );
    }

    @Test
    public void testDetachAttachPartition() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel tab = new TableModel(configuration, "tab", PartitionBy.DAY)) {
                createPopulateTable(tab
                                .timestamp("ts")
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG),
                        10,
                        "2022-06-01",
                        3
                );

                String expected = "ts\ti\tl\n" +
                        "2022-06-01T07:11:59.900000Z\t1\t1\n" +
                        "2022-06-01T14:23:59.800000Z\t2\t2\n" +
                        "2022-06-01T21:35:59.700000Z\t3\t3\n" +
                        "2022-06-02T04:47:59.600000Z\t4\t4\n" +
                        "2022-06-02T11:59:59.500000Z\t5\t5\n" +
                        "2022-06-02T19:11:59.400000Z\t6\t6\n" +
                        "2022-06-03T02:23:59.300000Z\t7\t7\n" +
                        "2022-06-03T09:35:59.200000Z\t8\t8\n" +
                        "2022-06-03T16:47:59.100000Z\t9\t9\n" +
                        "2022-06-03T23:59:59.000000Z\t10\t10\n";

                assertContent(expected, "tab", "ts");
                compile("ALTER TABLE tab DETACH PARTITION LIST '2022-06-01', '2022-06-02'", sqlExecutionContext);
                assertContent("ts\ti\tl\n" +
                                "2022-06-03T02:23:59.300000Z\t7\t7\n" +
                                "2022-06-03T09:35:59.200000Z\t8\t8\n" +
                                "2022-06-03T16:47:59.100000Z\t9\t9\n" +
                                "2022-06-03T23:59:59.000000Z\t10\t10\n",
                        "tab",
                        "ts"
                );

                compile("ALTER TABLE tab ATTACH PARTITION LIST '2022-06-01', '2022-06-02'", sqlExecutionContext);
                assertContent(expected, "tab", "ts");
            }
        });
    }

    @Test
    public void testDetachAttachPartitionMissingMetadata() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel tab = new TableModel(configuration, "tab", PartitionBy.DAY)) {
                createPopulateTable(tab
                                .timestamp("ts")
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG),
                        10,
                        "2022-06-01",
                        3
                );

                compile("ALTER TABLE tab DETACH PARTITION LIST '2022-06-01', '2022-06-02'", sqlExecutionContext);

                // remove _meta.detached simply prevents metadata checking, all else is the same
                path.trimTo(pathRootLen)
                        .concat("tab")
                        .concat("2022-06-02")
                        .put(DETACHED_DIR_MARKER)
                        .concat(META_FILE_NAME)
                        .put(DETACHED_DIR_MARKER)
                        .$();
                Assert.assertTrue(configuration.getFilesFacade().remove(path));

                compile("ALTER TABLE tab ATTACH PARTITION LIST '2022-06-01', '2022-06-02'", sqlExecutionContext);
                assertContent(
                        "ts\ti\tl\n" +
                                "2022-06-01T07:11:59.900000Z\t1\t1\n" +
                                "2022-06-01T14:23:59.800000Z\t2\t2\n" +
                                "2022-06-01T21:35:59.700000Z\t3\t3\n" +
                                "2022-06-02T04:47:59.600000Z\t4\t4\n" +
                                "2022-06-02T11:59:59.500000Z\t5\t5\n" +
                                "2022-06-02T19:11:59.400000Z\t6\t6\n" +
                                "2022-06-03T02:23:59.300000Z\t7\t7\n" +
                                "2022-06-03T09:35:59.200000Z\t8\t8\n" +
                                "2022-06-03T16:47:59.100000Z\t9\t9\n" +
                                "2022-06-03T23:59:59.000000Z\t10\t10\n",
                        "tab",
                        "ts"
                );
            }
        });
    }

    @Test
    public void testDetachAttachPartitionBrokenMetadataTableId() throws Exception {
        assertFailedAttachOperationBecauseOfMetadata(
                2,
                brokenMeta -> brokenMeta
                        .timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG),
                "insert into brokenMeta " +
                        "select " +
                        "CAST(1654041600000000L AS TIMESTAMP) + x * 3455990000  ts, " +
                        "cast(x as int) i, " +
                        "x l " +
                        "from long_sequence(100))",
                null,
                "[-100] Detached partition metadata [id] is not compatible with current table metadata"
        );
    }

    @Test
    public void testDetachAttachPartitionBrokenMetadataStructureVersion() throws Exception {
        assertFailedAttachOperationBecauseOfMetadata(
                1,
                brokenMeta -> brokenMeta
                        .timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG),
                "insert into brokenMeta " +
                        "select " +
                        "CAST(1654041600000000L AS TIMESTAMP) + x * 3455990000  ts, " +
                        "cast(x as int) i, " +
                        "x l " +
                        "from long_sequence(100))",
                "ALTER TABLE brokenMeta ADD COLUMN s SHORT",
                "[-100] Detached partition metadata [structure_version] is not compatible with current table metadata"
        );
    }

    @Test
    public void testDetachAttachPartitionBrokenMetadataTimestampIndex() throws Exception {
        assertFailedAttachOperationBecauseOfMetadata(
                brokenMeta -> brokenMeta
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG)
                        .timestamp("ts"),
                "insert into brokenMeta " +
                        "select " +
                        "cast(x as int) i, " +
                        "x l, " +
                        "CAST(1654041600000000L AS TIMESTAMP) + x * 3455990000  ts " +
                        "from long_sequence(100))",
                "[-100] Detached partition metadata [timestamp_index] is not compatible with current table metadata"
        );
    }

    @Test
    public void testDetachAttachPartitionBrokenMetadataColumnCount() throws Exception {
        assertFailedAttachOperationBecauseOfMetadata(
                brokenMeta -> brokenMeta
                        .timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("s", ColumnType.SHORT)
                        .col("l", ColumnType.LONG),
                "insert into brokenMeta " +
                        "select " +
                        "CAST(1654041600000000L AS TIMESTAMP) + x * 3455990000  ts, " +
                        "cast(x as int) i, " +
                        "cast(x as short) s, " +
                        "x l " +
                        "from long_sequence(100))",
                "[-100] Detached partition metadata [column_count] is not compatible with current table metadata"
        );
    }

    @Test
    public void testDetachAttachPartitionBrokenMetadataColumnName() throws Exception {
        assertFailedAttachOperationBecauseOfMetadata(
                brokenMeta -> brokenMeta
                        .timestamp("ts")
                        .col("ii", ColumnType.INT)
                        .col("l", ColumnType.LONG),
                "insert into brokenMeta " +
                        "select " +
                        "CAST(1654041600000000L AS TIMESTAMP) + x * 3455990000  ts, " +
                        "cast(x as int) ii, " +
                        "x l " +
                        "from long_sequence(100))",
                "[-100] Detached column [index=1, name=i, attribute=name] does not match current table metadata"
        );
    }

    @Test
    public void testDetachAttachPartitionBrokenMetadataColumnType() throws Exception {
        assertFailedAttachOperationBecauseOfMetadata(
                brokenMeta -> brokenMeta
                        .timestamp("ts")
                        .col("i", ColumnType.LONG)
                        .col("l", ColumnType.LONG),
                "insert into brokenMeta " +
                        "select " +
                        "CAST(1654041600000000L AS TIMESTAMP) + x * 3455990000  ts, " +
                        "cast(x as int) i, " +
                        "x l " +
                        "from long_sequence(100))",
                "[-100] Detached column [index=1, name=i, attribute=type] does not match current table metadata"
        );
    }

    private void assertFailedAttachOperationBecauseOfMetadata(
            Function<TableModel, TableModel> brokenMetaSetter,
            String insertStmt,
            String expectedErrorMessage
    ) throws Exception {
        assertFailedAttachOperationBecauseOfMetadata(1, brokenMetaSetter, insertStmt, null, expectedErrorMessage);
    }

    private void assertFailedAttachOperationBecauseOfMetadata(
            int brokenMetaTableId,
            Function<TableModel, TableModel> brokenMetaTableModel,
            String insertStmt,
            String finalStmt,
            String expectedErrorMessage
    ) throws Exception {
        assertMemoryLeak(() -> {
            try (
                    TableModel tab = new TableModel(configuration, "tab", PartitionBy.DAY);
                    TableModel brokenMeta = new TableModel(configuration, "brokenMeta", PartitionBy.DAY);
                    MemoryMARW mem = Vm.getMARWInstance()
            ) {
                createPopulateTable(tab
                                .timestamp("ts")
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG),
                        10,
                        "2022-06-01",
                        3
                );
                createPopulateTable(
                        brokenMetaTableId,
                        mem,
                        brokenMetaTableModel.apply(brokenMeta),
                        insertStmt,
                        finalStmt
                );

                // detach partitions and swap detached metadata
                compile("ALTER TABLE brokenMeta DETACH PARTITION LIST '2022-06-01', '2022-06-02'", sqlExecutionContext);
                compile("ALTER TABLE tab DETACH PARTITION LIST '2022-06-01', '2022-06-02'", sqlExecutionContext);
                swapDetachedMetadataFiles("tab", "brokenMeta", "2022-06-02");

                assertFailure("ALTER TABLE tab ATTACH PARTITION LIST '2022-06-01', '2022-06-02'", expectedErrorMessage);
            }
        });
    }

    private static void assertContent(String expected, String tableName, String timestamp) throws Exception {
        engine.releaseAllWriters();
        engine.releaseAllReaders();
        assertQuery(expected, tableName, null, timestamp, true, false, true);
    }

    private void assertFailedDetachOperation(String operation, String errorMsg) throws Exception {
        assertFailedDetachOperation(null, operation, errorMsg);
    }

    private void assertFailedDetachOperation(@Nullable FilesFacade withFf, String operation, String errorMsg) throws Exception {
        assertMemoryLeak(withFf, () -> {
            try (TableModel tab = new TableModel(configuration, "tab", PartitionBy.DAY)) {
                createPopulateTable(tab
                                .timestamp("ts")
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG),
                        100,
                        "2022-06-01",
                        5
                );
                assertFailure(operation, errorMsg);
            }
        });
    }

    private void assertFailure(String operation, String errorMsg) {
        try {
            compile(operation, sqlExecutionContext);
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), errorMsg);
        }
    }

    private void createPopulateTable(int tableId, MemoryMARW mem, TableModel tableModel, String sql, String finalSql) throws SqlException {
        TableUtils.createTable(configuration, mem, path, tableModel, tableId);
        if (sql != null) {
            compile(sql, sqlExecutionContext);
        }
        if (finalSql != null) {
            compile(finalSql, sqlExecutionContext);
        }
    }

    private void swapDetachedMetadataFiles(String tableName, String brokenName, String partitionName) {
        // replace tab.detached/_meta.detached with brokenMeta.detached/_meta.detached
        setPathForDetachedMeta(path, tableName, partitionName);
        setPathForDetachedMeta(brokenMetaPath, brokenName, partitionName);
        FilesFacade ff = configuration.getFilesFacade();
        Assert.assertTrue(ff.remove(path));
        Assert.assertTrue(ff.copy(brokenMetaPath, path) >= 0);
    }

    private static void setPathForDetachedMeta(Path path, String tableName, String partitionName) {
        path.trimTo(pathRootLen)
                .concat(tableName)
                .concat(partitionName)
                .put(DETACHED_DIR_MARKER)
                .concat(META_FILE_NAME)
                .put(DETACHED_DIR_MARKER)
                .$();
    }
}
