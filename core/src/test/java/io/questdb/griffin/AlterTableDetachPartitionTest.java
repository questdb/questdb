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
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.*;

import java.util.function.Function;

import static io.questdb.cairo.TableUtils.*;


public class AlterTableDetachPartitionTest extends AbstractGriffinTest {

    private Path path;
    private Path other;

    @Override
    @Before
    public void setUp() {
        super.setUp();
        other = new Path();
        path = new Path();
    }

    @Override
    @After
    public void tearDown() {
        super.tearDown();
        path = Misc.free(path);
        other = Misc.free(other);
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
    public void testDetachSyntaxErrorListOrWhereExpected() throws Exception {
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
    public void testDetachSyntaxErrorPartitionNameExpected() throws Exception {
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
    public void testDetachNotPartitioned() throws Exception {
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
    public void testDetachNoDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel tab = new TableModel(configuration, "tab0", PartitionBy.DAY)) {
                CairoTestUtils.create(tab
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG)
                );
                try {
                    compile("ALTER TABLE tab0 DETACH PARTITION LIST '2022-06-27'", sqlExecutionContext);
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
                "tab11",
                "ALTER TABLE tab11 DETACH PARTITION LIST '2022-06-06'",
                "could not detach [statusCode=PARTITION_EMPTY, table=tab11, partition='2022-06-06']"
        );
    }


    @Test
    public void testCannotDetachActivePartition() throws Exception {
        assertFailedDetachOperation(
                "tab17",
                "ALTER TABLE tab17 DETACH PARTITION LIST '2022-06-05'",
                "could not detach [statusCode=PARTITION_IS_ACTIVE, table=tab17, partition='2022-06-05']"
        );
    }

    @Test
    public void testAlreadyDetached() throws Exception {
        assertFailedDetachOperation(
                "tab143",
                "ALTER TABLE tab143 DETACH PARTITION LIST '2022-06-03', '2022-06-03'",
                "could not detach [statusCode=PARTITION_ALREADY_DETACHED, table=tab143, partition='2022-06-03']"
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
                "tab42",
                "ALTER TABLE tab42 DETACH PARTITION LIST '2022-06-03'",
                "could not detach [statusCode=PARTITION_FOLDER_CANNOT_RENAME, table=tab42, partition='2022-06-03']"
        );
    }

    @Test
    public void testPartitionFolderDoesNotExist() throws Exception {
        FilesFacade ff = new FilesFacadeImpl() {
            @Override
            public boolean exists(LPSZ path) {
                if (Chars.endsWith(path, "2022-06-03")) {
                    return false;
                }
                return super.exists(path);
            }
        };
        assertFailedDetachOperation(
                ff,
                "tab111",
                "ALTER TABLE tab111 DETACH PARTITION LIST '2022-06-03'",
                "could not detach [statusCode=PARTITION_FOLDER_DOES_NOT_EXIST, table=tab111, partition='2022-06-03']"
        );
    }

    @Test
    public void testCannotCopyMeta() throws Exception {
        assertCannotCopyMetadata("tabCopyMeta", 1);
    }

    @Test
    public void testCannotCopyColumnVersions() throws Exception {
        assertCannotCopyMetadata("tabCopyColumnVersions", 2);
    }

    @Test
    public void testCannotUndoRenameAfterBrokenCopyMeta() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabUndoRenameCopyMeta";
            try (TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY)) {
                createPopulateTable(tab
                                .timestamp("ts")
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG),
                        10,
                        "2022-06-01",
                        4
                );

                AbstractCairoTest.ff = new FilesFacadeImpl() {
                    private boolean copyCalled = false;

                    @Override
                    public int copy(LPSZ from, LPSZ to) {
                        copyCalled = true;
                        return -1;
                    }

                    @Override
                    public boolean rename(LPSZ from, LPSZ to) {
                        return copyCalled ? false : super.rename(from, to);
                    }
                };

                long timestamp = TimestampFormatUtils.parseTimestamp("2022-06-01T00:00:00.000000Z");
                engine.releaseAllWriters();
                try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, "detach partition")) {
                    StatusCode statusCode = writer.detachPartition(timestamp);
                    Assert.assertEquals(StatusCode.PARTITION_FOLDER_CANNOT_UNDO_RENAME, statusCode);
                }

                // all that is left is to remove ".detached" from the name, by hand!
                // this will only happen if rename fails due to an os limit
                try {
                    assertContent("does not matter", tableName);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(),
                            "Partition '2022-06-01' does not exist in table '" + tableName + "' directory"
                    );
                }
            }
        });
    }

    @Test
    public void testDetachAttachPartition() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabDetachAttach";
            try (TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY)) {
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

                assertContent(expected, tableName);
                compile("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01', '2022-06-02'", sqlExecutionContext);
                assertContent("ts\ti\tl\n" +
                                "2022-06-03T02:23:59.300000Z\t7\t7\n" +
                                "2022-06-03T09:35:59.200000Z\t8\t8\n" +
                                "2022-06-03T16:47:59.100000Z\t9\t9\n" +
                                "2022-06-03T23:59:59.000000Z\t10\t10\n",
                        tableName
                );

                compile("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01', '2022-06-02'", sqlExecutionContext);
                assertContent(expected, tableName);
            }
        });
    }

    @Test
    public void testDetachAttachPartitionMissingMetadata() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabDetachAttachMissingMeta";
            try (TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY)) {
                createPopulateTable(tab
                                .timestamp("ts")
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG),
                        10,
                        "2022-06-01",
                        3
                );
                compile("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01', '2022-06-02'", sqlExecutionContext);

                // remove _meta.detached simply prevents metadata checking, all else is the same
                path.of(configuration.getDetachedRoot())
                        .concat(tableName)
                        .concat("2022-06-02")
                        .put(DETACHED_DIR_MARKER)
                        .concat(META_FILE_NAME)
                        .$();
                Assert.assertTrue(Files.remove(path));

                compile("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01', '2022-06-02'", sqlExecutionContext);
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
                        tableName
                );
            }
        });
    }

    @Test
    public void testDetachAttachPartitionBrokenMetadataTableId() throws Exception {
        assertFailedAttachOperationBecauseOfMetadata(
                2,
                "tabBrokenTableId",
                "tabBrokenTableId2",
                brokenMeta -> brokenMeta
                        .timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG),
                "insert into tabBrokenTableId2 " +
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
                "tabBrokenStructureVersion",
                "tabBrokenStructureVersion2",
                brokenMeta -> brokenMeta
                        .timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG),
                "insert into tabBrokenStructureVersion2 " +
                        "select " +
                        "CAST(1654041600000000L AS TIMESTAMP) + x * 3455990000  ts, " +
                        "cast(x as int) i, " +
                        "x l " +
                        "from long_sequence(100))",
                "ALTER TABLE tabBrokenStructureVersion2 ADD COLUMN s SHORT",
                "[-100] Detached partition metadata [structure_version] is not compatible with current table metadata"
        );
    }

    @Test
    public void testDetachAttachPartitionBrokenMetadataTimestampIndex() throws Exception {
        assertFailedAttachOperationBecauseOfMetadata(
                "tabBrokenTimestampIndex",
                "tabBrokenTimestampIndex2",
                brokenMeta -> brokenMeta
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG)
                        .timestamp("ts"),
                "insert into tabBrokenTimestampIndex2 " +
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
                "tabBrokenColumnCount",
                "tabBrokenColumnCount2",
                brokenMeta -> brokenMeta
                        .timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("s", ColumnType.SHORT)
                        .col("l", ColumnType.LONG),
                "insert into tabBrokenColumnCount2 " +
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
                "tabBrokenColumnName",
                "tabBrokenColumnName2",
                brokenMeta -> brokenMeta
                        .timestamp("ts")
                        .col("ii", ColumnType.INT)
                        .col("l", ColumnType.LONG),
                "insert into tabBrokenColumnName2 " +
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
                "tabBrokenColumnType",
                "tabBrokenColumnType2",
                brokenMeta -> brokenMeta
                        .timestamp("ts")
                        .col("i", ColumnType.LONG)
                        .col("l", ColumnType.LONG),
                "insert into tabBrokenColumnType2 " +
                        "select " +
                        "CAST(1654041600000000L AS TIMESTAMP) + x * 3455990000  ts, " +
                        "cast(x as int) i, " +
                        "x l " +
                        "from long_sequence(100))",
                "[-100] Detached column [index=1, name=i, attribute=type] does not match current table metadata"
        );
    }

    @Test
    public void testDetachPartitionsTableInTransaction() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabInTransaction";
            try (TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY)) {
                createPopulateTable(tab
                                .col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        12,
                        "2022-06-01",
                        4);
                assertContent(
                        "l\ti\tts\n" +
                                "1\t1\t2022-06-01T07:59:59.916666Z\n" +
                                "2\t2\t2022-06-01T15:59:59.833332Z\n" +
                                "3\t3\t2022-06-01T23:59:59.749998Z\n" +
                                "4\t4\t2022-06-02T07:59:59.666664Z\n" +
                                "5\t5\t2022-06-02T15:59:59.583330Z\n" +
                                "6\t6\t2022-06-02T23:59:59.499996Z\n" +
                                "7\t7\t2022-06-03T07:59:59.416662Z\n" +
                                "8\t8\t2022-06-03T15:59:59.333328Z\n" +
                                "9\t9\t2022-06-03T23:59:59.249994Z\n" +
                                "10\t10\t2022-06-04T07:59:59.166660Z\n" +
                                "11\t11\t2022-06-04T15:59:59.083326Z\n" +
                                "12\t12\t2022-06-04T23:59:58.999992Z\n",
                        tableName
                );

                long timestamp = TimestampFormatUtils.parseTimestamp("2022-06-01T00:00:00.000000Z");
                long timestamp2 = TimestampFormatUtils.parseTimestamp("2022-06-01T09:59:59.999999Z");
                try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, "testing")) {
                    // structural change
                    writer.addColumn("new_column", ColumnType.INT);

                    TableWriter.Row row = writer.newRow(timestamp2);
                    row.putLong(0, 137L);
                    row.putInt(1, 137);
                    row.putInt(3, 137);
                    row.append(); // O3 append

                    row = writer.newRow(timestamp);
                    row.putLong(0, 2802L);
                    row.putInt(1, 2802);
                    row.putInt(3, 2802);
                    row.append(); // O3 append

                    Assert.assertTrue(writer.inTransaction());
                    writer.detachPartition(timestamp);
                    Assert.assertEquals(9, writer.size());
                    Assert.assertFalse(writer.inTransaction());
                }

                compile("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01'", sqlExecutionContext);
                assertContent(
                        "l\ti\tts\tnew_column\n" +
                                "2802\t2802\t2022-06-01T00:00:00.000000Z\t2802\n" +
                                "1\t1\t2022-06-01T07:59:59.916666Z\tNaN\n" +
                                "137\t137\t2022-06-01T09:59:59.999999Z\t137\n" +
                                "2\t2\t2022-06-01T15:59:59.833332Z\tNaN\n" +
                                "3\t3\t2022-06-01T23:59:59.749998Z\tNaN\n" +
                                "4\t4\t2022-06-02T07:59:59.666664Z\tNaN\n" +
                                "5\t5\t2022-06-02T15:59:59.583330Z\tNaN\n" +
                                "6\t6\t2022-06-02T23:59:59.499996Z\tNaN\n" +
                                "7\t7\t2022-06-03T07:59:59.416662Z\tNaN\n" +
                                "8\t8\t2022-06-03T15:59:59.333328Z\tNaN\n" +
                                "9\t9\t2022-06-03T23:59:59.249994Z\tNaN\n" +
                                "10\t10\t2022-06-04T07:59:59.166660Z\tNaN\n" +
                                "11\t11\t2022-06-04T15:59:59.083326Z\tNaN\n" +
                                "12\t12\t2022-06-04T23:59:58.999992Z\tNaN\n",
                        tableName
                );
            }
        });
    }

    @Test
    public void testDetachPartitionsTimeTravel() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabTimeTravel";
            try (TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY)) {
                createPopulateTable(tab
                                .col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        12,
                        "2022-06-01",
                        4);
                assertContent(
                        "l\ti\tts\n" +
                                "1\t1\t2022-06-01T07:59:59.916666Z\n" +
                                "2\t2\t2022-06-01T15:59:59.833332Z\n" +
                                "3\t3\t2022-06-01T23:59:59.749998Z\n" +
                                "4\t4\t2022-06-02T07:59:59.666664Z\n" +
                                "5\t5\t2022-06-02T15:59:59.583330Z\n" +
                                "6\t6\t2022-06-02T23:59:59.499996Z\n" +
                                "7\t7\t2022-06-03T07:59:59.416662Z\n" +
                                "8\t8\t2022-06-03T15:59:59.333328Z\n" +
                                "9\t9\t2022-06-03T23:59:59.249994Z\n" +
                                "10\t10\t2022-06-04T07:59:59.166660Z\n" +
                                "11\t11\t2022-06-04T15:59:59.083326Z\n" +
                                "12\t12\t2022-06-04T23:59:58.999992Z\n",
                        tableName
                );

                // drop the partition
                compile("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01'", sqlExecutionContext);

                // insert data, which will create the partition again
                long timestamp = TimestampFormatUtils.parseTimestamp("2022-06-01T00:00:00.000000Z");
                long timestamp2 = TimestampFormatUtils.parseTimestamp("2022-06-01T09:59:59.999999Z");
                try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, "testing")) {

                    TableWriter.Row row = writer.newRow(timestamp2);
                    row.putLong(0, 137L);
                    row.putInt(1, 137);
                    row.append(); // O3 append

                    row = writer.newRow(timestamp);
                    row.putLong(0, 2802L);
                    row.putInt(1, 2802);
                    row.append(); // O3 append

                    writer.commit();
                }
                assertContent(
                        "l\ti\tts\n" +
                                "2802\t2802\t2022-06-01T00:00:00.000000Z\n" +
                                "137\t137\t2022-06-01T09:59:59.999999Z\n" +
                                "4\t4\t2022-06-02T07:59:59.666664Z\n" +
                                "5\t5\t2022-06-02T15:59:59.583330Z\n" +
                                "6\t6\t2022-06-02T23:59:59.499996Z\n" +
                                "7\t7\t2022-06-03T07:59:59.416662Z\n" +
                                "8\t8\t2022-06-03T15:59:59.333328Z\n" +
                                "9\t9\t2022-06-03T23:59:59.249994Z\n" +
                                "10\t10\t2022-06-04T07:59:59.166660Z\n" +
                                "11\t11\t2022-06-04T15:59:59.083326Z\n" +
                                "12\t12\t2022-06-04T23:59:58.999992Z\n",
                        tableName
                );

                // hide the detached partition
                path.of(configuration.getDetachedRoot()).concat(tableName).concat("2022-06-01.detached").$();
                other.of(configuration.getDetachedRoot()).concat(tableName).concat("2022-06-01.detached.hide").$();
                Assert.assertTrue(Files.rename(path, other));
                // drop the latest version of the partition
                compile("ALTER TABLE " + tableName + " DROP PARTITION LIST '2022-06-01'", sqlExecutionContext);
                // resurface the hiden detached partition
                Assert.assertTrue(Files.rename(other, path));

                // reattach old version
//                engine.releaseAllWriters();
                compile("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01'");
                assertContent(
                        "l\ti\tts\n" +
                                "1\t1\t2022-06-01T07:59:59.916666Z\n" +
                                "2\t2\t2022-06-01T15:59:59.833332Z\n" +
                                "3\t3\t2022-06-01T23:59:59.749998Z\n" +
                                "4\t4\t2022-06-02T07:59:59.666664Z\n" +
                                "5\t5\t2022-06-02T15:59:59.583330Z\n" +
                                "6\t6\t2022-06-02T23:59:59.499996Z\n" +
                                "7\t7\t2022-06-03T07:59:59.416662Z\n" +
                                "8\t8\t2022-06-03T15:59:59.333328Z\n" +
                                "9\t9\t2022-06-03T23:59:59.249994Z\n" +
                                "10\t10\t2022-06-04T07:59:59.166660Z\n" +
                                "11\t11\t2022-06-04T15:59:59.083326Z\n" +
                                "12\t12\t2022-06-04T23:59:58.999992Z\n",
                        tableName
                );
            }
        });
    }

    @Test
    public void testDetachPartitionsTimeTravelFailsBecauseOfStructuralChanges() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabIncompatibleStructure";
            try (TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY)) {
                createPopulateTable(tab
                                .col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        12,
                        "2022-06-01",
                        4);
                assertContent(
                        "l\ti\tts\n" +
                                "1\t1\t2022-06-01T07:59:59.916666Z\n" +
                                "2\t2\t2022-06-01T15:59:59.833332Z\n" +
                                "3\t3\t2022-06-01T23:59:59.749998Z\n" +
                                "4\t4\t2022-06-02T07:59:59.666664Z\n" +
                                "5\t5\t2022-06-02T15:59:59.583330Z\n" +
                                "6\t6\t2022-06-02T23:59:59.499996Z\n" +
                                "7\t7\t2022-06-03T07:59:59.416662Z\n" +
                                "8\t8\t2022-06-03T15:59:59.333328Z\n" +
                                "9\t9\t2022-06-03T23:59:59.249994Z\n" +
                                "10\t10\t2022-06-04T07:59:59.166660Z\n" +
                                "11\t11\t2022-06-04T15:59:59.083326Z\n" +
                                "12\t12\t2022-06-04T23:59:58.999992Z\n",
                        tableName
                );

                // drop the partition
                compile("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01'", sqlExecutionContext);

                // insert data, which will create the partition again
                long timestamp = TimestampFormatUtils.parseTimestamp("2022-06-01T00:00:00.000000Z");
                long timestamp2 = TimestampFormatUtils.parseTimestamp("2022-06-01T09:59:59.999999Z");
                try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, "testing")) {

                    // structural change
                    writer.addColumn("new_column", ColumnType.INT);

                    TableWriter.Row row = writer.newRow(timestamp2);
                    row.putLong(0, 137L);
                    row.putInt(1, 137);
                    row.putInt(3, 137);
                    row.append(); // O3 append

                    row = writer.newRow(timestamp);
                    row.putLong(0, 2802L);
                    row.putInt(1, 2802);
                    row.putInt(3, 2802);
                    row.append(); // O3 append

                    writer.commit();
                }
                assertContent(
                        "l\ti\tts\tnew_column\n" +
                                "2802\t2802\t2022-06-01T00:00:00.000000Z\t2802\n" +
                                "137\t137\t2022-06-01T09:59:59.999999Z\t137\n" +
                                "4\t4\t2022-06-02T07:59:59.666664Z\tNaN\n" +
                                "5\t5\t2022-06-02T15:59:59.583330Z\tNaN\n" +
                                "6\t6\t2022-06-02T23:59:59.499996Z\tNaN\n" +
                                "7\t7\t2022-06-03T07:59:59.416662Z\tNaN\n" +
                                "8\t8\t2022-06-03T15:59:59.333328Z\tNaN\n" +
                                "9\t9\t2022-06-03T23:59:59.249994Z\tNaN\n" +
                                "10\t10\t2022-06-04T07:59:59.166660Z\tNaN\n" +
                                "11\t11\t2022-06-04T15:59:59.083326Z\tNaN\n" +
                                "12\t12\t2022-06-04T23:59:58.999992Z\tNaN\n",
                        tableName
                );

                // hide the detached partition
                path.of(configuration.getDetachedRoot()).concat(tableName).concat("2022-06-01.detached").$();
                other.of(configuration.getDetachedRoot()).concat(tableName).concat("2022-06-01.detached.hide").$();
                Assert.assertTrue(Files.rename(path, other));
                // drop the latest version of the partition
                compile("ALTER TABLE " + tableName + " DROP PARTITION LIST '2022-06-01'", sqlExecutionContext);
                // resurface the hiden detached partition
                Assert.assertTrue(Files.rename(other, path));

                // reattach old version
                assertFailure(
                        "ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01'",
                        "table '" + tableName + "' could not be altered: [-100] Detached partition metadata [structure_version] is not compatible with current table metadata"
                );
            }
        });
    }

    @Test
    public void testDetachPartitionsColumnTops() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabIncompatibleStructure";
            try (TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY)) {
                createPopulateTable(tab
                                .col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        12,
                        "2022-06-01",
                        4);

                // insert data, which will create the partition again
                try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, "testing")) {

                    TableWriter.Row row = writer.newRow(TimestampFormatUtils.parseTimestamp("2022-06-05T00:00:00.000000Z"));
                    row.putLong(0, 137L);
                    row.putInt(1, 137);
                    row.append();

                    // structural change
                    writer.addColumn("new_column", ColumnType.INT);
                }
                assertContent(
                        "l\ti\tts\tnew_column\n" +
                                "1\t1\t2022-06-01T07:59:59.916666Z\tNaN\n" +
                                "2\t2\t2022-06-01T15:59:59.833332Z\tNaN\n" +
                                "3\t3\t2022-06-01T23:59:59.749998Z\tNaN\n" +
                                "4\t4\t2022-06-02T07:59:59.666664Z\tNaN\n" +
                                "5\t5\t2022-06-02T15:59:59.583330Z\tNaN\n" +
                                "6\t6\t2022-06-02T23:59:59.499996Z\tNaN\n" +
                                "7\t7\t2022-06-03T07:59:59.416662Z\tNaN\n" +
                                "8\t8\t2022-06-03T15:59:59.333328Z\tNaN\n" +
                                "9\t9\t2022-06-03T23:59:59.249994Z\tNaN\n" +
                                "10\t10\t2022-06-04T07:59:59.166660Z\tNaN\n" +
                                "11\t11\t2022-06-04T15:59:59.083326Z\tNaN\n" +
                                "12\t12\t2022-06-04T23:59:58.999992Z\tNaN\n" +
                                "137\t137\t2022-06-05T00:00:00.000000Z\tNaN\n",
                        tableName
                );

                // drop the partition
                compile("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01'", sqlExecutionContext);

                // insert data, which will create the partition again
                try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, "testing")) {

                    TableWriter.Row row = writer.newRow(TimestampFormatUtils.parseTimestamp("2022-06-01T00:00:00.000000Z"));
                    row.putLong(0, 25160L);
                    row.putInt(1, 25160);
                    row.putInt(3, 25160);
                    row.append();
                    writer.commit();
                }

                // hide the detached partition
                path.of(configuration.getDetachedRoot()).concat(tableName).concat("2022-06-01.detached").$();
                other.of(configuration.getDetachedRoot()).concat(tableName).concat("2022-06-01.detached.hide").$();
                Assert.assertTrue(Files.rename(path, other));
                // drop the latest version of the partition
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                compile("ALTER TABLE " + tableName + " DROP PARTITION LIST '2022-06-01'", sqlExecutionContext);
                // resurface the hidden detached partition
                Assert.assertTrue(Files.rename(other, path));

                // reattach old version
                compile("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01'", sqlExecutionContext);
            }
        });
    }

    private void assertCannotCopyMetadata(String tableName, final int copyFailCallId) throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY)) {
                createPopulateTable(tab
                                .timestamp("ts")
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG),
                        10,
                        "2022-06-01",
                        4
                );
                String expected = "ts\ti\tl\n" +
                        "2022-06-01T09:35:59.900000Z\t1\t1\n" +
                        "2022-06-01T19:11:59.800000Z\t2\t2\n" +
                        "2022-06-02T04:47:59.700000Z\t3\t3\n" +
                        "2022-06-02T14:23:59.600000Z\t4\t4\n" +
                        "2022-06-02T23:59:59.500000Z\t5\t5\n" +
                        "2022-06-03T09:35:59.400000Z\t6\t6\n" +
                        "2022-06-03T19:11:59.300000Z\t7\t7\n" +
                        "2022-06-04T04:47:59.200000Z\t8\t8\n" +
                        "2022-06-04T14:23:59.100000Z\t9\t9\n" +
                        "2022-06-04T23:59:59.000000Z\t10\t10\n";
                assertContent(expected, tableName);

                AbstractCairoTest.ff = new FilesFacadeImpl() {
                    private int numberOfCalls = 0;

                    public int copy(LPSZ from, LPSZ to) {
                        ++numberOfCalls;
                        return numberOfCalls == copyFailCallId ? -1 : super.copy(from, to);
                    }
                };
                try {
                    long timestamp = TimestampFormatUtils.parseTimestamp("2022-06-01T00:00:00.000000Z");
                    try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, "detach partition")) {
                        StatusCode statusCode = writer.detachPartition(timestamp);
                        Assert.assertEquals(StatusCode.PARTITION_CANNOT_COPY_META, statusCode);
                    }

                    // the operation is reversible
                    assertContent(expected, tableName);

                    // check no metadata files were left behind
                    path.of(configuration.getRoot())
                            .concat(tableName)
                            .concat("2022-06-01");
                    int len = path.length();
                    Assert.assertFalse(Files.exists(path.concat(META_FILE_NAME).$()));
                    Assert.assertFalse(Files.exists(path.trimTo(len).concat(COLUMN_VERSION_FILE_NAME).$()));
                } finally {
                    AbstractCairoTest.ff = null;
                }
            }
        });
    }

    private void assertFailedAttachOperationBecauseOfMetadata(
            String tableName,
            String brokenTableName,
            Function<TableModel, TableModel> brokenMetaSetter,
            String insertStmt,
            String expectedErrorMessage
    ) throws Exception {
        assertFailedAttachOperationBecauseOfMetadata(
                1,
                tableName,
                brokenTableName,
                brokenMetaSetter,
                insertStmt,
                null,
                expectedErrorMessage
        );
    }

    private void assertFailedAttachOperationBecauseOfMetadata(
            int brokenMetaTableId,
            String tableName,
            String brokenTableName,
            Function<TableModel, TableModel> brokenMetaTableModelTransform,
            String insertStmt,
            String finalStmt,
            String expectedErrorMessage
    ) throws Exception {
        assertMemoryLeak(() -> {
            try (
                    TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
                    TableModel brokenMeta = new TableModel(configuration, brokenTableName, PartitionBy.DAY);
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
                        brokenMetaTableModelTransform.apply(brokenMeta),
                        insertStmt,
                        finalStmt
                );

                // detach partitions and override detached metadata with broken metadata
                compile(
                        "ALTER TABLE " + brokenTableName + " DETACH PARTITION LIST '2022-06-01', '2022-06-02'",
                        sqlExecutionContext
                );
                compile(
                        "ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01', '2022-06-02'",
                        sqlExecutionContext
                );
                path.of(configuration.getDetachedRoot())
                        .concat(tableName)
                        .concat("2022-06-02")
                        .put(DETACHED_DIR_MARKER)
                        .concat(META_FILE_NAME)
                        .$();
                other.of(configuration.getDetachedRoot())
                        .concat(brokenTableName)
                        .concat("2022-06-02")
                        .put(DETACHED_DIR_MARKER)
                        .concat(META_FILE_NAME)
                        .$();
                Assert.assertTrue(Files.remove(path));
                Assert.assertTrue(Files.copy(other, path) >= 0);

                // attempt to reattach
                assertFailure(
                        "ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01', '2022-06-02'",
                        expectedErrorMessage
                );
            }
        });
    }

    private static void assertContent(String expected, String tableName) throws Exception {
        engine.releaseAllWriters();
        engine.releaseAllReaders();
        assertQuery(expected, tableName, null, "ts", true, false, true);
    }

    private void assertFailedDetachOperation(String tableName, String operation, String errorMsg) throws Exception {
        assertFailedDetachOperation(null, tableName, operation, errorMsg);
    }

    private void assertFailedDetachOperation(
            @Nullable FilesFacade withFf,
            String tableName,
            String operation,
            String errorMsg
    ) throws Exception {
        assertMemoryLeak(withFf, () -> {
            try (TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY)) {
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
}
