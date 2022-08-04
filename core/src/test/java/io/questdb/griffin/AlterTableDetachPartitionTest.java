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
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.*;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
    public void testSyntaxErrorPartitionMissing() throws Exception {
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
    public void testSyntaxErrorListOrWhereExpected() throws Exception {
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
    public void testSyntaxErrorPartitionNameExpected() throws Exception {
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
    public void testNotPartitioned() throws Exception {
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
    public void testNoDesignatedTimestamp() throws Exception {
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
    public void testPartitionEmpty() throws Exception {
        assertFailure(
                "tab11",
                "ALTER TABLE tab11 DETACH PARTITION LIST '2022-06-06'",
                "could not detach [statusCode=PARTITION_ALREADY_DETACHED, table=tab11, partition='2022-06-06']"
        );
    }


    @Test
    public void testCannotDetachActivePartition() throws Exception {
        assertFailure(
                "tab17",
                "ALTER TABLE tab17 DETACH PARTITION LIST '2022-06-05'",
                "could not detach [statusCode=PARTITION_IS_ACTIVE, table=tab17, partition='2022-06-05']"
        );
    }

    @Test
    public void testAlreadyDetached1() throws Exception {
        assertFailure(
                "tab143",
                "ALTER TABLE tab143 DETACH PARTITION LIST '2022-06-03', '2022-06-03'",
                "could not detach [statusCode=PARTITION_ALREADY_DETACHED, table=tab143, partition='2022-06-03']"
        );
    }

    @Test
    public void testAlreadyDetached2() throws Exception {
        path.of(configuration.getDetachedRoot())
                .concat("tab143")
                .concat("2022-06-03")
                .put(DETACHED_DIR_MARKER)
                .slash$();
        Assert.assertEquals(0, FilesFacadeImpl.INSTANCE.mkdirs(path, 509));
        assertFailure(
                "tab143",
                "ALTER TABLE tab143 DETACH PARTITION LIST '2022-06-03'",
                "could not detach [statusCode=PARTITION_ALREADY_DETACHED, table=tab143, partition='2022-06-03']"
        );
    }

    @Test
    public void testCannotRenameFolder() throws Exception {
        assertFailure(
                new FilesFacadeImpl() {
                    @Override
                    public int rename(LPSZ from, LPSZ to) {
                        return -1;
                    }
                },
                "tab42",
                "ALTER TABLE tab42 DETACH PARTITION LIST '2022-06-03'",
                "could not detach [statusCode=PARTITION_FOLDER_CANNOT_RENAME, table=tab42, partition='2022-06-03']"
        );
    }

    @Test
    public void testCannotCreateDetachedFolder() throws Exception {
        String detachedFolderName = "d3t@ch3d";
        FilesFacade ff2 = new FilesFacadeImpl() {
            @Override
            public int mkdirs(Path path, int mode) {
                if (Chars.contains(path, detachedFolderName)) {
                    return -1;
                }
                return super.mkdirs(path, mode);
            }
        };

        assertMemoryLeak(ff2, () -> {
            CairoConfiguration conf2 = new DefaultCairoConfiguration(root) {
                @Override
                public CharSequence getDetachedRoot() {
                    CharSequence r = getRoot();
                    int idx = r.length() - 1;
                    while (idx > 0 && r.charAt(idx) != Files.SEPARATOR) {
                        idx--;
                    }
                    return r.subSequence(0, ++idx) + detachedFolderName;
                }

                public FilesFacade getFilesFacade() {
                    return ff2;
                }
            };
            CairoEngine engine2 = new CairoEngine(conf2, metrics);
            SqlCompiler compiler2 = new SqlCompiler(engine2, null, null);
            SqlExecutionContextImpl context2 = new SqlExecutionContextImpl(engine, 1);
            try (TableModel tab = new TableModel(conf2, "tab42", PartitionBy.DAY)) {
                TestUtils.createPopulateTable(
                        compiler2,
                        context2,
                        tab.timestamp("ts")
                                .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG)
                                .col("s2", ColumnType.SYMBOL),
                        100,
                        "2022-06-01",
                        5
                );
                try {
                    compiler2.compile(
                            "ALTER TABLE tab42 DETACH PARTITION LIST '2022-06-03'",
                            context2
                    ).execute(null).await();
                    Assert.fail();
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(),
                            "could not detach [statusCode=PARTITION_DETACHED_FOLDER_CANNOT_CREATE, table=tab42, partition='2022-06-03']"
                    );
                }
            }
            engine2.close();
            compiler2.close();
        });
    }

    @Test
    public void testPartitionFolderDoesNotExist() throws Exception {
        assertFailure(
                new FilesFacadeImpl() {
                    @Override
                    public boolean exists(LPSZ path) {
                        if (Chars.endsWith(path, "2022-06-03")) {
                            return false;
                        }
                        return super.exists(path);
                    }
                },
                "tab111",
                "ALTER TABLE tab111 DETACH PARTITION LIST '2022-06-03'",
                "could not detach [statusCode=PARTITION_FOLDER_DOES_NOT_EXIST, table=tab111, partition='2022-06-03']"
        );
    }

    @Test
    public void testCannotCopyMeta() throws Exception {
        assertCannotCopyMeta("testCannotCopyMeta", 1);
    }

    @Test
    public void testCannotCopyColumnVersions() throws Exception {
        assertCannotCopyMeta("testCannotCopyColumnVersions", 2);
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
                    public int rename(LPSZ from, LPSZ to) {
                        if (!copyCalled) {
                            return super.rename(from, to);
                        }
                        return -1;
                    }
                };

                engine.clear();
                long timestamp = TimestampFormatUtils.parseTimestamp("2022-06-01T00:00:00.000000Z");
                try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, "detach partition")) {
                    StatusCode statusCode = writer.detachPartition(timestamp);
                    Assert.assertEquals(StatusCode.PARTITION_FOLDER_CANNOT_UNDO_RENAME, statusCode);
                }
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
                                .col("si", ColumnType.SYMBOL).indexed(true, 32)
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG)
                                .col("s", ColumnType.SYMBOL),
                        10,
                        "2022-06-01",
                        3
                );

                compile("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01', '2022-06-02'", sqlExecutionContext);
                renameDetachedToAttachable(tableName, "2022-06-01", "2022-06-02");
                compile("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01', '2022-06-02'", sqlExecutionContext);
                assertContent(
                        "ts\tsi\ti\tl\ts\n" +
                                "2022-06-01T07:11:59.900000Z\tPEHN\t1\t1\tSXUX\n" +
                                "2022-06-01T14:23:59.800000Z\tVTJW\t2\t2\t\n" +
                                "2022-06-01T21:35:59.700000Z\t\t3\t3\tSXUX\n" +
                                "2022-06-02T04:47:59.600000Z\t\t4\t4\t\n" +
                                "2022-06-02T11:59:59.500000Z\t\t5\t5\tGPGW\n" +
                                "2022-06-02T19:11:59.400000Z\tPEHN\t6\t6\tRXGZ\n" +
                                "2022-06-03T02:23:59.300000Z\tCPSW\t7\t7\t\n" +
                                "2022-06-03T09:35:59.200000Z\t\t8\t8\t\n" +
                                "2022-06-03T16:47:59.100000Z\tPEHN\t9\t9\tRXGZ\n" +
                                "2022-06-03T23:59:59.000000Z\tVTJW\t10\t10\tIBBT\n",
                        tableName
                );
            }
        });
    }

    @Test
    public void testDetachAttachPartitionFailsYouDidNotRenameTheFolderToAttachable() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabDetachAttachNotAttachable";
            try (TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY)) {
                createPopulateTable(tab
                                .timestamp("ts")
                                .col("si", ColumnType.SYMBOL).indexed(true, 32)
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG)
                                .col("s", ColumnType.SYMBOL),
                        10,
                        "2022-06-01",
                        3
                );

                compile("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01', '2022-06-02'", sqlExecutionContext);
                assertFailure(
                        "ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01', '2022-06-02'",
                        "failed to attach partition '2022-06-01': PARTITION_CANNOT_ATTACH_MISSING"
                );
            }
        });
    }

    @Test
    public void testDetachPartitionsTableAddColumnTransaction() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabInAddColumnTransaction";
            try (TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY)) {
                createPopulateTable(
                        1,
                        tab.col("l", ColumnType.LONG)
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

                engine.clear();
                String timestampDay = "2022-06-01";
                long timestamp = TimestampFormatUtils.parseTimestamp(timestampDay + "T00:00:00.000000Z");
                long timestamp2 = TimestampFormatUtils.parseTimestamp(timestampDay + "T09:59:59.999999Z");
                try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, "testing")) {
                    // structural change
                    writer.addColumn("new_column", ColumnType.INT);

                    TableWriter.Row row = writer.newRow(timestamp2);
                    row.putLong(0, 137L);
                    row.putInt(1, 137);
                    row.putInt(3, 137);
                    row.append(); // O3 append

                    Assert.assertTrue(writer.inTransaction());
                    writer.detachPartition(timestamp);
                    Assert.assertEquals(9, writer.size());
                }
                path.of(configuration.getRoot())
                        .concat(tableName)
                        .concat(timestampDay)
                        .$();
                Assert.assertFalse(Files.exists(path));
                path.of(configuration.getDetachedRoot())
                        .concat(tableName)
                        .concat(timestampDay)
                        .put(DETACHED_DIR_MARKER)
                        .$();
                Assert.assertTrue(Files.exists(path));

                renameDetachedToAttachable(tableName, timestampDay);

                compile("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + timestampDay + "'", sqlExecutionContext);
                assertContent(
                        "l\ti\tts\tnew_column\n" +
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
                String timestampDay = "2022-06-01";
                createPopulateTable(tab
                                .col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        12,
                        timestampDay,
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
                compile("ALTER TABLE " + tableName + " DETACH PARTITION LIST '" + timestampDay + "'", sqlExecutionContext);

                // insert data, which will create the partition again
                engine.clear();
                long timestamp = TimestampFormatUtils.parseTimestamp(timestampDay + "T00:00:00.000000Z");
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

                dropCurrentVersionOfPartition(tableName, timestampDay);
                renameDetachedToAttachable(tableName, timestampDay);

                // reattach old version
                compile("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + timestampDay + "'");
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
    public void testAttachWillFailIfThePartitionWasRecreated() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabTimeTravel2";
            try (TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY)) {
                String timestampDay = "2022-06-01";
                createPopulateTable(tab
                                .col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        12,
                        timestampDay,
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
                compile("ALTER TABLE " + tableName + " DETACH PARTITION LIST '" + timestampDay + "'", sqlExecutionContext);

                // insert data, which will create the partition again
                engine.clear();
                long timestamp = TimestampFormatUtils.parseTimestamp(timestampDay + "T09:59:59.999999Z");
                try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, "testing")) {
                    TableWriter.Row row = writer.newRow(timestamp);
                    row.putLong(0, 137L);
                    row.putInt(1, 137);
                    row.append();
                    writer.commit();
                }
                String expected = "l\ti\tts\n" +
                        "137\t137\t2022-06-01T09:59:59.999999Z\n" +
                        "4\t4\t2022-06-02T07:59:59.666664Z\n" +
                        "5\t5\t2022-06-02T15:59:59.583330Z\n" +
                        "6\t6\t2022-06-02T23:59:59.499996Z\n" +
                        "7\t7\t2022-06-03T07:59:59.416662Z\n" +
                        "8\t8\t2022-06-03T15:59:59.333328Z\n" +
                        "9\t9\t2022-06-03T23:59:59.249994Z\n" +
                        "10\t10\t2022-06-04T07:59:59.166660Z\n" +
                        "11\t11\t2022-06-04T15:59:59.083326Z\n" +
                        "12\t12\t2022-06-04T23:59:58.999992Z\n";
                assertContent(expected, tableName);
                renameDetachedToAttachable(tableName, timestampDay);
                assertFailure(
                        "ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + timestampDay + "'",
                        "failed to attach partition '2022-06-01': PARTITION_ALREADY_ATTACHED"
                );
                assertContent(expected, tableName);
            }
        });
    }

    @Test
    public void testDetachPartitionsColumnTops() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabColumnTops";
            try (TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY)) {
                createPopulateTable(tab
                                .col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        12,
                        "2022-06-01",
                        4);

                engine.clear();
                String timestampDay = "2022-06-02";
                long timestamp = TimestampFormatUtils.parseTimestamp(timestampDay + "T22:00:00.000000Z");
                try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, "testing")) {
                    // structural change
                    writer.addColumn("new_column", ColumnType.INT);

                    TableWriter.Row row = writer.newRow(timestamp);
                    row.putLong(0, 33L);
                    row.putInt(1, 33);
                    row.putInt(3, 33);
                    row.append();

                    writer.commit();
                }
                assertContent(
                        "l\ti\tts\tnew_column\n" +
                                "1\t1\t2022-06-01T07:59:59.916666Z\tNaN\n" +
                                "2\t2\t2022-06-01T15:59:59.833332Z\tNaN\n" +
                                "3\t3\t2022-06-01T23:59:59.749998Z\tNaN\n" +
                                "4\t4\t2022-06-02T07:59:59.666664Z\tNaN\n" +
                                "5\t5\t2022-06-02T15:59:59.583330Z\tNaN\n" +
                                "33\t33\t2022-06-02T22:00:00.000000Z\t33\n" +
                                "6\t6\t2022-06-02T23:59:59.499996Z\tNaN\n" +
                                "7\t7\t2022-06-03T07:59:59.416662Z\tNaN\n" +
                                "8\t8\t2022-06-03T15:59:59.333328Z\tNaN\n" +
                                "9\t9\t2022-06-03T23:59:59.249994Z\tNaN\n" +
                                "10\t10\t2022-06-04T07:59:59.166660Z\tNaN\n" +
                                "11\t11\t2022-06-04T15:59:59.083326Z\tNaN\n" +
                                "12\t12\t2022-06-04T23:59:58.999992Z\tNaN\n",
                        tableName
                );

                // detach the partition
                compile("ALTER TABLE " + tableName + " DETACH PARTITION LIST '" + timestampDay + "'", sqlExecutionContext);

                // insert data, which will create the partition again
                engine.clear();
                try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, "testing")) {
                    TableWriter.Row row = writer.newRow(timestamp);
                    row.putLong(0, 25160L);
                    row.putInt(1, 25160);
                    row.putInt(3, 25160);
                    row.append();
                    writer.commit();
                }
                assertContent(
                        "l\ti\tts\tnew_column\n" +
                                "1\t1\t2022-06-01T07:59:59.916666Z\tNaN\n" +
                                "2\t2\t2022-06-01T15:59:59.833332Z\tNaN\n" +
                                "3\t3\t2022-06-01T23:59:59.749998Z\tNaN\n" +
                                "25160\t25160\t2022-06-02T22:00:00.000000Z\t25160\n" +
                                "7\t7\t2022-06-03T07:59:59.416662Z\tNaN\n" +
                                "8\t8\t2022-06-03T15:59:59.333328Z\tNaN\n" +
                                "9\t9\t2022-06-03T23:59:59.249994Z\tNaN\n" +
                                "10\t10\t2022-06-04T07:59:59.166660Z\tNaN\n" +
                                "11\t11\t2022-06-04T15:59:59.083326Z\tNaN\n" +
                                "12\t12\t2022-06-04T23:59:58.999992Z\tNaN\n",
                        tableName
                );

                dropCurrentVersionOfPartition(tableName, timestampDay);
                renameDetachedToAttachable(tableName, timestampDay);

                // reattach old version
                compile("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + timestampDay + "'", sqlExecutionContext);
                assertContent(
                        "l\ti\tts\tnew_column\n" +
                                "1\t1\t2022-06-01T07:59:59.916666Z\tNaN\n" +
                                "2\t2\t2022-06-01T15:59:59.833332Z\tNaN\n" +
                                "3\t3\t2022-06-01T23:59:59.749998Z\tNaN\n" +
                                "4\t4\t2022-06-02T07:59:59.666664Z\tNaN\n" +
                                "5\t5\t2022-06-02T15:59:59.583330Z\tNaN\n" +
                                "33\t33\t2022-06-02T22:00:00.000000Z\t33\n" +
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
    public void testDetachAttachPartitionMissingMetadata() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabDetachAttachMissingMeta";
            try (TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY)) {
                createPopulateTable(tab
                                .timestamp("ts")
                                .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG)
                                .col("s2", ColumnType.SYMBOL),
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
                path.parent().concat(COLUMN_VERSION_FILE_NAME).$();
                Assert.assertTrue(Files.remove(path));
                renameDetachedToAttachable(tableName, "2022-06-01", "2022-06-02");
                compile("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01', '2022-06-02'", sqlExecutionContext);
                assertContent(
                        "ts\ts1\ti\tl\ts2\n" +
                                "2022-06-01T07:11:59.900000Z\tPEHN\t1\t1\tSXUX\n" +
                                "2022-06-01T14:23:59.800000Z\tVTJW\t2\t2\t\n" +
                                "2022-06-01T21:35:59.700000Z\t\t3\t3\tSXUX\n" +
                                "2022-06-02T04:47:59.600000Z\t\t4\t4\t\n" +
                                "2022-06-02T11:59:59.500000Z\t\t5\t5\tGPGW\n" +
                                "2022-06-02T19:11:59.400000Z\tPEHN\t6\t6\tRXGZ\n" +
                                "2022-06-03T02:23:59.300000Z\tCPSW\t7\t7\t\n" +
                                "2022-06-03T09:35:59.200000Z\t\t8\t8\t\n" +
                                "2022-06-03T16:47:59.100000Z\tPEHN\t9\t9\tRXGZ\n" +
                                "2022-06-03T23:59:59.000000Z\tVTJW\t10\t10\tIBBT\n",
                        tableName
                );
            }
        });
    }

    @Test
    public void testDetachAttachPartitionBrokenMetadataTableId() throws Exception {
        assertFailedAttachBecauseOfMetadata(
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
                "ALTER TABLE tabBrokenTableId2 ADD COLUMN s SHORT",
                "[-100] Detached partition metadata [table_id] is not compatible with current table metadata"
        );
    }

    @Test
    public void testDetachAttachPartitionBrokenMetadataTimestampIndex() throws Exception {
        assertFailedAttachBecauseOfMetadata(
                1,
                "tabBrokenTimestampIdx",
                "tabBrokenTimestampIdx2",
                brokenMeta -> brokenMeta
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG)
                        .timestamp("ts"),
                "insert into tabBrokenTimestampIdx2 " +
                        "select " +
                        "cast(x as int) i, " +
                        "x l, " +
                        "CAST(1654041600000000L AS TIMESTAMP) + x * 3455990000  ts " +
                        "from long_sequence(100))",
                "ALTER TABLE tabBrokenTimestampIdx2 ADD COLUMN s SHORT",
                "[-100] Detached partition metadata [timestamp_index] is not compatible with current table metadata"
        );
    }

    @Test
    public void testDetachAttachPartitionBrokenMetadataStructureVersion() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabBrokenStructureVersion";
            String brokenTableName = "tabBrokenStructureVersion2";
            try (
                    TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
                    TableModel brokenMeta = new TableModel(configuration, brokenTableName, PartitionBy.DAY);
                    MemoryMARW mem = Vm.getMARWInstance()
            ) {
                createPopulateTable(tab
                                .timestamp("ts")
                                .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG)
                                .col("s2", ColumnType.SYMBOL),
                        10,
                        "2022-06-01",
                        3
                );
                String expected = "ts\ts1\ti\tl\ts2\n" +
                        "2022-06-01T07:11:59.900000Z\tPEHN\t1\t1\tSXUX\n" +
                        "2022-06-01T14:23:59.800000Z\tVTJW\t2\t2\t\n" +
                        "2022-06-01T21:35:59.700000Z\t\t3\t3\tSXUX\n" +
                        "2022-06-02T04:47:59.600000Z\t\t4\t4\t\n" +
                        "2022-06-02T11:59:59.500000Z\t\t5\t5\tGPGW\n" +
                        "2022-06-02T19:11:59.400000Z\tPEHN\t6\t6\tRXGZ\n" +
                        "2022-06-03T02:23:59.300000Z\tCPSW\t7\t7\t\n" +
                        "2022-06-03T09:35:59.200000Z\t\t8\t8\t\n" +
                        "2022-06-03T16:47:59.100000Z\tPEHN\t9\t9\tRXGZ\n" +
                        "2022-06-03T23:59:59.000000Z\tVTJW\t10\t10\tIBBT\n";
                assertContent(expected, tableName);

                // create populate broken metadata table
                TableUtils.createTable(
                        configuration,
                        mem,
                        path,
                        brokenMeta.timestamp("ts")
                                .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG)
                                .col("s2", ColumnType.SYMBOL)
                                .col("ss", ColumnType.SHORT),
                        1
                );
                TestUtils.insertFromSelectIntoTable(compiler, sqlExecutionContext, brokenMeta, 100, "2022-06-01", 3);
                compile("ALTER TABLE " + brokenTableName + " ADD COLUMN s SHORT", sqlExecutionContext);

                // detach partitions and override detached metadata with broken metadata
                engine.clear();
                compile("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-02'", sqlExecutionContext);
                compile("ALTER TABLE " + brokenTableName + " DETACH PARTITION LIST '2022-06-02'", sqlExecutionContext);
                engine.clear();
                path.of(configuration.getDetachedRoot())
                        .concat(tableName)
                        .concat("2022-06-02")
                        .put(DETACHED_DIR_MARKER)
                        .concat(META_FILE_NAME)
                        .$();
                Assert.assertTrue(Files.remove(path));
                other.of(configuration.getDetachedRoot())
                        .concat(brokenTableName)
                        .concat("2022-06-02")
                        .put(DETACHED_DIR_MARKER)
                        .concat(META_FILE_NAME)
                        .$();
                Assert.assertEquals(Files.FILES_RENAME_OK, Files.rename(other, path));
                path.parent().concat(COLUMN_VERSION_FILE_NAME).$();
                Assert.assertTrue(Files.remove(path));
                other.parent().concat(COLUMN_VERSION_FILE_NAME).$();
                Assert.assertEquals(Files.FILES_RENAME_OK, Files.rename(other, path));

                renameDetachedToAttachable(tableName, "2022-06-02");

                // attempt to reattach
                assertFailure(
                        "ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-02'",
                        "[-100] Detached partition metadata [structure_version] is not compatible with current table metadata"
                );
            }
        });
    }

    @Test
    public void testDetachAttachPartitionBrokenMetadataColumnType() throws Exception {
        assertFailedAttachBecauseOfMetadata(
                1,
                "tabBrokenColumnType",
                "tabBrokenColumnType2",
                brokenMeta -> brokenMeta
                        .timestamp("ts")
                        .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.INT)
                        .col("s2", ColumnType.SYMBOL),
                "insert into tabBrokenColumnType2 " +
                        "select " +
                        "CAST(1654041600000000L AS TIMESTAMP) + x * 3455990000  ts, " +
                        "'SUGUS' s1, " +
                        "cast(x as int) i, " +
                        "x l, " +
                        "'PINE' s2 " +
                        "from long_sequence(100))",
                null,
                "[-100] Detached column [index=3, name=l, attribute=type] does not match current table metadata"
        );
    }

    @Test
    public void testDetachAttachPartitionBrokenMetadataIsIndexed() throws Exception {
        assertFailedAttachBecauseOfMetadata(
                1,
                "tabBrokenIsIndexed",
                "tabBrokenIsIndexed2",
                brokenMeta -> brokenMeta
                        .timestamp("ts")
                        .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG)
                        .col("s2", ColumnType.SYMBOL).indexed(true, 32),
                "insert into tabBrokenIsIndexed2 " +
                        "select " +
                        "CAST(1654041600000000L AS TIMESTAMP) + x * 3455990000  ts, " +
                        "'SUGUS' s1, " +
                        "cast(x as int) i, " +
                        "x l, " +
                        "'PINE' s2 " +
                        "from long_sequence(100))",
                null,
                "[-100] Detached column [index=4, name=s2, attribute=is_indexed] does not match current table metadata"
        );
    }

    @Test
    public void testDetachAttachPartitionPingPongConcurrent() throws Throwable {
        ConcurrentLinkedQueue<Throwable> exceptions = new ConcurrentLinkedQueue<>();
        assertMemoryLeak(() -> {
            String tableName = "tabPingPong";
            try (TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY)) {
                createPopulateTable(tab
                                .timestamp("ts")
                                .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG)
                                .col("s2", ColumnType.SYMBOL),
                        10,
                        "2022-06-01",
                        3
                );
            }

            CyclicBarrier start = new CyclicBarrier(2);
            CountDownLatch end = new CountDownLatch(2);
            AtomicBoolean isLive = new AtomicBoolean(true);
            Set<Long> detachedPartitionTimestamps = new ConcurrentSkipListSet<>();
            AtomicInteger detachedCount = new AtomicInteger();
            AtomicInteger attachedCount = new AtomicInteger();
            Rnd rnd = TestUtils.generateRandom();

            Thread detThread = new Thread(() -> {
                try {
                    start.await();
                    while (isLive.get()) {
                        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, "test")) {
                            long partitionTimestamp = (rnd.nextInt() % writer.getPartitionCount()) * Timestamps.DAY_MICROS;
                            if (!detachedPartitionTimestamps.contains(partitionTimestamp)) {
                                writer.detachPartition(partitionTimestamp);
                                detachedCount.incrementAndGet();
                                detachedPartitionTimestamps.add(partitionTimestamp);
                            }
                        } catch (Throwable e) {
                            exceptions.add(e);
                            LOG.error().$(e).$();
                        }
                        TimeUnit.MILLISECONDS.sleep(rnd.nextLong(15L));
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    end.countDown();
                }
            });
            detThread.start();

            Thread attThread = new Thread(() -> {
                try {
                    start.await();
                    while (isLive.get() || !detachedPartitionTimestamps.isEmpty()) {
                        if (!detachedPartitionTimestamps.isEmpty()) {
                            Iterator<Long> timestamps = detachedPartitionTimestamps.iterator();
                            if (timestamps.hasNext()) {
                                long partitionTimestamp = timestamps.next();
                                try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, "test")) {
                                    renameDetachedToAttachable(tableName, partitionTimestamp);
                                    writer.attachPartition(partitionTimestamp);
                                    timestamps.remove();
                                    attachedCount.incrementAndGet();
                                } catch (Throwable e) {
                                    exceptions.add(e);
                                    TimeUnit.MILLISECONDS.sleep(rnd.nextLong(11L));
                                    LOG.error().$(e).$();
                                }
                            }
                        }
                        TimeUnit.MILLISECONDS.sleep(rnd.nextLong(10L));
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    end.countDown();
                }
            });
            attThread.start();

            // give it 2 seconds
            long deadline = System.currentTimeMillis() + 2000;
            while (System.currentTimeMillis() < deadline) {
                TimeUnit.MILLISECONDS.sleep(300L);
            }
            isLive.set(false);
            end.await();
            for (int i = 0, limit = exceptions.size(); i < limit; i++) {
                Assert.assertTrue(exceptions.poll() instanceof EntryUnavailableException);
            }
            Assert.assertEquals(detachedCount.get(), attachedCount.get() + detachedPartitionTimestamps.size());
        });
    }

    private void assertCannotCopyMeta(String tableName, int copyCallId) throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY)) {
                createPopulateTable(tab
                                .timestamp("ts")
                                .col("i", ColumnType.INT)
                                .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                                .col("l", ColumnType.LONG)
                                .col("s2", ColumnType.SYMBOL),
                        10,
                        "2022-06-01",
                        4
                );
                String expected = "ts\ti\ts1\tl\ts2\n" +
                        "2022-06-01T09:35:59.900000Z\t1\tPEHN\t1\tSXUX\n" +
                        "2022-06-01T19:11:59.800000Z\t2\tVTJW\t2\t\n" +
                        "2022-06-02T04:47:59.700000Z\t3\t\t3\tSXUX\n" +
                        "2022-06-02T14:23:59.600000Z\t4\t\t4\t\n" +
                        "2022-06-02T23:59:59.500000Z\t5\t\t5\tGPGW\n" +
                        "2022-06-03T09:35:59.400000Z\t6\tPEHN\t6\tRXGZ\n" +
                        "2022-06-03T19:11:59.300000Z\t7\tCPSW\t7\t\n" +
                        "2022-06-04T04:47:59.200000Z\t8\t\t8\t\n" +
                        "2022-06-04T14:23:59.100000Z\t9\tPEHN\t9\tRXGZ\n" +
                        "2022-06-04T23:59:59.000000Z\t10\tVTJW\t10\tIBBT\n";
                assertContent(expected, tableName);

                AbstractCairoTest.ff = new FilesFacadeImpl() {
                    private int copyCallCount = 0;

                    public int copy(LPSZ from, LPSZ to) {
                        return ++copyCallCount == copyCallId ? -1 : super.copy(from, to);
                    }
                };
                engine.clear(); // to recreate the writer with the new ff
                long timestamp = TimestampFormatUtils.parseTimestamp("2022-06-01T00:00:00.000000Z");
                try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, "detach partition")) {
                    StatusCode statusCode = writer.detachPartition(timestamp);
                    Assert.assertEquals(StatusCode.PARTITION_CANNOT_COPY_META, statusCode);
                }

                assertContent(expected, tableName);

                // check no metadata files were left behind
                path.of(configuration.getRoot()).concat(tableName)
                        .concat("2022-06-01").concat(META_FILE_NAME)
                        .$();
                Assert.assertFalse(Files.exists(path));
                path.parent().concat(COLUMN_VERSION_FILE_NAME).$();
                Assert.assertFalse(Files.exists(path));
            }
        });
    }

    private void assertFailedAttachBecauseOfMetadata(
            int brokenMetaId,
            String tableName,
            String brokenTableName,
            Function<TableModel, TableModel> brokenMetaTransform,
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
                                .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG)
                                .col("s2", ColumnType.SYMBOL),
                        10,
                        "2022-06-01",
                        3
                );
                // create populate broken metadata table
                TableUtils.createTable(configuration, mem, path, brokenMetaTransform.apply(brokenMeta), brokenMetaId);
                if (insertStmt != null) {
                    compile(insertStmt, sqlExecutionContext);
                }
                if (finalStmt != null) {
                    compile(finalStmt, sqlExecutionContext);
                }

                // detach partitions and override detached metadata with broken metadata
                engine.clear();
                compile(
                        "ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-02'",
                        sqlExecutionContext
                );
                compile(
                        "ALTER TABLE " + brokenTableName + " DETACH PARTITION LIST '2022-06-02'",
                        sqlExecutionContext
                );
                engine.clear();
                path.of(configuration.getDetachedRoot())
                        .concat(tableName)
                        .concat("2022-06-02")
                        .put(DETACHED_DIR_MARKER)
                        .concat(META_FILE_NAME)
                        .$();
                Assert.assertTrue(Files.remove(path));
                other.of(configuration.getDetachedRoot())
                        .concat(brokenTableName)
                        .concat("2022-06-02")
                        .put(DETACHED_DIR_MARKER)
                        .concat(META_FILE_NAME)
                        .$();
                Assert.assertEquals(Files.FILES_RENAME_OK, Files.rename(other, path));

                renameDetachedToAttachable(tableName, "2022-06-02");

                // attempt to reattach
                assertFailure(
                        "ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-02'",
                        expectedErrorMessage
                );
            }
        });
    }

    private void dropCurrentVersionOfPartition(String tableName, String partitionName) throws SqlException {
        engine.clear();
        // hide the detached partition
        path.of(configuration.getDetachedRoot()).concat(tableName).concat(partitionName + ".detached").$();
        other.of(configuration.getDetachedRoot()).concat(tableName).concat(partitionName + ".detached.hide").$();
        Assert.assertEquals(Files.FILES_RENAME_OK, Files.rename(path, other));
        // drop the latest version of the partition
        compile("ALTER TABLE " + tableName + " DROP PARTITION LIST '" + partitionName + "'", sqlExecutionContext);
        // resurface the hidden detached partition
        Assert.assertEquals(Files.FILES_RENAME_OK, Files.rename(other, path));
    }

    private void renameDetachedToAttachable(String tableName, String... partitions) {
        for (String partition : partitions) {
            path.of(configuration.getDetachedRoot()).concat(tableName).concat(partition).put(DETACHED_DIR_MARKER).$();
            other.of(configuration.getDetachedRoot()).concat(tableName).concat(partition).put(ATTACHABLE_DIR_MARKER).$();
            Assert.assertTrue(Files.rename(path, other) > -1);
        }
    }

    private void renameDetachedToAttachable(String tableName, long... partitions) {
        for (long partition : partitions) {
            PartitionBy.setSinkForPartition(
                    path.of(configuration.getDetachedRoot()).concat(tableName),
                    PartitionBy.DAY,
                    partition,
                    false
            );
            path.put(DETACHED_DIR_MARKER).$();
            PartitionBy.setSinkForPartition(
                    other.of(configuration.getDetachedRoot()).concat(tableName),
                    PartitionBy.DAY,
                    partition,
                    false
            );
            other.put(ATTACHABLE_DIR_MARKER).$();
            Assert.assertTrue(Files.rename(path, other) > -1);
        }
    }

    private static void assertContent(String expected, String tableName) throws Exception {
        engine.clear();
        assertQuery(expected, tableName, null, "ts", true, false, true);
    }

    private void assertFailure(String tableName, String operation, String errorMsg) throws Exception {
        assertFailure(null, tableName, operation, errorMsg);
    }

    private void assertFailure(
            @Nullable FilesFacade withFf,
            String tableName,
            String operation,
            String errorMsg
    ) throws Exception {
        assertMemoryLeak(withFf, () -> {
            try (TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY)) {
                createPopulateTable(tab
                                .timestamp("ts")
                                .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG)
                                .col("s2", ColumnType.SYMBOL),
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
}
