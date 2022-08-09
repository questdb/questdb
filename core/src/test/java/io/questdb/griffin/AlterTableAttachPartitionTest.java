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
import io.questdb.cairo.sql.DataFrame;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.*;

import java.util.concurrent.atomic.AtomicInteger;


public class AlterTableAttachPartitionTest extends AbstractGriffinTest {
    private final static int DIR_MODE = configuration.getMkDirMode();
    private final static StringSink partitions = new StringSink();
    private Path path;
    private Path other;
    private Path other2;

    @Override
    @Before
    public void setUp() {
        super.setUp();
        other = new Path();
        other2 = new Path();
        path = new Path();
    }

    @Override
    @After
    public void tearDown() {
        super.tearDown();
        path = Misc.free(path);
        other = Misc.free(other);
        other2 = Misc.free(other);
    }

    @Test
    public void testAttach2Partitions() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src1", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst1", PartitionBy.DAY)) {
                createPopulateTable(
                        1,
                        src.timestamp("ts")
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG),
                        10000,
                        "2020-01-01",
                        12);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                attachFromSrcIntoDst(src, dst, "2020-01-09", "2020-01-10");
            }
        });
    }

    @Test
    public void testAttachActive2PartitionsOneByOneInDescOrder() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src2", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst2", PartitionBy.DAY)) {

                createPopulateTable(
                        1,
                        src.timestamp("ts")
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG),
                        10000,
                        "2020-01-01",
                        12);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                attachFromSrcIntoDst(src, dst, "2020-01-10");
                attachFromSrcIntoDst(src, dst, "2020-01-09");
            }
        });
    }

    @Test
    public void testAttachActive3Partitions() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src3", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst3", PartitionBy.DAY)) {

                createPopulateTable(
                        1,
                        src.timestamp("ts")
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG),
                        10000,
                        "2020-01-01",
                        12);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                // 3 partitions unordered
                attachFromSrcIntoDst(src, dst, "2020-01-09", "2020-01-10", "2020-01-01");
            }
        });
    }

    @Test
    public void testAttachActiveWrittenPartition() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src4", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst4", PartitionBy.DAY)) {

                createPopulateTable(
                        1,
                        src.timestamp("ts")
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG),
                        10000,
                        "2020-01-01",
                        11);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                attachFromSrcIntoDst(src, dst, "2020-01-10");
            }
        });
    }

    @Test
    public void testAttachFailsInvalidFormat() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel dst = new TableModel(configuration, "dst5", PartitionBy.MONTH)) {

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                String alterCommand = "ALTER TABLE " + dst.getName() + " ATTACH PARTITION LIST '202A-01'";
                try {
                    compile(alterCommand, sqlExecutionContext);
                    Assert.fail();
                } catch (SqlException e) {
                    Assert.assertEquals("[39] 'YYYY-MM' expected[errno=0]", e.getMessage());
                }
            }
        });
    }

    @Test
    public void testAttachFailsInvalidFormatPartitionsAnnually() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel dst = new TableModel(configuration, "dst6", PartitionBy.YEAR)) {

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                String alterCommand = "ALTER TABLE " + dst.getName() + " ATTACH PARTITION LIST '2020-01-01'";
                try {
                    compile(alterCommand, sqlExecutionContext);
                    Assert.fail();
                } catch (SqlException e) {
                    Assert.assertEquals("[39] 'YYYY' expected[errno=0]", e.getMessage());
                }
            }
        });
    }

    @Test
    public void testAttachFailsInvalidFormatPartitionsMonthly() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel dst = new TableModel(configuration, "dst7", PartitionBy.MONTH)) {

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                String alterCommand = "ALTER TABLE " + dst.getName() + " ATTACH PARTITION LIST '2020-01-01'";
                try {
                    compile(alterCommand, sqlExecutionContext);
                    Assert.fail();
                } catch (SqlException e) {
                    Assert.assertEquals("[39] 'YYYY-MM' expected[errno=0]", e.getMessage());
                }
            }
        });
    }

    @Test
    public void testAttachFailsInvalidSeparatorFormat() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel dst = new TableModel(configuration, "dst8", PartitionBy.MONTH)) {

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                String alterCommand = "ALTER TABLE " + dst.getName() + " ATTACH PARTITION LIST '2020-01'.'2020-02'";
                try {
                    compile(alterCommand, sqlExecutionContext);
                    Assert.fail();
                } catch (SqlException e) {
                    Assert.assertEquals("[48] ',' expected", e.getMessage());
                }
            }
        });
    }

    @Test
    public void testAttachMissingPartition() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel dst = new TableModel(configuration, "dst9", PartitionBy.DAY)) {
                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                String alterCommand = "ALTER TABLE " + dst.getName() + " ATTACH PARTITION LIST '2020-01-01'";
                try {
                    compile(alterCommand, sqlExecutionContext);
                    Assert.fail();
                } catch (SqlException e) {
                    Assert.assertEquals("[24] failed to attach partition '2020-01-01': PARTITION_CANNOT_ATTACH_MISSING", e.getMessage());
                }
            }
        });
    }

    @Test
    public void testAttachNonExisting() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel dst = new TableModel(configuration, "dst10", PartitionBy.DAY)) {
                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                String alterCommand = "ALTER TABLE " + dst.getName() + " ATTACH PARTITION LIST '2020-01-01'";

                try {
                    compile(alterCommand, sqlExecutionContext);
                    Assert.fail();
                } catch (SqlException e) {
                    Assert.assertEquals("[25] failed to attach partition '2020-01-01': PARTITION_CANNOT_ATTACH_MISSING", e.getMessage());
                }
            }
        });
    }

    @Test
    public void testAttachPartitionInWrongDirectoryName() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src11", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst11", PartitionBy.DAY)) {

                createPopulateTable(
                        1,
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        10000,
                        "2020-01-01",
                        1);

                CairoTestUtils.create(
                        dst.col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG)
                                .timestamp("ts"));

                copyPartitionToAttachable(src.getName(), "2020-01-01", dst.getName(), "COCONUTS");

                try {
                    compile("ALTER TABLE " + dst.getName() + " ATTACH PARTITION LIST '2020-01-02'", sqlExecutionContext);
                    Assert.fail();
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getMessage(), "[25] failed to attach partition '2020-01-02': PARTITION_CANNOT_ATTACH_MISSING");
                }
            }
        });
    }

    @Test
    public void testAttachPartitionMissingColumnType() throws Exception {
        assertMemoryLeak(() -> {

            AddColumn src = s -> s.col("l", ColumnType.LONG)
                    .col("i", ColumnType.INT)
                    .timestamp("ts");

            assertSchemaMismatch("src12", src, "dst12", dst -> dst.col("str", ColumnType.STRING), "[-100] Detached partition metadata [missing_column=str]");
            assertSchemaMismatch("src13", src, "dst13", dst -> dst.col("sym", ColumnType.SYMBOL), "[-100] Detached partition metadata [missing_column=sym]");
            assertSchemaMismatch("src14", src, "dst14", dst -> dst.col("l1", ColumnType.LONG), "[-100] Detached partition metadata [missing_column=l1]");
            assertSchemaMismatch("src15", src, "dst15", dst -> dst.col("i1", ColumnType.INT), "[-100] Detached partition metadata [missing_column=i1]");
            assertSchemaMismatch("src16", src, "dst16", dst -> dst.col("b", ColumnType.BOOLEAN), "[-100] Detached partition metadata [missing_column=b]");
            assertSchemaMismatch("src17", src, "dst17", dst -> dst.col("db", ColumnType.DOUBLE), "[-100] Detached partition metadata [missing_column=db]");
            assertSchemaMismatch("src18", src, "dst18", dst -> dst.col("fl", ColumnType.FLOAT), "[-100] Detached partition metadata [missing_column=fl]");
            assertSchemaMismatch("src19", src, "dst19", dst -> dst.col("dt", ColumnType.DATE), "[-100] Detached partition metadata [missing_column=dt]");
            assertSchemaMismatch("src20", src, "dst20", dst -> dst.col("tss", ColumnType.TIMESTAMP), "[-100] Detached partition metadata [missing_column=tss]");
            assertSchemaMismatch("src21", src, "dst21", dst -> dst.col("l256", ColumnType.LONG256), "[-100] Detached partition metadata [missing_column=l256]");
            assertSchemaMismatch("src22", src, "dst22", dst -> dst.col("bin", ColumnType.BINARY), "[-100] Detached partition metadata [missing_column=bin]");
            assertSchemaMismatch("src23", src, "dst23", dst -> dst.col("byt", ColumnType.BYTE), "[-100] Detached partition metadata [missing_column=byt]");
            assertSchemaMismatch("src24", src, "dst24", dst -> dst.col("chr", ColumnType.CHAR), "[-100] Detached partition metadata [missing_column=chr]");
            assertSchemaMismatch("src25", src, "dst25", dst -> dst.col("shrt", ColumnType.SHORT), "[-100] Detached partition metadata [missing_column=shrt]");
        });
    }

    @Test
    public void testAttachPartitionStringColIndexMessedNotInOrder() throws Exception {
        assertMemoryLeak(() -> {
            AddColumn src = s -> {
                s.col("l", ColumnType.LONG)
                        .col("i", ColumnType.INT)
                        .timestamp("ts")
                        .col("str", ColumnType.STRING);

            };

            assertSchemaMismatch(
                    "src26",
                    src,
                    "dst26",
                    dst -> {
                    },
                    s -> writeToStrIndexFile(s, "2022-08-01", "str.i", 0L, 16L),
                    "Variable size column has invalid data address value"
            );
        });
    }

    @Test
    public void testAttachPartitionStringColIndexMessedOffsetOutsideFileBounds() throws Exception {
        assertMemoryLeak(() -> {
            AddColumn src = s -> s.col("i", ColumnType.INT)
                    .col("l", ColumnType.LONG)
                    .timestamp("ts")
                    .col("str", ColumnType.STRING);

            assertSchemaMismatch(
                    "src27",
                    src,
                    "dst27",
                    dst -> {
                    },
                    s -> writeToStrIndexFile(s, "2022-08-01", "str.i", Long.MAX_VALUE, 256L),
                    "dataAddress=" + Long.MAX_VALUE
            );

            assertSchemaMismatch(
                    "src28",
                    src,
                    "dst28",
                    dst -> {
                    },
                    s -> writeToStrIndexFile(s, "2022-08-01", "str.i", -1L, 256L),
                    "dataAddress=" + -1L
            );
        });
    }

    @Test
    public void testAttachPartitionColumnChangedTypeAndMetaShouldMismatch() throws Exception {
        assertMemoryLeak(() -> {
            AddColumn src = s -> s.col("i", ColumnType.INT)
                    .col("l", ColumnType.LONG)
                    .timestamp("ts")
                    .col("sh", ColumnType.SHORT);

            assertSchemaMismatch(
                    "src29",
                    src,
                    "dst29",
                    dst -> dst.col("sh", -1 * ColumnType.SHORT),
                    s -> writeToStrIndexFile(s, "2022-08-01", "str.i", -1L, 256L),
                    "[-100] Detached partition metadata [structure_version should be different]"
            );
        });
    }

    @Test
    public void testAttachPartitionStringIndexFileTooSmall() throws Exception {
        assertMemoryLeak(() -> {
            AddColumn src = s -> s.col("i", ColumnType.INT)
                    .col("l", ColumnType.LONG)
                    .timestamp("ts")
                    .col("sh", ColumnType.STRING);

            assertSchemaMismatch(
                    "src30",
                    src,
                    "dst30",
                    dst -> {
                    },
                    s -> {
                        engine.clear();
                        path.of(configuration.getRoot()).concat(s.getName()).concat("2022-08-01").concat("sh.i").$();
                        long fd = Files.openRW(path);
                        Files.truncate(fd, Files.length(fd) / 4);
                        Files.close(fd);
                    },
                    "Column file is too small"
            );
        });
    }

    @Test
    public void testAttachPartitionSymbolFileNegativeValue() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src31", PartitionBy.DAY)) {
                createPopulateTable(
                        1,
                        src.timestamp("ts")
                                .col("l", ColumnType.LONG)
                                .col("sym", ColumnType.SYMBOL)
                                .col("i", ColumnType.INT),
                        10000,
                        "2022-08-01",
                        3);

                writeToStrIndexFile(src, "2022-08-02", "sym.d", -1L, 4L);

                try (TableModel dst = new TableModel(configuration, "dst31", PartitionBy.DAY)) {
                    createPopulateTable(
                            1,
                            dst.timestamp("ts")
                                    .col("l", ColumnType.LONG)
                                    .col("sym", ColumnType.SYMBOL)
                                    .col("i", ColumnType.INT),
                            10000,
                            "2022-08-01",
                            1);

                    try {
                        attachFromSrcIntoDst(src, dst, "2022-08-02");
                        Assert.fail();
                    } catch (SqlException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "Symbol file does not match symbol column, invalid key");
                    }
                }
            }
        });
    }

    @Test
    public void testAttachPartitionSymbolFileTooSmall() throws Exception {
        assertMemoryLeak(() -> {

            AddColumn src = s -> s.col("i", ColumnType.INT)
                    .col("l", ColumnType.LONG)
                    .timestamp("ts")
                    .col("sh", ColumnType.SYMBOL);

            assertSchemaMismatch(
                    "src32",
                    src,
                    "dst32",
                    dst -> {
                    },
                    s -> {
                        // .v file
                        engine.clear();
                        path.of(configuration.getRoot()).concat(s.getName()).concat("2022-08-01").concat("sh.v").$();
                        long fd = Files.openRW(path);
                        Files.truncate(fd, Files.length(fd) / 2);
                        Files.close(fd);
                    },
                    "Symbol file does not match symbol column"
            );
        });
    }

    @Test
    public void testAttachPartitionWhereTimestampColumnNameIsOtherThanTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src33", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst33", PartitionBy.DAY)) {

                createPopulateTable(
                        1,
                        src.timestamp("ts")
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG),
                        10000,
                        "2022-08-01",
                        10);

                CairoTestUtils.create(dst.timestamp("ts1")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                try {
                    attachFromSrcIntoDst(src, dst, "2022-08-01");
                    Assert.fail();
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(),
                            "[-100] Detached partition metadata [missing_column=ts1] is not compatible with current table metadata"
                    );
                }
            }
        });
    }

    @Test
    public void testAttachPartitionWithColumnTypes() throws Exception {
        int idx = 0;
        assertSchemaMatch(dst -> dst.col("str", ColumnType.STRING), idx++);
        assertSchemaMatch(dst -> dst.col("l1", ColumnType.LONG), idx++);
        assertSchemaMatch(dst -> dst.col("i1", ColumnType.INT), idx++);
        assertSchemaMatch(dst -> dst.col("b", ColumnType.BOOLEAN), idx++);
        assertSchemaMatch(dst -> dst.col("db", ColumnType.DOUBLE), idx++);
        assertSchemaMatch(dst -> dst.col("fl", ColumnType.FLOAT), idx++);
        assertSchemaMatch(dst -> dst.col("dt", ColumnType.DATE), idx++);
        assertSchemaMatch(dst -> dst.col("ts1", ColumnType.TIMESTAMP), idx++);
        assertSchemaMatch(dst -> dst.col("l256", ColumnType.LONG256), idx++);
        assertSchemaMatch(dst -> dst.col("byt", ColumnType.BYTE), idx++);
        assertSchemaMatch(dst -> dst.col("ch", ColumnType.CHAR), idx++);
        assertSchemaMatch(dst -> dst.col("sh", ColumnType.SHORT), idx);
    }

    @Test
    public void testAttachPartitionWrongFixedColumn() throws Exception {
        assertMemoryLeak(() -> {
            AddColumn src = s -> s.col("l", ColumnType.LONG)
                    .col("i", ColumnType.INT)
                    .timestamp("ts")
                    .col("sh", ColumnType.SHORT);

            assertSchemaMismatch(
                    "src34",
                    src,
                    "dst34",
                    dst -> {
                    },
                    s -> {
                        // .d file
                        engine.clear();
                        path.of(configuration.getRoot()).concat(s.getName()).concat("2022-08-01").concat("sh.d").$();
                        long fd = Files.openRW(path);
                        Files.truncate(fd, Files.length(fd) / 10);
                        Files.close(fd);
                    },
                    "Column file is too small"
            );
        });
    }

    @Test
    public void testAttachPartitionsNonPartitioned() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src35", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst35", PartitionBy.NONE)) {

                createPopulateTable(
                        1,
                        src.timestamp("ts")
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG),
                        10000,
                        "2022-08-01",
                        10);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                try {
                    attachFromSrcIntoDst(src, dst, "2022-08-01");
                    Assert.fail();
                } catch (SqlException e) {
                    TestUtils.assertEquals("[25] table is not partitioned", e.getMessage());
                }
            }
        });
    }

    @Test
    public void testAttachPartitionsTableInTransaction() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src36", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst36", PartitionBy.DAY)) {

                int partitionRowCount = 111;
                createPopulateTable(
                        1,
                        src.timestamp("ts")
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG),
                        partitionRowCount,
                        "2022-08-01",
                        1);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                copyPartitionToAttachable(src.getName(), "2022-08-01", dst.getName(), "2022-08-01");

                // Add 1 row without commit
                long timestamp = TimestampFormatUtils.parseTimestamp("2022-08-01T00:00:00.000z");
                try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, dst.getName(), "testing")) {
                    long insertTs = TimestampFormatUtils.parseTimestamp("2022-08-01T23:59:59.999z");
                    TableWriter.Row row = writer.newRow(insertTs + 1000L);
                    row.putLong(0, 1L);
                    row.putInt(1, 1);
                    row.append();

                    Assert.assertTrue(writer.inTransaction());

                    // This commits the append before attaching
                    writer.attachPartition(timestamp);
                    Assert.assertEquals(partitionRowCount + 1, writer.size());

                    Assert.assertFalse(writer.inTransaction());
                }
            }
        });
    }

    @Test
    public void testAttachPartitionsWithIndexedSymbolsValueMatch() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src37", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst37", PartitionBy.DAY)) {

                createPopulateTable(
                        1,
                        src.timestamp("ts")
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG)
                                .col("s", ColumnType.SYMBOL).indexed(false, 4096),
                        10000,
                        "2022-08-01",
                        10);

                // Make sure nulls are included in the partition to be attached
                assertSql("select count() from " + src.getName() + " where ts in '2022-08-09' and s = null", "count\n302\n");

                createPopulateTable(
                        1,
                        dst.timestamp("ts")
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG)
                                .col("s", ColumnType.SYMBOL).indexed(false, 4096),
                        10000,
                        "2022-08-01",
                        10);

                compile("alter table " + dst.getName() + " drop partition list '2022-08-09'");

                attachFromSrcIntoDst(src, dst, "2022-08-09");
            }
        });
    }

    @Test
    public void testAttachPartitionsWithSymbolsValueDoesNotMatch() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src38", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst38", PartitionBy.DAY)) {

                createPopulateTable(
                        1,
                        src.timestamp("ts")
                                .col("i", ColumnType.INT)
                                .col("s2", ColumnType.SYMBOL)
                                .col("l", ColumnType.LONG),
                        10000,
                        "2022-08-01",
                        10);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG)
                        .col("s", ColumnType.SYMBOL));

                try {
                    attachFromSrcIntoDst(src, dst, "2022-08-09");
                    Assert.fail();
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(),
                            "[-100] Detached partition metadata [missing_column=s] is not compatible with current table metadata"
                    );
                }
            }
        });
    }

    @Test
    public void testAttachPartitionsWithSymbolsValueMatch() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src39", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst39", PartitionBy.DAY)) {

                createPopulateTable(
                        1,
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("s", ColumnType.SYMBOL)
                                .timestamp("ts"),
                        10000,
                        "2022-08-01",
                        10);

                // Make sure nulls are included in the partition to be attached
                assertSql("select count() from " + src.getName() + " where ts in '2022-08-09' and s = null", "count\n302\n");

                createPopulateTable(
                        1,
                        dst.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("s", ColumnType.SYMBOL)
                                .timestamp("ts"),
                        10000,
                        "2022-08-01",
                        10);

                compile("alter table " + dst.getName() + " drop partition list '2022-08-09'");

                attachFromSrcIntoDst(src, dst, "2022-08-09");
            }
        });
    }

    @Test
    public void testAttachPartitionsWithSymbolsValueMatchWithNoIndex() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src40", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst40", PartitionBy.DAY)) {

                createPopulateTable(
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("s", ColumnType.SYMBOL)
                                .timestamp("ts"),
                        10000,
                        "2022-08-01",
                        10);

                createPopulateTable(
                        1,
                        dst.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("s", ColumnType.SYMBOL).indexed(true, 4096)
                                .timestamp("ts"),
                        10000,
                        "2022-08-01",
                        10);

                compile("alter table " + dst.getName() + " drop partition list '2022-08-09'");

                try {
                    attachFromSrcIntoDst(src, dst, "2022-08-09");
                } catch (SqlException ex) {
                    TestUtils.assertContains(ex.getFlyweightMessage(),
                            "[-100] Detached column [index=2, name=s, attribute=is_indexed] does not match current table metadata"
                    );
                }
            }
        });
    }

    @Test
    public void testAttachPartitionsWithSymbolsValueMatchWithNoIndexKeyFile() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src41", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst41", PartitionBy.DAY)) {

                createPopulateTable(
                        1,
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("s", ColumnType.SYMBOL).indexed(true, 4096)
                                .timestamp("ts"),
                        10000,
                        "2022-08-01",
                        10);

                createPopulateTable(
                        1,
                        dst.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("s", ColumnType.SYMBOL).indexed(true, 4096)
                                .timestamp("ts"),
                        10000,
                        "2022-08-01",
                        10);

                compile("alter table " + dst.getName() + " drop partition list '2022-08-09'");

                // remove .k
                engine.clear();
                Assert.assertTrue(Files.remove(
                        path.of(configuration.getRoot()).concat(src.getName()).concat("2022-08-09").concat("s.k").$()
                ));

                try {
                    attachFromSrcIntoDst(src, dst, "2022-08-09");
                } catch (SqlException ex) {
                    TestUtils.assertContains(ex.getFlyweightMessage(), "Symbol index key file does not exist");
                }
            }
        });
    }

    @Test
    public void testAttachSamePartitionTwice() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src42", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst42", PartitionBy.DAY)) {

                createPopulateTable(
                        1,
                        src.timestamp("ts")
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG),
                        10000,
                        "2022-08-01",
                        10);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                attachFromSrcIntoDst(src, dst, "2022-08-09");

                String alterCommand = "ALTER TABLE " + dst.getName() + " ATTACH PARTITION LIST '2022-08-09'";

                try {
                    compile(alterCommand, sqlExecutionContext);
                    Assert.fail();
                } catch (SqlException e) {
                    Assert.assertEquals("[25] failed to attach partition '2022-08-09': PARTITION_ALREADY_ATTACHED", e.getMessage());
                }
            }
        });
    }

    @Test
    public void testCannotMapTimestampColumn() throws Exception {
        AtomicInteger counter = new AtomicInteger(2);
        FilesFacadeImpl ff = new FilesFacadeImpl() {
            private long tsdFd;

            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                if (tsdFd != fd) {
                    return super.mmap(fd, len, offset, flags, memoryTag);
                }
                tsdFd = 0;
                return -1;
            }

            @Override
            public long openRO(LPSZ name) {
                long fd = super.openRO(name);
                if (Chars.endsWith(name, "ts.d") && counter.decrementAndGet() == 0) {
                    this.tsdFd = fd;
                }
                return fd;
            }
        };

        testSqlFailedOnFsOperation(ff, "srcMap", "dstMap", "could not mmap");
    }

    @Test
    public void testCannotReadTimestampColumn() throws Exception {
        AtomicInteger counter = new AtomicInteger(1);
        FilesFacadeImpl ff = new FilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (Chars.endsWith(name, "ts.d") && counter.decrementAndGet() == 0) {
                    return -1;
                }
                return super.openRO(name);
            }
        };

        testSqlFailedOnFsOperation(ff, "srcTs", "dstTs", "could not open read-only");
    }

    @Test
    public void testCannotReadTimestampColumnFileDoesNotExist() throws Exception {
        AtomicInteger counter = new AtomicInteger(2);
        FilesFacadeImpl ff = new FilesFacadeImpl() {
            @Override
            public boolean exists(LPSZ name) {
                if (Chars.endsWith(name, "ts.d") && counter.decrementAndGet() == 0) {
                    return false;
                }
                return super.exists(name);
            }
        };

        testSqlFailedOnFsOperation(ff, "srcTs2", "dstTs2", "[-100] Detached partition metadata [missing_column=ts]");
    }

    @Test
    public void testCannotRenameDetachedFolderOnAttach() throws Exception {
        AtomicInteger counter = new AtomicInteger(1);
        FilesFacadeImpl ff = new FilesFacadeImpl() {
            @Override
            public int rename(LPSZ from, LPSZ to) {
                if (Chars.contains(to, "2020-01-01") && counter.decrementAndGet() == 0) {
                    return Files.FILES_RENAME_ERR_OTHER;
                }
                return super.rename(from, to);
            }
        };

        testSqlFailedOnFsOperation(ff, "srcRen", "dstRen", "PARTITION_FOLDER_CANNOT_RENAME");
    }

    @Test
    public void testDetachAttachDifferentPartitionTableReaderReload() throws Exception {
        if (FilesFacadeImpl.INSTANCE.isRestrictedFileSystem()) {
            // cannot remove opened files on Windows, test  not relevant
            return;
        }

        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src47", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst47", PartitionBy.DAY)) {

                int partitionRowCount = 5;
                createPopulateTable(
                        1,
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("str", ColumnType.STRING)
                                .timestamp("ts"),
                        partitionRowCount,
                        "2020-01-09",
                        2);

                createPopulateTable(
                        1,
                        dst.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("str", ColumnType.STRING)
                                .timestamp("ts"),
                        partitionRowCount - 3,
                        "2020-01-09",
                        2);

                try (TableReader dstReader = new TableReader(configuration, dst.getTableName())) {
                    dstReader.openPartition(0);
                    dstReader.openPartition(1);
                    dstReader.goPassive();

                    long timestamp = TimestampFormatUtils.parseTimestamp("2020-01-09T00:00:00.000z");

                    try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, dst.getTableName(), "testing")) {
                        writer.removePartition(timestamp);
                        copyPartitionToAttachable(src.getName(), "2020-01-09", dst.getName(), "2020-01-09");
                        Assert.assertEquals(StatusCode.OK, writer.attachPartition(timestamp));
                    }

                    // Go active
                    Assert.assertTrue(dstReader.reload());
                    try (TableReader srcReader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, src.getTableName())) {
                        String expected =
                                "l\ti\tstr\tts\n" +
                                        "1\t1\t1\t2020-01-09T09:35:59.800000Z\n" +
                                        "2\t2\t2\t2020-01-09T19:11:59.600000Z\n" +
                                        "3\t3\t3\t2020-01-10T04:47:59.400000Z\n" +
                                        "4\t4\t4\t2020-01-10T14:23:59.200000Z\n" +
                                        "5\t5\t5\t2020-01-10T23:59:59.000000Z\n";
                        assertCursor(expected, srcReader.getCursor(), srcReader.getMetadata(), true);

                        // Check that first 2 lines of partition 2020-01-09 match for src and dst tables
                        assertCursor("l\ti\tstr\tts\n" +
                                        "1\t1\t1\t2020-01-09T09:35:59.800000Z\n" +
                                        "2\t2\t2\t2020-01-09T19:11:59.600000Z\n" +
                                        "2\t2\t2\t2020-01-10T23:59:59.000000Z\n",
                                dstReader.getCursor(),
                                dstReader.getMetadata(),
                                true);
                    }
                }
            }
        });
    }

    @Test
    public void testAttachPartitionsDetachedHasExtraColumn() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src48", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst48", PartitionBy.DAY)) {

                int partitionRowCount = 11;
                createPopulateTable(
                        1,
                        src.timestamp("ts")
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG)
                                .col("s", ColumnType.SYMBOL).indexed(true, 128),
                        partitionRowCount,
                        "2022-08-01",
                        4);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                copyPartitionToAttachable(src.getName(), "2022-08-01", dst.getName(), "2022-08-01");

                long timestamp = TimestampFormatUtils.parseTimestamp("2022-08-01T00:00:00.000z");
                try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, dst.getName(), "testing")) {
                    writer.attachPartition(timestamp);
                }
                path.of(configuration.getRoot()).concat(dst.getName()).concat("2022-08-01");
                int pathLen = path.length();
                Assert.assertFalse(Files.exists(path.concat("s.d").$()));
                Assert.assertFalse(Files.exists(path.trimTo(pathLen).concat("s.i").$()));
                Assert.assertFalse(Files.exists(path.trimTo(pathLen).concat("s.k").$()));
                Assert.assertFalse(Files.exists(path.trimTo(pathLen).concat("s.v").$()));
                Assert.assertTrue(Files.exists(path.trimTo(pathLen).concat("l.d").$()));

                engine.clear();
                assertQuery(
                        "ts\ti\tl\n" +
                                "2022-08-01T08:43:38.090909Z\t1\t1\n" +
                                "2022-08-01T17:27:16.181818Z\t2\t2\n",
                        dst.getName(),
                        null,
                        "ts",
                        true,
                        false,
                        true
                );
            }
        });
    }

    @Test
    public void testAttachPartitionsDeletedColumnFromSrc() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src49", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst49", PartitionBy.DAY)) {

                createPopulateTable(
                        1,
                        src.timestamp("ts")
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG)
                                .col("s", ColumnType.SYMBOL).indexed(true, 128)
                                .col("str", ColumnType.STRING),
                        8,
                        "2022-08-01",
                        4);
                try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, src.getName(), "testing")) {
                    writer.removeColumn("s");
                    writer.removeColumn("str");
                }

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG)
                        .col("s", ColumnType.SYMBOL).indexed(true, 128)
                        .col("str", ColumnType.STRING)
                );

                copyPartitionToAttachable(src.getName(), "2022-08-02", dst.getName(), "2022-08-02");
                compile("ALTER TABLE " + dst.getName() + " ATTACH PARTITION LIST '2022-08-02'", sqlExecutionContext);

                engine.clear();
                assertQuery(
                        "ts\ti\tl\ts\tstr\n" +
                                "2022-08-02T11:59:59.625000Z\t3\t3\t\t\n" +
                                "2022-08-02T23:59:59.500000Z\t4\t4\t\t\n",
                        dst.getName(),
                        null,
                        "ts",
                        true,
                        false,
                        true
                );
            }
        });
    }

    private void assertSchemaMatch(AddColumn tm, int idx) throws Exception {
        if (idx > 0) {
            setUp();
        }
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "srcCM" + idx, PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dstCM" + idx, PartitionBy.DAY)) {
                src.col("l", ColumnType.LONG)
                        .col("i", ColumnType.INT)
                        .timestamp("ts");
                tm.add(src);

                createPopulateTable(
                        src,
                        10000,
                        "2022-08-01",
                        10);

                dst.col("l", ColumnType.LONG)
                        .col("i", ColumnType.INT)
                        .timestamp("ts");
                tm.add(dst);

                CairoTestUtils.create(dst);
                attachFromSrcIntoDst(src, dst, "2022-08-01");
            }
        });
        if (idx > 0) {
            tearDown();
        }
    }

    private void assertSchemaMismatch(
            String srcTableName,
            AddColumn srcTransform,
            String dstTableName,
            AddColumn dstTransform,
            String errorMessage
    ) throws Exception {
        assertSchemaMismatch(srcTableName, srcTransform, dstTableName, dstTransform, null, errorMessage);
    }

    private void assertSchemaMismatch(
            String srcTableName,
            AddColumn srcTransform,
            String dstTableName,
            AddColumn dstTransform,
            AddColumn afterCreateSrc,
            String errorMessage
    ) throws Exception {
        try (
                TableModel src = new TableModel(configuration, srcTableName, PartitionBy.DAY);
                TableModel dst = new TableModel(configuration, dstTableName, PartitionBy.DAY)
        ) {
            srcTransform.add(src);
            createPopulateTable(
                    1,
                    src,
                    45000,
                    "2022-08-01",
                    10
            );

            if (afterCreateSrc != null) {
                afterCreateSrc.add(src);
            }

            // make dst a copy of src
            int tsIdx = src.getTimestampIndex();
            for (int i = 0, limit = src.getColumnCount(); i < limit; i++) {
                if (i != tsIdx) {
                    dst.col(src.getColumnName(i), src.getColumnType(i));
                    if (src.isIndexed(i)) {
                        dst.indexed(true, src.getIndexBlockCapacity(i));
                    }
                } else {
                    dst.timestamp(src.getColumnName(i));
                }
            }

            // apply transform to dst and create table
            dstTransform.add(dst);
            CairoTestUtils.create(dst);
            try {
                attachFromSrcIntoDst(src, dst, "2022-08-01");
                Assert.fail("Expected exception with '" + errorMessage + "' message");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), errorMessage);
            }
            Files.rmdir(path.of(root).concat(dstTableName).concat("2022-08-01").put(TableUtils.ATTACHABLE_DIR_MARKER).$());
        }
    }

    private void attachFromSrcIntoDst(TableModel src, TableModel dst, String... partitionList) throws SqlException, NumericException {
        partitions.clear();
        for (int i = 0; i < partitionList.length; i++) {
            String partition = partitionList[i];
            if (i > 0) {
                partitions.put(",");
            }
            partitions.put("'");
            partitions.put(partition);
            partitions.put("'");
        }

        engine.clear();
        compile(
                "ALTER TABLE " + src.getName() + " DETACH PARTITION LIST " + partitions + ";",
                sqlExecutionContext
        );

        engine.clear();
        path.of(configuration.getDetachedRoot()).concat(src.getName());
        int pathLen = path.length();
        other.of(configuration.getDetachedRoot()).concat(dst.getName());
        int otherLen = other.length();
        for (int i = 0; i < partitionList.length; i++) {
            String partition = partitionList[i];
            path.trimTo(pathLen).concat(partition).put(TableUtils.DETACHED_DIR_MARKER).$();
            other.trimTo(otherLen).concat(partition).put(TableUtils.ATTACHABLE_DIR_MARKER).$();
            TestUtils.copyDirectory(path, other, DIR_MODE);
        }

        int rowCount = readAllRows(dst.getName());
        engine.clear();
        compile(
                "ALTER TABLE " + dst.getName() + " ATTACH PARTITION LIST " + partitions + ";",
                sqlExecutionContext
        );
        int newRowCount = readAllRows(dst.getName());
        Assert.assertTrue(newRowCount > rowCount);

        long timestamp = 0;
        for (String s : partitionList) {
            long ts = TimestampFormatUtils.parseTimestamp(s
                    + (src.getPartitionBy() == PartitionBy.YEAR ? "-01-01" : "")
                    + (src.getPartitionBy() == PartitionBy.MONTH ? "-01" : "")
                    + "T23:59:59.999z");
            if (ts > timestamp) {
                timestamp = ts;
            }
        }

        // Check table is writable after partition attach
        engine.clear();
        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, dst.getName(), "testing")) {
            TableWriter.Row row = writer.newRow(timestamp);
            row.putInt(1, 1);
            row.append();
            writer.commit();
        }
    }

    private void copyPartitionToAttachable(
            String srcTableName,
            String srcPartitionName,
            String dstTableName,
            String dstPartitionName
    ) {
        path.of(configuration.getRoot())
                .concat(srcTableName)
                .concat(srcPartitionName)
                .slash$();
        other.of(configuration.getDetachedRoot())
                .concat(dstTableName)
                .concat(dstPartitionName)
                .put(TableUtils.ATTACHABLE_DIR_MARKER)
                .slash$();

        TestUtils.copyDirectory(path, other, DIR_MODE);

        // copy _meta
        Files.copy(
                path.parent().parent().concat(TableUtils.META_FILE_NAME).$(),
                other.parent().concat(TableUtils.META_FILE_NAME).$()
        );
        // copy _cv
        Files.copy(
                path.parent().concat(TableUtils.COLUMN_VERSION_FILE_NAME).$(),
                other.parent().concat(TableUtils.COLUMN_VERSION_FILE_NAME).$()
        );
    }

    private CharSequence executeSql(String sql) throws SqlException {
        TestUtils.printSql(
                compiler,
                sqlExecutionContext,
                sql,
                sink
        );
        return sink;
    }

    private int readAllRows(String tableName) {
        try (FullFwdDataFrameCursor cursor = new FullFwdDataFrameCursor()) {
            cursor.of(engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName));
            DataFrame frame;
            int count = 0;
            while ((frame = cursor.next()) != null) {
                for (long index = frame.getRowHi() - 1, lo = frame.getRowLo() - 1; index > lo; index--) {
                    count++;
                }
            }
            return count;
        } catch (CairoException err) {
            return 0;
        }
    }

    private void testSqlFailedOnFsOperation(FilesFacadeImpl ff, String srcTableName, String dstTableName, String... errorContains) throws Exception {
        assertMemoryLeak(ff, () -> {
            try (
                    TableModel src = new TableModel(configuration, srcTableName, PartitionBy.DAY);
                    TableModel dst = new TableModel(configuration, dstTableName, PartitionBy.DAY)
            ) {

                createPopulateTable(
                        1,
                        src.timestamp("ts")
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG),
                        100,
                        "2020-01-01",
                        3);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                try {
                    attachFromSrcIntoDst(src, dst, "2020-01-01");
                    Assert.fail();
                } catch (SqlException e) {
                    for (String error : errorContains) {
                        TestUtils.assertContains(e.getFlyweightMessage(), error);
                    }
                }

                // second attempt without FilesFacade override should work ok
                attachFromSrcIntoDst(src, dst, "2020-01-02");
            }
        });
    }

    private void writeToStrIndexFile(TableModel src, String partition, String columnFileName, long value, long offset) {
        FilesFacade ff = FilesFacadeImpl.INSTANCE;
        long fd = -1;
        long writeBuff = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            // .i file
            engine.clear();
            path.of(configuration.getRoot()).concat(src.getName()).concat(partition).concat(columnFileName).$();
            fd = ff.openRW(path, CairoConfiguration.O_NONE);
            Unsafe.getUnsafe().putLong(writeBuff, value);
            ff.write(fd, writeBuff, Long.BYTES, offset);
        } finally {
            ff.close(fd);
            Unsafe.free(writeBuff, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @FunctionalInterface
    private interface AddColumn {
        void add(TableModel tm);
    }
}
