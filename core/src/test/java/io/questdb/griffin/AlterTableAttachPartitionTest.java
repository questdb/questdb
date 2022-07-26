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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;


public class AlterTableAttachPartitionTest extends AbstractGriffinTest {
    private final static Log LOG = LogFactory.getLog(AlterTableAttachPartitionTest.class);
    private final int DIR_MODE = configuration.getMkDirMode();

    @Test
    public void testAttachActive2Partitions() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst", PartitionBy.DAY)) {

                createPopulateTable(
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        10000,
                        "2020-01-01",
                        10);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                copyAttachPartition(src, dst, 0, "2020-01-09", "2020-01-10");
            }
        });
    }

    @Test
    public void testAttachActive2PartitionsOneByOneInDescOrder() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst", PartitionBy.DAY)) {

                createPopulateTable(
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        10000,
                        "2020-01-01",
                        10);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                copyAttachPartition(src, dst, 0, "2020-01-10");
                copyAttachPartition(src, dst, 1001, "2020-01-09");
            }
        });
    }

    @Test
    public void testAttachActive3Partitions() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst", PartitionBy.DAY)) {

                createPopulateTable(
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        10000,
                        "2020-01-01",
                        10);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                // 3 partitions unordered
                copyAttachPartition(src, dst, 0, "2020-01-09", "2020-01-10", "2020-01-01");
            }
        });
    }

    @Test
    public void testAttachActiveWrittenPartition() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst", PartitionBy.DAY)) {

                createPopulateTable(
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        10000,
                        "2020-01-01",
                        10);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                copyAttachPartition(src, dst, 0, "2020-01-10");
            }
        });
    }

    @Test
    public void testAttachFailsInvalidFormat() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel dst = new TableModel(configuration, "dst", PartitionBy.MONTH)) {

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                String alterCommand = "ALTER TABLE dst ATTACH PARTITION LIST '202A-01'";
                try {
                    compile(alterCommand, sqlExecutionContext);
                    Assert.fail();
                } catch (SqlException e) {
                    Assert.assertEquals("[38] 'YYYY-MM' expected[errno=0]", e.getMessage());
                }
            }
        });
    }

    @Test
    public void testAttachFailsInvalidFormatPartitionsAnnually() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel dst = new TableModel(configuration, "dst", PartitionBy.YEAR)) {

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                String alterCommand = "ALTER TABLE dst ATTACH PARTITION LIST '2020-01-01'";
                try {
                    compile(alterCommand, sqlExecutionContext);
                    Assert.fail();
                } catch (SqlException e) {
                    Assert.assertEquals("[38] 'YYYY' expected[errno=0]", e.getMessage());
                }
            }
        });
    }

    @Test
    public void testAttachFailsInvalidFormatPartitionsMonthly() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel dst = new TableModel(configuration, "dst", PartitionBy.MONTH)) {

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                String alterCommand = "ALTER TABLE dst ATTACH PARTITION LIST '2020-01-01'";
                try {
                    compile(alterCommand, sqlExecutionContext);
                    Assert.fail();
                } catch (SqlException e) {
                    Assert.assertEquals("[38] 'YYYY-MM' expected[errno=0]", e.getMessage());
                }
            }
        });
    }

    @Test
    public void testAttachFailsInvalidSeparatorFormat() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel dst = new TableModel(configuration, "dst", PartitionBy.MONTH)) {

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                String alterCommand = "ALTER TABLE dst ATTACH PARTITION LIST '2020-01'.'2020-02'";
                try {
                    compile(alterCommand, sqlExecutionContext);
                    Assert.fail();
                } catch (SqlException e) {
                    Assert.assertEquals("[47] ',' expected", e.getMessage());
                }
            }
        });
    }

    @Test
    public void testAttachMissingPartition() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel dst = new TableModel(configuration, "dst", PartitionBy.DAY)) {
                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                String alterCommand = "ALTER TABLE dst ATTACH PARTITION LIST '2020-01-01'";
                try {
                    compile(alterCommand, sqlExecutionContext);
                    Assert.fail();
                } catch (SqlException e) {
                    Assert.assertEquals("[23] attach partition failed, folder '2020-01-01' does not exist", e.getMessage());
                }
            }
        });
    }

    @Test
    public void testAttachNonExisting() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel dst = new TableModel(configuration, "dst", PartitionBy.DAY)) {
                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                String alterCommand = "ALTER TABLE dst ATTACH PARTITION LIST '2020-01-01'";

                try {
                    compile(alterCommand, sqlExecutionContext);
                    Assert.fail();
                } catch (SqlException e) {
                    Assert.assertEquals("[23] attach partition failed, folder '2020-01-01' does not exist", e.getMessage());
                }
            }
        });
    }

    @Test
    public void testAttachPartitionInWrongDirectoryName() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst", PartitionBy.DAY)) {

                createPopulateTable(
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        10000,
                        "2020-01-01",
                        1);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                copyPartitionToBackup(src.getName(), "2020-01-01", dst.getName(), "2020-01-02");
                try {
                    String alterCommand = "ALTER TABLE dst ATTACH PARTITION LIST '2020-01-02'";
                    compile(alterCommand, sqlExecutionContext);
                    Assert.fail();
                } catch (io.questdb.griffin.SqlException e) {
                    TestUtils.assertContains(e.getMessage(), "failed to attach partition '2020-01-02', data does not correspond to the partition folder or partition is empty");
                }
            }
        });
    }

    @Test
    public void testAttachPartitionMissingColumnType() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.DAY)) {

                createPopulateTable(
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        45000,
                        "2020-01-09",
                        2);

                assertSchemaMismatch(src, dst -> dst.col("str", ColumnType.STRING), "Column file does not exist");
                assertSchemaMismatch(src, dst -> dst.col("sym", ColumnType.SYMBOL), "Column file does not exist");
                assertSchemaMismatch(src, dst -> dst.col("l1", ColumnType.LONG), "Column file does not exist");
                assertSchemaMismatch(src, dst -> dst.col("i1", ColumnType.INT), "Column file does not exist");
                assertSchemaMismatch(src, dst -> dst.col("b", ColumnType.BOOLEAN), "Column file does not exist");
                assertSchemaMismatch(src, dst -> dst.col("db", ColumnType.DOUBLE), "Column file does not exist");
                assertSchemaMismatch(src, dst -> dst.col("fl", ColumnType.FLOAT), "Column file does not exist");
                assertSchemaMismatch(src, dst -> dst.col("dt", ColumnType.DATE), "Column file does not exist");
                assertSchemaMismatch(src, dst -> dst.col("ts", ColumnType.TIMESTAMP), "Column file does not exist");
                assertSchemaMismatch(src, dst -> dst.col("ts", ColumnType.LONG256), "Column file does not exist");
                assertSchemaMismatch(src, dst -> dst.col("ts", ColumnType.BINARY), "Column file does not exist");
                assertSchemaMismatch(src, dst -> dst.col("ts", ColumnType.BYTE), "Column file does not exist");
                assertSchemaMismatch(src, dst -> dst.col("ts", ColumnType.CHAR), "Column file does not exist");
                assertSchemaMismatch(src, dst -> dst.col("ts", ColumnType.SHORT), "Column file does not exist");
            }
        });
    }

    @Test
    public void testAttachPartitionStringColIndexMessedNotInOrder() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.DAY)) {
                createPopulateTable(
                        src.col("l", ColumnType.LONG)
                                .col("str", ColumnType.STRING)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        10000,
                        "2020-01-01",
                        10);

                long value = 0L;
                writeToStrIndexFile(src, "str.i", value, 16L);

                assertSchemaMismatch(src, dst -> dst.col("str", ColumnType.STRING), "Variable size column has invalid data address value");
            }
        });
    }

    @Test
    public void testAttachPartitionStringColIndexMessedOffsetOutsideFileBounds() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.DAY)) {
                createPopulateTable(
                        src.col("l", ColumnType.LONG)
                                .col("str", ColumnType.STRING)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        10000,
                        "2020-01-01",
                        10);

                long invalidValue = Long.MAX_VALUE;
                writeToStrIndexFile(src, "str.i", invalidValue, 256L);
                assertSchemaMismatch(src, dst -> dst.col("str", ColumnType.STRING), "dataAddress=" + invalidValue);

                invalidValue = -1;
                writeToStrIndexFile(src, "str.i", invalidValue, 256L);
                assertSchemaMismatch(src, dst -> dst.col("str", ColumnType.STRING), "dataAddress=" + invalidValue);
            }
        });
    }

    @Test
    public void testAttachPartitionStringColNoIndex() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.DAY)) {

                createPopulateTable(
                        src.col("l", ColumnType.LONG)
                                .col("str", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        10000,
                        "2020-01-01",
                        10);

                assertSchemaMismatch(src, dst -> dst.col("str", ColumnType.STRING), "Column file does not exist");
            }
        });
    }

    @Test
    public void testAttachPartitionStringIndexFileTooSmall() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.DAY)) {

                createPopulateTable(
                        src.col("l", ColumnType.LONG)
                                .col("sh", ColumnType.SHORT)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        45000,
                        "2020-01-09",
                        2);

                FilesFacade ff = FilesFacadeImpl.INSTANCE;
                try (Path path = new Path()) {
                    // .i file
                    path.of(configuration.getRoot()).concat(src.getName()).concat("2020-01-09").concat("sh.i").$();
                    ff.touch(path);
                }

                assertSchemaMismatch(src, dst -> dst.col("sh", ColumnType.STRING), "Column file is too small");
            }
        });
    }

    @Test
    public void testAttachPartitionSymbolFileNegativeValue() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.DAY)) {
                createPopulateTable(
                        src.col("l", ColumnType.LONG)
                                .col("sym", ColumnType.SYMBOL)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        10000,
                        "2020-01-09",
                        2);

                writeToStrIndexFile(src, "sym.d", -1L, 4L);

                try (TableModel dst = new TableModel(configuration, "dst", PartitionBy.DAY)) {
                    createPopulateTable(
                            dst.timestamp("ts")
                                    .col("i", ColumnType.INT)
                                    .col("l", ColumnType.LONG).
                                    col("sym", ColumnType.SYMBOL),
                            10000,
                            "2020-01-09",
                            2);

                    compile("alter table dst drop partition list '2020-01-09'");

                    try {
                        copyAttachPartition(src, dst, 0, "2020-01-09");
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
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.DAY)) {

                createPopulateTable(
                        src.col("l", ColumnType.LONG)
                                .col("sh", ColumnType.SHORT)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        45000,
                        "2020-01-09",
                        2);

                assertSchemaMismatch(src, dst -> dst.col("sh", ColumnType.SYMBOL), "Column file is too small");
            }
        });
    }

    @Test
    public void testAttachPartitionWhereTimestampColumnNameIsOtherThanTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst", PartitionBy.DAY)) {

                createPopulateTable(
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        10000,
                        "2020-01-01",
                        10);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                copyAttachPartition(src, dst, 0, "2020-01-01");
            }
        });
    }

    @Test
    public void testAttachPartitionWithColumnTypes() throws Exception {
        assertSchemaMatch(dst -> dst.col("str", ColumnType.STRING));
        assertSchemaMatch(dst -> dst.col("l1", ColumnType.LONG));
        assertSchemaMatch(dst -> dst.col("i1", ColumnType.INT));
        assertSchemaMatch(dst -> dst.col("b", ColumnType.BOOLEAN));
        assertSchemaMatch(dst -> dst.col("db", ColumnType.DOUBLE));
        assertSchemaMatch(dst -> dst.col("fl", ColumnType.FLOAT));
        assertSchemaMatch(dst -> dst.col("dt", ColumnType.DATE));
        assertSchemaMatch(dst -> dst.col("ts1", ColumnType.TIMESTAMP));
        assertSchemaMatch(dst -> dst.col("l256", ColumnType.LONG256));
        assertSchemaMatch(dst -> dst.col("byt", ColumnType.BYTE));
        assertSchemaMatch(dst -> dst.col("ch", ColumnType.CHAR));
        assertSchemaMatch(dst -> dst.col("sh", ColumnType.SHORT));
    }

    @Test
    public void testAttachPartitionWrongFixedColumn() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.DAY)) {

                createPopulateTable(
                        src.col("l", ColumnType.LONG)
                                .col("sh", ColumnType.SHORT)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        45000,
                        "2020-01-09",
                        2);

                assertSchemaMismatch(src, dst -> dst.col("sh", ColumnType.LONG), "Column file is too small");
            }
        });
    }

    @Test
    public void testAttachPartitionsNonPartitioned() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst", PartitionBy.NONE)) {

                createPopulateTable(
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        10000,
                        "2020-01-01",
                        10);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                try {
                    copyAttachPartition(src, dst, 0, "2020-01-09");
                    Assert.fail();
                } catch (SqlException e) {
                    TestUtils.assertEquals("[23] table is not partitioned", e.getMessage());
                }
            }
        });
    }

    @Test
    public void testAttachPartitionsTableInTransaction() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst", PartitionBy.DAY)) {

                int partitionRowCount = 111;
                createPopulateTable(
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        partitionRowCount,
                        "2020-01-09",
                        1);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                long timestamp = TimestampFormatUtils.parseTimestamp("2020-01-09T00:00:00.000z");
                copyPartitionToBackup(src.getName(), "2020-01-09", dst.getName());

                // Add 1 row without commit
                try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "dst", "testing")) {
                    long insertTs = TimestampFormatUtils.parseTimestamp("2020-01-10T23:59:59.999z");
                    TableWriter.Row row = writer.newRow(insertTs);
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
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst", PartitionBy.DAY)) {

                createPopulateTable(
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("s", ColumnType.SYMBOL).indexed(false, 4096)
                                .timestamp("ts"),
                        10000,
                        "2020-01-01",
                        10);

                // Make sure nulls are included in the partition to be attached
                assertSql("select count() from src where ts in '2020-01-09' and s = null", "count\n302\n");

                createPopulateTable(
                        dst.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("s", ColumnType.SYMBOL).indexed(false, 4096)
                                .timestamp("ts"),
                        10000,
                        "2020-01-01",
                        10);

                compile("alter table dst drop partition list '2020-01-09'");

                copyAttachPartition(src, dst, 9000, "2020-01-09");
            }
        });
    }

    @Test
    public void testAttachPartitionsWithSymbolsValueDoesNotMatch() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst", PartitionBy.DAY)) {

                createPopulateTable(
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("s", ColumnType.SYMBOL)
                                .timestamp("ts"),
                        10000,
                        "2020-01-01",
                        10);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG)
                        .col("s", ColumnType.SYMBOL));

                try {
                    copyAttachPartition(src, dst, 0, "2020-01-09");
                    Assert.fail();
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "Symbol file does not match symbol column");
                }
            }
        });
    }

    @Test
    public void testAttachPartitionsWithSymbolsValueMatch() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst", PartitionBy.DAY)) {

                createPopulateTable(
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("s", ColumnType.SYMBOL)
                                .timestamp("ts"),
                        10000,
                        "2020-01-01",
                        10);

                // Make sure nulls are included in the partition to be attached
                assertSql("select count() from src where ts in '2020-01-09' and s = null", "count\n302\n");

                createPopulateTable(
                        dst.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("s", ColumnType.SYMBOL)
                                .timestamp("ts"),
                        10000,
                        "2020-01-01",
                        10);

                compile("alter table dst drop partition list '2020-01-09'");

                copyAttachPartition(src, dst, 9000, "2020-01-09");
            }
        });
    }

    @Test
    public void testAttachPartitionsWithSymbolsValueMatchWithNoIndex() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst", PartitionBy.DAY)) {

                createPopulateTable(
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("s", ColumnType.SYMBOL)
                                .timestamp("ts"),
                        10000,
                        "2020-01-01",
                        10);

                createPopulateTable(
                        dst.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("s", ColumnType.SYMBOL).indexed(true, 4096)
                                .timestamp("ts"),
                        10000,
                        "2020-01-01",
                        10);

                compile("alter table dst drop partition list '2020-01-09'");

                try {
                    copyAttachPartition(src, dst, 9000, "2020-01-09");
                } catch (SqlException ex) {
                    TestUtils.assertContains(ex.getFlyweightMessage(), "Symbol index value file does not exist");
                }
            }
        });
    }

    @Test
    public void testAttachPartitionsWithSymbolsValueMatchWithNoIndexKeyFile() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst", PartitionBy.DAY)) {

                createPopulateTable(
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("s", ColumnType.SYMBOL).indexed(true, 4096)
                                .timestamp("ts"),
                        10000,
                        "2020-01-01",
                        10);

                createPopulateTable(
                        dst.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("s", ColumnType.SYMBOL).indexed(true, 4096)
                                .timestamp("ts"),
                        10000,
                        "2020-01-01",
                        10);

                compile("alter table dst drop partition list '2020-01-09'");
                FilesFacade ff = FilesFacadeImpl.INSTANCE;
                try (Path path = new Path()) {
                    // remove .k
                    path.of(configuration.getRoot()).concat(src.getName()).concat("2020-01-09").concat("s").put(".k").$();
                    ff.remove(path);
                }

                try {
                    copyAttachPartition(src, dst, 9000, "2020-01-09");
                } catch (SqlException ex) {
                    TestUtils.assertContains(ex.getFlyweightMessage(), "Symbol index key file does not exist");
                }
            }
        });
    }

    @Test
    public void testAttachSamePartitionTwice() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst", PartitionBy.DAY)) {

                createPopulateTable(
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        10000,
                        "2020-01-01",
                        10);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                copyAttachPartition(src, dst, 0, "2020-01-09");

                String alterCommand = "ALTER TABLE dst ATTACH PARTITION LIST '2020-01-09'";

                try {
                    compile(alterCommand, sqlExecutionContext);
                    Assert.fail();
                } catch (SqlException e) {
                    Assert.assertEquals("[23] failed to attach partition '2020-01-09', partition already attached to the table", e.getMessage());
                }
            }
        });
    }

    @Test
    public void testCannotMapTimestampColumn() throws Exception {
        AtomicInteger counter = new AtomicInteger(1);
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

        testSqlFailedOnFsOperation(ff, "could not mmap");
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

        testSqlFailedOnFsOperation(ff, "table 'dst' could not be altered: [", "] could not open");
    }

    @Test
    public void testCannotReadTimestampColumnFileDoesNotExist() throws Exception {
        AtomicInteger counter = new AtomicInteger(1);
        FilesFacadeImpl ff = new FilesFacadeImpl() {
            @Override
            public boolean exists(LPSZ name) {
                if (Chars.endsWith(name, "ts.d") && counter.decrementAndGet() == 0) {
                    return false;
                }
                return super.exists(name);
            }
        };

        testSqlFailedOnFsOperation(ff, "table 'dst' could not be altered: [0] path does not exist");
    }

    @Test
    public void testCannotRenameDetachedFolderOnAttach() throws Exception {
        AtomicInteger counter = new AtomicInteger(1);
        FilesFacadeImpl ff = new FilesFacadeImpl() {
            @Override
            public int rename(LPSZ from, LPSZ to) {
                if (Chars.endsWith(to, "2020-01-01") && counter.decrementAndGet() == 0) {
                    return Files.FILES_RENAME_ERR_OTHER;
                }
                return super.rename(from, to);
            }
        };

        testSqlFailedOnFsOperation(ff, "table 'dst' could not be altered: ", " File system error on trying to rename [");
    }

    @Test
    public void testDetachAttachDifferentPartitionTableReaderReload() throws Exception {
        if (FilesFacadeImpl.INSTANCE.isRestrictedFileSystem()) {
            // cannot remove opened files on Windows, test  not relevant
            return;
        }

        assertMemoryLeak(() -> {

            try (TableModel src = new TableModel(configuration, "src", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst", PartitionBy.DAY)) {

                int partitionRowCount = 5;
                createPopulateTable(
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("str", ColumnType.STRING)
                                .timestamp("ts"),
                        partitionRowCount,
                        "2020-01-09",
                        2);

                createPopulateTable(
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
                        copyPartitionToBackup(src.getName(), "2020-01-09", dst.getName());
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

    private void assertSchemaMatch(AddColumn tm) throws Exception {
        setUp();
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst", PartitionBy.DAY)) {
                src.col("l", ColumnType.LONG)
                        .col("i", ColumnType.INT)
                        .timestamp("ts");
                tm.add(src);

                createPopulateTable(
                        src,
                        10000,
                        "2020-01-01",
                        10);

                dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG);
                tm.add(dst);

                CairoTestUtils.create(dst);
                copyAttachPartition(src, dst, 0, "2020-01-01");
            }
        });
        tearDown();
    }

    private void assertSchemaMismatch(TableModel src, AddColumn tm, String errorMessage) throws NumericException {
        try (TableModel dst = new TableModel(configuration, "dst", PartitionBy.DAY);
             Path path = new Path()) {
            dst.timestamp("ts")
                    .col("i", ColumnType.INT)
                    .col("l", ColumnType.LONG);

            tm.add(dst);
            CairoTestUtils.create(dst);

            try {
                copyAttachPartition(src, dst, 0, "2020-01-09");
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), errorMessage);
            }
            Files.rmdir(path.concat(root).concat("dst").concat("2020-01-09").put(TableUtils.DETACHED_DIR_MARKER).$());
        }
    }

    private void copyAttachPartition(
            TableModel src,
            TableModel dst,
            int countAdjustment,
            String... partitionList
    ) throws SqlException, NumericException {
        copyAttachPartition(src, dst, countAdjustment, false, partitionList);
    }

    private void copyAttachPartition(
            TableModel src,
            TableModel dst,
            int countAdjustment,
            boolean skipCopy,
            String... partitionList
    ) throws SqlException, NumericException {
        StringBuilder partitions = new StringBuilder();
        for (int i = 0; i < partitionList.length; i++) {
            if (i > 0) {
                partitions.append(",");
            }
            partitions.append("'");
            partitions.append(partitionList[i]);
            partitions.append("'");
        }

        int rowCount = readAllRows();

        String alterCommand = "ALTER TABLE dst ATTACH PARTITION LIST " + partitions + ";";

        StringBuilder partitionsIn = new StringBuilder();
        for (int i = 0; i < partitionList.length; i++) {
            if (i > 0) {
                partitionsIn.append(" OR ");
            }
            partitionsIn.append("ts IN '");
            partitionsIn.append(partitionList[i]);
            partitionsIn.append("'");
        }

        String withClause = ", t1 as (select 1 as id, count() as cnt from src WHERE " + partitionsIn + ")\n";

        if (!skipCopy) {
            for (String partitionFolder : partitionList) {
                copyPartitionToBackup(src.getName(), partitionFolder, dst.getName());
            }
        }

        // Alter table
        compile(alterCommand, sqlExecutionContext);

        int newRowCount = readAllRows();
        Assert.assertTrue(newRowCount > rowCount);

        TestUtils.assertEquals(
                "cnt\n" +
                        (-countAdjustment) + "\n",
                executeSql("with t2 as (select 1 as id, count() as cnt from dst)\n" +
                        withClause +
                        "select t1.cnt - t2.cnt as cnt\n" +
                        "from t2 cross join t1"
                )
        );

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
        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "dst", "testing")) {
            TableWriter.Row row = writer.newRow(timestamp);
            row.putInt(1, 1);
            row.append();
            writer.commit();
        }

        TestUtils.assertEquals(
                "cnt\n" +
                        (-1 - countAdjustment) + "\n",
                executeSql("with " +
                        "t2 as (select 1 as id, count() as cnt from dst)\n" +
                        withClause +
                        "select t1.cnt - t2.cnt as cnt\n" +
                        "from t2 cross join t1"
                )
        );
    }

    private void copyDirectory(Path from, Path to) {
        LOG.info().$("copying folder [from=").$(from).$(", to=").$(to).$(']').$();
        TestUtils.copyDirectory(from, to, DIR_MODE);
    }

    private void copyPartitionToBackup(String src, String partitionFolder, String dst) {
        copyPartitionToBackup(src, partitionFolder, dst, partitionFolder);
    }

    private void copyPartitionToBackup(String src, String srcDir, String dst, String dstDir) {
        try (Path p1 = new Path().of(configuration.getRoot()).concat(src).concat(srcDir).$();
             Path backup = new Path().of(configuration.getRoot())) {

            copyDirectory(p1, backup.concat(dst).concat(dstDir).put(TableUtils.DETACHED_DIR_MARKER).$());
        }
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

    private int readAllRows() {
        try (FullFwdDataFrameCursor cursor = new FullFwdDataFrameCursor()) {
            cursor.of(engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "dst"));
            DataFrame frame;
            int count = 0;
            while ((frame = cursor.next()) != null) {
                for (long index = frame.getRowHi() - 1, lo = frame.getRowLo() - 1; index > lo; index--) {
                    count++;
                }
            }
            return count;
        }
    }

    private void testSqlFailedOnFsOperation(FilesFacadeImpl ff, String... errorContains) throws Exception {
        assertMemoryLeak(ff, () -> {
            try (
                    TableModel src = new TableModel(configuration, "src", PartitionBy.DAY);
                    TableModel dst = new TableModel(configuration, "dst", PartitionBy.DAY)
            ) {

                createPopulateTable(
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        100,
                        "2020-01-01",
                        1);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                try {
                    copyAttachPartition(src, dst, 0, "2020-01-01");
                    Assert.fail();
                } catch (SqlException e) {
                    for (String error : errorContains) {
                        TestUtils.assertContains(e.getFlyweightMessage(), error);
                    }
                }

                // second attempt without FilesFacade override should work ok
                copyAttachPartition(src, dst, 0, true, "2020-01-01");
            }
        });
    }

    private void writeToStrIndexFile(TableModel src, String columnFileName, long value, long offset) {
        FilesFacade ff = FilesFacadeImpl.INSTANCE;
        long fd = -1;
        long writeBuff = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try (Path path = new Path()) {
            // .i file
            path.of(configuration.getRoot()).concat(src.getName()).concat("2020-01-09").concat(columnFileName).$();
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
