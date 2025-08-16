/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.AttachDetachStatus;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.FullFwdPartitionFrameCursor;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.PartitionFrame;
import io.questdb.griffin.SqlException;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.MemoryTag;
import io.questdb.std.NumericException;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.cairo.TestTableReaderRecordCursor;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.AttachDetachStatus.ATTACH_ERR_RENAME;

@RunWith(Parameterized.class)
public class AlterTableAttachPartitionTest extends AbstractAlterTableAttachPartitionTest {
    private final TestTimestampType timestampType;

    public AlterTableAttachPartitionTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO}, {TestTimestampType.NANO}
        });
    }

    @Test
    public void testAttach2Partitions() throws Exception {
        assertMemoryLeak(() -> {
                    TableModel src = new TableModel(configuration, "src1", PartitionBy.DAY);
                    TableModel dst = new TableModel(configuration, "dst1", PartitionBy.DAY);
                    createPopulateTable(
                            1,
                            src.timestamp("ts", timestampType.getTimestampType())
                                    .col("i", ColumnType.INT)
                                    .col("l", ColumnType.LONG),
                            10000,
                            "2020-01-01",
                            12);

                    AbstractCairoTest.create(dst.timestamp("ts", timestampType.getTimestampType())
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG));

                    attachFromSrcIntoDst(src, dst, "2020-01-09", "2020-01-10");
                }
        );
    }

    @Test
    public void testAttachActive2PartitionsOneByOneInDescOrder() throws Exception {
        assertMemoryLeak(() -> {
                    TableModel src = new TableModel(configuration, "src2", PartitionBy.DAY);
                    TableModel dst = new TableModel(configuration, "dst2", PartitionBy.DAY);

                    createPopulateTable(
                            1,
                            src.timestamp("ts", timestampType.getTimestampType())
                                    .col("i", ColumnType.INT)
                                    .col("l", ColumnType.LONG),
                            10000,
                            "2020-01-01",
                            12);

                    AbstractCairoTest.create(dst.timestamp("ts", timestampType.getTimestampType())
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG));

                    attachFromSrcIntoDst(src, dst, "2020-01-10");
                    attachFromSrcIntoDst(src, dst, "2020-01-09");
                }
        );
    }

    @Test
    public void testAttachActive3Partitions() throws Exception {
        assertMemoryLeak(() -> {
                    TableModel src = new TableModel(configuration, "src3", PartitionBy.DAY);
                    TableModel dst = new TableModel(configuration, "dst3", PartitionBy.DAY);

                    createPopulateTable(
                            1,
                            src.timestamp("ts", timestampType.getTimestampType())
                                    .col("i", ColumnType.INT)
                                    .col("l", ColumnType.LONG),
                            10000,
                            "2020-01-01",
                            12);

                    AbstractCairoTest.create(dst.timestamp("ts", timestampType.getTimestampType())
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG));

                    // 3 partitions unordered
                    attachFromSrcIntoDst(src, dst, "2020-01-09", "2020-01-10", "2020-01-01");
                }
        );
    }

    @Test
    public void testAttachActiveWrittenPartition() throws Exception {
        assertMemoryLeak(() -> {
                    TableModel src = new TableModel(configuration, "src4", PartitionBy.DAY);
                    TableModel dst = new TableModel(configuration, "dst4", PartitionBy.DAY);

                    createPopulateTable(
                            1,
                            src.timestamp("ts", timestampType.getTimestampType())
                                    .col("i", ColumnType.INT)
                                    .col("l", ColumnType.LONG)
                                    .col("vch", ColumnType.VARCHAR),
                            10000,
                            "2020-01-01",
                            11);

                    AbstractCairoTest.create(dst.timestamp("ts", timestampType.getTimestampType())
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG));

                    attachFromSrcIntoDst(src, dst, "2020-01-10");
                }
        );
    }

    @Test
    public void testAttachFailsInvalidFormat() throws Exception {
        assertMemoryLeak(() -> {
                    TableModel dst = new TableModel(configuration, "dst5", PartitionBy.MONTH);

                    AbstractCairoTest.create(dst.timestamp("ts", timestampType.getTimestampType())
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG));

                    String alterCommand = "ALTER TABLE " + dst.getName() + " ATTACH PARTITION LIST '202A-01'";
                    try {
                        execute(alterCommand, sqlExecutionContext);
                        Assert.fail();
                    } catch (SqlException e) {
                        Assert.assertEquals("[39] 'yyyy-MM' expected, found [ts=202A-01]", e.getMessage());
                    }
                }
        );
    }

    @Test
    public void testAttachFailsInvalidFormatPartitionsAnnually0() throws Exception {
        assertMemoryLeak(() -> {
                    TableModel dst = new TableModel(configuration, "dst6a", PartitionBy.YEAR);

                    AbstractCairoTest.create(dst.timestamp("ts", timestampType.getTimestampType())
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG));

                    String alterCommand = "ALTER TABLE " + dst.getName() + " ATTACH PARTITION LIST 'nono'";
                    try {
                        execute(alterCommand, sqlExecutionContext);
                        Assert.fail();
                    } catch (SqlException e) {
                        Assert.assertEquals("[40] 'yyyy' expected, found [ts=nono]", e.getMessage());
                    }
                }
        );
    }

    @Test
    public void testAttachFailsInvalidFormatPartitionsAnnually1() throws Exception {
        assertMemoryLeak(() -> {
                    TableModel dst = new TableModel(configuration, "dst6b", PartitionBy.YEAR);

                    AbstractCairoTest.create(dst.timestamp("ts", timestampType.getTimestampType())
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG));

                    String alterCommand = "ALTER TABLE " + dst.getName() + " ATTACH PARTITION LIST '202'";
                    try {
                        execute(alterCommand, sqlExecutionContext);
                        Assert.fail();
                    } catch (SqlException e) {
                        Assert.assertEquals("[40] 'yyyy' expected, found [ts=202]", e.getMessage());
                    }
                }
        );
    }

    @Test
    public void testAttachFailsInvalidFormatPartitionsMonthly0() throws Exception {
        assertMemoryLeak(() -> {
                    TableModel dst = new TableModel(configuration, "dst7a", PartitionBy.MONTH);

                    AbstractCairoTest.create(dst.timestamp("ts", timestampType.getTimestampType())
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG));

                    String alterCommand = "ALTER TABLE " + dst.getName() + " ATTACH PARTITION LIST '2020-no'";
                    try {
                        execute(alterCommand, sqlExecutionContext);
                        Assert.fail();
                    } catch (SqlException e) {
                        Assert.assertEquals("[40] 'yyyy-MM' expected, found [ts=2020-no]", e.getMessage());
                    }
                }
        );
    }

    @Test
    public void testAttachFailsInvalidFormatPartitionsMonthly1() throws Exception {
        assertMemoryLeak(() -> {
                    TableModel dst = new TableModel(configuration, "dst7b", PartitionBy.MONTH);

                    AbstractCairoTest.create(dst.timestamp("ts", timestampType.getTimestampType())
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG));

                    String alterCommand = "ALTER TABLE " + dst.getName() + " ATTACH PARTITION LIST '2020'";
                    try {
                        execute(alterCommand, sqlExecutionContext);
                        Assert.fail();
                    } catch (SqlException e) {
                        Assert.assertEquals("[40] 'yyyy-MM' expected, found [ts=2020]", e.getMessage());
                    }
                }
        );
    }

    @Test
    public void testAttachFailsInvalidSeparatorFormat() throws Exception {
        assertMemoryLeak(() -> {
                    TableModel dst = new TableModel(configuration, "dst8", PartitionBy.MONTH);

                    AbstractCairoTest.create(dst.timestamp("ts", timestampType.getTimestampType())
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG));

                    String alterCommand = "ALTER TABLE " + dst.getName() + " ATTACH PARTITION LIST '2020-01'.'2020-02'";
                    try {
                        execute(alterCommand, sqlExecutionContext);
                        Assert.fail();
                    } catch (SqlException e) {
                        Assert.assertEquals("[48] ',' expected", e.getMessage());
                    }
                }
        );
    }

    @Test
    public void testAttachMissingPartition() throws Exception {
        assertMemoryLeak(() -> {
                    TableModel dst = new TableModel(configuration, "dst9", PartitionBy.DAY);
                    AbstractCairoTest.create(dst.timestamp("ts", timestampType.getTimestampType())
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG)
                            .col("vch", ColumnType.VARCHAR));

                    String alterCommand = "ALTER TABLE " + dst.getName() + " ATTACH PARTITION LIST '2020-01-01'";
                    try {
                        execute(alterCommand, sqlExecutionContext);
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "could not attach partition");
                    }
                }
        );
    }

    @Test
    public void testAttachNonExisting() throws Exception {
        // todo: what's the different between this and testAttachMissingPartition() ?
        assertMemoryLeak(() -> {
                    TableModel dst = new TableModel(configuration, "dst10", PartitionBy.DAY);
                    AbstractCairoTest.create(dst.timestamp("ts", timestampType.getTimestampType())
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG)
                            .col("vch", ColumnType.VARCHAR));

                    String alterCommand = "ALTER TABLE " + dst.getName() + " ATTACH PARTITION LIST '2020-01-01'";

                    try {
                        execute(alterCommand, sqlExecutionContext);
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "could not attach partition");
                    }
                }
        );
    }

    @Test
    public void testAttachPartitionInWrongDirectoryName() throws Exception {
        assertMemoryLeak(() -> {
                    TableModel src = new TableModel(configuration, "src11", PartitionBy.DAY);
                    TableModel dst = new TableModel(configuration, "dst11", PartitionBy.DAY);

                    TableToken srcTableToken = createPopulateTable(
                            1,
                            src.col("l", ColumnType.LONG)
                                    .col("i", ColumnType.INT)
                                    .col("vch", ColumnType.VARCHAR)
                                    .timestamp("ts", timestampType.getTimestampType()),
                            10000,
                            "2020-01-01",
                            1);

                    AbstractCairoTest.create(
                            dst.col("i", ColumnType.INT)
                                    .col("l", ColumnType.LONG)
                                    .timestamp("ts", timestampType.getTimestampType()));

                    copyPartitionToAttachable(srcTableToken, "2020-01-01", dst.getName(), "COCONUTS");

                    try {
                        execute("ALTER TABLE " + dst.getName() + " ATTACH PARTITION LIST '2020-01-02'", sqlExecutionContext);
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "could not attach partition");
                    }
                }
        );
    }

    @Test
    public void testAttachPartitionStringColIndexMessedNotInOrder() throws Exception {
        assertMemoryLeak(() -> {
            AddColumn src = s -> s.col("l", ColumnType.LONG)
                    .col("i", ColumnType.INT)
                    .timestamp("ts", timestampType.getTimestampType())
                    .col("str", ColumnType.STRING);

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
                    .timestamp("ts", timestampType.getTimestampType())
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
    public void testAttachPartitionStringIndexFileTooSmall() throws Exception {
        assertMemoryLeak(() -> {
            AddColumn src = s -> s.col("i", ColumnType.INT)
                    .col("l", ColumnType.LONG)
                    .timestamp("ts", timestampType.getTimestampType())
                    .col("sh", ColumnType.STRING);

            assertSchemaMismatch(
                    "src30",
                    src,
                    "dst30",
                    dst -> {
                    },
                    s -> {
                        engine.clear();
                        TableToken tableToken = engine.verifyTableName(s.getName());
                        path.of(configuration.getDbRoot()).concat(tableToken).concat("2022-08-01").concat("sh.i").$();
                        long fd = TestFilesFacadeImpl.INSTANCE.openRW(path.$(), CairoConfiguration.O_NONE);
                        Files.truncate(fd, Files.length(fd) / 4);
                        TestFilesFacadeImpl.INSTANCE.close(fd);
                    },
                    "Column file is too small"
            );
        });
    }

    @Test
    public void testAttachPartitionSymbolFileNegativeValue() throws Exception {
        assertMemoryLeak(() -> {
                    TableModel src = new TableModel(configuration, "src31", PartitionBy.DAY);
                    createPopulateTable(
                            1,
                            src.timestamp("ts", timestampType.getTimestampType())
                                    .col("l", ColumnType.LONG)
                                    .col("sym", ColumnType.SYMBOL)
                                    .col("i", ColumnType.INT),
                            10000,
                            "2022-08-01",
                            3);

                    writeToStrIndexFile(src, "2022-08-02", "sym.d", -1L, 4L);

                    TableModel dst = new TableModel(configuration, "dst31", PartitionBy.DAY);
                    createPopulateTable(
                            1,
                            dst.timestamp("ts", timestampType.getTimestampType())
                                    .col("l", ColumnType.LONG)
                                    .col("sym", ColumnType.SYMBOL)
                                    .col("i", ColumnType.INT),
                            10000,
                            "2022-08-01",
                            1);

                    try {
                        attachFromSrcIntoDst(src, dst, "2022-08-02");
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "Symbol file does not match symbol column, invalid key");
                    }
                }
        );
    }

    @Test
    public void testAttachPartitionSymbolFileTooSmall() throws Exception {
        assertMemoryLeak(() -> {

            AddColumn src = s -> s.col("i", ColumnType.INT)
                    .col("l", ColumnType.LONG)
                    .timestamp("ts", timestampType.getTimestampType())
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
                        TableToken tableToken = engine.verifyTableName(s.getName());
                        path.of(configuration.getDbRoot()).concat(tableToken).concat("2022-08-01").concat("sh.v").$();
                        long fd = TestFilesFacadeImpl.INSTANCE.openRW(path.$(), CairoConfiguration.O_NONE);
                        Files.truncate(fd, Files.length(fd) / 2);
                        TestFilesFacadeImpl.INSTANCE.close(fd);
                    },
                    "Symbol file does not match symbol column"
            );
        });
    }

    @Test
    public void testAttachPartitionWhereTimestampColumnNameIsOtherThanTimestamp() throws Exception {
        assertMemoryLeak(() -> {
                    TableModel src = new TableModel(configuration, "src33", PartitionBy.DAY);
                    TableModel dst = new TableModel(configuration, "dst33", PartitionBy.DAY);

                    createPopulateTable(
                            1,
                            src.timestamp("ts", timestampType.getTimestampType())
                                    .col("i", ColumnType.INT)
                                    .col("l", ColumnType.LONG),
                            10000,
                            "2022-08-01",
                            10);

                    AbstractCairoTest.create(dst.timestamp("ts1")
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG));

                    try {
                        attachFromSrcIntoDst(src, dst, "2022-08-01");
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(),
                                "could not open, file does not exist"
                        );
                        TestUtils.assertContains(e.getFlyweightMessage(),
                                "ts1.d"
                        );
                    }
                }
        );
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
        assertSchemaMatch(dst -> dst.col("vch", ColumnType.VARCHAR), idx);
    }

    @Test
    public void testAttachPartitionWrongUuidColumn() throws Exception {
        testAttachPartitionWrongFixedColumn(ColumnType.UUID);
        testAttachPartitionWrongFixedColumn(ColumnType.LONG128);
        testAttachPartitionWrongFixedColumn(ColumnType.SHORT);
    }

    @Test
    public void testAttachPartitionsDeletedColumnFromSrc() throws Exception {
        assertMemoryLeak(() -> {

                    TableModel src = new TableModel(configuration, "testAttachPartitionsDeletedColumnFromSrc" + "_src", PartitionBy.DAY);
                    TableModel dst = new TableModel(configuration, "testAttachPartitionsDeletedColumnFromSrc" + "_dst", PartitionBy.DAY);

                    TableToken srcTableToken = createPopulateTable(
                            1,
                            src.timestamp("ts", timestampType.getTimestampType())
                                    .col("i", ColumnType.INT)
                                    .col("l", ColumnType.LONG)
                                    .col("s", ColumnType.SYMBOL).indexed(true, 128)
                                    .col("str", ColumnType.STRING)
                                    .col("vch", ColumnType.VARCHAR),
                            8,
                            "2022-08-01",
                            4
                    );
                    try (TableWriter writer = getWriter(src.getName())) {
                        writer.removeColumn("s");
                        writer.removeColumn("str");
                        writer.removeColumn("i");
                    }

                    TableToken dstTableToken = AbstractCairoTest.create(dst.timestamp("ts", timestampType.getTimestampType())
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG)
                            .col("s", ColumnType.SYMBOL).indexed(true, 128)
                            .col("str", ColumnType.STRING)
                            .col("vch", ColumnType.VARCHAR)
                    );

                    copyPartitionToAttachable(srcTableToken, "2022-08-02", dstTableToken.getDirName(), "2022-08-02");
                    execute("ALTER TABLE " + dst.getName() + " ATTACH PARTITION LIST '2022-08-02'", sqlExecutionContext);

                    engine.clear();
                    assertQuery(
                            replaceTimestampSuffix(replaceTimestampSuffix("ts\ti\tl\ts\tstr\tvch\n" +
                                    "2022-08-02T11:59:59.625000Z\tnull\t3\t\t\t\uF2C1ӍKB\n" +
                                    "2022-08-02T23:59:59.500000Z\tnull\t4\t\t\tK䰭\n"), timestampType.getTypeName()),
                            dst.getName(),
                            "ts",
                            true,
                            true
                    );
                }
        );
    }

    @Test
    public void testAttachPartitionsDetachedHasExtraColumn() throws Exception {
        assertMemoryLeak(() -> {
                    TableModel src = new TableModel(configuration, "src48", PartitionBy.DAY);
                    TableModel dst = new TableModel(configuration, "dst48", PartitionBy.DAY);

                    int partitionRowCount = 11;
                    TableToken srcTableToken = createPopulateTable(
                            1,
                            src.timestamp("ts", timestampType.getTimestampType())
                                    .col("i", ColumnType.INT)
                                    .col("l", ColumnType.LONG)
                                    .col("s", ColumnType.SYMBOL).indexed(true, 128)
                                    .col("vch", ColumnType.VARCHAR),
                            partitionRowCount,
                            "2022-08-01",
                            4);

                    TableToken dstTableToken = AbstractCairoTest.create(dst.timestamp("ts", timestampType.getTimestampType())
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG));

                    copyPartitionToAttachable(srcTableToken, "2022-08-01", dstTableToken.getDirName(), "2022-08-01");

                    TimestampDriver driver = timestampType.getDriver();
                    long timestamp = driver.parseFloorLiteral("2022-08-01T00:00:00.000Z");
                    long txn;
                    try (TableWriter writer = getWriter(dst.getName())) {
                        txn = writer.getTxn();
                        writer.attachPartition(timestamp);
                    }
                    path.of(configuration.getDbRoot()).concat(dstTableToken);
                    TableUtils.setPathForNativePartition(path, driver.getTimestampType(), PartitionBy.DAY, driver.parseFloorLiteral("2022-08-01"), txn);
                    int pathLen = path.size();

                    // Extra columns not deleted
                    Assert.assertTrue(Files.exists(path.concat("s.d").$()));
                    Assert.assertTrue(Files.exists(path.trimTo(pathLen).concat("s.k").$()));
                    Assert.assertTrue(Files.exists(path.trimTo(pathLen).concat("s.v").$()));
                    Assert.assertTrue(Files.exists(path.trimTo(pathLen).concat("l.d").$()));
                    Assert.assertTrue(Files.exists(path.trimTo(pathLen).concat("vch.d").$()));
                    Assert.assertTrue(Files.exists(path.trimTo(pathLen).concat("vch.i").$()));

                    engine.clear();
                    assertQuery(
                            ColumnType.isTimestampMicro(timestampType.getTimestampType()) ?
                                    "ts\ti\tl\n" +
                                            "2022-08-01T08:43:38.090909Z\t1\t1\n" +
                                            "2022-08-01T17:27:16.181818Z\t2\t2\n" :
                                    "ts\ti\tl\n" +
                                            "2022-08-01T08:43:38.090909090Z\t1\t1\n" +
                                            "2022-08-01T17:27:16.181818180Z\t2\t2\n",
                            dst.getName(),
                            "ts",
                            true,
                            true
                    );
                }
        );
    }

    @Test
    public void testAttachPartitionsNonPartitioned() throws Exception {
        assertMemoryLeak(() -> {
                    TableModel src = new TableModel(configuration, "src35", PartitionBy.DAY);
                    TableModel dst = new TableModel(configuration, "dst35", PartitionBy.NONE);

                    createPopulateTable(
                            1,
                            src.timestamp("ts", timestampType.getTimestampType())
                                    .col("i", ColumnType.INT)
                                    .col("l", ColumnType.LONG),
                            10000,
                            "2022-08-01",
                            10);

                    AbstractCairoTest.create(dst.timestamp("ts", timestampType.getTimestampType())
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG));

                    try {
                        attachFromSrcIntoDst(src, dst, "2022-08-01");
                        Assert.fail();
                    } catch (SqlException e) {
                        TestUtils.assertEquals("[25] table is not partitioned", e.getMessage());
                    }
                }
        );
    }

    @Test
    public void testAttachPartitionsTableInTransaction() throws Exception {
        assertMemoryLeak(() -> {
                    TableModel src = new TableModel(configuration, "src36", PartitionBy.DAY);
                    TableModel dst = new TableModel(configuration, "dst36", PartitionBy.DAY);

                    int partitionRowCount = 111;
                    TableToken srcTableToken = createPopulateTable(
                            1,
                            src.timestamp("ts", timestampType.getTimestampType())
                                    .col("i", ColumnType.INT)
                                    .col("l", ColumnType.LONG),
                            partitionRowCount,
                            "2022-08-01",
                            1);

                    TableToken dstTableToken = AbstractCairoTest.create(dst.timestamp("ts", timestampType.getTimestampType())
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG));

                    copyPartitionToAttachable(srcTableToken, "2022-08-01", dstTableToken.getDirName(), "2022-08-01");

                    TimestampDriver driver = timestampType.getDriver();
                    // Add 1 row without commit
                    long timestamp = driver.parseFloorLiteral("2022-08-01T00:00:00.000Z");
                    try (TableWriter writer = getWriter(dst.getName())) {
                        long insertTs = driver.parseFloorLiteral("2022-08-01T23:59:59.999Z");
                        TableWriter.Row row = writer.newRow(insertTs + driver.fromMicros(1000L));
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
        );
    }

    @Test
    public void testAttachPartitionsWithExtraCharsInPartitionNameByDay() throws Exception {
        assertMemoryLeak(() -> {
                    TableModel src = new TableModel(configuration, "src3a", PartitionBy.DAY);
                    TableModel dst = new TableModel(configuration, "dst3a", PartitionBy.DAY);
                    createPopulateTable(
                            1,
                            src.timestamp("ts", timestampType.getTimestampType())
                                    .col("i", ColumnType.INT)
                                    .col("l", ColumnType.LONG),
                            10000,
                            "2020-01-01",
                            12);
                    AbstractCairoTest.create(dst.timestamp("ts", timestampType.getTimestampType())
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG));
                    attachFromSrcIntoDst(src, dst, "2020-01-09.10", "2020-01-10T19", "2020-01-01T20:22:24.262829Z");
                }
        );
    }

    @Test
    public void testAttachPartitionsWithIndexedSymbolsValueMatch() throws Exception {
        assertMemoryLeak(() -> {
                    TableModel src = new TableModel(configuration, "src37", PartitionBy.DAY);
                    TableModel dst = new TableModel(configuration, "dst37", PartitionBy.DAY);

                    createPopulateTable(
                            1,
                            src.timestamp("ts", timestampType.getTimestampType())
                                    .col("i", ColumnType.INT)
                                    .col("l", ColumnType.LONG)
                                    .col("s", ColumnType.SYMBOL).indexed(false, 4096),
                            10000,
                            "2022-08-01",
                            10);

                    // Make sure nulls are included in the partition to be attached
                    assertSql("count\n302\n", "select count() from " + src.getName() + " where ts in '2022-08-09' and s = null");

                    createPopulateTable(
                            1,
                            dst.timestamp("ts", timestampType.getTimestampType())
                                    .col("i", ColumnType.INT)
                                    .col("l", ColumnType.LONG)
                                    .col("s", ColumnType.SYMBOL).indexed(false, 4096),
                            10000,
                            "2022-08-01",
                            10);

                    execute("alter table " + dst.getName() + " drop partition list '2022-08-09'");

                    attachFromSrcIntoDst(src, dst, "2022-08-09");
                }
        );
    }

    @Test
    public void testAttachPartitionsWithSymbolsValueDoesNotMatch() throws Exception {
        assertMemoryLeak(() -> {
                    TableModel src = new TableModel(configuration, "src38", PartitionBy.DAY);
                    TableModel dst = new TableModel(configuration, "dst38", PartitionBy.DAY);

                    createPopulateTable(
                            1,
                            src.timestamp("ts", timestampType.getTimestampType())
                                    .col("i", ColumnType.INT)
                                    .col("s2", ColumnType.SYMBOL)
                                    .col("l", ColumnType.LONG),
                            20,
                            "2022-08-01",
                            3);

                    AbstractCairoTest.create(dst.timestamp("ts", timestampType.getTimestampType())
                            .col("i", ColumnType.INT)
                            .col("s", ColumnType.SYMBOL)
                            .col("l", ColumnType.LONG));

                    attachFromSrcIntoDst(src, dst, "2022-08-01", "2022-08-02");

                    // s2 column files from the attached partitions should be ignored
                    // and coltops for s column should be created instead.
                    assertSql("count\n0\n", "select count() from " + dst.getName() + " where s is not null");
                }
        );
    }

    @Test
    public void testAttachPartitionsWithSymbolsValueMatch() throws Exception {
        assertMemoryLeak(() -> {
                    TableModel src = new TableModel(configuration, "src39", PartitionBy.DAY);
                    TableModel dst = new TableModel(configuration, "dst39", PartitionBy.DAY);

                    createPopulateTable(
                            1,
                            src.col("l", ColumnType.LONG)
                                    .col("i", ColumnType.INT)
                                    .col("s", ColumnType.SYMBOL)
                                    .timestamp("ts", timestampType.getTimestampType()),
                            10000,
                            "2022-08-01",
                            10);

                    // Make sure nulls are included in the partition to be attached
                    assertSql("count\n302\n", "select count() from " + src.getName() + " where ts in '2022-08-09' and s = null");

                    createPopulateTable(
                            1,
                            dst.col("l", ColumnType.LONG)
                                    .col("i", ColumnType.INT)
                                    .col("s", ColumnType.SYMBOL)
                                    .timestamp("ts", timestampType.getTimestampType()),
                            10000,
                            "2022-08-01",
                            10);

                    execute("alter table " + dst.getName() + " drop partition list '2022-08-09'");

                    attachFromSrcIntoDst(src, dst, "2022-08-09");
                }
        );
    }

    @Test
    public void testAttachPartitionsWithSymbolsValueMatchWithNoIndex() throws Exception {
        assertMemoryLeak(() -> {
                    TableModel src = new TableModel(configuration, "src40", PartitionBy.DAY);
                    TableModel dst = new TableModel(configuration, "dst40", PartitionBy.DAY);

                    createPopulateTable(
                            src.col("l", ColumnType.LONG)
                                    .col("i", ColumnType.INT)
                                    .col("s", ColumnType.SYMBOL)
                                    .timestamp("ts", timestampType.getTimestampType()),
                            10000,
                            "2022-08-01",
                            10);

                    createPopulateTable(
                            1,
                            dst.col("l", ColumnType.LONG)
                                    .col("i", ColumnType.INT)
                                    .col("s", ColumnType.SYMBOL).indexed(true, 4096)
                                    .timestamp("ts", timestampType.getTimestampType()),
                            10000,
                            "2022-08-01",
                            10);

                    execute("alter table " + dst.getName() + " drop partition list '2022-08-09'");

                    try {
                        attachFromSrcIntoDst(src, dst, "2022-08-09");
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(),
                                "Symbol index value file does not exist"
                        );
                    }
                }
        );
    }

    @Test
    public void testAttachPartitionsWithSymbolsValueMatchWithNoIndexKeyFile() throws Exception {
        assertMemoryLeak(() -> {
                    TableModel src = new TableModel(configuration, "src41", PartitionBy.DAY);
                    TableModel dst = new TableModel(configuration, "dst41", PartitionBy.DAY);

                    createPopulateTable(
                            1,
                            src.col("l", ColumnType.LONG)
                                    .col("i", ColumnType.INT)
                                    .col("s", ColumnType.SYMBOL).indexed(true, 4096)
                                    .timestamp("ts", timestampType.getTimestampType()),
                            10000,
                            "2022-08-01",
                            10);

                    createPopulateTable(
                            1,
                            dst.col("l", ColumnType.LONG)
                                    .col("i", ColumnType.INT)
                                    .col("s", ColumnType.SYMBOL).indexed(true, 4096)
                                    .timestamp("ts", timestampType.getTimestampType()),
                            10000,
                            "2022-08-01",
                            10);

                    execute("alter table " + dst.getName() + " drop partition list '2022-08-09'");

                    // remove .k
                    engine.clear();
                    TableToken tableToken = engine.verifyTableName(src.getName());
                    path.of(configuration.getDbRoot()).concat(tableToken).concat("2022-08-09").concat("s.k").$();
                    Assert.assertTrue(Files.remove(path.$()));
                    try {
                        attachFromSrcIntoDst(src, dst, "2022-08-09");
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "Symbol index key file does not exist");
                    }
                }
        );
    }

    @Test
    public void testAttachSamePartitionTwice() throws Exception {
        assertMemoryLeak(() -> {
                    TableModel src = new TableModel(configuration, "src42", PartitionBy.DAY);
                    TableModel dst = new TableModel(configuration, "dst42", PartitionBy.DAY);

                    createPopulateTable(
                            1,
                            src.timestamp("ts", timestampType.getTimestampType())
                                    .col("i", ColumnType.INT)
                                    .col("l", ColumnType.LONG),
                            10000,
                            "2022-08-01",
                            10);

                    AbstractCairoTest.create(dst.timestamp("ts", timestampType.getTimestampType())
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG));

                    attachFromSrcIntoDst(src, dst, "2022-08-09");

                    String alterCommand = "ALTER TABLE " + dst.getName() + " ATTACH PARTITION LIST '2022-08-09'";

                    try {
                        execute(alterCommand, sqlExecutionContext);
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "could not attach partition");
                    }
                }
        );
    }

    @Test
    public void testCannotMapTimestampColumn() throws Exception {
        AtomicInteger counter = new AtomicInteger(1);
        FilesFacadeImpl ff = new TestFilesFacadeImpl() {

            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                if (this.fd != fd) {
                    return super.mmap(fd, len, offset, flags, memoryTag);
                }
                this.fd = -1;
                return -1;
            }

            @Override
            public long openRO(LPSZ name) {
                long fd = super.openRO(name);
                if (Utf8s.endsWithAscii(name, "ts.d") && counter.decrementAndGet() == 0) {
                    this.fd = fd;
                }
                return fd;
            }
        };

        testSqlFailedOnFsOperation(ff, "srcMap", "dstMap", false, "could not mmap");
    }

    @Test
    public void testCannotReadTimestampColumn() throws Exception {
        AtomicInteger counter = new AtomicInteger(1);
        FilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (Utf8s.endsWithAscii(name, "ts.d") && counter.decrementAndGet() == 0) {
                    return -1;
                }
                return super.openRO(name);
            }
        };

        testSqlFailedOnFsOperation(ff, "srcTs", "dstTs", false, "could not open, file does not exist");
    }

    @Test
    public void testCannotReadTimestampColumnFileDoesNotExist() throws Exception {
        AtomicInteger counter = new AtomicInteger(1);
        FilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (Utf8s.endsWithAscii(name, "ts.d") && counter.decrementAndGet() == 0) {
                    return -1;
                }
                return super.openRO(name);
            }
        };

        testSqlFailedOnFsOperation(ff, "srcTs2", "dstTs2", false, "could not open, file does not exist", "ts.d");
    }

    @Test
    public void testCannotRenameDetachedFolderOnAttach() throws Exception {
        AtomicInteger counter = new AtomicInteger(1);
        FilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public int rename(LPSZ from, LPSZ to) {
                if (Utf8s.containsAscii(to, "2020-01-01") && counter.decrementAndGet() == 0) {
                    return Files.FILES_RENAME_ERR_OTHER;
                }
                return super.rename(from, to);
            }
        };

        testSqlFailedOnFsOperation(ff, "srcRen", "dstRen", false, ATTACH_ERR_RENAME.name());
    }

    @Test
    public void testCannotSwitchPartition() throws Exception {
        AtomicInteger counter = new AtomicInteger(1);
        FilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.containsAscii(name, "dst" + "testCannotSwitchPartition") && Utf8s.containsAscii(name, "2020-01-01") && counter.decrementAndGet() == 0) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };

        testSqlFailedOnFsOperation(ff, "src" + "testCannotSwitchPartition", "dst" + "testCannotSwitchPartition", true, " is distressed");
    }

    @Test
    public void testDetachAttachDifferentPartitionTableReaderReload() throws Exception {
        if (TestFilesFacadeImpl.INSTANCE.isRestrictedFileSystem()) {
            // cannot remove opened files on Windows, test  not relevant
            return;
        }

        assertMemoryLeak(() -> {
            TableModel src = new TableModel(configuration, "src47", PartitionBy.DAY);
            TableModel dst = new TableModel(configuration, "dst47", PartitionBy.DAY);

            int partitionRowCount = 5;
            // the srcTable has 5 rows in total:
            // 2 rows in partition 2020-01-09
            // 3 rows in partition 2020-01-10
            TableToken srcTableToken = createPopulateTable(
                    1,
                    src.col("l", ColumnType.LONG)
                            .col("i", ColumnType.INT)
                            .col("str", ColumnType.STRING)
                            .col("vch", ColumnType.VARCHAR)
                            .timestamp("ts", timestampType.getTimestampType()),
                    partitionRowCount,
                    "2020-01-09",
                    2
            );

            // the dstTable has 3 rows in total:
            // 2 rows in partition 2020-01-09
            // 1 row in partition 2020-01-10
            TableToken dstTableToken = createPopulateTable(
                    1,
                    dst.col("l", ColumnType.LONG)
                            .col("i", ColumnType.INT)
                            .col("str", ColumnType.STRING)
                            .col("vch", ColumnType.VARCHAR)
                            .timestamp("ts", timestampType.getTimestampType()),
                    partitionRowCount - 3,
                    "2020-01-09",
                    2
            );

            try (
                    TableReader dstReader = newOffPoolReader(configuration, dst.getTableName());
                    TestTableReaderRecordCursor dstCursor = new TestTableReaderRecordCursor().of(dstReader)
            ) {
                dstReader.openPartition(0);
                dstReader.openPartition(1);
                dstReader.goPassive();

                TimestampDriver driver = timestampType.getDriver();
                try (TableWriter writer = getWriter(dst.getTableName())) {
                    // remove 2020-01-09 partition from dst table
                    long timestamp = driver.parseFloorLiteral("2020-01-09T00:00:00.000Z");
                    writer.removePartition(timestamp);
                    // at this point dst table has only 1 partition: 2020-01-10  and it has 1 row

                    // copy and attach the 2020-01-09 partition from src table to dst table
                    copyPartitionToAttachable(srcTableToken, "2020-01-09", dstTableToken.getDirName(), "2020-01-09");
                    Assert.assertEquals(AttachDetachStatus.OK, writer.attachPartition(timestamp));
                }

                // Go active
                Assert.assertTrue(dstReader.reload());
                try (
                        TableReader srcReader = getReader(src.getTableName());
                        TestTableReaderRecordCursor srcCursor = new TestTableReaderRecordCursor().of(srcReader)
                ) {
                    String tableHeader = "l\ti\tstr\tvch\tts\n";
                    // check the original src table is not affected
                    String srcPartition2020_01_09 = "1\t1\t1\t&\uDA1F\uDE98|\uD924\uDE04\t2020-01-09T09:35:59.800000Z\n" +
                            "2\t2\t2\t\t2020-01-09T19:11:59.600000Z\n";
                    String srcPartition2020_01_10 = "3\t3\t3\těȞ鼷G\uD991\uDE7E\t2020-01-10T04:47:59.400000Z\n" +
                            "4\t4\t4\t\t2020-01-10T14:23:59.200000Z\n" +
                            "5\t5\t5\t͛Ԉ龘и\uDA89\uDFA4~\t2020-01-10T23:59:59.000000Z\n";
                    String expected = tableHeader + srcPartition2020_01_09 + srcPartition2020_01_10;
                    assertCursor(replaceTimestampSuffix(expected), srcCursor, srcReader.getMetadata(), true);

                    // now check the dst table
                    // the first 2 rows must be the same as the src table - because we attached the 2020-01-09 partition from src table
                    // and 3rd line is a partition 2020-01-10 from the original dst table
                    String dstPartition2020_01_10 = "2\t2\t2\tqK䰭\u008B}ѱʜ\uDB8D\uDE4Eᯤ\\篸\t2020-01-10T23:59:59.000000Z\n";
                    expected = tableHeader + srcPartition2020_01_09 + dstPartition2020_01_10;
                    assertCursor(
                            replaceTimestampSuffix(expected),
                            dstCursor,
                            dstReader.getMetadata(),
                            true
                    );
                }
            }
        });
    }

    private void assertSchemaMatch(AddColumn tm, int idx) throws Exception {
        if (idx > 0) {
            setUp();
        }
        assertMemoryLeak(() -> {
                    TableModel src = new TableModel(configuration, "srcCM" + idx, PartitionBy.DAY);
                    TableModel dst = new TableModel(configuration, "dstCM" + idx, PartitionBy.DAY);
                    src.col("l", ColumnType.LONG)
                            .col("i", ColumnType.INT)
                            .timestamp("ts", timestampType.getTimestampType());
                    tm.add(src);

                    createPopulateTable(
                            src,
                            10000,
                            "2022-08-01",
                            10);

                    dst.col("l", ColumnType.LONG)
                            .col("i", ColumnType.INT)
                            .timestamp("ts", timestampType.getTimestampType());
                    tm.add(dst);

                    AbstractCairoTest.create(dst);
                    attachFromSrcIntoDst(src, dst, "2022-08-01");
                }
        );
        if (idx > 0) {
            tearDown();
        }
    }

    private void assertSchemaMismatch(
            String srcTableName,
            AddColumn srcTransform,
            String dstTableName,
            AddColumn dstTransform,
            AddColumn afterCreateSrc,
            String errorMessage
    ) throws Exception {
        TableModel src = new TableModel(configuration, srcTableName, PartitionBy.DAY);
        TableModel dst = new TableModel(configuration, dstTableName, PartitionBy.DAY);
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
        AbstractCairoTest.create(dst);
        try {
            attachFromSrcIntoDst(src, dst, "2022-08-01");
            Assert.fail("Expected exception with '" + errorMessage + "' message");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), errorMessage);
        }
        TableToken tableToken = engine.verifyTableName(dstTableName);
        Files.rmdir(path.of(root).concat(tableToken).concat("2022-08-01").put(configuration.getAttachPartitionSuffix()), true);
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

        TableToken tableToken = engine.verifyTableName(src.getName());
        path.of(configuration.getDbRoot()).concat(tableToken);
        int pathLen = path.size();

        TableToken tableToken0 = engine.verifyTableName(dst.getName());
        other.of(configuration.getDbRoot()).concat(tableToken0);
        int otherLen = other.size();

        int hi = -1;
        switch (dst.getPartitionBy()) {
            case PartitionBy.DAY:
                hi = 10; // yyyy-MM-dd;
                break;
            case PartitionBy.WEEK:
                hi = 8; // YYYY-Www
                break;
            case PartitionBy.MONTH:
                hi = 7; // yyyy-MM
                break;
            case PartitionBy.YEAR:
                hi = 4; // yyyy
                break;
            case PartitionBy.HOUR:
                hi = 13; // yyyy-MM-ddTHH
                break;
        }
        for (int i = 0; i < partitionList.length; i++) {
            String partition = partitionList[i];
            int limit;
            if (hi == -1) {
                // by none
                limit = partition.length();
            } else {
                limit = hi;
            }
            path.trimTo(pathLen).concat(partition, 0, limit).$();
            other.trimTo(otherLen).concat(partition, 0, limit).put(configuration.getAttachPartitionSuffix()).$();
            TestUtils.copyDirectory(path, other, configuration.getMkDirMode());
        }

        int rowCount = readAllRows(dst.getName());
        engine.clear();
        execute("ALTER TABLE " + dst.getName() + " ATTACH PARTITION LIST " + partitions + ";", sqlExecutionContext);
        int newRowCount = readAllRows(dst.getName());
        Assert.assertTrue(newRowCount > rowCount);

        TimestampDriver driver = timestampType.getDriver();
        long timestamp = 0;
        for (String partition : partitionList) {
            int limit = hi == -1 ? partition.length() : hi;
            long ts = driver.parseFloorLiteral(partition.substring(0, limit)
                    + (src.getPartitionBy() == PartitionBy.YEAR ? "-01-01" : "")
                    + (src.getPartitionBy() == PartitionBy.MONTH ? "-01" : "")
                    + "T23:59:59.999Z");
            if (ts > timestamp) {
                timestamp = ts;
            }
        }

        // Check table is writable after partition attach
        engine.clear();
        try (TableWriter writer = getWriter(dst.getName())) {
            TableWriter.Row row = writer.newRow(timestamp);
            row.putInt(1, 1);
            row.append();
            writer.commit();
        }
    }

    private void copyPartitionToAttachable(
            TableToken srcTableToken,
            String srcPartitionName,
            String dstTableName,
            String dstPartitionName
    ) {
        copyPartitionAndMetadata(
                configuration.getDbRoot(),
                srcTableToken,
                srcPartitionName,
                configuration.getDbRoot(),
                dstTableName,
                dstPartitionName,
                configuration.getAttachPartitionSuffix()
        );
    }

    private int readAllRows(String tableName) {
        try (FullFwdPartitionFrameCursor cursor = new FullFwdPartitionFrameCursor()) {
            cursor.of(getReader(tableName));
            PartitionFrame frame;
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

    private String replaceTimestampSuffix(String expected) {
        return ColumnType.isTimestampNano(timestampType.getTimestampType()) ? expected.replaceAll("Z\t", "000Z\t").replaceAll("Z\n", "000Z\n") : expected;
    }

    private void testAttachPartitionWrongFixedColumn(int columnType) throws Exception {
        assertMemoryLeak(() -> {
            AddColumn src = s -> s.col("l", ColumnType.LONG)
                    .col("i", ColumnType.INT)
                    .timestamp("ts", timestampType.getTimestampType())
                    .col("t", columnType);

            assertSchemaMismatch(
                    "src34" + ColumnType.nameOf(columnType),
                    src,
                    "dst34" + ColumnType.nameOf(columnType),
                    dst -> {
                    },
                    s -> {
                        engine.clear();
                        TableToken tableToken = engine.verifyTableName(s.getName());
                        path.of(configuration.getDbRoot()).concat(tableToken).concat("2022-08-01").concat("t.d").$();
                        long fd = TestFilesFacadeImpl.INSTANCE.openRW(path.$(), CairoConfiguration.O_NONE);
                        Files.truncate(fd, Files.length(fd) / 10);
                        TestFilesFacadeImpl.INSTANCE.close(fd);
                    },
                    "Column file is too small"
            );
        });
    }

    private void testSqlFailedOnFsOperation(
            FilesFacadeImpl ff,
            String srcTableName,
            String dstTableName,
            boolean catchAll,
            String... errorContains
    ) throws Exception {
        assertMemoryLeak(ff, () -> {
                    TableModel src = new TableModel(configuration, srcTableName, PartitionBy.DAY);
                    TableModel dst = new TableModel(configuration, dstTableName, PartitionBy.DAY);

                    createPopulateTable(
                            1,
                            src.timestamp("ts", timestampType.getTimestampType())
                                    .col("i", ColumnType.INT)
                                    .col("l", ColumnType.LONG),
                            100,
                            "2020-01-01",
                            3);

                    AbstractCairoTest.create(dst.timestamp("ts", timestampType.getTimestampType())
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG));

                    try {
                        attachFromSrcIntoDst(src, dst, "2020-01-01");
                        Assert.fail();
                    } catch (CairoException | SqlException e) {
                        for (String error : errorContains) {
                            TestUtils.assertContains(e.getFlyweightMessage(), error);
                        }
                    } catch (Throwable e) {
                        if (catchAll) {
                            for (String error : errorContains) {
                                TestUtils.assertContains(e.getMessage(), error);
                            }
                        } else {
                            throw e;
                        }
                    }

                    // second attempt without FilesFacade override should work ok
                    attachFromSrcIntoDst(src, dst, "2020-01-02");
                }
        );
    }

    private void writeToStrIndexFile(TableModel src, String partition, String columnFileName, long value, long offset) {
        FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
        long fd = -1;
        long writeBuff = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            // .i file
            engine.clear();
            TableToken tableToken = engine.verifyTableName(src.getName());
            path.of(configuration.getDbRoot()).concat(tableToken).concat(partition).concat(columnFileName).$();
            fd = ff.openRW(path.$(), CairoConfiguration.O_NONE);
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
