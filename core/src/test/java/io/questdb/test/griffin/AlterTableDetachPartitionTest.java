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

import io.questdb.PropertyKey;
import io.questdb.cairo.AttachDetachStatus;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.O3PartitionPurgeJob;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.cairo.Overrides;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static io.questdb.cairo.AttachDetachStatus.*;
import static io.questdb.cairo.TableUtils.*;

@RunWith(Parameterized.class)
public class AlterTableDetachPartitionTest extends AbstractAlterTableAttachPartitionTest {
    private static O3PartitionPurgeJob purgeJob;
    private final TestTimestampType timestampType;

    public AlterTableDetachPartitionTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractCairoTest.setUpStatic();
        purgeJob = new O3PartitionPurgeJob(engine, 1);
    }

    @AfterClass
    public static void tearDownStatic() {
        purgeJob = Misc.free(purgeJob);
        AbstractCairoTest.tearDownStatic();
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO}, {TestTimestampType.NANO}
        });
    }

    @Test
    public void testAlreadyDetached1() throws Exception {
        assertFailure(
                "tab143",
                "ALTER TABLE tab143 DETACH PARTITION LIST '2022-06-03', '2022-06-03'",
                "could not detach partition [table=tab143, detachStatus=DETACH_ERR_MISSING_PARTITION"
        );
    }

    @Test
    public void testAlreadyDetached2() throws Exception {
        assertFailure(
                null,
                "tab143",
                "ALTER TABLE tab143 DETACH PARTITION LIST '2022-06-03'",
                "could not detach partition [table=tab143, detachStatus=DETACH_ERR_ALREADY_DETACHED",
                () -> {
                    TableToken tableToken = engine.verifyTableName("tab143");
                    path.of(configuration.getDbRoot())
                            .concat(tableToken)
                            .concat("2022-06-03")
                            .put(DETACHED_DIR_MARKER)
                            .slash$();
                    Assert.assertEquals(0, TestFilesFacadeImpl.INSTANCE.mkdirs(path, 509));
                }
        );
    }

    @Test
    public void testAttachCannotCopy() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_ATTACH_PARTITION_COPY, true);
            String tableName = "tabDetachAttachMissingMeta";
            node1.setProperty(
                    PropertyKey.CAIRO_ATTACH_PARTITION_SUFFIX,
                    DETACHED_DIR_MARKER
            );
            ff = new TestFilesFacadeImpl() {
                @Override
                public int copyRecursive(Path src, Path dst, int dirMode) {
                    if (Utf8s.containsAscii(src, DETACHED_DIR_MARKER)) {
                        return 1000;
                    }
                    return 0;
                }
            };

            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    tab
                            .timestamp("ts", timestampType.getTimestampType())
                            .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG)
                            .col("s2", ColumnType.SYMBOL)
                            .col("vch", ColumnType.VARCHAR),
                    10,
                    "2022-06-01",
                    3
            );
            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01', '2022-06-02'");
            assertSql(
                    replaceTimestampSuffix("first\tts\n" +
                            "2022-06-03T02:23:59.300000Z\t2022-06-03T02:23:59.300000Z\n"), "select first(ts), ts from " + tableName + " sample by 1d ALIGN TO FIRST OBSERVATION"
            );

            assertFailure(
                    "ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01', '2022-06-02'",
                    ATTACH_ERR_COPY.name()
            );
        });
    }

    @Test
    public void testAttachFailsCopyMeta() throws Exception {
        FilesFacadeImpl ff1 = new TestFilesFacadeImpl() {
            int i = 0;

            @Override
            public int copy(LPSZ src, LPSZ dest) {
                if (Utf8s.containsAscii(dest, META_FILE_NAME)) {
                    i++;
                    if (i == 3) {
                        return -1;
                    }
                }
                return super.copy(src, dest);
            }
        };
        node1.setProperty(
                PropertyKey.CAIRO_ATTACH_PARTITION_SUFFIX,
                DETACHED_DIR_MARKER
        );
        assertMemoryLeak(ff1, () -> {
            String tableName = "tab";

            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    tab
                            .timestamp("ts", timestampType.getTimestampType())
                            .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG)
                            .col("s2", ColumnType.SYMBOL)
                            .col("vch", ColumnType.VARCHAR),
                    10,
                    "2022-06-01",
                    3
            );
            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01', '2022-06-02'");
            assertSql(
                    replaceTimestampSuffix("first\tts\n" +
                            "2022-06-03T02:23:59.300000Z\t2022-06-03T02:23:59.300000Z\n"), "select first(ts), ts from " + tableName + " sample by 1d ALIGN TO FIRST OBSERVATION"
            );

            execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01', '2022-06-02'");
            assertFailure("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01', '2022-06-02'", DETACH_ERR_COPY_META.name());
            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01', '2022-06-02'");
            execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01', '2022-06-02'");

            assertSql(
                    replaceTimestampSuffix("first\tts\n" +
                            "2022-06-01T07:11:59.900000Z\t2022-06-01T07:11:59.900000Z\n" +
                            "2022-06-02T11:59:59.500000Z\t2022-06-02T07:11:59.900000Z\n" +
                            "2022-06-03T09:35:59.200000Z\t2022-06-03T07:11:59.900000Z\n"), "select first(ts), ts from " + tableName + " sample by 1d ALIGN TO FIRST OBSERVATION"
            );
        });
    }

    @Test
    public void testAttachFailsRetried() throws Exception {
        FilesFacadeImpl ff1 = new TestFilesFacadeImpl() {
            int i = 0;

            @Override
            public int rename(LPSZ src, LPSZ dst) {
                if (Utf8s.containsAscii(src, DETACHED_DIR_MARKER) && i++ < 2) {
                    return -1;
                }
                super.rename(src, dst);
                return 0;
            }
        };
        assertMemoryLeak(ff1, () -> {
            String tableName = "tabDetachAttachMissingMeta";
            node1.setProperty(
                    PropertyKey.CAIRO_ATTACH_PARTITION_SUFFIX,
                    DETACHED_DIR_MARKER
            );

            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    tab
                            .timestamp("ts", timestampType.getTimestampType())
                            .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG)
                            .col("s2", ColumnType.SYMBOL)
                            .col("vch", ColumnType.VARCHAR),
                    10,
                    "2022-06-01",
                    3
            );
            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01', '2022-06-02'");
            assertSql(
                    replaceTimestampSuffix("first\tts\n" +
                            "2022-06-03T02:23:59.300000Z\t2022-06-03T02:23:59.300000Z\n"), "select first(ts), ts from " + tableName + " sample by 1d ALIGN TO FIRST OBSERVATION"
            );

            assertFailure(
                    "ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01', '2022-06-02'",
                    "could not attach partition"
            );
            assertFailure(
                    "ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01', '2022-06-02'",
                    "could not attach partition"
            );

            execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01', '2022-06-02'");
            assertSql(
                    replaceTimestampSuffix("first\tts\n" +
                            "2022-06-01T07:11:59.900000Z\t2022-06-01T07:11:59.900000Z\n" +
                            "2022-06-02T11:59:59.500000Z\t2022-06-02T07:11:59.900000Z\n" +
                            "2022-06-03T09:35:59.200000Z\t2022-06-03T07:11:59.900000Z\n"), "select first(ts), ts from " + tableName + " sample by 1d ALIGN TO FIRST OBSERVATION"
            );
        });
    }

    @Test
    public void testAttachPartitionAfterTruncate() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tab";
            TableModel src = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    src.col("sym", ColumnType.SYMBOL).timestamp("ts", timestampType.getTimestampType()),
                    3,
                    "2020-01-01",
                    1
            );

            execute("insert into " + tableName + " values ('foobar', '2020-01-02T23:59:59')");

            assertSql(
                    ColumnType.isTimestampMicro(timestampType.getTimestampType()) ?
                            "first\tsym\n" +
                                    "2020-01-01T07:59:59.666666Z\tCPSW\n" +
                                    "2020-01-01T15:59:59.333332Z\tHYRX\n" +
                                    "2020-01-01T23:59:58.999998Z\t\n" +
                                    "2020-01-02T23:59:59.000000Z\tfoobar\n"
                            : "first\tsym\n" +
                            "2020-01-01T07:59:59.666666666Z\tCPSW\n" +
                            "2020-01-01T15:59:59.333333332Z\tHYRX\n" +
                            "2020-01-01T23:59:58.999999998Z\t\n" +
                            "2020-01-02T23:59:59.000000000Z\tfoobar\n",
                    "select first(ts), sym from " + tableName + " sample by 1d ALIGN TO FIRST OBSERVATION"
            );

            execute("alter table " + tableName + " detach partition list '2020-01-01'");

            execute("truncate table " + tableName);

            renameDetachedToAttachable(tableName, "2020-01-01");
            execute("alter table " + tableName + " attach partition list '2020-01-01'");

            // No symbols are present.
            assertSql(
                    ColumnType.isTimestampMicro(timestampType.getTimestampType()) ?
                            "first\tsym\n" +
                                    "2020-01-01T07:59:59.666666Z\t\n" +
                                    "2020-01-01T15:59:59.333332Z\t\n" +
                                    "2020-01-01T23:59:58.999998Z\t\n"
                            : "first\tsym\n" +
                            "2020-01-01T07:59:59.666666666Z\t\n" +
                            "2020-01-01T15:59:59.333333332Z\t\n" +
                            "2020-01-01T23:59:58.999999998Z\t\n",
                    "select first(ts), sym from " + tableName + " sample by 1d ALIGN TO FIRST OBSERVATION"
            );
        });
    }

    @Test
    public void testAttachPartitionAfterTruncateKeepSymbolTables() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "testAttachPartitionAfterTruncateKeepSymbolTables";
            TableModel src = new TableModel(configuration, tableName, PartitionBy.DAY);
            // It's important to have a symbol column here to make sure
            // that we don't wipe symbol tables on TRUNCATE.
            createPopulateTable(
                    src.col("sym", ColumnType.SYMBOL).timestamp("ts", timestampType.getTimestampType()),
                    3,
                    "2020-01-01",
                    1
            );

            execute("insert into " + tableName + " values ('foobar', '2020-01-02T23:59:59')");

            assertSql(
                    ColumnType.isTimestampMicro(timestampType.getTimestampType()) ?
                            "first\tsym\n" +
                                    "2020-01-01T07:59:59.666666Z\tCPSW\n" +
                                    "2020-01-01T15:59:59.333332Z\tHYRX\n" +
                                    "2020-01-01T23:59:58.999998Z\t\n" +
                                    "2020-01-02T23:59:59.000000Z\tfoobar\n"
                            : "first\tsym\n" +
                            "2020-01-01T07:59:59.666666666Z\tCPSW\n" +
                            "2020-01-01T15:59:59.333333332Z\tHYRX\n" +
                            "2020-01-01T23:59:58.999999998Z\t\n" +
                            "2020-01-02T23:59:59.000000000Z\tfoobar\n",
                    "select first(ts), sym from " + tableName + " sample by 1d ALIGN TO FIRST OBSERVATION"
            );

            execute("alter table " + tableName + " detach partition list '2020-01-01'");

            execute("truncate table " + tableName + " keep symbol maps");

            renameDetachedToAttachable(tableName, "2020-01-01");
            execute("alter table " + tableName + " attach partition list '2020-01-01'");

            // All symbols are kept.
            assertSql(
                    ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? "first\tsym\n" +
                            "2020-01-01T07:59:59.666666Z\tCPSW\n" +
                            "2020-01-01T15:59:59.333332Z\tHYRX\n" +
                            "2020-01-01T23:59:58.999998Z\t\n" :
                            "first\tsym\n" +
                                    "2020-01-01T07:59:59.666666666Z\tCPSW\n" +
                                    "2020-01-01T15:59:59.333333332Z\tHYRX\n" +
                                    "2020-01-01T23:59:58.999999998Z\t\n", "select first(ts), sym from " + tableName + " sample by 1d ALIGN TO FIRST OBSERVATION"
            );
        });
    }

    @Test
    public void testAttachPartitionCommits() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tab";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    1,
                    tab.col("l", ColumnType.LONG)
                            .col("i", ColumnType.INT)
                            .timestamp("ts", timestampType.getTimestampType()),
                    5,
                    "2022-06-01",
                    4
            );

            String timestampDay = "2022-06-02";
            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '" + timestampDay + "'", sqlExecutionContext);

            renameDetachedToAttachable(tableName, timestampDay);

            TimestampDriver driver = timestampType.getDriver();
            try (TableWriter writer = getWriter(tableName)) {
                // structural change
                writer.addColumn("new_column", ColumnType.INT);

                TableWriter.Row row = writer.newRow(driver.parseFloorLiteral("2022-06-03T12:00:00.000000Z"));
                row.putLong(0, 33L);
                row.putInt(1, 33);
                row.append();

                Assert.assertEquals(AttachDetachStatus.OK, writer.attachPartition(driver.parseFloorLiteral(timestampDay)));
            }

            assertContent(
                    "l\ti\tts\tnew_column\n" +
                            "1\t1\t2022-06-01T19:11:59.800000Z\tnull\n" +
                            "2\t2\t2022-06-02T14:23:59.600000Z\tnull\n" +
                            "3\t3\t2022-06-03T09:35:59.400000Z\tnull\n" +
                            "33\t33\t2022-06-03T12:00:00.000000Z\tnull\n" +
                            "4\t4\t2022-06-04T04:47:59.200000Z\tnull\n" +
                            "5\t5\t2022-06-04T23:59:59.000000Z\tnull\n",
                    tableName
            );
        });
    }

    @Test
    public void testAttachPartitionCommitsToSamePartition() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tab";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    1,
                    tab.col("l", ColumnType.LONG)
                            .col("i", ColumnType.INT)
                            .col("vch", ColumnType.VARCHAR)
                            .timestamp("ts", timestampType.getTimestampType()),
                    5,
                    "2022-06-01",
                    4
            );

            String timestampDay = "2022-06-02";
            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '" + timestampDay + "'", sqlExecutionContext);

            renameDetachedToAttachable(tableName, timestampDay);

            TimestampDriver driver = timestampType.getDriver();
            long timestamp = driver.parseFloorLiteral(timestampDay + "T22:00:00.000000Z");
            try (TableWriter writer = getWriter(tableName)) {
                // structural change
                writer.addColumn("new_column", ColumnType.INT);

                TableWriter.Row row = writer.newRow(timestamp);
                row.putLong(0, 33L);
                row.putInt(1, 33);
                row.append();

                Assert.assertEquals(AttachDetachStatus.ATTACH_ERR_PARTITION_EXISTS, writer.attachPartition(driver.parseFloorLiteral(timestampDay)));
            }

            assertContent(
                    "l\ti\tvch\tts\tnew_column\n" +
                            "1\t1\t&\uDA1F\uDE98|\uD924\uDE04\t2022-06-01T19:11:59.800000Z\tnull\n" +
                            "33\t33\t\t2022-06-02T22:00:00.000000Z\tnull\n" +
                            "3\t3\těȞ鼷G\uD991\uDE7E\t2022-06-03T09:35:59.400000Z\tnull\n" +
                            "4\t4\t\t2022-06-04T04:47:59.200000Z\tnull\n" +
                            "5\t5\t͛Ԉ龘и\uDA89\uDFA4~\t2022-06-04T23:59:59.000000Z\tnull\n",
                    tableName
            );
        });
    }

    @Test
    public void testAttachPartitionWithColumnTops() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tab";
            TableModel src = new TableModel(configuration, tableName, PartitionBy.DAY);

            createPopulateTable(
                    src.col("l", ColumnType.LONG)
                            .col("i", ColumnType.INT)
                            .timestamp("ts", timestampType.getTimestampType()),
                    100,
                    "2020-01-01",
                    2
            );

            execute("alter table " + tableName + " add column str string");
            execute("alter table " + tableName + " add column vch varchar");

            execute("insert into " + tableName +
                    " select x, rnd_int(), timestamp_sequence('2020-01-02T23:59:59', 1000000L * 60 * 20), rnd_str('a', 'b', 'c', null), rnd_varchar('a', 'b', 'c', null)" +
                    " from long_sequence(100)");

            execute("alter table " + tableName + " detach partition list '2020-01-02', '2020-01-03'");

            assertSql(
                    replaceTimestampSuffix("first\tstr\n" +
                            "2020-01-01T00:28:47.990000Z\t\n" +
                            "2020-01-04T00:19:59.000000Z\tb\n" +
                            "2020-01-04T00:39:59.000000Z\ta\n" +
                            "2020-01-04T00:59:59.000000Z\tb\n" +
                            "2020-01-04T01:39:59.000000Z\t\n" +
                            "2020-01-04T05:19:59.000000Z\tc\n"), "select first(ts), str from " + tableName + " sample by 1d ALIGN TO FIRST OBSERVATION"
            );

            renameDetachedToAttachable(tableName, "2020-01-02", "2020-01-03");
            execute("alter table " + tableName + " attach partition list '2020-01-02', '2020-01-03'");

            assertSql(
                    replaceTimestampSuffix("first\tstr\n" +
                            "2020-01-01T00:28:47.990000Z\t\n" +
                            "2020-01-02T00:57:35.480000Z\t\n" +
                            "2020-01-02T23:59:59.000000Z\tc\n" +
                            "2020-01-03T00:19:59.000000Z\tb\n" +
                            "2020-01-03T00:39:59.000000Z\t\n" +
                            "2020-01-03T00:59:59.000000Z\ta\n" +
                            "2020-01-03T01:59:59.000000Z\tc\n" +
                            "2020-01-03T05:39:59.000000Z\tb\n" +
                            "2020-01-04T00:39:59.000000Z\ta\n" +
                            "2020-01-04T00:59:59.000000Z\tb\n" +
                            "2020-01-04T01:39:59.000000Z\t\n" +
                            "2020-01-04T05:19:59.000000Z\tc\n"), "select first(ts), str from " + tableName + " sample by 1d ALIGN TO FIRST OBSERVATION"
            );
        });
    }

    @Test
    public void testAttachWillFailIfThePartitionWasRecreated() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabTimeTravel2";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            String timestampDay = "2022-06-01";
            createPopulateTable(
                    tab
                            .col("l", ColumnType.LONG)
                            .col("i", ColumnType.INT)
                            .col("vch", ColumnType.VARCHAR)
                            .timestamp("ts", timestampType.getTimestampType()),
                    12,
                    timestampDay,
                    4
            );
            assertContent(
                    ColumnType.isTimestampMicro(timestampType.getTimestampType()) ?
                            "l\ti\tvch\tts\n" +
                                    "1\t1\t1\t2022-06-01T07:59:59.916666Z\n" +
                                    "2\t2\t2\t2022-06-01T15:59:59.833332Z\n" +
                                    "3\t3\t3\t2022-06-01T23:59:59.749998Z\n" +
                                    "4\t4\t4\t2022-06-02T07:59:59.666664Z\n" +
                                    "5\t5\t5\t2022-06-02T15:59:59.583330Z\n" +
                                    "6\t6\t6\t2022-06-02T23:59:59.499996Z\n" +
                                    "7\t7\t7\t2022-06-03T07:59:59.416662Z\n" +
                                    "8\t8\t8\t2022-06-03T15:59:59.333328Z\n" +
                                    "9\t9\t9\t2022-06-03T23:59:59.249994Z\n" +
                                    "10\t10\t10\t2022-06-04T07:59:59.166660Z\n" +
                                    "11\t11\t11\t2022-06-04T15:59:59.083326Z\n" +
                                    "12\t12\t12\t2022-06-04T23:59:58.999992Z\n"
                            : "l\ti\tvch\tts\n" +
                            "1\t1\t1\t2022-06-01T07:59:59.916666666Z\n" +
                            "2\t2\t2\t2022-06-01T15:59:59.833333332Z\n" +
                            "3\t3\t3\t2022-06-01T23:59:59.749999998Z\n" +
                            "4\t4\t4\t2022-06-02T07:59:59.666666664Z\n" +
                            "5\t5\t5\t2022-06-02T15:59:59.583333330Z\n" +
                            "6\t6\t6\t2022-06-02T23:59:59.499999996Z\n" +
                            "7\t7\t7\t2022-06-03T07:59:59.416666662Z\n" +
                            "8\t8\t8\t2022-06-03T15:59:59.333333328Z\n" +
                            "9\t9\t9\t2022-06-03T23:59:59.249999994Z\n" +
                            "10\t10\t10\t2022-06-04T07:59:59.166666660Z\n" +
                            "11\t11\t11\t2022-06-04T15:59:59.083333326Z\n" +
                            "12\t12\t12\t2022-06-04T23:59:58.999999992Z\n",
                    tableName
            );

            // drop the partition
            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '" + timestampDay + "'", sqlExecutionContext);

            // insert data, which will create the partition again
            engine.clear();
            TimestampDriver driver = timestampType.getDriver();
            long timestamp = driver.parseFloorLiteral(timestampDay + "T09:59:59.999999Z");
            try (TableWriter writer = getWriter(tableName)) {
                TableWriter.Row row = writer.newRow(timestamp);
                row.putLong(0, 137L);
                row.putInt(1, 137);
                row.append();
                writer.commit();
            }
            String expected = ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? "l\ti\tvch\tts\n" +
                    "137\t137\t\t2022-06-01T09:59:59.999999Z\n" +
                    "4\t4\t4\t2022-06-02T07:59:59.666664Z\n" +
                    "5\t5\t5\t2022-06-02T15:59:59.583330Z\n" +
                    "6\t6\t6\t2022-06-02T23:59:59.499996Z\n" +
                    "7\t7\t7\t2022-06-03T07:59:59.416662Z\n" +
                    "8\t8\t8\t2022-06-03T15:59:59.333328Z\n" +
                    "9\t9\t9\t2022-06-03T23:59:59.249994Z\n" +
                    "10\t10\t10\t2022-06-04T07:59:59.166660Z\n" +
                    "11\t11\t11\t2022-06-04T15:59:59.083326Z\n" +
                    "12\t12\t12\t2022-06-04T23:59:58.999992Z\n"
                    : "l\ti\tvch\tts\n" +
                    "137\t137\t\t2022-06-01T09:59:59.999999000Z\n" +
                    "4\t4\t4\t2022-06-02T07:59:59.666666664Z\n" +
                    "5\t5\t5\t2022-06-02T15:59:59.583333330Z\n" +
                    "6\t6\t6\t2022-06-02T23:59:59.499999996Z\n" +
                    "7\t7\t7\t2022-06-03T07:59:59.416666662Z\n" +
                    "8\t8\t8\t2022-06-03T15:59:59.333333328Z\n" +
                    "9\t9\t9\t2022-06-03T23:59:59.249999994Z\n" +
                    "10\t10\t10\t2022-06-04T07:59:59.166666660Z\n" +
                    "11\t11\t11\t2022-06-04T15:59:59.083333326Z\n" +
                    "12\t12\t12\t2022-06-04T23:59:58.999999992Z\n";
            assertQuery(expected, tableName, null, "ts", true, true);
            renameDetachedToAttachable(tableName, timestampDay);
            assertFailure(
                    "ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + timestampDay + "'",
                    "could not attach partition [table=tabTimeTravel2, detachStatus=ATTACH_ERR_PARTITION_EXISTS"
            );
            assertQuery(expected, tableName, null, "ts", true, true);
        });
    }

    @Test
    public void testCannotCopyColumnVersions() throws Exception {
        assertCannotCopyMeta("testCannotCopyColumnVersions", 2);
    }

    @Test
    public void testCannotCopyMeta() throws Exception {
        assertCannotCopyMeta("testCannotCopyMeta", 1);
    }

    @Test
    public void testCannotCopyTxn() throws Exception {
        assertCannotCopyMeta("tab", 3);
    }

    @Test
    public void testCannotDetachActivePartition() throws Exception {
        assertFailure(
                "tab17",
                "ALTER TABLE tab17 DETACH PARTITION LIST '2022-06-05'",
                "could not detach partition [table=tab17, detachStatus=DETACH_ERR_ACTIVE"
        );
    }

    @Test
    public void testCannotRemoveFolder() throws Exception {
        AtomicInteger counter = new AtomicInteger();
        ff = new TestFilesFacadeImpl() {
            @Override
            public boolean rmdir(Path path, boolean lazy) {
                if (Utf8s.containsAscii(path, "2022-06-03")) {
                    if (counter.getAndIncrement() == 0) {
                        return false;
                    }
                }
                return super.rmdir(path, lazy);
            }
        };

        assertMemoryLeak(ff, () -> {
            String tableName = "tab";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    tab
                            .timestamp("ts", timestampType.getTimestampType())
                            .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG)
                            .col("s2", ColumnType.SYMBOL)
                            .col("vch", ColumnType.VARCHAR),
                    100,
                    "2022-06-01",
                    5
            );


            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-03'");

            Assert.assertEquals(1, counter.get());
            runPartitionPurgeJobs();
            Assert.assertEquals(2, counter.get());
        });
    }

    @Test
    public void testCannotUndoRenameAfterBrokenCopyMeta() throws Exception {
        FilesFacadeImpl ff1 = new TestFilesFacadeImpl() {
            private boolean copyCalled = false;

            @Override
            public int copy(LPSZ from, LPSZ to) {
                copyCalled = true;
                return -1;
            }

            @Override
            public boolean rmdir(Path path, boolean lazy) {
                if (!copyCalled) {
                    return super.rmdir(path, lazy);
                }
                return false;
            }
        };
        assertMemoryLeak(
                ff1,
                () -> {
                    String tableName = "tabUndoRenameCopyMeta";
                    TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
                    createPopulateTable(
                            tab
                                    .timestamp("ts", timestampType.getTimestampType())
                                    .col("i", ColumnType.INT)
                                    .col("l", ColumnType.LONG)
                                    .col("vch", ColumnType.VARCHAR),
                            10,
                            "2022-06-01",
                            4
                    );

                    engine.clear();
                    TimestampDriver driver = timestampType.getDriver();
                    long timestamp = driver.parseFloorLiteral("2022-06-01T00:00:00.000000Z");
                    try (TableWriter writer = getWriter(tableName)) {
                        AttachDetachStatus attachDetachStatus = writer.detachPartition(timestamp);
                        Assert.assertEquals(DETACH_ERR_COPY_META, attachDetachStatus);
                    }
                    assertContent("ts\ti\tl\tvch\n" +
                            "2022-06-01T09:35:59.900000Z\t1\t1\t1\n" +
                            "2022-06-01T19:11:59.800000Z\t2\t2\t2\n" +
                            "2022-06-02T04:47:59.700000Z\t3\t3\t3\n" +
                            "2022-06-02T14:23:59.600000Z\t4\t4\t4\n" +
                            "2022-06-02T23:59:59.500000Z\t5\t5\t5\n" +
                            "2022-06-03T09:35:59.400000Z\t6\t6\t6\n" +
                            "2022-06-03T19:11:59.300000Z\t7\t7\t7\n" +
                            "2022-06-04T04:47:59.200000Z\t8\t8\t8\n" +
                            "2022-06-04T14:23:59.100000Z\t9\t9\t9\n" +
                            "2022-06-04T23:59:59.000000Z\t10\t10\t10\n", tableName);
                }
        );
    }

    @Test
    public void testDetachAttachAnotherDrive() throws Exception {
        FilesFacadeImpl ff1 = new TestFilesFacadeImpl() {
            @Override
            public int hardLinkDirRecursive(Path src, Path dst, int dirMode) {
                return 100018;
            }

            public boolean isCrossDeviceCopyError(int errno) {
                return true;
            }
        };

        assertMemoryLeak(
                ff1,
                () -> {
                    node1.setProperty(PropertyKey.CAIRO_ATTACH_PARTITION_COPY, true);
                    String tableName = "tab";
                    node1.setProperty(
                            PropertyKey.CAIRO_ATTACH_PARTITION_SUFFIX,
                            DETACHED_DIR_MARKER
                    );

                    TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
                    createPopulateTable(
                            tab
                                    .timestamp("ts", timestampType.getTimestampType())
                                    .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                                    .col("i", ColumnType.INT)
                                    .col("l", ColumnType.LONG)
                                    .col("s2", ColumnType.SYMBOL),
                            10,
                            "2022-06-01",
                            3
                    );
                    execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01', '2022-06-02'");
                    assertSql(
                            replaceTimestampSuffix("first\tts\n" +
                                    "2022-06-03T02:23:59.300000Z\t2022-06-03T02:23:59.300000Z\n"), "select first(ts), ts from " + tableName + " sample by 1d ALIGN TO FIRST OBSERVATION"
                    );

                    for (int i = 0; i < 2; i++) {
                        execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01', '2022-06-02'");
                        assertSql(
                                replaceTimestampSuffix("first\tts\n" +
                                        "2022-06-01T07:11:59.900000Z\t2022-06-01T07:11:59.900000Z\n" +
                                        "2022-06-02T11:59:59.500000Z\t2022-06-02T07:11:59.900000Z\n" +
                                        "2022-06-03T09:35:59.200000Z\t2022-06-03T07:11:59.900000Z\n"), "select first(ts), ts from " + tableName + " sample by 1d ALIGN TO FIRST OBSERVATION"
                        );
                        execute("ALTER TABLE " + tableName + " DROP PARTITION LIST '2022-06-01', '2022-06-02'");

                    }
                }
        );
    }

    @Test
    public void testDetachAttachAnotherDriveFailsToCopy() throws Exception {
        FilesFacadeImpl ff1 = new TestFilesFacadeImpl() {
            @Override
            public int copyRecursive(Path src, Path dst, int dirMode) {
                return 100018;
            }

            @Override
            public int hardLinkDirRecursive(Path src, Path dst, int dirMode) {
                return 100018;
            }

            public boolean isCrossDeviceCopyError(int errno) {
                return true;
            }
        };

        assertMemoryLeak(
                ff1,
                () -> {
                    node1.setProperty(PropertyKey.CAIRO_ATTACH_PARTITION_COPY, true);
                    String tableName = "testDetachAttachAnotherDriveFailsToCopy";
                    node1.setProperty(
                            PropertyKey.CAIRO_ATTACH_PARTITION_SUFFIX,
                            DETACHED_DIR_MARKER
                    );

                    TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
                    createPopulateTable(tab
                                    .timestamp("ts", timestampType.getTimestampType())
                                    .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                                    .col("i", ColumnType.INT)
                                    .col("l", ColumnType.LONG)
                                    .col("s2", ColumnType.SYMBOL)
                                    .col("vch", ColumnType.VARCHAR),
                            10,
                            "2022-06-01",
                            3
                    );
                    assertFailure(
                            "ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01', '2022-06-02'",
                            DETACH_ERR_COPY.name()
                    );

                }
        );
    }

    @Test
    public void testDetachAttachAnotherDriveFailsToHardLink() throws Exception {
        FilesFacadeImpl ff1 = new TestFilesFacadeImpl() {
            @Override
            public int hardLinkDirRecursive(Path src, Path dst, int dirMode) {
                return 100018;
            }
        };

        assertMemoryLeak(
                ff1,
                () -> {
                    String tableName = "tab";
                    TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
                    createPopulateTable(
                            tab
                                    .timestamp("ts", timestampType.getTimestampType())
                                    .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                                    .col("i", ColumnType.INT)
                                    .col("l", ColumnType.LONG)
                                    .col("s2", ColumnType.SYMBOL)
                                    .col("vch", ColumnType.VARCHAR),
                            10,
                            "2022-06-01",
                            3
                    );
                    assertFailure(
                            "ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01', '2022-06-02'",
                            DETACH_ERR_HARD_LINK.name()
                    );

                }
        );
    }

    @Test
    public void testDetachAttachParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tab";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    1,
                    tab.timestamp("ts", timestampType.getTimestampType())
                            .col("si", ColumnType.SYMBOL).indexed(true, 250)
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG)
                            .col("s", ColumnType.SYMBOL)
                            .col("vch", ColumnType.VARCHAR),
                    10,
                    "2022-06-01",
                    2
            );

            String expected = "ts\tsi\ti\tl\ts\tvch\n" +
                    "2022-06-01T04:47:59.900000Z\tPEHN\t1\t1\tSXUX\t擉q\uDAE2\uDC5E͛\n" +
                    "2022-06-01T09:35:59.800000Z\t\t2\t2\t\t蝰L➤~2\uDAC6\uDED3ڎBH\n" +
                    "2022-06-01T14:23:59.700000Z\tVTJW\t3\t3\tRXGZ\t\n" +
                    "2022-06-01T19:11:59.600000Z\tVTJW\t4\t4\tGPGW\t:}w?5J8A.mS+F~W\n" +
                    "2022-06-01T23:59:59.500000Z\tCPSW\t5\t5\tGPGW\td^Z\n" +
                    "2022-06-02T04:47:59.400000Z\tPEHN\t6\t6\tGPGW\t篸{\uD9D7\uDFE5\uDAE9\uDF46OF\n" +
                    "2022-06-02T09:35:59.300000Z\tVTJW\t7\t7\t\t\n" +
                    "2022-06-02T14:23:59.200000Z\tVTJW\t8\t8\t\t䒭ܲ\u0379軦۽㒾\uD99D\uDEA7K裷\uD9CC\uDE73+\u0093ً\n" +
                    "2022-06-02T19:11:59.100000Z\t\t9\t9\tGPGW\tK\uD8E2\uDE25ӽ-\uDBED\uDC98\n" +
                    "2022-06-02T23:59:59.000000Z\t\t10\t10\t\ty\u0086W\n";

            assertContent(expected, tableName);

            engine.clear();
            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01'", sqlExecutionContext);
            renameDetachedToAttachable(tableName, "2022-06-01");
            execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01'", sqlExecutionContext);

            engine.clear();
            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2022-06-01'", sqlExecutionContext);
            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01'", sqlExecutionContext);
            renameDetachedToAttachable(tableName, "2022-06-01");
            execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01'", sqlExecutionContext);

            assertContent(expected, tableName);
        });
    }

    @Test
    public void testDetachAttachPartition() throws Exception {
        assertMemoryLeak(
                () -> {
                    String tableName = "tab";
                    TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
                    createPopulateTable(
                            1,
                            tab.timestamp("ts", timestampType.getTimestampType())
                                    .col("si", ColumnType.SYMBOL).indexed(true, 250)
                                    .col("i", ColumnType.INT)
                                    .col("l", ColumnType.LONG)
                                    .col("s", ColumnType.SYMBOL)
                                    .col("vch", ColumnType.VARCHAR),
                            10,
                            "2022-06-01",
                            2
                    );

                    String expected = "ts\tsi\ti\tl\ts\tvch\n" +
                            "2022-06-01T04:47:59.900000Z\tPEHN\t1\t1\tSXUX\t擉q\uDAE2\uDC5E͛\n" +
                            "2022-06-01T09:35:59.800000Z\t\t2\t2\t\t蝰L➤~2\uDAC6\uDED3ڎBH\n" +
                            "2022-06-01T14:23:59.700000Z\tVTJW\t3\t3\tRXGZ\t\n" +
                            "2022-06-01T19:11:59.600000Z\tVTJW\t4\t4\tGPGW\t:}w?5J8A.mS+F~W\n" +
                            "2022-06-01T23:59:59.500000Z\tCPSW\t5\t5\tGPGW\td^Z\n" +
                            "2022-06-02T04:47:59.400000Z\tPEHN\t6\t6\tGPGW\t篸{\uD9D7\uDFE5\uDAE9\uDF46OF\n" +
                            "2022-06-02T09:35:59.300000Z\tVTJW\t7\t7\t\t\n" +
                            "2022-06-02T14:23:59.200000Z\tVTJW\t8\t8\t\t䒭ܲ\u0379軦۽㒾\uD99D\uDEA7K裷\uD9CC\uDE73+\u0093ً\n" +
                            "2022-06-02T19:11:59.100000Z\t\t9\t9\tGPGW\tK\uD8E2\uDE25ӽ-\uDBED\uDC98\n" +
                            "2022-06-02T23:59:59.000000Z\t\t10\t10\t\ty\u0086W\n";

                    assertContent(expected, tableName);

                    engine.clear();
                    execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01'", sqlExecutionContext);
                    renameDetachedToAttachable(tableName, "2022-06-01");
                    execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01'", sqlExecutionContext);

                    engine.clear();
                    execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01'", sqlExecutionContext);
                    renameDetachedToAttachable(tableName, "2022-06-01");
                    execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01'", sqlExecutionContext);

                    assertContent(expected, tableName);
                });
    }

    @Test
    public void testDetachAttachPartitionBrokenMetadataColumnType() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabBrokenColType";
            String brokenTableName = "tabBrokenColType2";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            TableModel brokenMeta = new TableModel(configuration, brokenTableName, PartitionBy.DAY);
            try (
                    MemoryMARW mem = Vm.getCMARWInstance()
            ) {
                String timestampDay = "2022-06-01";
                createPopulateTable(
                        1,
                        tab.timestamp("ts", timestampType.getTimestampType())
                                .col("s1", ColumnType.SYMBOL).indexed(true, 256)
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG)
                                .col("s2", ColumnType.SYMBOL),
                        10,
                        timestampDay,
                        3
                );
                TestUtils.createTable(
                        engine,
                        mem,
                        path,
                        brokenMeta.timestamp("ts", timestampType.getTimestampType())
                                .col("s1", ColumnType.SYMBOL).indexed(true, 256)
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.INT)
                                .col("s2", ColumnType.SYMBOL),
                        1,
                        brokenMeta.getTableName()
                );
                execute("INSERT INTO " + brokenMeta.getName() + " SELECT * FROM " + tab.getName());

                engine.clear();
                execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '" + timestampDay + "'");
                execute("ALTER TABLE " + brokenTableName + " DETACH PARTITION LIST '" + timestampDay + "'");

                engine.clear();
                TableToken tableToken = engine.verifyTableName(brokenTableName);
                TableToken tableToken1 = engine.verifyTableName(tableName);

                path.of(configuration.getDbRoot()).concat(tableToken).concat(timestampDay).put(DETACHED_DIR_MARKER).$();
                other.of(configuration.getDbRoot()).concat(tableToken1).concat(timestampDay).put(configuration.getAttachPartitionSuffix()).$();

                Assert.assertTrue(Files.rename(path.$(), other.$()) > -1);

                // attempt to reattach
                assertFailure(
                        "ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + timestampDay + "'",
                        "Detached column [index=3, name=l, attribute=type] does not match current table metadata"
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
                        .timestamp("ts", timestampType.getTimestampType())
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG),
                "insert into tabBrokenTableId2 " +
                        "select " +
                        "CAST(1654041600000000L AS TIMESTAMP) + x * 3455990000  ts, " +
                        "cast(x as int) i, " +
                        "x l " +
                        "from long_sequence(100)",
                "ALTER TABLE tabBrokenTableId2 ADD COLUMN s SHORT",
                "Detached partition metadata [table_id] is not compatible with current table metadata"
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
                        .timestamp("ts", timestampType.getTimestampType()),
                "insert into tabBrokenTimestampIdx2 " +
                        "select " +
                        "cast(x as int) i, " +
                        "x l, " +
                        "CAST(1654041600000000L AS TIMESTAMP) + x * 3455990000  ts " +
                        "from long_sequence(100)",
                "ALTER TABLE tabBrokenTimestampIdx2 ADD COLUMN s SHORT",
                "Detached partition metadata [timestamp_index] is not compatible with current table metadata"
        );
    }

    @Test
    public void testDetachAttachPartitionFailsYouDidNotRenameTheFolderToAttachable() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabDetachAttachNotAttachable";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    tab
                            .timestamp("ts", timestampType.getTimestampType())
                            .col("si", ColumnType.SYMBOL).indexed(true, 32)
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG)
                            .col("s", ColumnType.SYMBOL),
                    10,
                    "2022-06-01",
                    3
            );

            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01', '2022-06-02'", sqlExecutionContext);
            assertFailure(
                    "ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01', '2022-06-02'",
                    "could not attach partition [table=tabDetachAttachNotAttachable, detachStatus=ATTACH_ERR_MISSING_PARTITION"
            );
        });
    }

    @Test
    public void testDetachAttachPartitionMissingMetadata() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabDetachAttachMissingMeta";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    tab
                            .timestamp("ts", timestampType.getTimestampType())
                            .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG)
                            .col("s2", ColumnType.SYMBOL)
                            .col("vch", ColumnType.VARCHAR),
                    10,
                    "2022-06-01",
                    3
            );
            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01', '2022-06-02'", sqlExecutionContext);

            // remove _meta.detached simply prevents metadata checking, all else is the same
            TableToken tableToken = engine.verifyTableName(tableName);
            path.of(configuration.getDbRoot())
                    .concat(tableToken)
                    .concat("2022-06-02")
                    .put(DETACHED_DIR_MARKER)
                    .concat(META_FILE_NAME)
                    .$();
            Assert.assertTrue(Files.remove(path.$()));
            path.parent().concat(COLUMN_VERSION_FILE_NAME).$();
            Assert.assertTrue(Files.remove(path.$()));
            renameDetachedToAttachable(tableName, "2022-06-01", "2022-06-02");
            execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01', '2022-06-02'", sqlExecutionContext);
            assertContent(
                    "ts\ts1\ti\tl\ts2\tvch\n" +
                            "2022-06-01T07:11:59.900000Z\tPEHN\t1\t1\tSXUX\t1\n" +
                            "2022-06-01T14:23:59.800000Z\tVTJW\t2\t2\t\t2\n" +
                            "2022-06-01T21:35:59.700000Z\t\t3\t3\tSXUX\t3\n" +
                            "2022-06-02T04:47:59.600000Z\t\t4\t4\t\t4\n" +
                            "2022-06-02T11:59:59.500000Z\t\t5\t5\tGPGW\t5\n" +
                            "2022-06-02T19:11:59.400000Z\tPEHN\t6\t6\tRXGZ\t6\n" +
                            "2022-06-03T02:23:59.300000Z\tCPSW\t7\t7\t\t7\n" +
                            "2022-06-03T09:35:59.200000Z\t\t8\t8\t\t8\n" +
                            "2022-06-03T16:47:59.100000Z\tPEHN\t9\t9\tRXGZ\t9\n" +
                            "2022-06-03T23:59:59.000000Z\tVTJW\t10\t10\tIBBT\t10\n",
                    tableName
            );
        });
    }

    @Test
    public void testDetachAttachPartitionPingPongConcurrent() throws Throwable {
        ConcurrentLinkedQueue<Throwable> exceptions = new ConcurrentLinkedQueue<>();
        assertMemoryLeak(() -> {
            String tableName = "tabPingPong";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    tab.timestamp("ts", timestampType.getTimestampType())
                            .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG)
                            .col("s2", ColumnType.SYMBOL)
                            .col("vch", ColumnType.VARCHAR),
                    10,
                    "2022-06-01",
                    3
            );


            CyclicBarrier start = new CyclicBarrier(2);
            CountDownLatch end = new CountDownLatch(2);
            AtomicBoolean isLive = new AtomicBoolean(true);
            AtomicInteger failureCounter = new AtomicInteger();
            Set<Long> detachedPartitionTimestamps = new ConcurrentSkipListSet<>();
            AtomicInteger detachedCount = new AtomicInteger();
            AtomicInteger attachedCount = new AtomicInteger();
            Rnd rnd = TestUtils.generateRandom(LOG);

            Thread detThread = new Thread(() -> {
                try {
                    TestUtils.unchecked(() -> {
                        start.await();
                        TimestampDriver driver = timestampType.getDriver();
                        while (isLive.get()) {
                            try (TableWriter writer = getWriter(tableName)) {
                                long partitionTimestamp = (rnd.nextInt() % writer.getPartitionCount()) * driver.fromDays(1);
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
                    }, failureCounter);
                } finally {
                    Path.clearThreadLocals();
                    end.countDown();
                }
            });
            detThread.start();

            Thread attThread = new Thread(() -> {
                try {
                    TestUtils.unchecked(() -> {
                        start.await();
                        while (isLive.get() || !detachedPartitionTimestamps.isEmpty()) {
                            if (!detachedPartitionTimestamps.isEmpty()) {
                                Iterator<Long> timestamps = detachedPartitionTimestamps.iterator();
                                if (timestamps.hasNext()) {
                                    long partitionTimestamp = timestamps.next();
                                    try (TableWriter writer = getWriter(tableName)) {
                                        renameDetachedToAttachable(tableName, TableUtils.getTimestampType(tab), partitionTimestamp);
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
                    }, failureCounter);
                } finally {
                    Path.clearThreadLocals();
                    end.countDown();
                }
            });
            attThread.start();
            isLive.set(false);
            end.await();
            for (int i = 0, limit = exceptions.size(); i < limit; i++) {
                Assert.assertTrue(exceptions.poll() instanceof EntryUnavailableException);
            }
            Assert.assertEquals(detachedCount.get(), attachedCount.get() + detachedPartitionTimestamps.size());
        });
    }

    @Test
    public void testDetachAttachSameDrive() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_ATTACH_PARTITION_COPY, true);
            String tableName = "tabDetachAttachMissingMeta";
            node1.setProperty(
                    PropertyKey.CAIRO_ATTACH_PARTITION_SUFFIX,
                    DETACHED_DIR_MARKER
            );
            ff = new TestFilesFacadeImpl() {
                @Override
                public int hardLinkDirRecursive(Path src, Path dst, int dirMode) {
                    return 100018;
                }

                public boolean isCrossDeviceCopyError(int errno) {
                    return true;
                }
            };

            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    tab
                            .timestamp("ts", timestampType.getTimestampType())
                            .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG)
                            .col("s2", ColumnType.SYMBOL),
                    10,
                    "2022-06-01",
                    3
            );
            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01', '2022-06-02'");
            assertSql(
                    replaceTimestampSuffix("first\tts\n" +
                            "2022-06-03T02:23:59.300000Z\t2022-06-03T02:23:59.300000Z\n"), "select first(ts), ts from " + tableName + " sample by 1d ALIGN TO FIRST OBSERVATION"
            );

            for (int i = 0; i < 2; i++) {
                execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01', '2022-06-02'");
                assertSql(
                        replaceTimestampSuffix("first\tts\n" +
                                "2022-06-01T07:11:59.900000Z\t2022-06-01T07:11:59.900000Z\n" +
                                "2022-06-02T11:59:59.500000Z\t2022-06-02T07:11:59.900000Z\n" +
                                "2022-06-03T09:35:59.200000Z\t2022-06-03T07:11:59.900000Z\n"), "select first(ts), ts from " + tableName + " sample by 1d ALIGN TO FIRST OBSERVATION"
                );
                execute("ALTER TABLE " + tableName + " DROP PARTITION LIST '2022-06-01', '2022-06-02'");

            }
        });
    }

    @Test
    public void testDetachAttachSplitPartition() throws Exception {
        assertMemoryLeak(
                () -> {
                    String tableName = "testDetachAttachSplitPartition";
                    Overrides overrides = node1.getConfigurationOverrides();
                    overrides.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 300);
                    TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
                    TableToken token = createPopulateTable(
                            1,
                            tab.timestamp("ts", timestampType.getTimestampType())
                                    .col("si", ColumnType.SYMBOL).indexed(true, 250)
                                    .col("i", ColumnType.INT)
                                    .col("l", ColumnType.LONG)
                                    .col("s", ColumnType.SYMBOL),
                            1000,
                            "2022-06-01",
                            2
                    );

                    assertSql(replaceTimestampSuffix("count\tmin\tmax\n" +
                            "500\t2022-06-01T00:02:52.799000Z\t2022-06-01T23:59:59.500000Z\n"), "select count(1), min(ts), max(ts) from " + tableName + " where ts in '2022-06-01'");
                    assertSql(replaceTimestampSuffix("count\tmin\tmax\n" +
                            "500\t2022-06-02T00:02:52.299000Z\t2022-06-02T23:59:59.000000Z\n"), "select count(1), min(ts), max(ts) from " + tableName + " where ts in '2022-06-02'");

                    try (TableReader ignore = getReader(token)) {
                        // Split partition by committing O3 to "2022-06-01"
                        execute("insert into " + tableName + "(ts) select ts + 20 * 60 * 60 * " + (ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? "1000000L" : "1000000000L") + " from " + tableName, sqlExecutionContext);

                        Path path = Path.getThreadLocal(configuration.getDbRoot()).concat(token).concat(ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? "2022-06-01T200057-183001.1" : "2022-06-01T200057-183000001.1").concat("ts.d");
                        FilesFacade ff = configuration.getFilesFacade();
                        Assert.assertTrue(ff.exists(path.$()));

                        execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01'", sqlExecutionContext);
                    }

                    renameDetachedToAttachable(tableName, "2022-06-01");
                    execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01'", sqlExecutionContext);
                    assertSql(replaceTimestampSuffix("min\n" +
                            "2022-06-01T00:02:52.799000Z\n"), "select min(ts) from " + tableName);
                });
    }

    @Test
    public void testDetachNonPartitionedNotAllowed() throws Exception {
        assertMemoryLeak(
                () -> {
                    String tableName = "tab";
                    TableModel tab = new TableModel(configuration, tableName, PartitionBy.NONE);
                    createPopulateTable(
                            tab
                                    .timestamp("ts", timestampType.getTimestampType())
                                    .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                                    .col("i", ColumnType.INT)
                                    .col("l", ColumnType.LONG)
                                    .col("s2", ColumnType.SYMBOL),
                            10,
                            "2022-06-01",
                            1
                    );
                    assertFailure(
                            "ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01', '2022-06-02'",
                            "table is not partitioned"
                    );
                });
    }

    @Test
    public void testDetachPartitionCommits() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tab";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    1,
                    tab.col("l", ColumnType.LONG)
                            .col("i", ColumnType.INT)
                            .col("vch", ColumnType.VARCHAR)
                            .timestamp("ts", timestampType.getTimestampType()),
                    5,
                    "2022-06-01",
                    4
            );

            TimestampDriver driver = timestampType.getDriver();
            String timestampDay = "2022-06-02";
            try (TableWriter writer = getWriter(tableName)) {
                // structural change
                writer.addColumn("new_column", ColumnType.INT);

                TableWriter.Row row = writer.newRow(driver.parseFloorLiteral("2022-05-03T12:00:00.000000Z"));
                row.putLong(0, 33L);
                row.putInt(1, 33);
                row.append();

                Assert.assertEquals(AttachDetachStatus.OK, writer.detachPartition((driver.parseFloorLiteral(timestampDay))));
            }

            renameDetachedToAttachable(tableName, timestampDay);
            execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + timestampDay + "'", sqlExecutionContext);

            // attach the partition
            assertContent(
                    "l\ti\tvch\tts\tnew_column\n" +
                            "33\t33\t\t2022-05-03T12:00:00.000000Z\tnull\n" +
                            "1\t1\t&\uDA1F\uDE98|\uD924\uDE04\t2022-06-01T19:11:59.800000Z\tnull\n" +
                            "2\t2\t\t2022-06-02T14:23:59.600000Z\tnull\n" +
                            "3\t3\těȞ鼷G\uD991\uDE7E\t2022-06-03T09:35:59.400000Z\tnull\n" +
                            "4\t4\t\t2022-06-04T04:47:59.200000Z\tnull\n" +
                            "5\t5\t͛Ԉ龘и\uDA89\uDFA4~\t2022-06-04T23:59:59.000000Z\tnull\n",
                    tableName
            );
        });
    }

    @Test
    public void testDetachPartitionIndexFilesGetIndexed() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabIndexFilesIndex";
            String brokenTableName = "tabIndexFilesIndex2";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            TableModel brokenMeta = new TableModel(configuration, brokenTableName, PartitionBy.DAY);
            try (
                    MemoryMARW mem = Vm.getCMARWInstance()
            ) {
                String timestampDay = "2022-06-01";
                createPopulateTable(
                        1,
                        tab.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("s", ColumnType.SYMBOL).indexed(true, 512)
                                .col("vch", ColumnType.VARCHAR)
                                .timestamp("ts", timestampType.getTimestampType()),
                        12,
                        timestampDay,
                        4
                );

                TestUtils.createTable(
                        engine,
                        mem,
                        path,
                        brokenMeta.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("s", ColumnType.SYMBOL)
                                .col("vch", ColumnType.VARCHAR)
                                .timestamp("ts", timestampType.getTimestampType()),
                        1,
                        brokenMeta.getTableName()
                );
                execute("INSERT INTO " + brokenMeta.getName() + " SELECT * FROM " + tab.getName());

                TimestampDriver driver = timestampType.getDriver();
                long timestamp = driver.parseFloorLiteral(timestampDay + "T00:00:00.000000Z");
                try (TableWriter writer = getWriter(brokenTableName)) {
                    writer.detachPartition(timestamp);
                }
                try (TableWriter writer = getWriter(tableName)) {
                    writer.detachPartition(timestamp);
                }
                TableToken tableToken = engine.verifyTableName(brokenTableName);
                TableToken tableToken1 = engine.verifyTableName(tableName);

                path.of(configuration.getDbRoot()).concat(tableToken).concat(timestampDay).put(DETACHED_DIR_MARKER).$();
                other.of(configuration.getDbRoot()).concat(tableToken1).concat(timestampDay).put(configuration.getAttachPartitionSuffix()).$();

                Assert.assertTrue(Files.rename(path.$(), other.$()) > -1);

                try (TableWriter writer = getWriter(tableName)) {
                    writer.attachPartition(timestamp);
                }

                assertContent(
                        ColumnType.isTimestampMicro(timestampType.getTimestampType()) ?
                                "l\ti\ts\tvch\tts\n" +
                                        "1\t1\tCPSW\těȞ鼷G\uD991\uDE7E\t2022-06-01T07:59:59.916666Z\n" +
                                        "2\t2\t\t͛Ԉ龘и\uDA89\uDFA4~\t2022-06-01T15:59:59.833332Z\n" +
                                        "3\t3\tPEHN\t\uF2C1ӍKB\t2022-06-01T23:59:59.749998Z\n" +
                                        "4\t4\tPEHN\tK䰭\t2022-06-02T07:59:59.666664Z\n" +
                                        "5\t5\tHYRX\tѱʜ\uDB8D\uDE4Eᯤ\\篸{\t2022-06-02T15:59:59.583330Z\n" +
                                        "6\t6\tCPSW\tl\";&=RON\t2022-06-02T23:59:59.499996Z\n" +
                                        "7\t7\tVTJW\t\t2022-06-03T07:59:59.416662Z\n" +
                                        "8\t8\tPEHN\t\uDBAE\uDD12ɜ|\\軦۽\t2022-06-03T15:59:59.333328Z\n" +
                                        "9\t9\tPEHN\t7=\t2022-06-03T23:59:59.249994Z\n" +
                                        "10\t10\tHYRX\t\t2022-06-04T07:59:59.166660Z\n" +
                                        "11\t11\tPEHN\txL?49M\t2022-06-04T15:59:59.083326Z\n" +
                                        "12\t12\tCPSW\t鳓\t2022-06-04T23:59:58.999992Z\n"
                                : "l\ti\ts\tvch\tts\n" +
                                "1\t1\tCPSW\těȞ鼷G\uD991\uDE7E\t2022-06-01T07:59:59.916666666Z\n" +
                                "2\t2\t\t͛Ԉ龘и\uDA89\uDFA4~\t2022-06-01T15:59:59.833333332Z\n" +
                                "3\t3\tPEHN\t\uF2C1ӍKB\t2022-06-01T23:59:59.749999998Z\n" +
                                "4\t4\tPEHN\tK䰭\t2022-06-02T07:59:59.666666664Z\n" +
                                "5\t5\tHYRX\tѱʜ\uDB8D\uDE4Eᯤ\\篸{\t2022-06-02T15:59:59.583333330Z\n" +
                                "6\t6\tCPSW\tl\";&=RON\t2022-06-02T23:59:59.499999996Z\n" +
                                "7\t7\tVTJW\t\t2022-06-03T07:59:59.416666662Z\n" +
                                "8\t8\tPEHN\t\uDBAE\uDD12ɜ|\\軦۽\t2022-06-03T15:59:59.333333328Z\n" +
                                "9\t9\tPEHN\t7=\t2022-06-03T23:59:59.249999994Z\n" +
                                "10\t10\tHYRX\t\t2022-06-04T07:59:59.166666660Z\n" +
                                "11\t11\tPEHN\txL?49M\t2022-06-04T15:59:59.083333326Z\n" +
                                "12\t12\tCPSW\t鳓\t2022-06-04T23:59:58.999999992Z\n",
                        tableName
                );
            }
        });
    }

    @Test
    public void testDetachPartitionIndexFilesGetReIndexed() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabIndexFilesReIndex";
            String brokenTableName = "tabIndexFilesReIndex2";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            TableModel brokenMeta = new TableModel(configuration, brokenTableName, PartitionBy.DAY);

            try (
                    MemoryMARW mem = Vm.getCMARWInstance()
            ) {
                String timestampDay = "2022-06-01";
                createPopulateTable(
                        1,
                        tab.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("s", ColumnType.SYMBOL).indexed(true, 512)
                                .col("vch", ColumnType.VARCHAR)
                                .timestamp("ts", timestampType.getTimestampType()),
                        12,
                        timestampDay,
                        4
                );


                TestUtils.createTable(
                        engine,
                        mem,
                        path,
                        brokenMeta.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("s", ColumnType.SYMBOL).indexed(true, 32)
                                .col("vch", ColumnType.VARCHAR)
                                .timestamp("ts", timestampType.getTimestampType()),
                        1,
                        brokenMeta.getTableName()
                );
                execute("INSERT INTO " + brokenMeta.getName() + " SELECT * FROM " + tab.getName());

                TimestampDriver driver = timestampType.getDriver();
                long timestamp = driver.parseFloorLiteral(timestampDay + "T00:00:00.000000Z");
                try (TableWriter writer = getWriter(brokenTableName)) {
                    writer.detachPartition(timestamp);
                }
                try (TableWriter writer = getWriter(tableName)) {
                    writer.detachPartition(timestamp);
                }

                TableToken tableToken = engine.verifyTableName(brokenTableName);
                TableToken tableToken1 = engine.verifyTableName(tableName);

                path.of(configuration.getDbRoot()).concat(tableToken).concat(timestampDay).put(DETACHED_DIR_MARKER).$();
                other.of(configuration.getDbRoot()).concat(tableToken1).concat(timestampDay).put(configuration.getAttachPartitionSuffix()).$();

                Assert.assertTrue(Files.rename(path.$(), other.$()) > -1);

                try (TableWriter writer = getWriter(tableName)) {
                    writer.attachPartition(timestamp);
                }

                assertContent(
                        ColumnType.isTimestampMicro(timestampType.getTimestampType()) ?
                                "l\ti\ts\tvch\tts\n" +
                                        "1\t1\tCPSW\těȞ鼷G\uD991\uDE7E\t2022-06-01T07:59:59.916666Z\n" +
                                        "2\t2\t\t͛Ԉ龘и\uDA89\uDFA4~\t2022-06-01T15:59:59.833332Z\n" +
                                        "3\t3\tPEHN\t\uF2C1ӍKB\t2022-06-01T23:59:59.749998Z\n" +
                                        "4\t4\tPEHN\tK䰭\t2022-06-02T07:59:59.666664Z\n" +
                                        "5\t5\tHYRX\tѱʜ\uDB8D\uDE4Eᯤ\\篸{\t2022-06-02T15:59:59.583330Z\n" +
                                        "6\t6\tCPSW\tl\";&=RON\t2022-06-02T23:59:59.499996Z\n" +
                                        "7\t7\tVTJW\t\t2022-06-03T07:59:59.416662Z\n" +
                                        "8\t8\tPEHN\t\uDBAE\uDD12ɜ|\\軦۽\t2022-06-03T15:59:59.333328Z\n" +
                                        "9\t9\tPEHN\t7=\t2022-06-03T23:59:59.249994Z\n" +
                                        "10\t10\tHYRX\t\t2022-06-04T07:59:59.166660Z\n" +
                                        "11\t11\tPEHN\txL?49M\t2022-06-04T15:59:59.083326Z\n" +
                                        "12\t12\tCPSW\t鳓\t2022-06-04T23:59:58.999992Z\n"
                                : "l\ti\ts\tvch\tts\n" +
                                "1\t1\tCPSW\těȞ鼷G\uD991\uDE7E\t2022-06-01T07:59:59.916666666Z\n" +
                                "2\t2\t\t͛Ԉ龘и\uDA89\uDFA4~\t2022-06-01T15:59:59.833333332Z\n" +
                                "3\t3\tPEHN\t\uF2C1ӍKB\t2022-06-01T23:59:59.749999998Z\n" +
                                "4\t4\tPEHN\tK䰭\t2022-06-02T07:59:59.666666664Z\n" +
                                "5\t5\tHYRX\tѱʜ\uDB8D\uDE4Eᯤ\\篸{\t2022-06-02T15:59:59.583333330Z\n" +
                                "6\t6\tCPSW\tl\";&=RON\t2022-06-02T23:59:59.499999996Z\n" +
                                "7\t7\tVTJW\t\t2022-06-03T07:59:59.416666662Z\n" +
                                "8\t8\tPEHN\t\uDBAE\uDD12ɜ|\\軦۽\t2022-06-03T15:59:59.333333328Z\n" +
                                "9\t9\tPEHN\t7=\t2022-06-03T23:59:59.249999994Z\n" +
                                "10\t10\tHYRX\t\t2022-06-04T07:59:59.166666660Z\n" +
                                "11\t11\tPEHN\txL?49M\t2022-06-04T15:59:59.083333326Z\n" +
                                "12\t12\tCPSW\t鳓\t2022-06-04T23:59:58.999999992Z\n",
                        tableName
                );
            }
        });
    }

    @Test
    public void testDetachPartitionIndexFilesGetRemoved() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabIndexFiles";
            String brokenTableName = "tabIndexFiles2";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            TableModel brokenMeta = new TableModel(configuration, brokenTableName, PartitionBy.DAY);

            try (
                    MemoryMARW mem = Vm.getCMARWInstance()
            ) {
                String timestampDay = "2022-06-01";
                createPopulateTable(
                        1,
                        tab.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("s", ColumnType.SYMBOL)
                                .timestamp("ts", timestampType.getTimestampType()),
                        12,
                        timestampDay,
                        4
                );

                TestUtils.createTable(
                        engine,
                        mem,
                        path,
                        brokenMeta.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("s", ColumnType.SYMBOL).indexed(true, 32)
                                .timestamp("ts", timestampType.getTimestampType()),
                        3,
                        brokenMeta.getTableName()
                );

                execute("INSERT INTO " + brokenMeta.getName() + " SELECT * FROM " + tab.getName());

                String expected = ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? "l\ti\ts\tts\n" +
                        "1\t1\tCPSW\t2022-06-01T07:59:59.916666Z\n" +
                        "2\t2\tHYRX\t2022-06-01T15:59:59.833332Z\n" +
                        "3\t3\t\t2022-06-01T23:59:59.749998Z\n" +
                        "4\t4\tVTJW\t2022-06-02T07:59:59.666664Z\n" +
                        "5\t5\tPEHN\t2022-06-02T15:59:59.583330Z\n" +
                        "6\t6\t\t2022-06-02T23:59:59.499996Z\n" +
                        "7\t7\tVTJW\t2022-06-03T07:59:59.416662Z\n" +
                        "8\t8\t\t2022-06-03T15:59:59.333328Z\n" +
                        "9\t9\tCPSW\t2022-06-03T23:59:59.249994Z\n" +
                        "10\t10\t\t2022-06-04T07:59:59.166660Z\n" +
                        "11\t11\tPEHN\t2022-06-04T15:59:59.083326Z\n" +
                        "12\t12\tCPSW\t2022-06-04T23:59:58.999992Z\n"
                        : "l\ti\ts\tts\n" +
                        "1\t1\tCPSW\t2022-06-01T07:59:59.916666666Z\n" +
                        "2\t2\tHYRX\t2022-06-01T15:59:59.833333332Z\n" +
                        "3\t3\t\t2022-06-01T23:59:59.749999998Z\n" +
                        "4\t4\tVTJW\t2022-06-02T07:59:59.666666664Z\n" +
                        "5\t5\tPEHN\t2022-06-02T15:59:59.583333330Z\n" +
                        "6\t6\t\t2022-06-02T23:59:59.499999996Z\n" +
                        "7\t7\tVTJW\t2022-06-03T07:59:59.416666662Z\n" +
                        "8\t8\t\t2022-06-03T15:59:59.333333328Z\n" +
                        "9\t9\tCPSW\t2022-06-03T23:59:59.249999994Z\n" +
                        "10\t10\t\t2022-06-04T07:59:59.166666660Z\n" +
                        "11\t11\tPEHN\t2022-06-04T15:59:59.083333326Z\n" +
                        "12\t12\tCPSW\t2022-06-04T23:59:58.999999992Z\n";

                assertContent(expected, tableName);
                assertContent(expected, brokenTableName);

                TimestampDriver driver = timestampType.getDriver();
                long timestamp = driver.parseFloorLiteral(timestampDay + "T00:00:00.000000Z");
                try (TableWriter writer = getWriter(brokenTableName)) {
                    writer.detachPartition(timestamp);
                }
                try (TableWriter writer = getWriter(tableName)) {
                    writer.detachPartition(timestamp);
                }

                TableToken tableToken = engine.verifyTableName(tableName);
                TableToken brokenTableToken = engine.verifyTableName(brokenTableName);

                path.of(configuration.getDbRoot()).concat(brokenTableToken).concat(timestampDay).put(DETACHED_DIR_MARKER).$();
                other.of(configuration.getDbRoot()).concat(tableToken).concat(timestampDay).put(configuration.getAttachPartitionSuffix()).$();
                Assert.assertTrue(Files.rename(path.$(), other.$()) > -1);

                // Change table id in the metadata file in the partition
                other.concat(META_FILE_NAME).$();
                try (MemoryCMARW mem2 = Vm.getCMARWInstance()) {
                    mem2.smallFile(configuration.getFilesFacade(), other.$(), MemoryTag.NATIVE_DEFAULT);
                    mem2.putInt(META_OFFSET_TABLE_ID, tableToken.getTableId());
                }

                try (TableWriter writer = getWriter(tableName)) {
                    writer.attachPartition(timestamp);
                }

                Assert.assertFalse(Files.exists(other.of(configuration.getDbRoot()).concat(tableToken).concat(timestampDay).concat("s.k").$()));
                Assert.assertFalse(Files.exists(other.parent().concat("s.v").$()));

                assertContent(expected, tableName);
            }
        });
    }

    @Test
    public void testDetachPartitionLongerName() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tab";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    1,
                    tab.col("l", ColumnType.LONG)
                            .col("i", ColumnType.INT)
                            .timestamp("ts", timestampType.getTimestampType()),
                    5,
                    "2022-06-01",
                    4
            );

            TimestampDriver driver = timestampType.getDriver();
            String timestampDay = "2022-06-02";
            try (TableWriter writer = getWriter(tableName)) {
                Assert.assertEquals(AttachDetachStatus.OK, writer.detachPartition((driver.parseFloorLiteral(timestampDay))));
            }
            renameDetachedToAttachable(tableName, timestampDay);
            execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + timestampDay + "T23:59:59.000000Z'", sqlExecutionContext);
            assertContent(
                    "l\ti\tts\n" +
                            "1\t1\t2022-06-01T19:11:59.800000Z\n" +
                            "2\t2\t2022-06-02T14:23:59.600000Z\n" +
                            "3\t3\t2022-06-03T09:35:59.400000Z\n" +
                            "4\t4\t2022-06-04T04:47:59.200000Z\n" +
                            "5\t5\t2022-06-04T23:59:59.000000Z\n",
                    tableName
            );
        });
    }

    @Test
    public void testDetachPartitionsColumnTops() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabColumnTops";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    1,
                    tab.col("l", ColumnType.LONG)
                            .col("i", ColumnType.INT)
                            .col("vch", ColumnType.VARCHAR)
                            .timestamp("ts", timestampType.getTimestampType()),
                    12,
                    "2022-06-01",
                    4
            );

            TimestampDriver driver = timestampType.getDriver();
            String timestampDay = "2022-06-02";
            long timestamp = driver.parseFloorLiteral(timestampDay + "T22:00:00.000000Z");
            Utf8StringSink utf8sink = new Utf8StringSink();
            utf8sink.put("33");
            try (TableWriter writer = getWriter(tableName)) {
                // structural change
                writer.addColumn("new_column", ColumnType.INT);

                TableWriter.Row row = writer.newRow(timestamp);
                row.putLong(0, 33L);
                row.putInt(1, 33);
                row.putVarchar(2, utf8sink);
                row.putInt(4, 33);
                row.append();

                writer.commit();
            }
            assertContent(
                    ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? "l\ti\tvch\tts\tnew_column\n" +
                            "1\t1\t&\uDA1F\uDE98|\uD924\uDE04\t2022-06-01T07:59:59.916666Z\tnull\n" +
                            "2\t2\t\t2022-06-01T15:59:59.833332Z\tnull\n" +
                            "3\t3\těȞ鼷G\uD991\uDE7E\t2022-06-01T23:59:59.749998Z\tnull\n" +
                            "4\t4\t\t2022-06-02T07:59:59.666664Z\tnull\n" +
                            "5\t5\t͛Ԉ龘и\uDA89\uDFA4~\t2022-06-02T15:59:59.583330Z\tnull\n" +
                            "33\t33\t33\t2022-06-02T22:00:00.000000Z\t33\n" +
                            "6\t6\tṟ\u1AD3ڎB\t2022-06-02T23:59:59.499996Z\tnull\n" +
                            "7\t7\tqK䰭\u008B}ѱʜ\uDB8D\uDE4Eᯤ\\篸\t2022-06-03T07:59:59.416662Z\tnull\n" +
                            "8\t8\t䇜\"\t2022-06-03T15:59:59.333328Z\tnull\n" +
                            "9\t9\t\t2022-06-03T23:59:59.249994Z\tnull\n" +
                            "10\t10\t\t2022-06-04T07:59:59.166660Z\tnull\n" +
                            "11\t11\t(OFг\uDBAE\uDD12ɜ|\\軦۽㒾\t2022-06-04T15:59:59.083326Z\tnull\n" +
                            "12\t12\t\"+zM\t2022-06-04T23:59:58.999992Z\tnull\n"
                            : "l\ti\tvch\tts\tnew_column\n" +
                            "1\t1\t&\uDA1F\uDE98|\uD924\uDE04\t2022-06-01T07:59:59.916666666Z\tnull\n" +
                            "2\t2\t\t2022-06-01T15:59:59.833333332Z\tnull\n" +
                            "3\t3\těȞ鼷G\uD991\uDE7E\t2022-06-01T23:59:59.749999998Z\tnull\n" +
                            "4\t4\t\t2022-06-02T07:59:59.666666664Z\tnull\n" +
                            "5\t5\t͛Ԉ龘и\uDA89\uDFA4~\t2022-06-02T15:59:59.583333330Z\tnull\n" +
                            "33\t33\t33\t2022-06-02T22:00:00.000000Z\t33\n" +
                            "6\t6\tṟ\u1AD3ڎB\t2022-06-02T23:59:59.499999996Z\tnull\n" +
                            "7\t7\tqK䰭\u008B}ѱʜ\uDB8D\uDE4Eᯤ\\篸\t2022-06-03T07:59:59.416666662Z\tnull\n" +
                            "8\t8\t䇜\"\t2022-06-03T15:59:59.333333328Z\tnull\n" +
                            "9\t9\t\t2022-06-03T23:59:59.249999994Z\tnull\n" +
                            "10\t10\t\t2022-06-04T07:59:59.166666660Z\tnull\n" +
                            "11\t11\t(OFг\uDBAE\uDD12ɜ|\\軦۽㒾\t2022-06-04T15:59:59.083333326Z\tnull\n" +
                            "12\t12\t\"+zM\t2022-06-04T23:59:58.999999992Z\tnull\n",
                    tableName
            );

            // detach the partition
            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '" + timestampDay + "'", sqlExecutionContext);

            assertContent(
                    ColumnType.isTimestampMicro(timestampType.getTimestampType())
                            ? "l\ti\tvch\tts\tnew_column\n" +
                            "1\t1\t&\uDA1F\uDE98|\uD924\uDE04\t2022-06-01T07:59:59.916666Z\tnull\n" +
                            "2\t2\t\t2022-06-01T15:59:59.833332Z\tnull\n" +
                            "3\t3\těȞ鼷G\uD991\uDE7E\t2022-06-01T23:59:59.749998Z\tnull\n" +
                            "7\t7\tqK䰭\u008B}ѱʜ\uDB8D\uDE4Eᯤ\\篸\t2022-06-03T07:59:59.416662Z\tnull\n" +
                            "8\t8\t䇜\"\t2022-06-03T15:59:59.333328Z\tnull\n" +
                            "9\t9\t\t2022-06-03T23:59:59.249994Z\tnull\n" +
                            "10\t10\t\t2022-06-04T07:59:59.166660Z\tnull\n" +
                            "11\t11\t(OFг\uDBAE\uDD12ɜ|\\軦۽㒾\t2022-06-04T15:59:59.083326Z\tnull\n" +
                            "12\t12\t\"+zM\t2022-06-04T23:59:58.999992Z\tnull\n"
                            : "l\ti\tvch\tts\tnew_column\n" +
                            "1\t1\t&\uDA1F\uDE98|\uD924\uDE04\t2022-06-01T07:59:59.916666666Z\tnull\n" +
                            "2\t2\t\t2022-06-01T15:59:59.833333332Z\tnull\n" +
                            "3\t3\těȞ鼷G\uD991\uDE7E\t2022-06-01T23:59:59.749999998Z\tnull\n" +
                            "7\t7\tqK䰭\u008B}ѱʜ\uDB8D\uDE4Eᯤ\\篸\t2022-06-03T07:59:59.416666662Z\tnull\n" +
                            "8\t8\t䇜\"\t2022-06-03T15:59:59.333333328Z\tnull\n" +
                            "9\t9\t\t2022-06-03T23:59:59.249999994Z\tnull\n" +
                            "10\t10\t\t2022-06-04T07:59:59.166666660Z\tnull\n" +
                            "11\t11\t(OFг\uDBAE\uDD12ɜ|\\軦۽㒾\t2022-06-04T15:59:59.083333326Z\tnull\n" +
                            "12\t12\t\"+zM\t2022-06-04T23:59:58.999999992Z\tnull\n",
                    tableName
            );

            // insert data, which will create the partition again
            engine.clear();
            utf8sink.clear();
            utf8sink.put("25160");
            try (TableWriter writer = getWriter(tableName)) {
                TableWriter.Row row = writer.newRow(timestamp);
                row.putLong(0, 25160L);
                row.putInt(1, 25160);
                row.putVarchar(2, utf8sink);
                row.putInt(4, 25160);
                row.append();
                writer.commit();
            }
            assertContent(
                    ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? "l\ti\tvch\tts\tnew_column\n" +
                            "1\t1\t&\uDA1F\uDE98|\uD924\uDE04\t2022-06-01T07:59:59.916666Z\tnull\n" +
                            "2\t2\t\t2022-06-01T15:59:59.833332Z\tnull\n" +
                            "3\t3\těȞ鼷G\uD991\uDE7E\t2022-06-01T23:59:59.749998Z\tnull\n" +
                            "25160\t25160\t25160\t2022-06-02T22:00:00.000000Z\t25160\n" +
                            "7\t7\tqK䰭\u008B}ѱʜ\uDB8D\uDE4Eᯤ\\篸\t2022-06-03T07:59:59.416662Z\tnull\n" +
                            "8\t8\t䇜\"\t2022-06-03T15:59:59.333328Z\tnull\n" +
                            "9\t9\t\t2022-06-03T23:59:59.249994Z\tnull\n" +
                            "10\t10\t\t2022-06-04T07:59:59.166660Z\tnull\n" +
                            "11\t11\t(OFг\uDBAE\uDD12ɜ|\\軦۽㒾\t2022-06-04T15:59:59.083326Z\tnull\n" +
                            "12\t12\t\"+zM\t2022-06-04T23:59:58.999992Z\tnull\n"
                            : "l\ti\tvch\tts\tnew_column\n" +
                            "1\t1\t&\uDA1F\uDE98|\uD924\uDE04\t2022-06-01T07:59:59.916666666Z\tnull\n" +
                            "2\t2\t\t2022-06-01T15:59:59.833333332Z\tnull\n" +
                            "3\t3\těȞ鼷G\uD991\uDE7E\t2022-06-01T23:59:59.749999998Z\tnull\n" +
                            "25160\t25160\t25160\t2022-06-02T22:00:00.000000Z\t25160\n" +
                            "7\t7\tqK䰭\u008B}ѱʜ\uDB8D\uDE4Eᯤ\\篸\t2022-06-03T07:59:59.416666662Z\tnull\n" +
                            "8\t8\t䇜\"\t2022-06-03T15:59:59.333333328Z\tnull\n" +
                            "9\t9\t\t2022-06-03T23:59:59.249999994Z\tnull\n" +
                            "10\t10\t\t2022-06-04T07:59:59.166666660Z\tnull\n" +
                            "11\t11\t(OFг\uDBAE\uDD12ɜ|\\軦۽㒾\t2022-06-04T15:59:59.083333326Z\tnull\n" +
                            "12\t12\t\"+zM\t2022-06-04T23:59:58.999999992Z\tnull\n",
                    tableName
            );

            dropCurrentVersionOfPartition(tableName, timestampDay);
            renameDetachedToAttachable(tableName, timestampDay);

            // reattach old version
            execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + timestampDay + "'", sqlExecutionContext);
            assertContent(
                    ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? "l\ti\tvch\tts\tnew_column\n" +
                            "1\t1\t&\uDA1F\uDE98|\uD924\uDE04\t2022-06-01T07:59:59.916666Z\tnull\n" +
                            "2\t2\t\t2022-06-01T15:59:59.833332Z\tnull\n" +
                            "3\t3\těȞ鼷G\uD991\uDE7E\t2022-06-01T23:59:59.749998Z\tnull\n" +
                            "4\t4\t\t2022-06-02T07:59:59.666664Z\tnull\n" +
                            "5\t5\t͛Ԉ龘и\uDA89\uDFA4~\t2022-06-02T15:59:59.583330Z\tnull\n" +
                            "33\t33\t33\t2022-06-02T22:00:00.000000Z\t33\n" +
                            "6\t6\tṟ\u1AD3ڎB\t2022-06-02T23:59:59.499996Z\tnull\n" +
                            "7\t7\tqK䰭\u008B}ѱʜ\uDB8D\uDE4Eᯤ\\篸\t2022-06-03T07:59:59.416662Z\tnull\n" +
                            "8\t8\t䇜\"\t2022-06-03T15:59:59.333328Z\tnull\n" +
                            "9\t9\t\t2022-06-03T23:59:59.249994Z\tnull\n" +
                            "10\t10\t\t2022-06-04T07:59:59.166660Z\tnull\n" +
                            "11\t11\t(OFг\uDBAE\uDD12ɜ|\\軦۽㒾\t2022-06-04T15:59:59.083326Z\tnull\n" +
                            "12\t12\t\"+zM\t2022-06-04T23:59:58.999992Z\tnull\n"
                            : "l\ti\tvch\tts\tnew_column\n" +
                            "1\t1\t&\uDA1F\uDE98|\uD924\uDE04\t2022-06-01T07:59:59.916666666Z\tnull\n" +
                            "2\t2\t\t2022-06-01T15:59:59.833333332Z\tnull\n" +
                            "3\t3\těȞ鼷G\uD991\uDE7E\t2022-06-01T23:59:59.749999998Z\tnull\n" +
                            "4\t4\t\t2022-06-02T07:59:59.666666664Z\tnull\n" +
                            "5\t5\t͛Ԉ龘и\uDA89\uDFA4~\t2022-06-02T15:59:59.583333330Z\tnull\n" +
                            "33\t33\t33\t2022-06-02T22:00:00.000000Z\t33\n" +
                            "6\t6\tṟ\u1AD3ڎB\t2022-06-02T23:59:59.499999996Z\tnull\n" +
                            "7\t7\tqK䰭\u008B}ѱʜ\uDB8D\uDE4Eᯤ\\篸\t2022-06-03T07:59:59.416666662Z\tnull\n" +
                            "8\t8\t䇜\"\t2022-06-03T15:59:59.333333328Z\tnull\n" +
                            "9\t9\t\t2022-06-03T23:59:59.249999994Z\tnull\n" +
                            "10\t10\t\t2022-06-04T07:59:59.166666660Z\tnull\n" +
                            "11\t11\t(OFг\uDBAE\uDD12ɜ|\\軦۽㒾\t2022-06-04T15:59:59.083333326Z\tnull\n" +
                            "12\t12\t\"+zM\t2022-06-04T23:59:58.999999992Z\tnull\n",
                    tableName
            );
        });
    }

    @Test
    public void testDetachPartitionsIndexCapacityDiffers() throws Exception {
        assertFailedAttachBecauseOfMetadata(
                2,
                "tabBrokenIndexCapacity",
                "tabBrokenIndexCapacity2",
                brokenMeta -> brokenMeta
                        .timestamp("ts", timestampType.getTimestampType())
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG),
                "insert into tabBrokenIndexCapacity2 " +
                        "select " +
                        "CAST(1654041600000000L AS TIMESTAMP) + x * 3455990000  ts, " +
                        "cast(x as int) i, " +
                        "x l " +
                        "from long_sequence(100)",
                "ALTER TABLE tabBrokenIndexCapacity2 ADD COLUMN s SHORT",
                "Detached partition metadata [table_id] is not compatible with current table metadata"
        );
    }

    @Test
    public void testDetachPartitionsTableAddColumn() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabInAddColumn";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    1,
                    tab.col("l", ColumnType.LONG)
                            .col("i", ColumnType.INT)
                            .timestamp("ts", timestampType.getTimestampType()),
                    12,
                    "2022-06-01",
                    4
            );

            engine.clear();
            TimestampDriver driver = timestampType.getDriver();
            String timestampDay = "2022-06-01";
            long timestamp = driver.parseFloorLiteral(timestampDay + "T00:00:00.000000Z");
            try (TableWriter writer = getWriter(tableName)) {
                // structural change
                writer.addColumn("new_column", ColumnType.INT);
                writer.detachPartition(timestamp);
                Assert.assertEquals(9, writer.size());
            }

            renameDetachedToAttachable(tableName, timestampDay);

            execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + timestampDay + "'", sqlExecutionContext);
            assertContent(
                    ColumnType.isTimestampMicro(timestampType.getTimestampType()) ?
                            "l\ti\tts\tnew_column\n" +
                                    "1\t1\t2022-06-01T07:59:59.916666Z\tnull\n" +
                                    "2\t2\t2022-06-01T15:59:59.833332Z\tnull\n" +
                                    "3\t3\t2022-06-01T23:59:59.749998Z\tnull\n" +
                                    "4\t4\t2022-06-02T07:59:59.666664Z\tnull\n" +
                                    "5\t5\t2022-06-02T15:59:59.583330Z\tnull\n" +
                                    "6\t6\t2022-06-02T23:59:59.499996Z\tnull\n" +
                                    "7\t7\t2022-06-03T07:59:59.416662Z\tnull\n" +
                                    "8\t8\t2022-06-03T15:59:59.333328Z\tnull\n" +
                                    "9\t9\t2022-06-03T23:59:59.249994Z\tnull\n" +
                                    "10\t10\t2022-06-04T07:59:59.166660Z\tnull\n" +
                                    "11\t11\t2022-06-04T15:59:59.083326Z\tnull\n" +
                                    "12\t12\t2022-06-04T23:59:58.999992Z\tnull\n"
                            : "l\ti\tts\tnew_column\n" +
                            "1\t1\t2022-06-01T07:59:59.916666666Z\tnull\n" +
                            "2\t2\t2022-06-01T15:59:59.833333332Z\tnull\n" +
                            "3\t3\t2022-06-01T23:59:59.749999998Z\tnull\n" +
                            "4\t4\t2022-06-02T07:59:59.666666664Z\tnull\n" +
                            "5\t5\t2022-06-02T15:59:59.583333330Z\tnull\n" +
                            "6\t6\t2022-06-02T23:59:59.499999996Z\tnull\n" +
                            "7\t7\t2022-06-03T07:59:59.416666662Z\tnull\n" +
                            "8\t8\t2022-06-03T15:59:59.333333328Z\tnull\n" +
                            "9\t9\t2022-06-03T23:59:59.249999994Z\tnull\n" +
                            "10\t10\t2022-06-04T07:59:59.166666660Z\tnull\n" +
                            "11\t11\t2022-06-04T15:59:59.083333326Z\tnull\n" +
                            "12\t12\t2022-06-04T23:59:58.999999992Z\tnull\n",
                    tableName
            );
        });
    }

    @Test
    public void testDetachPartitionsTableAddColumn2() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabInAddColumn2";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    1,
                    tab.col("l", ColumnType.LONG)
                            .col("i", ColumnType.INT)
                            .timestamp("ts", timestampType.getTimestampType()),
                    12,
                    "2022-06-01",
                    4
            );

            engine.clear();
            TimestampDriver driver = timestampType.getDriver();
            String timestampDay = "2022-06-01";
            long timestamp = driver.parseFloorLiteral(timestampDay + "T00:00:00.000000Z");
            try (TableWriter writer = getWriter(tableName)) {
                writer.detachPartition(timestamp);
                // structural change
                writer.addColumn("new_column", ColumnType.INT);
                Assert.assertEquals(9, writer.size());
            }

            renameDetachedToAttachable(tableName, timestampDay);

            execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + timestampDay + "'", sqlExecutionContext);
            assertContent(
                    ColumnType.isTimestampMicro(timestampType.getTimestampType()) ?
                            "l\ti\tts\tnew_column\n" +
                                    "1\t1\t2022-06-01T07:59:59.916666Z\tnull\n" +
                                    "2\t2\t2022-06-01T15:59:59.833332Z\tnull\n" +
                                    "3\t3\t2022-06-01T23:59:59.749998Z\tnull\n" +
                                    "4\t4\t2022-06-02T07:59:59.666664Z\tnull\n" +
                                    "5\t5\t2022-06-02T15:59:59.583330Z\tnull\n" +
                                    "6\t6\t2022-06-02T23:59:59.499996Z\tnull\n" +
                                    "7\t7\t2022-06-03T07:59:59.416662Z\tnull\n" +
                                    "8\t8\t2022-06-03T15:59:59.333328Z\tnull\n" +
                                    "9\t9\t2022-06-03T23:59:59.249994Z\tnull\n" +
                                    "10\t10\t2022-06-04T07:59:59.166660Z\tnull\n" +
                                    "11\t11\t2022-06-04T15:59:59.083326Z\tnull\n" +
                                    "12\t12\t2022-06-04T23:59:58.999992Z\tnull\n"
                            : "l\ti\tts\tnew_column\n" +
                            "1\t1\t2022-06-01T07:59:59.916666666Z\tnull\n" +
                            "2\t2\t2022-06-01T15:59:59.833333332Z\tnull\n" +
                            "3\t3\t2022-06-01T23:59:59.749999998Z\tnull\n" +
                            "4\t4\t2022-06-02T07:59:59.666666664Z\tnull\n" +
                            "5\t5\t2022-06-02T15:59:59.583333330Z\tnull\n" +
                            "6\t6\t2022-06-02T23:59:59.499999996Z\tnull\n" +
                            "7\t7\t2022-06-03T07:59:59.416666662Z\tnull\n" +
                            "8\t8\t2022-06-03T15:59:59.333333328Z\tnull\n" +
                            "9\t9\t2022-06-03T23:59:59.249999994Z\tnull\n" +
                            "10\t10\t2022-06-04T07:59:59.166666660Z\tnull\n" +
                            "11\t11\t2022-06-04T15:59:59.083333326Z\tnull\n" +
                            "12\t12\t2022-06-04T23:59:58.999999992Z\tnull\n",
                    tableName
            );
        });
    }

    @Test
    public void testDetachPartitionsTableAddColumnAndData() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabInAddColumnAndData";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    1,
                    tab.col("l", ColumnType.LONG)
                            .col("i", ColumnType.INT)
                            .timestamp("ts", timestampType.getTimestampType()),
                    12,
                    "2022-06-01",
                    4
            );

            engine.clear();
            TimestampDriver driver = timestampType.getDriver();
            String timestampDay = "2022-06-01";
            long timestamp = driver.parseFloorLiteral(timestampDay + "T00:00:00.000000Z");
            try (TableWriter writer = getWriter(tableName)) {
                // structural change
                writer.addColumn("new_column", ColumnType.INT);
                TableWriter.Row row = writer.newRow(timestamp);
                row.putLong(0, 33L);
                row.putInt(1, 33);
                row.append();
                writer.detachPartition(timestamp);
                Assert.assertEquals(9, writer.size());
            }

            renameDetachedToAttachable(tableName, timestampDay);

            execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + timestampDay + "'", sqlExecutionContext);
            assertContent(
                    ColumnType.isTimestampMicro(timestampType.getTimestampType()) ?
                            "l\ti\tts\tnew_column\n" +
                                    "33\t33\t2022-06-01T00:00:00.000000Z\tnull\n" +
                                    "1\t1\t2022-06-01T07:59:59.916666Z\tnull\n" +
                                    "2\t2\t2022-06-01T15:59:59.833332Z\tnull\n" +
                                    "3\t3\t2022-06-01T23:59:59.749998Z\tnull\n" +
                                    "4\t4\t2022-06-02T07:59:59.666664Z\tnull\n" +
                                    "5\t5\t2022-06-02T15:59:59.583330Z\tnull\n" +
                                    "6\t6\t2022-06-02T23:59:59.499996Z\tnull\n" +
                                    "7\t7\t2022-06-03T07:59:59.416662Z\tnull\n" +
                                    "8\t8\t2022-06-03T15:59:59.333328Z\tnull\n" +
                                    "9\t9\t2022-06-03T23:59:59.249994Z\tnull\n" +
                                    "10\t10\t2022-06-04T07:59:59.166660Z\tnull\n" +
                                    "11\t11\t2022-06-04T15:59:59.083326Z\tnull\n" +
                                    "12\t12\t2022-06-04T23:59:58.999992Z\tnull\n"
                            : "l\ti\tts\tnew_column\n" +
                            "33\t33\t2022-06-01T00:00:00.000000Z\tnull\n" +
                            "1\t1\t2022-06-01T07:59:59.916666666Z\tnull\n" +
                            "2\t2\t2022-06-01T15:59:59.833333332Z\tnull\n" +
                            "3\t3\t2022-06-01T23:59:59.749999998Z\tnull\n" +
                            "4\t4\t2022-06-02T07:59:59.666666664Z\tnull\n" +
                            "5\t5\t2022-06-02T15:59:59.583333330Z\tnull\n" +
                            "6\t6\t2022-06-02T23:59:59.499999996Z\tnull\n" +
                            "7\t7\t2022-06-03T07:59:59.416666662Z\tnull\n" +
                            "8\t8\t2022-06-03T15:59:59.333333328Z\tnull\n" +
                            "9\t9\t2022-06-03T23:59:59.249999994Z\tnull\n" +
                            "10\t10\t2022-06-04T07:59:59.166666660Z\tnull\n" +
                            "11\t11\t2022-06-04T15:59:59.083333326Z\tnull\n" +
                            "12\t12\t2022-06-04T23:59:58.999999992Z\tnull\n",
                    tableName
            );
        });
    }

    @Test
    public void testDetachPartitionsTableAddColumnAndData2() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabInAddColumn2";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    1,
                    tab.col("l", ColumnType.LONG)
                            .col("i", ColumnType.INT)
                            .timestamp("ts", timestampType.getTimestampType()),
                    12,
                    "2022-06-01",
                    4
            );

            engine.clear();
            TimestampDriver driver = timestampType.getDriver();
            String timestampDay = "2022-06-01";
            long timestamp = driver.parseFloorLiteral(timestampDay + "T00:00:00.000000Z");
            try (TableWriter writer = getWriter(tableName)) {
                writer.detachPartition(timestamp);

                // structural change
                writer.addColumn("new_column", ColumnType.INT);

                TableWriter.Row row = writer.newRow(driver.parseFloorLiteral("2022-06-02T00:00:00.000000Z"));
                row.putLong(0, 33L);
                row.putInt(1, 33);
                row.putInt(3, 333);
                row.append();

                Assert.assertEquals(10, writer.size());
                writer.commit();
            }

            renameDetachedToAttachable(tableName, timestampDay);

            execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + timestampDay + "'", sqlExecutionContext);
            assertContent(
                    ColumnType.isTimestampMicro(timestampType.getTimestampType()) ?
                            "l\ti\tts\tnew_column\n" +
                                    "1\t1\t2022-06-01T07:59:59.916666Z\tnull\n" +
                                    "2\t2\t2022-06-01T15:59:59.833332Z\tnull\n" +
                                    "3\t3\t2022-06-01T23:59:59.749998Z\tnull\n" +
                                    "33\t33\t2022-06-02T00:00:00.000000Z\t333\n" +
                                    "4\t4\t2022-06-02T07:59:59.666664Z\tnull\n" +
                                    "5\t5\t2022-06-02T15:59:59.583330Z\tnull\n" +
                                    "6\t6\t2022-06-02T23:59:59.499996Z\tnull\n" +
                                    "7\t7\t2022-06-03T07:59:59.416662Z\tnull\n" +
                                    "8\t8\t2022-06-03T15:59:59.333328Z\tnull\n" +
                                    "9\t9\t2022-06-03T23:59:59.249994Z\tnull\n" +
                                    "10\t10\t2022-06-04T07:59:59.166660Z\tnull\n" +
                                    "11\t11\t2022-06-04T15:59:59.083326Z\tnull\n" +
                                    "12\t12\t2022-06-04T23:59:58.999992Z\tnull\n" :
                            "l\ti\tts\tnew_column\n" +
                                    "1\t1\t2022-06-01T07:59:59.916666666Z\tnull\n" +
                                    "2\t2\t2022-06-01T15:59:59.833333332Z\tnull\n" +
                                    "3\t3\t2022-06-01T23:59:59.749999998Z\tnull\n" +
                                    "33\t33\t2022-06-02T00:00:00.000000Z\t333\n" +
                                    "4\t4\t2022-06-02T07:59:59.666666664Z\tnull\n" +
                                    "5\t5\t2022-06-02T15:59:59.583333330Z\tnull\n" +
                                    "6\t6\t2022-06-02T23:59:59.499999996Z\tnull\n" +
                                    "7\t7\t2022-06-03T07:59:59.416666662Z\tnull\n" +
                                    "8\t8\t2022-06-03T15:59:59.333333328Z\tnull\n" +
                                    "9\t9\t2022-06-03T23:59:59.249999994Z\tnull\n" +
                                    "10\t10\t2022-06-04T07:59:59.166666660Z\tnull\n" +
                                    "11\t11\t2022-06-04T15:59:59.083333326Z\tnull\n" +
                                    "12\t12\t2022-06-04T23:59:58.999999992Z\tnull\n",
                    tableName
            );
        });
    }

    @Test
    public void testDetachPartitionsTimeTravel() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabTimeTravel";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            String timestampDay = "2022-06-01";
            createPopulateTable(
                    tab
                            .col("l", ColumnType.LONG)
                            .col("i", ColumnType.INT)
                            .timestamp("ts", timestampType.getTimestampType()),
                    12,
                    timestampDay,
                    4
            );
            assertContent(
                    ColumnType.isTimestampMicro(timestampType.getTimestampType()) ?
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
                                    "12\t12\t2022-06-04T23:59:58.999992Z\n"
                            : "l\ti\tts\n" +
                            "1\t1\t2022-06-01T07:59:59.916666666Z\n" +
                            "2\t2\t2022-06-01T15:59:59.833333332Z\n" +
                            "3\t3\t2022-06-01T23:59:59.749999998Z\n" +
                            "4\t4\t2022-06-02T07:59:59.666666664Z\n" +
                            "5\t5\t2022-06-02T15:59:59.583333330Z\n" +
                            "6\t6\t2022-06-02T23:59:59.499999996Z\n" +
                            "7\t7\t2022-06-03T07:59:59.416666662Z\n" +
                            "8\t8\t2022-06-03T15:59:59.333333328Z\n" +
                            "9\t9\t2022-06-03T23:59:59.249999994Z\n" +
                            "10\t10\t2022-06-04T07:59:59.166666660Z\n" +
                            "11\t11\t2022-06-04T15:59:59.083333326Z\n" +
                            "12\t12\t2022-06-04T23:59:58.999999992Z\n",
                    tableName
            );

            // drop the partition
            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '" + timestampDay + "'", sqlExecutionContext);

            // insert data, which will create the partition again
            engine.clear();
            TimestampDriver driver = timestampType.getDriver();
            long timestamp = driver.parseFloorLiteral(timestampDay + "T00:00:00.000000Z");
            long timestamp2 = driver.parseFloorLiteral("2022-06-01T09:59:59.999999Z");
            try (TableWriter writer = getWriter(tableName)) {

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
            assertQuery(
                    ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? "l\ti\tts\n" +
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
                            "12\t12\t2022-06-04T23:59:58.999992Z\n"
                            : "l\ti\tts\n" +
                            "2802\t2802\t2022-06-01T00:00:00.000000000Z\n" +
                            "137\t137\t2022-06-01T09:59:59.999999000Z\n" +
                            "4\t4\t2022-06-02T07:59:59.666666664Z\n" +
                            "5\t5\t2022-06-02T15:59:59.583333330Z\n" +
                            "6\t6\t2022-06-02T23:59:59.499999996Z\n" +
                            "7\t7\t2022-06-03T07:59:59.416666662Z\n" +
                            "8\t8\t2022-06-03T15:59:59.333333328Z\n" +
                            "9\t9\t2022-06-03T23:59:59.249999994Z\n" +
                            "10\t10\t2022-06-04T07:59:59.166666660Z\n" +
                            "11\t11\t2022-06-04T15:59:59.083333326Z\n" +
                            "12\t12\t2022-06-04T23:59:58.999999992Z\n",
                    tableName,
                    null, "ts", true, true
            );

            dropCurrentVersionOfPartition(tableName, timestampDay);
            renameDetachedToAttachable(tableName, timestampDay);

            // reattach old version
            execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + timestampDay + "'");
            assertContent(
                    ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? "l\ti\tts\n" +
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
                            "12\t12\t2022-06-04T23:59:58.999992Z\n"
                            : "l\ti\tts\n" +
                            "1\t1\t2022-06-01T07:59:59.916666666Z\n" +
                            "2\t2\t2022-06-01T15:59:59.833333332Z\n" +
                            "3\t3\t2022-06-01T23:59:59.749999998Z\n" +
                            "4\t4\t2022-06-02T07:59:59.666666664Z\n" +
                            "5\t5\t2022-06-02T15:59:59.583333330Z\n" +
                            "6\t6\t2022-06-02T23:59:59.499999996Z\n" +
                            "7\t7\t2022-06-03T07:59:59.416666662Z\n" +
                            "8\t8\t2022-06-03T15:59:59.333333328Z\n" +
                            "9\t9\t2022-06-03T23:59:59.249999994Z\n" +
                            "10\t10\t2022-06-04T07:59:59.166666660Z\n" +
                            "11\t11\t2022-06-04T15:59:59.083333326Z\n" +
                            "12\t12\t2022-06-04T23:59:58.999999992Z\n",
                    tableName
            );
        });
    }

    @Test
    public void testDetachPartitionsTimestampColumnTooShort() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tab";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    1,
                    tab.col("l", ColumnType.LONG)
                            .col("i", ColumnType.INT)
                            .timestamp("ts", timestampType.getTimestampType()),
                    12,
                    "2022-06-01",
                    4
            );

            String timestampDay = "2022-06-01";
            String timestampWrongDay2 = "2022-06-02";

            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '" + timestampDay + "','" + timestampWrongDay2 + "'");
            renameDetachedToAttachable(tableName, timestampDay);

            TableToken tableToken = engine.verifyTableName(tableName);
            Path src = Path.PATH.get().of(configuration.getDbRoot()).concat(tableToken).concat(timestampDay).put(configuration.getAttachPartitionSuffix()).slash();
            FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            dFile(src, "ts", -1);
            long fd = TableUtils.openRW(ff, src.$(), LOG, configuration.getWriterFileOpenOpts());
            try {
                ff.truncate(fd, 8);
            } finally {
                ff.close(fd);
            }

            assertFailure(
                    "ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + timestampDay + "'",
                    "cannot read min, max timestamp from the"
            );
        });
    }

    @Test
    public void testDetachPartitionsWrongFolderName() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tab";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    1,
                    tab.col("l", ColumnType.LONG)
                            .col("i", ColumnType.INT)
                            .timestamp("ts", timestampType.getTimestampType()),
                    12,
                    "2022-06-01",
                    4
            );

            String timestampDay = "2022-06-01";
            String timestampWrongDay2 = "2022-06-02";

            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '" + timestampDay + "','" + timestampWrongDay2 + "'");
            renameDetachedToAttachable(tableName, timestampDay);

            String timestampWrongDay = "2021-06-01";

            // Partition does not exist in copied _dtxn
            TableToken tableToken = engine.verifyTableName(tableName);
            Path src = Path.PATH.get().of(configuration.getDbRoot()).concat(tableToken).concat(timestampDay).put(configuration.getAttachPartitionSuffix()).slash();
            Path dst = Path.PATH2.get().of(configuration.getDbRoot()).concat(tableToken).concat(timestampWrongDay).put(configuration.getAttachPartitionSuffix()).slash();

            FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            Assert.assertEquals(0, ff.rename(src.$(), dst.$()));
            assertFailure("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + timestampWrongDay + "'", "partition is not preset in detached txn file");

            // Existing partition but wrong folder name
            dst = Path.PATH2.get().of(configuration.getDbRoot()).concat(tableToken).concat(timestampWrongDay).put(configuration.getAttachPartitionSuffix()).slash();
            Path dst2 = Path.PATH.get().of(configuration.getDbRoot()).concat(tableToken).concat(timestampWrongDay2).put(configuration.getAttachPartitionSuffix()).slash();
            Assert.assertEquals(0, ff.rename(dst.$(), dst2.$()));

            assertFailure(
                    "ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + timestampWrongDay2 + "'",
                    "invalid timestamp data in detached partition, data does not match partition directory name"
            );
        });
    }

    @Test
    public void testNoDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            TableModel tab = new TableModel(configuration, "tab0", PartitionBy.DAY);
            AbstractCairoTest.create(tab
                    .col("i", ColumnType.INT)
                    .col("l", ColumnType.LONG)
            );
            try {
                execute("ALTER TABLE tab0 DETACH PARTITION LIST '2022-06-27'", sqlExecutionContext);
                Assert.fail();
            } catch (AssertionError e) {
                Assert.assertEquals(-1, tab.getTimestampIndex());
            }
        });
    }

    @Test
    public void testNotPartitioned() throws Exception {
        TableModel tableModel = new TableModel(configuration, "tab", PartitionBy.NONE).timestamp();
        AbstractSqlParserTest.assertSyntaxError(
                "ALTER TABLE tab DETACH PARTITION LIST '2022-06-27'",
                23,
                "table is not partitioned",
                tableModel
        );
    }

    @Test
    public void testPartitionEmpty() throws Exception {
        assertFailure(
                "tab11",
                "ALTER TABLE tab11 DETACH PARTITION LIST '2022-06-06'",
                "could not detach partition [table=tab11, detachStatus=DETACH_ERR_MISSING_PARTITION"
        );
    }

    @Test
    public void testPartitionFolderDoesNotExist() throws Exception {
        assertFailure(
                new TestFilesFacadeImpl() {
                    @Override
                    public boolean exists(LPSZ path) {
                        if (Utf8s.endsWithAscii(path, "2022-06-03")) {
                            return false;
                        }
                        return super.exists(path);
                    }
                },
                "tab111",
                "ALTER TABLE tab111 DETACH PARTITION LIST '2022-06-03'",
                "could not detach partition [table=tab111, detachStatus=DETACH_ERR_MISSING_PARTITION_DIR"
        );
    }

    @Test
    public void testSyntaxErrorListOrWhereExpected() throws Exception {
        TableModel tableModel = new TableModel(configuration, "tab", PartitionBy.DAY).timestamp();
        AbstractSqlParserTest.assertSyntaxError(
                "ALTER TABLE tab DETACH PARTITION",
                32,
                "'list' or 'where' expected",
                tableModel
        );
    }

    @Test
    public void testSyntaxErrorPartitionMissing() throws Exception {
        TableModel tableModel = new TableModel(configuration, "tab", PartitionBy.DAY).timestamp();
        AbstractSqlParserTest.assertSyntaxError(
                "ALTER TABLE tab DETACH '2022-06-27'",
                23,
                "'partition' expected",
                tableModel
        );
    }

    @Test
    public void testSyntaxErrorPartitionNameExpected() throws Exception {
        TableModel tableModel = new TableModel(configuration, "tab", PartitionBy.DAY).timestamp();
        AbstractSqlParserTest.assertSyntaxError(
                "ALTER TABLE tab DETACH PARTITION LIST",
                37,
                "partition name expected",
                tableModel
        );
    }

    @Test
    public void testSyntaxErrorUnknownKeyword() throws Exception {
        TableModel tableModel = new TableModel(configuration, "tab", PartitionBy.DAY).timestamp();
        AbstractSqlParserTest.assertSyntaxError(
                "ALTER TABLE tab foobar",
                16,
                SqlCompilerImpl.ALTER_TABLE_EXPECTED_TOKEN_DESCR,
                tableModel
        );
    }

    private void assertCannotCopyMeta(String tableName, int copyCallId) throws Exception {
        assertMemoryLeak(() -> {
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    tab
                            .timestamp("ts", timestampType.getTimestampType())
                            .col("i", ColumnType.INT)
                            .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                            .col("l", ColumnType.LONG)
                            .col("s2", ColumnType.SYMBOL)
                            .col("vch", ColumnType.VARCHAR),
                    10,
                    "2022-06-01",
                    4
            );
            String expected = "ts\ti\ts1\tl\ts2\tvch\n" +
                    "2022-06-01T09:35:59.900000Z\t1\tPEHN\t1\tSXUX\t1\n" +
                    "2022-06-01T19:11:59.800000Z\t2\tVTJW\t2\t\t2\n" +
                    "2022-06-02T04:47:59.700000Z\t3\t\t3\tSXUX\t3\n" +
                    "2022-06-02T14:23:59.600000Z\t4\t\t4\t\t4\n" +
                    "2022-06-02T23:59:59.500000Z\t5\t\t5\tGPGW\t5\n" +
                    "2022-06-03T09:35:59.400000Z\t6\tPEHN\t6\tRXGZ\t6\n" +
                    "2022-06-03T19:11:59.300000Z\t7\tCPSW\t7\t\t7\n" +
                    "2022-06-04T04:47:59.200000Z\t8\t\t8\t\t8\n" +
                    "2022-06-04T14:23:59.100000Z\t9\tPEHN\t9\tRXGZ\t9\n" +
                    "2022-06-04T23:59:59.000000Z\t10\tVTJW\t10\tIBBT\t10\n";
            assertContent(expected, tableName);

            AbstractCairoTest.ff = new TestFilesFacadeImpl() {
                private int copyCallCount = 0;

                public int copy(LPSZ from, LPSZ to) {
                    return ++copyCallCount == copyCallId ? -1 : super.copy(from, to);
                }
            };
            engine.clear(); // to recreate the writer with the new ff
            TimestampDriver driver = timestampType.getDriver();
            long timestamp = driver.parseFloorLiteral("2022-06-01T00:00:00.000000Z");
            try (TableWriter writer = getWriter(tableName)) {
                AttachDetachStatus attachDetachStatus = writer.detachPartition(timestamp);
                Assert.assertEquals(DETACH_ERR_COPY_META, attachDetachStatus);
            }

            assertContent(expected, tableName);

            // check no metadata files were left behind
            TableToken tableToken = engine.verifyTableName(tableName);
            path.of(configuration.getDbRoot())
                    .concat(tableToken)
                    .concat("2022-06-01").concat(META_FILE_NAME)
                    .$();
            Assert.assertFalse(Files.exists(path.$()));
            path.parent().concat(COLUMN_VERSION_FILE_NAME).$();
            Assert.assertFalse(Files.exists(path.$()));
        });
    }

    private void assertContent(String expected, String tableName) throws Exception {
        engine.clear();
        expected = replaceTimestampSuffix1(expected, timestampType.getTypeName());
        assertQuery(expected, tableName, null, "ts", true, true);
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
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            TableModel brokenMeta = new TableModel(configuration, brokenTableName, PartitionBy.DAY);
            try (
                    MemoryMARW mem = Vm.getCMARWInstance()
            ) {
                createPopulateTable(
                        1,
                        tab.timestamp("ts", timestampType.getTimestampType())
                                .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG)
                                .col("s2", ColumnType.SYMBOL)
                                .col("vch", ColumnType.VARCHAR),
                        10,
                        "2022-06-01",
                        3
                );
                // create populate broken metadata table
                TestUtils.createTable(
                        engine,
                        mem,
                        path,
                        brokenMetaTransform.apply(brokenMeta),
                        brokenMetaId,
                        brokenMeta.getTableName()
                );
                if (insertStmt != null) {
                    execute(insertStmt, sqlExecutionContext);
                }
                if (finalStmt != null) {
                    execute(finalStmt, sqlExecutionContext);
                }

                // detach partitions and override detached metadata with broken metadata
                engine.clear();
                execute(
                        "ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-02'",
                        sqlExecutionContext
                );
                execute(
                        "ALTER TABLE " + brokenTableName + " DETACH PARTITION LIST '2022-06-02'",
                        sqlExecutionContext
                );
                engine.clear();
                TableToken tableToken = engine.verifyTableName(tableName);
                path.of(configuration.getDbRoot())
                        .concat(tableToken)
                        .concat("2022-06-02")
                        .put(DETACHED_DIR_MARKER)
                        .concat(META_FILE_NAME)
                        .$();
                Assert.assertTrue(Files.remove(path.$()));
                other.of(configuration.getDbRoot())
                        .concat(engine.verifyTableName(brokenTableName))
                        .concat("2022-06-02")
                        .put(DETACHED_DIR_MARKER)
                        .concat(META_FILE_NAME)
                        .$();
                Assert.assertEquals(Files.FILES_RENAME_OK, Files.rename(other.$(), path.$()));

                renameDetachedToAttachable(tableName, "2022-06-02");

                // attempt to reattach
                assertFailure(
                        "ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-02'",
                        expectedErrorMessage
                );
            }
        });
    }

    private void assertFailure(String tableName, String operation, String errorMsg) throws Exception {
        assertFailure(null, tableName, operation, errorMsg);
    }

    private void assertFailure(
            @Nullable FilesFacade ff,
            String tableName,
            String operation,
            String errorMsg
    ) throws Exception {
        assertFailure(
                ff,
                tableName,
                operation,
                errorMsg,
                null
        );
    }

    private void assertFailure(
            @Nullable FilesFacade ff,
            String tableName,
            String operation,
            String errorMsg,
            @Nullable Runnable mutator
    ) throws Exception {
        assertMemoryLeak(ff, () -> {
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    tab
                            .timestamp("ts", timestampType.getTimestampType())
                            .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG)
                            .col("s2", ColumnType.SYMBOL),
                    100,
                    "2022-06-01",
                    5
            );
            if (mutator != null) {
                mutator.run();
            }
            assertFailure(operation, errorMsg);
        });
    }

    private void assertFailure(String operation, String errorMsg) {
        try {
            execute(operation, sqlExecutionContext);
            Assert.fail();
        } catch (SqlException | CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), errorMsg);
        }
    }

    private void dropCurrentVersionOfPartition(String tableName, String partitionName) throws SqlException {
        engine.clear();
        // hide the detached partition
        TableToken tableToken = engine.verifyTableName(tableName);
        path.of(configuration.getDbRoot()).concat(tableToken).concat(partitionName + ".detached").$();
        other.of(configuration.getDbRoot()).concat(tableToken).concat(partitionName + ".detached.hide").$();

        Assert.assertEquals(Files.FILES_RENAME_OK, Files.rename(path.$(), other.$()));
        // drop the latest version of the partition
        execute("ALTER TABLE " + tableName + " DROP PARTITION LIST '" + partitionName + "'", sqlExecutionContext);
        // resurface the hidden detached partition
        Assert.assertEquals(Files.FILES_RENAME_OK, Files.rename(other.$(), path.$()));
    }

    private void renameDetachedToAttachable(String tableName, int timestampType, long... partitions) {
        TableToken tableToken = engine.verifyTableName(tableName);
        for (long partition : partitions) {
            TableUtils.setSinkForNativePartition(
                    path.of(configuration.getDbRoot()).concat(tableToken),
                    timestampType,
                    PartitionBy.DAY,
                    partition,
                    -1
            );
            path.put(DETACHED_DIR_MARKER).$();
            TableUtils.setSinkForNativePartition(
                    other.of(configuration.getDbRoot()).concat(tableToken),
                    timestampType,
                    PartitionBy.DAY,
                    partition,
                    -1
            );
            other.put(configuration.getAttachPartitionSuffix()).$();
            Assert.assertTrue(Files.rename(path.$(), other.$()) > -1);
        }
    }

    private void renameDetachedToAttachable(String tableName, String... partitions) {
        TableToken tableToken = engine.verifyTableName(tableName);
        for (String partition : partitions) {
            path.of(configuration.getDbRoot()).concat(tableToken).concat(partition).put(DETACHED_DIR_MARKER).$();
            other.of(configuration.getDbRoot()).concat(tableToken).concat(partition).put(configuration.getAttachPartitionSuffix()).$();
            Assert.assertTrue(Files.rename(path.$(), other.$()) > -1);
        }
    }

    private String replaceTimestampSuffix(String expected) {
        return ColumnType.isTimestampNano(timestampType.getTimestampType()) ? expected.replaceAll("Z\t", "000Z\t").replaceAll("Z\n", "000Z\n") : expected;
    }

    private void runPartitionPurgeJobs() {
        // when reader is returned to pool it remains in open state
        // holding files such that purge fails with access violation
        if (Os.isWindows()) {
            engine.releaseInactive();
        }
        purgeJob.drain(0);
    }
}
