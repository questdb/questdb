/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.griffin.SqlException;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Files;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.TableUtils.ATTACHABLE_DIR_MARKER;
import static io.questdb.cairo.TableUtils.DETACHED_DIR_MARKER;
import static io.questdb.test.tools.TestUtils.replaceSizeToMatchOS;

@RunWith(Parameterized.class)
public class ShowPartitionsTest extends AbstractCairoTest {

    private final boolean isWal;
    private final String tableNameSuffix;

    public ShowPartitionsTest(WalMode walMode, String tableNameSuffix) {
        isWal = walMode == WalMode.WITH_WAL;
        this.tableNameSuffix = tableNameSuffix;
        node1.setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, isWal);
    }

    @Parameterized.Parameters(name = "{0}-{1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {WalMode.WITH_WAL, null},
                {WalMode.NO_WAL, null},
                {WalMode.WITH_WAL, "テンション"},
                {WalMode.NO_WAL, "テンション"}
        });
    }

    public static String testTableName(String tableName, @Nullable String tableNameSuffix) {
        int idx = tableName.indexOf('[');
        tableName = idx > 0 ? tableName.substring(0, idx) : tableName;
        return tableNameSuffix == null ? tableName : tableName + '_' + tableNameSuffix;
    }

    @Test
    public void testShowPartitionsAttachablePartitionOfWrongPartitionBy() throws Exception {
        Assume.assumeFalse(Os.isWindows());
        String tabName = testTableName(testName.getMethodName());
        String tab2Name = tabName + "_fubar";
        assertMemoryLeak(() -> {
            TableModel tab = new TableModel(configuration, tabName, PartitionBy.DAY);
            TableModel tab2 = new TableModel(configuration, tab2Name, PartitionBy.MONTH);
            try (
                    Path dstPath = new Path();
                    Path srcPath = new Path()
            ) {
                if (isWal) {
                    tab.wal();
                    tab2.wal();
                }
                createPopulateTable(
                        1,
                        tab.timestamp("ts")
                                .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG)
                                .col("s2", ColumnType.SYMBOL),
                        10,
                        "2023-03-13",
                        2
                );
                createPopulateTable(
                        1,
                        tab2.timestamp("ts")
                                .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG)
                                .col("s2", ColumnType.SYMBOL),
                        20,
                        "2023-03-13",
                        4
                );
                execute("ALTER TABLE " + tab2Name + " DETACH PARTITION LIST '2023-03'");
                if (isWal) {
                    drainWalQueue();
                }
                engine.releaseAllWriters();
                srcPath.of(configuration.getDbRoot()).concat(engine.verifyTableName(tab2Name)).concat("2023-03").put(DETACHED_DIR_MARKER);
                dstPath.of(configuration.getDbRoot()).concat(engine.verifyTableName(tabName)).concat("2023-03").put(ATTACHABLE_DIR_MARKER);
                Assert.assertEquals(0, Files.softLink(srcPath.$(), dstPath.$()));
                assertShowPartitions(
                        "index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\tisParquet\tparquetFileSize\n" +
                                "0\tDAY\t2023-03-13\t2023-03-13T04:47:59.900000Z\t2023-03-13T23:59:59.500000Z\t5\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\n" +
                                "1\tDAY\t2023-03-14\t2023-03-14T04:47:59.400000Z\t2023-03-14T23:59:59.000000Z\t5\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\tfalse\t-1\n" +
                                "null\tDAY\t2023-03.attachable\t\t\t-1\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\ttrue\tfalse\t-1\n",
                        tabName);
            }
        });
    }

    @Test
    public void testShowPartitionsAttachablePartitionOfWrongTableId() throws Exception {
        Assume.assumeFalse(Os.isWindows()); // symlink required, and not well-supported in Windows
        String tabName = testTableName(testName.getMethodName());
        String tab2Name = tabName + "_fubar";
        assertMemoryLeak(() -> {
            TableModel tab = new TableModel(configuration, tabName, PartitionBy.DAY);
            TableModel tab2 = new TableModel(configuration, tab2Name, PartitionBy.DAY);
            try (
                    Path dstPath = new Path();
                    Path srcPath = new Path()
            ) {
                if (isWal) {
                    tab.wal();
                    tab2.wal();
                }
                createPopulateTable(
                        1,
                        tab.timestamp("ts")
                                .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG)
                                .col("s2", ColumnType.SYMBOL),
                        10,
                        "2023-03-13",
                        2
                );
                createPopulateTable(
                        99,
                        tab2.timestamp("ts")
                                .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG)
                                .col("s2", ColumnType.SYMBOL),
                        20,
                        "2023-03-13",
                        4
                );
                execute("ALTER TABLE " + tab2Name + " DETACH PARTITION LIST '2023-03-15'");
                if (isWal) {
                    drainWalQueue();
                }
                engine.releaseAllWriters();
                srcPath.of(configuration.getDbRoot()).concat(engine.verifyTableName(tab2Name)).concat("2023-03-15").put(DETACHED_DIR_MARKER);
                dstPath.of(configuration.getDbRoot()).concat(engine.verifyTableName(tabName)).concat("2023-03-15").put(ATTACHABLE_DIR_MARKER);
                Assert.assertEquals(0, Files.softLink(srcPath.$(), dstPath.$()));
                assertShowPartitions(
                        "index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\tisParquet\tparquetFileSize\n" +
                                "0\tDAY\t2023-03-13\t2023-03-13T04:47:59.900000Z\t2023-03-13T23:59:59.500000Z\t5\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\n" +
                                "1\tDAY\t2023-03-14\t2023-03-14T04:47:59.400000Z\t2023-03-14T23:59:59.000000Z\t5\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\tfalse\t-1\n" +
                                "null\tDAY\t2023-03-15.attachable\t\t\t-1\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\ttrue\tfalse\t-1\n",
                        tabName);
            }
        });
    }

    @Test
    public void testShowPartitionsBadSyntax() throws Exception {
        Assume.assumeFalse(Os.isWindows());
        String tabName = testTableName(testName.getMethodName());
        assertMemoryLeak(() -> {
            createTable(tabName);
            try {
                execute("SHOW PARTITIONS FROM " + tabName + " WHERE active=true", sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "unexpected token [WHERE]");
            }
        });
    }

    @Test
    public void testShowPartitionsDetachedPartitionPlusAttachable() throws Exception {
        Assume.assumeFalse(Os.isWindows()); // no links in windows
        String tableName = testTableName(testName.getMethodName());
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(tableName);
            execute("ALTER TABLE " + tableName + " DETACH PARTITION WHERE timestamp < '2023-06-01T00:00:00.000000Z'", sqlExecutionContext);
            if (isWal) {
                drainWalQueue();
            }
            // prepare 3 partitions for attachment
            try (
                    Path path = new Path().of(configuration.getDbRoot()).concat(tableToken).concat("2023-0");
                    Path link = new Path().of(configuration.getDbRoot()).concat(tableToken).concat("2023-0")
            ) {
                int len = path.size();
                for (int i = 2; i < 5; i++) {
                    path.trimTo(len).put(i).put(TableUtils.DETACHED_DIR_MARKER);
                    link.trimTo(len).put(i).put(TableUtils.ATTACHABLE_DIR_MARKER);
                    Assert.assertEquals(0, Files.softLink(path.$(), link.$()));
                }
            }
            assertShowPartitions(
                    "index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\tisParquet\tparquetFileSize\n" +
                            "0\tMONTH\t2023-06\t2023-06-01T00:00:00.000000Z\t2023-06-25T00:00:00.000000Z\t97\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\tfalse\t-1\n" +
                            "null\tMONTH\t2023-01.detached\t2023-01-01T06:00:00.000000Z\t2023-01-31T18:00:00.000000Z\t123\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\t-1\n" +
                            "null\tMONTH\t2023-02.detached\t2023-02-01T00:00:00.000000Z\t2023-02-28T18:00:00.000000Z\t112\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\t-1\n" +
                            "null\tMONTH\t2023-03.detached\t2023-03-01T00:00:00.000000Z\t2023-03-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\t-1\n" +
                            "null\tMONTH\t2023-04.detached\t2023-04-01T00:00:00.000000Z\t2023-04-30T18:00:00.000000Z\t120\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\t-1\n" +
                            "null\tMONTH\t2023-05.detached\t2023-05-01T00:00:00.000000Z\t2023-05-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\t-1\n" +
                            "null\tMONTH\t2023-02.attachable\t2023-02-01T00:00:00.000000Z\t2023-02-28T18:00:00.000000Z\t112\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\ttrue\tfalse\t-1\n" +
                            "null\tMONTH\t2023-03.attachable\t2023-03-01T00:00:00.000000Z\t2023-03-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\ttrue\tfalse\t-1\n" +
                            "null\tMONTH\t2023-04.attachable\t2023-04-01T00:00:00.000000Z\t2023-04-30T18:00:00.000000Z\t120\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\ttrue\tfalse\t-1\n",
                    tableName);
            execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2023-02', '2023-03'", sqlExecutionContext);
            if (isWal) {
                drainWalQueue();
            }
            assertShowPartitions(
                    "index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\tisParquet\tparquetFileSize\n" +
                            "0\tMONTH\t2023-02\t2023-02-01T00:00:00.000000Z\t2023-02-28T18:00:00.000000Z\t112\tSIZE\tHUMAN\ttrue\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\n" +
                            "1\tMONTH\t2023-03\t2023-03-01T00:00:00.000000Z\t2023-03-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\ttrue\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\n" +
                            "2\tMONTH\t2023-06\t2023-06-01T00:00:00.000000Z\t2023-06-25T00:00:00.000000Z\t97\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\tfalse\t-1\n" +
                            "null\tMONTH\t2023-01.detached\t2023-01-01T06:00:00.000000Z\t2023-01-31T18:00:00.000000Z\t123\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\t-1\n" +
                            "null\tMONTH\t2023-02.detached\t2023-02-01T00:00:00.000000Z\t2023-02-28T18:00:00.000000Z\t112\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\t-1\n" +
                            "null\tMONTH\t2023-03.detached\t2023-03-01T00:00:00.000000Z\t2023-03-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\t-1\n" +
                            "null\tMONTH\t2023-04.detached\t2023-04-01T00:00:00.000000Z\t2023-04-30T18:00:00.000000Z\t120\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\t-1\n" +
                            "null\tMONTH\t2023-05.detached\t2023-05-01T00:00:00.000000Z\t2023-05-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\t-1\n" +
                            "null\tMONTH\t2023-04.attachable\t2023-04-01T00:00:00.000000Z\t2023-04-30T18:00:00.000000Z\t120\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\ttrue\tfalse\t-1\n",
                    tableName);
        });
    }

    @Test
    public void testShowPartitionsOnlyDetachedPartitionMissingMeta() throws Exception {
        String tableName = testTableName(testName.getMethodName());
        assertMemoryLeak(() -> {
            createTable(tableName);
            execute("ALTER TABLE " + tableName + " DETACH PARTITION WHERE timestamp < '2023-06-01T00:00:00.000000Z'", sqlExecutionContext);
            if (isWal) {
                drainWalQueue();
            }
            deleteFile(tableName, "2023-04" + TableUtils.DETACHED_DIR_MARKER, TableUtils.META_FILE_NAME);
            assertShowPartitions(
                    "index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\tisParquet\tparquetFileSize\n" +
                            "0\tMONTH\t2023-06\t2023-06-01T00:00:00.000000Z\t2023-06-25T00:00:00.000000Z\t97\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\tfalse\t-1\n" +
                            "null\tMONTH\t2023-01.detached\t2023-01-01T06:00:00.000000Z\t2023-01-31T18:00:00.000000Z\t123\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\t-1\n" +
                            "null\tMONTH\t2023-02.detached\t2023-02-01T00:00:00.000000Z\t2023-02-28T18:00:00.000000Z\t112\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\t-1\n" +
                            "null\tMONTH\t2023-03.detached\t2023-03-01T00:00:00.000000Z\t2023-03-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\t-1\n" +
                            "null\tMONTH\t2023-04.detached\t\t\t-1\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\t-1\n" +
                            "null\tMONTH\t2023-05.detached\t2023-05-01T00:00:00.000000Z\t2023-05-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\t-1\n",
                    tableName);
        });
    }

    @Test
    public void testShowPartitionsOnlyDetachedPartitionMissingTimestampColumn() throws Exception {
        String tableName = testTableName(testName.getMethodName());
        assertMemoryLeak(() -> {
            createTable(tableName);
            execute("ALTER TABLE " + tableName + " DETACH PARTITION WHERE timestamp < '2023-06-01T00:00:00.000000Z'", sqlExecutionContext);
            if (isWal) {
                drainWalQueue();
            }
            deleteFile(tableName, "2023-04" + TableUtils.DETACHED_DIR_MARKER, "timestamp.d");
            assertShowPartitions(
                    "index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\tisParquet\tparquetFileSize\n" +
                            "0\tMONTH\t2023-06\t2023-06-01T00:00:00.000000Z\t2023-06-25T00:00:00.000000Z\t97\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\tfalse\t-1\n" +
                            "null\tMONTH\t2023-01.detached\t2023-01-01T06:00:00.000000Z\t2023-01-31T18:00:00.000000Z\t123\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\t-1\n" +
                            "null\tMONTH\t2023-02.detached\t2023-02-01T00:00:00.000000Z\t2023-02-28T18:00:00.000000Z\t112\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\t-1\n" +
                            "null\tMONTH\t2023-03.detached\t2023-03-01T00:00:00.000000Z\t2023-03-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\t-1\n" +
                            "null\tMONTH\t2023-04.detached\t\t\t120\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\t-1\n" +
                            "null\tMONTH\t2023-05.detached\t2023-05-01T00:00:00.000000Z\t2023-05-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\t-1\n",
                    tableName);
        });
    }

    @Test
    public void testShowPartitionsOnlyDetachedPartitionMissingTxn() throws Exception {
        String tableName = testTableName(testName.getMethodName());
        assertMemoryLeak(() -> {
            createTable(tableName);
            execute("ALTER TABLE " + tableName + " DETACH PARTITION WHERE timestamp < '2023-06-01T00:00:00.000000Z'", sqlExecutionContext);
            if (isWal) {
                drainWalQueue();
            }
            deleteFile(tableName, "2023-04" + TableUtils.DETACHED_DIR_MARKER, TableUtils.TXN_FILE_NAME);
            assertShowPartitions(
                    "index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\tisParquet\tparquetFileSize\n" +
                            "0\tMONTH\t2023-06\t2023-06-01T00:00:00.000000Z\t2023-06-25T00:00:00.000000Z\t97\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\tfalse\t-1\n" +
                            "null\tMONTH\t2023-01.detached\t2023-01-01T06:00:00.000000Z\t2023-01-31T18:00:00.000000Z\t123\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\t-1\n" +
                            "null\tMONTH\t2023-02.detached\t2023-02-01T00:00:00.000000Z\t2023-02-28T18:00:00.000000Z\t112\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\t-1\n" +
                            "null\tMONTH\t2023-03.detached\t2023-03-01T00:00:00.000000Z\t2023-03-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\t-1\n" +
                            "null\tMONTH\t2023-04.detached\t\t\t-1\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\t-1\n" +
                            "null\tMONTH\t2023-05.detached\t2023-05-01T00:00:00.000000Z\t2023-05-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\t-1\n",
                    tableName);
        });
    }

    @Test
    public void testShowPartitionsOnlyDetachedPartitions() throws Exception {
        String tableName = testTableName(testName.getMethodName());
        assertMemoryLeak(() -> {
            createTable(tableName);
            execute("ALTER TABLE " + tableName + " DETACH PARTITION WHERE timestamp < '2023-06-01T00:00:00.000000Z'", sqlExecutionContext);
            execute("ALTER TABLE " + tableName + " DROP PARTITION LIST '2023-06'", sqlExecutionContext);
            if (isWal) {
                drainWalQueue();
            }
            assertShowPartitions(
                    "index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\tisParquet\tparquetFileSize\n" +
                            "null\tMONTH\t2023-01.detached\t2023-01-01T06:00:00.000000Z\t2023-01-31T18:00:00.000000Z\t123\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\t-1\n" +
                            "null\tMONTH\t2023-02.detached\t2023-02-01T00:00:00.000000Z\t2023-02-28T18:00:00.000000Z\t112\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\t-1\n" +
                            "null\tMONTH\t2023-03.detached\t2023-03-01T00:00:00.000000Z\t2023-03-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\t-1\n" +
                            "null\tMONTH\t2023-04.detached\t2023-04-01T00:00:00.000000Z\t2023-04-30T18:00:00.000000Z\t120\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\t-1\n" +
                            "null\tMONTH\t2023-05.detached\t2023-05-01T00:00:00.000000Z\t2023-05-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\t-1\n",
                    tableName);
        });
    }

    @Test
    public void testShowPartitionsSelectActive() throws Exception {
        String tableName = testTableName(testName.getMethodName());
        assertMemoryLeak(() -> {
            createTable(tableName);
            assertQueryNoLeakCheck(
                    replaceSizeToMatchOS(
                            "index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\tisParquet\tparquetFileSize\n" +
                                    "5\tMONTH\t2023-06\t2023-06-01T00:00:00.000000Z\t2023-06-25T00:00:00.000000Z\t97\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\tfalse\t-1\n",
                            tableName, configuration, engine, sink),
                    "SELECT * FROM table_partitions('" + tableName + "') WHERE active = true;",
                    null,
                    false,
                    false,
                    true);
        });
    }

    @Test
    public void testShowPartitionsSelectActiveByWeek() throws Exception {
        String tableName = testTableName(testName.getMethodName());
        assertMemoryLeak(() -> {
            createTable(tableName, PartitionBy.WEEK);
            assertQueryNoLeakCheck(
                    replaceSizeToMatchOS(
                            "index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\tisParquet\tparquetFileSize\n" +
                                    "25\tWEEK\t2023-W25\t2023-06-19T00:00:00.000000Z\t2023-06-25T00:00:00.000000Z\t25\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\tfalse\t-1\n",
                            tableName, configuration, engine, sink),
                    "SELECT * FROM table_partitions('" + tableName + "') WHERE active = true;",
                    null,
                    false,
                    false,
                    true);
        });
    }

    @Test
    public void testShowPartitionsSelectActiveMaterializing() throws Exception {
        String tableName = testTableName(testName.getMethodName());
        assertMemoryLeak(() -> {
            createTable(tableName);
            execute("CREATE TABLE partitions AS (SELECT * FROM table_partitions('" + tableName + "'))", sqlExecutionContext);
            if (isWal) {
                drainWalQueue();
            }
            assertQueryNoLeakCheck(
                    replaceSizeToMatchOS(
                            "index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\tisParquet\tparquetFileSize\n" +
                                    "5\tMONTH\t2023-06\t2023-06-01T00:00:00.000000Z\t2023-06-25T00:00:00.000000Z\t97\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\tfalse\t-1\n",
                            tableName, configuration, engine, sink),
                    "SELECT * FROM partitions WHERE active = true;",
                    null,
                    true,
                    false,
                    true);
        });
    }

    @Test
    public void testShowPartitionsTableDoesNotExist() throws Exception {
        assertMemoryLeak(() -> {
            assertException("show partitions from banana", 21, "table does not exist [table=banana]");
            assertException("SELECT * FROM table_partitions('banana')", 31, "table does not exist [table=banana]");
        });
    }

    @Test
    public void testShowPartitionsWhenThereAreNoDetachedNorAttachable() throws Exception {
        String tableName = testTableName(testName.getMethodName());
        assertMemoryLeak(() -> {
            createTable(tableName);
            assertShowPartitions(
                    "index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\tisParquet\tparquetFileSize\n" +
                            "0\tMONTH\t2023-01\t2023-01-01T06:00:00.000000Z\t2023-01-31T18:00:00.000000Z\t123\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\n" +
                            "1\tMONTH\t2023-02\t2023-02-01T00:00:00.000000Z\t2023-02-28T18:00:00.000000Z\t112\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\n" +
                            "2\tMONTH\t2023-03\t2023-03-01T00:00:00.000000Z\t2023-03-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\n" +
                            "3\tMONTH\t2023-04\t2023-04-01T00:00:00.000000Z\t2023-04-30T18:00:00.000000Z\t120\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\n" +
                            "4\tMONTH\t2023-05\t2023-05-01T00:00:00.000000Z\t2023-05-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\n" +
                            "5\tMONTH\t2023-06\t2023-06-01T00:00:00.000000Z\t2023-06-25T00:00:00.000000Z\t97\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\tfalse\t-1\n",
                    tableName);
        });
    }

    @Test
    public void testShowPartitionsWhenThereAreNoDetachedNorAttachableMissingTimestampColumn() throws Exception {
        String tableName = testTableName(testName.getMethodName());
        createTable(tableName);
        deleteFile(tableName, "2023-04", "timestamp.d");

        final String finallyExpected = replaceSizeToMatchOS(
                "index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\tisParquet\tparquetFileSize\n" +
                        "0\tMONTH\t2023-01\t2023-01-01T06:00:00.000000Z\t2023-01-31T18:00:00.000000Z\t123\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\n" +
                        "1\tMONTH\t2023-02\t2023-02-01T00:00:00.000000Z\t2023-02-28T18:00:00.000000Z\t112\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\n" +
                        "2\tMONTH\t2023-03\t2023-03-01T00:00:00.000000Z\t2023-03-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\n" +
                        "null\tMONTH\t2023-04\t\t\t120\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\n" +
                        "4\tMONTH\t2023-05\t2023-05-01T00:00:00.000000Z\t2023-05-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\n" +
                        "5\tMONTH\t2023-06\t2023-06-01T00:00:00.000000Z\t2023-06-25T00:00:00.000000Z\t97\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\tfalse\t-1\n",
                tableName, configuration, engine, sink
        );

        engine.releaseInactive();

        assertQuery(
                finallyExpected,
                "SELECT * FROM table_partitions('" + tableName + "')",
                null,
                null,
                false,
                true
        );

        assertQuery(
                finallyExpected,
                "show partitions from " + tableName,
                null,
                null,
                false,
                true
        );
    }

    private static void deleteFile(String tableName, String... pathParts) {
        engine.releaseAllWriters();
        TableToken tableToken = engine.verifyTableName(tableName);
        try (Path path = new Path().of(configuration.getDbRoot()).concat(tableToken)) {
            for (String part : pathParts) {
                path.concat(part);
            }
            path.$();
            Assert.assertTrue(Files.exists(path.$()));
            Assert.assertTrue(TestUtils.remove(path.$()));
            Assert.assertFalse(Files.exists(path.$()));
        }
    }

    private void assertShowPartitions(String expected, String tableName) throws SqlException {
        SOCountDownLatch done = new SOCountDownLatch(1);
        String finallyExpected = replaceSizeToMatchOS(expected, tableName, configuration, engine, sink);
        AtomicInteger failureCounter = new AtomicInteger();
        new Thread(() -> {
            try {
                try {
                    assertQueryNoLeakCheck(
                            finallyExpected,
                            "show partitions from " + tableName,
                            null,
                            false,
                            true,
                            true
                    );
                } catch (Throwable e) {
                    e.printStackTrace(System.out);
                    failureCounter.incrementAndGet();
                }
            } finally {
                Path.clearThreadLocals();
                done.countDown();
            }
        }).start();
        done.await();
        Assert.assertEquals(0, failureCounter.get());
        assertQueryNoLeakCheck(
                finallyExpected,
                "SELECT * FROM table_partitions('" + tableName + "')",
                null,
                false,
                true,
                true
        );
    }

    private TableToken createTable(String tableName) throws SqlException {
        return createTable(tableName, PartitionBy.MONTH);
    }

    private TableToken createTable(String tableName, int partitionBy) throws SqlException {
        assert partitionBy != PartitionBy.NONE;
        String createTable = "CREATE TABLE " + tableName + " AS (" +
                "    SELECT" +
                "        rnd_symbol('EURO', 'USD', 'OTHER') symbol," +
                "        rnd_double() * 50.0 price," +
                "        rnd_double() * 20.0 amount," +
                "        to_timestamp('2023-01-01', 'yyyy-MM-dd') + x * 6 * 3600 * 1000000L timestamp" +
                "    FROM long_sequence(700)" +
                "), INDEX(symbol capacity 32) TIMESTAMP(timestamp) PARTITION BY " + PartitionBy.toString(partitionBy);
        SOCountDownLatch returned = new SOCountDownLatch(1);
        if (isWal) {
            createTable += " WAL";
            engine.setPoolListener((factoryType, thread, tableToken, event, segment, position) -> {
                if (tableToken != null && tableToken.getTableName().equals(tableName) && factoryType == PoolListener.SRC_WRITER && event == PoolListener.EV_RETURN) {
                    returned.countDown();
                }
            });
        }
        execute(createTable);
        if (isWal) {
            drainWalQueue();
            returned.await();
        }
        return engine.verifyTableName(tableName);
    }

    private String testTableName(String tableName) {
        return testTableName(tableName, tableNameSuffix);
    }
}
