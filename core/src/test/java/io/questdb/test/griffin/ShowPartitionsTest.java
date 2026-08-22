/*+*****************************************************************************
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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxWriter;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Files;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
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

    @Override
    public void setUp() {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_DEFAULT_SYMBOL_INDEX_TYPE, TestUtils.randomSymbolIndexTypeName(rnd));
        super.setUp();
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
                        """
                                index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\thasParquetGenerated\tisParquet\tparquetFileSize\tseqTxn\tisRemotelyServed
                                0\tDAY\t2023-03-13\t2023-03-13T04:47:59.900000Z\t2023-03-13T23:59:59.500000Z\t5\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                                1\tDAY\t2023-03-14\t2023-03-14T04:47:59.400000Z\t2023-03-14T23:59:59.000000Z\t5\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                                null\tDAY\t2023-03.attachable\t\t\t-1\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\ttrue\tfalse\tfalse\t-1\tSEQTXN\tfalse
                                """,
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
                        """
                                index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\thasParquetGenerated\tisParquet\tparquetFileSize\tseqTxn\tisRemotelyServed
                                0\tDAY\t2023-03-13\t2023-03-13T04:47:59.900000Z\t2023-03-13T23:59:59.500000Z\t5\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                                1\tDAY\t2023-03-14\t2023-03-14T04:47:59.400000Z\t2023-03-14T23:59:59.000000Z\t5\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                                null\tDAY\t2023-03-15.attachable\t\t\t-1\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\ttrue\tfalse\tfalse\t-1\tSEQTXN\tfalse
                                """,
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
    public void testShowPartitionsRemotelyServedPartition() throws Exception {
        String tableName = testTableName(testName.getMethodName());
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE " + tableName + " AS (" +
                            "    SELECT x::INT id," +
                            "        timestamp_sequence('2023-01-01', 24 * 3600 * 1_000_000L) ts" +
                            "    FROM long_sequence(3)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY" + (isWal ? " WAL" : "")
            );
            if (isWal) {
                drainWalQueue();
            }
            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2023-01-01'");
            if (isWal) {
                drainWalQueue();
            }

            // Stage the remotely-served shape: clear the generated bit and set REMOTE on the converted partition.
            TableToken token = engine.verifyTableName(tableName);
            try (TableWriter writer = engine.getWriter(token, "test")) {
                TxWriter tx = writer.getTxWriter();
                Assert.assertTrue("partition must be parquet format", tx.isPartitionParquet(0));
                tx.setPartitionParquetGenerated(0, false);
                tx.setPartitionRemote(0, true);
                tx.bumpPartitionTableVersion();
                tx.commit(writer.getDenseSymbolMapWriters());
                Assert.assertTrue("partition must remain parquet format", tx.isPartitionParquet(0));
                Assert.assertTrue("partition must be remote", tx.isPartitionRemote(0));
            }

            assertQuery("SELECT name, isParquet, hasParquetGenerated, isRemotelyServed" +
                    " FROM table_partitions('" + tableName + "')" +
                    " WHERE attached" +
                    " ORDER BY name")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns("""
                            name\tisParquet\thasParquetGenerated\tisRemotelyServed
                            2023-01-01\ttrue\tfalse\ttrue
                            2023-01-02\tfalse\tfalse\tfalse
                            2023-01-03\tfalse\tfalse\tfalse
                            """);
        });
    }

    @Test
    public void testShowPartitionsParquetGeneratedNotSwitchedPartition() throws Exception {
        String tableName = testTableName(testName.getMethodName());
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE " + tableName + " AS (" +
                            "    SELECT x::INT id," +
                            "        timestamp_sequence('2023-01-01', 24 * 3600 * 1_000_000L) ts" +
                            "    FROM long_sequence(3)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY" + (isWal ? " WAL" : "")
            );
            if (isWal) {
                drainWalQueue();
            }

            // Stage the generated-not-switched shape: encode the first partition to a local
            // data.parquet, then mark it parquet_generated while it stays native-format.
            TableToken token = engine.verifyTableName(tableName);
            long partitionTs;
            long dataParquetSize;
            try (
                    TableReader reader = engine.getReader(token);
                    Path path = new Path();
                    PartitionDescriptor descriptor = new PartitionDescriptor()
            ) {
                partitionTs = reader.getTxFile().getPartitionTimestampByIndex(0);
                long nameTxn = reader.getTxFile().getPartitionNameTxn(0);
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForNativePartition(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, nameTxn);
                path.concat(TableUtils.PARQUET_PARTITION_NAME);
                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);
                dataParquetSize = configuration.getFilesFacade().length(path.$());
                Assert.assertTrue("encoded data.parquet must have positive size", dataParquetSize > 0);
            }
            try (TableWriter writer = engine.getWriter(token, "test")) {
                Assert.assertTrue("markPartitionParquetReady must accept the staged partition",
                        writer.markPartitionParquetReady(partitionTs));
                Assert.assertFalse("partition must stay native-format", writer.getTxWriter().isPartitionParquet(0));
                Assert.assertTrue("partition must be parquet_generated", writer.getTxWriter().isPartitionParquetGenerated(0));
            }

            // The generated-not-switched branch stats the local data.parquet for parquetFileSize:
            // offset 3 of _txn holds the seqTxn for a native partition, not a size.
            assertQuery("SELECT name, isParquet, hasParquetGenerated, isRemotelyServed, parquetFileSize" +
                    " FROM table_partitions('" + tableName + "')" +
                    " WHERE attached" +
                    " ORDER BY name")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns("name\tisParquet\thasParquetGenerated\tisRemotelyServed\tparquetFileSize\n" +
                            "2023-01-01\tfalse\ttrue\tfalse\t" + dataParquetSize + "\n" +
                            "2023-01-02\tfalse\tfalse\tfalse\t-1\n" +
                            "2023-01-03\tfalse\tfalse\tfalse\t-1\n");
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
                    """
                            index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\thasParquetGenerated\tisParquet\tparquetFileSize\tseqTxn\tisRemotelyServed
                            0\tMONTH\t2023-06\t2023-06-01T00:00:00.000000Z\t2023-06-25T00:00:00.000000Z\t97\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            null\tMONTH\t2023-01.detached\t2023-01-01T06:00:00.000000Z\t2023-01-31T18:00:00.000000Z\t123\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            null\tMONTH\t2023-02.detached\t2023-02-01T00:00:00.000000Z\t2023-02-28T18:00:00.000000Z\t112\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            null\tMONTH\t2023-03.detached\t2023-03-01T00:00:00.000000Z\t2023-03-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            null\tMONTH\t2023-04.detached\t2023-04-01T00:00:00.000000Z\t2023-04-30T18:00:00.000000Z\t120\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            null\tMONTH\t2023-05.detached\t2023-05-01T00:00:00.000000Z\t2023-05-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            null\tMONTH\t2023-02.attachable\t2023-02-01T00:00:00.000000Z\t2023-02-28T18:00:00.000000Z\t112\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\ttrue\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            null\tMONTH\t2023-03.attachable\t2023-03-01T00:00:00.000000Z\t2023-03-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\ttrue\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            null\tMONTH\t2023-04.attachable\t2023-04-01T00:00:00.000000Z\t2023-04-30T18:00:00.000000Z\t120\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\ttrue\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            """,
                    tableName);
            execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2023-02', '2023-03'", sqlExecutionContext);
            if (isWal) {
                drainWalQueue();
            }
            assertShowPartitions(
                    """
                            index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\thasParquetGenerated\tisParquet\tparquetFileSize\tseqTxn\tisRemotelyServed
                            0\tMONTH\t2023-02\t2023-02-01T00:00:00.000000Z\t2023-02-28T18:00:00.000000Z\t112\tSIZE\tHUMAN\ttrue\tfalse\ttrue\tfalse\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            1\tMONTH\t2023-03\t2023-03-01T00:00:00.000000Z\t2023-03-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\ttrue\tfalse\ttrue\tfalse\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            2\tMONTH\t2023-06\t2023-06-01T00:00:00.000000Z\t2023-06-25T00:00:00.000000Z\t97\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            null\tMONTH\t2023-01.detached\t2023-01-01T06:00:00.000000Z\t2023-01-31T18:00:00.000000Z\t123\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            null\tMONTH\t2023-02.detached\t2023-02-01T00:00:00.000000Z\t2023-02-28T18:00:00.000000Z\t112\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            null\tMONTH\t2023-03.detached\t2023-03-01T00:00:00.000000Z\t2023-03-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            null\tMONTH\t2023-04.detached\t2023-04-01T00:00:00.000000Z\t2023-04-30T18:00:00.000000Z\t120\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            null\tMONTH\t2023-05.detached\t2023-05-01T00:00:00.000000Z\t2023-05-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            null\tMONTH\t2023-04.attachable\t2023-04-01T00:00:00.000000Z\t2023-04-30T18:00:00.000000Z\t120\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\ttrue\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            """,
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
                    """
                            index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\thasParquetGenerated\tisParquet\tparquetFileSize\tseqTxn\tisRemotelyServed
                            0\tMONTH\t2023-06\t2023-06-01T00:00:00.000000Z\t2023-06-25T00:00:00.000000Z\t97\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            null\tMONTH\t2023-01.detached\t2023-01-01T06:00:00.000000Z\t2023-01-31T18:00:00.000000Z\t123\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            null\tMONTH\t2023-02.detached\t2023-02-01T00:00:00.000000Z\t2023-02-28T18:00:00.000000Z\t112\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            null\tMONTH\t2023-03.detached\t2023-03-01T00:00:00.000000Z\t2023-03-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            null\tMONTH\t2023-04.detached\t\t\t-1\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            null\tMONTH\t2023-05.detached\t2023-05-01T00:00:00.000000Z\t2023-05-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            """,
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
                    """
                            index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\thasParquetGenerated\tisParquet\tparquetFileSize\tseqTxn\tisRemotelyServed
                            0\tMONTH\t2023-06\t2023-06-01T00:00:00.000000Z\t2023-06-25T00:00:00.000000Z\t97\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            null\tMONTH\t2023-01.detached\t2023-01-01T06:00:00.000000Z\t2023-01-31T18:00:00.000000Z\t123\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            null\tMONTH\t2023-02.detached\t2023-02-01T00:00:00.000000Z\t2023-02-28T18:00:00.000000Z\t112\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            null\tMONTH\t2023-03.detached\t2023-03-01T00:00:00.000000Z\t2023-03-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            null\tMONTH\t2023-04.detached\t\t\t120\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            null\tMONTH\t2023-05.detached\t2023-05-01T00:00:00.000000Z\t2023-05-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            """,
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
                    """
                            index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\thasParquetGenerated\tisParquet\tparquetFileSize\tseqTxn\tisRemotelyServed
                            0\tMONTH\t2023-06\t2023-06-01T00:00:00.000000Z\t2023-06-25T00:00:00.000000Z\t97\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            null\tMONTH\t2023-01.detached\t2023-01-01T06:00:00.000000Z\t2023-01-31T18:00:00.000000Z\t123\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            null\tMONTH\t2023-02.detached\t2023-02-01T00:00:00.000000Z\t2023-02-28T18:00:00.000000Z\t112\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            null\tMONTH\t2023-03.detached\t2023-03-01T00:00:00.000000Z\t2023-03-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            null\tMONTH\t2023-04.detached\t\t\t-1\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            null\tMONTH\t2023-05.detached\t2023-05-01T00:00:00.000000Z\t2023-05-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            """,
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
                    """
                            index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\thasParquetGenerated\tisParquet\tparquetFileSize\tseqTxn\tisRemotelyServed
                            null\tMONTH\t2023-01.detached\t2023-01-01T06:00:00.000000Z\t2023-01-31T18:00:00.000000Z\t123\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            null\tMONTH\t2023-02.detached\t2023-02-01T00:00:00.000000Z\t2023-02-28T18:00:00.000000Z\t112\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            null\tMONTH\t2023-03.detached\t2023-03-01T00:00:00.000000Z\t2023-03-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            null\tMONTH\t2023-04.detached\t2023-04-01T00:00:00.000000Z\t2023-04-30T18:00:00.000000Z\t120\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            null\tMONTH\t2023-05.detached\t2023-05-01T00:00:00.000000Z\t2023-05-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            """,
                    tableName);
        });
    }

    @Test
    public void testShowPartitionsParquetAllNullValueColumnRowGroup() throws Exception {
        // Force a row group whose entire non-timestamp column is NULL
        // (null_count == num_values boundary). The _pm row-group min/max for
        // the timestamp must remain accurate; SHOW PARTITIONS surfaces only
        // the timestamp range, but the underlying _pm contains chunks for
        // every column. A regression that crashes on an all-NULL chunk's
        // stats would surface as a SHOW PARTITIONS failure rather than a
        // silent garbled timestamp.
        String tableName = testTableName(testName.getMethodName());
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE " + tableName + " (id INT, v LONG, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY" + (isWal ? " WAL" : "")
            );
            // Eight rows in one partition — the first row group of 4 has v
            // entirely NULL, the second has v populated. Timestamp column is
            // never NULL (designated timestamps cannot be). The trailing
            // 2023-01-02 row pushes 2023-01-01 out of the "active partition"
            // slot so non-WAL CONVERT is allowed against it.
            execute(
                    "INSERT INTO " + tableName + " VALUES" +
                            "(1, NULL, '2023-01-01T00:00:00.000000Z'),"
                            + "(2, NULL, '2023-01-01T01:00:00.000000Z'),"
                            + "(3, NULL, '2023-01-01T02:00:00.000000Z'),"
                            + "(4, NULL, '2023-01-01T03:00:00.000000Z'),"
                            + "(5, 5,    '2023-01-01T04:00:00.000000Z'),"
                            + "(6, 6,    '2023-01-01T05:00:00.000000Z'),"
                            + "(7, 7,    '2023-01-01T06:00:00.000000Z'),"
                            + "(8, 8,    '2023-01-01T07:00:00.000000Z'),"
                            + "(9, 9,    '2023-01-02T00:00:00.000000Z')"
            );
            if (isWal) {
                drainWalQueue();
            }
            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2023-01-01'");
            if (isWal) {
                drainWalQueue();
            }

            assertQuery("SELECT name, minTimestamp, maxTimestamp, numRows, isParquet" +
                    " FROM table_partitions('" + tableName + "')" +
                    " WHERE attached" +
                    " ORDER BY name")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns("""
                            name\tminTimestamp\tmaxTimestamp\tnumRows\tisParquet
                            2023-01-01\t2023-01-01T00:00:00.000000Z\t2023-01-01T07:00:00.000000Z\t8\ttrue
                            2023-01-02\t2023-01-02T00:00:00.000000Z\t2023-01-02T00:00:00.000000Z\t1\tfalse
                            """);

            // Underlying data is still queryable — sum() over the partial-NULL
            // column returns the populated values without exploding on the
            // all-NULL chunk.
            assertQuery("SELECT sum(v) FROM " + tableName)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("sum\n35\n");
        });
    }

    @Test
    public void testShowPartitionsParquetFormatWithoutGeneratedFlag() throws Exception {
        // A partition can be in parquet FORMAT while its parquet-generated bit is
        // cleared. Parquet conversions go through
        // switchNativePartitionWithParquet and recovery paths can reset the
        // generated bit independently, so isParquet ends up true while the raw
        // parquetGenerated bit is false. SHOW PARTITIONS must report
        // hasParquetGenerated as true whenever the partition is parquet: a parquet
        // partition implies a parquet file was generated for it.
        String tableName = testTableName(testName.getMethodName());
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE " + tableName + " AS (" +
                            "    SELECT x::INT id," +
                            "        timestamp_sequence('2023-01-01', 24 * 3600 * 1_000_000L) ts" +
                            "    FROM long_sequence(3)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY" + (isWal ? " WAL" : "")
            );
            if (isWal) {
                drainWalQueue();
            }
            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2023-01-01'");
            if (isWal) {
                drainWalQueue();
            }

            // Clear the parquet-generated bit while leaving the partition in
            // parquet format, mimicking the divergent state described above.
            TableToken token = engine.verifyTableName(tableName);
            try (TableWriter writer = engine.getWriter(token, "test")) {
                TxWriter tx = writer.getTxWriter();
                Assert.assertTrue("partition must be parquet format", tx.isPartitionParquet(0));
                Assert.assertTrue("partition must be parquet generated", tx.isPartitionParquetGenerated(0));
                // Clear only the generated bit, preserving the parquet file size so the
                // partition stays a valid parquet-format partition.
                tx.setPartitionParquetGeneratedByRawIndex(0, false);
                tx.bumpPartitionTableVersion();
                tx.commit(writer.getDenseSymbolMapWriters());
                Assert.assertFalse("raw parquet-generated bit must be cleared", tx.isPartitionParquetGenerated(0));
                Assert.assertTrue("partition must remain parquet format", tx.isPartitionParquet(0));
            }

            // Despite the cleared raw bit, the parquet partition reports
            // hasParquetGenerated as true.
            assertQuery("SELECT name, hasParquetGenerated, isParquet" +
                    " FROM table_partitions('" + tableName + "')" +
                    " WHERE attached" +
                    " ORDER BY name")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns("""
                            name\thasParquetGenerated\tisParquet
                            2023-01-01\ttrue\ttrue
                            2023-01-02\tfalse\tfalse
                            2023-01-03\tfalse\tfalse
                            """);
        });
    }

    @Test
    public void testShowPartitionsParquetWithNullsInValueColumn() throws Exception {
        // A converted partition that has NULL values in a non-timestamp
        // column. The _pm carries a per-chunk null_count for v, but the
        // designated timestamp column is unaffected. SHOW PARTITIONS must
        // still surface the correct ts min/max.
        String tableName = testTableName(testName.getMethodName());
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE " + tableName + " (id INT, v LONG, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY" + (isWal ? " WAL" : "")
            );
            execute(
                    "INSERT INTO " + tableName + " VALUES" +
                            "(1, 10,   '2023-01-01T00:00:00.000000Z'),"
                            + "(2, NULL, '2023-01-01T06:00:00.000000Z'),"
                            + "(3, 30,   '2023-01-01T12:00:00.000000Z'),"
                            + "(4, NULL, '2023-01-01T18:00:00.000000Z'),"
                            + "(5, 50,   '2023-01-02T00:00:00.000000Z')"
            );
            if (isWal) {
                drainWalQueue();
            }
            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2023-01-01'");
            if (isWal) {
                drainWalQueue();
            }

            assertQuery("SELECT name, minTimestamp, maxTimestamp, numRows, isParquet" +
                    " FROM table_partitions('" + tableName + "')" +
                    " WHERE attached" +
                    " ORDER BY name")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns("""
                            name\tminTimestamp\tmaxTimestamp\tnumRows\tisParquet
                            2023-01-01\t2023-01-01T00:00:00.000000Z\t2023-01-01T18:00:00.000000Z\t4\ttrue
                            2023-01-02\t2023-01-02T00:00:00.000000Z\t2023-01-02T00:00:00.000000Z\t1\tfalse
                            """);

            // NULLs survived the round trip and the underlying data is
            // queryable.
            assertQuery("SELECT count() FROM " + tableName + " WHERE v IS NULL")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n2\n");
        });
    }

    @Test
    public void testShowPartitionsSelectActive() throws Exception {
        String tableName = testTableName(testName.getMethodName());
        assertMemoryLeak(() -> {
            createTable(tableName);
            assertQuery("SELECT * FROM table_partitions('" + tableName + "') WHERE active = true;")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns(replaceSizeToMatchOS(
                            """
                                    index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\thasParquetGenerated\tisParquet\tparquetFileSize\tseqTxn\tisRemotelyServed
                                    5\tMONTH\t2023-06\t2023-06-01T00:00:00.000000Z\t2023-06-25T00:00:00.000000Z\t97\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                                    """,
                            tableName, configuration, engine, sink));
        });
    }

    @Test
    public void testShowPartitionsSelectActiveByWeek() throws Exception {
        String tableName = testTableName(testName.getMethodName());
        assertMemoryLeak(() -> {
            createTable(tableName, PartitionBy.WEEK);
            assertQuery("SELECT * FROM table_partitions('" + tableName + "') WHERE active = true;")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns(replaceSizeToMatchOS(
                            """
                                    index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\thasParquetGenerated\tisParquet\tparquetFileSize\tseqTxn\tisRemotelyServed
                                    25\tWEEK\t2023-W25\t2023-06-19T00:00:00.000000Z\t2023-06-25T00:00:00.000000Z\t25\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                                    """,
                            tableName, configuration, engine, sink));
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
            assertQuery("SELECT * FROM partitions WHERE active = true;")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns(replaceSizeToMatchOS(
                            """
                                    index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\thasParquetGenerated\tisParquet\tparquetFileSize\tseqTxn\tisRemotelyServed
                                    5\tMONTH\t2023-06\t2023-06-01T00:00:00.000000Z\t2023-06-25T00:00:00.000000Z\t97\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                                    """,
                            tableName, configuration, engine, sink));
        });
    }

    @Test
    public void testShowPartitionsSeqTxn() throws Exception {
        // Focused value check for the seqTxn column (the rendering tests mask it behind a
        // per-row token). Native seqTxn comes from _txn field 3; parquet seqTxn from the _pm footer.
        String tableName = testTableName(testName.getMethodName());
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE " + tableName + " AS (" +
                            "    SELECT x::INT id," +
                            "        timestamp_sequence('2023-01-01', 24 * 3600 * 1_000_000L) ts" +
                            "    FROM long_sequence(3)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY" + (isWal ? " WAL" : "")
            );
            if (isWal) {
                drainWalQueue();
            }

            String query = "SELECT name, isParquet, seqTxn" +
                    " FROM table_partitions('" + tableName + "')" +
                    " WHERE attached" +
                    " ORDER BY name";

            if (isWal) {
                // Every native partition written by the single CTAS commit carries that commit's seqTxn.
                assertQuery(query)
                        .noLeakCheck()
                        .sizeMayVary()
                        .returns("""
                                name\tisParquet\tseqTxn
                                2023-01-01\tfalse\t1
                                2023-01-02\tfalse\t1
                                2023-01-03\tfalse\t1
                                """);
                // An O3 insert into the oldest partition is a new commit; only that partition's seqTxn moves.
                execute("INSERT INTO " + tableName + " VALUES (99, '2023-01-01T12:00:00.000000Z')");
                drainWalQueue();
                assertQuery(query)
                        .noLeakCheck()
                        .sizeMayVary()
                        .returns("""
                                name\tisParquet\tseqTxn
                                2023-01-01\tfalse\t2
                                2023-01-02\tfalse\t1
                                2023-01-03\tfalse\t1
                                """);
            } else {
                // Non-WAL native partitions are never stamped; seqTxn has no value, rendered null.
                assertQuery(query)
                        .noLeakCheck()
                        .sizeMayVary()
                        .returns("""
                                name\tisParquet\tseqTxn
                                2023-01-01\tfalse\tnull
                                2023-01-02\tfalse\tnull
                                2023-01-03\tfalse\tnull
                                """);
            }

            // Converting the oldest partition to parquet moves its seqTxn into the _pm footer.
            // The footer carries the partition's own data seqTxn (the O3 insert's commit, 2),
            // not the conversion commit's: conversion rewrites the storage format, not the data,
            // so the stamp stays at the last write the parquet contains. A non-WAL table has no
            // WAL seqTxn, so the footer records 0, which renders null (no value).
            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2023-01-01'");
            if (isWal) {
                drainWalQueue();
            }
            assertQuery("SELECT name, isParquet, seqTxn" +
                    " FROM table_partitions('" + tableName + "')" +
                    " WHERE name = '2023-01-01'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("name\tisParquet\tseqTxn\n2023-01-01\ttrue\t" + (isWal ? "2" : "null") + "\n");
        });
    }

    @Test
    public void testShowPartitionsTableDoesNotExist() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("show partitions from banana")
                    .fails(21, "table does not exist [table=banana]");
            assertQuery("SELECT * FROM table_partitions('banana')")
                    .fails(31, "table does not exist [table=banana]");
        });
    }

    @Test
    public void testShowPartitionsWhenPartitionsAreConvertedToParquet() throws Exception {
        String tableName = testTableName(testName.getMethodName());
        assertMemoryLeak(() -> {
            createTable(tableName);
            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET WHERE timestamp < '2023-04-01T00:00:00.000000Z'");
            if (isWal) {
                drainWalQueue();
            }
            // Three partitions are converted: 2023-01, 2023-02, 2023-03.
            // The remaining three stay native. Both hasParquetGenerated and isParquet
            // must be true for converted partitions; parquetFileSize must be > 0.
            assertQuery("SELECT name, hasParquetGenerated, isParquet, parquetFileSize > 0 hasParquetFile" +
                    " FROM table_partitions('" + tableName + "')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            name\thasParquetGenerated\tisParquet\thasParquetFile
                            2023-01\ttrue\ttrue\ttrue
                            2023-02\ttrue\ttrue\ttrue
                            2023-03\ttrue\ttrue\ttrue
                            2023-04\tfalse\tfalse\tfalse
                            2023-05\tfalse\tfalse\tfalse
                            2023-06\tfalse\tfalse\tfalse
                            """);
        });
    }

    @Test
    public void testShowPartitionsWhenThereAreNoDetachedNorAttachable() throws Exception {
        String tableName = testTableName(testName.getMethodName());
        assertMemoryLeak(() -> {
            createTable(tableName);
            assertShowPartitions(
                    """
                            index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\thasParquetGenerated\tisParquet\tparquetFileSize\tseqTxn\tisRemotelyServed
                            0\tMONTH\t2023-01\t2023-01-01T06:00:00.000000Z\t2023-01-31T18:00:00.000000Z\t123\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            1\tMONTH\t2023-02\t2023-02-01T00:00:00.000000Z\t2023-02-28T18:00:00.000000Z\t112\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            2\tMONTH\t2023-03\t2023-03-01T00:00:00.000000Z\t2023-03-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            3\tMONTH\t2023-04\t2023-04-01T00:00:00.000000Z\t2023-04-30T18:00:00.000000Z\t120\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            4\tMONTH\t2023-05\t2023-05-01T00:00:00.000000Z\t2023-05-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            5\tMONTH\t2023-06\t2023-06-01T00:00:00.000000Z\t2023-06-25T00:00:00.000000Z\t97\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                            """,
                    tableName);
        });
    }

    @Test
    public void testShowPartitionsWhenThereAreNoDetachedNorAttachableMissingTimestampColumn() throws Exception {
        String tableName = testTableName(testName.getMethodName());
        createTable(tableName);
        deleteFile(tableName, "2023-04", "timestamp.d");

        final String finallyExpected = replaceSizeToMatchOS(
                """
                        index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\thasParquetGenerated\tisParquet\tparquetFileSize\tseqTxn\tisRemotelyServed
                        0\tMONTH\t2023-01\t2023-01-01T06:00:00.000000Z\t2023-01-31T18:00:00.000000Z\t123\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                        1\tMONTH\t2023-02\t2023-02-01T00:00:00.000000Z\t2023-02-28T18:00:00.000000Z\t112\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                        2\tMONTH\t2023-03\t2023-03-01T00:00:00.000000Z\t2023-03-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                        null\tMONTH\t2023-04\t\t\t120\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                        4\tMONTH\t2023-05\t2023-05-01T00:00:00.000000Z\t2023-05-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                        5\tMONTH\t2023-06\t2023-06-01T00:00:00.000000Z\t2023-06-25T00:00:00.000000Z\t97\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\tfalse\tfalse\t-1\tSEQTXN\tfalse
                        """,
                tableName, configuration, engine, sink
        );

        engine.releaseInactive();

        assertQuery("SELECT * FROM table_partitions('" + tableName + "')")
                .ddl(null)
                .noRandomAccess()
                .expectSize()
                .returns(finallyExpected);

        assertQuery("show partitions from " + tableName)
                .ddl(null)
                .noRandomAccess()
                .expectSize()
                .returns(finallyExpected);
    }

    @Test
    public void testShowPartitionsWithParquetPartition() throws Exception {
        String tableName = testTableName(testName.getMethodName());
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE " + tableName + " AS (" +
                            "    SELECT x::INT id," +
                            "        timestamp_sequence('2023-01-01', 24 * 3600 * 1_000_000L) ts" +
                            "    FROM long_sequence(3)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY" + (isWal ? " WAL" : "")
            );
            if (isWal) {
                drainWalQueue();
            }
            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2023-01-01'");
            if (isWal) {
                drainWalQueue();
            }
            // Verify parquet partition has correct timestamps and isParquet flag.
            // This exercises the ShowPartitionsRecordCursorFactory code path that
            // reads min/max timestamps from the _pm sidecar file.
            assertQuery("SELECT name, minTimestamp, maxTimestamp, numRows, isParquet" +
                    " FROM table_partitions('" + tableName + "')" +
                    " WHERE attached" +
                    " ORDER BY name")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns("""
                            name\tminTimestamp\tmaxTimestamp\tnumRows\tisParquet
                            2023-01-01\t2023-01-01T00:00:00.000000Z\t2023-01-01T00:00:00.000000Z\t1\ttrue
                            2023-01-02\t2023-01-02T00:00:00.000000Z\t2023-01-02T00:00:00.000000Z\t1\tfalse
                            2023-01-03\t2023-01-03T00:00:00.000000Z\t2023-01-03T00:00:00.000000Z\t1\tfalse
                            """);
        });
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

    private void assertShowPartitions(String expected, String tableName) throws Exception {
        SOCountDownLatch done = new SOCountDownLatch(1);
        String finallyExpected = replaceSizeToMatchOS(expected, tableName, configuration, engine, sink);
        AtomicInteger failureCounter = new AtomicInteger();
        new Thread(() -> {
            try {
                try {
                    assertQuery("show partitions from " + tableName)
                            .noLeakCheck()
                            .noRandomAccess()
                            .sizeMayVary()
                            .returns(finallyExpected);
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
        assertQuery("SELECT * FROM table_partitions('" + tableName + "')")
                .noLeakCheck()
                .noRandomAccess()
                .sizeMayVary()
                .returns(finallyExpected);
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
            engine.setPoolListener((factoryType, _, tableToken, event, _, _) -> {
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
