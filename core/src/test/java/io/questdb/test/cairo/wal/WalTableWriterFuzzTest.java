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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.std.DirectBinarySequence;
import io.questdb.std.Files;
import io.questdb.std.Hash;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.tasks.WalTxnNotificationTask;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.griffin.AbstractMultiNodeTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

public class WalTableWriterFuzzTest extends AbstractMultiNodeTest {

    @Before
    public void setUp() {
        super.setUp();
        setCurrentMicros(0);
    }

    @Test
    public void testNonStructuralAlterViaWal() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final String tableCopyName = tableName + "_copy";
            TableToken tableToken = createTableAndCopy(tableName, tableCopyName);

            long tsIncrement = Micros.SECOND_MICROS;
            long ts = MicrosTimestampDriver.floor("2022-07-14T00:00:00");
            int rowCount = (int) (Files.PAGE_SIZE / 32);
            ts += (Micros.SECOND_MICROS * (60 * 60 - rowCount - 10));
            Rnd rnd = TestUtils.generateRandom(LOG);

            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    WalWriter walWriter = engine.getWalWriter(tableToken)
            ) {
                final int tableId = addRowsToWalAndApplyToTable(0, tableName, tableCopyName, rowCount, tsIncrement, ts, rnd, walWriter, true);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);

                updateMaxUncommittedRows(tableName, 60, tableId);
                assertMaxUncommittedRows(tableName, 60);
                updateMaxUncommittedRows(tableCopyName, 60);
                assertMaxUncommittedRows(tableCopyName, 60);
                updateMaxUncommittedRows(tableName, 55, tableId);
                assertMaxUncommittedRows(tableName, 55);
                updateMaxUncommittedRows(tableCopyName, 55);
                assertMaxUncommittedRows(tableCopyName, 55);
                updateMaxUncommittedRows(tableName, 50, tableId);
                assertMaxUncommittedRows(tableName, 50);
                updateMaxUncommittedRows(tableName, 77, tableId);
                assertMaxUncommittedRows(tableName, 77);

                // assert that data is not changed
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);
            }
        });
    }

    @Test
    public void testOutOfOrderDuplicateTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY)
                    .col("i", ColumnType.INT)
                    .timestamp("ts")
                    .wal();
            TableToken tt = createTable(model);

            try (WalWriter walWriter = engine.getWalWriter(tt)) {
                TableWriter.Row row = walWriter.newRow(1000);
                row.putInt(0, 1);
                row.append();
                // second row is out-of-order
                row = walWriter.newRow(500);
                row.putInt(0, 2);
                row.append();
                // third row is in-order
                row = walWriter.newRow(1500);
                row.putInt(0, 3);
                row.append();
                // forth row is in-order with duplicate timestamp
                row = walWriter.newRow(1500);
                row.putInt(0, 4);
                row.append();
                walWriter.commit();

                drainWalQueue();
            }

            assertSql("i\tts\n" +
                    "2\t1970-01-01T00:00:00.000500Z\n" +
                    "1\t1970-01-01T00:00:00.001000Z\n" +
                    "3\t1970-01-01T00:00:00.001500Z\n" +
                    "4\t1970-01-01T00:00:00.001500Z\n", tableName);
        });
    }

    @Test
    public void testPartitionOverflowAppend() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final String tableCopyName = tableName + "_copy";
            TableToken tt = createTableAndCopy(tableName, tableCopyName);

            long tsIncrement = Micros.SECOND_MICROS;
            long ts = MicrosTimestampDriver.floor("2022-07-14T00:00:00");
            int rowCount = (int) (Files.PAGE_SIZE / 32);
            ts += (Micros.SECOND_MICROS * (60 * 60 - rowCount - 10));
            Rnd rnd = TestUtils.generateRandom(LOG);

            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    WalWriter walWriter = engine.getWalWriter(tt)
            ) {
                long start = ts;
                addRowsToWalAndApplyToTable(0, tableName, tableCopyName, rowCount, tsIncrement, start, rnd, walWriter, true);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);

                start += rowCount * tsIncrement + 1;
                addRowsToWalAndApplyToTable(1, tableName, tableCopyName, rowCount, tsIncrement, start, rnd, walWriter, true);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);
            }
        });
    }

    @Test
    public void testPartitionOverflowMerge() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final String tableCopyName = tableName + "_copy";
            TableToken tt = createTableAndCopy(tableName, tableCopyName);

            long tsIncrement = Micros.SECOND_MICROS;
            long ts = MicrosTimestampDriver.floor("2022-07-14T00:00:00");
            int rowCount = (int) (Files.PAGE_SIZE / 32);
            ts += (Micros.SECOND_MICROS * (60 * 60 - rowCount - 10));
            Rnd rnd = TestUtils.generateRandom(LOG);

            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    WalWriter walWriter = engine.getWalWriter(tt)
            ) {
                long start = ts;
                addRowsToWalAndApplyToTable(0, tableName, tableCopyName, rowCount, tsIncrement, start, rnd, walWriter, true);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);

                start += rowCount * tsIncrement - 2 * Micros.SECOND_MICROS;
                addRowsToWalAndApplyToTable(1, tableName, tableCopyName, rowCount, tsIncrement, start, rnd, walWriter, true);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);
            }
        });
    }

    @Test
    public void testRandomInOutOfOrderMultipleWalInserts() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final String tableCopyName = tableName + "_copy";
            TableToken tt = createTableAndCopy(tableName, tableCopyName);

            long tsIncrement;
            long now = Os.currentTimeMicros();
            LOG.info().$("now :").$(now).$();
            Rnd rnd = TestUtils.generateRandom(LOG);

            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    WalWriter walWriter1 = engine.getWalWriter(tt);
                    WalWriter walWriter2 = engine.getWalWriter(tt);
                    WalWriter walWriter3 = engine.getWalWriter(tt)
            ) {
                long start = now;
                WalWriter[] writers = new WalWriter[]{walWriter1, walWriter2, walWriter3};

                for (int i = 0; i < 5; i++) {
                    boolean inOrder = rnd.nextBoolean();
                    int walIndex = rnd.nextInt(writers.length);
                    WalWriter walWriter = writers[walIndex];
                    int rowCount = rnd.nextInt(1000) + 2;
                    tsIncrement = rnd.nextLong(Micros.MINUTE_MICROS);

                    LOG.infoW().$("generating wal [")
                            .$("iteration:").$(i)
                            .$(", walIndex: ").$(walIndex)
                            .$(", inOrder: ").$(inOrder)
                            .$(" rowCount: ").$(rowCount)
                            .$(" tsIncrement: ").$(tsIncrement)
                            .I$();

                    addRowsToWalAndApplyToTable(i, tableName, tableCopyName, rowCount, tsIncrement, start, rnd, walWriter, inOrder);

                    LOG.info().$("verifying wal [").$("iteration:").$(i).I$();
                    TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);

                    start += rowCount * tsIncrement + 1;
                }
            }
        });
    }

    @Test
    public void testRandomInOutOfOrderOverlappingInserts() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final String tableCopyName = tableName + "_copy";
            TableToken tableToken = createTableAndCopy(tableName, tableCopyName);

            long tsIncrement;
            long now = Os.currentTimeMicros();
            LOG.info().$("now :").$(now).$();
            Rnd rnd = TestUtils.generateRandom(LOG);

            int releaseWriterSeed = 2;
            int overlapSeed = 3;

            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    WalWriter walWriter1 = engine.getWalWriter(tableToken);
                    WalWriter walWriter2 = engine.getWalWriter(tableToken);
                    WalWriter walWriter3 = engine.getWalWriter(tableToken)
            ) {

                long start = now;
                WalWriter[] writers = new WalWriter[]{walWriter1, walWriter2, walWriter3};

                for (int i = 0; i < 20; i++) {
                    boolean inOrder = rnd.nextBoolean();
                    int walIndex = rnd.nextInt(writers.length);
                    WalWriter walWriter = writers[walIndex];
                    int rowCount = rnd.nextInt(10000) + 1;
                    int partitions = rnd.nextInt(3) + 1;
                    tsIncrement = partitions * Micros.HOUR_MICROS / rowCount;
                    long tsOffset = rnd.nextLong(2 * Micros.HOUR_MICROS);
                    int sign = rnd.nextInt(overlapSeed);
                    tsOffset *= sign == 0 ? -1 : 1;
                    start += tsOffset;

                    LOG.infoW().$("generating wal [")
                            .$("iteration:").$(i)
                            .$(", walIndex: ").$(walIndex)
                            .$(", inOrder: ").$(inOrder)
                            .$(" rowCount: ").$(rowCount)
                            .$(", tsIncrement: ").$(tsIncrement)
                            .$(", tsOffset: ").$(tsOffset)
                            .I$();

                    if (rnd.nextInt(releaseWriterSeed) == 0) {
                        // Close writer
                        LOG.info().$("=== releasing writers ===").$();
                        engine.releaseInactive();
                    }

                    addRowsToWalAndApplyToTable(i, tableName, tableCopyName, rowCount, tsIncrement, start, rnd, walWriter, inOrder);

                    LOG.info().$("verifying wal [").$("iteration:").$(i).I$();
                    TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);

                    engine.releaseInactive();
                    TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);

                    start += rowCount * tsIncrement + 1;
                }
            }
        });
    }

    @Test
    public void testReadAndWriteAllTypes() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final String tableCopyName = tableName + "_copy";
            TableToken tableToken = createTableAndCopy(tableName, tableCopyName);

            final int rowsToInsertTotal = 100;
            final long tsIncrement = 1000;
            long ts = Os.currentTimeMicros();

            Rnd rnd = new Rnd();
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    WalWriter walWriter = engine.getWalWriter(tableToken)
            ) {
                addRowsToWalAndApplyToTable(0, tableName, tableCopyName, rowsToInsertTotal, tsIncrement, ts, rnd, walWriter, true);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);
            }
        });
    }

    @Test
    public void testReadAndWriteFrom2WallsInOrder() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final String tableCopyName = tableName + "_copy";
            TableToken tableToken = createTableAndCopy(tableName, tableCopyName);

            final int rowsToInsertTotal = 100;
            final long tsIncrement = 1000;
            long ts = Os.currentTimeMicros();

            Rnd rnd = new Rnd();
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    WalWriter walWriter1 = engine.getWalWriter(tableToken);
                    WalWriter walWriter2 = engine.getWalWriter(tableToken)
            ) {

                addRowsToWalAndApplyToTable(0, tableName, tableCopyName, rowsToInsertTotal, tsIncrement, ts, rnd, walWriter1, true);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);

                addRowsToWalAndApplyToTable(1, tableName, tableCopyName, rowsToInsertTotal, tsIncrement, ts + rowsToInsertTotal * tsIncrement, rnd, walWriter2, true);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);
            }
        });
    }

    @Test
    public void testReadAndWriteFrom2WallsOutOfOrder() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final String tableCopyName = tableName + "_copy";
            TableToken tableToken = createTableAndCopy(tableName, tableCopyName);

            final int rowsToInsertTotal = 100;
            final long tsIncrement = 1000;
            long ts = Os.currentTimeMicros();

            Rnd rnd = new Rnd();
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    WalWriter walWriter1 = engine.getWalWriter(tableToken);
                    WalWriter walWriter2 = engine.getWalWriter(tableToken)
            ) {

                long start = ts;
                addRowsToWalAndApplyToTable(0, tableName, tableCopyName, rowsToInsertTotal, tsIncrement, start, rnd, walWriter1, false);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);

                start = ts + rowsToInsertTotal * tsIncrement;
                addRowsToWalAndApplyToTable(1, tableName, tableCopyName, 2 * rowsToInsertTotal, tsIncrement, start, rnd, walWriter2, false);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);

                start = ts + 3 * rowsToInsertTotal * tsIncrement;
                addRowsToWalAndApplyToTable(2, tableName, tableCopyName, rowsToInsertTotal, tsIncrement, start, rnd, walWriter1, false);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);
            }
        });
    }

    @Test
    public void testUpdateViaWal_CopyIntoDeletedColumn() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.INT)
                    .timestamp("ts")
                    .wal();
            TableToken tableToken = createTable(model);

            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                TableWriter.Row row = walWriter.newRow(0);
                row.putInt(0, 10);
                row.append();
                row = walWriter.newRow(0);
                row.putInt(0, 11);
                row.append();
                row = walWriter.newRow(0);
                row.putInt(0, 12);
                row.append();
                walWriter.commit();

                WalWriterTest.removeColumn(walWriter, "b");

                assertExceptionNoLeakCheck("UPDATE " + tableName + " SET b = a");
            } catch (Exception e) {
                assertTrue(e.getMessage().endsWith("Invalid column: b"));
            }
        });
    }

    @Test
    public void testUpdateViaWal_CopyIntoNewColumn() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.INT)
                    .timestamp("ts")
                    .wal();
            TableToken tableToken = createTable(model);

            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                TableWriter.Row row = walWriter.newRow(0);
                row.putInt(0, 10);
                row.append();
                row = walWriter.newRow(0);
                row.putInt(0, 11);
                row.append();
                row = walWriter.newRow(0);
                row.putInt(0, 12);
                row.append();
                walWriter.commit();

                addColumn(walWriter, "c", ColumnType.INT);
                drainWalQueue();

                update("UPDATE " + tableName + " SET b = a");
                update("UPDATE " + tableName + " SET c = a");
                drainWalQueue();
            }

            assertSql("a\tb\tts\tc\n" +
                    "10\t10\t1970-01-01T00:00:00.000000Z\t10\n" +
                    "11\t11\t1970-01-01T00:00:00.000000Z\t11\n" +
                    "12\t12\t1970-01-01T00:00:00.000000Z\t12\n", tableName);
        });
    }

    @Test
    public void testUpdateViaWal_IndexedVariables() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final String tableCopyName = tableName + "_copy";
            TableToken tableToken = createTableAndCopy(tableName, tableCopyName);

            long tsIncrement = Micros.SECOND_MICROS;
            long ts = MicrosTimestampDriver.floor("2022-07-14T00:00:00");
            int rowCount = (int) (Files.PAGE_SIZE / 32);
            ts += (Micros.SECOND_MICROS * (60 * 60 - rowCount - 10));
            Rnd rnd = TestUtils.generateRandom(LOG);

            final int binarySize = 64;
            final long pointer = Unsafe.malloc(binarySize, MemoryTag.NATIVE_DEFAULT);
            try {
                final DirectBinarySequence binSeq = new DirectBinarySequence();
                WalWriterTest.prepareBinPayload(pointer, binarySize);
                final Utf8String varChar = new Utf8String("₴ п'ять доллярів");
                try (
                        SqlCompiler compiler = engine.getSqlCompiler();
                        WalWriter walWriter = engine.getWalWriter(tableToken)
                ) {
                    addRowsToWalAndApplyToTable(0, tableName, tableCopyName, rowCount, tsIncrement, ts, rnd, walWriter, true);
                    TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);

                    sqlExecutionContext.getBindVariableService().setInt(0, 567890);
                    sqlExecutionContext.getBindVariableService().setByte(1, (byte) 122);
                    sqlExecutionContext.getBindVariableService().setShort(2, (short) 42567);
                    sqlExecutionContext.getBindVariableService().setLong(3, 33342567L);
                    sqlExecutionContext.getBindVariableService().setFloat(4, (float) 34.34);
                    sqlExecutionContext.getBindVariableService().setDouble(5, 357.35);
                    sqlExecutionContext.getBindVariableService().setTimestamp(6, 100_000L);
                    sqlExecutionContext.getBindVariableService().setDate(7, 100_000L);
                    sqlExecutionContext.getBindVariableService().setChar(8, 'Q');
                    sqlExecutionContext.getBindVariableService().setBoolean(9, true);
                    sqlExecutionContext.getBindVariableService().setStr(10, "updated");
                    sqlExecutionContext.getBindVariableService().setStr(11, "labelUpdate");
                    sqlExecutionContext.getBindVariableService().setBin(12, binSeq.of(pointer, binarySize));
                    sqlExecutionContext.getBindVariableService().setGeoHash(13, rnd.nextGeoHashByte(5), ColumnType.getGeoHashTypeWithBits(5));
                    sqlExecutionContext.getBindVariableService().setGeoHash(14, rnd.nextGeoHashShort(10), ColumnType.getGeoHashTypeWithBits(10));
                    sqlExecutionContext.getBindVariableService().setGeoHash(15, rnd.nextGeoHashInt(20), ColumnType.getGeoHashTypeWithBits(20));
                    sqlExecutionContext.getBindVariableService().setGeoHash(16, rnd.nextGeoHashLong(35), ColumnType.getGeoHashTypeWithBits(35));
                    sqlExecutionContext.getBindVariableService().setVarchar(17, varChar);
                    sqlExecutionContext.getBindVariableService().setDecimal(18, 0, 0, 0, 9, ColumnType.getDecimalType(2, 0));
                    sqlExecutionContext.getBindVariableService().setDecimal(19, 0, 0, 0, 1234, ColumnType.getDecimalType(4, 1));
                    sqlExecutionContext.getBindVariableService().setDecimal(20, 0, 0, 0, 123456, ColumnType.getDecimalType(9, 1));
                    sqlExecutionContext.getBindVariableService().setDecimal(21, 0, 0, 0, 12345678, ColumnType.getDecimalType(18, 1));
                    sqlExecutionContext.getBindVariableService().setDecimal(22, 0, 0, 0, 1234567890, ColumnType.getDecimalType(38, 1));
                    sqlExecutionContext.getBindVariableService().setDecimal(23, 0, 0, 0, 123456789012L, ColumnType.getDecimalType(76, 1));

                    update(
                            "UPDATE " + tableName + " SET " +
                                    "INT=$1, " +
                                    "BYTE=$2, " +
                                    "SHORT=$3, " +
                                    "LONG=$4, " +
                                    "FLOAT=$5, " +
                                    "DOUBLE=$6, " +
                                    "TIMESTAMP=$7, " +
                                    "DATE=$8, " +
                                    "CHAR=$9, " +
                                    "BOOLEAN=$10, " +
                                    "STRING=$11, " +
                                    "LABEL=$12, " +
                                    "BIN=$13, " +
                                    "GEOBYTE=$14, " +
                                    "GEOSHORT=$15, " +
                                    "GEOINT=$16, " +
                                    "GEOLONG=$17, " +
                                    "VARCHAR=$18, " +
                                    "DECIMAL8=$19, " +
                                    "DECIMAL16=$20, " +
                                    "DECIMAL32=$21, " +
                                    "DECIMAL64=$22, " +
                                    "DECIMAL128=$23, " +
                                    "DECIMAL256=$24 " +
                                    "WHERE INT > 5"
                    );

                    drainWalQueue();

                    update(
                            "UPDATE " + tableCopyName + " SET " +
                                    "INT=$1, " +
                                    "BYTE=$2, " +
                                    "SHORT=$3, " +
                                    "LONG=$4, " +
                                    "FLOAT=$5, " +
                                    "DOUBLE=$6, " +
                                    "TIMESTAMP=$7, " +
                                    "DATE=$8, " +
                                    "CHAR=$9, " +
                                    "BOOLEAN=$10, " +
                                    "STRING=$11, " +
                                    "LABEL=$12, " +
                                    "BIN=$13, " +
                                    "GEOBYTE=$14, " +
                                    "GEOSHORT=$15, " +
                                    "GEOINT=$16, " +
                                    "GEOLONG=$17, " +
                                    "VARCHAR=$18, " +
                                    "DECIMAL8=$19, " +
                                    "DECIMAL16=$20, " +
                                    "DECIMAL32=$21, " +
                                    "DECIMAL64=$22, " +
                                    "DECIMAL128=$23, " +
                                    "DECIMAL256=$24 " +
                                    "WHERE INT > 5"
                    );

                    TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);
                }
            } finally {
                Unsafe.free(pointer, binarySize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testUpdateViaWal_JoinRejected() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final String tableCopyName = tableName + "_copy";
            TableToken tableToken = createTableAndCopy(tableName, tableCopyName);

            long tsIncrement = Micros.SECOND_MICROS;
            long ts = MicrosTimestampDriver.floor("2022-07-14T00:00:00");
            int rowCount = (int) (Files.PAGE_SIZE / 32);
            ts += (Micros.SECOND_MICROS * (60 * 60 - rowCount - 10));
            Rnd rnd = TestUtils.generateRandom(LOG);

            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    WalWriter walWriter = engine.getWalWriter(tableToken)
            ) {
                addRowsToWalAndApplyToTable(0, tableName, tableCopyName, rowCount, tsIncrement, ts, rnd, walWriter, true);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);

                update("UPDATE " + tableCopyName + " SET INT=12345678");
                try {
                    assertExceptionNoLeakCheck("UPDATE " + tableName + " t SET INT=12345678 FROM " + tableCopyName + " c WHERE t.INT=c.INT");
                } catch (Exception e) {
                    assertTrue(e.getMessage().endsWith("UPDATE statements with join are not supported yet for WAL tables"));
                }
            }
        });
    }

    @Test
    public void testUpdateViaWal_NamedVariables() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final String tableCopyName = tableName + "_copy";
            TableToken tableToken = createTableAndCopy(tableName, tableCopyName);

            long tsIncrement = Micros.SECOND_MICROS;
            long ts = MicrosTimestampDriver.floor("2022-07-14T00:00:00");
            int rowCount = (int) (Files.PAGE_SIZE / 32);
            ts += (Micros.SECOND_MICROS * (60 * 60 - rowCount - 10));
            Rnd rnd = TestUtils.generateRandom(LOG);

            final int binarySize = 64;
            final long pointer = Unsafe.malloc(binarySize, MemoryTag.NATIVE_DEFAULT);
            try {
                final DirectBinarySequence binSeq = new DirectBinarySequence();
                WalWriterTest.prepareBinPayload(pointer, binarySize);

                final Utf8String varChar = new Utf8String("₴ п'ять доллярів");
                try (
                        SqlCompiler compiler = engine.getSqlCompiler();
                        WalWriter walWriter = engine.getWalWriter(tableToken)
                ) {
                    addRowsToWalAndApplyToTable(0, tableName, tableCopyName, rowCount, tsIncrement, ts, rnd, walWriter, true);
                    TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);

                    sqlExecutionContext.getBindVariableService().setInt("INTVAL", 567890);
                    sqlExecutionContext.getBindVariableService().setByte("BYTEVAL", (byte) 122);
                    sqlExecutionContext.getBindVariableService().setShort("SHORTVAL", (short) 42567);
                    sqlExecutionContext.getBindVariableService().setLong("LONGVAL", 33342567L);
                    sqlExecutionContext.getBindVariableService().setFloat("FLOATVAL", (float) 34.34);
                    sqlExecutionContext.getBindVariableService().setDouble("DOUBLEVAL", 357.35);
                    sqlExecutionContext.getBindVariableService().setTimestamp("TIMESTAMPVAL", 100_000L);
                    sqlExecutionContext.getBindVariableService().setDate("DATEVAL", 100_000L);
                    sqlExecutionContext.getBindVariableService().setChar("CHARVAL", 'Q');
                    sqlExecutionContext.getBindVariableService().setBoolean("BOOLVAL", true);
                    sqlExecutionContext.getBindVariableService().setStr("STRVAL", "updated");
                    sqlExecutionContext.getBindVariableService().setStr("SYMVAL", "labelUpdate");
                    sqlExecutionContext.getBindVariableService().setBin("BINVAL", binSeq.of(pointer, binarySize));
                    sqlExecutionContext.getBindVariableService().setGeoHash("GEOBYTEVAL", rnd.nextGeoHashByte(5), ColumnType.getGeoHashTypeWithBits(5));
                    sqlExecutionContext.getBindVariableService().setGeoHash("GEOSHORTVAL", rnd.nextGeoHashShort(10), ColumnType.getGeoHashTypeWithBits(10));
                    sqlExecutionContext.getBindVariableService().setGeoHash("GEOINTVAL", rnd.nextGeoHashInt(20), ColumnType.getGeoHashTypeWithBits(20));
                    sqlExecutionContext.getBindVariableService().setGeoHash("GEOLONGVAL", rnd.nextGeoHashLong(35), ColumnType.getGeoHashTypeWithBits(35));
                    sqlExecutionContext.getBindVariableService().setUuid("UUIDVAL", rnd.nextLong(), rnd.nextLong());
                    sqlExecutionContext.getBindVariableService().setVarchar("VARCHARVAL", varChar);
                    sqlExecutionContext.getBindVariableService().setDecimal("DECIMAL8VAL", 0, 0, 0, 12, ColumnType.getDecimalType(2, 1));
                    sqlExecutionContext.getBindVariableService().setDecimal("DECIMAL16VAL", 0, 0, 0, 1234, ColumnType.getDecimalType(4, 1));
                    sqlExecutionContext.getBindVariableService().setDecimal("DECIMAL32VAL", 0, 0, 0, 123456, ColumnType.getDecimalType(9, 1));
                    sqlExecutionContext.getBindVariableService().setDecimal("DECIMAL64VAL", 0, 0, 0, 12345678, ColumnType.getDecimalType(18, 1));
                    sqlExecutionContext.getBindVariableService().setDecimal("DECIMAL128VAL", 0, 0, 0, 1234567890, ColumnType.getDecimalType(38, 1));
                    sqlExecutionContext.getBindVariableService().setDecimal("DECIMAL256VAL", 0, 0, 0, 123456789012L, ColumnType.getDecimalType(76, 1));

                    update(
                            "UPDATE " + tableName + " SET " +
                                    "INT=:INTVAL, " +
                                    "BYTE=:BYTEVAL, " +
                                    "SHORT=:SHORTVAL, " +
                                    "LONG=:LONGVAL, " +
                                    "FLOAT=:FLOATVAL, " +
                                    "DOUBLE=:DOUBLEVAL, " +
                                    "TIMESTAMP=:TIMESTAMPVAL, " +
                                    "DATE=:DATEVAL, " +
                                    "CHAR=:CHARVAL, " +
                                    "BOOLEAN=:BOOLVAL, " +
                                    "STRING=:STRVAL, " +
                                    "LABEL=:SYMVAL, " +
                                    "BIN=:BINVAL, " +
                                    "GEOBYTE=:GEOBYTEVAL, " +
                                    "GEOSHORT=:GEOSHORTVAL, " +
                                    "GEOINT=:GEOINTVAL, " +
                                    "GEOLONG=:GEOLONGVAL, " +
                                    "UUID=:UUIDVAL," +
                                    "VARCHAR=:VARCHARVAL, " +
                                    "DECIMAL8=:DECIMAL8VAL, " +
                                    "DECIMAL16=:DECIMAL16VAL, " +
                                    "DECIMAL32=:DECIMAL32VAL, " +
                                    "DECIMAL64=:DECIMAL64VAL, " +
                                    "DECIMAL128=:DECIMAL128VAL, " +
                                    "DECIMAL256=:DECIMAL256VAL " +
                                    "WHERE INT > 5"
                    );
                    drainWalQueue();

                    update(
                            "UPDATE " + tableCopyName + " SET " +
                                    "INT=:INTVAL, " +
                                    "BYTE=:BYTEVAL, " +
                                    "SHORT=:SHORTVAL, " +
                                    "LONG=:LONGVAL, " +
                                    "FLOAT=:FLOATVAL, " +
                                    "DOUBLE=:DOUBLEVAL, " +
                                    "TIMESTAMP=:TIMESTAMPVAL, " +
                                    "DATE=:DATEVAL, " +
                                    "CHAR=:CHARVAL, " +
                                    "BOOLEAN=:BOOLVAL, " +
                                    "STRING=:STRVAL, " +
                                    "LABEL=:SYMVAL, " +
                                    "BIN=:BINVAL, " +
                                    "GEOBYTE=:GEOBYTEVAL, " +
                                    "GEOSHORT=:GEOSHORTVAL, " +
                                    "GEOINT=:GEOINTVAL, " +
                                    "GEOLONG=:GEOLONGVAL, " +
                                    "UUID=:UUIDVAL, " +
                                    "VARCHAR=:VARCHARVAL, " +
                                    "DECIMAL8=:DECIMAL8VAL, " +
                                    "DECIMAL16=:DECIMAL16VAL, " +
                                    "DECIMAL32=:DECIMAL32VAL, " +
                                    "DECIMAL64=:DECIMAL64VAL, " +
                                    "DECIMAL128=:DECIMAL128VAL, " +
                                    "DECIMAL256=:DECIMAL256VAL " +
                                    "WHERE INT > 5"
                    );
                    TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);
                }
            } finally {
                Unsafe.free(pointer, binarySize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testUpdateViaWal_Now() throws Exception {
        testUpdateToNowFunction("now");
    }

    @Test
    public void testUpdateViaWal_Random() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setCurrentMicros(rnd.nextPositiveLong());
        sqlExecutionContext.getRandom().reset(currentMicros * 1000, currentMicros);

        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final String tableCopyName = tableName + "_copy";
            TableToken tableToken = createTableAndCopy(tableName, tableCopyName);

            long tsIncrement = Micros.SECOND_MICROS;
            long ts = MicrosTimestampDriver.floor("2022-07-14T00:00:00");
            int rowCount = (int) (Files.PAGE_SIZE / 32);
            ts += (Micros.SECOND_MICROS * (60 * 60 - rowCount - 10));

            final String walName;
            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                addRowsToWalAndApplyToTable(0, tableName, tableCopyName, rowCount, tsIncrement, ts, rnd, walWriter, true);
                walName = walWriter.getWalName();
            }
            replicateAndApplyToAllNodes(tableName, walName);

            TestUtils.assertSqlCursors(node1, nodes, tableCopyName, tableName, LOG, false);

            update("UPDATE " + tableName + " SET INT=rnd_int()");
            drainWalQueue();
            replicateAndApplyToAllNodes(tableName, walName);

            update("UPDATE " + tableCopyName + " SET INT=rnd_int()");
            TestUtils.assertSqlCursors(node1, nodes, tableCopyName, tableName, LOG, false);
        });
    }

    @Test
    public void testUpdateViaWal_SQLFailure() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final String tableCopyName = tableName + "_copy";
            TableToken tableToken = createTableAndCopy(tableName, tableCopyName);

            long tsIncrement = Micros.SECOND_MICROS;
            long ts = MicrosTimestampDriver.floor("2022-07-14T00:00:00");
            int rowCount = (int) (Files.PAGE_SIZE / 32);
            ts += (Micros.SECOND_MICROS * (60 * 60 - rowCount - 10));
            Rnd rnd = TestUtils.generateRandom(LOG);

            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                addRowsToWalAndApplyToTable(0, tableName, tableCopyName, rowCount, tsIncrement, ts, rnd, walWriter, true);
                assertSqlCursors(tableCopyName, tableName);

                assertExceptionNoLeakCheck(
                        "UPDATE " + tableName + " SET INT=systimestamp()",
                        43
                );
                drainWalQueue();
                assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName(tableName)));

                assertExceptionNoLeakCheck(
                        "UPDATE " + tableCopyName + " SET INT=systimestamp()",
                        48
                );
                assertSqlCursors(tableCopyName, tableName);
            }
        });
    }

    @Test
    public void testUpdateViaWal_Simple() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final String tableCopyName = tableName + "_copy";
            TableToken tableToken = createTableAndCopy(tableName, tableCopyName);

            long tsIncrement = Micros.SECOND_MICROS;
            long ts = MicrosTimestampDriver.floor("2022-07-14T00:00:00");
            int rowCount = (int) (Files.PAGE_SIZE / 32);
            ts += (Micros.SECOND_MICROS * (60 * 60 - rowCount - 10));
            Rnd rnd = TestUtils.generateRandom(LOG);

            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    WalWriter walWriter = engine.getWalWriter(tableToken)
            ) {
                addRowsToWalAndApplyToTable(0, tableName, tableCopyName, rowCount, tsIncrement, ts, rnd, walWriter, true);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);

                update("UPDATE " + tableName + " SET INT=12345678");
                drainWalQueue();

                update("UPDATE " + tableCopyName + " SET INT=12345678");
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);
            }
        });
    }

    @Test
    public void testUpdateViaWal_SimpleWhere() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final String tableCopyName = tableName + "_copy";
            TableToken tableToken = createTableAndCopy(tableName, tableCopyName);

            long tsIncrement = Micros.SECOND_MICROS;
            long ts = MicrosTimestampDriver.floor("2022-07-14T00:00:00");
            int rowCount = (int) (Files.PAGE_SIZE / 32);
            ts += (Micros.SECOND_MICROS * (60 * 60 - rowCount - 10));
            Rnd rnd = TestUtils.generateRandom(LOG);

            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    WalWriter walWriter = engine.getWalWriter(tableToken)
            ) {
                addRowsToWalAndApplyToTable(0, tableName, tableCopyName, rowCount, tsIncrement, ts, rnd, walWriter, true);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);

                update("UPDATE " + tableName + " SET INT=12345678 WHERE INT > 5");
                drainWalQueue();

                update("UPDATE " + tableCopyName + " SET INT=12345678 WHERE INT > 5");
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);
            }
        });
    }

    @Test
    public void testUpdateViaWal_SysTimestamp() throws Exception {
        testUpdateToNowFunction("systimestamp");
    }

    @Test
    public void testWalTxnAutoRepublishing() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_WAL_SEQUENCER_CHECK_INTERVAL, 1);
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final String tableCopyName = tableName + "_copy";
            TableToken tableToken = createTable(createTableModel(tableName).wal());
            TableToken tableCopyToken = createTable(createTableModel(tableCopyName).wal());

            execute("INSERT INTO " + tableName + " (ts) VALUES ('2014')");
            execute("INSERT INTO " + tableCopyName + " (ts) VALUES ('2015')");

            CheckWalTransactionsJob checkWalTransactionsJob = new CheckWalTransactionsJob(engine);
            checkWalTransactionsJob.runSerially();

            // Artificially notify transactions up to fill the queue
            boolean full;
            do {
                engine.notifyWalTxnCommitted(tableToken);
                full = !engine.notifyWalTxnCommitted(tableCopyToken);
            } while (!full);

            // This supposed to republish the transactions
            long currentRepublishCounter = engine.getUnpublishedWalTxnCount();
            for (int i = 0; i < 10; i++) {
                setCurrentMicros(currentMicros + 200000);
                Assert.assertFalse(checkWalTransactionsJob.runSerially());
                // Check that only 1 attempt is made to publish notification and then the job backs off
                Assert.assertEquals(currentRepublishCounter + i + 1, engine.getUnpublishedWalTxnCount());
            }
        });
    }

    @Test
    public void testWalTxnRepublishing() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final String tableCopyName = tableName + "_copy";
            TableToken tableToken = createTableAndCopy(tableName, tableCopyName);

            long tsIncrement = Micros.SECOND_MICROS;
            long ts = MicrosTimestampDriver.floor("2022-07-14T00:00:00");
            int rowCount = (int) (Files.PAGE_SIZE / 32);
            ts += (Micros.SECOND_MICROS * (60 * 60 - rowCount - 10));
            Rnd rnd = TestUtils.generateRandom(LOG);

            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    WalWriter walWriter = engine.getWalWriter(tableToken)
            ) {

                long start = ts;
                addRowsToWalAndApplyToTable(0, tableName, tableCopyName, rowCount, tsIncrement, start, rnd, walWriter, true);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);

                start += rowCount * tsIncrement + 1;
                addRowsToWal(1, tableName, tableCopyName, rowCount, tsIncrement, start, rnd, walWriter, true);

                drainWalQueue(true);
                new CheckWalTransactionsJob(engine).runSerially();

                drainWalQueue(false);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);
            }
        });
    }

    @Test
    public void testWalWriterWithExistingTable() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final String tableCopyName = tableName + "_copy";
            TableToken tableToken = createTableAndCopy(tableName, tableCopyName);

            long tsIncrement = Micros.SECOND_MICROS;
            long ts = MicrosTimestampDriver.floor("2022-07-14T00:00:00");
            int rowCount = (int) (Files.PAGE_SIZE / 32);
            ts += (Micros.SECOND_MICROS * (60 * 60 - rowCount - 10));
            Rnd rnd = TestUtils.generateRandom(LOG);

            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    WalWriter walWriter = engine.getWalWriter(tableToken)
            ) {

                long start = ts;
                addRowsToWalAndApplyToTable(0, tableName, tableCopyName, rowCount, tsIncrement, start, rnd, walWriter, true);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);

                try (
                        WalWriter walWriter2 = engine.getWalWriter(tableToken)
                ) {
                    rnd.reset();
                    start += rowCount * tsIncrement - Micros.HOUR_MICROS / 2 + 1;
                    addRowsToWalAndApplyToTable(1, tableName, tableCopyName, rowCount, tsIncrement, start, rnd, walWriter2, true);
                    TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);
                }

                start += rowCount * tsIncrement - Micros.HOUR_MICROS / 2 + 3;
                addRowsToWalAndApplyToTable(0, tableName, tableCopyName, rowCount, tsIncrement, start, rnd, walWriter, true);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);
            }
        });
    }

    private void addRowRwAllTypes(int iteration, TableWriter.Row row, int i, CharSequence symbol, String rndStr, Utf8Sequence rndVarchar) {
        int col = 0;
        row.putInt(col++, i);
        row.putByte(col++, (byte) i);
        row.putLong(col++, i);
        row.putLong256(col++, i, i + 1, i + 2, i + 3);
        row.putDouble(col++, i + .5);
        row.putFloat(col++, i + .5f);
        row.putShort(col++, (short) iteration);
        row.putTimestamp(col++, i);
        row.putChar(col++, (char) (65 + i % 26));
        row.putBool(col++, i % 2 == 0);
        row.putDate(col++, i);
        row.putStr(col++, rndStr);
        row.putGeoHash(col++, i); // geo byte
        row.putGeoHash(col++, i); // geo int
        row.putGeoHash(col++, i); // geo short
        row.putGeoHash(col++, i); // geo long
        row.putStr(col++, (char) (65 + i % 26));
        row.putSym(col++, symbol);
        row.putLong128(col++, Hash.hashLong64(i), Hash.hashLong64(i + 1)); // UUID
        col++; // binary ('bin') column is not set
        row.putVarchar(col++, rndVarchar);
        row.putByte(col++, (byte) (i % 100)); // decimal8
        row.putShort(col++, (short) (i % 10000)); // decimal16
        row.putInt(col++, i); // decimal32
        row.putLong(col++, i); // decimal64
        row.putDecimal128(col++, 0, i); // decimal128
        row.putDecimal256(col, 0, 0, 0, i); // decimal256

        row.append();
    }

    @SuppressWarnings("SameParameterValue")
    private int addRowsToWal(
            int iteration,
            String tableName,
            String tableCopyName,
            int rowsToInsertTotal,
            long tsIncrement,
            long startTs,
            Rnd rnd,
            WalWriter walWriter,
            boolean inOrder
    ) {
        final int tableId;
        try (
                TableWriter copyWriter = getWriter(tableCopyName);
                TableWriter tableWriter = getWriter(tableName)
        ) {
            tableId = tableWriter.getMetadata().getTableId();
            if (!inOrder) {
                startTs += (rowsToInsertTotal - 1) * tsIncrement;
                tsIncrement = -tsIncrement;
            }

            Utf8StringSink utf8Sink = new Utf8StringSink();
            for (int i = 0; i < rowsToInsertTotal; i++) {
                String symbol = rnd.nextInt(10) == 5 ? null : rnd.nextString(rnd.nextInt(9) + 1);
                String rndStr = rnd.nextInt(10) == 5 ? null : rnd.nextString(20);
                Utf8Sequence rndUtf8Seq = null;
                if (rnd.nextInt(10) != 5) {
                    utf8Sink.clear();
                    rnd.nextUtf8Str(20, utf8Sink);
                    rndUtf8Seq = utf8Sink;
                }

                long rowTs = startTs;
                if (!inOrder && i > 3 && i < rowsToInsertTotal - 3) {
                    // Add jitter to the timestamps to randomise things even more.
                    rowTs += rnd.nextLong(2 * tsIncrement);
                }

                addRowRwAllTypes(iteration, walWriter.newRow(rowTs), i, symbol, rndStr, rndUtf8Seq);
                addRowRwAllTypes(iteration, copyWriter.newRow(rowTs), i, symbol, rndStr, rndUtf8Seq);
                startTs += tsIncrement;
            }

            copyWriter.commit();
            walWriter.commit();
        }
        return tableId;
    }

    private int addRowsToWalAndApplyToTable(
            int iteration,
            String tableName,
            String tableCopyName,
            int rowsToInsertTotal,
            long tsIncrement,
            long startTs,
            Rnd rnd,
            WalWriter walWriter,
            boolean inOrder
    ) {
        final int tableId = addRowsToWal(iteration, tableName, tableCopyName, rowsToInsertTotal, tsIncrement, startTs, rnd, walWriter, inOrder);
        drainWalQueue();
        return tableId;
    }

    private void assertMaxUncommittedRows(CharSequence tableName, int expectedMaxUncommittedRows) throws SqlException {
        try (TableReader reader = getReader(tableName)) {
            assertSql("maxUncommittedRows\n" + expectedMaxUncommittedRows + "\n", "SELECT maxUncommittedRows FROM tables() WHERE table_name = '" + tableName + "'"
            );
            reader.reload();
            assertEquals(expectedMaxUncommittedRows, reader.getMetadata().getMaxUncommittedRows());
        }
    }

    @SuppressWarnings("SameParameterValue")
    private TableToken createTableAndCopy(String tableName, String tableCopyName) {
        AtomicReference<TableToken> tableToken = new AtomicReference<>();
        // tableName is WAL enabled
        final TableModel model = createTableModel(tableName).wal();
        forEachNode(node -> tableToken.set(TestUtils.createTable(node.getEngine(), model))
        );

        // tableCopyName is not WAL enabled
        createTable(createTableModel(tableCopyName).noWal());
        return tableToken.get();
    }

    private TableModel createTableModel(String tableName) {
        return new TableModel(configuration, tableName, PartitionBy.HOUR)
                .col("int", ColumnType.INT)
                .col("byte", ColumnType.BYTE)
                .col("long", ColumnType.LONG)
                .col("long256", ColumnType.LONG256)
                .col("double", ColumnType.DOUBLE)
                .col("float", ColumnType.FLOAT)
                .col("short", ColumnType.SHORT)
                .col("timestamp", ColumnType.TIMESTAMP)
                .col("char", ColumnType.CHAR)
                .col("boolean", ColumnType.BOOLEAN)
                .col("date", ColumnType.DATE)
                .col("string", ColumnType.STRING)
                .col("geoByte", ColumnType.getGeoHashTypeWithBits(5))
                .col("geoInt", ColumnType.getGeoHashTypeWithBits(20))
                .col("geoShort", ColumnType.getGeoHashTypeWithBits(10))
                .col("geoLong", ColumnType.getGeoHashTypeWithBits(35))
                .col("stringc", ColumnType.STRING)
                .col("label", ColumnType.SYMBOL)
                .col("uuid", ColumnType.UUID)
                .col("bin", ColumnType.BINARY)
                .col("varchar", ColumnType.VARCHAR)
                .col("decimal8", ColumnType.getDecimalType(2, 1))
                .col("decimal16", ColumnType.getDecimalType(4, 1))
                .col("decimal32", ColumnType.getDecimalType(9, 1))
                .col("decimal64", ColumnType.getDecimalType(18, 1))
                .col("decimal128", ColumnType.getDecimalType(38, 1))
                .col("decimal256", ColumnType.getDecimalType(76, 1))
                .timestamp("ts");
    }

    private void testUpdateToNowFunction(String nowName) throws Exception {
        // regardless of randomised clocks tables on each node should be identical
        final Rnd rnd = TestUtils.generateRandom(LOG);
        forEachNode(node -> node.getConfigurationOverrides().setCurrentMicros(rnd.nextPositiveLong()));

        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final String tableCopyName = tableName + "_copy";
            TableToken tableToken = createTableAndCopy(tableName, tableCopyName);

            long tsIncrement = Micros.SECOND_MICROS;
            long ts = MicrosTimestampDriver.floor("2022-07-14T00:00:00");
            int rowCount = (int) (Files.PAGE_SIZE / 32);
            ts += (Micros.SECOND_MICROS * (60 * 60 - rowCount - 10));

            final String walName;
            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                addRowsToWalAndApplyToTable(0, tableName, tableCopyName, rowCount, tsIncrement, ts, rnd, walWriter, true);
                walName = walWriter.getWalName();
            }

            update("UPDATE " + tableName + " SET LONG=2*" + nowName + "()-" + nowName + "()");
            drainWalQueue();
            replicateAndApplyToAllNodes(tableName, walName);

            update("UPDATE " + tableCopyName + " SET LONG=2*" + nowName + "()-" + nowName + "()");
            TestUtils.assertSqlCursors(node1, nodes, tableCopyName, tableName, LOG, false);
        });
    }

    private void updateMaxUncommittedRows(CharSequence tableName, int maxUncommittedRows) throws SqlException {
        updateMaxUncommittedRows(tableName, maxUncommittedRows, -1);
    }

    private void updateMaxUncommittedRows(CharSequence tableName, int maxUncommittedRows, int tableId) throws SqlException {
        execute("ALTER TABLE " + tableName + " SET PARAM maxUncommittedRows = " + maxUncommittedRows);
        if (tableId > 0) {
            drainWalQueue();
        }
    }

    protected static void drainWalQueue(boolean cleanup) throws IOException {
        class QueueCleanerJob extends AbstractQueueConsumerJob<WalTxnNotificationTask> implements Closeable {
            public QueueCleanerJob(CairoEngine engine) {
                super(engine.getMessageBus().getWalTxnNotificationQueue(), engine.getMessageBus().getWalTxnNotificationSubSequence());
            }

            @Override
            public void close() {
            }

            @Override
            protected boolean doRun(int workerId, long cursor, RunStatus runStatus) {
                try {
                    queue.get(cursor);
                } finally {
                    subSeq.done(cursor);
                }
                return true;
            }
        }

        final AbstractQueueConsumerJob<?> job = cleanup ? new QueueCleanerJob(engine) : createWalApplyJob(engine);
        try {
            job.drain(0);
        } finally {
            ((Closeable) job).close();
        }
    }
}
