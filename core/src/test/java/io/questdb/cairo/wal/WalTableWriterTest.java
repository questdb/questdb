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

package io.questdb.cairo.wal;

import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.tasks.WalTxnNotificationTask;
import io.questdb.test.tools.TestUtils;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;

import static io.questdb.cairo.wal.WalWriterTest.createTable;
import static io.questdb.cairo.wal.WalWriterTest.removeColumn;
import static org.junit.Assert.*;

public class WalTableWriterTest extends AbstractGriffinTest {

    @Before
    public void setUp() {
        super.setUp();
        currentMicros = 0L;
    }

    @After
    public void tearDown() {
        super.tearDown();
        currentMicros = -1L;
    }

    @Test
    public void testPartitionOverflowAppend() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final String tableCopyName = tableName + "_copy";
            createTableAndCopy(tableName, tableCopyName);

            long tsIncrement = Timestamps.SECOND_MICROS;
            long ts = IntervalUtils.parseFloorPartialTimestamp("2022-07-14T00:00:00");
            int rowCount = (int) (Files.PAGE_SIZE / 32);
            ts += (Timestamps.SECOND_MICROS * (60 * 60 - rowCount - 10));
            Rnd rnd = TestUtils.generateRandom(LOG);

            try (
                    WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)
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
            createTableAndCopy(tableName, tableCopyName);

            long tsIncrement = Timestamps.SECOND_MICROS;
            long ts = IntervalUtils.parseFloorPartialTimestamp("2022-07-14T00:00:00");
            int rowCount = (int) (Files.PAGE_SIZE / 32);
            ts += (Timestamps.SECOND_MICROS * (60 * 60 - rowCount - 10));
            Rnd rnd = TestUtils.generateRandom(LOG);

            try (
                    WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)
            ) {

                long start = ts;
                addRowsToWalAndApplyToTable(0, tableName, tableCopyName, rowCount, tsIncrement, start, rnd, walWriter, true);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);

                start += rowCount * tsIncrement - 2 * Timestamps.SECOND_MICROS;
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
            createTableAndCopy(tableName, tableCopyName);

            long tsIncrement;
            long now = Os.currentTimeMicros();
            LOG.info().$("now :").$(now).$();
            Rnd rnd = TestUtils.generateRandom(LOG);

            try (
                    WalWriter walWriter1 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName);
                    WalWriter walWriter2 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName);
                    WalWriter walWriter3 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)
            ) {

                long start = now;
                WalWriter[] writers = new WalWriter[]{walWriter1, walWriter2, walWriter3};

                for (int i = 0; i < 5; i++) {
                    boolean inOrder = rnd.nextBoolean();
                    int walIndex = rnd.nextInt(writers.length);
                    WalWriter walWriter = writers[walIndex];
                    int rowCount = rnd.nextInt(1000) + 2;
                    tsIncrement = rnd.nextLong(Timestamps.MINUTE_MICROS);

                    LOG.infoW().$("generating wall [")
                            .$("iteration:").$(i)
                            .$(", walIndex: ").$(walIndex)
                            .$(", inOrder: ").$(inOrder)
                            .$(" rowCount: ").$(rowCount)
                            .$(" tsIncrement: ").$(tsIncrement)
                            .I$();

                    addRowsToWalAndApplyToTable(i, tableName, tableCopyName, rowCount, tsIncrement, start, rnd, walWriter, inOrder);

                    LOG.info().$("verifying wall [").$("iteration:").$(i).I$();
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
            createTableAndCopy(tableName, tableCopyName);

            long tsIncrement;
            long now = Os.currentTimeMicros();
            LOG.info().$("now :").$(now).$();
            Rnd rnd = TestUtils.generateRandom(LOG);

            int releaseWriterSeed = 3;
            int overlapSeed = 3;

            try (
                    WalWriter walWriter1 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName);
                    WalWriter walWriter2 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName);
                    WalWriter walWriter3 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)
            ) {

                long start = now;
                WalWriter[] writers = new WalWriter[]{walWriter1, walWriter2, walWriter3};

                for (int i = 0; i < 20; i++) {
                    boolean inOrder = rnd.nextBoolean();
                    int walIndex = rnd.nextInt(writers.length);
                    WalWriter walWriter = writers[walIndex];
                    int rowCount = rnd.nextInt(10000) + 1;
                    int partitions = rnd.nextInt(3) + 1;
                    tsIncrement = partitions * Timestamps.HOUR_MICROS / rowCount;
                    long tsOffset = rnd.nextLong(2 * Timestamps.HOUR_MICROS);
                    int sign = rnd.nextInt(overlapSeed);
                    tsOffset *= sign == 0 ? -1 : 1;
                    start += tsOffset;

                    LOG.infoW().$("generating wall [")
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

                    LOG.info().$("verifying wall [").$("iteration:").$(i).I$();
                    TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);

                    start += rowCount * tsIncrement + 1;
                }
            }
        });
    }

    @Test
    public void testOutOfOrderDuplicateTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            try (
                    TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY)
                            .col("i", ColumnType.INT)
                            .timestamp("ts")
                            .wal()
            ) {
                createTable(model);
            }

            final int tableId;
            try (TableWriter tableWriter = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), tableName, "test")) {
                tableId = tableWriter.getMetadata().getId();
            }

            try (
                    WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)
            ) {
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

                applyWalToTable(walWriter.getSystemTableName(), tableId);
            }

            assertSql(tableName, "i\tts\n" +
                    "2\t1970-01-01T00:00:00.000500Z\n" +
                    "1\t1970-01-01T00:00:00.001000Z\n" +
                    "3\t1970-01-01T00:00:00.001500Z\n" +
                    "4\t1970-01-01T00:00:00.001500Z\n");
        });
    }

    @Test
    public void testReadAndWriteAllTypes() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final String tableCopyName = tableName + "_copy";
            createTableAndCopy(tableName, tableCopyName);

            final int rowsToInsertTotal = 100;
            final long tsIncrement = 1000;
            long ts = Os.currentTimeMicros();

            Rnd rnd = new Rnd();
            try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
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
            createTableAndCopy(tableName, tableCopyName);

            final int rowsToInsertTotal = 100;
            final long tsIncrement = 1000;
            long ts = Os.currentTimeMicros();

            Rnd rnd = new Rnd();
            try (
                    WalWriter walWriter1 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName);
                    WalWriter walWriter2 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)
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
            createTableAndCopy(tableName, tableCopyName);

            final int rowsToInsertTotal = 100;
            final long tsIncrement = 1000;
            long ts = Os.currentTimeMicros();

            Rnd rnd = new Rnd();
            try (
                    WalWriter walWriter1 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName);
                    WalWriter walWriter2 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)
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
    public void testWalWriterWithExistingTable() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final String tableCopyName = tableName + "_copy";
            createTableAndCopy(tableName, tableCopyName);

            long tsIncrement = Timestamps.SECOND_MICROS;
            long ts = IntervalUtils.parseFloorPartialTimestamp("2022-07-14T00:00:00");
            int rowCount = (int) (Files.PAGE_SIZE / 32);
            ts += (Timestamps.SECOND_MICROS * (60 * 60 - rowCount - 10));
            Rnd rnd = TestUtils.generateRandom(LOG);

            try (
                    WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)
            ) {

                long start = ts;
                addRowsToWalAndApplyToTable(0, tableName, tableCopyName, rowCount, tsIncrement, start, rnd, walWriter, true);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);

                try (
                        WalWriter walWriter2 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)
                ) {
                    rnd.reset();
                    start += rowCount * tsIncrement - Timestamps.HOUR_MICROS / 2 + 1;
                    addRowsToWalAndApplyToTable(1, tableName, tableCopyName, rowCount, tsIncrement, start, rnd, walWriter2, true);
                    TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);
                }

                start += rowCount * tsIncrement - Timestamps.HOUR_MICROS / 2 + 3;
                addRowsToWalAndApplyToTable(0, tableName, tableCopyName, rowCount, tsIncrement, start, rnd, walWriter, true);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);
            }
        });
    }

    @Test
    public void testApplyWalUpdates() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final String tableCopyName = tableName + "_copy";
            createTableAndCopy(tableName, tableCopyName);

            long tsIncrement = Timestamps.SECOND_MICROS;
            long ts = IntervalUtils.parseFloorPartialTimestamp("2022-07-14T00:00:00");
            int rowCount = (int) (Files.PAGE_SIZE / 32);
            ts += (Timestamps.SECOND_MICROS * (60 * 60 - rowCount - 10));
            Rnd rnd = TestUtils.generateRandom(LOG);

            try (
                    WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)
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
    public void testNonStructuralAlterViaWal() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final String tableCopyName = tableName + "_copy";
            createTableAndCopy(tableName, tableCopyName);

            long tsIncrement = Timestamps.SECOND_MICROS;
            long ts = IntervalUtils.parseFloorPartialTimestamp("2022-07-14T00:00:00");
            int rowCount = (int) (Files.PAGE_SIZE / 32);
            ts += (Timestamps.SECOND_MICROS * (60 * 60 - rowCount - 10));
            Rnd rnd = TestUtils.generateRandom(LOG);

            try (
                    SqlToOperation sqlToOperation = new SqlToOperation(engine);
                    WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)
            ) {
                final int tableId = addRowsToWalAndApplyToTable(0, tableName, tableCopyName, rowCount, tsIncrement, ts, rnd, walWriter, true);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);

                updateMaxUncommittedRows(tableName, 60, tableId, sqlToOperation);
                assertMaxUncommittedRows(tableName, 60);
                updateMaxUncommittedRows(tableCopyName, 60);
                assertMaxUncommittedRows(tableCopyName, 60);
                updateMaxUncommittedRows(tableName, 55, tableId, sqlToOperation);
                assertMaxUncommittedRows(tableName, 55);
                updateMaxUncommittedRows(tableCopyName, 55);
                assertMaxUncommittedRows(tableCopyName, 55);
                updateMaxUncommittedRows(tableName, 50, tableId, sqlToOperation);
                assertMaxUncommittedRows(tableName, 50);
                updateMaxUncommittedRows(tableName, 77, tableId, sqlToOperation);
                assertMaxUncommittedRows(tableName, 77);

                // assert that data is not changed
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);
            }
        });
    }

    @Test
    public void testUpdateViaWal_CopyIntoDeletedColumn() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            try (TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.INT)
                    .timestamp("ts")
                    .wal()
            ) {
                createTable(model);
            }

            try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                TableWriter.Row row = walWriter.newRow();
                row.putInt(0, 10);
                row.append();
                row = walWriter.newRow();
                row.putInt(0, 11);
                row.append();
                row = walWriter.newRow();
                row.putInt(0, 12);
                row.append();
                walWriter.commit();

                removeColumn(walWriter, "b");

                executeOperation("UPDATE " + tableName + " SET b = a", CompiledQuery.UPDATE);
                fail("Expected exception is missing");
            } catch (Exception e) {
                MatcherAssert.assertThat(e.getMessage(), CoreMatchers.endsWith("Invalid column: b"));
            }
        });
    }

    @Test
    public void testUpdateViaWal_JoinRejected() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final String tableCopyName = tableName + "_copy";
            createTableAndCopy(tableName, tableCopyName);

            long tsIncrement = Timestamps.SECOND_MICROS;
            long ts = IntervalUtils.parseFloorPartialTimestamp("2022-07-14T00:00:00");
            int rowCount = (int) (Files.PAGE_SIZE / 32);
            ts += (Timestamps.SECOND_MICROS * (60 * 60 - rowCount - 10));
            Rnd rnd = TestUtils.generateRandom(LOG);

            try (
                    WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)
            ) {
                addRowsToWalAndApplyToTable(0, tableName, tableCopyName, rowCount, tsIncrement, ts, rnd, walWriter, true);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);

                executeOperation("UPDATE " + tableCopyName + " SET INT=12345678", CompiledQuery.UPDATE);
                try {
                    executeOperation("UPDATE " + tableName + " t SET INT=12345678 FROM " + tableCopyName + " c WHERE t.INT=c.INT", CompiledQuery.UPDATE);
                    fail("Expected exception is not thrown");
                } catch (Exception e) {
                    assertTrue(e.getMessage().endsWith("UPDATE statements with join are not supported yet for WAL tables"));
                }
            }
        });
    }

    @Test
    public void testUpdateViaWal_IndexedVariables() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final String tableCopyName = tableName + "_copy";
            createTableAndCopy(tableName, tableCopyName);

            long tsIncrement = Timestamps.SECOND_MICROS;
            long ts = IntervalUtils.parseFloorPartialTimestamp("2022-07-14T00:00:00");
            int rowCount = (int) (Files.PAGE_SIZE / 32);
            ts += (Timestamps.SECOND_MICROS * (60 * 60 - rowCount - 10));
            Rnd rnd = TestUtils.generateRandom(LOG);

            final int binarySize = 64;
            final long pointer = Unsafe.getUnsafe().allocateMemory(binarySize);
            final DirectBinarySequence binSeq = new DirectBinarySequence();
            WalWriterTest.prepareBinPayload(pointer, binarySize);

            try (
                    SqlToOperation sqlToOperation = new SqlToOperation(engine);
                    WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)
            ) {
                final int tableId = addRowsToWalAndApplyToTable(0, tableName, tableCopyName, rowCount, tsIncrement, ts, rnd, walWriter, true);
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

                executeOperation("UPDATE " + tableName + " SET " +
                        "INT=$1, BYTE=$2, SHORT=$3, LONG=$4, FLOAT=$5, DOUBLE=$6, TIMESTAMP=$7, DATE=$8, " +
                        "CHAR=$9, BOOLEAN=$10, STRING=$11, LABEL=$12, BIN=$13, GEOBYTE=$14, GEOSHORT=$15, GEOINT=$16, GEOLONG=$17 " +
                        "WHERE INT > 5", CompiledQuery.UPDATE);
                ApplyWal2TableJob.processWalTxnNotification(walWriter.getSystemTableName(), tableId, engine, sqlToOperation);

                executeOperation("UPDATE " + tableCopyName + " SET " +
                        "INT=$1, BYTE=$2, SHORT=$3, LONG=$4, FLOAT=$5, DOUBLE=$6, TIMESTAMP=$7, DATE=$8, " +
                        "CHAR=$9, BOOLEAN=$10, STRING=$11, LABEL=$12, BIN=$13, GEOBYTE=$14, GEOSHORT=$15, GEOINT=$16, GEOLONG=$17 " +
                        "WHERE INT > 5", CompiledQuery.UPDATE);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);
            }
        });
    }

    @Test
    public void testUpdateViaWal_NamedVariables() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final String tableCopyName = tableName + "_copy";
            createTableAndCopy(tableName, tableCopyName);

            long tsIncrement = Timestamps.SECOND_MICROS;
            long ts = IntervalUtils.parseFloorPartialTimestamp("2022-07-14T00:00:00");
            int rowCount = (int) (Files.PAGE_SIZE / 32);
            ts += (Timestamps.SECOND_MICROS * (60 * 60 - rowCount - 10));
            Rnd rnd = TestUtils.generateRandom(LOG);

            final int binarySize = 64;
            final long pointer = Unsafe.getUnsafe().allocateMemory(binarySize);
            final DirectBinarySequence binSeq = new DirectBinarySequence();
            WalWriterTest.prepareBinPayload(pointer, binarySize);

            try (
                    SqlToOperation sqlToOperation = new SqlToOperation(engine);
                    WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)
            ) {
                final int tableId = addRowsToWalAndApplyToTable(0, tableName, tableCopyName, rowCount, tsIncrement, ts, rnd, walWriter, true);
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

                executeOperation("UPDATE " + tableName + " SET " +
                        "INT=:INTVAL, BYTE=:BYTEVAL, SHORT=:SHORTVAL, LONG=:LONGVAL, " +
                        "FLOAT=:FLOATVAL, DOUBLE=:DOUBLEVAL, TIMESTAMP=:TIMESTAMPVAL, DATE=:DATEVAL, " +
                        "CHAR=:CHARVAL, BOOLEAN=:BOOLVAL, STRING=:STRVAL, LABEL=:SYMVAL, BIN=:BINVAL, " +
                        "GEOBYTE=:GEOBYTEVAL, GEOSHORT=:GEOSHORTVAL, GEOINT=:GEOINTVAL, GEOLONG=:GEOLONGVAL " +
                        "WHERE INT > 5", CompiledQuery.UPDATE);
                ApplyWal2TableJob.processWalTxnNotification(walWriter.getSystemTableName(), tableId, engine, sqlToOperation);

                executeOperation("UPDATE " + tableCopyName + " SET " +
                        "INT=:INTVAL, BYTE=:BYTEVAL, SHORT=:SHORTVAL, LONG=:LONGVAL, " +
                        "FLOAT=:FLOATVAL, DOUBLE=:DOUBLEVAL, TIMESTAMP=:TIMESTAMPVAL, DATE=:DATEVAL, " +
                        "CHAR=:CHARVAL, BOOLEAN=:BOOLVAL, STRING=:STRVAL, LABEL=:SYMVAL, BIN=:BINVAL, " +
                        "GEOBYTE=:GEOBYTEVAL, GEOSHORT=:GEOSHORTVAL, GEOINT=:GEOINTVAL, GEOLONG=:GEOLONGVAL " +
                        "WHERE INT > 5", CompiledQuery.UPDATE);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);
            }
        });
    }

    @Test
    public void testUpdateViaWal_Simple() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final String tableCopyName = tableName + "_copy";
            createTableAndCopy(tableName, tableCopyName);

            long tsIncrement = Timestamps.SECOND_MICROS;
            long ts = IntervalUtils.parseFloorPartialTimestamp("2022-07-14T00:00:00");
            int rowCount = (int) (Files.PAGE_SIZE / 32);
            ts += (Timestamps.SECOND_MICROS * (60 * 60 - rowCount - 10));
            Rnd rnd = TestUtils.generateRandom(LOG);

            try (
                    SqlToOperation sqlToOperation = new SqlToOperation(engine);
                    WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)
            ) {
                final int tableId = addRowsToWalAndApplyToTable(0, tableName, tableCopyName, rowCount, tsIncrement, ts, rnd, walWriter, true);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);

                executeOperation("UPDATE " + tableName + " SET INT=12345678", CompiledQuery.UPDATE);
                ApplyWal2TableJob.processWalTxnNotification(walWriter.getSystemTableName(), tableId, engine, sqlToOperation);

                executeOperation("UPDATE " + tableCopyName + " SET INT=12345678", CompiledQuery.UPDATE);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);
            }
        });
    }

    @Test
    public void testUpdateViaWal_SimpleWhere() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final String tableCopyName = tableName + "_copy";
            createTableAndCopy(tableName, tableCopyName);

            long tsIncrement = Timestamps.SECOND_MICROS;
            long ts = IntervalUtils.parseFloorPartialTimestamp("2022-07-14T00:00:00");
            int rowCount = (int) (Files.PAGE_SIZE / 32);
            ts += (Timestamps.SECOND_MICROS * (60 * 60 - rowCount - 10));
            Rnd rnd = TestUtils.generateRandom(LOG);

            try (
                    SqlToOperation sqlToOperation = new SqlToOperation(engine);
                    WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)
            ) {
                final int tableId = addRowsToWalAndApplyToTable(0, tableName, tableCopyName, rowCount, tsIncrement, ts, rnd, walWriter, true);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);

                executeOperation("UPDATE " + tableName + " SET INT=12345678 WHERE INT > 5", CompiledQuery.UPDATE);
                ApplyWal2TableJob.processWalTxnNotification(walWriter.getSystemTableName(), tableId, engine, sqlToOperation);

                executeOperation("UPDATE " + tableCopyName + " SET INT=12345678 WHERE INT > 5", CompiledQuery.UPDATE);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);
            }
        });
    }

    @Test
    public void testUpdateViaWal_CopyIntoNewColumn() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            try (TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.INT)
                    .timestamp("ts")
                    .wal()
            ) {
                createTable(model);
            }

            final int tableId;
            try (TableWriter tableWriter = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), tableName, "test")) {
                tableId = tableWriter.getMetadata().getId();
            }

            try (
                    SqlToOperation sqlToOperation = new SqlToOperation(engine);
                    WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)
            ) {
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

                executeOperation("UPDATE " + tableName + " SET b = a", CompiledQuery.UPDATE);
                executeOperation("UPDATE " + tableName + " SET c = a", CompiledQuery.UPDATE);
                ApplyWal2TableJob.processWalTxnNotification(walWriter.getSystemTableName(), tableId, engine, sqlToOperation);
            }

            assertSql(tableName, "a\tb\tts\tc\n" +
                    "10\t10\t1970-01-01T00:00:00.000000Z\t10\n" +
                    "11\t11\t1970-01-01T00:00:00.000000Z\t11\n" +
                    "12\t12\t1970-01-01T00:00:00.000000Z\t12\n");
        });
    }

    protected static void drainWalQueue(boolean cleanup) throws IOException {
        class QueueCleanerJob extends AbstractQueueConsumerJob<WalTxnNotificationTask> implements Closeable {
            public QueueCleanerJob(CairoEngine engine) {
                super(engine.getMessageBus().getWalTxnNotificationQueue(), engine.getMessageBus().getWalTxnNotificationSubSequence());
            }

            @Override
            public void close() throws IOException {
            }

            @Override
            protected boolean doRun(int workerId, long cursor) {
                try {
                    queue.get(cursor);
                } finally {
                    subSeq.done(cursor);
                }
                return true;
            }
        }

        AbstractQueueConsumerJob<?> job = cleanup ? new QueueCleanerJob(engine) : new ApplyWal2TableJob(engine);
        while (job.run(0)) {
            // run until empty
        }
        ((Closeable) job).close();
    }

    private void updateMaxUncommittedRows(CharSequence tableName, int maxUncommittedRows) throws SqlException {
        updateMaxUncommittedRows(tableName, maxUncommittedRows, -1, null);
    }

    private int addRowsToWalAndApplyToTable(int iteration, String tableName, String tableCopyName, int rowsToInsertTotal, long tsIncrement, long startTs, Rnd rnd, WalWriter walWriter, boolean inOrder) {
        final int tableId = addRowsToWal(iteration, tableName, tableCopyName, rowsToInsertTotal, tsIncrement, startTs, rnd, walWriter, inOrder);
        applyWalToTable(walWriter.getSystemTableName(), tableId);
        return tableId;
    }

    private void assertMaxUncommittedRows(CharSequence tableName, int expectedMaxUncommittedRows) throws SqlException {
        try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
            assertSql("SELECT maxUncommittedRows FROM tables() WHERE name = '" + tableName + "'",
                    "maxUncommittedRows\n" + expectedMaxUncommittedRows + "\n");
            reader.reload();
            assertEquals(expectedMaxUncommittedRows, reader.getMetadata().getMaxUncommittedRows());
        }
    }

    private void addRowRwAllTypes(int iteration, TableWriter.Row row, int i, CharSequence symbol, String rndStr) {
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
        row.putSym(col, symbol);
        row.append();
    }

    @SuppressWarnings("SameParameterValue")
    private int addRowsToWal(int iteration, String tableName, String tableCopyName, int rowsToInsertTotal, long tsIncrement, long startTs, Rnd rnd, WalWriter walWriter, boolean inOrder) {
        final int tableId;
        try (
                TableWriter copyWriter = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), tableCopyName, "test");
                TableWriter tableWriter = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), tableName, "apply wal")
        ) {
            tableId = tableWriter.getMetadata().getId();
            if (!inOrder) {
                startTs += (rowsToInsertTotal - 1) * tsIncrement;
                tsIncrement = -tsIncrement;
            }

            for (int i = 0; i < rowsToInsertTotal; i++) {
                String symbol = rnd.nextInt(10) == 5 ? null : rnd.nextString(rnd.nextInt(9) + 1);
                String rndStr = rnd.nextInt(10) == 5 ? null : rnd.nextString(20);

                long rowTs = startTs;
                if (!inOrder && i > 3 && i < rowsToInsertTotal - 3) {
                    // Add jitter to the timestamps to randomise things even more.
                    rowTs += rnd.nextLong(2 * tsIncrement);
                }

                addRowRwAllTypes(iteration, walWriter.newRow(rowTs), i, symbol, rndStr);
                addRowRwAllTypes(iteration, copyWriter.newRow(rowTs), i, symbol, rndStr);
                startTs += tsIncrement;
            }

            copyWriter.commit();
            walWriter.commit();
        }
        return tableId;
    }

    @SuppressWarnings("SameParameterValue")
    private void createTableAndCopy(String tableName, String tableCopyName) {
        // tableName is WAL enabled
        try (TableModel model = createTableModel(tableName).wal()) {
            engine.createTable(
                    AllowAllCairoSecurityContext.INSTANCE,
                    model.getMem(),
                    model.getPath(),
                    model,
                    false
            );
        }

        // tableCopyName is not WAL enabled
        try (TableModel model = createTableModel(tableCopyName).noWal()) {
            engine.createTable(
                    AllowAllCairoSecurityContext.INSTANCE,
                    model.getMem(),
                    model.getPath(),
                    model,
                    false
            );
        }
    }

    private static void applyWalToTable(String tableName, int tableId) {
        try (SqlToOperation sqlToOperation = new SqlToOperation(engine)) {
            ApplyWal2TableJob.processWalTxnNotification(tableName, tableId, engine, sqlToOperation);
        }
    }

    private void updateMaxUncommittedRows(CharSequence tableName, int maxUncommittedRows, int tableId, SqlToOperation sqlToOperation) throws SqlException {
        executeOperation("ALTER TABLE " + tableName + " SET PARAM maxUncommittedRows = " + maxUncommittedRows, CompiledQuery.ALTER);
        if (tableId > 0) {
            ApplyWal2TableJob.processWalTxnNotification(engine.getSystemTableName(tableName), tableId, engine, sqlToOperation);
        }
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
                .col("bin", ColumnType.BINARY)
                .timestamp("ts");
    }
}
