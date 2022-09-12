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
import io.questdb.griffin.SqlToOperation;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.std.Files;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.tasks.WalTxnNotificationTask;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;

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
            long ts = IntervalUtils.parseFloorPartialDate("2022-07-14T00:00:00");
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
            long ts = IntervalUtils.parseFloorPartialDate("2022-07-14T00:00:00");
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
            long ts = IntervalUtils.parseFloorPartialDate("2022-07-14T00:00:00");
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
    public void testUpdateViaWal_Simple() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final String tableCopyName = tableName + "_copy";
            createTableAndCopy(tableName, tableCopyName);

            long tsIncrement = Timestamps.SECOND_MICROS;
            long ts = IntervalUtils.parseFloorPartialDate("2022-07-14T00:00:00");
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
                ApplyWal2TableJob.processWalTxnNotification(tableName, tableId, engine, sqlToOperation);

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
            long ts = IntervalUtils.parseFloorPartialDate("2022-07-14T00:00:00");
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
                ApplyWal2TableJob.processWalTxnNotification(tableName, tableId, engine, sqlToOperation);

                executeOperation("UPDATE " + tableCopyName + " SET INT=12345678 WHERE INT > 5", CompiledQuery.UPDATE);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);
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
            long ts = IntervalUtils.parseFloorPartialDate("2022-07-14T00:00:00");
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
                    fail("Expected UnsupportedOperationException was not thrown");
                } catch(UnsupportedOperationException e) {
                    assertEquals("UPDATE statements with join are not supported yet for WAL tables", e.getMessage());
                }
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
            long ts = IntervalUtils.parseFloorPartialDate("2022-07-14T00:00:00");
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
    public void testApplyWalUpdates() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final String tableCopyName = tableName + "_copy";
            createTableAndCopy(tableName, tableCopyName);

            long tsIncrement = Timestamps.SECOND_MICROS;
            long ts = IntervalUtils.parseFloorPartialDate("2022-07-14T00:00:00");
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
                engine.getTableRegistry().forAllWalTables(engine::checkNotifyOutstandingTxnInWal);
                drainWalQueue(false);

                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);
            }
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

    private void updateMaxUncommittedRows(CharSequence tableName, int maxUncommittedRows, int tableId, SqlToOperation sqlToOperation) throws SqlException {
        executeOperation("ALTER TABLE " + tableName + " SET PARAM maxUncommittedRows = " + maxUncommittedRows, CompiledQuery.ALTER);
        if (tableId > 0) {
            ApplyWal2TableJob.processWalTxnNotification(tableName, tableId, engine, sqlToOperation);
        }
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

    private int addRowsToWalAndApplyToTable(int iteration, String tableName, String tableCopyName, int rowsToInsertTotal, long tsIncrement, long startTs, Rnd rnd, WalWriter walWriter, boolean inOrder) {
        final int tableId = addRowsToWal(iteration, tableName, tableCopyName, rowsToInsertTotal, tsIncrement, startTs, rnd, walWriter, inOrder);
        applyWalToTable(tableName, tableId);
        return tableId;
    }

    private static void applyWalToTable(String tableName, int tableId) {
        try (SqlToOperation sqlToOperation = new SqlToOperation(engine)) {
            ApplyWal2TableJob.processWalTxnNotification(tableName, tableId, engine, sqlToOperation);
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void createTableAndCopy(String tableName, String tableCopyName) {
        // tableName is WAL enabled
        try (TableModel model = createTableModel(tableName).wal()) {
            engine.createTableUnsafe(
                    AllowAllCairoSecurityContext.INSTANCE,
                    model.getMem(),
                    model.getPath(),
                    model
            );
        }

        // tableCopyName is not WAL enabled
        try (TableModel model = createTableModel(tableCopyName).noWal()) {
            engine.createTableUnsafe(
                    AllowAllCairoSecurityContext.INSTANCE,
                    model.getMem(),
                    model.getPath(),
                    model
            );
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
                .col("geoLong", ColumnType.getGeoHashTypeWithBits(30))
                .col("stringc", ColumnType.STRING)
                .col("label", ColumnType.SYMBOL)
                .timestamp("ts");
    }
}
