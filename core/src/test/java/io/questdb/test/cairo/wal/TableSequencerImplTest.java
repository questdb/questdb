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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.TableTransactionLog;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class TableSequencerImplTest extends AbstractCairoTest {
    @BeforeClass
    public static void setUpStatic() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_RECREATE_DISTRESSED_SEQUENCER_ATTEMPTS, Integer.MAX_VALUE);
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testCanReadStructureVersionV1() {
        testTableTransactionLogCanReadStructureVersion();
    }

    @Test
    public void testCanReadStructureVersionV2() {
        Rnd rnd = TestUtils.generateRandom(LOG);
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, rnd.nextInt(20) + 10);
        testTableTransactionLogCanReadStructureVersion();
    }

    @Test
    public void testCopyMetadataRace() throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(2);
        final String tableName = testName.getMethodName();
        AtomicReference<Throwable> exception = new AtomicReference<>();
        int iterations = 100;
        int initialColumnCount = 2;

        runAddColumnRace(
                barrier, tableName, iterations, 1, exception,
                () -> {
                    try {
                        TestUtils.await(barrier);

                        TableToken tableToken = engine.verifyTableName(tableName);
                        int metadataColumnCount;
                        do {
                            try (TableRecordMetadata metadata = engine.getSequencerMetadata(tableToken)) {
                                Assert.assertEquals(metadata.getColumnCount() - initialColumnCount, metadata.getMetadataVersion());
                                metadataColumnCount = metadata.getColumnCount();
                            }
                        } while (metadataColumnCount < initialColumnCount + iterations && exception.get() == null);
                    } catch (Throwable e) {
                        exception.set(e);
                    } finally {
                        Path.clearThreadLocals();
                    }
                }
        );
    }

    @Test
    public void testGetCursorDistressedSequencerRace() throws Exception {
        int readerCount = 2;
        CyclicBarrier barrier = new CyclicBarrier(readerCount + 1);
        final String tableName = testName.getMethodName();
        AtomicReference<Throwable> exception = new AtomicReference<>();
        int iterations = 50;

        AtomicInteger threadId = new AtomicInteger();

        runAddColumnRace(
                barrier, tableName, iterations, readerCount, exception,
                () -> {
                    try {
                        TestUtils.await(barrier);
                        int threadIdValue = threadId.getAndIncrement();
                        long sv = 0;
                        TableToken tableToken = engine.verifyTableName(tableName);
                        do {
                            long sv2 = engine.getTableSequencerAPI().lastTxn(tableToken);
                            if (threadIdValue != 0) {
                                engine.getTableSequencerAPI().setDistressed(tableToken);
                                if (sv != sv2) {
                                    sv = sv2;
                                    LOG.info().$("destroyed sv ").$(sv).$();
                                }
                            } else {
                                try (TransactionLogCursor cursor = engine.getTableSequencerAPI().getCursor(tableToken, 0)) {
                                    long transactions = 0;
                                    while (cursor.hasNext()) {
                                        transactions++;
                                    }
                                    Assert.assertTrue(transactions >= sv2);
                                }
                            }
                        } while (engine.getTableSequencerAPI().lastTxn(tableToken) < iterations && exception.get() == null);
                    } catch (Throwable e) {
                        exception.set(e);
                    } finally {
                        Path.clearThreadLocals();
                    }
                }
        );
    }

    @Test
    public void testGetTxnRace() throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(2);
        final String tableName = testName.getMethodName();
        AtomicReference<Throwable> exception = new AtomicReference<>();
        int iterations = 100;

        runAddColumnRace(
                barrier, tableName, iterations, 1, exception,
                () -> {
                    try {
                        TestUtils.await(barrier);
                        TableToken tableToken = engine.verifyTableName(tableName);
                        do {
                            engine.getTableSequencerAPI().lastTxn(tableToken);
                        } while (engine.getTableSequencerAPI().lastTxn(tableToken) < iterations && exception.get() == null);
                    } catch (Throwable e) {
                        exception.set(e);
                    } finally {
                        Path.clearThreadLocals();
                    }
                }
        );
    }

    @Test
    public void testTxnDistressedCursorRace() throws Exception {
        int readers = 3;
        CyclicBarrier barrier = new CyclicBarrier(readers + 1);
        final String tableName = testName.getMethodName();
        AtomicReference<Throwable> exception = new AtomicReference<>();
        int iterations = 100;

        runAddColumnRace(
                barrier, tableName, iterations, readers, exception,
                () -> {
                    try {
                        TestUtils.await(barrier);
                        TableToken tableToken = engine.verifyTableName(tableName);
                        long lastTxn = 0;
                        do {
                            try (TransactionLogCursor cursor = engine.getTableSequencerAPI().getCursor(tableToken, lastTxn)) {
                                while (cursor.hasNext()) {
                                    lastTxn = cursor.getTxn();
                                }
                            }
                        } while (engine.getTableSequencerAPI().lastTxn(tableToken) < iterations && exception.get() == null);
                    } catch (Throwable e) {
                        exception.set(e);
                    } finally {
                        Path.clearThreadLocals();
                    }
                }
        );
    }

    private void runAddColumnRace(CyclicBarrier barrier, String tableName, int iterations, int readerThreads, AtomicReference<Throwable> exception, Runnable runnable) throws Exception {
        assertMemoryLeak(() -> {
            TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR)
                    .col("int", ColumnType.INT)
                    .timestamp("ts")
                    .wal();
            createTable(model);
            ObjList<Thread> readerThreadList = new ObjList<>();
            for (int i = 0; i < readerThreads; i++) {
                Thread t = new Thread(runnable);
                readerThreadList.add(t);
                t.start();
            }

            runColumnAdd(barrier, tableName, exception, iterations);

            for (int i = 0; i < readerThreads; i++) {
                readerThreadList.get(i).join();
            }

            if (exception.get() != null) {
                throw new AssertionError(exception.get());
            }
        });
    }

    private void runColumnAdd(CyclicBarrier barrier, String tableName, AtomicReference<Throwable> exception, int iterations) {
        try (WalWriter ww = engine.getWalWriter(engine.verifyTableName(tableName))) {
            TestUtils.await(barrier);

            for (int i = 0; i < iterations; i++) {
                addColumn(ww, "newCol" + i, ColumnType.INT);
                if (exception.get() != null) {
                    break;
                }
            }
        } catch (Throwable e) {
            exception.set(e);
        }
    }

    private void testTableTransactionLogCanReadStructureVersion() {
        final String tableName = testName.getMethodName();
        int iterations = 100;

        TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR)
                .col("int", ColumnType.INT)
                .timestamp("ts")
                .wal();
        createTable(model);

        TableToken tableToken = engine.verifyTableName(tableName);
        try (
                Path path = new Path();
                WalWriter ww = engine.getWalWriter(tableToken)
        ) {

            path.concat(engine.getConfiguration().getDbRoot()).concat(ww.getTableToken()).concat(WalUtils.SEQ_DIR);
            for (int i = 0; i < iterations; i++) {
                addColumn(ww, "newCol" + i, ColumnType.INT);
                try (TableRecordMetadata metadata = engine.getSequencerMetadata(tableToken)) {
                    Assert.assertEquals(i + 1, metadata.getMetadataVersion());
                    long seqMeta = TableTransactionLog.readMaxStructureVersion(engine.getConfiguration().getFilesFacade(), path);
                    Assert.assertEquals(metadata.getMetadataVersion(), seqMeta);
                }
            }
        }
    }
}
