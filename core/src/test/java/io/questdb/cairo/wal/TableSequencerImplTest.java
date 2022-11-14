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
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.hamcrest.number.OrderingComparison;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class TableSequencerImplTest extends AbstractCairoTest {
    @BeforeClass
    public static void setUpStatic() {
        recreateDistressedSequencerAttempts = Integer.MAX_VALUE;
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testCopyMetadataRace() throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(2);
        final String tableName = testName.getMethodName();
        AtomicReference<Throwable> exception = new AtomicReference<>();
        int iterations = 100;
        int initialColumnCount = 2;

        runAddColumnRace(
                barrier, tableName, iterations, 1,
                () -> {
                    try (GenericTableRecordMetadata metadata = new GenericTableRecordMetadata()) {
                        TestUtils.await(barrier);

                        do {
                            engine.getTableSequencerAPI().getTableMetadata(tableName, metadata);
                            MatcherAssert.assertThat((int) metadata.getStructureVersion(), Matchers.equalTo(metadata.getColumnCount() - initialColumnCount));
                        } while (metadata.getColumnCount() < initialColumnCount + iterations && exception.get() == null);
                    } catch (Throwable e) {
                        exception.set(e);
                    } finally {
                        Path.clearThreadLocals();
                    }
                });
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
                barrier, tableName, iterations, readerCount,
                () -> {
                    try {
                        TestUtils.await(barrier);
                        int threadIdValue = threadId.getAndIncrement();
                        long sv = 0;
                        do {
                            long sv2 = engine.getTableSequencerAPI().lastTxn(tableName);
                            if (threadIdValue != 0) {
                                engine.getTableSequencerAPI().setDistressed(tableName);
                                if (sv != sv2) {
                                    sv = sv2;
                                    LOG.info().$("destroyed sv ").$(sv).$();
                                }
                            } else {
                                try (TransactionLogCursor cursor = engine.getTableSequencerAPI().getCursor(tableName, 0)) {
                                    long transactions = 0;
                                    while (cursor.hasNext()) {
                                        transactions++;
                                    }
                                    MatcherAssert.assertThat(transactions, OrderingComparison.greaterThanOrEqualTo(sv2));
                                }
                            }
                        } while (engine.getTableSequencerAPI().lastTxn(tableName) < iterations && exception.get() == null);
                    } catch (Throwable e) {
                        exception.set(e);
                    } finally {
                        Path.clearThreadLocals();
                    }
                });
    }

    @Test
    public void testGetTxnRace() throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(2);
        final String tableName = testName.getMethodName();
        AtomicReference<Throwable> exception = new AtomicReference<>();
        int iterations = 100;

        runAddColumnRace(
                barrier, tableName, iterations, 1,
                () -> {
                    try {
                        TestUtils.await(barrier);
                        do {
                            engine.getTableSequencerAPI().lastTxn(tableName);
                        } while (engine.getTableSequencerAPI().lastTxn(tableName) < iterations && exception.get() == null);
                    } catch (Throwable e) {
                        exception.set(e);
                    } finally {
                        Path.clearThreadLocals();
                    }
                });
    }

    @Test
    public void testTxnDistressedCursorRace() throws Exception {
        int readers = 3;
        CyclicBarrier barrier = new CyclicBarrier(readers + 1);
        final String tableName = testName.getMethodName();
        AtomicReference<Throwable> exception = new AtomicReference<>();
        int iterations = 100;

        runAddColumnRace(
                barrier, tableName, iterations, readers,
                () -> {
                    try {
                        TestUtils.await(barrier);
                        long lastTxn = 0;
                        do {
                            try (TransactionLogCursor cursor = engine.getTableSequencerAPI().getCursor(tableName, lastTxn)) {
                                while (cursor.hasNext()) {
                                    lastTxn = cursor.getTxn();
                                }
                            }
                        } while (engine.getTableSequencerAPI().lastTxn(tableName) < iterations && exception.get() == null);
                    } catch (Throwable e) {
                        exception.set(e);
                    } finally {
                        Path.clearThreadLocals();
                    }
                });
    }

    private void runAddColumnRace(CyclicBarrier barrier, String tableName, int iterations, int readerThreads, Runnable runnable) throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR)
                    .col("int", ColumnType.INT)
                    .timestamp("ts")
                    .wal()) {
                engine.createTableUnsafe(
                        AllowAllCairoSecurityContext.INSTANCE,
                        model.getMem(),
                        model.getPath(),
                        model
                );
            }
            AtomicReference<Throwable> exception = new AtomicReference<>();
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
        try (WalWriter ww = engine.getWalWriter(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
            TestUtils.await(barrier);

            for (int i = 0; i < iterations; i++) {
                addColumn(ww, "newCol" + i);
                if (exception.get() != null) {
                    break;
                }
            }
        } catch (Throwable e) {
            exception.set(e);
        }
    }

    static void addColumn(TableWriterAPI writer, String columnName) {
        AlterOperationBuilder addColumnC = new AlterOperationBuilder().ofAddColumn(0, Chars.toString(writer.getTableName()), 0);
        addColumnC.ofAddColumn(columnName, 11, ColumnType.INT, 0, false, false, 0);
        writer.apply(addColumnC.build(), true);
    }
}
