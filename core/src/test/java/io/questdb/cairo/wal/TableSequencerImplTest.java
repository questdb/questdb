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
import io.questdb.cairo.wal.seq.SequencerMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.std.Chars;
import io.questdb.test.tools.TestUtils;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

public class TableSequencerImplTest extends AbstractCairoTest {

    @Test
    public void testCopyMetadataRace() throws InterruptedException {
        CyclicBarrier barrier = new CyclicBarrier(2);
        final String tableName = testName.getMethodName();
        AtomicReference<Throwable> exception = new AtomicReference<>();
        int iterations = 100;
        int initialColumnCount = 2;

        runAddColumnRace(
                barrier, tableName, iterations,
                () -> {
                    try (SequencerMetadata metadata = new SequencerMetadata(engine.getConfiguration().getFilesFacade())) {
                        TestUtils.await(barrier);

                        do {
                            engine.getTableSequencerAPI().copyMetadataTo(tableName, metadata);
                            MatcherAssert.assertThat((int) metadata.getStructureVersion(), Matchers.equalTo(metadata.getColumnCount() - initialColumnCount));
                        } while (metadata.getColumnCount() < initialColumnCount + iterations && exception.get() == null);
                    } catch (Throwable e) {
                        exception.set(e);
                    }
                });
    }

    @Test
    public void testGetTxnRace() throws InterruptedException {
        CyclicBarrier barrier = new CyclicBarrier(2);
        final String tableName = testName.getMethodName();
        AtomicReference<Throwable> exception = new AtomicReference<>();
        int iterations = 100;

        runAddColumnRace(
                barrier, tableName, iterations,
                () -> {
                    try {
                        TestUtils.await(barrier);
                        do {
                            engine.getTableSequencerAPI().lastTxn(tableName);
                        } while (engine.getTableSequencerAPI().lastTxn(tableName) < iterations && exception.get() == null);
                    } catch (Throwable e) {
                        exception.set(e);
                    }
                });
    }

    static void addColumn(TableWriterAPI writer, String columnName, int columnType) throws SqlException {
        AlterOperationBuilder addColumnC = new AlterOperationBuilder().ofAddColumn(0, Chars.toString(writer.getTableName()), 0);
        addColumnC.ofAddColumn(columnName, columnType, 0, false, false, 0);
        writer.apply(addColumnC.build(), true);
    }

    private void runAddColumnRace(CyclicBarrier barrier, String tableName, int iterations, Runnable runnable) throws InterruptedException {
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
        Thread copyThread = new Thread(runnable);
        copyThread.start();
        runColumnAdd(barrier, tableName, exception, iterations);
        copyThread.join();

        if (exception.get() != null) {
            throw new AssertionError(exception.get());
        }
    }

    private void runColumnAdd(CyclicBarrier barrier, String tableName, AtomicReference<Throwable> exception, int iterations) throws InterruptedException {
        try (WalWriter ww = engine.getTableSequencerAPI().getWalWriter(tableName)) {
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
}
