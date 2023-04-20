/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.cairo.*;
import io.questdb.cutlass.line.tcp.*;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.ByteCharSequence;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class IOTableUpdateDetailsPoolTest extends AbstractCairoTest {

    private static final DefaultColumnTypes DEFAULT_COLUMN_TYPES;
    private static final DefaultLineTcpReceiverConfiguration LINE_TCP_RECEIVER_CONFIGURATION;

    @Test
    public void testConcurrentGetPut() throws Throwable {
        AtomicReference<Throwable> error = new AtomicReference<>();

        assertMemoryLeak(() -> {
            final int tableCount = 2;
            final int threadCount = 4;
            final int iterations = 1000;

            IOTableUpdateDetailsPool pool = new IOTableUpdateDetailsPool(threadCount);
            ObjList<ByteCharSequence> tableNames = new ObjList<>(2 * tableCount);
            ObjList<Thread> threads = new ObjList<>(threadCount);

            for (int i = 0; i < tableCount; i++) {
                String tableName = testName.getMethodName() + "_t" + i;
                TableToken token;
                token = createTestTable(tableName);

                for (int th = 0; th < threadCount; th++) {
                    ByteCharSequence tableNameUtf8 = new ByteCharSequence(tableName.getBytes());
                    TableUpdateDetails tud = createTud(token, tableNameUtf8);
                    pool.put(tud.getTableNameUtf8(), tud);
                    tableNames.add(tableNameUtf8);
                }
            }

            CyclicBarrier barrier = new CyclicBarrier(threadCount);
            for (int th = 0; th < threadCount; th++) {
                threads.add(new Thread(() -> {
                    try {
                        Rnd rnd = new Rnd();
                        barrier.await();
                        for (int i = 0; i < iterations; i++) {
                            ByteCharSequence tableName = tableNames.getQuick(rnd.nextInt(tableNames.size()));
                            TableUpdateDetails tud = pool.get(tableName);
                            Assert.assertNotNull(tud);
                            pool.put(tableName, tud);
                        }
                    } catch (Throwable e) {
                        error.set(e);
                    }
                }));
                threads.getLast().start();
            }


            for (int i = 0; i < threads.size(); i++) {
                threads.getQuick(i).join();
            }

            pool.close();
        });

        if (error.get() != null) {
            throw error.get();
        }
    }

    @Test
    public void testConcurrentGetPutExpire() throws Throwable {
        AtomicReference<Throwable> error = new AtomicReference<>();

        assertMemoryLeak(() -> {
            final int tableCount = 2;
            final int threadCount = 4;
            final int closeIdleThreadCount = 2;
            final int iterations = 1000;

            IOTableUpdateDetailsPool pool = new IOTableUpdateDetailsPool(threadCount);
            ObjList<ByteCharSequence> tableNames = new ObjList<>(2 * tableCount);
            ObjList<Thread> threads = new ObjList<>(threadCount);

            for (int i = 0; i < tableCount; i++) {
                String tableName = testName.getMethodName() + "_t" + i;
                createTestTable(tableName);

                for (int th = 0; th < threadCount; th++) {
                    ByteCharSequence tableNameUtf8 = new ByteCharSequence(tableName.getBytes());
                    tableNames.add(tableNameUtf8);
                }
            }

            CyclicBarrier barrier = new CyclicBarrier(threadCount + closeIdleThreadCount);
            AtomicInteger done = new AtomicInteger();
            for (int th = 0; th < threadCount; th++) {
                threads.add(new Thread(() -> {
                    try {
                        Rnd rnd = new Rnd();
                        barrier.await();
                        for (int i = 0; i < iterations; i++) {
                            ByteCharSequence tableName = tableNames.getQuick(rnd.nextInt(tableNames.size()));
                            TableUpdateDetails tud = pool.get(tableName);
                            if (tud == null) {
                                TableToken token = engine.verifyTableName(tableName);
                                tud = createTud(token, tableName);
                            }
                            pool.put(tableName, tud);
                        }
                    } catch (Throwable e) {
                        error.set(e);
                    } finally {
                        done.incrementAndGet();
                        Path.clearThreadLocals();
                    }
                }));
                threads.getLast().start();
            }

            for (int i = 0; i < closeIdleThreadCount; i++) {
                threads.add(new Thread(() -> {
                    try {
                        barrier.await();
                        while (done.get() != threadCount) {
                            pool.closeIdle(Long.MAX_VALUE, 0);
                        }
                    } catch (Throwable e) {
                        error.set(e);
                    }
                }));
                threads.getLast().start();
            }

            for (int i = 0; i < threads.size(); i++) {
                threads.getQuick(i).join();
            }

            pool.close();
        });

        if (error.get() != null) {
            throw error.get();
        }
    }

    @Test
    public void testPutClosedPool() throws Throwable {
        assertMemoryLeak(() -> {
            IOTableUpdateDetailsPool pool = new IOTableUpdateDetailsPool(1);

            String tableName = testName.getMethodName();
            TableToken token = createTestTable(tableName);
            ByteCharSequence tableNameUtf8 = new ByteCharSequence(testName.getMethodName().getBytes());
            TableUpdateDetails tud = createTud(token, tableNameUtf8);

            pool.close();

            pool.put(tableName, tud);
            Assert.assertNull(pool.get(tableName));
        });
    }

    @NotNull
    private static TableToken createTestTable(String tableName) {
        TableToken token;
        try (TableModel tm = new TableModel(configuration, tableName, PartitionBy.DAY)) {
            tm.col("sym", ColumnType.SYMBOL);
            tm.col("bid", ColumnType.DOUBLE);
            tm.col("ask", ColumnType.DOUBLE);
            tm.timestamp("ts");
            tm.wal();
            token = engine.createTable(
                    securityContext,
                    tm.getMem(),
                    Path.PATH.get(),
                    false,
                    tm,
                    false
            );
            Assert.assertTrue(engine.isWalTable(token));
        }
        return token;
    }

    @NotNull
    private static TableUpdateDetails createTud(TableToken tableToken, ByteCharSequence tableNameUtf8) {
        return new TableUpdateDetails(
                LINE_TCP_RECEIVER_CONFIGURATION,
                engine,
                engine.getTableWriterAPI(tableToken, "test"),
                -1,
                new NetworkIOJob[0],
                DEFAULT_COLUMN_TYPES,
                tableNameUtf8
        );
    }

    static {
        LINE_TCP_RECEIVER_CONFIGURATION = new DefaultLineTcpReceiverConfiguration();
        DEFAULT_COLUMN_TYPES = new DefaultColumnTypes(LINE_TCP_RECEIVER_CONFIGURATION);
    }
}