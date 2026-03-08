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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Chars;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class BitmapIndexConcurrentFuzzTest extends AbstractCairoTest {
    private static final int MAX_ID = 100;

    @Test
    public void testConcurrentBitmapIndexAccess() throws Exception {
        final Rnd masterRnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            Overrides overrides = node1.getConfigurationOverrides();
            overrides.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);

            execute(
                    "CREATE TABLE trades (" +
                            "  id INT," +
                            "  symbol SYMBOL INDEX," +
                            "  ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY HOUR WAL"
            );

            populateTable();
            testConcurrentOperations(masterRnd);
        });
    }

    private void populateTable() {
        try (TableWriterAPI w = engine.getTableWriterAPI("trades", "initial population")) {
            long baseTimestamp = 1704067200000000L; // 2024-01-01T00:00:00.000000Z in micros
            int currentId = 0;

            for (int partition = 0; partition < 10; partition++) {
                long partitionTimestamp = baseTimestamp + (partition * 3600000000L);

                for (int i = 1; i <= 100; i++) {
                    TableWriter.Row r = w.newRow(partitionTimestamp + (i * 100000L));
                    r.putInt(0, currentId % MAX_ID);
                    r.putSym(1, "SYM" + i);
                    r.append();
                    currentId++;
                }
            }
            w.commit();
        }
        drainWalQueue();
    }

    private void testConcurrentOperations(Rnd masterRnd) throws Exception {
        final int numWriterThreads = 3;
        final int numReaderThreads = 5;
        final int numUpdateThreads = 1;

        final AtomicBoolean stopFlag = new AtomicBoolean(false);
        final AtomicInteger totalInserts = new AtomicInteger(0);
        final AtomicInteger totalQueries = new AtomicInteger(0);
        final AtomicInteger totalUpdates = new AtomicInteger(0);
        final AtomicInteger nextId = new AtomicInteger(1000);
        final AtomicInteger errorCount = new AtomicInteger(0);
        final AtomicReference<Throwable> firstError = new AtomicReference<>();

        final CyclicBarrier startBarrier = new CyclicBarrier(numWriterThreads + numReaderThreads + numUpdateThreads + 1);
        final SOCountDownLatch completionLatch = new SOCountDownLatch(numWriterThreads + numReaderThreads + numUpdateThreads);

        ExecutorService executor = Executors.newFixedThreadPool(numWriterThreads + numReaderThreads + numUpdateThreads);

        // Writer threads
        for (int i = 0; i < numWriterThreads; i++) {
            final int threadId = i;
            final long seed0 = masterRnd.nextLong();
            final long seed1 = masterRnd.nextLong();

            executor.submit(() -> {
                try {
                    startBarrier.await();

                    Rnd rnd = new Rnd(seed0, seed1);
                    try (TableWriterAPI w = engine.getTableWriterAPI("trades", "Concurrent Writer " + threadId)) {
                        do {
                            try {
                                for (int batch = 0; batch < 10; batch++) {
                                    int id = nextId.getAndIncrement() % MAX_ID;
                                    String randomSymbol = "SYM" + (rnd.nextInt(100) + 1);
                                    long oooTimestamp = System.currentTimeMillis() * 1000L - (rnd.nextInt(600) * 60000000L);

                                    TableWriter.Row r = w.newRow(oooTimestamp);
                                    r.putInt(0, id);
                                    r.putSym(1, randomSymbol);
                                    r.append();
                                }
                                w.commit();
                                drainWalQueue();
                                totalInserts.addAndGet(10);

                                if (rnd.nextInt(10) == 0) {
                                    Os.sleep(rnd.nextInt(10));
                                }
                            } catch (Exception e) {
                                errorCount.incrementAndGet();
                                firstError.compareAndSet(null, e);
                                e.printStackTrace();
                            }
                        } while (!stopFlag.get() && errorCount.get() == 0);
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    firstError.compareAndSet(null, e);
                    e.printStackTrace();
                } finally {
                    Path.clearThreadLocals();
                    completionLatch.countDown();
                }
            });
        }

        // Update threads
        for (int i = 0; i < numUpdateThreads; i++) {
            final long seed0 = masterRnd.nextLong();
            final long seed1 = masterRnd.nextLong();

            executor.submit(() -> {
                try {
                    startBarrier.await();

                    Rnd rnd = new Rnd(seed0, seed1);
                    // Create thread-local SqlExecutionContext to avoid concurrency issues
                    try (var threadLocalContext = TestUtils.createSqlExecutionCtx(engine)) {
                        do {
                            try {
                                int randomId = rnd.nextInt(MAX_ID);
                                String randomSymbol = "SYM" + (rnd.nextInt(100) + 1);

                                String updateSql = String.format(
                                        "UPDATE trades SET symbol = '%s' WHERE id = %d",
                                        randomSymbol, randomId
                                );

                                engine.execute(updateSql, threadLocalContext);
                                drainWalQueue();
                                totalUpdates.incrementAndGet();

                                // UPDATE is slow, we cannot do it too frequently
                                Os.sleep(rnd.nextInt(100) + 1);
                            } catch (Exception e) {
                                errorCount.incrementAndGet();
                                firstError.compareAndSet(null, e);
                                e.printStackTrace();
                            }
                        } while (!stopFlag.get() && errorCount.get() == 0);
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    firstError.compareAndSet(null, e);
                    e.printStackTrace();
                } finally {
                    Path.clearThreadLocals();
                    completionLatch.countDown();
                }
            });
        }

        // Reader threads
        for (int i = 0; i < numReaderThreads; i++) {
            final int threadId = i;
            // Generate unique seeds for this thread using masterRnd
            final long seed0 = masterRnd.nextLong();
            final long seed1 = masterRnd.nextLong();

            executor.submit(() -> {
                try {
                    startBarrier.await(); // Wait for all threads to be ready

                    Rnd rnd = new Rnd(seed0, seed1); // Unique deterministic seeds per thread
                    try (var threadLocalContext = TestUtils.createSqlExecutionCtx(engine)) {
                        do {
                            try {
                                String randomSymbol = "SYM" + (rnd.nextInt(100) + 1);
                                String querySql = String.format(
                                        "SELECT symbol, count(*) FROM trades WHERE symbol = '%s' GROUP BY symbol",
                                        randomSymbol
                                );

                                try (RecordCursorFactory factory = select(querySql, threadLocalContext)) {
                                    try (RecordCursor cursor = factory.getCursor(threadLocalContext)) {
                                        int rowCount = 0;
                                        CharSequence foundSymbol = null;

                                        while (cursor.hasNext()) {
                                            rowCount++;
                                            if (rowCount > 1) {
                                                errorCount.incrementAndGet();
                                                firstError.compareAndSet(null, new AssertionError("Reader " + threadId + ": Multiple rows for symbol " + randomSymbol));
                                                return;
                                            }
                                            foundSymbol = cursor.getRecord().getSymA(0);
                                        }

                                        if (rowCount == 1 && !Chars.equals(foundSymbol, randomSymbol)) {
                                            errorCount.incrementAndGet();
                                            firstError.compareAndSet(null, new AssertionError("Reader " + threadId + ": Expected " + randomSymbol + " but got " + foundSymbol));
                                            return;
                                        }

                                        // It's OK if rowCount is 0 or 1 (symbol might not exist yet or might exist)
                                        totalQueries.incrementAndGet();
                                    }
                                }

                                Os.sleep(rnd.nextInt(5) + 1);
                            } catch (Exception e) {
                                errorCount.incrementAndGet();
                                firstError.compareAndSet(null, e);
                                e.printStackTrace();
                            }
                        } while (!stopFlag.get() && errorCount.get() == 0);
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    firstError.compareAndSet(null, e);
                    e.printStackTrace();
                } finally {
                    Path.clearThreadLocals();
                    completionLatch.countDown();
                }
            });
        }

        startBarrier.await();
        System.out.println("Started " + numWriterThreads + " writer threads, " + numUpdateThreads + " update threads, and " + numReaderThreads + " reader threads");

        stopFlag.set(true);
        completionLatch.await();

        executor.shutdown();
        Assert.assertTrue("failed to terminate threads within 60s", executor.awaitTermination(60, TimeUnit.SECONDS));

        System.out.println("Test completed: " + totalInserts.get() + " inserts, " + totalUpdates.get() + " updates, " + totalQueries.get() + " queries");
        if (errorCount.get() > 0) {
            throw new AssertionError("Concurrent test failed with " + errorCount.get() + " errors. First error: ", firstError.get());
        }
    }
}