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

package io.questdb.test.cutlass.http.line;

import io.questdb.cutlass.line.http.BufferSetPool;
import io.questdb.cutlass.line.http.PendingFlush;
import io.questdb.cutlass.line.tcp.v4.IlpV4TableBuffer;
import io.questdb.std.ObjList;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cutlass.line.tcp.v4.IlpV4Constants.TYPE_LONG;

/**
 * Tests for BufferSetPool class.
 */
public class BufferSetPoolTest {

    // ======================== Basic Tests ========================

    @Test
    public void testAcquireReturnsEmptyBufferSet() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (BufferSetPool pool = new BufferSetPool(4)) {
                BufferSetPool.BufferSet set = pool.acquire();
                Assert.assertNotNull(set);
                Assert.assertTrue(set.isEmpty());
                Assert.assertEquals(0, set.getTableCount());
                Assert.assertNotNull(set.getTableBuffers());
                Assert.assertNotNull(set.getTableOrder());
            }
        });
    }

    @Test
    public void testAcquireMultipleReturnsDistinctSets() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (BufferSetPool pool = new BufferSetPool(4)) {
                BufferSetPool.BufferSet set1 = pool.acquire();
                BufferSetPool.BufferSet set2 = pool.acquire();
                BufferSetPool.BufferSet set3 = pool.acquire();

                Assert.assertNotSame(set1, set2);
                Assert.assertNotSame(set2, set3);
                Assert.assertNotSame(set1, set3);
            }
        });
    }

    @Test
    public void testReleaseAndReacquire() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (BufferSetPool pool = new BufferSetPool(4)) {
                Assert.assertEquals(0, pool.size());

                BufferSetPool.BufferSet set1 = pool.acquire();
                pool.release(set1);
                Assert.assertEquals(1, pool.size());

                BufferSetPool.BufferSet set2 = pool.acquire();
                Assert.assertEquals(0, pool.size());

                // Should get the same object back (reused)
                Assert.assertSame(set1, set2);
            }
        });
    }

    @Test
    public void testReleaseResetsBufferSet() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (BufferSetPool pool = new BufferSetPool(4)) {
                BufferSetPool.BufferSet set = pool.acquire();

                // Add some data
                IlpV4TableBuffer buffer = new IlpV4TableBuffer("test");
                buffer.getOrCreateColumn("v", TYPE_LONG, false).addLong(1);
                buffer.nextRow();
                set.getTableBuffers().put("test", buffer);
                set.getTableOrder().add("test");

                Assert.assertEquals(1, buffer.getRowCount());

                pool.release(set);

                // After release, buffer should be reset (row count = 0)
                Assert.assertEquals(0, buffer.getRowCount());
                // But column definitions should still exist
                Assert.assertEquals(1, buffer.getColumnCount());
            }
        });
    }

    @Test
    public void testAcquireClearsBufferSet() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (BufferSetPool pool = new BufferSetPool(4)) {
                BufferSetPool.BufferSet set = pool.acquire();

                // Add some data
                IlpV4TableBuffer buffer = new IlpV4TableBuffer("test");
                buffer.getOrCreateColumn("v", TYPE_LONG, false).addLong(1);
                buffer.nextRow();
                set.getTableBuffers().put("test", buffer);
                set.getTableOrder().add("test");

                pool.release(set);

                // Reacquire
                BufferSetPool.BufferSet reacquired = pool.acquire();
                Assert.assertSame(set, reacquired);

                // After acquire, set should be completely cleared
                Assert.assertTrue(reacquired.isEmpty());
                Assert.assertEquals(0, reacquired.getTableCount());
                Assert.assertEquals(0, reacquired.getTableBuffers().size());
                Assert.assertEquals(0, reacquired.getTableOrder().size());
            }
        });
    }

    // ======================== Pool Size Limit Tests ========================

    @Test
    public void testPoolSizeLimit() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (BufferSetPool pool = new BufferSetPool(2)) {
                BufferSetPool.BufferSet set1 = pool.acquire();
                BufferSetPool.BufferSet set2 = pool.acquire();
                BufferSetPool.BufferSet set3 = pool.acquire();

                // Release all three
                pool.release(set1);
                pool.release(set2);
                pool.release(set3);

                // Pool should only keep max 2
                Assert.assertEquals(2, pool.size());
            }
        });
    }

    @Test
    public void testPoolSizeOne() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (BufferSetPool pool = new BufferSetPool(1)) {
                BufferSetPool.BufferSet set1 = pool.acquire();
                BufferSetPool.BufferSet set2 = pool.acquire();

                pool.release(set1);
                pool.release(set2);

                // Pool should only keep 1
                Assert.assertEquals(1, pool.size());
            }
        });
    }

    // ======================== Close Tests ========================

    @Test
    public void testCloseEmptiesPool() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            BufferSetPool pool = new BufferSetPool(4);
            BufferSetPool.BufferSet set = pool.acquire();
            pool.release(set);

            Assert.assertEquals(1, pool.size());
            Assert.assertFalse(pool.isClosed());

            pool.close();

            Assert.assertTrue(pool.isClosed());
            Assert.assertEquals(0, pool.size());
        });
    }

    @Test
    public void testAcquireAfterCloseReturnsNewSet() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            BufferSetPool pool = new BufferSetPool(4);
            BufferSetPool.BufferSet set1 = pool.acquire();
            pool.release(set1);
            pool.close();

            // Acquire after close should return new set (not throw)
            BufferSetPool.BufferSet set2 = pool.acquire();
            Assert.assertNotNull(set2);
            Assert.assertTrue(set2.isEmpty());
        });
    }

    @Test
    public void testReleaseAfterCloseIsIgnored() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            BufferSetPool pool = new BufferSetPool(4);
            BufferSetPool.BufferSet set = pool.acquire();
            pool.close();

            // Release after close should not throw, just discard
            pool.release(set);
            Assert.assertEquals(0, pool.size());
        });
    }

    @Test
    public void testDoubleCloseIsSafe() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            BufferSetPool pool = new BufferSetPool(4);
            pool.close();
            pool.close(); // Should not throw
            Assert.assertTrue(pool.isClosed());
        });
    }

    // ======================== Null Handling Tests ========================

    @Test
    public void testReleaseNullIsIgnored() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (BufferSetPool pool = new BufferSetPool(4)) {
                pool.release(null);
                Assert.assertEquals(0, pool.size());
            }
        });
    }

    // ======================== PendingFlush Integration Tests ========================

    @Test
    public void testReleaseFromPendingFlush() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (BufferSetPool pool = new BufferSetPool(4)) {
                Map<String, IlpV4TableBuffer> buffers = new HashMap<>();
                ObjList<String> order = new ObjList<>();

                IlpV4TableBuffer buffer = new IlpV4TableBuffer("test");
                buffer.getOrCreateColumn("v", TYPE_LONG, false).addLong(42);
                buffer.nextRow();
                buffers.put("test", buffer);
                order.add("test");

                PendingFlush flush = new PendingFlush(buffers, order, 1, 0, true, false);
                pool.releaseFromPendingFlush(flush);

                Assert.assertEquals(1, pool.size());

                // Acquire - the set is cleared for fresh use
                BufferSetPool.BufferSet set = pool.acquire();
                // After acquire, the set is cleared (ready for new tables)
                Assert.assertTrue(set.isEmpty());
                Assert.assertEquals(0, set.getTableCount());
            }
        });
    }

    @Test
    public void testReleaseFromNullPendingFlush() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (BufferSetPool pool = new BufferSetPool(4)) {
                pool.releaseFromPendingFlush(null);
                Assert.assertEquals(0, pool.size());
            }
        });
    }

    @Test
    public void testReleaseFromPendingFlushAfterClose() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            BufferSetPool pool = new BufferSetPool(4);
            pool.close();

            Map<String, IlpV4TableBuffer> buffers = new HashMap<>();
            ObjList<String> order = new ObjList<>();
            PendingFlush flush = new PendingFlush(buffers, order, 0, 0, true, false);

            pool.releaseFromPendingFlush(flush);
            Assert.assertEquals(0, pool.size());
        });
    }

    // ======================== BufferSet Tests ========================

    @Test
    public void testBufferSetReset() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            BufferSetPool.BufferSet set = new BufferSetPool.BufferSet();

            IlpV4TableBuffer buffer = new IlpV4TableBuffer("t");
            buffer.getOrCreateColumn("v", TYPE_LONG, false).addLong(1);
            buffer.nextRow();
            set.getTableBuffers().put("t", buffer);
            set.getTableOrder().add("t");

            Assert.assertEquals(1, buffer.getRowCount());

            set.reset();

            // Reset should clear row data but keep structure
            Assert.assertEquals(0, buffer.getRowCount());
            Assert.assertEquals(1, buffer.getColumnCount());
            Assert.assertEquals(1, set.getTableCount());
        });
    }

    @Test
    public void testBufferSetClear() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            BufferSetPool.BufferSet set = new BufferSetPool.BufferSet();

            IlpV4TableBuffer buffer = new IlpV4TableBuffer("t");
            buffer.getOrCreateColumn("v", TYPE_LONG, false).addLong(1);
            buffer.nextRow();
            set.getTableBuffers().put("t", buffer);
            set.getTableOrder().add("t");

            set.clear();

            // Clear should remove everything
            Assert.assertEquals(0, buffer.getRowCount());
            Assert.assertEquals(0, buffer.getColumnCount());
            Assert.assertEquals(0, set.getTableCount());
            Assert.assertTrue(set.isEmpty());
        });
    }

    // ======================== Concurrent Access Tests ========================

    @Test
    public void testConcurrentAcquireRelease() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int poolSize = 4;
            final int numThreads = 8;
            final int operationsPerThread = 100;

            try (BufferSetPool pool = new BufferSetPool(poolSize)) {
                ExecutorService executor = Executors.newFixedThreadPool(numThreads);
                CyclicBarrier barrier = new CyclicBarrier(numThreads);
                AtomicInteger errorCount = new AtomicInteger(0);

                List<Future<?>> futures = new ArrayList<>();
                for (int i = 0; i < numThreads; i++) {
                    futures.add(executor.submit(() -> {
                        try {
                            barrier.await();
                            for (int j = 0; j < operationsPerThread; j++) {
                                BufferSetPool.BufferSet set = pool.acquire();
                                // Simulate some work
                                Thread.sleep(1);
                                pool.release(set);
                            }
                        } catch (Exception e) {
                            errorCount.incrementAndGet();
                        }
                    }));
                }

                for (Future<?> future : futures) {
                    future.get(30, TimeUnit.SECONDS);
                }

                executor.shutdown();
                Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
                Assert.assertEquals(0, errorCount.get());
                Assert.assertTrue(pool.size() <= poolSize);
            }
        });
    }

    @Test
    public void testConcurrentAcquireWithClose() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int numThreads = 5;
            BufferSetPool pool = new BufferSetPool(4);
            CountDownLatch startLatch = new CountDownLatch(1);
            AtomicInteger successCount = new AtomicInteger(0);

            List<Thread> threads = new ArrayList<>();
            for (int i = 0; i < numThreads; i++) {
                Thread t = new Thread(() -> {
                    try {
                        startLatch.await();
                        for (int j = 0; j < 50; j++) {
                            BufferSetPool.BufferSet set = pool.acquire();
                            if (set != null) {
                                successCount.incrementAndGet();
                                pool.release(set);
                            }
                        }
                    } catch (Exception e) {
                        // Expected when pool closes
                    }
                });
                t.start();
                threads.add(t);
            }

            startLatch.countDown();
            Thread.sleep(50);
            pool.close();

            for (Thread t : threads) {
                t.join(5000);
            }

            // Some operations should have succeeded before close
            Assert.assertTrue(successCount.get() > 0);
            Assert.assertTrue(pool.isClosed());
        });
    }

    @Test
    public void testHighContention() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Very small pool, many threads to maximize contention
            final int poolSize = 2;
            final int numThreads = 20;
            final int operationsPerThread = 50;

            try (BufferSetPool pool = new BufferSetPool(poolSize)) {
                ExecutorService executor = Executors.newFixedThreadPool(numThreads);
                CyclicBarrier barrier = new CyclicBarrier(numThreads);
                AtomicInteger totalOps = new AtomicInteger(0);

                List<Future<?>> futures = new ArrayList<>();
                for (int i = 0; i < numThreads; i++) {
                    futures.add(executor.submit(() -> {
                        try {
                            barrier.await();
                            for (int j = 0; j < operationsPerThread; j++) {
                                BufferSetPool.BufferSet set = pool.acquire();
                                Assert.assertNotNull(set);
                                Assert.assertTrue(set.isEmpty());
                                totalOps.incrementAndGet();
                                // Very short operation
                                pool.release(set);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }));
                }

                for (Future<?> future : futures) {
                    future.get(60, TimeUnit.SECONDS);
                }

                executor.shutdown();
                Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
                Assert.assertEquals(numThreads * operationsPerThread, totalOps.get());
            }
        });
    }
}
