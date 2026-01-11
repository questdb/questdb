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

import io.questdb.DefaultHttpClientConfiguration;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.line.http.HttpClientPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Comprehensive tests for HttpClientPool thread safety, blocking behavior,
 * and edge cases.
 */
public class HttpClientPoolTest {

    // ======================== Construction Tests ========================

    @Test
    public void testConstructionWithValidPoolSize() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (HttpClientPool pool = new HttpClientPool(DefaultHttpClientConfiguration.INSTANCE, null, 3)) {
                Assert.assertEquals(3, pool.getPoolSize());
                Assert.assertEquals(3, pool.getAvailableCount());
                Assert.assertFalse(pool.isClosed());
            }
        });
    }

    @Test
    public void testConstructionWithPoolSizeOne() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (HttpClientPool pool = new HttpClientPool(DefaultHttpClientConfiguration.INSTANCE, null, 1)) {
                Assert.assertEquals(1, pool.getPoolSize());
                Assert.assertEquals(1, pool.getAvailableCount());
            }
        });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructionWithZeroPoolSizeThrows() {
        new HttpClientPool(DefaultHttpClientConfiguration.INSTANCE, null, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructionWithNegativePoolSizeThrows() {
        new HttpClientPool(DefaultHttpClientConfiguration.INSTANCE, null, -1);
    }

    // ======================== Basic Acquire/Release Tests ========================

    @Test
    public void testAcquireAndRelease() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (HttpClientPool pool = new HttpClientPool(DefaultHttpClientConfiguration.INSTANCE, null, 2)) {
                Assert.assertEquals(2, pool.getAvailableCount());

                HttpClient client1 = pool.acquire();
                Assert.assertNotNull(client1);
                Assert.assertEquals(1, pool.getAvailableCount());

                HttpClient client2 = pool.acquire();
                Assert.assertNotNull(client2);
                Assert.assertEquals(0, pool.getAvailableCount());

                // Release in reverse order
                pool.release(client2);
                Assert.assertEquals(1, pool.getAvailableCount());

                pool.release(client1);
                Assert.assertEquals(2, pool.getAvailableCount());
            }
        });
    }

    @Test
    public void testAcquiredClientsAreDistinct() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (HttpClientPool pool = new HttpClientPool(DefaultHttpClientConfiguration.INSTANCE, null, 3)) {
                HttpClient client1 = pool.acquire();
                HttpClient client2 = pool.acquire();
                HttpClient client3 = pool.acquire();

                Assert.assertNotSame(client1, client2);
                Assert.assertNotSame(client2, client3);
                Assert.assertNotSame(client1, client3);

                pool.release(client1);
                pool.release(client2);
                pool.release(client3);
            }
        });
    }

    @Test
    public void testTryAcquireWhenAvailable() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (HttpClientPool pool = new HttpClientPool(DefaultHttpClientConfiguration.INSTANCE, null, 2)) {
                HttpClient client = pool.tryAcquire();
                Assert.assertNotNull(client);
                Assert.assertEquals(1, pool.getAvailableCount());
                pool.release(client);
            }
        });
    }

    @Test
    public void testTryAcquireWhenExhausted() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (HttpClientPool pool = new HttpClientPool(DefaultHttpClientConfiguration.INSTANCE, null, 1)) {
                HttpClient client = pool.acquire();
                Assert.assertNotNull(client);

                // Pool is exhausted, tryAcquire should return null
                HttpClient client2 = pool.tryAcquire();
                Assert.assertNull(client2);

                pool.release(client);
            }
        });
    }

    @Test
    public void testAcquireWithTimeoutSuccess() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (HttpClientPool pool = new HttpClientPool(DefaultHttpClientConfiguration.INSTANCE, null, 1)) {
                HttpClient client = pool.acquire(1, TimeUnit.SECONDS);
                Assert.assertNotNull(client);
                pool.release(client);
            }
        });
    }

    @Test
    public void testAcquireWithTimeoutExhausted() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (HttpClientPool pool = new HttpClientPool(DefaultHttpClientConfiguration.INSTANCE, null, 1)) {
                HttpClient client = pool.acquire();

                // Pool exhausted, timeout should elapse
                long start = System.nanoTime();
                HttpClient client2 = pool.acquire(100, TimeUnit.MILLISECONDS);
                long elapsed = System.nanoTime() - start;

                Assert.assertNull(client2);
                // Should have waited at least 100ms (with some tolerance)
                Assert.assertTrue("Should have waited at least 80ms, waited: " + elapsed / 1_000_000 + "ms",
                        elapsed >= 80_000_000);

                pool.release(client);
            }
        });
    }

    // ======================== Blocking Behavior Tests ========================

    @Test
    public void testAcquireBlocksWhenExhausted() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (HttpClientPool pool = new HttpClientPool(DefaultHttpClientConfiguration.INSTANCE, null, 1)) {
                HttpClient client = pool.acquire();
                AtomicBoolean acquired = new AtomicBoolean(false);
                AtomicReference<HttpClient> acquiredClient = new AtomicReference<>();

                // Start a thread that will block on acquire
                Thread blocker = new Thread(() -> {
                    try {
                        HttpClient c = pool.acquire();
                        acquiredClient.set(c);
                        acquired.set(true);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                blocker.start();

                // Give the thread time to start and block
                Thread.sleep(100);
                Assert.assertFalse("Thread should be blocked", acquired.get());

                // Release the client
                pool.release(client);

                // Wait for the blocked thread to acquire
                blocker.join(1000);
                Assert.assertTrue("Thread should have acquired", acquired.get());
                Assert.assertNotNull(acquiredClient.get());

                pool.release(acquiredClient.get());
            }
        });
    }

    @Test
    public void testMultipleThreadsBlockAndAcquire() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int poolSize = 2;
            final int numThreads = 5;

            try (HttpClientPool pool = new HttpClientPool(DefaultHttpClientConfiguration.INSTANCE, null, poolSize)) {
                CountDownLatch startLatch = new CountDownLatch(1);
                CountDownLatch doneLatch = new CountDownLatch(numThreads);
                AtomicInteger successCount = new AtomicInteger(0);

                for (int i = 0; i < numThreads; i++) {
                    new Thread(() -> {
                        try {
                            startLatch.await();
                            HttpClient client = pool.acquire();
                            // Simulate some work
                            Thread.sleep(50);
                            pool.release(client);
                            successCount.incrementAndGet();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } finally {
                            doneLatch.countDown();
                        }
                    }).start();
                }

                startLatch.countDown();
                boolean completed = doneLatch.await(10, TimeUnit.SECONDS);

                Assert.assertTrue("All threads should complete", completed);
                Assert.assertEquals(numThreads, successCount.get());
            }
        });
    }

    // ======================== Close Behavior Tests ========================

    @Test
    public void testCloseReleasesResources() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            HttpClientPool pool = new HttpClientPool(DefaultHttpClientConfiguration.INSTANCE, null, 3);
            Assert.assertFalse(pool.isClosed());

            pool.close();

            Assert.assertTrue(pool.isClosed());
            Assert.assertEquals(0, pool.getAvailableCount());
        });
    }

    @Test
    public void testDoubleCloseIsSafe() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            HttpClientPool pool = new HttpClientPool(DefaultHttpClientConfiguration.INSTANCE, null, 2);
            pool.close();
            pool.close(); // Should not throw
            Assert.assertTrue(pool.isClosed());
        });
    }

    @Test(expected = IllegalStateException.class)
    public void testAcquireAfterCloseThrows() throws Exception {
        HttpClientPool pool = new HttpClientPool(DefaultHttpClientConfiguration.INSTANCE, null, 1);
        pool.close();
        pool.acquire();
    }

    @Test(expected = IllegalStateException.class)
    public void testTryAcquireAfterCloseThrows() throws Exception {
        HttpClientPool pool = new HttpClientPool(DefaultHttpClientConfiguration.INSTANCE, null, 1);
        pool.close();
        pool.tryAcquire();
    }

    @Test(expected = IllegalStateException.class)
    public void testAcquireWithTimeoutAfterCloseThrows() throws Exception {
        HttpClientPool pool = new HttpClientPool(DefaultHttpClientConfiguration.INSTANCE, null, 1);
        pool.close();
        pool.acquire(1, TimeUnit.SECONDS);
    }

    @Test
    public void testReleaseAfterCloseClosesClient() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            HttpClientPool pool = new HttpClientPool(DefaultHttpClientConfiguration.INSTANCE, null, 1);
            HttpClient client = pool.acquire();

            pool.close();

            // Releasing after close should close the client (not throw)
            pool.release(client);
            Assert.assertEquals(0, pool.getAvailableCount());
        });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReleaseNullThrows() throws Exception {
        try (HttpClientPool pool = new HttpClientPool(DefaultHttpClientConfiguration.INSTANCE, null, 1)) {
            pool.release(null);
        }
    }

    // ======================== Concurrent Access Tests ========================

    @Test
    public void testConcurrentAcquireRelease() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int poolSize = 4;
            final int numThreads = 10;
            final int operationsPerThread = 100;

            try (HttpClientPool pool = new HttpClientPool(DefaultHttpClientConfiguration.INSTANCE, null, poolSize)) {
                ExecutorService executor = Executors.newFixedThreadPool(numThreads);
                AtomicInteger errorCount = new AtomicInteger(0);
                CyclicBarrier barrier = new CyclicBarrier(numThreads);

                List<Future<?>> futures = new ArrayList<>();
                for (int i = 0; i < numThreads; i++) {
                    futures.add(executor.submit(() -> {
                        try {
                            barrier.await();
                            for (int j = 0; j < operationsPerThread; j++) {
                                HttpClient client = pool.acquire();
                                if (client == null) {
                                    errorCount.incrementAndGet();
                                    continue;
                                }
                                // Simulate some work
                                Thread.sleep(1);
                                pool.release(client);
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
                Assert.assertEquals(poolSize, pool.getAvailableCount());
            }
        });
    }

    @Test
    public void testConcurrentAcquireWithClose() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int poolSize = 2;
            final int numAcquireThreads = 5;

            HttpClientPool pool = new HttpClientPool(DefaultHttpClientConfiguration.INSTANCE, null, poolSize);
            CountDownLatch startLatch = new CountDownLatch(1);
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger errorCount = new AtomicInteger(0);
            List<Thread> threads = new ArrayList<>();

            for (int i = 0; i < numAcquireThreads; i++) {
                Thread t = new Thread(() -> {
                    try {
                        startLatch.await();
                        HttpClient client = pool.acquire(500, TimeUnit.MILLISECONDS);
                        if (client != null) {
                            successCount.incrementAndGet();
                            Thread.sleep(50);
                            pool.release(client);
                        }
                    } catch (IllegalStateException e) {
                        // Expected when pool is closed during acquire
                        errorCount.incrementAndGet();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                t.start();
                threads.add(t);
            }

            // Start all threads
            startLatch.countDown();

            // Give some time for threads to start acquiring
            Thread.sleep(100);

            // Close the pool while threads are competing
            pool.close();

            // Wait for all threads to complete
            for (Thread t : threads) {
                t.join(5000);
            }

            // Some threads might have succeeded, some might have failed
            // We just verify that all threads completed without hanging
            Assert.assertTrue("Threads should have completed", threads.stream().noneMatch(Thread::isAlive));
        });
    }

    // ======================== Reuse Tests ========================

    @Test
    public void testReleasedClientCanBeReacquired() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (HttpClientPool pool = new HttpClientPool(DefaultHttpClientConfiguration.INSTANCE, null, 1)) {
                HttpClient client1 = pool.acquire();
                pool.release(client1);

                HttpClient client2 = pool.acquire();
                // With pool size 1, should get the same client back
                Assert.assertSame(client1, client2);

                pool.release(client2);
            }
        });
    }

    @Test
    public void testPoolMaintainsAllClients() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int poolSize = 3;
            try (HttpClientPool pool = new HttpClientPool(DefaultHttpClientConfiguration.INSTANCE, null, poolSize)) {
                List<HttpClient> clients = new ArrayList<>();

                // Acquire all
                for (int i = 0; i < poolSize; i++) {
                    clients.add(pool.acquire());
                }

                Assert.assertEquals(0, pool.getAvailableCount());

                // Release all
                for (HttpClient client : clients) {
                    pool.release(client);
                }

                Assert.assertEquals(poolSize, pool.getAvailableCount());

                // Reacquire all - should get the same clients
                List<HttpClient> reacquired = new ArrayList<>();
                for (int i = 0; i < poolSize; i++) {
                    reacquired.add(pool.acquire());
                }

                // All reacquired clients should be from the original set
                List<HttpClient> sortedOriginal = new ArrayList<>(clients);
                List<HttpClient> sortedReacquired = new ArrayList<>(reacquired);
                Collections.sort(sortedOriginal, (a, b) -> Integer.compare(System.identityHashCode(a), System.identityHashCode(b)));
                Collections.sort(sortedReacquired, (a, b) -> Integer.compare(System.identityHashCode(a), System.identityHashCode(b)));
                Assert.assertEquals(sortedOriginal, sortedReacquired);

                // Release all for cleanup
                for (HttpClient client : reacquired) {
                    pool.release(client);
                }
            }
        });
    }

    // ======================== Interrupt Handling Tests ========================

    @Test
    public void testAcquireInterruptedWhileWaiting() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (HttpClientPool pool = new HttpClientPool(DefaultHttpClientConfiguration.INSTANCE, null, 1)) {
                HttpClient client = pool.acquire();
                AtomicBoolean interrupted = new AtomicBoolean(false);

                Thread t = new Thread(() -> {
                    try {
                        pool.acquire(); // Will block
                    } catch (InterruptedException e) {
                        interrupted.set(true);
                        Thread.currentThread().interrupt();
                    }
                });
                t.start();

                // Wait for thread to start blocking
                Thread.sleep(100);

                // Interrupt the thread
                t.interrupt();
                t.join(1000);

                Assert.assertTrue("Thread should have been interrupted", interrupted.get());
                pool.release(client);
            }
        });
    }

    // ======================== Edge Case Tests ========================

    @Test
    public void testHighContention() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Small pool, many threads
            final int poolSize = 2;
            final int numThreads = 20;
            final int iterationsPerThread = 50;

            try (HttpClientPool pool = new HttpClientPool(DefaultHttpClientConfiguration.INSTANCE, null, poolSize)) {
                ExecutorService executor = Executors.newFixedThreadPool(numThreads);
                AtomicInteger totalAcquires = new AtomicInteger(0);
                CyclicBarrier barrier = new CyclicBarrier(numThreads);

                List<Future<?>> futures = new ArrayList<>();
                for (int i = 0; i < numThreads; i++) {
                    futures.add(executor.submit(() -> {
                        try {
                            barrier.await();
                            for (int j = 0; j < iterationsPerThread; j++) {
                                HttpClient client = pool.acquire();
                                totalAcquires.incrementAndGet();
                                // Very short work to maximize contention
                                pool.release(client);
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
                Assert.assertEquals(numThreads * iterationsPerThread, totalAcquires.get());
                Assert.assertEquals(poolSize, pool.getAvailableCount());
            }
        });
    }
}
