/*+*****************************************************************************
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

package io.questdb.test.cairo.pool;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.DefaultLifecycleManager;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.pool.WriterPool;
import io.questdb.cairo.pool.ex.EntryLockedException;
import io.questdb.cairo.pool.ex.PoolClosedException;
import io.questdb.cairo.sql.AsyncWriterCommand;
import io.questdb.cairo.wal.MetadataService;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.tasks.TableWriterTask;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.cairo.TestFilesFacade;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;


public class WriterPoolTest extends AbstractCairoTest {
    private TableToken zTableToken;

    @Before
    public void setUpInstance() {
        TableModel model = new TableModel(configuration, "z", PartitionBy.NONE).col("ts", ColumnType.DATE);
        AbstractCairoTest.create(model);
        zTableToken = engine.verifyTableName("z");
    }

    @Test
    public void testAllocateAndClear() throws Exception {
        assertWithPool(pool -> {
            int n = 2;
            final CyclicBarrier barrier = new CyclicBarrier(n);
            final CountDownLatch halt = new CountDownLatch(n);
            final AtomicInteger errors1 = new AtomicInteger();
            final AtomicInteger errors2 = new AtomicInteger();
            final AtomicInteger writerCount = new AtomicInteger();
            new Thread(() -> {
                try {
                    for (int i = 0; i < 1000; i++) {
                        try (TableWriter ignored = pool.get(zTableToken, "testing")) {
                            writerCount.incrementAndGet();
                        } catch (EntryUnavailableException ignored) {
                        }

                        if (i == 1) {
                            barrier.await();
                        } else {
                            Os.pause();
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace(System.out);
                    errors1.incrementAndGet();
                } finally {
                    Path.clearThreadLocals();
                    halt.countDown();
                }
            }).start();

            new Thread(() -> {
                try {
                    barrier.await();

                    for (int i = 0; i < 1000; i++) {
                        pool.releaseInactive();
                        Os.pause();
                    }

                } catch (Exception e) {
                    e.printStackTrace(System.out);
                    errors2.incrementAndGet();
                } finally {
                    Path.clearThreadLocals();
                    halt.countDown();
                }
            }).start();

            halt.await();

            Assert.assertTrue(writerCount.get() > 0);
            Assert.assertEquals(0, errors1.get());
            Assert.assertEquals(0, errors2.get());
        });
    }

    @Test
    public void testAsyncCommandPublishConcurrentWithWriterClose() throws Exception {
        // Stress the async-command publish path against writers that constantly go
        // distressed and get closed. Before the fix a publisher could serialize into a
        // TableWriterTask buffer that closeWriter() freed underneath it, which surfaced
        // either as a JVM SIGSEGV or as an "async command/event queue buffer overflow"
        // CairoException (a write into a closed/zeroed task). After the fix the close
        // drains in-flight publishers first, so neither happens.
        final int publisherCount = 3;
        final int iterations = 3_000;
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final AtomicBoolean running = new AtomicBoolean(true);

        assertWithPool(pool -> {
            final int tableId = zTableToken.getTableId();
            final CyclicBarrier barrier = new CyclicBarrier(publisherCount + 1);
            final ObjList<Thread> publishers = new ObjList<>();

            for (int p = 0; p < publisherCount; p++) {
                Thread publisher = new Thread(() -> {
                    try {
                        barrier.await();
                        while (running.get() && error.get() == null) {
                            try {
                                TableWriter w = pool.getWriterOrPublishCommand(
                                        zTableToken, "publisher", new TestPublishCommand(zTableToken, tableId));
                                if (w != null) {
                                    // Writer was free, not busy - nothing was published.
                                    w.close();
                                }
                            } catch (EntryUnavailableException | PoolClosedException ignore) {
                                // expected: writer busy / pool closing
                            } catch (CairoException e) {
                                // "queue is full" is a legitimate outcome under stress (the
                                // distressed closes never tick the queue). A buffer overflow
                                // is the bug manifesting and must fail the test.
                                if (!Chars.contains(e.getFlyweightMessage(), "queue is full")) {
                                    error.compareAndSet(null, e);
                                }
                            }
                        }
                    } catch (Throwable t) {
                        error.compareAndSet(null, t);
                    } finally {
                        Path.clearThreadLocals();
                    }
                });
                publishers.add(publisher);
                publisher.start();
            }

            try {
                barrier.await();
                // Owner/closer: repeatedly grab the writer (making it busy so publishers
                // take the publish path), force it distressed, and release it so it takes
                // the distressed close path that frees the command queue.
                for (int i = 0; i < iterations && error.get() == null; i++) {
                    try (TableWriter w = pool.get(zTableToken, "owner")) {
                        w.markDistressed();
                    } catch (EntryUnavailableException ignore) {
                        i--;
                    }
                }
            } finally {
                running.set(false);
                for (int i = 0, n = publishers.size(); i < n; i++) {
                    publishers.get(i).join();
                }
            }
        });

        if (error.get() != null) {
            throw new AssertionError("async publish raced a writer close unsafely", error.get());
        }
    }

    @Test
    public void testAsyncCommandPublishDrainsBeforeDistressedClose() throws Exception {
        // Deterministic proof of the fix: closeWriter() must not free a writer's command
        // queue while a thread is serializing an async command into it. A publisher is
        // parked mid-serialize (holding the in-flight publisher count); a concurrent
        // distressed close must block in drainCommandPublishers() until the publisher
        // finishes, rather than free the buffer underneath it.
        assertWithPool(pool -> {
            final SOCountDownLatch publisherInSerialize = new SOCountDownLatch(1);
            final SOCountDownLatch releaseSerialize = new SOCountDownLatch(1);
            final AtomicBoolean closeReturned = new AtomicBoolean();
            final AtomicReference<Throwable> error = new AtomicReference<>();

            // Hold the writer busy on this thread so the publisher is forced down the
            // getWriterOrPublishCommand -> addCommandToWriterQueue -> serialize path.
            final TableWriter ownerWriter = pool.get(zTableToken, "owner");
            final int tableId = zTableToken.getTableId();
            final TestPublishCommand command =
                    new TestPublishCommand(zTableToken, tableId, publisherInSerialize, releaseSerialize);

            Thread publisher = new Thread(() -> {
                try {
                    TableWriter w = pool.getWriterOrPublishCommand(zTableToken, "publisher", command);
                    Assert.assertNull(w); // command was published, no writer handed back
                } catch (Throwable t) {
                    error.compareAndSet(null, t);
                } finally {
                    Path.clearThreadLocals();
                }
            });

            Thread closer = new Thread(() -> {
                try {
                    ownerWriter.markDistressed();
                    ownerWriter.close(); // distressed close -> closeWriter -> drainCommandPublishers
                } catch (Throwable t) {
                    error.compareAndSet(null, t);
                } finally {
                    closeReturned.set(true);
                    Path.clearThreadLocals();
                }
            });

            boolean closerStarted = false;
            try {
                publisher.start();
                // Wait until the publisher is inside serialize, holding the in-flight count.
                publisherInSerialize.await();

                closer.start();
                closerStarted = true;

                // The close must stay blocked while the publisher is parked in serialize.
                // Poll briefly; with the fix closeReturned never flips here, without it the
                // close races ahead and frees the queue almost immediately.
                for (int i = 0; i < 250 && !closeReturned.get(); i++) {
                    Os.sleep(1);
                }
                Assert.assertFalse(
                        "closeWriter() freed the command queue while a publisher was mid-serialize",
                        closeReturned.get());
            } finally {
                releaseSerialize.countDown();
                publisher.join();
                if (closerStarted) {
                    closer.join();
                }
                if (ownerWriter.isOpen()) {
                    ownerWriter.close();
                }
            }

            Assert.assertTrue("close did not complete after publisher finished", closeReturned.get());
            Assert.assertTrue("publisher did not finish serializing into a live buffer", command.serializeComplete);
            if (error.get() != null) {
                throw new AssertionError("publisher or closer failed", error.get());
            }
        });
    }

    @Test
    public void testBasicCharSequence() throws Exception {
        TableModel model = new TableModel(configuration, "x", PartitionBy.NONE).col("ts", ColumnType.DATE);
        AbstractCairoTest.create(model);

        assertWithPool(pool -> {
            sink.clear();
            sink.put("x");
            TableToken xTableToken = engine.verifyTableName(sink);

            TableWriter writer1 = pool.get(xTableToken, "testing");
            Assert.assertNotNull(writer1);
            writer1.close();

            // mutate sink
            sink.clear();
            sink.put("y");

            try (TableWriter writer2 = pool.get(xTableToken, "testing")) {
                Assert.assertSame(writer1, writer2);
            }
        });
    }

    @Test
    public void testCannotLockWriter() throws Exception {
        final TestFilesFacade ff = new TestFilesFacade() {
            int count = 1;

            @Override
            public long openRWNoCache(LPSZ name, int opts) {
                if (Utf8s.endsWithAscii(name, zTableToken.getDirName() + ".lock") && count-- > 0) {
                    return -1;
                }
                return super.openRWNoCache(name, opts);
            }

            @Override
            public boolean wasCalled() {
                return count <= 0;
            }
        };

        DefaultCairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
            @Override
            public @NotNull FilesFacade getFilesFacade() {
                return ff;
            }
        };

        assertWithPool(pool -> {
            // fail first time
            Assert.assertEquals(WriterPool.OWNERSHIP_REASON_MISSING, pool.lock(zTableToken, "testing"));

            Assert.assertTrue(ff.wasCalled());

            TableWriter writer = pool.get(zTableToken, "testing");
            Assert.assertNotNull(writer);
            writer.close();

            Assert.assertEquals(WriterPool.OWNERSHIP_REASON_NONE, pool.lock(zTableToken, "testing"));

            // check that we can't get writer from pool
            try {
                pool.get(zTableToken, "testing");
                Assert.fail();
            } catch (CairoException ignore) {
            }

            // check that we can't create standalone writer either
            try {
                newOffPoolWriter(configuration, "z").close();
                Assert.fail();
            } catch (CairoException ignored) {
            }

            pool.unlock(zTableToken);

            // check if we can create standalone writer after pool unlocked it
            writer = newOffPoolWriter(configuration, "z");
            Assert.assertNotNull(writer);
            writer.close();

            // check if we can create writer via pool
            writer = pool.get(zTableToken, "testing");
            Assert.assertNotNull(writer);
            writer.close();
        }, configuration);

        Assert.assertTrue(ff.wasCalled());
    }

    @Test
    public void testClosedPoolLock() throws Exception {
        assertWithPool(pool -> {
            class X implements PoolListener {
                short ev = -1;

                @Override
                public void onEvent(byte factoryType, long thread, TableToken tableToken, short event, short segment, short position) {
                    this.ev = event;
                }
            }
            X x = new X();
            pool.setPoolListener(x);
            pool.close();
            try {
                pool.lock(zTableToken, "testing");
                Assert.fail();
            } catch (PoolClosedException ignored) {
            }
            Assert.assertEquals(PoolListener.EV_POOL_CLOSED, x.ev);
        });
    }

    @Test
    public void testDistressedWriterRaceCondition() throws Exception {
        ObjList<Thread> threads = new ObjList<>();
        int iterations = 100;
        int threadCount = 5;
        AtomicReference<Throwable> error = new AtomicReference<>();

        assertWithPool(pool -> {

            // Run iterations with concurrent access to trigger failures and distressed writers
            final CyclicBarrier barrier = new CyclicBarrier(threadCount);
            final CountDownLatch latch = new CountDownLatch(threadCount);

            for (int i = 0; i < threadCount; i++) {
                // Thread 1: Perform operations that may trigger distressed writers due to simulated failures
                Thread worker1 = new Thread(() -> {
                    try {
                        barrier.await();

                        for (int j = 0; j < iterations; j++) {
                            try (TableWriter w = pool.get(zTableToken, "worker")) {
                                // Perform operations that trigger file I/O and can fail
                                w.markDistressed();
                            } catch (EntryUnavailableException e) {
                                // Expected when writer becomes distressed or I/O fails
                                LOG.debug().$("Worker1 caught exception: ").$(e.getMessage()).$();
                                j--;
                            }
                        }
                    } catch (Throwable e) {
                        // Handle exceptions
                        error.set(e);
                        LOG.error().$(e).$();
                    } finally {
                        latch.countDown();
                        Path.clearThreadLocals();
                    }
                });
                threads.add(worker1);
                worker1.start();
            }

            for (int i = 0; i < threads.size(); i++) {
                threads.get(i).join();
            }
        });

        if (error.get() != null) {
            Assert.fail("Test failed with exception: " + error.get().getMessage());
        }
    }

    @Test
    public void testFactoryCloseBeforeRelease() throws Exception {
        assertWithPool(pool -> {
            TableWriter x;

            x = pool.get(zTableToken, "testing");
            try {
                Assert.assertEquals(0, pool.countFreeWriters());
                Assert.assertNotNull(x);
                Assert.assertTrue(x.isOpen());
                pool.close();
            } finally {
                x.close();
            }

            Assert.assertFalse(x.isOpen());
            try {
                pool.get(zTableToken, "testing");
                Assert.fail();
            } catch (PoolClosedException ignored) {
            }
        });
    }

    @Test
    public void testGetAndCloseRace() throws Exception {
        TableModel model = new TableModel(configuration, "xyz", PartitionBy.NONE).col("ts", ColumnType.DATE);
        AbstractCairoTest.create(model);

        TableToken xyzTableToken = engine.verifyTableName("xyz");
        for (int i = 0; i < 100; i++) {
            assertWithPool(pool -> {
                AtomicInteger exceptionCount = new AtomicInteger();
                CyclicBarrier barrier = new CyclicBarrier(2);
                CountDownLatch stopLatch = new CountDownLatch(2);

                // make sure writer exists in pool
                try (TableWriter writer = pool.get(xyzTableToken, "testing")) {
                    Assert.assertNotNull(writer);
                }

                new Thread(() -> {
                    try {
                        barrier.await();
                        pool.close();
                    } catch (Exception e) {
                        exceptionCount.incrementAndGet();
                        e.printStackTrace(System.out);
                    } finally {
                        stopLatch.countDown();
                    }
                }).start();

                new Thread(() -> {
                    try {
                        barrier.await();
                        try (TableWriter writer = pool.get(xyzTableToken, "testing")) {
                            Assert.assertNotNull(writer);
                        } catch (PoolClosedException | EntryUnavailableException ignore) {
                            // this can also happen when this thread is delayed enough for pool close to complete
                        }
                    } catch (Exception e) {
                        exceptionCount.incrementAndGet();
                        e.printStackTrace(System.out);
                    } finally {
                        stopLatch.countDown();
                    }
                }).start();

                Assert.assertTrue(stopLatch.await(2, TimeUnit.SECONDS));
                Assert.assertEquals(0, exceptionCount.get());
            });
        }
    }

    @Test
    public void testGetAndReleaseRace() throws Exception {
        TableModel model = new TableModel(configuration, "xyz", PartitionBy.NONE).col("ts", ColumnType.DATE);
        AbstractCairoTest.create(model);

        TableToken xyzTableName = engine.verifyTableName("xyz");
        for (int i = 0; i < 100; i++) {
            assertWithPool(pool -> {
                AtomicInteger exceptionCount = new AtomicInteger();
                CyclicBarrier barrier = new CyclicBarrier(2);
                CountDownLatch stopLatch = new CountDownLatch(2);

                // make sure writer exists in pool
                try (TableWriter writer = pool.get(xyzTableName, "testing")) {
                    Assert.assertNotNull(writer);
                }

                new Thread(() -> {
                    try {
                        barrier.await();
                        pool.releaseInactive();
                    } catch (Exception e) {
                        exceptionCount.incrementAndGet();
                        e.printStackTrace(System.out);
                    } finally {
                        Path.clearThreadLocals();
                        stopLatch.countDown();
                    }
                }).start();

                new Thread(() -> {
                    try {
                        barrier.await();
                        try (TableWriter writer = pool.get(xyzTableName, "testing")) {
                            Assert.assertNotNull(writer);
                        } catch (PoolClosedException ex) {
                            // this can also happen when this thread is delayed enough for pool close to complete
                        }
                    } catch (Exception e) {
                        exceptionCount.incrementAndGet();
                        e.printStackTrace(System.out);
                    } finally {
                        Path.clearThreadLocals();
                        stopLatch.countDown();
                    }
                }).start();

                Assert.assertTrue(stopLatch.await(2, TimeUnit.SECONDS));
                Assert.assertEquals(0, exceptionCount.get());
            });
        }
    }

    @Test
    public void testLockNonExisting() throws Exception {
        assertWithPool(pool -> {
            Assert.assertEquals(WriterPool.OWNERSHIP_REASON_NONE, pool.lock(zTableToken, "testing"));

            try {
                pool.get(zTableToken, "testing");
                Assert.fail();
            } catch (EntryLockedException ignored) {
            }

            pool.unlock(zTableToken);

            try (TableWriter wx = pool.get(zTableToken, "testing")) {
                Assert.assertNotNull(wx);
            }
        });
    }

    @Test
    public void testLockUnlock() throws Exception {
        TableModel model = new TableModel(configuration, "x", PartitionBy.NONE).col("ts", ColumnType.DATE);
        AbstractCairoTest.create(model);

        model = new TableModel(configuration, "y", PartitionBy.NONE).col("ts", ColumnType.DATE);
        AbstractCairoTest.create(model);

        TableToken xTableToken = engine.verifyTableName("x");
        TableToken yTableToken = engine.verifyTableName("y");

        assertWithPool(pool -> {
            try (TableWriter wy = pool.get(yTableToken, "testing")) {
                Assert.assertNotNull(wy);
                Assert.assertTrue(wy.isOpen());

                // check that lock is successful
                Assert.assertEquals(WriterPool.OWNERSHIP_REASON_NONE, pool.lock(xTableToken, "testing"));

                // check that writer x is closed and writer y is open (lock must not spill out to other writers)
                Assert.assertTrue(wy.isOpen());

                // check that when name is locked writers are not created
                try {
                    pool.get(xTableToken, "testing");
                    Assert.fail();
                } catch (EntryLockedException ignored) {
                }

                final CountDownLatch done = new CountDownLatch(1);
                final AtomicBoolean result = new AtomicBoolean();

                // have new thread try to allocate this writer
                new Thread(() -> {
                    try (TableWriter ignored = pool.get(xTableToken, "testing")) {
                        result.set(false);
                    } catch (EntryUnavailableException ignored) {
                        result.set(true);
                    } catch (CairoException e) {
                        e.printStackTrace(System.out);
                        result.set(false);
                    }
                    done.countDown();
                }).start();

                Assert.assertTrue(done.await(1, TimeUnit.SECONDS));
                Assert.assertTrue(result.get());

                pool.unlock(xTableToken);

                try (TableWriter wx = pool.get(xTableToken, "testing")) {
                    Assert.assertNotNull(wx);
                    Assert.assertTrue(wx.isOpen());

                    try {
                        // unlocking writer that has not been locked must produce exception
                        // and not affect open writer
                        pool.unlock(xTableToken);
                        Assert.fail();
                    } catch (CairoException ignored) {
                    }

                    Assert.assertTrue(wx.isOpen());
                }

            }
        });
    }

    @Test
    public void testLockUnlockAndReleaseRace() throws Exception {
        TableModel model = new TableModel(configuration, "xyz", PartitionBy.NONE).col("ts", ColumnType.DATE);
        AbstractCairoTest.create(model);

        TableToken xyzTableToken = engine.verifyTableName("xyz");
        for (int i = 0; i < 100; i++) {
            assertWithPool(pool -> {
                AtomicInteger exceptionCount = new AtomicInteger();
                CyclicBarrier barrier = new CyclicBarrier(2);
                CountDownLatch stopLatch = new CountDownLatch(2);

                // make sure writer exists in pool
                try (TableWriter writer = pool.get(xyzTableToken, "testing")) {
                    Assert.assertNotNull(writer);
                }

                new Thread(() -> {
                    try {
                        barrier.await();
                        pool.releaseInactive();
                    } catch (Exception e) {
                        exceptionCount.incrementAndGet();
                        e.printStackTrace(System.out);
                    } finally {
                        stopLatch.countDown();
                    }
                }).start();

                new Thread(() -> {
                    try {
                        barrier.await();
                        //noinspection StringEquality
                        if (pool.lock(xyzTableToken, "testing") == WriterPool.OWNERSHIP_REASON_NONE) {
                            pool.unlock(xyzTableToken);
                        }
                    } catch (Exception e) {
                        exceptionCount.incrementAndGet();
                        e.printStackTrace(System.out);
                    } finally {
                        Path.clearThreadLocals();
                        stopLatch.countDown();
                    }
                }).start();

                Assert.assertTrue(stopLatch.await(2, TimeUnit.SECONDS));
                Assert.assertEquals(0, exceptionCount.get());
            });
        }
    }

    @Test
    public void testLockWorkflow() throws Exception {
        TableModel model = new TableModel(configuration, "x", PartitionBy.NONE).col("ts", ColumnType.DATE);
        AbstractCairoTest.create(model);

        assertWithPool(pool -> {
            TableToken xTableToken = engine.verifyTableName("x");
            Assert.assertEquals(WriterPool.OWNERSHIP_REASON_NONE, pool.lock(xTableToken, "testing"));
            pool.unlock(xTableToken);
            pool.get(xTableToken, "testing").close();
            Assert.assertEquals(WriterPool.OWNERSHIP_REASON_NONE, pool.lock(xTableToken, "testing"));
            pool.unlock(xTableToken);
        });
    }

    @Test
    public void testNThreadsRaceToLock() throws Exception {
        assertWithPool(pool -> {
            int N = 8;
            for (int k = 0; k < 1000; k++) {
                final CyclicBarrier barrier = new CyclicBarrier(N);
                final CountDownLatch halt = new CountDownLatch(N);
                final AtomicInteger errors = new AtomicInteger();
                final AtomicInteger writerCount = new AtomicInteger();

                for (int i = 0; i < N; i++) {
                    TableToken tableName = new TableToken("table_" + i, "table_" + i, null, i, false, false, false);
                    new Thread(() -> {
                        try {
                            barrier.await();
                            //noinspection StringEquality
                            if (pool.lock(tableName, "testing") == WriterPool.OWNERSHIP_REASON_NONE) {
                                Os.pause();
                                pool.unlock(tableName);
                            } else {
                                Os.pause();
                            }
                        } catch (Exception e) {
                            e.printStackTrace(System.out);
                            errors.incrementAndGet();
                        } finally {
                            Path.clearThreadLocals();
                            halt.countDown();
                        }
                    }).start();
                }

                halt.await();

                // this check is unreliable on slow build servers
                // it is very often the case that there are limited number of cores
                // available and threads execute sequentially rather than
                // simultaneously. We should check that none of the threads
                // receive error.
                Assert.assertEquals(0, writerCount.get());
                Assert.assertEquals(0, errors.get());
                Assert.assertEquals(0, pool.countFreeWriters());
            }
        });
    }

    @Test
    public void testNThreadsRaceToLockSameTable() throws Exception {
        assertWithPool(pool -> {
            int N = 8;
            final AtomicInteger errors = new AtomicInteger();
            Thread[] threads = new Thread[N];
            for (int i = 0; i < N; i++) {
                threads[i] = new Thread(() -> {
                    //noinspection EmptyTryBlock
                    try (TableWriter ignored1 = pool.get(zTableToken, "testing")) {
                    } catch (Throwable ignored) {
                        errors.incrementAndGet();
                    } finally {
                        Path.clearThreadLocals();
                    }
                });
                threads[i].start();
            }
            //noinspection ForLoopReplaceableByForEach
            for (int i = 0; i < threads.length; i++) {
                threads[i].join();
            }
        });
    }

    @Test
    public void testNewLock() throws Exception {
        assertWithPool(pool -> {
            Assert.assertEquals(WriterPool.OWNERSHIP_REASON_NONE, pool.lock(zTableToken, "testing"));
            try {
                pool.get(zTableToken, "testing");
                Assert.fail();
            } catch (EntryLockedException ignored) {
            }
            pool.unlock(zTableToken);
        });
    }

    @Test
    public void testOneThreadGetRelease() throws Exception {
        assertWithPool(pool -> {
            TableWriter x;
            TableWriter y;

            x = pool.get(zTableToken, "testing");
            try {
                Assert.assertEquals(0, pool.countFreeWriters());
                Assert.assertNotNull(x);
                Assert.assertTrue(x.isOpen());
            } finally {
                x.close();
            }

            Assert.assertEquals(1, pool.countFreeWriters());

            y = pool.get(zTableToken, "testing");
            try {
                Assert.assertNotNull(y);
                Assert.assertTrue(y.isOpen());
                Assert.assertSame(y, x);
            } finally {
                y.close();
            }

            Assert.assertEquals(1, pool.countFreeWriters());
        });
    }

    @Test
    public void testReplaceWriterAfterUnlock() throws Exception {
        assertWithPool(pool -> {
            String x = "x";
            TableModel model = new TableModel(configuration, x, PartitionBy.NONE).col("ts", ColumnType.DATE);
            AbstractCairoTest.create(model);

            TableToken tableToken = engine.verifyTableName(x);
            Assert.assertEquals(WriterPool.OWNERSHIP_REASON_NONE, pool.lock(tableToken, "testing"));

            TableWriter writer = new TableWriter(
                    engine.getConfiguration(),
                    tableToken,
                    engine.getMessageBus(),
                    null,
                    false,
                    DefaultLifecycleManager.INSTANCE,
                    engine.getConfiguration().getDbRoot(),
                    engine.getDdlListener(tableToken),
                    engine
            );
            for (int i = 0; i < 100; i++) {
                TableWriter.Row row = writer.newRow();
                row.putDate(0, i);
                row.append();
            }
            writer.commit();

            pool.unlock(tableToken, writer, false);

            // make sure our writer stays in pool and close() doesn't destroy it
            Assert.assertSame(writer, pool.get(tableToken, "testing"));
            writer.close();

            // this isn't a mistake, need to check that writer is still alive after close
            Assert.assertSame(writer, pool.get(tableToken, "testing"));
            writer.close();
        });
    }

    @Test
    public void testToStringOnWriter() throws Exception {
        assertWithPool(pool -> {
            try (TableWriter w = pool.get(zTableToken, "testing")) {
                Assert.assertEquals("TableWriter{name=z}", w.toString());
            }
        });
    }

    @Test
    public void testTwoThreadsRaceToAllocate() throws Exception {
        assertWithPool(pool -> {
            for (int k = 0; k < 1000; k++) {
                int n = 2;
                final CyclicBarrier barrier = new CyclicBarrier(n);
                final CountDownLatch halt = new CountDownLatch(n);
                final AtomicInteger errors = new AtomicInteger();
                final AtomicInteger writerCount = new AtomicInteger();

                for (int i = 0; i < n; i++) {
                    new Thread(() -> {
                        try {
                            barrier.await();
                            try (TableWriter w = pool.get(zTableToken, "testing")) {
                                writerCount.incrementAndGet();
                                populate(w);
                            } catch (EntryUnavailableException ignored) {
                            }
                        } catch (Exception e) {
                            e.printStackTrace(System.out);
                            errors.incrementAndGet();
                        } finally {
                            Path.clearThreadLocals();
                            halt.countDown();
                        }
                    }).start();
                }

                halt.await();

                // this check is unreliable on slow build servers
                // it is very often the case that there are limited number of cores
                // available and threads execute sequentially rather than
                // simultaneously. We should check that none of the threads
                // receive error.
                Assert.assertTrue(writerCount.get() > 0);
                Assert.assertEquals(0, errors.get());
                Assert.assertEquals(1, pool.countFreeWriters());
            }
        });
    }

    @Test
    public void testTwoThreadsRaceToAllocateAndLock() throws Exception {
        assertWithPool(pool -> {
            for (int k = 0; k < 1000; k++) {
                int n = 2;
                final CyclicBarrier barrier = new CyclicBarrier(n);
                final CountDownLatch halt = new CountDownLatch(n);
                final AtomicInteger errors = new AtomicInteger();
                final AtomicInteger writerCount = new AtomicInteger();

                for (int i = 0; i < n; i++) {
                    new Thread(() -> {
                        try {
                            barrier.await();
                            //------------- thread 1
                            try (TableWriter w = pool.get(zTableToken, "testing")) {
                                writerCount.incrementAndGet();
                                populate(w);

                                Assert.assertSame(w, pool.get(zTableToken, "testing"));

                            } catch (EntryUnavailableException ignored) {
                            }

                            // lock frees up writer, make sure on next iteration threads have something to compete for
                            //noinspection StringEquality
                            if (pool.lock(zTableToken, "testing") == WriterPool.OWNERSHIP_REASON_NONE) {
                                pool.unlock(zTableToken);
                            }
                        } catch (Exception e) {
                            e.printStackTrace(System.out);
                            errors.incrementAndGet();
                        } finally {
                            Path.clearThreadLocals();
                            halt.countDown();
                        }
                    }).start();
                }

                halt.await();

                // this check is unreliable on slow build servers
                // it is very often the case that there are limited number of cores
                // available and threads execute sequentially rather than
                // simultaneously. We should check that none of the threads
                // receive error.
                Assert.assertTrue(writerCount.get() > 0);
                Assert.assertEquals(0, errors.get());
                Assert.assertEquals(0, pool.countFreeWriters());
            }
        });
    }

    @Test
    public void testUnlockInAnotherThread() throws Exception {
        assertWithPool(pool -> {

            Assert.assertEquals(WriterPool.OWNERSHIP_REASON_NONE, pool.lock(zTableToken, "testing"));
            AtomicInteger errors = new AtomicInteger();

            CountDownLatch latch = new CountDownLatch(1);
            new Thread(() -> {
                try {
                    try {
                        pool.unlock(zTableToken);
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "Not lock owner");
                    }
                } catch (Throwable e) {
                    e.printStackTrace(System.out);
                    errors.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            }).start();

            Assert.assertTrue(latch.await(2, TimeUnit.SECONDS));
            Assert.assertEquals(0, errors.get());

            try {
                pool.get(zTableToken, "testing");
                Assert.fail();
            } catch (EntryLockedException ignore) {
            }
            pool.unlock(zTableToken);
        });
    }

    @Test
    public void testUnlockNonExisting() throws Exception {
        assertWithPool(pool -> {
            class X implements PoolListener {
                short ev = -1;

                @Override
                public void onEvent(byte factoryType, long thread, TableToken tableToken, short event, short segment, short position) {
                    this.ev = event;
                }
            }
            X x = new X();
            pool.setPoolListener(x);
            pool.unlock(zTableToken);
            Assert.assertEquals(PoolListener.EV_NOT_LOCKED, x.ev);
        });
    }

    @Test
    public void testUnlockWriterWhenPoolIsClosed() throws Exception {
        assertWithPool(pool -> {
            Assert.assertEquals(WriterPool.OWNERSHIP_REASON_NONE, pool.lock(zTableToken, "testing"));

            pool.close();

            TableWriter writer = newOffPoolWriter(configuration, "z");
            Assert.assertNotNull(writer);
            writer.close();
        });
    }

    @Test
    public void testWriterDoubleClose() throws Exception {
        assertWithPool(pool -> {
            class X implements PoolListener {
                short ev = -1;

                @Override
                public void onEvent(byte factoryType, long thread, TableToken tableToken, short event, short segment, short position) {
                    this.ev = event;
                }
            }
            X x = new X();
            pool.setPoolListener(x);
            TableWriter w = pool.get(zTableToken, "testing");
            Assert.assertNotNull(w);
            Assert.assertEquals(1, pool.getBusyCount());
            w.close();
            Assert.assertEquals(PoolListener.EV_RETURN, x.ev);
            Assert.assertEquals(0, pool.getBusyCount());

            w.close();
            Assert.assertEquals(PoolListener.EV_UNEXPECTED_CLOSE, x.ev);
            Assert.assertEquals(0, pool.getBusyCount());
        });
    }

    @Test
    public void testWriterOpenFailOnce() throws Exception {

        final TestFilesFacade ff = new TestFilesFacade() {
            int count = 1;

            @Override
            public long openRWNoCache(LPSZ name, int opts) {
                if (Utf8s.endsWithAscii(name, zTableToken.getDirName() + ".lock") && count-- > 0) {
                    return -1;
                }
                return super.openRWNoCache(name, opts);
            }

            @Override
            public boolean wasCalled() {
                return count <= 0;
            }
        };

        DefaultCairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
            @Override
            public @NotNull FilesFacade getFilesFacade() {
                return ff;
            }
        };

        assertWithPool(pool -> {
            try {
                pool.get(zTableToken, "testing");
                Assert.fail();
            } catch (CairoException ignore) {
            }

            Assert.assertEquals(1, pool.size());
            Assert.assertEquals(0, pool.getBusyCount());

            pool.releaseInactive();
            Assert.assertEquals(0, pool.size());

            // try again
            TableWriter w = pool.get(zTableToken, "testing");
            Assert.assertEquals(1, pool.getBusyCount());
            w.close();
        }, configuration);

        Assert.assertTrue(ff.wasCalled());
    }

    @Test
    public void testWriterPingPong() throws Exception {
        assertWithPool(pool -> {
            for (int i = 0; i < 10_000; i++) {
                final SOCountDownLatch next = new SOCountDownLatch(1);

                // listener will release the latch
                pool.setPoolListener((factoryType, thread, name, event, segment, position) -> {
                    if (event == PoolListener.EV_RETURN) {
                        next.countDown();
                    }
                });

                new Thread(() -> {
                    // trigger the release
                    pool.get(zTableToken, "test").close();
                    Path.clearThreadLocals();
                }).start();

                next.await();
                pool.get(zTableToken, "test2").close();
            }
        });
    }

    private void assertWithPool(PoolAwareCode code, CairoConfiguration configuration) throws Exception {
        assertMemoryLeak(() -> {
            try (WriterPool pool = new WriterPool(configuration, engine, engine.getRecentWriteTracker())) {
                code.run(pool);
            }
        });
    }

    private void assertWithPool(PoolAwareCode code) throws Exception {
        assertWithPool(code, configuration);
    }

    private void populate(TableWriter w) {
        long start = w.getMaxTimestamp();
        for (int i = 0; i < 1000; i++) {
            w.newRow(start + i).append();
            w.commit();
        }
    }

    private interface PoolAwareCode {
        void run(WriterPool pool) throws Exception;
    }

    // Minimal non-structural async writer command for the WriterPool publish-vs-close
    // tests. In "blocking" mode (latches supplied) it parks inside serialize() so a test
    // can deterministically race a writer close; otherwise it just widens the serialize
    // window. Only the methods the publish path touches do real work; the rest are stubs.
    private static class TestPublishCommand implements AsyncWriterCommand {
        static final long CORRELATION_ID = 7L;
        private final SOCountDownLatch inSerialize;
        private final SOCountDownLatch release;
        private final int tableId;
        private final TableToken tableToken;
        volatile boolean serializeComplete;

        TestPublishCommand(TableToken tableToken, int tableId) {
            this(tableToken, tableId, null, null);
        }

        TestPublishCommand(TableToken tableToken, int tableId, SOCountDownLatch inSerialize, SOCountDownLatch release) {
            this.tableToken = tableToken;
            this.tableId = tableId;
            this.inSerialize = inSerialize;
            this.release = release;
        }

        @Override
        public long apply(MetadataService svc, boolean contextAllowsAnyStructureChanges) {
            return 0;
        }

        @Override
        public void close() {
        }

        @Override
        public AsyncWriterCommand deserialize(TableWriterTask task) {
            return this;
        }

        @Override
        public int getCmdType() {
            return TableWriterTask.CMD_ALTER_TABLE;
        }

        @Override
        public String getCommandName() {
            return "TEST_PUBLISH";
        }

        @Override
        public long getCorrelationId() {
            return CORRELATION_ID;
        }

        @Override
        public int getTableId() {
            return tableId;
        }

        @Override
        public int getTableNamePosition() {
            return 0;
        }

        @Override
        public TableToken getTableToken() {
            return tableToken;
        }

        @Override
        public long getTableVersion() {
            return 0;
        }

        @Override
        public boolean isStructural() {
            return false;
        }

        @Override
        public void serialize(TableWriterTask task) {
            task.of(getCmdType(), tableId, tableToken);
            task.setInstance(CORRELATION_ID);
            if (inSerialize != null) {
                // Blocking mode: announce we are mid-serialize (addCommandToWriterQueue is
                // holding the in-flight publisher count) then block so the test can race a
                // writer close against this in-progress serialize.
                inSerialize.countDown();
                release.await();
            } else {
                // Widen the serialize window so a concurrent close is likely to land while
                // we are writing into the task buffer.
                for (int i = 0; i < 64; i++) {
                    Os.pause();
                }
            }
            // Post-fix these land in a live buffer. Pre-fix a racing close has freed and
            // zeroed the task, so this overflows the (now zero-length) buffer or writes to
            // address 0.
            for (int i = 0; i < 32; i++) {
                task.putLong(i);
            }
            serializeComplete = true;
        }

        @Override
        public void setCommandCorrelationId(long correlationId) {
        }

        @Override
        public void startAsync() {
        }
    }
}
