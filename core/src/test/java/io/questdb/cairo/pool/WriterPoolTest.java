/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cairo.pool;

import io.questdb.MessageBusImpl;
import io.questdb.cairo.*;
import io.questdb.cairo.pool.ex.EntryLockedException;
import io.questdb.cairo.pool.ex.PoolClosedException;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class WriterPoolTest extends AbstractCairoTest {

    private static final DefaultCairoConfiguration CONFIGURATION;

    @Before
    public void setUpInstance() {
        try (TableModel model = new TableModel(configuration, "z", PartitionBy.NONE).col("ts", ColumnType.DATE)) {
            CairoTestUtils.create(model);
        }
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
                        try (TableWriter ignored = pool.get("z", "testing")) {
                            writerCount.incrementAndGet();
                        } catch (EntryUnavailableException ignored) {
                        }

                        if (i == 1) {
                            barrier.await();
                        } else {
                            LockSupport.parkNanos(1L);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    errors1.incrementAndGet();
                } finally {
                    halt.countDown();
                }
            }).start();

            new Thread(() -> {
                try {
                    barrier.await();

                    for (int i = 0; i < 1000; i++) {
                        pool.releaseInactive();
                        LockSupport.parkNanos(1L);
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    errors2.incrementAndGet();
                } finally {
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
    public void testBasicCharSequence() throws Exception {

        try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE).col("ts", ColumnType.DATE)) {
            CairoTestUtils.create(model);
        }

        assertWithPool(pool -> {
            sink.clear();
            sink.put("x");

            TableWriter writer1 = pool.get(sink, "testing");
            Assert.assertNotNull(writer1);
            writer1.close();

            // mutate sink
            sink.clear();
            sink.put("y");

            try (TableWriter writer2 = pool.get("x", "testing")) {
                Assert.assertSame(writer1, writer2);
            }
        });
    }

    @Test
    public void testCannotLockWriter() throws Exception {

        final TestFilesFacade ff = new TestFilesFacade() {
            int count = 1;

            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, "z.lock") && count-- > 0) {
                    return -1;
                }
                return super.openRW(name);
            }

            @Override
            public boolean wasCalled() {
                return count <= 0;
            }
        };

        DefaultCairoConfiguration configuration = new DefaultCairoConfiguration(root) {
            @Override
            public FilesFacade getFilesFacade() {
                return ff;
            }
        };

        assertWithPool(pool -> {

            // fail first time
            Assert.assertEquals(WriterPool.UNLOCKING, pool.lock("z", "testing"));

            Assert.assertTrue(ff.wasCalled());

            TableWriter writer = pool.get("z", "testing");
            Assert.assertNotNull(writer);
            writer.close();


            Assert.assertNull(pool.lock("z", "testing"));

            // check that we can't get writer from pool
            try {
                pool.get("z", "testing");
                Assert.fail();
            } catch (CairoException ignore) {
            }


            // check that we can't create standalone writer either
            try {
                new TableWriter(configuration, "z");
                Assert.fail();
            } catch (CairoException ignored) {
            }

            pool.unlock("z");

            // check if we can create standalone writer after pool unlocked it
            writer = new TableWriter(configuration, "z");
            Assert.assertNotNull(writer);
            writer.close();

            // check if we can create writer via pool
            writer = pool.get("z", "testing");
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
                public void onEvent(byte factoryType, long thread, CharSequence name, short event, short segment, short position) {
                    this.ev = event;
                }
            }
            X x = new X();
            pool.setPoolListener(x);
            pool.close();
            try {
                pool.lock("x", "testing");
                Assert.fail();
            } catch (PoolClosedException ignored) {
            }
            Assert.assertEquals(PoolListener.EV_POOL_CLOSED, x.ev);
        });
    }

    @Test
    public void testFactoryCloseBeforeRelease() throws Exception {
        assertWithPool(pool -> {
            TableWriter x;

            x = pool.get("z", "testing");
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
                pool.get("z", "testing");
                Assert.fail();
            } catch (PoolClosedException ignored) {
            }
        });
    }

    @Test
    public void testGetAndCloseRace() throws Exception {

        try (TableModel model = new TableModel(configuration, "xyz", PartitionBy.NONE).col("ts", ColumnType.DATE)) {
            CairoTestUtils.create(model);
        }

        for (int i = 0; i < 100; i++) {
            assertWithPool(pool -> {
                AtomicInteger exceptionCount = new AtomicInteger();
                CyclicBarrier barrier = new CyclicBarrier(2);
                CountDownLatch stopLatch = new CountDownLatch(2);

                // make sure writer exists in pool
                try (TableWriter writer = pool.get("xyz", "testing")) {
                    Assert.assertNotNull(writer);
                }

                new Thread(() -> {
                    try {
                        barrier.await();
                        pool.close();
                    } catch (Exception e) {
                        exceptionCount.incrementAndGet();
                        e.printStackTrace();
                    } finally {
                        stopLatch.countDown();
                    }
                }).start();


                new Thread(() -> {
                    try {
                        barrier.await();
                        try (TableWriter writer = pool.get("xyz", "testing")) {
                            Assert.assertNotNull(writer);
                        } catch (PoolClosedException | EntryUnavailableException ignore) {
                            // this can also happen when this thread is delayed enough for pool close to complete
                        }
                    } catch (Exception e) {
                        exceptionCount.incrementAndGet();
                        e.printStackTrace();
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
    public void testLockNonExisting() throws Exception {
        assertWithPool(pool -> {
            Assert.assertNull(pool.lock("z", "testing"));

            try {
                pool.get("z", "testing");
                Assert.fail();
            } catch (EntryLockedException ignored) {
            }

            pool.unlock("z");

            try (TableWriter wx = pool.get("z", "testing")) {
                Assert.assertNotNull(wx);
            }
        });
    }

    @Test
    public void testLockUnlock() throws Exception {
        try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE).col("ts", ColumnType.DATE)) {
            CairoTestUtils.create(model);
        }

        try (TableModel model = new TableModel(configuration, "y", PartitionBy.NONE).col("ts", ColumnType.DATE)) {
            CairoTestUtils.create(model);
        }

        assertWithPool(pool -> {
            TableWriter wy = pool.get("y", "testing");

            try {
                Assert.assertNotNull(wy);
                Assert.assertTrue(wy.isOpen());

                // check that lock is successful
                Assert.assertNull(pool.lock("x", "testing"));

                // check that writer x is closed and writer y is open (lock must not spill out to other writers)
                Assert.assertTrue(wy.isOpen());

                // check that when name is locked writers are not created
                try {
                    pool.get("x", "testing");
                    Assert.fail();
                } catch (EntryLockedException ignored) {

                }

                final CountDownLatch done = new CountDownLatch(1);
                final AtomicBoolean result = new AtomicBoolean();

                // have new thread try to allocated this writer
                new Thread(() -> {
                    try (TableWriter ignored = pool.get("x", "testing")) {
                        result.set(false);
                    } catch (EntryUnavailableException ignored) {
                        result.set(true);
                    } catch (CairoException e) {
                        e.printStackTrace();
                        result.set(false);
                    }
                    done.countDown();
                }).start();

                Assert.assertTrue(done.await(1, TimeUnit.SECONDS));
                Assert.assertTrue(result.get());

                pool.unlock("x");

                try (TableWriter wx = pool.get("x", "testing")) {
                    Assert.assertNotNull(wx);
                    Assert.assertTrue(wx.isOpen());

                    try {
                        // unlocking writer that has not been locked must produce exception
                        // and not affect open writer
                        pool.unlock("x");
                        Assert.fail();
                    } catch (CairoException ignored) {
                    }

                    Assert.assertTrue(wx.isOpen());
                }

            } finally {
                wy.close();
            }
        });
    }

    @Test
    public void testNewLock() throws Exception {
        assertWithPool(pool -> {
            Assert.assertNull(pool.lock("z", "testing"));
            try {
                pool.get("z", "testing");
                Assert.fail();
            } catch (EntryLockedException ignored) {
            }
            pool.unlock("z");
        });
    }

    @Test
    public void testOneThreadGetRelease() throws Exception {

        assertWithPool(pool -> {
            TableWriter x;
            TableWriter y;

            x = pool.get("z", "testing");
            try {
                Assert.assertEquals(0, pool.countFreeWriters());
                Assert.assertNotNull(x);
                Assert.assertTrue(x.isOpen());
            } finally {
                x.close();
            }

            Assert.assertEquals(1, pool.countFreeWriters());

            y = pool.get("z", "testing");
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
            try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE).col("ts", ColumnType.DATE)) {
                CairoTestUtils.create(model);
            }

            Assert.assertNull(pool.lock("x", "testing"));

            TableWriter writer = new TableWriter(configuration, "x", messageBus, false, DefaultLifecycleManager.INSTANCE);
            for (int i = 0; i < 100; i++) {
                TableWriter.Row row = writer.newRow();
                row.putDate(0, i);
                row.append();
            }
            writer.commit();

            pool.unlock("x", writer, false);

            // make sure our writer stays in pool and close() doesn't destroy it
            Assert.assertSame(writer, pool.get("x", "testing"));
            writer.close();

            // this isn't a mistake, need to check that writer is still alive after close
            Assert.assertSame(writer, pool.get("x", "testing"));
            writer.close();
        });
    }

    @Test
    public void testToStringOnWriter() throws Exception {
        assertWithPool(pool -> {
            try (TableWriter w = pool.get("z", "testing")) {
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
                            try (TableWriter w = pool.get("z", "testing")) {
                                writerCount.incrementAndGet();
                                populate(w);
                            } catch (EntryUnavailableException ignored) {
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            errors.incrementAndGet();
                        } finally {
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
                            try (TableWriter w = pool.get("z", "testing")) {
                                writerCount.incrementAndGet();
                                populate(w);

                                Assert.assertSame(w, pool.get("z", "testing"));

                            } catch (EntryUnavailableException ignored) {
                            }

                            // lock frees up writer, make sure on next iteration threads have something to compete for
                            if (null == pool.lock("z", "testing")) {
                                pool.unlock("z");
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            errors.incrementAndGet();
                        } finally {
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
    public void testTwoThreadsRaceToLock() throws Exception {
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
                            if (null == pool.lock("z", "testing")) {
                                LockSupport.parkNanos(1);
                                pool.unlock("z");
                            } else {
                                Thread.yield();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            errors.incrementAndGet();
                        } finally {
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
    public void testUnlockInAnotherThread() throws Exception {
        assertWithPool(pool -> {

            Assert.assertNull(pool.lock("x", "testing"));
            AtomicInteger errors = new AtomicInteger();

            CountDownLatch latch = new CountDownLatch(1);
            new Thread(() -> {
                try {
                    try {
                        pool.unlock("x");
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "Not lock owner");
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                    errors.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            }).start();

            Assert.assertTrue(latch.await(2, TimeUnit.SECONDS));
            Assert.assertEquals(0, errors.get());

            try {
                pool.get("x", "testing");
                Assert.fail();
            } catch (EntryLockedException ignore) {
            }
            pool.unlock("x");
        });
    }

    @Test
    public void testUnlockNonExisting() throws Exception {
        assertWithPool(pool -> {
            class X implements PoolListener {
                short ev = -1;

                @Override
                public void onEvent(byte factoryType, long thread, CharSequence name, short event, short segment, short position) {
                    this.ev = event;
                }
            }
            X x = new X();
            pool.setPoolListener(x);
            pool.unlock("x");
            Assert.assertEquals(PoolListener.EV_NOT_LOCKED, x.ev);
        });
    }

    @Test
    public void testUnlockWriterWhenPoolIsClosed() throws Exception {
        assertWithPool(pool -> {
            Assert.assertNull(pool.lock("z", "testing"));

            pool.close();

            TableWriter writer = new TableWriter(configuration, "z");
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
                public void onEvent(byte factoryType, long thread, CharSequence name, short event, short segment, short position) {
                    this.ev = event;
                }
            }
            X x = new X();
            pool.setPoolListener(x);
            TableWriter w = pool.get("z", "testing");
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
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, "z.lock") && count-- > 0) {
                    return -1;
                }
                return super.openRW(name);
            }

            @Override
            public boolean wasCalled() {
                return count <= 0;
            }
        };

        DefaultCairoConfiguration configuration = new DefaultCairoConfiguration(root) {
            @Override
            public FilesFacade getFilesFacade() {
                return ff;
            }
        };

        assertWithPool(pool -> {
            try {
                pool.get("z", "testing");
                Assert.fail();
            } catch (CairoException ignore) {
            }

            Assert.assertEquals(1, pool.size());
            Assert.assertEquals(0, pool.getBusyCount());

            pool.releaseInactive();
            Assert.assertEquals(0, pool.size());

            // try again
            TableWriter w = pool.get("z", "testing");
            Assert.assertEquals(1, pool.getBusyCount());
            w.close();
        }, configuration);

        Assert.assertTrue(ff.wasCalled());

    }

    private void assertWithPool(PoolAwareCode code, CairoConfiguration configuration) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (WriterPool pool = new WriterPool(configuration, new MessageBusImpl(configuration))) {
                code.run(pool);
            }
        });
    }

    private void assertWithPool(PoolAwareCode code) throws Exception {
        assertWithPool(code, CONFIGURATION);
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

    static {
        CONFIGURATION = new DefaultCairoConfiguration(root);
    }
}