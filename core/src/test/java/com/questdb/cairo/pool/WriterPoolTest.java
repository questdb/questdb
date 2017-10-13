package com.questdb.cairo.pool;

import com.questdb.cairo.*;
import com.questdb.cairo.pool.ex.EntryLockedException;
import com.questdb.cairo.pool.ex.EntryUnavailableException;
import com.questdb.cairo.pool.ex.PoolClosedException;
import com.questdb.factory.configuration.JournalStructure;
import com.questdb.test.tools.TestUtils;
import org.junit.After;
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
    private WriterPool pool;

    @Before
    public void setUpInstance() throws Exception {
        this.pool = new WriterPool(FilesFacadeImpl.INSTANCE, root, -100000);
        createTable();
    }

    @After
    public void tearDown() throws Exception {
        this.pool.close();
    }

    @Test
    public void testAllocateAndClear() throws Exception {

        TestUtils.assertMemoryLeak(() -> {
            int n = 2;
            final CyclicBarrier barrier = new CyclicBarrier(n);
            final CountDownLatch halt = new CountDownLatch(n);
            final AtomicInteger errors = new AtomicInteger();
            final AtomicInteger writerCount = new AtomicInteger();

            try {
                new Thread(() -> {
                    try {
                        for (int i = 0; i < 1000; i++) {
                            try (TableWriter ignored = pool.writer("z")) {
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
                        errors.incrementAndGet();
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
                        errors.incrementAndGet();
                    } finally {
                        halt.countDown();
                    }
                }).start();

                halt.await();

                Assert.assertTrue(writerCount.get() > 0);
                Assert.assertEquals(0, errors.get());
            } catch (Throwable e) {
                e.printStackTrace();
            } finally {
                pool.close();
            }
        });
    }

    @Test
    public void testFactoryCloseBeforeRelease() throws Exception {

        TableWriter x;

        x = pool.writer("z");
        try {
            Assert.assertEquals(0, pool.countFreeWriters());
            Assert.assertNotNull(x);
            Assert.assertTrue(x.isOpen());
            Assert.assertTrue(x == pool.writer("z"));
            pool.close();
        } finally {
            x.close();
        }

        Assert.assertFalse(x.isOpen());
        try {
            pool.writer("z");
            Assert.fail();
        } catch (PoolClosedException ignored) {
        }
    }

    @Test
    public void testLockNonExisting() throws Exception {
        pool.lock("z");

        try {
            pool.writer("z");
            Assert.fail();
        } catch (EntryLockedException ignored) {
        }

        pool.unlock("z");

        try (TableWriter wx = pool.writer("z")) {
            Assert.assertNotNull(wx);
        }
    }

    @Test
    public void testLockUnlock() throws Exception {

        TableUtils.create(FilesFacadeImpl.INSTANCE, root, new JournalStructure("x").$date("ts").$().build(), 509);
        TableUtils.create(FilesFacadeImpl.INSTANCE, root, new JournalStructure("y").$date("ts").$().build(), 509);

        TableWriter wx = pool.writer("x");
        Assert.assertNotNull(wx);
        Assert.assertTrue(wx.isOpen());

        TableWriter wy = pool.writer("y");
        Assert.assertNotNull(wy);
        Assert.assertTrue(wy.isOpen());

        try {

            // check that lock is successful
            pool.lock("x");

            // check that writer x is closed and writer y is open (lock must not spill out to other writers)
            Assert.assertFalse(wx.isOpen());
            Assert.assertTrue(wy.isOpen());

            // check that when name is locked writers are not created
            try {
                pool.writer("x");
                Assert.fail();
            } catch (EntryLockedException ignored) {

            }

            final CountDownLatch done = new CountDownLatch(1);
            final AtomicBoolean result = new AtomicBoolean();

            // have new thread try to allocated this writers
            new Thread(() -> {
                try (TableWriter ignored = pool.writer("x")) {
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

            wx = pool.writer("x");
            Assert.assertNotNull(wx);
            Assert.assertTrue(wx.isOpen());

            try {
                // unlocking writer that has not been locked must produce exception
                // and not affect open writer
                pool.unlock("x");
                Assert.fail();
            } catch (IllegalStateException ignored) {
            }

            Assert.assertTrue(wx.isOpen());

        } finally {
            wx.close();
            wy.close();
        }
    }

    @Test
    public void testNewLock() throws Exception {
        pool.lock("z");
        try {
            pool.writer("z");
            Assert.fail();
        } catch (EntryLockedException ignored) {
        }
        pool.unlock("z");
    }

    @Test
    public void testOneThreadGetRelease() throws Exception {

        TableWriter x;
        TableWriter y;

        x = pool.writer("z");
        try {
            Assert.assertEquals(0, pool.countFreeWriters());
            Assert.assertNotNull(x);
            Assert.assertTrue(x.isOpen());
            Assert.assertTrue(x == pool.writer("z"));
        } finally {
            x.close();
        }

        Assert.assertEquals(1, pool.countFreeWriters());

        y = pool.writer("z");
        try {
            Assert.assertNotNull(y);
            Assert.assertTrue(y.isOpen());
            Assert.assertTrue(y == x);
        } finally {
            y.close();
        }

        Assert.assertEquals(1, pool.countFreeWriters());
    }

    @Test
    public void testTwoThreadsRaceToAllocate() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
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
                                try (TableWriter w = pool.writer("z")) {
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
            } finally {
                pool.close();
            }
        });
    }

    @Test
    public void testTwoThreadsRaceToAllocateAndLock() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
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
                                getFromPoolPopulateAndLock(writerCount);
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
            } finally {
                pool.close();
            }
        });
    }

    @Test
    public void testTwoThreadsRaceToLock() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
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
                                try {
                                    pool.lock("z");
                                    LockSupport.parkNanos(1);
                                    pool.unlock("z");
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
                    Assert.assertTrue(writerCount.get() == 0);
                    Assert.assertEquals(0, errors.get());
                    Assert.assertEquals(0, pool.countFreeWriters());
                }
            } finally {
                pool.close();
            }
        });
    }

    private void createTable() {
        TableUtils.create(FilesFacadeImpl.INSTANCE, root, new JournalStructure("z").$date("ts").$().build(), 509);
    }

    private void getFromPoolPopulateAndLock(AtomicInteger writerCount) {
        try (TableWriter w = pool.writer("z")) {
            writerCount.incrementAndGet();
            populate(w);

            Assert.assertTrue(w == pool.writer("z"));

            // lock frees up writer, make sure on next iteration threads have something to compete for
            pool.lock("z");
            pool.unlock("z");
        } catch (EntryUnavailableException ignored) {
        }
    }

    private void populate(TableWriter w) {
        long start = w.getMaxTimestamp();
        for (int i = 0; i < 1000; i++) {
            w.newRow(start + i).append();
            w.commit();
        }
    }
}