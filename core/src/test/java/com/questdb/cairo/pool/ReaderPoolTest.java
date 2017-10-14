package com.questdb.cairo.pool;

import com.questdb.cairo.*;
import com.questdb.cairo.pool.ex.EntryLockedException;
import com.questdb.cairo.pool.ex.EntryUnavailableException;
import com.questdb.factory.configuration.JournalMetadata;
import com.questdb.factory.configuration.JournalStructure;
import com.questdb.misc.Rnd;
import com.questdb.std.LongList;
import com.questdb.std.ObjList;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class ReaderPoolTest extends AbstractCairoTest {
    private static final FilesFacade ff = FilesFacadeImpl.INSTANCE;
    private static DefaultCairoConfiguration configuration = new DefaultCairoConfiguration(root);

    @Before
    public void setUpInstance() throws Exception {
        createTable();
    }

    @Test
    public void testAllocateAndClear() throws Exception {

        assertWithPool(pool -> {
            int n = 2;
            final CyclicBarrier barrier = new CyclicBarrier(n);
            final CountDownLatch halt = new CountDownLatch(n);
            final AtomicInteger errors = new AtomicInteger();
            final AtomicInteger readerCount = new AtomicInteger();

            new Thread(() -> {
                try {
                    for (int i = 0; i < 1000; i++) {
                        try (TableReader ignored = pool.reader("z")) {
                            readerCount.incrementAndGet();
                        } catch (EntryUnavailableException ignored) {
                        }

                        if (i == 1) {
                            barrier.await();
                        }
                        LockSupport.parkNanos(10L);
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
                        LockSupport.parkNanos(10L);
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    errors.incrementAndGet();
                } finally {
                    halt.countDown();
                }
            }).start();

            halt.await();

            Assert.assertTrue(readerCount.get() > 0);
            Assert.assertEquals(0, errors.get());
        });
    }

    @Test
    public void testCloseWithActiveReader() throws Exception {
        assertWithPool(pool -> {
            TableReader reader = pool.reader("z");
            Assert.assertNotNull(reader);
            pool.close();
            Assert.assertTrue(reader.isOpen());
            reader.close();
            Assert.assertFalse(reader.isOpen());
        });
    }

    @Test
    public void testCloseWithInactiveReader() throws Exception {
        assertWithPool(pool -> {
            TableReader reader = pool.reader("z");
            Assert.assertNotNull(reader);
            reader.close();
            Assert.assertTrue(reader.isOpen());
            pool.close();
            Assert.assertFalse(reader.isOpen());
        });
    }

    @Test
    public void testConcurrentOpenAndClose() throws Exception {
        final int readerCount = 5;
        int threadCount = 2;
        final int iterations = 1000;

        final String[] names = new String[readerCount];
        for (int i = 0; i < readerCount; i++) {
            names[i] = "x" + i;
            final JournalMetadata<?> m = new JournalStructure(names[i]).$date("ts").$().build();
            TableUtils.create(ff, root, m, 509);
        }

        assertWithPool(pool -> {
            final CyclicBarrier barrier = new CyclicBarrier(threadCount);
            final CountDownLatch halt = new CountDownLatch(threadCount);
            final AtomicInteger errors = new AtomicInteger();

            for (int i = 0; i < threadCount; i++) {
                final int x = i;
                new Thread(() -> {
                    Rnd rnd = new Rnd(x, -x);
                    try {
                        barrier.await();

                        for (int i1 = 0; i1 < iterations; i1++) {
                            String m = names[rnd.nextPositiveInt() % readerCount];

                            try (TableReader ignored = pool.reader(m)) {
                                LockSupport.parkNanos(100);
                            }
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
            Assert.assertEquals(0, errors.get());
        });
    }

    @SuppressWarnings("InfiniteLoopStatement")
    @Test
    public void testGetReadersBeforeFailure() throws Exception {
        assertWithPool(pool -> {
            ObjList<TableReader> readers = new ObjList<>();
            try {
                do {
                    readers.add(pool.reader("z"));
                } while (true);
            } catch (EntryUnavailableException e) {
                Assert.assertEquals(pool.getMaxEntries(), readers.size());
            } finally {
                for (int i = 0, n = readers.size(); i < n; i++) {
                    readers.getQuick(i).close();
                }
            }
        });
    }

    @Test
    public void testLockBusyReader() throws Exception {
        final int readerCount = 5;
        int threadCount = 2;
        final int iterations = 10000;

        final String[] names = new String[readerCount];
        for (int i = 0; i < readerCount; i++) {
            names[i] = "x" + i;
            final JournalMetadata<?> m = new JournalStructure(names[i]).$date("ts").$().build();
            TableUtils.create(ff, root, m, 509);
        }

        assertWithPool(pool -> {
            final CyclicBarrier barrier = new CyclicBarrier(threadCount);
            final CountDownLatch halt = new CountDownLatch(threadCount);
            final AtomicInteger errors = new AtomicInteger();
            final LongList lockTimes = new LongList();
            final LongList workerTimes = new LongList();

            new Thread(() -> {
                Rnd rnd = new Rnd();
                try {
                    barrier.await();
                    String name = null;
                    for (int i = 0; i < iterations; i++) {
                        if (name == null) {
                            name = names[rnd.nextPositiveInt() % readerCount];
                        }
                        while (true) {
                            try {
                                pool.lock(name);
                                lockTimes.add(System.currentTimeMillis());
                                LockSupport.parkNanos(100L);
                                pool.unlock(name);
                                name = null;
                                break;
                            } catch (CairoException e) {
                                if (!(e instanceof EntryLockedException)) {
                                    e.printStackTrace();
                                    errors.incrementAndGet();
                                    break;
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    errors.incrementAndGet();
                }
                halt.countDown();
            }).start();

            new Thread(() -> {
                Rnd rnd = new Rnd();

                workerTimes.add(System.currentTimeMillis());
                for (int i = 0; i < iterations; i++) {
                    String name = names[rnd.nextPositiveInt() % readerCount];
                    try (TableReader ignored = pool.reader(name)) {
                        if (name.equals(names[readerCount - 1]) && barrier.getNumberWaiting() > 0) {
                            barrier.await();
                        }
                        LockSupport.parkNanos(10L);
                    } catch (EntryLockedException ignored) {
                    } catch (Exception e) {
                        e.printStackTrace();
                        errors.incrementAndGet();
                    }
                }
                workerTimes.add(System.currentTimeMillis());

                halt.countDown();
            }).start();

            halt.await();
            Assert.assertEquals(0, errors.get());

            // check that there are lock times between worker times
            int count = 0;

            // ensure that we have worker times
            Assert.assertEquals(2, workerTimes.size());
            long lo = workerTimes.get(0);
            long hi = workerTimes.get(1);

            Assert.assertTrue(lockTimes.size() > 0);

            for (int i = 0, n = lockTimes.size(); i < n; i++) {
                long t = lockTimes.getQuick(i);
                if (t > lo && t < hi) {
                    count++;
                }
            }
            Assert.assertTrue(count > 0);
        });
    }

    @Test
    public void testLockUnlock() throws Exception {
        // create journals
        final JournalMetadata<?> m1 = new JournalStructure("x").$date("ts").$().build();
        TableUtils.create(ff, root, m1, 509);

        final JournalMetadata<?> m2 = new JournalStructure("y").$date("ts").$().build();
        TableUtils.create(ff, root, m2, 509);

        assertWithPool(pool -> {
            TableReader x, y;
            x = pool.reader("x");
            Assert.assertNotNull(x);

            y = pool.reader("y");
            Assert.assertNotNull(y);

            // expect lock to fail because we have "x" open
            try {
                pool.lock("x");
                Assert.fail();
            } catch (EntryLockedException ignore) {
            }

            x.close();

            // expect lock to succeed after we closed "x"
            pool.lock("x");

            // expect "x" to be physically closed
            Assert.assertFalse(x.isOpen());

            // "x" is locked, expect this to fail
            try {
                Assert.assertNull(pool.reader("x"));
            } catch (EntryLockedException ignored) {
            }

            pool.unlock("x");

            x = pool.reader("x");
            Assert.assertNotNull(x);
            x.close();

            Assert.assertTrue(x.isOpen());

            Assert.assertTrue(y.isOpen());
            y.close();

            pool.close();

            Assert.assertFalse(y.isOpen());

            // "x" was not busy and should be closed by factory
            Assert.assertFalse(x.isOpen());
        });
    }

    @Test
    public void testLockUnlockMultiple() throws Exception {
        assertWithPool(pool -> {
            TableReader r1 = pool.reader("z");
            TableReader r2 = pool.reader("z");
            r1.close();
            try {
                pool.lock("z");
            } catch (EntryLockedException ignored) {
            }
            r2.close();
            pool.lock("z");
            pool.unlock("z");
        });
    }

    @Test
    public void testSerialOpenClose() throws Exception {
        assertWithPool(pool -> {
            TableReader firstReader = null;
            for (int i = 0; i < 1000; i++) {
                try (TableReader reader = pool.reader("z")) {
                    if (firstReader == null) {
                        firstReader = reader;
                    }
                    Assert.assertNotNull(reader);
                    Assert.assertSame(firstReader, reader);
                }
            }
        });
    }

    private void assertWithPool(PoolAwareCode code) throws Exception {
        // clear thread locals so that initial memory usage reading
        // does not include these values
        TableUtils.freeThreadLocals();
        TestUtils.assertMemoryLeak(() -> {
            try (ReaderPool pool = new ReaderPool(configuration)) {
                code.run(pool);
            }
            // clear thread locals at end of test to exclude any memory usage by
            // those from assertion
            TableUtils.freeThreadLocals();
        });
    }

    private void createTable() {
        TableUtils.create(FilesFacadeImpl.INSTANCE, root, new JournalStructure("z").$date("ts").$().build(), 509);
    }

    private interface PoolAwareCode {
        void run(ReaderPool pool) throws Exception;
    }
}