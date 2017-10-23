package com.questdb.cairo.pool;

import com.questdb.cairo.*;
import com.questdb.cairo.pool.ex.EntryLockedException;
import com.questdb.cairo.pool.ex.EntryUnavailableException;
import com.questdb.cairo.pool.ex.PoolClosedException;
import com.questdb.factory.configuration.JournalStructure;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.misc.Rnd;
import com.questdb.std.LongList;
import com.questdb.std.ObjHashSet;
import com.questdb.std.ObjList;
import com.questdb.std.str.LPSZ;
import com.questdb.std.str.StringSink;
import com.questdb.test.tools.TestUtils;
import com.questdb.txt.RecordSourcePrinter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class ReaderPoolTest extends AbstractCairoTest {
    private static final FilesFacade ff = FilesFacadeImpl.INSTANCE;
    private static final Log LOG = LogFactory.getLog(ReaderPoolTest.class);
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
                        try (TableReader ignored = pool.getReader("z")) {
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
    public void testCloseReaderWhenPoolClosed() throws Exception {
        assertWithPool(pool -> {
            TableReader reader = pool.getReader("z");
            Assert.assertNotNull(reader);
            pool.close();
            Assert.assertTrue(reader.isOpen());
            reader.close();
            reader.close();
        });
    }

    @Test
    public void testCloseWithActiveReader() throws Exception {
        assertWithPool(pool -> {
            TableReader reader = pool.getReader("z");
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
            TableReader reader = pool.getReader("z");
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
            CairoTestUtils.createTable(ff, root, new JournalStructure(names[i]).$date("ts").$());
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

                            try (TableReader ignored = pool.getReader(m)) {
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

    @Test
    public void testGetMultipleReaders() throws Exception {
        assertWithPool(pool -> {
            ObjHashSet<TableReader> readers = new ObjHashSet<>();
            for (int i = 0; i < 64; i++) {
                Assert.assertTrue(readers.add(pool.getReader("z")));
            }
            for (int i = 0, n = readers.size(); i < n; i++) {
                TableReader reader = readers.get(i);
                Assert.assertTrue(reader.isOpen());
                reader.close();
            }
        });
    }

    @Test
    public void testGetReaderFailure() throws Exception {
        final int N = 3;
        final int K = 40;

        TestFilesFacade ff = new TestFilesFacade() {
            int count = N;

            @Override
            public long openRO(LPSZ name) {
                if (count-- > 0) {
                    return -1;
                }
                return super.openRO(name);
            }

            @Override
            public boolean wasCalled() {
                return count < N;
            }
        };

        assertWithPool(pool -> {
            for (int i = 0; i < 3; i++) {
                try {
                    pool.getReader("z");
                    Assert.fail();
                } catch (CairoException ignored) {
                }

                Assert.assertEquals(0, pool.getBusyCount());
            }

            ObjHashSet<TableReader> readers = new ObjHashSet<>();
            for (int i = 0; i < K; i++) {
                Assert.assertTrue(readers.add(pool.getReader("z")));
            }

            Assert.assertEquals(K, pool.getBusyCount());
            Assert.assertEquals(K, readers.size());

            for (int i = 0; i < K; i++) {
                TableReader reader = readers.get(i);
                Assert.assertTrue(reader.isOpen());
                reader.close();
            }

        }, new DefaultCairoConfiguration(root) {
            @Override
            public FilesFacade getFilesFacade() {
                return ff;
            }
        });

        Assert.assertTrue(ff.wasCalled());
    }

    @Test
    public void testGetReaderWhenPoolClosed() throws Exception {
        assertWithPool(pool -> {
            pool.close();

            try {
                pool.getReader("z");
                Assert.fail();
            } catch (PoolClosedException ignored) {
            }
        });
    }

    @SuppressWarnings("InfiniteLoopStatement")
    @Test
    public void testGetReadersBeforeFailure() throws Exception {
        assertWithPool(pool -> {
            ObjList<TableReader> readers = new ObjList<>();
            try {
                do {
                    readers.add(pool.getReader("z"));
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
        Rnd dataRnd = new Rnd();
        StringSink sink = new StringSink();
        RecordSourcePrinter printer = new RecordSourcePrinter(sink);


        final String[] names = new String[readerCount];
        final String[] expectedRows = new String[readerCount];

        for (int i = 0; i < readerCount; i++) {
            names[i] = "x" + i;
            CairoTestUtils.createTable(ff, root, new JournalStructure(names[i]).$date("ts").$());

            try (TableWriter w = new TableWriter(ff, root, names[i])) {
                for (int k = 0; k < 10; k++) {
                    TableWriter.Row r = w.newRow(0);
                    r.putDate(0, dataRnd.nextLong());
                    r.append();
                }
                w.commit();
            }

            sink.clear();
            try (TableReader r = new TableReader(ff, root, names[i])) {
                printer.print(r, true, r.getMetadata());
            }
            expectedRows[i] = sink.toString();
        }

        LOG.info().$("testLockBusyReader BEGIN").$();

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
                    String name;
                    for (int i = 0; i < iterations; i++) {
                        name = names[rnd.nextPositiveInt() % readerCount];
                        while (true) {
                            try {
                                pool.lock(name);
                                lockTimes.add(System.currentTimeMillis());
                                LockSupport.parkNanos(10L);
                                pool.unlock(name);
                                name = null;
                                break;
                            } catch (EntryUnavailableException ignored) {
                            }
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    halt.countDown();
                }
            }).start();

            new Thread(() -> {
                Rnd rnd = new Rnd();

                try {
                    workerTimes.add(System.currentTimeMillis());
                    for (int i = 0; i < iterations; i++) {
                        int index = rnd.nextPositiveInt() % readerCount;
                        String name = names[index];
                        try (TableReader r = pool.getReader(name)) {
                            r.toTop();
                            sink.clear();
                            printer.print(r, true, r.getMetadata());
                            TestUtils.assertEquals(expectedRows[index], sink);

                            if (name.equals(names[readerCount - 1]) && barrier.getNumberWaiting() > 0) {
                                barrier.await();
                            }
                            LockSupport.parkNanos(10L);
                        } catch (EntryLockedException | EntryUnavailableException ignored) {
                        } catch (Exception e) {
                            errors.incrementAndGet();
                            e.printStackTrace();
                            break;
                        }
                    }
                    workerTimes.add(System.currentTimeMillis());
                } finally {
                    halt.countDown();
                }
            }).start();

            halt.await();
            Assert.assertEquals(0, halt.getCount());
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

            LOG.info().$("testLockBusyReader END").$();
        });
    }

    @Test
    public void testLockMultipleReaders() throws Exception {
        assertWithPool(pool -> {
            ObjHashSet<TableReader> readers = new ObjHashSet<>();
            for (int i = 0; i < 64; i++) {
                Assert.assertTrue(readers.add(pool.getReader("z")));
            }
            Assert.assertEquals(64, pool.getBusyCount());

            for (int i = 0, n = readers.size(); i < n; i++) {
                TableReader reader = readers.get(i);
                Assert.assertTrue(reader.isOpen());
                reader.close();
            }
            pool.lock("z");
            Assert.assertEquals(0, pool.getBusyCount());
            for (int i = 0, n = readers.size(); i < n; i++) {
                Assert.assertFalse(readers.get(i).isOpen());
            }
            pool.unlock("z");
        });
    }

    @Test
    public void testLockUnlock() throws Exception {
        // create journals
        CairoTestUtils.createTable(ff, root, new JournalStructure("x").$date("ts").$());
        CairoTestUtils.createTable(ff, root, new JournalStructure("y").$date("ts").$());

        assertWithPool(pool -> {
            TableReader x, y;
            x = pool.getReader("x");
            Assert.assertNotNull(x);

            y = pool.getReader("y");
            Assert.assertNotNull(y);

            // expect lock to fail because we have "x" open
            try {
                pool.lock("x");
                Assert.fail();
            } catch (EntryUnavailableException ignore) {
            }

            x.close();

            // expect lock to succeed after we closed "x"
            pool.lock("x");

            // expect "x" to be physically closed
            Assert.assertFalse(x.isOpen());

            // "x" is locked, expect this to fail
            try {
                Assert.assertNull(pool.getReader("x"));
            } catch (EntryLockedException ignored) {
            }

            pool.unlock("x");

            x = pool.getReader("x");
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
            TableReader r1 = pool.getReader("z");
            TableReader r2 = pool.getReader("z");
            r1.close();
            try {
                pool.lock("z");
            } catch (EntryUnavailableException ignored) {
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
                try (TableReader reader = pool.getReader("z")) {
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
        assertWithPool(code, configuration);
    }

    private void assertWithPool(PoolAwareCode code, final CairoConfiguration configuration) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (ReaderPool pool = new ReaderPool(configuration)) {
                code.run(pool);
            }
        });
    }

    private void createTable() {
        CairoTestUtils.createTable(FilesFacadeImpl.INSTANCE, root, new JournalStructure("z").$date("ts").$());
    }

    private interface PoolAwareCode {
        void run(ReaderPool pool) throws Exception;
    }
}