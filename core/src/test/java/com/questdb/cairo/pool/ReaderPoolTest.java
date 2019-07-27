/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cairo.pool;

import com.questdb.cairo.*;
import com.questdb.cairo.pool.ex.EntryLockedException;
import com.questdb.cairo.pool.ex.EntryUnavailableException;
import com.questdb.cairo.pool.ex.PoolClosedException;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.std.*;
import com.questdb.std.str.LPSZ;
import com.questdb.std.str.StringSink;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class ReaderPoolTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(ReaderPoolTest.class);

    @Before
    public void setUpInstance() {
        try (TableModel model = new TableModel(configuration, "u", PartitionBy.NONE).col("ts", ColumnType.DATE)) {
            CairoTestUtils.create(model);
        }
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
                        try (TableReader ignored = pool.get("u")) {
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
    public void testBasicCharSequence() throws Exception {

        try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE).col("ts", ColumnType.DATE)) {
            CairoTestUtils.create(model);
        }

        assertWithPool(pool -> {
            sink.clear();
            sink.put("x");

            TableReader reader1 = pool.get(sink);
            Assert.assertNotNull(reader1);
            reader1.close();

            // mutate sink
            sink.clear();
            sink.put("y");

            try (TableReader reader2 = pool.get("x")) {
                Assert.assertSame(reader1, reader2);
            }
        });
    }

    @Test
    public void testClosePoolWhenReaderIsOut() throws Exception {
        assertWithPool(pool -> {
            try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE).col("ts", ColumnType.DATE)) {
                CairoTestUtils.create(model);
            }

            try (TableReader reader = pool.get("x")) {
                Assert.assertNotNull(reader);
                pool.close();
                Assert.assertTrue(reader.isOpen());
            }
        });
    }

    @Test
    public void testCloseReaderWhenPoolClosed() throws Exception {
        assertWithPool(pool -> {
            TableReader reader = pool.get("u");
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
            TableReader reader = pool.get("u");
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
            TableReader reader = pool.get("u");
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
            try (TableModel model = new TableModel(configuration, names[i], PartitionBy.NONE).col("ts", ColumnType.DATE)) {
                CairoTestUtils.create(model);
            }
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

                            try (TableReader ignored = pool.get(m)) {
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
    public void testConcurrentRead() throws Exception {
        final int readerCount = 5;
        int threadCount = 2;
        final int iterations = 1000000;
        Rnd dataRnd = new Rnd();


        final String[] names = new String[readerCount];
        final String[] expectedRows = new String[readerCount];
        final CharSequenceObjHashMap<String> expectedRowMap = new CharSequenceObjHashMap<>();

        for (int i = 0; i < readerCount; i++) {
            names[i] = "x" + i;
            try (TableModel model = new TableModel(configuration, names[i], PartitionBy.NONE).col("ts", ColumnType.DATE)) {
                CairoTestUtils.create(model);
            }

            try (TableWriter w = new TableWriter(configuration, names[i])) {
                for (int k = 0; k < 10; k++) {
                    TableWriter.Row r = w.newRow();
                    r.putDate(0, dataRnd.nextLong());
                    r.append();
                }
                w.commit();
            }

            sink.clear();
            try (TableReader r = new TableReader(configuration, names[i])) {
                printer.print(r.getCursor(), r.getMetadata(), true);
            }
            expectedRows[i] = sink.toString();
            expectedRowMap.put(names[i], expectedRows[i]);
        }

        assertWithPool((ReaderPool pool) -> {
            final CyclicBarrier barrier = new CyclicBarrier(threadCount);
            final CountDownLatch halt = new CountDownLatch(threadCount);
            final AtomicInteger errors = new AtomicInteger();

            for (int k = 0; k < threadCount; k++) {
                new Thread(new Runnable() {

                    final ObjHashSet<TableReader> readers = new ObjHashSet<>();
                    final StringSink sink = new StringSink();
                    final RecordCursorPrinter printer = new RecordCursorPrinter(sink);

                    @Override
                    public void run() {
                        Rnd rnd = new Rnd();
                        try {
                            barrier.await();
                            String name;

                            // on each iteration thread will do between 1 and 3 things:
                            // 1. it will open a random reader
                            // 2. it will read from one of readers it has opened
                            // 3. it will close of of readers if has opened
                            for (int i = 0; i < iterations; i++) {

                                if (readers.size() == 0 || (readers.size() < 40 && rnd.nextPositiveInt() % 4 == 0)) {
                                    name = names[rnd.nextPositiveInt() % readerCount];
                                    try {
                                        Assert.assertTrue(readers.add(pool.get(name)));
                                    } catch (EntryUnavailableException ignore) {
                                    }
                                }

                                Thread.yield();

                                if (readers.size() == 0) {
                                    continue;
                                }

                                int index = rnd.nextPositiveInt() % readers.size();
                                TableReader reader = readers.get(index);
                                Assert.assertTrue(reader.isOpen());

                                // read rows
                                sink.clear();
                                printer.print(reader.getCursor(), reader.getMetadata(), true);
                                TestUtils.assertEquals(expectedRowMap.get(reader.getTableName()), sink);

                                Thread.yield();

                                if (readers.size() > 0 && rnd.nextPositiveInt() % 4 == 0) {
                                    TableReader r2 = readers.get(rnd.nextPositiveInt() % readers.size());
                                    Assert.assertTrue(r2.isOpen());
                                    r2.close();
                                    Assert.assertTrue(readers.remove(r2));
                                }

                                Thread.yield();

                            }
                        } catch (Exception e) {
                            errors.incrementAndGet();
                            e.printStackTrace();
                        } finally {
                            for (int i = 0; i < readers.size(); i++) {
                                readers.get(i).close();
                            }
                            halt.countDown();
                        }
                    }
                }).start();
            }

            halt.await();
            Assert.assertEquals(0, halt.getCount());
            Assert.assertEquals(0, errors.get());
        });
    }

    @Test
    public void testDoubleLock() throws Exception {
        try (TableModel model = new TableModel(configuration, "xyz", PartitionBy.NONE).col("ts", ColumnType.DATE)) {
            CairoTestUtils.create(model);
        }

        assertWithPool(pool -> {
            Assert.assertTrue(pool.lock("xyz"));
            Assert.assertTrue(pool.lock("xyz"));

            try {
                pool.get("xyz");
                Assert.fail();
            } catch (EntryLockedException ignore) {
            }

            pool.unlock("xyz");

            try (TableReader reader = pool.get("xyz")) {
                Assert.assertNotNull(reader);
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
                        try (TableReader reader = pool.get("xyz")) {
                            Assert.assertNotNull(reader);
                        } catch (PoolClosedException ignore) {
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
    public void testGetMultipleReaders() throws Exception {
        assertWithPool(pool -> {
            ObjHashSet<TableReader> readers = new ObjHashSet<>();
            for (int i = 0; i < 64; i++) {
                Assert.assertTrue(readers.add(pool.get("u")));
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
                    pool.get("u");
                    Assert.fail();
                } catch (CairoException ignored) {
                }

                Assert.assertEquals(0, pool.getBusyCount());
            }

            ObjHashSet<TableReader> readers = new ObjHashSet<>();
            for (int i = 0; i < K; i++) {
                Assert.assertTrue(readers.add(pool.get("u")));
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
                pool.get("u");
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
                    readers.add(pool.get("u"));
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
        RecordCursorPrinter printer = new RecordCursorPrinter(sink);


        final String[] names = new String[readerCount];
        final String[] expectedRows = new String[readerCount];

        for (int i = 0; i < readerCount; i++) {
            names[i] = "x" + i;
            try (TableModel model = new TableModel(configuration, names[i], PartitionBy.NONE).col("ts", ColumnType.DATE)) {
                CairoTestUtils.create(model);
            }

            try (TableWriter w = new TableWriter(configuration, names[i])) {
                for (int k = 0; k < 10; k++) {
                    TableWriter.Row r = w.newRow();
                    r.putDate(0, dataRnd.nextLong());
                    r.append();
                }
                w.commit();
            }

            sink.clear();
            try (TableReader r = new TableReader(configuration, names[i])) {
                printer.print(r.getCursor(), r.getMetadata(), true);
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
                            if (pool.lock(name)) {
                                lockTimes.add(System.currentTimeMillis());
                                LockSupport.parkNanos(10L);
                                pool.unlock(name);
                                break;
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
                        try (TableReader r = pool.get(name)) {
                            sink.clear();
                            printer.print(r.getCursor(), r.getMetadata(), true);
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
                Assert.assertTrue(readers.add(pool.get("u")));
            }
            Assert.assertEquals(64, pool.getBusyCount());

            for (int i = 0, n = readers.size(); i < n; i++) {
                TableReader reader = readers.get(i);
                Assert.assertTrue(reader.isOpen());
                reader.close();
            }
            Assert.assertTrue(pool.lock("u"));
            Assert.assertEquals(0, pool.getBusyCount());
            for (int i = 0, n = readers.size(); i < n; i++) {
                Assert.assertFalse(readers.get(i).isOpen());
            }
            pool.unlock("u");
        });
    }

    @Test
    public void testLockRace() throws Exception {
        assertWithPool(pool -> {
            AtomicInteger successCount = new AtomicInteger();
            AtomicInteger failureCount = new AtomicInteger();
            AtomicInteger exceptionCount = new AtomicInteger();
            CyclicBarrier barrier = new CyclicBarrier(2);
            CountDownLatch stopLatch = new CountDownLatch(2);

            final Runnable runnable = () -> {
                try {
                    barrier.await();
                    if (pool.lock("xyz")) {
                        successCount.incrementAndGet();
                    } else {
                        failureCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    exceptionCount.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    stopLatch.countDown();
                }
            };

            new Thread(runnable).start();
            new Thread(runnable).start();

            Assert.assertTrue(stopLatch.await(2, TimeUnit.SECONDS));
            Assert.assertEquals(0, exceptionCount.get());
            Assert.assertEquals(1, successCount.get());
            Assert.assertEquals(1, failureCount.get());
        });
    }

    @Test
    public void testLockRaceAgainstGet() throws Exception {
        assertWithPool(pool -> {

            try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE).col("ts", ColumnType.DATE)) {
                CairoTestUtils.create(model);
            }

            for (int k = 0; k < 10000; k++) {
                // allocate 32 readers to get to the start race at edge of next entry
                int n = 64;
                TableReader[] readers = new TableReader[n];
                try {
                    for (int i = 0; i < n; i++) {
                        readers[i] = pool.get("x");
                        Assert.assertNotNull(readers[i]);
                    }

                    CyclicBarrier barrier = new CyclicBarrier(2);
                    CountDownLatch latch = new CountDownLatch(1);

                    new Thread(() -> {
                        try {
                            barrier.await();
                            boolean locked = pool.lock("x");
                            if (locked) {
                                pool.unlock("x");
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        } finally {
                            latch.countDown();
                        }
                    }).start();

                    barrier.await();
                    try {
                        pool.get("x").close();
                    } catch (EntryLockedException ignore) {
                    }
                    Assert.assertTrue(latch.await(2, TimeUnit.SECONDS));
                } finally {
                    for (int i = 0; i < n; i++) {
                        readers[i].close();
                    }
                }
            }
        });
    }

    @Test
    public void testLockUnlock() throws Exception {
        // create tables

        try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE).col("ts", ColumnType.DATE)) {
            CairoTestUtils.create(model);
        }

        try (TableModel model = new TableModel(configuration, "y", PartitionBy.NONE).col("ts", ColumnType.DATE)) {
            CairoTestUtils.create(model);
        }

        assertWithPool(pool -> {
            TableReader x, y;
            x = pool.get("x");
            Assert.assertNotNull(x);

            y = pool.get("y");
            Assert.assertNotNull(y);

            // expect lock to fail because we have "x" open
            Assert.assertFalse(pool.lock("x"));

            x.close();

            // expect lock to succeed after we closed "x"
            Assert.assertTrue(pool.lock("x"));

            // expect "x" to be physically closed
            Assert.assertFalse(x.isOpen());

            // "x" is locked, expect this to fail
            try {
                Assert.assertNull(pool.get("x"));
            } catch (EntryLockedException ignored) {
            }

            pool.unlock("x");

            x = pool.get("x");
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
            TableReader r1 = pool.get("u");
            TableReader r2 = pool.get("u");
            r1.close();
            Assert.assertFalse(pool.lock("u"));
            r2.close();
            Assert.assertTrue(pool.lock("u"));
            pool.unlock("u");
        });
    }

    @Test
    public void testReaderDoubleClose() throws Exception {
        assertWithPool(pool -> {

            class Listener implements PoolListener {
                private final ObjList<CharSequence> names = new ObjList<>();
                private final IntList events = new IntList();

                @Override
                public void onEvent(byte factoryType, long thread, CharSequence name, short event, short segment, short position) {
                    names.add(name == null ? "" : Chars.stringOf(name));
                    events.add(event);
                }
            }

            Listener listener = new Listener();
            pool.setPoolListener(listener);

            TableReader reader = pool.get("u");

            Assert.assertNotNull(reader);
            Assert.assertTrue(reader.isOpen());
            Assert.assertEquals(1, pool.getBusyCount());
            reader.close();
            Assert.assertEquals(0, pool.getBusyCount());
            reader.close();

            reader = pool.get("u");
            Assert.assertNotNull(reader);
            Assert.assertTrue(reader.isOpen());
            reader.close();

            Assert.assertEquals("[10,1,11,1]", listener.events.toString());
        });
    }

    @Test
    public void testSerialOpenClose() throws Exception {
        assertWithPool(pool -> {
            TableReader firstReader = null;
            for (int i = 0; i < 1000; i++) {
                try (TableReader reader = pool.get("u")) {
                    if (firstReader == null) {
                        firstReader = reader;
                    }
                    Assert.assertNotNull(reader);
                    Assert.assertSame(firstReader, reader);
                }
            }
        });
    }

    @Test
    public void testUnlockByAnotherThread() throws Exception {
        assertWithPool(pool -> {
            Assert.assertTrue(pool.lock("x"));
            AtomicInteger errors = new AtomicInteger();

            CountDownLatch latch = new CountDownLatch(1);
            new Thread(() -> {
                try {
                    try {
                        pool.unlock("x");
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getMessage(), "Not the lock owner");
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
                pool.get("x");
                Assert.fail();
            } catch (EntryLockedException ignore) {
            }
            pool.unlock("x");
        });
    }

    @Test
    public void testUnlockNonExisting() throws Exception {
        assertWithPool(pool -> {
            AtomicInteger counter = new AtomicInteger();
            pool.setPoolListener((factoryType, thread, name, event, segment, position) -> {
                if (event == PoolListener.EV_NOT_LOCKED) {
                    counter.incrementAndGet();
                }
            });
            pool.unlock("xyz");
            Assert.assertEquals(1, counter.get());
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

    private interface PoolAwareCode {
        void run(ReaderPool pool) throws Exception;
    }
}