/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

package com.questdb;


import com.questdb.ex.JournalException;
import com.questdb.factory.JournalReaderFactory;
import com.questdb.factory.ReaderFactoryPool;
import com.questdb.factory.configuration.JournalConfiguration;
import com.questdb.misc.Files;
import com.questdb.model.Quote;
import com.questdb.test.tools.AbstractTest;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class ReaderFactoryPoolTest extends AbstractTest {

    @Test
    public void testFactoriesCanExceedCapacity() throws Exception {
        JournalConfiguration configuration = getReaderFactory().getConfiguration();
        try {
            final ReaderFactoryPool pool = new ReaderFactoryPool(configuration, 1);
            JournalReaderFactory factory1 = pool.get();
            JournalReaderFactory factory2 = pool.get();

            Assert.assertNotNull(factory1);
            Assert.assertNotNull(factory2);
            Assert.assertEquals(2, pool.getOpenCount());
            Assert.assertEquals(0, pool.getAvailableCount());

            factory1.close();
            Assert.assertEquals(1, pool.getOpenCount());
            Assert.assertEquals(0, pool.getAvailableCount());

            factory2.close();
            pool.close();
        } finally {
            Files.delete(configuration.getJournalBase());
        }
    }

    @Test
    public void testFactoriesReused() throws Exception {
        JournalConfiguration configuration = getReaderFactory().getConfiguration();
        try {
            final ReaderFactoryPool pool = new ReaderFactoryPool(configuration, 2);
            JournalReaderFactory factory = pool.get();
            factory.close();
            factory = pool.get();
            Assert.assertEquals(1, pool.getOpenCount());
            Assert.assertEquals(0, pool.getAvailableCount());

            pool.close();
            factory.close();
        } finally {
            Files.delete(configuration.getJournalBase());
        }
    }

    @Test
    public void testNonPartitionedReads() throws Exception {
        JournalConfiguration configuration = getReaderFactory().getConfiguration();
        try {
            try (final ReaderFactoryPool pool = new ReaderFactoryPool(configuration, 10)) {
                final int threadCount = 5;
                final int recordCount = 1000;

                try (JournalWriter<Quote> w = getWriterFactory().writer(Quote.class)) {
                    TestUtils.generateQuoteData(w, recordCount);
                }

                ExecutorService service = Executors.newCachedThreadPool();

                final CyclicBarrier barrier = new CyclicBarrier(threadCount);
                final CountDownLatch latch = new CountDownLatch(threadCount);

                final List<Exception> exceptions = new ArrayList<>();

                for (int i = 0; i < threadCount; i++) {
                    service.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                barrier.await();
                                for (int k = 0; k < 10; k++) {
                                    try (JournalReaderFactory rf = pool.get()) {
                                        try (Journal<Quote> r = rf.reader(Quote.class)) {
                                            Assert.assertEquals(recordCount, r.query().all().asResultSet().read().length);
                                        }
                                    } catch (InterruptedException | JournalException e) {
                                        exceptions.add(e);
                                        break;
                                    }
                                }
                                latch.countDown();
                            } catch (InterruptedException | BrokenBarrierException e) {
                                exceptions.add(e);
                            }
                        }
                    });
                }

                latch.await();
                Assert.assertEquals(0, exceptions.size());
            }
        } finally {
            Files.delete(configuration.getJournalBase());
        }
    }

    @Test
    public void testPoolReuseDoesNotCreateNew() throws Exception {
        JournalConfiguration configuration = getReaderFactory().getConfiguration();
        try {
            final ReaderFactoryPool pool = new ReaderFactoryPool(configuration, 10);
            pool.get().close();
            pool.get().close();
            pool.get().close();

            Assert.assertEquals(1, pool.getOpenCount());
            Assert.assertEquals(1, pool.getAvailableCount());

            pool.close();
        } finally {
            Files.delete(configuration.getJournalBase());
        }
    }

    @Test
    public void testPoolTumbleDry() throws Exception {
        JournalConfiguration configuration = getReaderFactory().getConfiguration();
        try {
            final int capacity = 10;
            final ReaderFactoryPool pool = new ReaderFactoryPool(configuration, capacity);
            final ExecutorService es = Executors.newCachedThreadPool();

            for (int k = 0; k < capacity; k++) {
                final int size = capacity * 2;
                es.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            JournalReaderFactory[] factories = new JournalReaderFactory[size];
                            for (int i = 0; i < size; i++) {
                                factories[i] = pool.get();
                            }
                            for (int i = size - 1; i >= 0; i--) {
                                factories[i].close();
                            }
                        } catch (InterruptedException e) {
                            Assert.fail();
                        }
                    }
                });
            }
            es.shutdown();
            es.awaitTermination(1, TimeUnit.MINUTES);
            Assert.assertTrue("Pool available threads does not match the capacity " + pool.getAvailableCount(),
                    pool.getAvailableCount() <= capacity);
            Assert.assertEquals("Open non-pooled journal exist", pool.getOpenCount(), pool.getAvailableCount());
            pool.close();
        } finally {
            Files.delete(configuration.getJournalBase());
        }
    }

    @Test(expected = InterruptedException.class)
    public void testThrowsAfterPoolClosed() throws Exception {
        JournalConfiguration configuration = getReaderFactory().getConfiguration();
        try {
            final ReaderFactoryPool pool = new ReaderFactoryPool(configuration, 1);
            pool.close();
            pool.get();
        } finally {
            Files.delete(configuration.getJournalBase());
        }
    }
}
