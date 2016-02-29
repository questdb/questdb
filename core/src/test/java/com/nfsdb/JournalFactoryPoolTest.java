/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb;


import com.nfsdb.ex.JournalException;
import com.nfsdb.factory.JournalFactoryPool;
import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.factory.configuration.JournalConfiguration;
import com.nfsdb.misc.Files;
import com.nfsdb.model.Quote;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class JournalFactoryPoolTest extends AbstractTest {

    @Test
    public void testFactoriesCanExceedCapacity() throws Exception {
        JournalConfiguration configuration = factory.getConfiguration();
        try {
            final JournalFactoryPool pool = new JournalFactoryPool(configuration, 1);
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
        JournalConfiguration configuration = factory.getConfiguration();
        try {
            final JournalFactoryPool pool = new JournalFactoryPool(configuration, 2);
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
        JournalConfiguration configuration = factory.getConfiguration();
        try {
            final JournalFactoryPool pool = new JournalFactoryPool(configuration, 10);
            final int threadCount = 5;
            final int recordCount = 1000;

            JournalWriter<Quote> w = factory.writer(Quote.class);
            TestUtils.generateQuoteData(w, recordCount);
            w.close();

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
            pool.close();
        } finally {
            Files.delete(configuration.getJournalBase());
        }
    }

    @Test
    public void testPoolReuseDoesNotCreateNew() throws Exception {
        JournalConfiguration configuration = factory.getConfiguration();
        try {
            final JournalFactoryPool pool = new JournalFactoryPool(configuration, 10);
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
        JournalConfiguration configuration = factory.getConfiguration();
        try {
            final int capacity = 10;
            final JournalFactoryPool pool = new JournalFactoryPool(configuration, capacity);
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
        JournalConfiguration configuration = factory.getConfiguration();
        try {
            final JournalFactoryPool pool = new JournalFactoryPool(configuration, 1);
            pool.close();
            pool.get();
        } finally {
            Files.delete(configuration.getJournalBase());
        }
    }
}
