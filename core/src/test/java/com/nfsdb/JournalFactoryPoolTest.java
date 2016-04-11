/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

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
