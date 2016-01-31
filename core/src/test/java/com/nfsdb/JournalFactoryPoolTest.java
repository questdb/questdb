/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
                            for (int i = 0; i < 10; i++) {
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
}
