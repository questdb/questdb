/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cairo.wal.seq;

import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

public class SeqTxnTrackerTest {
    private static final Log LOG = LogFactory.getLog(SeqTxnTrackerTest.class);

    @Test
    public void testConcurrentInitTxns() throws Exception {
        LOG.info().$("testConcurrentInitTxns").$();
        TestUtils.assertMemoryLeak(() -> {
            final int threads = 4;

            final SeqTxnTracker tracker = new SeqTxnTracker();
            Assert.assertFalse(tracker.isInitialised());

            final CyclicBarrier startBarrier = new CyclicBarrier(threads);
            final SOCountDownLatch doneLatch = new SOCountDownLatch(threads);
            final AtomicInteger successes = new AtomicInteger();
            final AtomicInteger errors = new AtomicInteger();

            for (int i = 0; i < threads; i++) {
                int finalI = i;
                new Thread(() -> {
                    try {
                        startBarrier.await();
                        if (tracker.initTxns(1, 2 + finalI, false)) {
                            successes.incrementAndGet();
                        }
                        doneLatch.countDown();
                    } catch (Throwable th) {
                        th.printStackTrace();
                        errors.incrementAndGet();
                    }
                }).start();
            }

            doneLatch.await();

            Assert.assertEquals(0, errors.get());
            Assert.assertEquals(threads, successes.get());

            Assert.assertEquals(1, tracker.getWriterTxn());
            Assert.assertEquals(1 + threads, tracker.getSeqTxn());
            Assert.assertFalse(tracker.isSuspended());
        });
    }

    @Test
    public void testConcurrentNotifyOnCheck() throws Exception {
        LOG.info().$("testConcurrentNotifyOnCheck").$();
        TestUtils.assertMemoryLeak(() -> {
            final int threads = 4;

            final SeqTxnTracker tracker = new SeqTxnTracker();
            tracker.initTxns(1, 1, false);
            Assert.assertTrue(tracker.isInitialised());

            final CyclicBarrier startBarrier = new CyclicBarrier(threads);
            final SOCountDownLatch doneLatch = new SOCountDownLatch(threads);
            final AtomicInteger successes = new AtomicInteger();
            final AtomicInteger errors = new AtomicInteger();

            for (int i = 0; i < threads; i++) {
                int finalI = i;
                new Thread(() -> {
                    try {
                        startBarrier.await();
                        if (tracker.notifyOnCheck(2 + finalI)) {
                            successes.incrementAndGet();
                        }
                        doneLatch.countDown();
                    } catch (Throwable th) {
                        th.printStackTrace();
                        errors.incrementAndGet();
                    }
                }).start();
            }

            doneLatch.await();

            Assert.assertEquals(0, errors.get());
            Assert.assertEquals(threads, successes.get());

            Assert.assertEquals(1, tracker.getWriterTxn());
            Assert.assertEquals(1 + threads, tracker.getSeqTxn());
            Assert.assertFalse(tracker.isSuspended());
        });
    }

    @Test
    public void testConcurrentNotifyOnCommit() throws Exception {
        LOG.info().$("testConcurrentNotifyOnCommit").$();
        TestUtils.assertMemoryLeak(() -> {
            final int threads = 4;

            final SeqTxnTracker tracker = new SeqTxnTracker();
            tracker.initTxns(1, 1, false);
            Assert.assertTrue(tracker.isInitialised());

            final CyclicBarrier startBarrier = new CyclicBarrier(threads);
            final SOCountDownLatch doneLatch = new SOCountDownLatch(threads);
            final AtomicInteger successes = new AtomicInteger();
            final AtomicInteger errors = new AtomicInteger();

            for (int i = 0; i < threads; i++) {
                int finalI = i;
                new Thread(() -> {
                    try {
                        startBarrier.await();
                        if (tracker.notifyOnCommit(2 + finalI)) {
                            successes.incrementAndGet();
                        }
                        doneLatch.countDown();
                    } catch (Throwable th) {
                        th.printStackTrace();
                        errors.incrementAndGet();
                    }
                }).start();
            }

            doneLatch.await();

            Assert.assertEquals(0, errors.get());
            Assert.assertEquals(1, successes.get());

            Assert.assertEquals(1, tracker.getWriterTxn());
            Assert.assertEquals(1 + threads, tracker.getSeqTxn());
            Assert.assertFalse(tracker.isSuspended());
        });
    }
}
