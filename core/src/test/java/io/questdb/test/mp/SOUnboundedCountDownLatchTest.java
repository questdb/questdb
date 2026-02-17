/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.test.mp;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.mp.SPSequence;
import io.questdb.std.Os;
import io.questdb.std.datetime.millitime.Dates;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

public class SOUnboundedCountDownLatchTest {

    private static final Log LOG = LogFactory.getLog(SOUnboundedCountDownLatchTest.class);

    @Test
    public void testUnboundedLatch() throws BrokenBarrierException, InterruptedException {
        SPSequence pubSeq = new SPSequence(1024);
        SCSequence subSeq = new SCSequence();

        // this latch does not require initial count. Instead, it is able to wait
        // for a specific target count. This is useful when it is hard to
        // determine work item count upfront.

        final SOUnboundedCountDownLatch latch = new SOUnboundedCountDownLatch();
        final AtomicInteger dc = new AtomicInteger();

        pubSeq.then(subSeq).then(pubSeq);

        int count = 1_000_000;

        doTest(pubSeq, subSeq, latch, dc, count, 0);

        LOG.info().$("waiting on [count=").$(count).$(']').$();
        latch.await(count);

        LOG.info().$("section 1 done").$();

        Assert.assertEquals(count, dc.get());

        dc.set(0);
        latch.reset();

        doTest(pubSeq, subSeq, latch, dc, count, count);

        LOG.info().$("waiting on [count=").$(count).$(']').$();
        latch.await(count);

        LOG.info().$("section 2 done").$();

        Assert.assertEquals(count, dc.get());
    }

    private void doTest(
            SPSequence pubSeq,
            SCSequence subSeq,
            SOUnboundedCountDownLatch latch,
            AtomicInteger dc,
            int count,
            int s
    ) throws BrokenBarrierException, InterruptedException {
        final CyclicBarrier barrier = new CyclicBarrier(2);
        AtomicInteger errors = new AtomicInteger();
        LOG.info().$("starting thread").$();
        new Thread() {
            int doneCount = 0;
            long time = System.currentTimeMillis();

            @Override
            public void run() {
                try {
                    LOG.info().$("thread is running").$();
                    barrier.await();
                    LOG.info().$("thread is away").$();
                    long last = s - 1;
                    while (doneCount < count) {
                        long c = subSeq.next();
                        if (c > -1) {
                            subSeq.done(c);
                            doneCount++;
                            dc.incrementAndGet();
                            latch.countDown();
                            Assert.assertEquals(last + 1, c);
                            last = c;
                        } else if (c == -1) {
                            Os.pause();
                        }
                        if (System.currentTimeMillis() - time > Dates.MINUTE_MILLIS) {
                            LOG.error()
                                    .$("so_latch_state [doneCount=").$(doneCount)
                                    .$(", count=").$(count)
                                    .$(", seq.current").$(subSeq.current())
                                    .$(", seq.cache=").$(pubSeq.getCached())
                                    .$(", latch.count=").$(latch.getCount())
                                    .$(']').$();
                            time = System.currentTimeMillis();
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                }
                LOG.info().$("thread ended").$();
            }
        }.start();

        LOG.info().$("about to start publishing").$();
        barrier.await();
        LOG.info().$("publishing").$();
        for (int i = 0; i < count; ) {
            Assert.assertEquals(0, errors.get());
            long c = pubSeq.next();
            if (c > -1) {
                i++;
                pubSeq.done(c);
            } else if (c == -1) {
                Os.pause();
            }
        }
        LOG.info().$("all published").$();
    }
}
