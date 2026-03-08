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
import io.questdb.mp.*;
import io.questdb.std.*;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class ConcurrentTest {
    private final static Log LOG = LogFactory.getLog(ConcurrentTest.class);

    @Test
    public void testConcurrentFanOutAnd() {
        int cycle = 1024;
        SPSequence pubSeq = new SPSequence(cycle);
        FanOut fout = new FanOut();
        pubSeq.then(fout).then(pubSeq);

        int threads = 2;
        CyclicBarrier start = new CyclicBarrier(threads);
        SOCountDownLatch latch = new SOCountDownLatch(threads);
        int iterations = 30;

        AtomicInteger doneCount = new AtomicInteger();
        for (int i = 0; i < threads; i++) {
            new Thread(() -> {
                try {
                    start.await();
                    for (int j = 0; j < iterations; j++) {
                        SCSequence consumer = new SCSequence();
                        FanOut fout2 = fout.and(consumer);
                        fout2.remove(consumer);
                    }
                    doneCount.addAndGet(iterations);
                } catch (InterruptedException | BrokenBarrierException e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        latch.await();
        Assert.assertEquals(threads * iterations, doneCount.get());
    }

    @Test
    public void testConcurrentFanOutAsyncOffloadPattern() {
        // This test aims to reproduce a simplified variant of async offload inter-thread communication pattern.


        final int capacity = 2;
        final int collectorThreads = 2;
        final int reducerThreads = 2;
        final int itemsToDispatch = 8;
        final int iterations = 100;

        try (final RingQueue<Item> queue = new RingQueue<>(Item::new, capacity)) {
            final MPSequence reducePubSeq = new MPSequence(capacity);
            final MCSequence reduceSubSeq = new MCSequence(capacity);
            final FanOut collectFanOut = new FanOut();

            reducePubSeq.then(reduceSubSeq).then(collectFanOut).then(reducePubSeq);

            final CyclicBarrier start = new CyclicBarrier(collectorThreads + reducerThreads);
            final SOCountDownLatch latch = new SOCountDownLatch(collectorThreads + reducerThreads);
            final AtomicInteger doneCollectors = new AtomicInteger();
            final AtomicInteger anomalies = new AtomicInteger();

            // Start reducer threads.
            for (int i = 0; i < reducerThreads; i++) {
                new Thread(() -> {
                    try {
                        start.await();

                        while (doneCollectors.get() != collectorThreads) {
                            long cursor = reduceSubSeq.next();
                            if (cursor > -1) {
                                reduceSubSeq.done(cursor);
                            } else {
                                Os.pause();
                            }
                        }
                    } catch (InterruptedException | BrokenBarrierException e) {
                        e.printStackTrace();
                        anomalies.incrementAndGet();
                    } finally {
                        latch.countDown();
                    }
                }).start();
            }

            // Start collector threads.
            for (int i = 0; i < collectorThreads; i++) {
                final int threadI = i;
                new Thread(() -> {
                    final SCSequence collectSubSeq = new SCSequence();

                    try {
                        start.await();

                        for (int j = 0; j < iterations; j++) {
                            collectFanOut.and(collectSubSeq);

                            int collected = 0;

                            // Dispatch all items.
                            for (int k = 0; k < itemsToDispatch; k++) {
                                while (true) {
                                    long dispatchCursor = reducePubSeq.next();
                                    if (dispatchCursor > -1) {
                                        queue.get(dispatchCursor).owner = threadI;
                                        reducePubSeq.done(dispatchCursor);
                                        break;
                                    } else if (dispatchCursor == -1) {
                                        // Collect as many items as we can.
                                        long collectCursor;
                                        while ((collectCursor = collectSubSeq.next()) > -1) {
                                            if (queue.get(collectCursor).owner == threadI) {
                                                queue.get(collectCursor).owner = -1;
                                                collected++;
                                            }
                                            collectSubSeq.done(collectCursor);
                                        }
                                        Os.pause();
                                    } else {
                                        Os.pause();
                                    }
                                }
                            }

                            // Await for all remaining items.
                            while (collected != itemsToDispatch) {
                                long collectCursor = collectSubSeq.next();
                                if (collectCursor > -1) {
                                    if (queue.get(collectCursor).owner == threadI) {
                                        queue.get(collectCursor).owner = -1;
                                        collected++;
                                    }
                                    collectSubSeq.done(collectCursor);
                                } else {
                                    Os.pause();
                                }
                            }

                            collectFanOut.remove(collectSubSeq);
                        }
                    } catch (InterruptedException | BrokenBarrierException e) {
                        e.printStackTrace();
                        anomalies.incrementAndGet();
                    } finally {
                        doneCollectors.incrementAndGet();
                        latch.countDown();
                    }
                }).start();
            }

            if (!latch.await(TimeUnit.SECONDS.toNanos(60))) {
                Assert.fail("Wait limit exceeded");
            }
            Assert.assertEquals(0, anomalies.get());
        }
    }

    @Test
    public void testDoneMemBarrier() throws InterruptedException {
        int cycle = 128;
        try (RingQueue<TwoLongMsg> pingQueue = new RingQueue<>(TwoLongMsg::new, cycle)) {
            MCSequence sub = new MCSequence(cycle);
            MPSequence pub = new MPSequence(cycle);

            pub.then(sub).then(pub);
            final int total = 100_000;
            int subThreads = 4;

            CyclicBarrier barrier = new CyclicBarrier(subThreads + 1);

            Thread pubTh = new Thread(() -> {
                TestUtils.await(barrier);

                for (int i = 0; i < total; i++) {
                    long seq;
                    while (true) {
                        seq = pub.next();
                        if (seq > -1) {
                            TwoLongMsg msg = pingQueue.get(seq);

                            msg.f4 = i + 3;
                            msg.f3 = i + 2;
                            msg.f2 = i + 1;
                            msg.f1 = i;

                            pub.done(seq);

                            break;
                        }
                    }
                }
            });
            pubTh.start();

            AtomicLong anomalies = new AtomicLong();
            ObjList<Thread> threads = new ObjList<>();
            AtomicBoolean done = new AtomicBoolean(false);

            for (int th = 0; th < subThreads; th++) {
                Thread subTh = new Thread(() -> {
                    TestUtils.await(barrier);

                    for (int i = 0; i < total; i++) {
                        long seq;
                        while (!done.get()) {
                            seq = sub.next();
                            if (seq > -1) {
                                TwoLongMsg msg = pingQueue.get(seq);
                                long f1 = msg.f1;
                                long f2 = msg.f2;
                                long f3 = msg.f3;
                                long f4 = msg.f4;

                                sub.done(seq);

                                if (f2 != f1 + 1 ||
                                        f3 != f1 + 2 ||
                                        f4 != f1 + 3) {
                                    anomalies.incrementAndGet();
                                    return;
                                }
                                break;
                            }
                        }
                    }
                });
                subTh.start();
                threads.add(subTh);
            }

            pubTh.join();
            done.set(true);
            for (int i = 0; i < threads.size(); i++) {
                Thread subTh = threads.get(i);
                subTh.join();
            }

            Assert.assertEquals("Anomalies detected", 0, anomalies.get());
        }
    }

    @Test
    public void testFanOutChain() {
        LOG.info().$("testFanOutChain").$();
        int cycle = 1024;
        Sequence a = new SPSequence(cycle);
        Sequence b = new SCSequence();
        Sequence c = new SCSequence();
        Sequence d = new SCSequence();
        Sequence e = new SCSequence();

        a.then(
                FanOut
                        .to(
                                FanOut.to(d).and(e)
                        )
                        .and(
                                b.then(c)
                        )
        ).then(a);
    }

    @Test
    public void testFanOutDoesNotProcessQueueFromStart() {
        // Main thread is going to publish K events
        // and then give green light to the relay thread,
        // which should not be processing first K-1 events
        //
        // There is also a generic consumer thread that's processing
        // all messages

        try (final RingQueue<LongMsg> queue = new RingQueue<>(LongMsg::new, 64)) {
            final SPSequence pubSeq = new SPSequence(queue.getCycle());
            final FanOut subFo = new FanOut();
            pubSeq.then(subFo).then(pubSeq);

            final SOCountDownLatch haltLatch = new SOCountDownLatch(2);
            int N = 20;
            int K = 12;

            // The relay sequence is created by publisher to
            // make test deterministic. To pass this sequence to
            // the relay thread we use the atomic reference and countdown latch
            final AtomicReference<SCSequence> relaySubSeq = new AtomicReference<>();
            final SOCountDownLatch relayLatch = new SOCountDownLatch(1);

            // this barrier is to make sure threads are ready before publisher starts
            final SOCountDownLatch threadsRunBarrier = new SOCountDownLatch(2);

            new Thread(() -> {
                final SCSequence subSeq = new SCSequence();
                subFo.and(subSeq);

                // thread is ready to consume
                threadsRunBarrier.countDown();

                int count = N;
                while (count > 0) {
                    long cursor = subSeq.nextBully();
                    subSeq.done(cursor);
                    count--;
                }
                subFo.remove(subSeq);
                haltLatch.countDown();
            }).start();

            // indicator showing relay thread did not process messages before K
            final AtomicBoolean relayThreadSuccess = new AtomicBoolean();

            new Thread(() -> {

                // thread is ready to wait :)
                threadsRunBarrier.countDown();

                relayLatch.await();

                final SCSequence subSeq = relaySubSeq.get();
                int count = N - K;
                boolean success = true;
                while (count > 0) {
                    long cursor = subSeq.nextBully();
                    if (success) {
                        success = queue.get(cursor).correlationId >= K;
                    }
                    subSeq.done(cursor);
                    count--;
                }
                relayThreadSuccess.set(success);
                subFo.remove(subSeq);
                haltLatch.countDown();
            }).start();

            // wait for threads to get ready
            threadsRunBarrier.await();

            // publish
            for (int i = 0; i < N; i++) {
                if (i == K) {
                    final SCSequence sub = new SCSequence();
                    subFo.and(sub);
                    relaySubSeq.set(sub);
                    relayLatch.countDown();
                }

                long cursor = pubSeq.nextBully();
                queue.get(cursor).correlationId = i;
                pubSeq.done(cursor);
            }

            haltLatch.await();
            Assert.assertTrue(relayThreadSuccess.get());
        }
    }

    @Test
    public void testFanOutPingPong() {
        final int threads = 2;
        final int iterations = 30;

        // Requests inbox
        try (final RingQueue<LongMsg> pingQueue = new RingQueue<>(LongMsg::new, threads)) {
            final MPSequence pingPubSeq = new MPSequence(threads);
            final FanOut pingSubFo = new FanOut();
            pingPubSeq.then(pingSubFo).then(pingPubSeq);

            // Request inbox hook for processor
            final SCSequence pingSubSeq = new SCSequence();
            pingSubFo.and(pingSubSeq);

            // Response outbox
            try (final RingQueue<LongMsg> pongQueue = new RingQueue<>(LongMsg::new, threads)) {
                final SPSequence pongPubSeq = new SPSequence(threads);
                final FanOut pongSubFo = new FanOut();
                pongPubSeq.then(pongSubFo).then(pongPubSeq);

                CyclicBarrier start = new CyclicBarrier(threads);
                SOCountDownLatch latch = new SOCountDownLatch(threads + 1);
                AtomicLong doneCount = new AtomicLong();
                AtomicLong idGen = new AtomicLong();

                // Processor
                new Thread(() -> {
                    try {
                        int i = 0;
                        while (i < threads * iterations) {
                            long seq = pingSubSeq.next();
                            if (seq > -1) {
                                // Get next request
                                LongMsg msg = pingQueue.get(seq);
                                long requestId = msg.correlationId;
                                pingSubSeq.done(seq);

                                // Uncomment this and the following lines when in need for debugging
                                // LOG.info().$("ping received ").$(requestId).$();

                                long resp;
                                while ((resp = pongPubSeq.next()) < 0) {
                                    Os.pause();
                                }
                                pongQueue.get(resp).correlationId = requestId;
                                pongPubSeq.done(resp);

                                // LOG.info().$("pong sent ").$(requestId).$();
                                i++;
                            } else {
                                Os.pause();
                            }
                        }
                        doneCount.incrementAndGet();
                    } finally {
                        latch.countDown();
                    }
                }).start();

                // Request threads
                for (int th = 0; th < threads; th++) {
                    new Thread(() -> {
                        try {
                            SCSequence pongSubSeq = new SCSequence();
                            start.await();
                            for (int i = 0; i < iterations; i++) {
                                // Put local response sequence into response FanOut
                                pongSubFo.and(pongSubSeq);

                                // Send next request
                                long requestId = idGen.incrementAndGet();
                                long reqSeq;
                                while ((reqSeq = pingPubSeq.next()) < 0) {
                                    Os.pause();
                                }
                                pingQueue.get(reqSeq).correlationId = requestId;
                                pingPubSeq.done(reqSeq);
                                // LOG.info().$(threadId).$(", ping sent ").$(requestId).$();

                                // Wait for response
                                long responseId, respCursor;
                                do {
                                    while ((respCursor = pongSubSeq.next()) < 0) {
                                        Os.pause();
                                    }
                                    responseId = pongQueue.get(respCursor).correlationId;
                                    pongSubSeq.done(respCursor);
                                } while (responseId != requestId);

                                // LOG.info().$(threadId).$(", pong received ").$(requestId).$();

                                // Remove local response sequence from response FanOut
                                pongSubFo.remove(pongSubSeq);
                            }
                            doneCount.incrementAndGet();
                        } catch (BrokenBarrierException | InterruptedException e) {
                            e.printStackTrace();
                        } finally {
                            latch.countDown();
                        }
                    }).start();
                }

                latch.await();
                Assert.assertEquals(threads + 1, doneCount.get());
            }
        }
    }

    @Test
    public void testFanOutPingPongStableSequences() {
        final int threads = 2;
        final int iterations = 30;
        final int cycle = Numbers.ceilPow2(threads * iterations);

        // Requests inbox
        try (RingQueue<LongMsg> pingQueue = new RingQueue<>(LongMsg::new, cycle)) {
            MPSequence pingPubSeq = new MPSequence(cycle);
            FanOut pingSubFo = new FanOut();
            pingPubSeq.then(pingSubFo).then(pingPubSeq);

            // Request inbox hook for processor
            SCSequence pingSubSeq = new SCSequence();
            pingSubFo.and(pingSubSeq);

            // Response outbox
            try (RingQueue<LongMsg> pongQueue = new RingQueue<>(LongMsg::new, cycle)) {
                SPSequence pongPubSeq = new SPSequence(threads);
                FanOut pongSubFo = new FanOut();
                pongPubSeq.then(pongSubFo).then(pongPubSeq);

                CyclicBarrier start = new CyclicBarrier(threads);
                SOCountDownLatch latch = new SOCountDownLatch(threads + 1);
                AtomicLong doneCount = new AtomicLong();
                AtomicLong idGen = new AtomicLong();

                // Processor
                new Thread(() -> {
                    try {
                        LongList pingPong = new LongList();
                        int i = 0;
                        while (i < threads * iterations) {
                            long pingCursor = pingSubSeq.next();
                            if (pingCursor > -1) {
                                // Get next request
                                LongMsg msg = pingQueue.get(pingCursor);
                                long requestId = msg.correlationId;
                                pingSubSeq.done(pingCursor);

                                pingPong.add(pingCursor);

                                // Uncomment this and the following lines when in need for debugging
                                // System.out.println("* ping " + requestId);

                                long pongCursor;
                                while ((pongCursor = pongPubSeq.next()) < 0) {
                                    Os.pause();
                                }
                                pongQueue.get(pongCursor).correlationId = requestId;
                                pongPubSeq.done(pongCursor);
                                pingPong.add(pongCursor);

                                // System.out.println("* pong " + requestId);
                                i++;
                            } else {
                                Os.pause();
                            }
                        }
                        doneCount.incrementAndGet();
                    } finally {
                        latch.countDown();
                    }
                }).start();

                final SCSequence[] sequences = new SCSequence[threads];
                for (int i = 0; i < threads; i++) {
                    SCSequence pongSubSeq = new SCSequence();
                    pongSubFo.and(pongSubSeq);
                    sequences[i] = pongSubSeq;
                }

                // Request threads
                for (int th = 0; th < threads; th++) {
                    final int threadId = th;
                    new Thread(() -> {
                        final LongList pingPong = new LongList();
                        try {
                            start.await();

                            final SCSequence pongSubSeq = sequences[threadId];

                            for (int i = 0; i < iterations; i++) {
                                // Put local response sequence into response FanOut
                                // System.out.println("thread:" + threadId + ", added at " + pingPubSeq.value);

                                // Send next request
                                long requestId = idGen.incrementAndGet();
                                long pingCursor;
                                while ((pingCursor = pingPubSeq.next()) < 0) {
                                    Os.pause();
                                }
                                pingQueue.get(pingCursor).correlationId = requestId;
                                pingPubSeq.done(pingCursor);
                                pingPong.add(pingCursor);

                                // System.out.println("thread:" + threadId + ", ask: " + requestId);

                                // Wait for response
                                long responseId, pongCursor;
                                do {
                                    while ((pongCursor = pongSubSeq.next()) < 0) {
                                        Os.pause();
                                    }
                                    pingPong.add(pongCursor);
                                    responseId = pongQueue.get(pongCursor).correlationId;
                                    pongSubSeq.done(pongCursor);
                                    pingPong.add(pongCursor);
                                    // System.out.println("thread:" + threadId + ", ping: " + responseId + ", expected: " + requestId);
                                } while (responseId != requestId);

                                // System.out.println("thread " + threadId + ", pong " + requestId);
                                // Remove local response sequence from response FanOut
                            }
                            pongSubFo.remove(pongSubSeq);
                            doneCount.incrementAndGet();
                        } catch (BrokenBarrierException | InterruptedException e) {
                            e.printStackTrace();
                        } finally {
                            latch.countDown();
                        }
                    }).start();
                }

                latch.await();
                Assert.assertEquals(threads + 1, doneCount.get());
            }
        }
    }

    @Test
    public void testManyHybrid() throws Exception {
        LOG.info().$("testManyHybrid").$();
        int threadCount = 4;
        int cycle = 4;
        int size = 1024 * cycle;
        RingQueue<Event> queue = new RingQueue<>(Event.FACTORY, cycle);
        MPSequence pubSeq = new MPSequence(cycle);
        MCSequence subSeq = new MCSequence(cycle);
        pubSeq.then(subSeq).then(pubSeq);

        CyclicBarrier barrier = new CyclicBarrier(threadCount + 1);
        CountDownLatch latch = new CountDownLatch(threadCount);

        BusyProducerConsumer[] threads = new BusyProducerConsumer[threadCount];
        for (int i = 0; i < threadCount; i++) {
            threads[i] = new BusyProducerConsumer(size, pubSeq, subSeq, queue, barrier, latch);
            threads[i].start();
        }

        barrier.await();
        latch.await();

        int totalProduced = 0;
        int totalConsumed = 0;
        for (int i = 0; i < threadCount; i++) {
            totalProduced += threads[i].produced;
            totalConsumed += threads[i].consumed;
        }
        Assert.assertEquals(threadCount * size, totalProduced);
        Assert.assertEquals(threadCount * size, totalConsumed);
    }

    @Test
    public void testManyToManyBusy() throws Exception {
        LOG.info().$("testManyToManyBusy").$();
        int cycle = 128;
        int size = 1024 * cycle;
        RingQueue<Event> queue = new RingQueue<>(Event.FACTORY, cycle);
        MPSequence pubSeq = new MPSequence(cycle);
        MCSequence subSeq = new MCSequence(cycle);
        pubSeq.then(subSeq).then(pubSeq);

        CyclicBarrier barrier = new CyclicBarrier(5);
        CountDownLatch latch = new CountDownLatch(4);

        BusyProducer[] producers = new BusyProducer[2];
        producers[0] = new BusyProducer(size / 2, pubSeq, queue, barrier, latch);
        producers[1] = new BusyProducer(size / 2, pubSeq, queue, barrier, latch);

        producers[0].start();
        producers[1].start();

        BusyConsumer[] consumers = new BusyConsumer[2];
        consumers[0] = new BusyConsumer(size, subSeq, queue, barrier, latch);
        consumers[1] = new BusyConsumer(size, subSeq, queue, barrier, latch);

        consumers[0].start();
        consumers[1].start();

        barrier.await();
        latch.await();

        int[] buf = new int[size];
        System.arraycopy(consumers[0].buf, 0, buf, 0, consumers[0].finalIndex);
        System.arraycopy(consumers[1].buf, 0, buf, consumers[0].finalIndex, consumers[1].finalIndex);
        Arrays.sort(buf);
        for (int i = 0; i < buf.length / 2; i++) {
            Assert.assertEquals(i, buf[2 * i]);
            Assert.assertEquals(i, buf[2 * i + 1]);
        }
    }

    /**
     * <pre>
     *                    +--------+
     *               +--->| worker |
     *     +-----+   |    +--------+
     *     | pub |-->|
     *     +-----+   |    +--------+
     *               +--->| worker |
     *                    +--------+
     * </pre>
     */
    @Test
    public void testOneToManyBusy() throws Exception {
        LOG.info().$("testOneToManyBusy").$();
        int cycle = 1024;
        int size = 1024 * cycle;
        RingQueue<Event> queue = new RingQueue<>(Event.FACTORY, cycle);
        SPSequence pubSeq = new SPSequence(cycle);
        MCSequence subSeq = new MCSequence(cycle);
        pubSeq.then(subSeq).then(pubSeq);

        CyclicBarrier barrier = new CyclicBarrier(3);
        CountDownLatch latch = new CountDownLatch(2);

        BusyConsumer[] consumers = new BusyConsumer[2];
        consumers[0] = new BusyConsumer(size, subSeq, queue, barrier, latch);
        consumers[1] = new BusyConsumer(size, subSeq, queue, barrier, latch);

        consumers[0].start();
        consumers[1].start();

        barrier.await();
        int i = 0;
        while (true) {
            long cursor = pubSeq.next();
            if (cursor < 0) {
                continue;
            }
            queue.get(cursor).value = i++;
            pubSeq.done(cursor);

            if (i == size) {
                break;
            }
        }

        publishEOE(queue, pubSeq);
        publishEOE(queue, pubSeq);

        latch.await();

        int[] buf = new int[size];
        System.arraycopy(consumers[0].buf, 0, buf, 0, consumers[0].finalIndex);
        System.arraycopy(consumers[1].buf, 0, buf, consumers[0].finalIndex, consumers[1].finalIndex);
        Arrays.sort(buf);
        for (i = 0; i < buf.length; i++) {
            Assert.assertEquals(i, buf[i]);
        }
    }

    @Test
    public void testOneToManyWaiting() throws Exception {
        int cycle = 1024;
        int size = 1024 * cycle;
        RingQueue<Event> queue = new RingQueue<>(Event.FACTORY, cycle);
        SPSequence pubSeq = new SPSequence(cycle);
        MCSequence subSeq = new MCSequence(cycle, new YieldingWaitStrategy());
        pubSeq.then(subSeq).then(pubSeq);

        CyclicBarrier barrier = new CyclicBarrier(3);
        SOCountDownLatch latch = new SOCountDownLatch(2);

        WaitingConsumer[] consumers = new WaitingConsumer[2];
        consumers[0] = new WaitingConsumer(size, subSeq, queue, barrier, latch);
        consumers[1] = new WaitingConsumer(size, subSeq, queue, barrier, latch);

        consumers[0].start();
        consumers[1].start();

        barrier.await();
        int i = 0;
        do {
            long cursor = pubSeq.nextBully();
            queue.get(cursor).value = i++;
            pubSeq.done(cursor);
        } while (i != size);

        publishEOE(queue, pubSeq);
        publishEOE(queue, pubSeq);

        latch.await();

        int[] buf = new int[size];
        System.arraycopy(consumers[0].buf, 0, buf, 0, consumers[0].finalIndex);
        System.arraycopy(consumers[1].buf, 0, buf, consumers[0].finalIndex, consumers[1].finalIndex);
        Arrays.sort(buf);
        for (i = 0; i < buf.length; i++) {
            Assert.assertEquals(i, buf[i]);
        }
    }

    @Test
    public void testOneToOneBatched() throws BrokenBarrierException, InterruptedException {
        final int cycle = 1024;
        final int size = 1024 * cycle;
        try (final RingQueue<Event> queue = new RingQueue<>(Event.FACTORY, cycle)) {
            final SPSequence pubSeq = new SPSequence(cycle);
            final SCSequence subSeq = new SCSequence();
            pubSeq.then(subSeq).then(pubSeq);

            CyclicBarrier barrier = new CyclicBarrier(2);

            new Thread(() -> {
                try {
                    barrier.await();
                    Rnd rnd = new Rnd();
                    for (int i = 0; i < size; ) {
                        long cursor = pubSeq.next();
                        if (cursor > -1) {
                            long available = pubSeq.available();
                            while (cursor < available && i < size) {
                                Event event = queue.get(cursor++);
                                event.value = rnd.nextInt();
                                i++;
                            }
                            pubSeq.done(cursor - 1);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();

            barrier.await();
            int consumed = 0;
            final Rnd rnd2 = new Rnd();
            while (consumed < size) {
                long cursor = subSeq.next();
                if (cursor > -1) {
                    long available = subSeq.available();
                    while (cursor < available) {
                        Assert.assertEquals(rnd2.nextInt(), queue.get(cursor++).value);
                        consumed++;
                    }
                    subSeq.done(available - 1);
                }
            }
        }
    }

    @Test
    public void testOneToOneBusy() throws Exception {
        LOG.info().$("testOneToOneBusy").$();
        int cycle = 1024;
        int size = 1024 * cycle;
        RingQueue<Event> queue = new RingQueue<>(Event.FACTORY, cycle);
        Sequence pubSeq = new SPSequence(cycle);
        Sequence subSeq = new SCSequence(new YieldingWaitStrategy());
        pubSeq.then(subSeq).then(pubSeq);

        CyclicBarrier barrier = new CyclicBarrier(2);
        CountDownLatch latch = new CountDownLatch(1);

        BusyConsumer consumer = new BusyConsumer(size, subSeq, queue, barrier, latch);
        consumer.start();

        barrier.await();
        int i = 0;
        while (true) {
            long cursor = pubSeq.next();
            if (cursor < 0) {
                continue;
            }
            queue.get(cursor).value = i++;
            pubSeq.done(cursor);
            if (i == size) {
                break;
            }
        }

        publishEOE(queue, pubSeq);

        latch.await();

        int[] buf = consumer.buf;
        for (i = 0; i < buf.length; i++) {
            Assert.assertEquals(i, buf[i]);
        }
    }

    @Test
    public void testOneToOneWaiting() throws Exception {
        LOG.info().$("testOneToOneWaiting").$();
        int cycle = 1024;
        int size = 1024 * cycle;
        RingQueue<Event> queue = new RingQueue<>(Event.FACTORY, cycle);
        Sequence pubSeq = new SPSequence(cycle);
        Sequence subSeq = new SCSequence(new YieldingWaitStrategy());
        pubSeq.then(subSeq).then(pubSeq);

        CyclicBarrier barrier = new CyclicBarrier(2);
        SOCountDownLatch latch = new SOCountDownLatch(1);

        WaitingConsumer consumer = new WaitingConsumer(size, subSeq, queue, barrier, latch);
        consumer.start();

        barrier.await();
        int i = 0;
        do {
            long cursor = pubSeq.nextBully();
            queue.get(cursor).value = i++;
            pubSeq.done(cursor);
        } while (i != size);

        publishEOE(queue, pubSeq);

        latch.await();

        int[] buf = consumer.buf;
        for (i = 0; i < buf.length; i++) {
            Assert.assertEquals(i, buf[i]);
        }
    }

    @Test
    public void testOneToParallelMany() throws Exception {
        LOG.info().$("testOneToParallelMany").$();
        int cycle = 1024;
        int size = 1024 * cycle;
        RingQueue<Event> queue = new RingQueue<>(Event.FACTORY, cycle);
        SPSequence pubSeq = new SPSequence(cycle);
        Sequence sub1 = new SCSequence();
        Sequence sub2 = new SCSequence();
        pubSeq.then(FanOut.to(sub1).and(sub2)).then(pubSeq);

        CyclicBarrier barrier = new CyclicBarrier(3);
        CountDownLatch latch = new CountDownLatch(2);

        BusyConsumer[] consumers = new BusyConsumer[2];
        consumers[0] = new BusyConsumer(size, sub1, queue, barrier, latch);
        consumers[1] = new BusyConsumer(size, sub2, queue, barrier, latch);

        consumers[0].start();
        consumers[1].start();

        barrier.await();
        int i = 0;
        while (true) {
            long cursor = pubSeq.next();
            if (cursor < 0) {
                Os.pause();
                continue;
            }
            queue.get(cursor).value = i++;
            pubSeq.done(cursor);

            if (i == size) {
                break;
            }
        }

        publishEOE(queue, pubSeq);
        publishEOE(queue, pubSeq);

        latch.await();

        for (int k = 0; k < 2; k++) {
            for (i = 0; i < consumers[k].buf.length; i++) {
                Assert.assertEquals(i, consumers[k].buf[i]);
            }
        }
    }

    static void publishEOE(RingQueue<Event> queue, Sequence sequence) {
        long cursor = sequence.nextBully();
        queue.get(cursor).value = Integer.MIN_VALUE;
        sequence.done(cursor);
    }

    private static class BusyConsumer extends Thread {
        private final CyclicBarrier barrier;
        private final int[] buf;
        private final CountDownLatch doneLatch;
        private final RingQueue<Event> queue;
        private final Sequence sequence;
        private volatile int finalIndex = 0;

        BusyConsumer(int cycle, Sequence sequence, RingQueue<Event> queue, CyclicBarrier barrier, CountDownLatch doneLatch) {
            this.sequence = sequence;
            this.buf = new int[cycle];
            this.queue = queue;
            this.barrier = barrier;
            this.doneLatch = doneLatch;
        }

        @Override
        public void run() {
            try {
                barrier.await();
                int p = 0;
                while (true) {
                    long cursor = sequence.next();
                    if (cursor < 0) {
                        Os.pause();
                        continue;
                    }
                    int v = queue.get(cursor).value;
                    sequence.done(cursor);

                    if (v == Integer.MIN_VALUE) {
                        break;
                    }
                    buf[p++] = v;
                }

                finalIndex = p;
                doneLatch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static class BusyProducer extends Thread {
        private final CyclicBarrier barrier;
        private final int cycle;
        private final CountDownLatch doneLatch;
        private final RingQueue<Event> queue;
        private final Sequence sequence;

        BusyProducer(int cycle, Sequence sequence, RingQueue<Event> queue, CyclicBarrier barrier, CountDownLatch doneLatch) {
            this.sequence = sequence;
            this.cycle = cycle;
            this.queue = queue;
            this.barrier = barrier;
            this.doneLatch = doneLatch;
        }

        @Override
        public void run() {
            try {
                barrier.await();
                int p = 0;
                while (p < cycle) {
                    long cursor = sequence.next();
                    if (cursor < 0) {
                        Os.pause();
                        continue;
                    }
                    queue.get(cursor).value = ++p == cycle ? Integer.MIN_VALUE : p;
                    sequence.done(cursor);
                }

                doneLatch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static class BusyProducerConsumer extends Thread {
        private final CyclicBarrier barrier;
        private final Sequence consumerSequence;
        private final int cycle;
        private final CountDownLatch doneLatch;
        private final Sequence producerSequence;
        private final RingQueue<Event> queue;
        private int consumed;
        private int produced;

        BusyProducerConsumer(int cycle, Sequence producerSequence, Sequence consumerSequence, RingQueue<Event> queue, CyclicBarrier barrier, CountDownLatch doneLatch) {
            this.producerSequence = producerSequence;
            this.consumerSequence = consumerSequence;
            this.cycle = cycle;
            this.queue = queue;
            this.barrier = barrier;
            this.doneLatch = doneLatch;
        }

        @Override
        public void run() {
            try {
                barrier.await();

                while (produced < cycle) {
                    long producerCursor;
                    do {
                        producerCursor = producerSequence.next();
                        if (producerCursor < 0) {
                            consume();
                        } else {
                            break;
                        }
                    } while (true);
                    assert queue.get(producerCursor).value == 0;
                    queue.get(producerCursor).value = 42;
                    producerSequence.done(producerCursor);
                    produced++;
                }
                // Consume the remaining messages.
                consume();

                doneLatch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void consume() {
            while (true) {
                final long cursor = consumerSequence.next();
                if (cursor > -1) {
                    assert queue.get(cursor).value == 42;
                    queue.get(cursor).value = 0;
                    consumerSequence.done(cursor);
                    consumed++;
                } else {
                    break;
                }
            }
        }
    }

    static class Item {
        int owner;
    }

    private static class LongMsg {
        public long correlationId;
    }

    private static class TwoLongMsg {
        public long f1;
        public int f2;
        public long f3;
        public int f4;
    }

    private static class WaitingConsumer extends Thread {
        private final CyclicBarrier barrier;
        private final int[] buf;
        private final SOCountDownLatch latch;
        private final RingQueue<Event> queue;
        private final Sequence sequence;
        private volatile int finalIndex = 0;

        WaitingConsumer(int cycle, Sequence sequence, RingQueue<Event> queue, CyclicBarrier barrier, SOCountDownLatch latch) {
            this.sequence = sequence;
            this.buf = new int[cycle];
            this.queue = queue;
            this.barrier = barrier;
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                barrier.await();
                int p = 0;
                while (true) {
                    long cursor = sequence.waitForNext();
                    int v = queue.get(cursor).value;
                    sequence.done(cursor);

                    if (v == Integer.MIN_VALUE) {
                        break;
                    }
                    buf[p++] = v;
                }

                finalIndex = p;
            } catch (Throwable e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }
    }
}