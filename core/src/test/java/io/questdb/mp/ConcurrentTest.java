/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.mp;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

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
    public void testFanOutPingPongStableSequences() {
        final int threads = 2;
        final int iterations = 30;
        final int cycle = Numbers.ceilPow2(threads * iterations);

        // Requests inbox
        RingQueue<LongMsg> pingQueue = new RingQueue<>(LongMsg::new, cycle);
        MPSequence pingPubSeq = new MPSequence(cycle);
        FanOut pingSubFo = new FanOut();
        pingPubSeq.then(pingSubFo).then(pingPubSeq);

        // Request inbox hook for processor
        SCSequence pingSubSeq = new SCSequence();
        pingSubFo.and(pingSubSeq);

        // Response outbox
        RingQueue<LongMsg> pongQueue = new RingQueue<>(LongMsg::new, cycle);
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

                        System.out.println("* ping " + requestId);

                        long pongCursor;
                        while ((pongCursor = pongPubSeq.next()) < 0) {
                            LockSupport.parkNanos(10);
                        }
                        pongQueue.get(pongCursor).correlationId = requestId;
                        pongPubSeq.done(pongCursor);
                        pingPong.add(pongCursor);

                        System.out.println("* pong " + requestId);
                        i++;
                    } else {
                        LockSupport.parkNanos(10);
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
                        System.out.println("thread:" + threadId + ", added at " + pingPubSeq.value);

                        // Send next request
                        long requestId = idGen.incrementAndGet();
                        long pingCursor;
                        while ((pingCursor = pingPubSeq.next()) < 0) {
                            LockSupport.parkNanos(10);
                        }
                        pingQueue.get(pingCursor).correlationId = requestId;
                        pingPubSeq.done(pingCursor);
                        pingPong.add(pingCursor);

                        System.out.println("thread:" + threadId + ", ask: " + requestId);

                        // Wait for response
                        long responseId, pongCursor;
                        do {
                            while ((pongCursor = pongSubSeq.next()) < 0) {
                                LockSupport.parkNanos(10);
                            }
                            pingPong.add(pongCursor);
                            responseId = pongQueue.get(pongCursor).correlationId;
                            pongSubSeq.done(pongCursor);
                            pingPong.add(pongCursor);
                            System.out.println("thread:" + threadId + ", ping: " + responseId + ", expected: " + requestId);
                        } while (responseId != requestId);

                        System.out.println("thread " + threadId + ", pong " + requestId);
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

    @Test
    public void testFanOutPingPong() {
        final int threads = 2;
        final int iterations = 30;

        // Requests inbox
        final RingQueue<LongMsg> pingQueue = new RingQueue<>(LongMsg::new, threads);
        final MPSequence pingPubSeq = new MPSequence(threads);
        final FanOut pingSubFo = new FanOut();
        pingPubSeq.then(pingSubFo).then(pingPubSeq);

        // Request inbox hook for processor
        final SCSequence pingSubSeq = new SCSequence();
        pingSubFo.and(pingSubSeq);

        // Response outbox
        final RingQueue<LongMsg> pongQueue = new RingQueue<>(LongMsg::new, threads);
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
                while(i < threads * iterations) {
                    long seq = pingSubSeq.next();
                    if (seq > -1) {
                        // Get next request
                        LongMsg msg = pingQueue.get(seq);
                        long requestId = msg.correlationId;
                        pingSubSeq.done(seq);

                        LOG.info().$("ping received ").$(requestId).$();

                        long resp;
                        while ((resp = pongPubSeq.next()) < 0) {
                            LockSupport.parkNanos(10);
                        }
                        pongQueue.get(resp).correlationId = requestId;
                        pongPubSeq.done(resp);

                        LOG.info().$("pong sent ").$(requestId).$();
                        i++;
                    } else {
                        LockSupport.parkNanos(10);
                    }
                }
                doneCount.incrementAndGet();
            } finally {
                latch.countDown();
            }
        }).start();

        // Request threads
        for(int th = 0; th < threads; th++) {
            final int threadId = th;
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
                            LockSupport.parkNanos(10);
                        }
                        pingQueue.get(reqSeq).correlationId = requestId;
                        pingPubSeq.done(reqSeq);
                        LOG.info().$(threadId).$(", ping sent ").$(requestId).$();

                        // Wait for response
                        long responseId, respCursor;
                        do {
                            while ((respCursor = pongSubSeq.next()) < 0) {
                                LockSupport.parkNanos(10);
                            }
                            responseId = pongQueue.get(respCursor).correlationId;
                            pongSubSeq.done(respCursor);
                        } while (responseId != requestId);

                        LOG.info().$(threadId).$(", pong received ").$(requestId).$();

                        // Remove local response sequence from response FanOut
                        pongSubFo.remove(pongSubSeq);
                        pongSubSeq.clear();
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

    @Test
    public void testFanOutDoesNotProcessQueueFromStart() {
        // Main thread is going to publish K events
        // and then give green light to the relay thread,
        // which should not be processing first K-1 events
        //
        // There is also a generic consumer thread that's processing
        // all messages

        final RingQueue<LongMsg> queue = new RingQueue<LongMsg>(LongMsg::new, 64);
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
        final RingQueue<Event> queue = new RingQueue<>(Event.FACTORY, cycle);
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
                LockSupport.parkNanos(1);
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

    @Test
    public void testOneToParallelSubscriber() throws Exception {
        LOG.info().$("testOneToParallelSubscriber").$();
        int cycle = 1024;
        int size = 1024 * cycle;
        RingQueue<Event> queue = new RingQueue<>(Event.FACTORY, cycle);
        SPSequence pubSeq = new SPSequence(cycle);
        Sequence sub1 = new SCSequence();
        Sequence sub2 = new SCSequence();
        FanOut fanOut = FanOut.to(sub1).and(sub2);
        pubSeq.then(fanOut).then(pubSeq);

        CyclicBarrier barrier = new CyclicBarrier(4);
        CountDownLatch latch = new CountDownLatch(3);

        BusyConsumer[] consumers = new BusyConsumer[2];
        consumers[0] = new BusyConsumer(size, sub1, queue, barrier, latch);
        consumers[1] = new BusyConsumer(size, sub2, queue, barrier, latch);

        BusySubscriber subscriber = new BusySubscriber(queue, barrier, latch, fanOut);
        subscriber.start();

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

        for (int k = 0; k < 2; k++) {
            for (i = 0; i < consumers[k].buf.length; i++) {
                Assert.assertEquals(i, consumers[k].buf[i]);
            }
        }

        for (i = 0; i < subscriber.buf.length; i++) {
            Assert.assertTrue(subscriber.buf[i] > 0);
        }
    }

    static void publishEOE(RingQueue<Event> queue, Sequence sequence) {
        long cursor = sequence.nextBully();
        queue.get(cursor).value = Integer.MIN_VALUE;
        sequence.done(cursor);
    }

    private static class LongMsg {
        public long correlationId;
    }

    private static class BusyConsumer extends Thread {
        private final Sequence sequence;
        private final int[] buf;
        private final RingQueue<Event> queue;
        private final CyclicBarrier barrier;
        private final CountDownLatch latch;
        private volatile int finalIndex = 0;

        BusyConsumer(int cycle, Sequence sequence, RingQueue<Event> queue, CyclicBarrier barrier, CountDownLatch latch) {
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
                    long cursor = sequence.next();
                    if (cursor < 0) {
                        LockSupport.parkNanos(1);
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
                latch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static class BusySubscriber extends Thread {
        private final int[] buf;
        private final RingQueue<Event> queue;
        private final CyclicBarrier barrier;
        private final CountDownLatch latch;
        private final FanOut fanOut;

        BusySubscriber(RingQueue<Event> queue, CyclicBarrier barrier, CountDownLatch latch, FanOut fanOut) {
            this.buf = new int[20];
            this.queue = queue;
            this.barrier = barrier;
            this.latch = latch;
            this.fanOut = fanOut;
        }

        @Override
        public void run() {
            try {
                barrier.await();
                Os.sleep(10);

                // subscribe
                Sequence sequence = new SCSequence(0);
                fanOut.and(sequence);
                int p = 0;
                while (p < buf.length) {
                    long cursor = sequence.next();
                    if (cursor < 0) {
                        LockSupport.parkNanos(1);
                        continue;
                    }
                    int v = queue.get(cursor).value;
                    sequence.done(cursor);

                    if (v == Integer.MIN_VALUE) {
                        break;
                    }
                    buf[p++] = v;
                }

                fanOut.remove(sequence);

                latch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static class WaitingConsumer extends Thread {
        private final Sequence sequence;
        private final int[] buf;
        private final RingQueue<Event> queue;
        private final CyclicBarrier barrier;
        private final SOCountDownLatch latch;
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