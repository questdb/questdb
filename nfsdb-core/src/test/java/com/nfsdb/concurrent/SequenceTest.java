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

package com.nfsdb.concurrent;

import com.nfsdb.utils.Rnd;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

public class SequenceTest {

    @Test
    public void testOneToOneSpin() throws Exception {
        // setup
        RingQ<Event> ring = new RingQ<>(Event.FACTORY, 1024);
        SPSequence pseq = new SPSequence(ring.getCycle());
        SCSequence cseq = new SCSequence();
        pseq.followedBy(cseq);
        cseq.followedBy(pseq);


        CyclicBarrier barrier = new CyclicBarrier(2);
        CountDownLatch breakLatch = new CountDownLatch(1);
        int len = 8 * 1024;
        BusyConsumer consumer = new BusyConsumer(barrier, breakLatch, cseq, len, ring);
        consumer.start();

        Rnd rnd = new Rnd();
        long buf[] = new long[len];

        for (int i = 0; i < len; i++) {
            buf[i] = rnd.nextPositiveLong();
        }

        int index = 0;
        barrier.await();

        // publish events
        while (true) {
            long cursor = pseq.next();
            if (cursor < 0) {
                continue;
            }
            ring.get(cursor).value = buf[index++];
            pseq.done(cursor);
            if (index == len) {
                break;
            }
        }

        // publish STOP message
        while (true) {
            long cursor = pseq.next();
            if (cursor < 0) {
                continue;
            }
            ring.get(cursor).value = Long.MIN_VALUE;
            pseq.done(cursor);
            break;
        }

        breakLatch.await();
        Assert.assertArrayEquals(buf, consumer.capture);
    }

    private static class BusyConsumer extends Thread {
        private final Sequence sequence;
        private final CyclicBarrier barrier;
        private final CountDownLatch breakLatch;
        private final long[] capture;
        private final RingQ<Event> ringQ;

        public BusyConsumer(CyclicBarrier barrier, CountDownLatch latch, Sequence sequence, int pubSize, RingQ<Event> ringQ) {
            this.barrier = barrier;
            this.sequence = sequence;
            this.breakLatch = latch;
            this.capture = new long[pubSize];
            this.ringQ = ringQ;
        }

        @Override
        public void run() {
            try {
                int index = 0;
                barrier.await();
                while (true) {
                    long cursor = sequence.next();
                    if (cursor < 0) {
                        continue;
                    }
                    long l = ringQ.get(cursor).value;
                    sequence.done(cursor);
                    if (l == Long.MIN_VALUE) {
                        break;
                    }
                    capture[index++] = l;
                }
                breakLatch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static class Event implements RingEntryFactory<Event> {
        private static final Event FACTORY = new Event();
        long value;

        @Override
        public Event newInstance() {
            return new Event();
        }
    }
}
