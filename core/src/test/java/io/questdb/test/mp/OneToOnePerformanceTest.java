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

import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SPSequence;
import io.questdb.mp.Sequence;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import static io.questdb.test.mp.ConcurrentTest.publishEOE;

public class OneToOnePerformanceTest {
    public static void main(String[] args) throws BrokenBarrierException, InterruptedException {

        int cycle = 1024;
        int size = 1024 * cycle * 100;

        // ring queue
        RingQueue<Event> queue = new RingQueue<>(Event.FACTORY, cycle);

        // producer thread sequence
        SPSequence pubSeq = new SPSequence(cycle);
        // consumer thread sequence, which is shared between worker threads
        SCSequence subSeq = new SCSequence(cycle, null);

        pubSeq.then(subSeq).then(pubSeq);

        // test furniture
        int workerCount = 1;
        CyclicBarrier barrier = new CyclicBarrier(workerCount + 1);
        CountDownLatch latch = new CountDownLatch(workerCount);

        // create consumers that are threads at the same time
        BusyConsumer[] consumers = new BusyConsumer[workerCount];
        for (int i = 0; i < workerCount; i++) {
            consumers[i] = new BusyConsumer(subSeq, queue, barrier, latch);
        }

        for (int i = 0; i < workerCount; i++) {
            consumers[i].start();
        }

        barrier.await();
        long t = System.currentTimeMillis();

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

        for (int j = 0; j < workerCount; j++) {
            publishEOE(queue, pubSeq);
        }

        latch.await();
        System.out.format("Processed = %,d ops/sec%n", (size * 1000L) / (System.currentTimeMillis() - t));
    }

    static class BusyConsumer extends Thread {
        private final CyclicBarrier barrier;
        private final CountDownLatch latch;
        private final RingQueue<Event> queue;
        private final Sequence sequence;

        BusyConsumer(Sequence sequence, RingQueue<Event> queue, CyclicBarrier barrier, CountDownLatch latch) {
            this.sequence = sequence;
            this.queue = queue;
            this.barrier = barrier;
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                barrier.await();
                while (true) {
                    // consumer receives next cursor
                    // if cursor is >= 0 - thread can use it as ring queue index
                    // if cursor == -1 - ring queue is full, thread has a choice to do something else
                    // if cursor == -2 - there was CAS failure and thread can retry more eagerly
                    // for purpose of this test we busy loop regardless
                    long cursor = sequence.next();
                    if (cursor < 0) {
                        continue;
                    }
                    int v = queue.get(cursor).value;
                    sequence.done(cursor);

                    if (v == Integer.MIN_VALUE) {
                        break;
                    }
                }
                latch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
