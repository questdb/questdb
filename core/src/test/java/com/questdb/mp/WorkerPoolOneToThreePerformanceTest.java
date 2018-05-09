/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.mp;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import static com.questdb.mp.ConcurrentTest.publishEOE;

public class WorkerPoolOneToThreePerformanceTest {
    public static void main(String[] args) throws BrokenBarrierException, InterruptedException {

        int cycle = 1024;
        int size = 1024 * cycle * 100;

        // ring queue
        RingQueue<Event> queue = new RingQueue<>(Event.FACTORY, cycle);

        // producer thread sequence
        SPSequence pubSeq = new SPSequence(cycle);
        // consumer thread sequence, which is shared between worker threads
        MCSequence subSeq = new MCSequence(cycle);

        pubSeq.then(subSeq).then(pubSeq);

        // test furniture
        int workerCount = 3;
        CyclicBarrier barrier = new CyclicBarrier(workerCount + 1);
        CountDownLatch latch = new CountDownLatch(workerCount);

        // create consumers that are threads at the same time
        BusyConsumer consumers[] = new BusyConsumer[workerCount];
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
        private final Sequence sequence;
        private final RingQueue<Event> queue;
        private final CyclicBarrier barrier;
        private final CountDownLatch latch;

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
