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

import io.questdb.mp.ConcurrentQueue;
import io.questdb.mp.ValueHolder;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrentQueueFuzzTest {
    @Test
    public void testManyConsumers() throws InterruptedException {
        runFuzz(-3, 5);
    }

    @Test
    public void testManyProducers() throws InterruptedException {
        runFuzz(5, -3);
    }

    @Test
    public void testRandomBalance() throws InterruptedException {
        runFuzz(-3, -3);
    }

    private static void runFuzz(int producerMultiplier, int consumerMultiplier) throws InterruptedException {
        Rnd rnd = TestUtils.generateRandom(null);

        int nProducers = -1;
        int nConsumers = -1;

        assert producerMultiplier < 0 || consumerMultiplier < 0;

        // Randomize balance of producers and consumers
        if (producerMultiplier < 0) {
            nProducers = 1 + rnd.nextInt(Math.abs(producerMultiplier));
        }
        if (consumerMultiplier < 0) {
            nConsumers = 1 + rnd.nextInt(Math.abs(consumerMultiplier));
        }
        if (nConsumers < 0) {
            nConsumers = nProducers * (1 + rnd.nextInt(consumerMultiplier));
        }
        if (nProducers < 0) {
            nProducers = nConsumers * (1 + rnd.nextInt(producerMultiplier));
        }

        int elementsCount = 33 + rnd.nextInt(1_000) + (int) Math.pow(2, rnd.nextInt(20));
        boolean[] received = new boolean[elementsCount];

        ConcurrentQueue<IntHolderQueue> queue = ConcurrentQueue.createConcurrentQueue(IntHolderQueue::new);
        AtomicInteger counter = new AtomicInteger();

        CyclicBarrier barrier = new CyclicBarrier(nProducers + nConsumers);

        ObjList<Thread> threads = new ObjList<>();
        AtomicBoolean allPublished = new AtomicBoolean(false);
        ConcurrentLinkedQueue<Integer> errors = new ConcurrentLinkedQueue<>();

        for (int i = 0; i < nProducers; i++) {
            Thread th = new Thread(() -> {
                try {
                    barrier.await();
                    IntHolderQueue holder = new IntHolderQueue();
                    do {
                        int next = counter.getAndIncrement();
                        if (next >= elementsCount) {
                            break;
                        }
                        holder.value = next;
                        queue.enqueue(holder);
                    } while (true);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            th.start();
            threads.add(th);
        }

        boolean pauseReader = rnd.nextBoolean();
        for (int i = 0; i < nConsumers; i++) {
            Thread th = new Thread(() -> {
                try {
                    barrier.await();
                    IntHolderQueue holder = new IntHolderQueue();
                    boolean found = queue.tryDequeue(holder);
                    do {
                        if (found) {
                            if (received[holder.value]) {
                                errors.add(holder.value);
                            }
                            received[holder.value] = true;

                            if (pauseReader) {
                                int pause = rnd.nextInt(100) - 98;
                                if (pause > 0) {
                                    Os.sleep(pause);
                                }
                            }
                            found = queue.tryDequeue(holder);
                        } else {
                            if (allPublished.get() && !(found = queue.tryDequeue(holder))) {
                                break;
                            }
                            Os.pause();
                        }
                    } while (true);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            th.start();
            threads.add(th);
        }

        for (int i = 0; i < nProducers; i++) {
            threads.getQuick(i).join();
        }
        allPublished.set(true);

        for (int i = nProducers; i < nProducers + nConsumers; i++) {
            threads.getQuick(i).join();
        }

        if (!errors.isEmpty()) {
            Assert.assertTrue(errors.toString(), errors.isEmpty());
        }

        IntList missing = new IntList();
        for (int i = 0; i < elementsCount; i++) {
            if (!received[i]) {
                missing.add(i);
            }
        }
        if (missing.size() > 0) {
            Assert.fail("Items not received: " + missing);
        }

        System.out.println("Processed " + elementsCount + " queue size: " + queue.capacity());
    }

    static class IntHolderQueue implements ValueHolder<IntHolderQueue> {
        int value;

        @Override
        public void clear() {
            value = 0;
        }

        @Override
        public void copyTo(IntHolderQueue intHolder) {
            intHolder.value = value;
        }
    }
}
