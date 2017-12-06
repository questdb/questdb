/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

package com.questdb.net.ha;

import com.questdb.net.ha.bridge.JournalEventBridge;
import com.questdb.net.ha.bridge.JournalEventHandler;
import com.questdb.net.ha.bridge.JournalEventProcessor;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.*;

public class JournalEventBridgeTest {
    @Test
    public void testStartStop() {
        JournalEventBridge bridge = new JournalEventBridge(2, TimeUnit.SECONDS);
        for (int i = 0; i < 10000; i++) {
            bridge.publish(10, System.currentTimeMillis());
        }
    }

    @Test
    public void testTwoPublishersThreeConsumers() throws Exception {
        ExecutorService service = Executors.newCachedThreadPool();
        final JournalEventBridge bridge = new JournalEventBridge(50, TimeUnit.MILLISECONDS);
        final Future[] publishers = new Future[2];
        final Handler[] consumers = new Handler[3];
        final int batchSize = 1000;

        final CyclicBarrier barrier = new CyclicBarrier(publishers.length + consumers.length);
        final CountDownLatch latch = new CountDownLatch(publishers.length + consumers.length);

        for (int i = 0; i < publishers.length; i++) {
            final int index = i;
            publishers[i] = service.submit(() -> {
                int count = 0;
                try {
                    barrier.await();
                    for (int k = 0; k < batchSize; k++) {
                        long ts = System.nanoTime();
                        bridge.publish(index, ts);
                        count++;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }

                return count;
            });
        }


        for (int i = 0; i < consumers.length; i++) {
            final JournalEventProcessor processor = new JournalEventProcessor(bridge);
            final Handler handler = new Handler(i);
            consumers[i] = handler;
            service.submit(() -> {
                try {
                    barrier.await();
                    while (true) {
                        if (!processor.process(handler, true)) {
                            break;
                        }
                    }
                } catch (InterruptedException | BrokenBarrierException e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

//        service.submit(new Runnable() {
//            @Override
//            public void run() {
//                try {
//                    barrier.await();
//                    for (int i = 0; i < 1000; i++) {
//                        Sequence sequence = bridge.createAgentSequence();
//                        LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(10));
//                        bridge.removeAgentSequence(sequence);
//                    }
//                } catch (InterruptedException | BrokenBarrierException e) {
//                    e.printStackTrace();
//                } finally {
//                    latch.countDown();
//                }
//            }
//        });

        latch.await();

        for (Future f : publishers) {
            Assert.assertEquals(batchSize, f.get());
        }

        Assert.assertEquals(batchSize, consumers[0].getCounter());
        Assert.assertEquals(batchSize, consumers[1].getCounter());
        Assert.assertEquals(0, consumers[2].getCounter());
    }

    private class Handler implements JournalEventHandler {
        private final int index;
        private int counter;

        private Handler(int index) {
            this.index = index;
        }

        public int getCounter() {
            return counter;
        }

        @Override
        public void handle(int journalIndex) {
            if (journalIndex == index) {
                counter++;
            }
        }
    }
}
