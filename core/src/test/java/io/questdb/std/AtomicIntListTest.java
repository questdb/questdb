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

package io.questdb.std;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

public class AtomicIntListTest {

    @Test
    public void testBasic() {
        AtomicIntList list = new AtomicIntList();
        Assert.assertEquals(0, list.size());
        Assert.assertEquals(16, list.capacity());

        list.add(0);
        Assert.assertEquals(0, list.get(0));
        Assert.assertEquals(1, list.size());
        Assert.assertEquals(16, list.capacity());

        list = new AtomicIntList(1);
        Assert.assertEquals(0, list.size());
        Assert.assertEquals(1, list.capacity());
        try {
            list.get(0);
            Assert.fail();
        } catch (IndexOutOfBoundsException ignore) {
        }
        try {
            list.set(0, 1);
            Assert.fail();
        } catch (IndexOutOfBoundsException ignore) {
        }

        list.extendAndSet(2, 2);
        Assert.assertEquals(-1, list.get(0));
        Assert.assertEquals(-1, list.get(1));
        Assert.assertEquals(2, list.get(2));
        Assert.assertEquals(3, list.size());
        Assert.assertEquals(6, list.capacity());

        list.set(1, 1);
        Assert.assertEquals(1, list.get(1));

        list.extendAndSet(2, -2);
        Assert.assertEquals(-2, list.get(2));
        Assert.assertEquals(3, list.size());

        int capacity = list.capacity();
        for (int i = 0; i < capacity; i++) {
            list.add(i);
        }
        Assert.assertEquals(3 + capacity, list.size());
        Assert.assertEquals(2 * capacity, list.capacity());
    }

    @Test
    public void testHammerListSingleReader() throws Exception {
        testHammerList(1, 1024);
    }

    @Test
    public void testHammerListMultipleReaders() throws Exception {
        testHammerList(4, 1024);
    }

    private void testHammerList(int readers, int maxSize) throws Exception {
        final AtomicIntList list = new AtomicIntList();
        final CyclicBarrier barrier = new CyclicBarrier(readers + 1);
        final CountDownLatch latch = new CountDownLatch(readers + 1);
        final AtomicInteger anomalies = new AtomicInteger();

        for (int i = 0; i < readers; i++) {
            Reader reader = new Reader(list, barrier, latch, anomalies, maxSize);
            reader.start();
        }

        Writer writer = new Writer(list, barrier, latch, maxSize);
        writer.start();

        latch.await();

        Assert.assertEquals(0, anomalies.get());
    }

    private static class Reader extends Thread {

        private final AtomicIntList list;
        private final CyclicBarrier barrier;
        private final CountDownLatch latch;
        private final AtomicInteger anomalies;
        private final int maxSize;

        private Reader(AtomicIntList list, CyclicBarrier barrier, CountDownLatch latch, AtomicInteger anomalies, int maxSize) {
            this.list = list;
            this.barrier = barrier;
            this.latch = latch;
            this.anomalies = anomalies;
            this.maxSize = maxSize;
        }

        @Override
        public void run() {
            try {
                barrier.await();
                while (true) {
                    int size = list.size();
                    if (size == maxSize) {
                        break;
                    }
                    for (int i = 0; i < size; i++) {
                        int v = list.get(i);
                        if (v != i) {
                            anomalies.incrementAndGet();
                        }
                    }
                }
            } catch (Exception e) {
                anomalies.incrementAndGet();
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }
    }

    private static class Writer extends Thread {

        private final AtomicIntList list;
        private final CyclicBarrier barrier;
        private final CountDownLatch latch;
        private final int maxSize;

        private Writer(AtomicIntList list, CyclicBarrier barrier, CountDownLatch latch, int maxSize) {
            this.list = list;
            this.barrier = barrier;
            this.latch = latch;
            this.maxSize = maxSize;
        }

        @Override
        public void run() {
            try {
                barrier.await();
                for (int i = 0; i < maxSize; i++) {
                    // Use both methods that assume list growth.
                    if ((i & 1) == 1) {
                        list.add(i);
                    } else {
                        list.extendAndSet(i, i);
                    }
                    Os.pause();
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }
    }
}
