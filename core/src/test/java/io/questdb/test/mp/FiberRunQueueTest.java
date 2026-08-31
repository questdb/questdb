/*+*****************************************************************************
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

import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;

public class FiberRunQueueTest {

    @Test
    public void testConcurrentLaunchAndDrain() throws Exception {
        final int consumerCount = 4;
        final int iterationCount = 20_000;
        final int producerCount = 4;
        final FiberRuntime runtime = new FiberRuntime(producerCount);
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final AtomicBoolean isStopped = new AtomicBoolean();
        final CountDownLatch producersDone = new CountDownLatch(producerCount);
        final CountDownLatch ready = new CountDownLatch(producerCount + consumerCount);
        final CountDownLatch start = new CountDownLatch(1);
        final ObjList<CountingTask> tasks = new ObjList<>(producerCount);
        final ObjList<Thread> threads = new ObjList<>(producerCount + consumerCount);

        for (int i = 0; i < producerCount; i++) {
            final CountingTask task = new CountingTask();
            tasks.add(task);
            final Thread producer = new Thread(() -> {
                ready.countDown();
                try {
                    start.await();
                    for (int iteration = 0; iteration < iterationCount && !isStopped.get(); iteration++) {
                        if (iteration > 0) {
                            if (!waitUntilDone(task, isStopped)) {
                                return;
                            }
                            task.reopen();
                        }
                        if (!launchUntilAccepted(runtime, task, isStopped)) {
                            return;
                        }
                    }
                    waitUntilDone(task, isStopped);
                } catch (Throwable th) {
                    error.compareAndSet(null, th);
                    isStopped.set(true);
                } finally {
                    producersDone.countDown();
                }
            }, "fiber-run-queue-producer-" + i);
            threads.add(producer);
            producer.start();
        }
        for (int i = 0; i < consumerCount; i++) {
            final Thread consumer = new Thread(() -> {
                ready.countDown();
                try {
                    start.await();
                    while (!isStopped.get()
                            && (producersDone.getCount() > 0 || runtime.getOutstandingTaskCount() > 0)) {
                        if (runtime.getQueuedCount() < 0) {
                            throw new AssertionError("fiber run queue depth became negative");
                        }
                        if (runtime.drain(1) == 0) {
                            Os.pause();
                        }
                    }
                } catch (Throwable th) {
                    error.compareAndSet(null, th);
                    isStopped.set(true);
                }
            }, "fiber-run-queue-consumer-" + i);
            threads.add(consumer);
            consumer.start();
        }

        try {
            try {
                Assert.assertTrue(ready.await(10, TimeUnit.SECONDS));
            } finally {
                start.countDown();
            }
            joinThreads(threads, isStopped);
            if (error.get() != null) {
                throw new AssertionError(error.get());
            }
            for (int i = 0; i < producerCount; i++) {
                Assert.assertEquals(iterationCount, tasks.getQuick(i).runCount.get());
            }
            Assert.assertTrue(runtime.getCreatedFiberCount() > 0);
            Assert.assertTrue(runtime.getCreatedFiberCount() <= producerCount);
            Assert.assertEquals(0, runtime.getOutstandingTaskCount());
            Assert.assertEquals(0, runtime.getQueuedCount());
        } finally {
            isStopped.set(true);
            close(runtime);
        }
    }

    @Test
    public void testLiveLimitIncreaseGrowsTheRunQueue() {
        final int burstSize = 64;
        final FiberRuntime runtime = new FiberRuntime(1, 1);
        final AtomicInteger nextIndex = new AtomicInteger();
        final AtomicIntegerArray order = new AtomicIntegerArray(burstSize);
        final ObjList<OrderedTask> tasks = new ObjList<>(burstSize);
        for (int i = 0; i < burstSize; i++) {
            tasks.add(new OrderedTask(i, nextIndex, order));
        }

        try {
            final int initialCapacity = runtime.getRunQueueCapacity();
            runtime.updateConfiguration(burstSize, burstSize, 64);
            nextIndex.set(0);
            for (int i = 0; i < burstSize; i++) {
                Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(tasks.getQuick(i)));
            }
            final int capacity = runtime.getRunQueueCapacity();
            Assert.assertTrue(
                    "run queue must grow beyond its startup segment [initial=" + initialCapacity
                            + ", current=" + capacity + ']',
                    capacity > initialCapacity
            );
            Assert.assertEquals(burstSize, runtime.getQueuedCount());
            Assert.assertEquals(initialCapacity, runtime.drain(initialCapacity));
            Assert.assertEquals(1, runtime.getBudgetExhaustionCount());
            Assert.assertEquals(burstSize - initialCapacity, runtime.getQueuedCount());
            Assert.assertEquals(burstSize - initialCapacity, runtime.drain(burstSize));
            Assert.assertEquals(0, runtime.getQueuedCount());
            Assert.assertEquals(burstSize, runtime.getCreatedFiberCount());

            for (int round = 0; round < 100; round++) {
                nextIndex.set(0);
                for (int i = 0; i < burstSize; i++) {
                    final OrderedTask task = tasks.getQuick(i);
                    task.reopen();
                    Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
                }
                Assert.assertEquals(burstSize, runtime.getQueuedCount());
                Assert.assertEquals(burstSize, runtime.drain(burstSize));
                Assert.assertEquals(capacity, runtime.getRunQueueCapacity());
                Assert.assertEquals(burstSize, runtime.getCreatedFiberCount());
            }
        } finally {
            close(runtime);
        }
    }

    @Test
    public void testRepeatedBurstsPreserveFifoOrderAndReuseFibers() {
        final int burstSize = 257;
        final FiberRuntime runtime = new FiberRuntime(burstSize);
        final AtomicInteger nextIndex = new AtomicInteger();
        final AtomicIntegerArray order = new AtomicIntegerArray(burstSize);
        final ObjList<OrderedTask> tasks = new ObjList<>(burstSize);
        for (int i = 0; i < burstSize; i++) {
            tasks.add(new OrderedTask(i, nextIndex, order));
        }

        try {
            final int capacity = runtime.getRunQueueCapacity();
            Assert.assertTrue(
                    "run queue must be sized for the startup live limit [capacity=" + capacity
                            + ", burstSize=" + burstSize + ']',
                    capacity >= burstSize
            );
            for (int round = 0; round < 100; round++) {
                nextIndex.set(0);
                for (int i = 0; i < burstSize; i++) {
                    final OrderedTask task = tasks.getQuick(i);
                    if (round > 0) {
                        task.reopen();
                    }
                    Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
                }
                Assert.assertEquals(burstSize, runtime.getQueuedCount());
                Assert.assertEquals(burstSize, runtime.drain(burstSize));
                Assert.assertEquals(0, runtime.getOutstandingTaskCount());
                Assert.assertEquals(0, runtime.getQueuedCount());
                Assert.assertEquals(capacity, runtime.getRunQueueCapacity());
                Assert.assertEquals(burstSize, runtime.getCreatedFiberCount());
                Assert.assertEquals(burstSize, runtime.getRetainedFiberCount());
                for (int i = 0; i < burstSize; i++) {
                    Assert.assertEquals(i, order.get(i));
                }
            }
        } finally {
            close(runtime);
        }
    }

    @Test
    public void testStartupCapacityIsClampedBelowLargeLiveLimits() {
        final FiberRuntime runtime = new FiberRuntime(1, 1 << 30);
        try {
            Assert.assertEquals(1 << 20, runtime.getRunQueueCapacity());
        } finally {
            close(runtime);
        }
    }

    private static void close(FiberRuntime runtime) {
        runtime.beginQuiesce();
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (runtime.state() != FiberRuntimeState.CLOSED && System.nanoTime() < deadline) {
            runtime.drain(64);
        }
        Assert.assertTrue(runtime.awaitClosed(deadline));
        runtime.closeAfterDrained();
    }

    private static void joinThreads(ObjList<Thread> threads, AtomicBoolean isStopped) throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        for (int i = 0, n = threads.size(); i < n; i++) {
            final Thread thread = threads.getQuick(i);
            thread.join(Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())));
            if (thread.isAlive()) {
                isStopped.set(true);
            }
        }
        for (int i = 0, n = threads.size(); i < n; i++) {
            final Thread thread = threads.getQuick(i);
            if (thread.isAlive()) {
                thread.join(1_000);
            }
            Assert.assertFalse(thread.getName() + " did not stop", thread.isAlive());
        }
    }

    private static boolean launchUntilAccepted(
            FiberRuntime runtime,
            FiberTask task,
            AtomicBoolean isStopped
    ) {
        while (!isStopped.get()) {
            final LaunchResult result = runtime.launch(task);
            if (result == LaunchResult.LAUNCHED) {
                return true;
            }
            if (result != LaunchResult.SATURATED) {
                throw new AssertionError("unexpected fiber launch result [result=" + result + ']');
            }
            Os.pause();
        }
        return false;
    }

    private static boolean waitUntilDone(FiberTask task, AtomicBoolean isStopped) {
        while (!task.isDone()) {
            if (isStopped.get()) {
                return false;
            }
            Os.pause();
        }
        return true;
    }

    private static class CountingTask extends FiberTask {
        private final AtomicInteger runCount = new AtomicInteger();

        @Override
        protected boolean runStep() {
            runCount.incrementAndGet();
            return true;
        }
    }

    private static class OrderedTask extends FiberTask {
        private final int id;
        private final AtomicInteger nextIndex;
        private final AtomicIntegerArray order;

        private OrderedTask(int id, AtomicInteger nextIndex, AtomicIntegerArray order) {
            this.id = id;
            this.nextIndex = nextIndex;
            this.order = order;
        }

        @Override
        protected boolean runStep() {
            order.set(nextIndex.getAndIncrement(), id);
            return true;
        }
    }
}
