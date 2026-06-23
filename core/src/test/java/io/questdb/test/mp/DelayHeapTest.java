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

import io.questdb.mp.continuation.DelayHeap;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests for {@link DelayHeap}. The {@link TestEntry} helper uses an <em>absolute
 * deadline</em> (in {@code System.nanoTime()} space) so {@code getDelay} naturally
 * decreases over wall-clock time - mirroring how real {@link Delayed} entries
 * (e.g., {@code TxnWaiter}) behave. {@link Delayed#compareTo} is overridden to
 * compare deadlines directly, avoiding the {@code getDelay(now1) vs getDelay(now2)}
 * drift that two separate {@code now} reads would introduce.
 */
public class DelayHeapTest {

    @Test(timeout = 5_000)
    public void testClearEmptiesHeap() {
        DelayHeap<TestEntry> heap = new DelayHeap<>();
        for (int i = 0; i < 10; i++) {
            heap.offer(TestEntry.future("e" + i, TimeUnit.MILLISECONDS.toNanos(10)));
        }
        Assert.assertEquals(10, heap.size());
        heap.clear();
        Assert.assertEquals(0, heap.size());
        Assert.assertEquals(0, heap.toArray().length);
    }

    @Test(timeout = 10_000)
    public void testConcurrentProducersOrdering() throws InterruptedException, BrokenBarrierException {
        // Many producers stamp their entries with distinct absolute deadlines; the
        // consumer must see entries pop in deadline order. Verifies that synchronized
        // + notify() doesn't lose or reorder entries under contention.
        DelayHeap<TestEntry> heap = new DelayHeap<>();
        final int producers = 8;
        final int perProducer = 200;
        final int total = producers * perProducer;
        // Push the base far enough into the future that nothing fires before drain.
        long base = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(500);

        CyclicBarrier startGate = new CyclicBarrier(producers + 1);
        CountDownLatch producersDone = new CountDownLatch(producers);
        AtomicReference<Throwable> producerError = new AtomicReference<>();

        for (int p = 0; p < producers; p++) {
            final int pid = p;
            Thread t = new Thread(() -> {
                try {
                    startGate.await();
                    for (int i = 0; i < perProducer; i++) {
                        long deadlineNanos = base + (long) i * producers + pid;
                        heap.offer(new TestEntry("p" + pid + "_" + i, deadlineNanos));
                    }
                } catch (Throwable th) {
                    producerError.set(th);
                } finally {
                    producersDone.countDown();
                }
            }, "producer-" + p);
            t.setDaemon(true);
            t.start();
        }
        startGate.await();
        Assert.assertTrue("producers did not finish", producersDone.await(5, TimeUnit.SECONDS));
        Assert.assertNull(producerError.get());
        Assert.assertEquals(total, heap.size());

        // Drain via take. All deadlines have likely passed by now, so each take returns
        // immediately. Order must be ascending by deadline.
        long prev = Long.MIN_VALUE;
        for (int i = 0; i < total; i++) {
            TestEntry e = heap.take();
            Assert.assertTrue(
                    "ordering violated at index " + i + ": " + prev + " > " + e.deadlineNanos,
                    prev <= e.deadlineNanos
            );
            prev = e.deadlineNanos;
        }
        Assert.assertEquals(0, heap.size());
    }

    @Test(timeout = 30_000)
    public void testConcurrentProducersSingleConsumer() throws InterruptedException, BrokenBarrierException {
        // Stress: many producers, single consumer, mixed already-expired and
        // soon-to-expire entries. The consumer must observe every entry exactly once.
        DelayHeap<TestEntry> heap = new DelayHeap<>();
        final int producers = 6;
        final int perProducer = 500;
        final int total = producers * perProducer;
        Random outer = new Random(0xD377_1EAFL);

        CyclicBarrier startGate = new CyclicBarrier(producers + 1);
        CountDownLatch producersDone = new CountDownLatch(producers);
        AtomicReference<Throwable> producerError = new AtomicReference<>();

        for (int p = 0; p < producers; p++) {
            final int pid = p;
            final long seed = outer.nextLong();
            Thread t = new Thread(() -> {
                try {
                    startGate.await();
                    Random r = new Random(seed);
                    for (int i = 0; i < perProducer; i++) {
                        long offsetNanos = r.nextInt(3) == 0
                                ? -TimeUnit.MILLISECONDS.toNanos(1)  // already expired
                                : TimeUnit.MICROSECONDS.toNanos(r.nextInt(2_000));
                        heap.offer(TestEntry.future("p" + pid + "_" + i, offsetNanos));
                    }
                } catch (Throwable th) {
                    producerError.set(th);
                } finally {
                    producersDone.countDown();
                }
            }, "producer-" + p);
            t.setDaemon(true);
            t.start();
        }

        AtomicReference<Throwable> consumerError = new AtomicReference<>();
        Set<String> consumed = Collections.synchronizedSet(new HashSet<>());
        CountDownLatch consumerDone = new CountDownLatch(1);
        Thread consumer = new Thread(() -> {
            try {
                for (int i = 0; i < total; i++) {
                    TestEntry e = heap.take();
                    if (!consumed.add(e.id)) {
                        throw new AssertionError("duplicate consumed: " + e.id);
                    }
                }
            } catch (Throwable th) {
                consumerError.set(th);
            } finally {
                consumerDone.countDown();
            }
        }, "consumer");
        consumer.setDaemon(true);
        consumer.start();

        startGate.await();
        Assert.assertTrue("producers did not finish", producersDone.await(15, TimeUnit.SECONDS));
        Assert.assertNull(producerError.get());
        Assert.assertTrue("consumer did not finish", consumerDone.await(15, TimeUnit.SECONDS));
        Assert.assertNull(consumerError.get());
        Assert.assertEquals(total, consumed.size());
        Assert.assertEquals(0, heap.size());
    }

    @Test(timeout = 5_000)
    public void testHeapOrderManyEntries() throws InterruptedException {
        // Insert in shuffled order with distinct already-expired deadlines (so each
        // take returns immediately), assert drain order matches sort.
        DelayHeap<TestEntry> heap = new DelayHeap<>();
        long now = System.nanoTime();
        long[] deadlines = new long[200];
        for (int i = 0; i < deadlines.length; i++) {
            // All in the past; spaced by 1ns so order is well-defined.
            deadlines[i] = now - TimeUnit.SECONDS.toNanos(1) + i;
        }
        List<Long> shuffled = new ArrayList<>();
        for (long d : deadlines) shuffled.add(d);
        Collections.shuffle(shuffled, new Random(0xCAFEBABEL));

        for (int i = 0; i < shuffled.size(); i++) {
            heap.offer(new TestEntry("e" + i, shuffled.get(i)));
        }
        Assert.assertEquals(deadlines.length, heap.size());

        long prev = Long.MIN_VALUE;
        for (int i = 0; i < deadlines.length; i++) {
            TestEntry e = heap.take();
            Assert.assertTrue("non-monotone at " + i + ": prev=" + prev + ", cur=" + e.deadlineNanos,
                    e.deadlineNanos >= prev);
            prev = e.deadlineNanos;
        }
    }

    @Test(timeout = 5_000)
    public void testOfferReturnsTrue() {
        DelayHeap<TestEntry> heap = new DelayHeap<>();
        Assert.assertTrue(heap.offer(TestEntry.future("a", TimeUnit.MILLISECONDS.toNanos(10))));
        Assert.assertEquals(1, heap.size());
    }

    @Test(timeout = 5_000)
    public void testTakeBlocksUntilOffer() throws InterruptedException {
        DelayHeap<TestEntry> heap = new DelayHeap<>();
        AtomicReference<TestEntry> result = new AtomicReference<>();
        CountDownLatch consumerStarted = new CountDownLatch(1);
        CountDownLatch consumerDone = new CountDownLatch(1);

        Thread consumer = new Thread(() -> {
            try {
                consumerStarted.countDown();
                result.set(heap.take());
            } catch (InterruptedException ignored) {
            } finally {
                consumerDone.countDown();
            }
        }, "consumer");
        consumer.setDaemon(true);
        consumer.start();

        Assert.assertTrue(consumerStarted.await(2, TimeUnit.SECONDS));
        // Give the consumer a moment to enter wait().
        Thread.sleep(50);
        Assert.assertEquals(0, heap.size());

        TestEntry e = TestEntry.future("hello", -TimeUnit.MILLISECONDS.toNanos(1));
        heap.offer(e);

        Assert.assertTrue("consumer did not return", consumerDone.await(2, TimeUnit.SECONDS));
        Assert.assertSame(e, result.get());
        Assert.assertEquals(0, heap.size());
    }

    @Test(timeout = 5_000)
    public void testTakeEarlierOfferWakesConsumer() throws InterruptedException {
        // Consumer is waiting on a far-future head; producer inserts a sooner entry
        // that should pop first. notify() must wake the waiter so it re-checks the
        // head deadline.
        DelayHeap<TestEntry> heap = new DelayHeap<>();
        heap.offer(TestEntry.future("far", TimeUnit.SECONDS.toNanos(10)));

        AtomicReference<TestEntry> result = new AtomicReference<>();
        AtomicReference<Throwable> err = new AtomicReference<>();
        CountDownLatch consumerStarted = new CountDownLatch(1);
        CountDownLatch consumerDone = new CountDownLatch(1);
        Thread consumer = new Thread(() -> {
            try {
                consumerStarted.countDown();
                result.set(heap.take());
            } catch (Throwable t) {
                err.set(t);
            } finally {
                consumerDone.countDown();
            }
        }, "consumer");
        consumer.setDaemon(true);
        consumer.start();

        Assert.assertTrue(consumerStarted.await(2, TimeUnit.SECONDS));
        Thread.sleep(50);
        // Now insert a sooner entry; consumer must wake and return it within the
        // sooner deadline, well before the 10s "far" deadline.
        TestEntry soon = TestEntry.future("soon", TimeUnit.MILLISECONDS.toNanos(50));
        heap.offer(soon);

        Assert.assertTrue("consumer did not wake", consumerDone.await(2, TimeUnit.SECONDS));
        Assert.assertNull(err.get());
        Assert.assertSame(soon, result.get());
        Assert.assertEquals(1, heap.size()); // "far" still in heap
    }

    @Test(timeout = 5_000)
    public void testTakeInterrupt() throws InterruptedException {
        DelayHeap<TestEntry> heap = new DelayHeap<>();
        AtomicBoolean interrupted = new AtomicBoolean();
        CountDownLatch consumerStarted = new CountDownLatch(1);
        CountDownLatch consumerDone = new CountDownLatch(1);

        Thread consumer = new Thread(() -> {
            try {
                consumerStarted.countDown();
                heap.take();
            } catch (InterruptedException e) {
                interrupted.set(true);
            } finally {
                consumerDone.countDown();
            }
        }, "consumer");
        consumer.setDaemon(true);
        consumer.start();

        Assert.assertTrue(consumerStarted.await(2, TimeUnit.SECONDS));
        Thread.sleep(50);
        consumer.interrupt();

        Assert.assertTrue("consumer did not return", consumerDone.await(2, TimeUnit.SECONDS));
        Assert.assertTrue("expected InterruptedException", interrupted.get());
    }

    @Test(timeout = 5_000)
    public void testTakeRespectsDeadline() throws InterruptedException {
        DelayHeap<TestEntry> heap = new DelayHeap<>();
        long delayNanos = TimeUnit.MILLISECONDS.toNanos(150);
        long deadline = System.nanoTime() + delayNanos;
        heap.offer(new TestEntry("a", deadline));
        long start = System.nanoTime();
        TestEntry e = heap.take();
        long elapsed = System.nanoTime() - start;
        Assert.assertEquals("a", e.id);
        Assert.assertTrue("take returned too early: " + elapsed + " < " + delayNanos,
                elapsed >= delayNanos - TimeUnit.MILLISECONDS.toNanos(10));
        Assert.assertTrue("take returned suspiciously late: " + elapsed,
                elapsed < TimeUnit.SECONDS.toNanos(2));
    }

    @Test(timeout = 5_000)
    public void testTakeReturnsExpiredImmediately() throws InterruptedException {
        DelayHeap<TestEntry> heap = new DelayHeap<>();
        TestEntry e = TestEntry.future("immediate", -TimeUnit.SECONDS.toNanos(1));
        heap.offer(e);
        long start = System.nanoTime();
        TestEntry got = heap.take();
        long elapsed = System.nanoTime() - start;
        Assert.assertSame(e, got);
        Assert.assertTrue("take blocked unexpectedly: " + elapsed,
                elapsed < TimeUnit.MILLISECONDS.toNanos(50));
    }

    @Test(timeout = 5_000)
    public void testTakeReturnsHeadByDeadline() throws InterruptedException {
        // Three entries with distinct already-expired deadlines; take must return the
        // earliest first regardless of offer order.
        DelayHeap<TestEntry> heap = new DelayHeap<>();
        long now = System.nanoTime();
        TestEntry early = new TestEntry("early", now - 3_000L);
        TestEntry mid = new TestEntry("mid", now - 2_000L);
        TestEntry late = new TestEntry("late", now - 1_000L);
        heap.offer(mid);
        heap.offer(late);
        heap.offer(early);
        Assert.assertSame(early, heap.take());
        Assert.assertSame(mid, heap.take());
        Assert.assertSame(late, heap.take());
        Assert.assertEquals(0, heap.size());
    }

    @Test(timeout = 5_000)
    public void testToArrayMatchesContents() {
        DelayHeap<TestEntry> heap = new DelayHeap<>();
        TestEntry[] in = {
                TestEntry.future("a", 100L),
                TestEntry.future("b", 200L),
                TestEntry.future("c", 50L),
        };
        for (TestEntry e : in) heap.offer(e);
        Object[] out = heap.toArray();
        Assert.assertEquals(in.length, out.length);
        Set<TestEntry> outSet = new HashSet<>(Arrays.asList((TestEntry[]) Arrays.copyOf(out, out.length, TestEntry[].class)));
        Assert.assertEquals(in.length, outSet.size());
        for (TestEntry e : in) {
            Assert.assertTrue("missing " + e.id, outSet.contains(e));
        }
    }

    /**
     * {@link Delayed} entry parameterized by an absolute {@code System.nanoTime()}
     * deadline. {@link #getDelay} returns {@code deadline - now}, so the value
     * decreases monotonically with wall-clock time and crosses zero at the deadline.
     * This mirrors real {@code TxnWaiter} semantics; helpers using a fixed delay
     * value would never expire and {@link DelayHeap#take} would loop on a stale
     * {@code wait(delay)}.
     */
    private static final class TestEntry implements Delayed {
        final long deadlineNanos;
        final String id;

        TestEntry(String id, long deadlineNanos) {
            this.id = id;
            this.deadlineNanos = deadlineNanos;
        }

        static TestEntry future(String id, long offsetNanos) {
            return new TestEntry(id, System.nanoTime() + offsetNanos);
        }

        @Override
        public int compareTo(@NotNull Delayed o) {
            // Compare deadlines directly rather than going through getDelay twice,
            // which would call System.nanoTime() at two different instants and could
            // give an inconsistent ordering for entries with close deadlines.
            if (o instanceof TestEntry) {
                return Long.compare(deadlineNanos, ((TestEntry) o).deadlineNanos);
            }
            return Long.compare(getDelay(TimeUnit.NANOSECONDS), o.getDelay(TimeUnit.NANOSECONDS));
        }

        @Override
        public long getDelay(@NotNull TimeUnit unit) {
            return unit.convert(deadlineNanos - System.nanoTime(), TimeUnit.NANOSECONDS);
        }

        @Override
        public String toString() {
            return id + "@" + deadlineNanos;
        }
    }
}
