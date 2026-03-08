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

package io.questdb.test.std;

import io.questdb.metrics.Counter;
import io.questdb.metrics.CounterImpl;
import io.questdb.metrics.LongGauge;
import io.questdb.metrics.LongGaugeImpl;
import io.questdb.metrics.NullCounter;
import io.questdb.metrics.NullLongGauge;
import io.questdb.std.AssociativeCache;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.ConcurrentAssociativeCache;
import io.questdb.std.ConcurrentCacheConfiguration;
import io.questdb.std.MemoryTag;
import io.questdb.std.NoOpAssociativeCache;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Rnd;
import io.questdb.std.SimpleAssociativeCache;
import io.questdb.std.Unsafe;
import io.questdb.std.str.FlyweightDirectUtf16Sink;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(Parameterized.class)
public class AssociativeCacheTest {
    private final CacheType cacheType;

    public AssociativeCacheTest(CacheType cacheType) {
        this.cacheType = cacheType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {CacheType.SIMPLE},
                {CacheType.CONCURRENT},
        });
    }

    @Test
    public void testBasic() {
        try (AssociativeCache<String> cache = createCache(8, 64)) {
            cache.put("X", "1");
            cache.put("Y", "2");
            cache.put("Z", "3");
            Assert.assertEquals("1", cache.poll("X"));
            Assert.assertEquals("2", cache.poll("Y"));
            Assert.assertEquals("3", cache.poll("Z"));
            Assert.assertNull(cache.poll("X"));
            Assert.assertNull(cache.poll("Y"));
            Assert.assertNull(cache.poll("Z"));
        }
    }

    @Test
    public void testClear() {
        try (AssociativeCache<String> cache = createCache(8, 8)) {
            cache.put("X", "1");
            cache.put("Y", "2");
            cache.put("Z", "3");

            cache.clear();

            Assert.assertNull(cache.poll("X"));
            Assert.assertNull(cache.poll("Y"));
            Assert.assertNull(cache.poll("Z"));
        }
    }

    @Test
    public void testConcurrent() throws Exception {
        Assume.assumeTrue(cacheType == CacheType.CONCURRENT);

        final int N = 100_000;
        try (AssociativeCache<AtomicInteger> cache = createCache(4, 4)) {
            final int threadCount = 8;

            final CyclicBarrier barrier = new CyclicBarrier(threadCount);
            final AtomicInteger errors = new AtomicInteger();
            final ObjList<Thread> threads = new ObjList<>();
            for (int t = 0; t < threadCount; t++) {
                final int threadId = t;
                Thread thread = new Thread(() -> {
                    Rnd rnd = new Rnd(threadId, threadId);
                    try {
                        barrier.await();

                        for (int i = 0; i < N; i++) {
                            CharSequence k = rnd.nextString(2);
                            AtomicInteger counter = cache.poll(k);
                            if (counter != null) {
                                // Manifest that we've acquired the counter.
                                int c = counter.incrementAndGet();
                                // Do some sleep.
                                Os.pause();
                                // Check that no one else acquired the counter.
                                Assert.assertEquals(c, counter.get());
                                cache.put(k, counter);
                            } else {
                                cache.put(k, new AtomicInteger());
                            }
                        }
                    } catch (Throwable e) {
                        errors.incrementAndGet();
                    }
                });
                threads.add(thread);
                thread.start();
            }

            for (int i = 0, n = threads.size(); i < n; i++) {
                threads.getQuick(i).join();
            }

            Assert.assertEquals(0, errors.get());
        }
    }

    @Test
    public void testEmptyValuePromotion() {
        // just a single row to make sure keys are in the same row
        try (AssociativeCache<String> cache = createCache(8, 1)) {
            cache.put("x", "val1");
            cache.put("y", "val2");

            Assert.assertEquals("val1", cache.poll("x"));
            Assert.assertNull(cache.poll("x"));


            cache.put("x", "val3");
            Assert.assertEquals("val3", cache.poll("x"));
        }
    }

    @Test
    public void testFull() {
        final CharSequenceHashSet all = new CharSequenceHashSet();
        final HashSet<Object> closed = new HashSet<>();
        class CloseTracker implements QuietCloseable {
            @Override
            public void close() {
                closed.add(this);
            }
        }

        final int blocks = 8;
        final int rows = 64;
        try (AssociativeCache<CloseTracker> cache = createCache(blocks, rows)) {
            final int N = 2 * blocks * rows;
            final Rnd rnd = new Rnd();

            Assert.assertEquals(blocks * rows, cache.capacity());

            for (int i = 0; i < N; i++) {
                CharSequence k = rnd.nextString(10);
                all.add(k);
                cache.put(k, new CloseTracker());
            }

            for (int i = 0; i < all.size(); i++) {
                CharSequence k = all.get(i);
                CloseTracker v = cache.poll(k);
                if (v != null) {
                    Assert.assertFalse(closed.contains(v));
                }
            }
            // At least cache.capacity() objects should be evicted.
            Assert.assertTrue(closed.size() >= cache.capacity());
        }
    }

    @Test
    public void testGaugeUpdates() {
        LongGauge gauge = new LongGaugeImpl("foobar");
        Counter hitCounter = new CounterImpl("hits");
        Counter missCounter = new CounterImpl("misses");

        try (AssociativeCache<String> cache = createCache(8, 64, gauge, hitCounter, missCounter)) {
            Assert.assertEquals(0, gauge.getValue());
            Assert.assertEquals(0, hitCounter.getValue());
            Assert.assertEquals(0, missCounter.getValue());

            for (int i = 0; i < 10; i++) {
                cache.put(Integer.toString(i), Integer.toString(i));
                Assert.assertEquals(i + 1, gauge.getValue());
            }

            cache.poll("0");
            Assert.assertEquals(9, gauge.getValue());
            Assert.assertEquals(1, hitCounter.getValue());
            Assert.assertEquals(0, missCounter.getValue());
            // Second poll() on the same key should be ignored.
            Assert.assertNull(cache.poll("0"));
            Assert.assertEquals(9, gauge.getValue());
            Assert.assertEquals(1, hitCounter.getValue());
            Assert.assertEquals(1, missCounter.getValue());
            // put() should insert value for key-value pair cleared by poll().
            cache.put("0", "42");
            Assert.assertEquals(10, gauge.getValue());
            Assert.assertEquals(1, hitCounter.getValue());
            Assert.assertEquals(1, missCounter.getValue());
        }

        // Cached gauge should go to zero once the cache is closed.
        Assert.assertEquals(0, gauge.getValue());
        Assert.assertEquals(1, hitCounter.getValue());
        Assert.assertEquals(1, missCounter.getValue());
    }

    @Test
    public void testImmutableKeys() {
        try (AssociativeCache<String> cache = createCache(8, 8)) {
            long mem = Unsafe.malloc(1024, MemoryTag.NATIVE_DEFAULT);
            final FlyweightDirectUtf16Sink dcs = new FlyweightDirectUtf16Sink();

            try {
                Unsafe.getUnsafe().putChar(mem, 'A');
                Unsafe.getUnsafe().putChar(mem + 2, 'B');

                dcs.of(mem, mem + 4);
                dcs.clear(4);

                cache.put(dcs, "hello1");

                Unsafe.getUnsafe().putChar(mem, 'C');
                Unsafe.getUnsafe().putChar(mem + 2, 'D');

                cache.put(dcs, "hello2");

                Unsafe.getUnsafe().putChar(mem, 'A');
                Unsafe.getUnsafe().putChar(mem + 2, 'B');

                Assert.assertEquals("hello1", cache.poll(dcs));

                Unsafe.getUnsafe().putChar(mem, 'C');
                Unsafe.getUnsafe().putChar(mem + 2, 'D');

                Assert.assertEquals("hello2", cache.poll(dcs));
            } finally {
                Unsafe.free(mem, 1024, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    @Test
    public void testMinSize() {
        try (AssociativeCache<String> cache = createCache(1, 1)) {
            cache.put("X", "1");
            cache.put("Y", "2");
            cache.put("Z", "3");
            Assert.assertNull(cache.poll("X"));
            Assert.assertNull(cache.poll("Y"));
            Assert.assertEquals("3", cache.poll("Z"));
        }
    }

    @Test
    public void testMultiValues() {
        try (AssociativeCache<String> cache = createCache(8, 8)) {
            String val1 = "myval1";
            String val2 = "myval2";

            // put two values under the same key.
            cache.put("x", val1);
            cache.put("x", val2);

            // poll should return one of the values.
            String fromCache1 = cache.poll("x");
            Assert.assertNotNull(fromCache1);
            Assert.assertTrue(fromCache1 == val1 || fromCache1 == val2);

            // second poll should return the other value.
            String fromCache2 = cache.poll("x");
            Assert.assertNotNull(fromCache2);
            Assert.assertNotSame(fromCache1, fromCache2);
            Assert.assertTrue(fromCache2 == val1 || fromCache2 == val2);
        }
    }

    @Test
    public void testNoOpCache() {
        Assume.assumeTrue(cacheType == CacheType.SIMPLE);

        final AtomicInteger closed = new AtomicInteger();
        class CloseTracker implements QuietCloseable {
            @Override
            public void close() {
                closed.incrementAndGet();
            }
        }

        try (AssociativeCache<CloseTracker> cache = new NoOpAssociativeCache<>()) {
            final int N = 100;
            final Rnd rnd = new Rnd();

            Assert.assertEquals(0, cache.capacity());

            for (int i = 0; i < N; i++) {
                CharSequence k = rnd.nextString(10);
                cache.put(k, new CloseTracker());
            }
            Assert.assertEquals(N, closed.get());

            rnd.reset();

            for (int i = 0; i < N; i++) {
                CharSequence k = rnd.nextString(10);
                Assert.assertNull(cache.poll(k));
            }
        }
    }

    @Test
    public void testNoUnnecessaryShift() {
        try (AssociativeCache<String> cache = createCache(8, 8)) {
            String value = "myval";

            cache.put("x", value);
            Assert.assertEquals(value, cache.poll("x"));
            cache.put("x", value);
        }
    }

    @Test
    public void testSimpleAssociativeCachePeek() {
        Assume.assumeTrue(cacheType == CacheType.SIMPLE);

        try (SimpleAssociativeCache<String> cache = new SimpleAssociativeCache<>(8, 64)) {
            cache.put("X", "1");
            cache.put("Y", "2");
            cache.put("Z", "3");

            Assert.assertEquals("1", cache.peek("X"));
            Assert.assertEquals("2", cache.peek("Y"));
            Assert.assertEquals("3", cache.peek("Z"));
            Assert.assertEquals("1", cache.peek("X"));
            Assert.assertEquals("2", cache.peek("Y"));
            Assert.assertEquals("3", cache.peek("Z"));

            Assert.assertEquals("1", cache.poll("X"));
            Assert.assertEquals("2", cache.poll("Y"));
            Assert.assertEquals("3", cache.poll("Z"));
            Assert.assertNull(cache.poll("X"));
            Assert.assertNull(cache.poll("Y"));
            Assert.assertNull(cache.poll("Z"));
        }
    }

    @Test
    public void testSimpleAssociativeCachePutAfterPeekDoesNotDuplicate() {
        Assume.assumeTrue(cacheType == CacheType.SIMPLE);

        try (SimpleAssociativeCache<Object> cache = new SimpleAssociativeCache<>(8, 1)) {
            Object o1 = new Object();
            Object o2 = new Object();
            Object o3 = new Object();
            cache.put("X", o1);
            cache.put("Y", o2);
            cache.put("Z", o3);

            Object peeked = cache.peek("Y");
            Assert.assertSame(o2, peeked);

            cache.put("Y", o2);

            Assert.assertNotNull(cache.poll("Y"));
            Assert.assertNull(cache.poll("Y"));
        }

    }

    private <V> AssociativeCache<V> createCache(int blocks, int rows) {
        return createCache(blocks, rows, NullLongGauge.INSTANCE, NullCounter.INSTANCE, NullCounter.INSTANCE);
    }

    private <V> AssociativeCache<V> createCache(int blocks, int rows, LongGauge cachedGauge, Counter hitCounter, Counter missCounter) {
        switch (cacheType) {
            case SIMPLE:
                return new SimpleAssociativeCache<>(blocks, rows, cachedGauge, hitCounter, missCounter);
            case CONCURRENT:
                return new ConcurrentAssociativeCache<>(
                        new ConcurrentCacheConfiguration() {
                            @Override
                            public int getBlocks() {
                                return blocks;
                            }

                            @Override
                            public LongGauge getCachedGauge() {
                                return cachedGauge;
                            }

                            @Override
                            public Counter getHiCounter() {
                                return hitCounter;
                            }

                            @Override
                            public Counter getMissCounter() {
                                return missCounter;
                            }

                            @Override
                            public int getRows() {
                                return rows;
                            }
                        }
                );
            default:
                throw new IllegalArgumentException("Unexpected cache type: " + cacheType);
        }
    }

    public enum CacheType {
        SIMPLE, CONCURRENT
    }
}
