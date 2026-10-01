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

package io.questdb.test.metrics;

import io.questdb.metrics.MetricsRegistryImpl;
import io.questdb.metrics.Target;
import io.questdb.std.str.BorrowableUtf8Sink;
import io.questdb.std.str.DirectUtf8Sink;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Pins the copy-on-write target-list snapshot and removeTarget machinery in
 * {@link MetricsRegistryImpl} from the OSS side, so an OSS revert of the lifecycle changes (the
 * copy-on-write snapshot and removeTarget) fails OSS CI rather than passing silently. The ENT-side
 * witnesses (the metrics-scrape role-switch race and after-demote scrape tests) live in the
 * enterprise module and do not run in OSS CI, which left these properties unguarded upstream.
 *
 * <p>The properties pinned here:
 * <ul>
 *   <li><b>removeTarget removes:</b> a scrape taken after removal never invokes the removed
 *       target (a {@link CountingTarget} records invocations).</li>
 *   <li><b>copy-on-write iteration safety:</b> concurrent addTarget/removeTarget while a scrape
 *       iterates the snapshot neither throws ({@code ArrayIndexOutOfBounds} /
 *       {@code ConcurrentModification}) nor tears (a target appears exactly once per scrape pass,
 *       never partially); reverting the COW snapshot to an in-place mutable list breaks this.</li>
 *   <li><b>add/remove balance:</b> after N add-then-remove cycles the registry scrapes exactly the
 *       baseline target set; a removeTarget that no-ops or removes the wrong element fails this.</li>
 * </ul>
 */
public class MetricsRegistryImplLifecycleTest {

    private static final int RACE_MILLIS = 1_500;

    @Test
    public void testAddRemoveBalanceAcrossManyCycles() {
        final MetricsRegistryImpl registry = new MetricsRegistryImpl();

        final CountingTarget baseline = new CountingTarget("baseline");
        registry.addTarget(baseline);
        Assert.assertEquals(1, scrapeAndCount(registry));

        // Add then remove a fresh target N times. If removeTarget silently no-ops, or removes the
        // wrong element, the scrape count drifts away from the single baseline target.
        for (int i = 0; i < 1_000; i++) {
            final CountingTarget t = new CountingTarget("cycle-" + i);
            registry.addTarget(t);
            Assert.assertEquals("baseline + one transient target must be scraped",
                    2, scrapeAndCount(registry));
            registry.removeTarget(t);
            // Reset the transient target's counter so the next scrape proves removeTarget stopped it
            // (the pre-removal scrape above legitimately touched it once).
            t.count.set(0);
            Assert.assertEquals("after removal only the baseline target remains",
                    1, scrapeAndCount(registry));
            Assert.assertEquals("the removed target must not be scraped after removal",
                    0, t.count.get());
        }

        // After all cycles the registry is back at the baseline: exactly one target scraped.
        baseline.count.set(0);
        Assert.assertEquals(1, scrapeAndCount(registry));
        Assert.assertEquals("only the baseline target survives all add/remove cycles",
                1, baseline.count.get());
    }

    @Test
    public void testConcurrentAddRemoveDuringScrapeIsSafeAndUntorn() throws Exception {
        final MetricsRegistryImpl registry = new MetricsRegistryImpl();

        // A stable baseline target that must be scraped exactly once on every pass. If the scrape
        // iterated a concurrently-mutated array in place (no COW snapshot), it could skip it,
        // double-count it, or throw mid-iteration.
        final CountingTarget stable = new CountingTarget("stable");
        registry.addTarget(stable);

        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final AtomicBoolean stop = new AtomicBoolean();
        final CyclicBarrier start = new CyclicBarrier(3);

        // Mutator thread: churns transient targets in and out of the registry.
        final Thread mutator = new Thread(() -> {
            try {
                start.await();
                int i = 0;
                while (!stop.get()) {
                    final CountingTarget t = new CountingTarget("m-" + (i++));
                    registry.addTarget(t);
                    registry.removeTarget(t);
                }
            } catch (Throwable e) {
                failure.compareAndSet(null, e);
            }
        }, "metrics-mutator");

        // Scrape thread: iterates the snapshot continuously. A per-pass tear (the stable target
        // missing or seen twice) or any thrown exception fails the test.
        final Thread scraper = new Thread(() -> {
            try (DirectUtf8Sink sink = new DirectUtf8Sink(256)) {
                start.await();
                while (!stop.get()) {
                    stable.count.set(0);
                    sink.clear();
                    registry.scrapeIntoPrometheus(sink);
                    final int seen = stable.count.get();
                    if (seen != 1) {
                        failure.compareAndSet(null, new AssertionError(
                                "torn scrape: stable target scraped " + seen + " times, expected exactly 1"));
                        return;
                    }
                }
            } catch (Throwable e) {
                failure.compareAndSet(null, e);
            }
        }, "metrics-scraper");

        mutator.start();
        scraper.start();
        start.await();

        Thread.sleep(RACE_MILLIS);
        stop.set(true);

        mutator.join(TimeUnit.SECONDS.toMillis(10));
        scraper.join(TimeUnit.SECONDS.toMillis(10));

        if (failure.get() != null) {
            throw new AssertionError("concurrent add/remove during scrape failed", failure.get());
        }

        // After the race settles, exactly the baseline survives.
        stable.count.set(0);
        Assert.assertEquals(1, scrapeAndCount(registry));
        Assert.assertEquals(1, stable.count.get());
    }

    @Test
    public void testRemoveTargetStopsScrapingIt() {
        final MetricsRegistryImpl registry = new MetricsRegistryImpl();

        final CountingTarget a = new CountingTarget("a");
        final CountingTarget b = new CountingTarget("b");
        final CountingTarget c = new CountingTarget("c");
        registry.addTarget(a);
        registry.addTarget(b);
        registry.addTarget(c);

        Assert.assertEquals(3, scrapeAndCount(registry));
        Assert.assertEquals(1, a.count.get());
        Assert.assertEquals(1, b.count.get());
        Assert.assertEquals(1, c.count.get());

        // Remove the middle target. A scrape after removal must never touch it again.
        registry.removeTarget(b);
        b.count.set(0);
        a.count.set(0);
        c.count.set(0);

        Assert.assertEquals(2, scrapeAndCount(registry));
        Assert.assertEquals("a survives the removal of b", 1, a.count.get());
        Assert.assertEquals("b is never scraped after removeTarget", 0, b.count.get());
        Assert.assertEquals("c survives the removal of b", 1, c.count.get());

        // Removing a target that is not registered is a no-op (does not disturb the others).
        final CountingTarget notRegistered = new CountingTarget("ghost");
        registry.removeTarget(notRegistered);
        a.count.set(0);
        c.count.set(0);
        Assert.assertEquals(2, scrapeAndCount(registry));
        Assert.assertEquals(1, a.count.get());
        Assert.assertEquals(1, c.count.get());

        // Remove the rest; an empty registry scrapes nothing without throwing.
        registry.removeTarget(a);
        registry.removeTarget(c);
        a.count.set(0);
        c.count.set(0);
        Assert.assertEquals(0, scrapeAndCount(registry));
        Assert.assertEquals(0, a.count.get());
        Assert.assertEquals(0, c.count.get());
    }

    private static int scrapeAndCount(MetricsRegistryImpl registry) {
        // Each CountingTarget writes exactly one byte, so the sink size equals the number of
        // targets the registry iterated. No custom sink subclass needed.
        try (DirectUtf8Sink sink = new DirectUtf8Sink(256)) {
            registry.scrapeIntoPrometheus(sink);
            return sink.size();
        }
    }

    /**
     * A scrape target that records how many times it was scraped and emits one byte per scrape, so
     * the consuming sink's size reflects the number of targets the registry iterated.
     */
    private static final class CountingTarget implements Target {
        final AtomicInteger count = new AtomicInteger();
        private final String name;

        CountingTarget(String name) {
            this.name = name;
        }

        @Override
        public void scrapeIntoPrometheus(BorrowableUtf8Sink sink) {
            count.incrementAndGet();
            sink.putAscii('#');
        }

        @Override
        public String toString() {
            return "CountingTarget{" + name + '}';
        }
    }
}
