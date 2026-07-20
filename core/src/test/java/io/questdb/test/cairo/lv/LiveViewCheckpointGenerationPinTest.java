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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationTracker;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Standalone coverage for the generation-pin mechanism
 * ({@link LiveViewCheckpointGenerationTracker} / {@link LiveViewCheckpointGenerationPin},
 * design section 5 invariants 4-5, section 16.2), plus its garbage-collection
 * contract exercised through the test-only
 * {@link LiveViewCheckpointInMemoryPayloadStore} stand-in for the Phase 2 state
 * store.
 * <p>
 * The properties under test: a pin snapshots exactly the generation current at pin
 * time and keeps it after a later publication (a reader pins one generation before
 * resolving any reference); {@link LiveViewCheckpointGenerationTracker#minPinnedGeneration()}
 * tracks the oldest live pin and advances as pins release; and purge retains a
 * generation's payload roots while any reader pins it (files referenced by a pinned
 * reader are not deleted).
 */
public class LiveViewCheckpointGenerationPinTest extends AbstractCairoTest {

    @Test
    public void testConcurrentPinAndRelease() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointGenerationTracker tracker = new LiveViewCheckpointGenerationTracker();
            try {
                final LiveViewCheckpointPageRef ref = ref(1, 0, 8);
                tracker.setCurrentGeneration(0, ref, ref, ref);

                final int threadCount = 4;
                final int iterations = 2_000;
                final CyclicBarrier barrier = new CyclicBarrier(threadCount + 2);
                final AtomicInteger errors = new AtomicInteger();
                final ObjList<Thread> workers = new ObjList<>();
                for (int t = 0; t < threadCount; t++) {
                    final Thread th = new Thread(() -> {
                        try {
                            barrier.await();
                            for (int i = 0; i < iterations; i++) {
                                try (LiveViewCheckpointGenerationPin pin = tracker.pin()) {
                                    // The pinned generation is a real published generation
                                    // (>= 0), never the NO_GENERATION sentinel.
                                    if (pin.getGeneration() < 0) {
                                        errors.incrementAndGet();
                                    }
                                }
                            }
                        } catch (Throwable e) {
                            errors.incrementAndGet();
                        }
                    });
                    workers.add(th);
                    th.start();
                }
                final Thread publisher = new Thread(() -> {
                    try {
                        barrier.await();
                        for (long g = 1; g <= 200; g++) {
                            tracker.setCurrentGeneration(g, ref, ref, ref);
                            Thread.yield();
                        }
                    } catch (Throwable e) {
                        errors.incrementAndGet();
                    }
                });
                publisher.start();

                barrier.await();
                for (int t = 0; t < threadCount; t++) {
                    workers.getQuick(t).join();
                }
                publisher.join();

                Assert.assertEquals(0, errors.get());
                Assert.assertEquals(0, tracker.getActivePinCount());
                Assert.assertEquals(LiveViewCheckpointGenerationTracker.NO_PINS, tracker.minPinnedGeneration());
            } finally {
                tracker.close();
            }
        });
    }

    @Test
    public void testDoubleCloseIsNoOp() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointGenerationTracker tracker = new LiveViewCheckpointGenerationTracker();
            try {
                final LiveViewCheckpointPageRef ref = ref(1, 0, 8);
                tracker.setCurrentGeneration(3, ref, ref, ref);
                final LiveViewCheckpointGenerationPin pin = tracker.pin();
                Assert.assertEquals(1, tracker.getActivePinCount());
                pin.close();
                Assert.assertFalse(pin.isPinned());
                Assert.assertEquals(0, tracker.getActivePinCount());
                // A second close must not double-decrement or otherwise corrupt state.
                pin.close();
                Assert.assertEquals(0, tracker.getActivePinCount());
                Assert.assertEquals(LiveViewCheckpointGenerationTracker.NO_PINS, tracker.minPinnedGeneration());
            } finally {
                tracker.close();
            }
        });
    }

    @Test
    public void testGenerationCannotMoveBackwards() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointGenerationTracker tracker = new LiveViewCheckpointGenerationTracker();
            try {
                final LiveViewCheckpointPageRef ref = ref(1, 0, 8);
                tracker.setCurrentGeneration(5, ref, ref, ref);
                try {
                    tracker.setCurrentGeneration(4, ref, ref, ref);
                    Assert.fail("expected a backwards-generation rejection");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "must not move backwards");
                }
                // Re-recording the same generation is allowed (a recovery re-open).
                tracker.setCurrentGeneration(5, ref, ref, ref);
                Assert.assertEquals(5, tracker.getCurrentGeneration());
            } finally {
                tracker.close();
            }
        });
    }

    @Test
    public void testMinAdvancesAsOldGenerationsRelease() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointGenerationTracker tracker = new LiveViewCheckpointGenerationTracker();
            try {
                final LiveViewCheckpointPageRef ref = ref(1, 0, 8);
                tracker.setCurrentGeneration(1, ref, ref, ref);
                final LiveViewCheckpointGenerationPin p1 = tracker.pin();
                tracker.setCurrentGeneration(2, ref, ref, ref);
                final LiveViewCheckpointGenerationPin p2 = tracker.pin();
                tracker.setCurrentGeneration(3, ref, ref, ref);
                final LiveViewCheckpointGenerationPin p3 = tracker.pin();

                Assert.assertEquals(3, tracker.getActivePinCount());
                Assert.assertEquals(1, tracker.minPinnedGeneration());

                p1.close();
                Assert.assertEquals(2, tracker.minPinnedGeneration());
                Assert.assertEquals(2, tracker.getActivePinCount());

                p2.close();
                Assert.assertEquals(3, tracker.minPinnedGeneration());

                p3.close();
                Assert.assertEquals(LiveViewCheckpointGenerationTracker.NO_PINS, tracker.minPinnedGeneration());
                Assert.assertEquals(0, tracker.getActivePinCount());
            } finally {
                tracker.close();
            }
        });
    }

    @Test
    public void testMultiplePinsSameGeneration() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointGenerationTracker tracker = new LiveViewCheckpointGenerationTracker();
            try {
                final LiveViewCheckpointPageRef ref = ref(1, 0, 8);
                tracker.setCurrentGeneration(4, ref, ref, ref);
                final LiveViewCheckpointGenerationPin a = tracker.pin();
                final LiveViewCheckpointGenerationPin b = tracker.pin();
                final LiveViewCheckpointGenerationPin c = tracker.pin();

                Assert.assertEquals(3, tracker.pinCount(4));
                Assert.assertEquals(3, tracker.getActivePinCount());
                Assert.assertTrue(tracker.isGenerationPinned(4));
                Assert.assertEquals(4, tracker.minPinnedGeneration());

                // Releasing one of several pins on a generation keeps it pinned.
                a.close();
                Assert.assertEquals(2, tracker.pinCount(4));
                Assert.assertTrue(tracker.isGenerationPinned(4));
                Assert.assertEquals(4, tracker.minPinnedGeneration());

                b.close();
                c.close();
                Assert.assertEquals(0, tracker.pinCount(4));
                Assert.assertFalse(tracker.isGenerationPinned(4));
                Assert.assertEquals(LiveViewCheckpointGenerationTracker.NO_PINS, tracker.minPinnedGeneration());
            } finally {
                tracker.close();
            }
        });
    }

    @Test
    public void testNoPinsReturnsSentinel() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointGenerationTracker tracker = new LiveViewCheckpointGenerationTracker();
            try {
                Assert.assertEquals(0, tracker.getActivePinCount());
                Assert.assertEquals(LiveViewCheckpointGenerationTracker.NO_PINS, tracker.minPinnedGeneration());
                Assert.assertEquals(LiveViewCheckpointGenerationTracker.NO_GENERATION, tracker.getCurrentGeneration());
                Assert.assertFalse(tracker.isGenerationPinned(0));
            } finally {
                tracker.close();
            }
        });
    }

    @Test
    public void testPinCapturesCurrentGeneration() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointGenerationTracker tracker = new LiveViewCheckpointGenerationTracker();
            try {
                final LiveViewCheckpointPageRef timeline = ref(2, 4_096, 256);
                final LiveViewCheckpointPageRef rowPos = new LiveViewCheckpointPageRef(); // null
                final LiveViewCheckpointPageRef segDir = ref(9, 0, 24);
                tracker.setCurrentGeneration(7, timeline, rowPos, segDir);

                final LiveViewCheckpointGenerationPin pin = tracker.pin();
                Assert.assertTrue(pin.isPinned());
                Assert.assertEquals(7, pin.getGeneration());

                Assert.assertFalse(pin.getTimelineRootRef().isNull());
                Assert.assertEquals(2, pin.getTimelineRootRef().getSegmentId());
                Assert.assertEquals(4_096, pin.getTimelineRootRef().getOffset());
                Assert.assertEquals(256, pin.getTimelineRootRef().getLength());

                // A null generation reference is captured as null.
                Assert.assertTrue(pin.getRowPositionDeltaRootRef().isNull());

                Assert.assertFalse(pin.getSegmentDirectoryRootRef().isNull());
                Assert.assertEquals(9, pin.getSegmentDirectoryRootRef().getSegmentId());
                Assert.assertEquals(0, pin.getSegmentDirectoryRootRef().getOffset());
                Assert.assertEquals(24, pin.getSegmentDirectoryRootRef().getLength());

                pin.close();
                Assert.assertFalse(pin.isPinned());
                // Accessing a released pin is a programming error.
                try {
                    pin.getGeneration();
                    Assert.fail("expected an unpinned-access rejection");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "is not held");
                }
            } finally {
                tracker.close();
            }
        });
    }

    @Test
    public void testPinPoolReuse() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointGenerationTracker tracker = new LiveViewCheckpointGenerationTracker();
            try {
                final LiveViewCheckpointPageRef ref = ref(1, 0, 8);
                tracker.setCurrentGeneration(1, ref, ref, ref);
                final LiveViewCheckpointGenerationPin first = tracker.pin();
                first.close();
                // A freed pin is pooled and reused, keeping the read path allocation-free.
                final LiveViewCheckpointGenerationPin second = tracker.pin();
                Assert.assertSame(first, second);
                second.close();
            } finally {
                tracker.close();
            }
        });
    }

    @Test
    public void testPinResolvesItsGenerationAfterAdvance() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointGenerationTracker tracker = new LiveViewCheckpointGenerationTracker();
            try {
                final LiveViewCheckpointPageRef timelineG1 = ref(1, 100, 40);
                tracker.setCurrentGeneration(1, timelineG1, ref(1, 200, 20), ref(1, 300, 10));
                final LiveViewCheckpointGenerationPin p1 = tracker.pin();

                // Publish a new generation with different roots while p1 is held.
                final LiveViewCheckpointPageRef timelineG2 = ref(2, 4_096, 256);
                tracker.setCurrentGeneration(2, timelineG2, ref(2, 8_192, 128), ref(2, 0, 64));

                // Invariant 4: p1 still resolves generation 1's roots, not generation 2's.
                Assert.assertEquals(1, p1.getGeneration());
                Assert.assertEquals(1, p1.getTimelineRootRef().getSegmentId());
                Assert.assertEquals(100, p1.getTimelineRootRef().getOffset());
                Assert.assertEquals(40, p1.getTimelineRootRef().getLength());

                // A fresh pin snapshots the now-current generation 2.
                final LiveViewCheckpointGenerationPin p2 = tracker.pin();
                Assert.assertEquals(2, p2.getGeneration());
                Assert.assertEquals(2, p2.getTimelineRootRef().getSegmentId());
                Assert.assertEquals(4_096, p2.getTimelineRootRef().getOffset());
                Assert.assertEquals(256, p2.getTimelineRootRef().getLength());

                Assert.assertEquals(1, tracker.minPinnedGeneration());
                p1.close();
                p2.close();
            } finally {
                tracker.close();
            }
        });
    }

    @Test
    public void testPinWithoutGenerationThrows() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointGenerationTracker tracker = new LiveViewCheckpointGenerationTracker();
            try {
                try {
                    tracker.pin();
                    Assert.fail("expected a no-generation rejection");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "no published generation to pin");
                }
            } finally {
                tracker.close();
            }
        });
    }

    @Test
    public void testPurgeHonorsPinsAndSlots() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointGenerationTracker tracker = new LiveViewCheckpointGenerationTracker();
            final LiveViewCheckpointInMemoryPayloadStore store = new LiveViewCheckpointInMemoryPayloadStore();
            try {
                final LiveViewCheckpointPageRef ref = ref(0, 0, 8);
                // Each generation adds one root that shares a segment with the previous
                // generation and introduces one exclusive segment. Segments:
                //   gen1 -> {100, 101}, gen2 -> {101, 102}, gen3 -> {102, 103},
                //   gen4 -> {103, 104}, gen5 -> {104, 105}
                LiveViewCheckpointGenerationPin pinnedOnG2 = null;
                for (long gen = 1; gen <= 5; gen++) {
                    tracker.setCurrentGeneration(gen, ref, ref, ref);
                    store.publishGeneration(gen, roots(rootFor(gen)));
                    if (gen == 2) {
                        pinnedOnG2 = tracker.pin();
                    }
                }
                Assert.assertNotNull(pinnedOnG2);
                Assert.assertEquals(5, store.generationCount());

                // current = 5, fallback = 4. Generations 1 and 3 are obsolete; generation
                // 2 is retained only because a reader pins it.
                store.purge(5, 4, tracker);
                Assert.assertEquals(3, store.generationCount());
                Assert.assertFalse(store.hasGeneration(1));
                Assert.assertTrue(store.hasGeneration(2));
                Assert.assertFalse(store.hasGeneration(3));
                Assert.assertTrue(store.hasGeneration(4));
                Assert.assertTrue(store.hasGeneration(5));

                // Segment 100 was exclusive to dropped generation 1 -> collected.
                Assert.assertFalse(store.segmentExists(100));
                // Segments 101 and 102 survive because pinned generation 2 references them.
                Assert.assertTrue(store.segmentExists(101));
                Assert.assertTrue(store.segmentExists(102));
                // Segments referenced by the live slots survive.
                Assert.assertTrue(store.segmentExists(103));
                Assert.assertTrue(store.segmentExists(104));
                Assert.assertTrue(store.segmentExists(105));

                // Release the reader and purge again: generation 2 and its now-unreferenced
                // segments are collected.
                pinnedOnG2.close();
                store.purge(5, 4, tracker);
                Assert.assertEquals(2, store.generationCount());
                Assert.assertFalse(store.hasGeneration(2));
                Assert.assertFalse(store.segmentExists(101));
                Assert.assertFalse(store.segmentExists(102));
                Assert.assertTrue(store.segmentExists(103));
                Assert.assertTrue(store.segmentExists(104));
                Assert.assertTrue(store.segmentExists(105));
            } finally {
                tracker.close();
            }
        });
    }

    private static LiveViewCheckpointPageRef ref(long segmentId, long offset, int length) {
        return new LiveViewCheckpointPageRef().of(segmentId, offset, length);
    }

    private static LiveViewCheckpointInMemoryPayloadStore.PayloadRoot rootFor(long gen) {
        // Shares segment 100 + gen with the previous generation; introduces 101 + gen.
        return new LiveViewCheckpointInMemoryPayloadStore.PayloadRoot(
                gen,
                gen * 10,
                1,
                99 + gen,
                100 + gen
        );
    }

    private static ObjList<LiveViewCheckpointInMemoryPayloadStore.PayloadRoot> roots(
            LiveViewCheckpointInMemoryPayloadStore.PayloadRoot root
    ) {
        final ObjList<LiveViewCheckpointInMemoryPayloadStore.PayloadRoot> list = new ObjList<>();
        list.add(root);
        return list;
    }
}
