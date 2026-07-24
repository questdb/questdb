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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.lv.LiveViewCheckpointPageCache;
import io.questdb.cairo.lv.LiveViewCheckpointPageCacheBudget;
import io.questdb.cairo.lv.LiveViewCheckpointRangeRingStateReader;
import io.questdb.cairo.lv.LiveViewCheckpointStateCodec;
import io.questdb.cairo.lv.LiveViewCheckpointStatePageRef;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LiveViewCheckpointPageCacheTest extends AbstractCairoTest {

    private static final int MAX_PAGE_BYTES = LiveViewCheckpointStateCodec.CHUNK_ROWS * Long.BYTES;
    private static final int TIMESTAMP_KIND = LiveViewCheckpointRangeRingStateReader.TIMESTAMP_PAGE_KIND;
    private static final int VALUE_KIND = LiveViewCheckpointRangeRingStateReader.DOUBLE_VALUE_PAGE_KIND;

    @Test
    public void testAdmissionFractionSelectsTheSameSubsetEverywhere() throws Exception {
        assertMemoryLeak(() -> {
            final int pages = 4096;
            final int pageBytes = 64;
            final long address = allocScratch(pageBytes, 1);
            try {
                final boolean[] first = new boolean[pages];
                final boolean[] second = new boolean[pages];
                admitAll(0.25, pages, pageBytes, address, first, false);
                // A cache that sees the same pages in the reverse order must pin
                // the same subset: the decision is a hash of the page identity,
                // not of the order or of what the cache already holds.
                admitAll(0.25, pages, pageBytes, address, second, true);

                int admitted = 0;
                for (int i = 0; i < pages; i++) {
                    Assert.assertEquals("page " + i, first[i], second[i]);
                    if (first[i]) {
                        admitted++;
                    }
                }
                // A quarter of 4096 keys, give or take the hash's own spread.
                Assert.assertTrue("admitted=" + admitted, admitted > 900 && admitted < 1150);
            } finally {
                Unsafe.free(address, pageBytes, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testAdmissionFractionZeroKeepsServingWhatItHolds() throws Exception {
        assertMemoryLeak(() -> {
            final int pageBytes = 512;
            final long address = allocScratch(pageBytes, 7);
            final LiveViewCheckpointPageCacheBudget budget =
                    new LiveViewCheckpointPageCacheBudget(64L * LiveViewCheckpointPageCache.SLAB_BYTES);
            try (LiveViewCheckpointPageCache cache = new LiveViewCheckpointPageCache(budget)) {
                Assert.assertTrue(cache.admit(ref(1, 0, TIMESTAMP_KIND, pageBytes), address));

                cache.setAdmissionFraction(0);
                Assert.assertEquals(0.0, cache.getAdmissionFraction(), 0.0);
                Assert.assertFalse(cache.admit(ref(1, pageBytes, TIMESTAMP_KIND, pageBytes), address));
                Assert.assertEquals(1, cache.getPageCount());
                // Closing admission does not close the door on the pages already in.
                Assert.assertTrue(cache.probe(ref(1, 0, TIMESTAMP_KIND, pageBytes)) != 0);

                cache.setAdmissionFraction(1);
                Assert.assertEquals(1.0, cache.getAdmissionFraction(), 0.0);
                Assert.assertTrue(cache.admit(ref(1, pageBytes, TIMESTAMP_KIND, pageBytes), address));
            } finally {
                Unsafe.free(address, pageBytes, MemoryTag.NATIVE_DEFAULT);
            }
            Assert.assertEquals(0, budget.getUsedBytes());
        });
    }

    @Test
    public void testAdmitCopiesTheImageAndProbeReturnsIt() throws Exception {
        assertMemoryLeak(() -> {
            final int pageBytes = 1024;
            final long address = allocScratch(pageBytes, 42);
            final LiveViewCheckpointPageCacheBudget budget =
                    new LiveViewCheckpointPageCacheBudget(LiveViewCheckpointPageCache.SLAB_BYTES);
            try (LiveViewCheckpointPageCache cache = new LiveViewCheckpointPageCache(budget)) {
                final LiveViewCheckpointStatePageRef ref = ref(7, 4096, TIMESTAMP_KIND, pageBytes);
                Assert.assertEquals(0, cache.probe(ref));
                Assert.assertEquals(0, cache.getHits());
                Assert.assertEquals(1, cache.getMisses());

                Assert.assertTrue(cache.admit(ref, address));
                Assert.assertEquals(1, cache.getPageCount());

                final long cached = cache.probe(ref);
                Assert.assertTrue(cached != 0);
                Assert.assertNotEquals(address, cached);
                assertPattern(cached, pageBytes, 42);
                Assert.assertEquals(1, cache.getHits());
                Assert.assertEquals(1, cache.getMisses());

                // The cache took a copy, so the caller is free to reuse its scratch.
                fillPattern(address, pageBytes, 99);
                assertPattern(cache.probe(ref), pageBytes, 42);

                // A second admission of the same immutable page is a no-op.
                Assert.assertTrue(cache.admit(ref, address));
                Assert.assertEquals(1, cache.getPageCount());
                assertPattern(cache.probe(ref), pageBytes, 42);
            } finally {
                Unsafe.free(address, pageBytes, MemoryTag.NATIVE_DEFAULT);
            }
            Assert.assertEquals(0, budget.getUsedBytes());
            Assert.assertEquals(0, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_CHECKPOINT_CACHE));
        });
    }

    @Test
    public void testAdmitRejectsPagesOutsideTheStatePageShape() throws Exception {
        assertMemoryLeak(() -> {
            final int pageBytes = MAX_PAGE_BYTES;
            final long address = allocScratch(pageBytes, 3);
            final LiveViewCheckpointPageCacheBudget budget =
                    new LiveViewCheckpointPageCacheBudget(64L * LiveViewCheckpointPageCache.SLAB_BYTES);
            try (LiveViewCheckpointPageCache cache = new LiveViewCheckpointPageCache(budget)) {
                Assert.assertFalse(cache.admit(ref(1, 0, TIMESTAMP_KIND, pageBytes), 0));
                Assert.assertFalse(cache.admit(
                        ref(LiveViewCheckpointStatePageRef.NULL_SEGMENT_ID, 0, TIMESTAMP_KIND, pageBytes),
                        address
                ));
                Assert.assertFalse(cache.admit(ref(1, 0, TIMESTAMP_KIND, 0), address));
                Assert.assertFalse(cache.admit(ref(1, 0, TIMESTAMP_KIND, MAX_PAGE_BYTES + 1), address));
                Assert.assertEquals(0, cache.getPageCount());
                Assert.assertEquals(0, cache.getUsedBytes());

                Assert.assertTrue(cache.admit(ref(1, 0, TIMESTAMP_KIND, MAX_PAGE_BYTES), address));
            } finally {
                Unsafe.free(address, pageBytes, MemoryTag.NATIVE_DEFAULT);
            }
            Assert.assertEquals(0, budget.getUsedBytes());
        });
    }

    @Test
    public void testBudgetIsSharedAndNeverExceeded() throws Exception {
        assertMemoryLeak(() -> {
            // Two slabs of the widest slot class: four pages between both caches.
            final int pageBytes = MAX_PAGE_BYTES;
            final long capacity = 2L * LiveViewCheckpointPageCache.SLAB_BYTES;
            final long address = allocScratch(pageBytes, 11);
            final LiveViewCheckpointPageCacheBudget budget = new LiveViewCheckpointPageCacheBudget(capacity);
            try (
                    LiveViewCheckpointPageCache first = new LiveViewCheckpointPageCache(budget);
                    LiveViewCheckpointPageCache second = new LiveViewCheckpointPageCache(budget)
            ) {
                Assert.assertTrue(first.admit(ref(1, 0, TIMESTAMP_KIND, pageBytes), address));
                Assert.assertTrue(first.admit(ref(1, pageBytes, TIMESTAMP_KIND, pageBytes), address));
                Assert.assertEquals(LiveViewCheckpointPageCache.SLAB_BYTES, first.getUsedBytes());

                Assert.assertTrue(second.admit(ref(2, 0, TIMESTAMP_KIND, pageBytes), address));
                Assert.assertTrue(second.admit(ref(2, pageBytes, TIMESTAMP_KIND, pageBytes), address));
                Assert.assertEquals(capacity, budget.getUsedBytes());

                // The budget is engine-wide, so whichever cache asks next is the
                // one that goes without.
                Assert.assertFalse(first.admit(ref(1, 2L * pageBytes, TIMESTAMP_KIND, pageBytes), address));
                Assert.assertFalse(second.admit(ref(2, 2L * pageBytes, TIMESTAMP_KIND, pageBytes), address));
                Assert.assertEquals(capacity, budget.getUsedBytes());
                Assert.assertEquals(capacity, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_CHECKPOINT_CACHE));

                // Freeing one cache hands its slabs back for the other to take.
                first.close();
                Assert.assertEquals(LiveViewCheckpointPageCache.SLAB_BYTES, budget.getUsedBytes());
                Assert.assertTrue(second.admit(ref(2, 2L * pageBytes, TIMESTAMP_KIND, pageBytes), address));
            } finally {
                Unsafe.free(address, pageBytes, MemoryTag.NATIVE_DEFAULT);
            }
            Assert.assertEquals(0, budget.getUsedBytes());
            Assert.assertEquals(0, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_CHECKPOINT_CACHE));
        });
    }

    @Test
    public void testBumpEpochDropsEveryPageAndReusesTheSlabs() throws Exception {
        assertMemoryLeak(() -> {
            final int pageBytes = 2048;
            final int pages = 200;
            final long address = allocScratch(pageBytes, 5);
            final LiveViewCheckpointPageCacheBudget budget =
                    new LiveViewCheckpointPageCacheBudget(64L * LiveViewCheckpointPageCache.SLAB_BYTES);
            try (LiveViewCheckpointPageCache cache = new LiveViewCheckpointPageCache(budget)) {
                for (int i = 0; i < pages; i++) {
                    Assert.assertTrue(cache.admit(ref(9, (long) i * pageBytes, TIMESTAMP_KIND, pageBytes), address));
                }
                final long warmBytes = cache.getUsedBytes();
                Assert.assertEquals(pages, cache.getPageCount());
                Assert.assertEquals(0, cache.getEpoch());

                cache.bumpEpoch();

                Assert.assertEquals(1, cache.getEpoch());
                Assert.assertEquals(0, cache.getPageCount());
                // A rebuilt timeline re-mints segment ids from the bottom, so no
                // page the old epoch cached may answer for the new one.
                for (int i = 0; i < pages; i++) {
                    Assert.assertEquals(0, cache.probe(ref(9, (long) i * pageBytes, TIMESTAMP_KIND, pageBytes)));
                }
                // The slabs stay with the cache, so refilling costs the budget nothing.
                Assert.assertEquals(warmBytes, cache.getUsedBytes());
                for (int i = 0; i < pages; i++) {
                    Assert.assertTrue(cache.admit(ref(9, (long) i * pageBytes, TIMESTAMP_KIND, pageBytes), address));
                }
                Assert.assertEquals(warmBytes, cache.getUsedBytes());
                Assert.assertEquals(pages, cache.getPageCount());
            } finally {
                Unsafe.free(address, pageBytes, MemoryTag.NATIVE_DEFAULT);
            }
            Assert.assertEquals(0, budget.getUsedBytes());
            Assert.assertEquals(0, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_CHECKPOINT_CACHE));
        });
    }

    @Test
    public void testDisabledBudgetAdmitsNothing() throws Exception {
        assertMemoryLeak(() -> {
            final int pageBytes = 128;
            final long address = allocScratch(pageBytes, 1);
            final LiveViewCheckpointPageCacheBudget budget = new LiveViewCheckpointPageCacheBudget(0);
            Assert.assertFalse(budget.isEnabled());
            try (LiveViewCheckpointPageCache cache = new LiveViewCheckpointPageCache(budget)) {
                final LiveViewCheckpointStatePageRef ref = ref(1, 0, TIMESTAMP_KIND, pageBytes);
                Assert.assertFalse(cache.admit(ref, address));
                Assert.assertEquals(0, cache.probe(ref));
                Assert.assertEquals(0, cache.getUsedBytes());
                Assert.assertEquals(0, cache.getPageCount());
            } finally {
                Unsafe.free(address, pageBytes, MemoryTag.NATIVE_DEFAULT);
            }
            Assert.assertEquals(0, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_CHECKPOINT_CACHE));
        });
    }

    @Test
    public void testEvictSegmentDropsOnlyThatSegment() throws Exception {
        assertMemoryLeak(() -> {
            final int pageBytes = 512;
            final int pagesPerSegment = 50;
            final long address = allocScratch(pageBytes, 21);
            final LiveViewCheckpointPageCacheBudget budget =
                    new LiveViewCheckpointPageCacheBudget(64L * LiveViewCheckpointPageCache.SLAB_BYTES);
            try (LiveViewCheckpointPageCache cache = new LiveViewCheckpointPageCache(budget)) {
                for (long segmentId = 1; segmentId <= 3; segmentId++) {
                    for (int i = 0; i < pagesPerSegment; i++) {
                        Assert.assertTrue(cache.admit(
                                ref(segmentId, (long) i * pageBytes, TIMESTAMP_KIND, pageBytes),
                                address
                        ));
                    }
                }
                Assert.assertEquals(3 * pagesPerSegment, cache.getPageCount());

                cache.evictSegment(2);

                Assert.assertEquals(2 * pagesPerSegment, cache.getPageCount());
                for (int i = 0; i < pagesPerSegment; i++) {
                    Assert.assertTrue(cache.probe(ref(1, (long) i * pageBytes, TIMESTAMP_KIND, pageBytes)) != 0);
                    Assert.assertEquals(0, cache.probe(ref(2, (long) i * pageBytes, TIMESTAMP_KIND, pageBytes)));
                    Assert.assertTrue(cache.probe(ref(3, (long) i * pageBytes, TIMESTAMP_KIND, pageBytes)) != 0);
                }
                // Evicting a segment nothing was cached from changes nothing.
                cache.evictSegment(42);
                Assert.assertEquals(2 * pagesPerSegment, cache.getPageCount());
            } finally {
                Unsafe.free(address, pageBytes, MemoryTag.NATIVE_DEFAULT);
            }
            Assert.assertEquals(0, budget.getUsedBytes());
            Assert.assertEquals(0, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_CHECKPOINT_CACHE));
        });
    }

    @Test
    public void testProbeWithAContradictingRefMissesAndDropsTheEntry() throws Exception {
        assertMemoryLeak(() -> {
            final int pageBytes = 256;
            final long address = allocScratch(pageBytes, 8);
            final LiveViewCheckpointPageCacheBudget budget =
                    new LiveViewCheckpointPageCacheBudget(64L * LiveViewCheckpointPageCache.SLAB_BYTES);
            try (LiveViewCheckpointPageCache cache = new LiveViewCheckpointPageCache(budget)) {
                final int rows = pageBytes / Long.BYTES;
                final LiveViewCheckpointStatePageRef ref = new LiveViewCheckpointStatePageRef()
                        .of(4, 128, pageBytes, pageBytes, TIMESTAMP_KIND,
                                LiveViewCheckpointStateCodec.TIMESTAMP_RAW_64, rows, 0);
                Assert.assertTrue(cache.admit(ref, address));

                // Same page identity, a codec that disagrees: the bytes behind an
                // immutable segment cannot have changed, so one ref is wrong.
                final LiveViewCheckpointStatePageRef otherCodec = new LiveViewCheckpointStatePageRef()
                        .of(4, 128, pageBytes, pageBytes, TIMESTAMP_KIND,
                                LiveViewCheckpointStateCodec.TIMESTAMP_DELTA_OF_DELTA_VARINT, rows, 0);
                Assert.assertEquals(0, cache.probe(otherCodec));
                Assert.assertEquals(0, cache.getPageCount());
                Assert.assertEquals(0, cache.getHits());

                Assert.assertTrue(cache.admit(ref, address));
                final LiveViewCheckpointStatePageRef otherRowCount = new LiveViewCheckpointStatePageRef()
                        .of(4, 128, pageBytes, pageBytes, TIMESTAMP_KIND,
                                LiveViewCheckpointStateCodec.TIMESTAMP_RAW_64, rows - 1, 0);
                Assert.assertEquals(0, cache.probe(otherRowCount));
                Assert.assertEquals(0, cache.getPageCount());

                Assert.assertTrue(cache.admit(ref, address));
                final LiveViewCheckpointStatePageRef otherLength = new LiveViewCheckpointStatePageRef()
                        .of(4, 128, pageBytes, pageBytes / 2, TIMESTAMP_KIND,
                                LiveViewCheckpointStateCodec.TIMESTAMP_RAW_64, rows, 0);
                Assert.assertEquals(0, cache.probe(otherLength));
                Assert.assertEquals(0, cache.getPageCount());

                // A page kind is part of the identity, so it reads as another page
                // rather than as a contradiction.
                Assert.assertTrue(cache.admit(ref, address));
                Assert.assertEquals(0, cache.probe(ref(4, 128, VALUE_KIND, pageBytes)));
                Assert.assertEquals(1, cache.getPageCount());
            } finally {
                Unsafe.free(address, pageBytes, MemoryTag.NATIVE_DEFAULT);
            }
            Assert.assertEquals(0, budget.getUsedBytes());
            Assert.assertEquals(0, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_CHECKPOINT_CACHE));
        });
    }

    @Test
    public void testRandomAdmitEvictNeverServesTheWrongBytes() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = new Rnd();
            final long capacity = 96L * LiveViewCheckpointPageCache.SLAB_BYTES;
            final LiveViewCheckpointPageCacheBudget budget = new LiveViewCheckpointPageCacheBudget(capacity);
            final Map<String, Integer> expected = new HashMap<>();
            final List<LiveViewCheckpointStatePageRef> refs = new ArrayList<>();
            final int[] seeds = new int[512];
            final long scratch = Unsafe.malloc(MAX_PAGE_BYTES, MemoryTag.NATIVE_DEFAULT);
            try (LiveViewCheckpointPageCache cache = new LiveViewCheckpointPageCache(budget)) {
                for (int i = 0; i < seeds.length; i++) {
                    final long segmentId = rnd.nextPositiveInt() % 8;
                    final int rows = 1 + rnd.nextPositiveInt() % 512;
                    final int pageKind = (i & 1) == 0 ? TIMESTAMP_KIND : VALUE_KIND;
                    refs.add(ref(segmentId, (long) i * MAX_PAGE_BYTES, pageKind, rows * Long.BYTES));
                    seeds[i] = i + 1;
                }

                for (int op = 0; op < 20_000; op++) {
                    final int i = rnd.nextPositiveInt() % seeds.length;
                    final LiveViewCheckpointStatePageRef ref = refs.get(i);
                    final String key = keyOf(ref);
                    final int roll = rnd.nextPositiveInt() % 100;
                    if (roll < 55) {
                        final long cached = cache.probe(ref);
                        final Integer seed = expected.get(key);
                        if (seed == null) {
                            if (cached != 0) {
                                Assert.fail("op " + op + " served a page the cache does not hold, key=" + key);
                            }
                        } else {
                            if (cached == 0) {
                                Assert.fail("op " + op + " lost a page the cache took, key=" + key);
                            }
                            assertPattern(cached, ref.getDecodedLength(), seed);
                        }
                    } else if (roll < 90) {
                        fillPattern(scratch, ref.getDecodedLength(), seeds[i]);
                        if (cache.admit(ref, scratch)) {
                            expected.put(key, seeds[i]);
                        }
                    } else if (roll < 98) {
                        final long segmentId = rnd.nextPositiveInt() % 8;
                        cache.evictSegment(segmentId);
                        expected.keySet().removeIf(k -> k.startsWith(segmentId + ":"));
                    } else {
                        cache.bumpEpoch();
                        expected.clear();
                    }
                    if (expected.size() != cache.getPageCount() || budget.getUsedBytes() > capacity) {
                        Assert.fail("op " + op + " [pages=" + cache.getPageCount()
                                + ", expected=" + expected.size()
                                + ", budget=" + budget.getUsedBytes() + ']');
                    }
                }

                Assert.assertTrue("the run must exercise more than a handful of pages", expected.size() > 16);
                Assert.assertEquals(budget.getUsedBytes(), cache.getUsedBytes());
                Assert.assertEquals(
                        budget.getUsedBytes(),
                        Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_CHECKPOINT_CACHE)
                );
            } finally {
                Unsafe.free(scratch, MAX_PAGE_BYTES, MemoryTag.NATIVE_DEFAULT);
            }
            Assert.assertEquals(0, budget.getUsedBytes());
            Assert.assertEquals(0, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_CHECKPOINT_CACHE));
        });
    }

    @Test
    public void testReleaseBeyondAcquiredRaises() {
        final LiveViewCheckpointPageCacheBudget budget = new LiveViewCheckpointPageCacheBudget(1024);
        Assert.assertTrue(budget.tryAcquire(512));
        try {
            budget.release(768);
            Assert.fail();
        } catch (Exception e) {
            TestUtils.assertContains(e.getMessage(), "released more than acquired");
        }
        Assert.assertEquals(512, budget.getUsedBytes());
        budget.release(512);
        Assert.assertEquals(0, budget.getUsedBytes());
    }

    private static void admitAll(
            double fraction,
            int pages,
            int pageBytes,
            long address,
            boolean[] decisions,
            boolean reverse
    ) {
        final LiveViewCheckpointPageCacheBudget budget =
                new LiveViewCheckpointPageCacheBudget(1024L * LiveViewCheckpointPageCache.SLAB_BYTES);
        try (LiveViewCheckpointPageCache cache = new LiveViewCheckpointPageCache(budget)) {
            cache.setAdmissionFraction(fraction);
            for (int i = 0; i < pages; i++) {
                final int page = reverse ? pages - 1 - i : i;
                decisions[page] = cache.admit(
                        ref(page % 16, (long) page * pageBytes, TIMESTAMP_KIND, pageBytes),
                        address
                );
            }
        }
        Assert.assertEquals(0, budget.getUsedBytes());
    }

    private static long allocScratch(int pageBytes, int seed) {
        final long address = Unsafe.malloc(pageBytes, MemoryTag.NATIVE_DEFAULT);
        fillPattern(address, pageBytes, seed);
        return address;
    }

    private static void assertPattern(long address, int length, int seed) {
        for (int i = 0; i < length; i++) {
            final byte actual = Unsafe.getByte(address + i);
            if (actual != (byte) (seed + i)) {
                Assert.fail("byte " + i + " [expected=" + (byte) (seed + i) + ", actual=" + actual + ']');
            }
        }
    }

    private static void fillPattern(long address, int length, int seed) {
        for (int i = 0; i < length; i++) {
            Unsafe.putByte(address + i, (byte) (seed + i));
        }
    }

    private static String keyOf(LiveViewCheckpointStatePageRef ref) {
        return ref.getSegmentId() + ":" + ref.getOffset() + ":" + ref.getPageKind();
    }

    private static LiveViewCheckpointStatePageRef ref(long segmentId, long offset, int pageKind, int decodedLength) {
        return new LiveViewCheckpointStatePageRef().of(
                segmentId,
                offset,
                decodedLength,
                decodedLength,
                pageKind,
                LiveViewCheckpointStateCodec.TIMESTAMP_RAW_64,
                Math.max(1, decodedLength / Long.BYTES),
                0
        );
    }
}
