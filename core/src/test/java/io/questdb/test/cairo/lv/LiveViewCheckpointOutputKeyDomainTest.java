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

import com.sun.management.ThreadMXBean;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.lv.LiveViewCheckpointOutputKeyDomain;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.std.Hash;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Coverage for {@code Q}, the output key domain a localized repair publishes against.
 * <p>
 * Two properties carry it, and they pull in opposite directions. The domain has to match
 * an encoded partition key by its CONTENT - the key a seal probes with is encoded afresh
 * off a map record, never the bytes the repair plan put in - and it has to do that
 * without allocating, because every seal loop probes it once per key per function root,
 * so the probe count is the key domain times the roots.
 * <p>
 * The domain keeps its keys in native memory it owns, so the cases below also pin what
 * that ownership promises: a copy is independent of its source, every native byte comes
 * back on close and on a restore to the initial capacity, and a domain filled by the same
 * operations iterates its keys in exactly the slot order the heap table it replaced did.
 */
public class LiveViewCheckpointOutputKeyDomainTest {
    private static final int COPY_COUNT = 4;
    private static final int DOMAIN_SIZE = 512;
    // Past the retention bound the keyed replay and the repair plan keep, so a table this
    // domain grows is one only an unbounded owner would still hold.
    private static final int GROWN_DOMAIN_SIZE = 20_000;
    private static final int NARROW_DOMAIN_SIZE = 100;
    // Well above any per-probe allocation, well below what one wrapper per probe costs:
    // a 16-byte-header wrapper over 200k probes is several megabytes.
    private static final long PROBE_ALLOCATION_LIMIT_BYTES = 64 * 1024;
    private static final int PROBE_COUNT = 200_000;
    // A bound on a whole measured operation that does not grow with the key count: a few
    // object shells per operation fit in it many times over, while one allocation per key
    // of a WIDE_DOMAIN_SIZE domain does not fit at all.
    private static final long WIDE_ALLOCATION_LIMIT_BYTES = 16 * 1024;
    private static final int WIDE_DOMAIN_SIZE = 4_096;
    private static final int WIDE_KEY_BYTES = 12;

    @Test
    public void testACopyIsSizedForItsKeysWhateverItsSourceGrewTo() throws Exception {
        // The worker's repair plan and keyed replay are reused from repair to repair, and
        // clear() keeps the table the widest domain grew. The session and the capture each
        // copy Q out of them, so a copy that took its source's table would cost every narrow
        // repair the peak an earlier wide one left behind, for as long as the capture or the
        // session holds it.
        TestUtils.assertMemoryLeak(() -> {
            try (
                    LiveViewCheckpointOutputKeyDomain fresh = new LiveViewCheckpointOutputKeyDomain();
                    LiveViewCheckpointOutputKeyDomain reused = new LiveViewCheckpointOutputKeyDomain()
            ) {
                for (int i = 0; i < NARROW_DOMAIN_SIZE; i++) {
                    add(fresh, key(i));
                }
                final long freshCopyBytes = nativeBytesOfCopy(fresh);
                Assert.assertTrue(freshCopyBytes > 0);

                for (int i = 0; i < GROWN_DOMAIN_SIZE; i++) {
                    add(reused, key(i));
                }
                reused.clear();
                for (int i = 0; i < NARROW_DOMAIN_SIZE; i++) {
                    add(reused, key(i));
                }
                Assert.assertEquals(
                        "a copy of a narrow domain must cost what its keys need, not what its source grew to",
                        freshCopyBytes,
                        nativeBytesOfCopy(reused)
                );

                reused.clear();
                Assert.assertEquals("a copy of an empty domain holds nothing", 0, nativeBytesOfCopy(reused));
            }
        });
    }

    @Test
    public void testACopyPeaksAtTheBytesItKeeps() throws Exception {
        // A copy that grew its table on the way would hold the old and the new one at once,
        // so the process would need more memory to take the copy than the copy keeps, and
        // under the RSS ceiling a copy whose kept bytes fit could still be refused. A copy of
        // Q is process memory tagged NATIVE_LIVE_VIEW_IN_MEM and charges no view's refresh
        // tracker, so the ceiling is the limit it meets. The copy allocates its storage once:
        // the tightest ceiling that admits it is exactly the bytes it keeps, and one byte
        // less refuses it cleanly.
        TestUtils.assertMemoryLeak(() -> {
            final int[] keyCounts = {1, 100, 1_000, 1_024, 16_384};
            for (int keyCount : keyCounts) {
                try (LiveViewCheckpointOutputKeyDomain source = new LiveViewCheckpointOutputKeyDomain()) {
                    for (int i = 0; i < keyCount; i++) {
                        add(source, key(i));
                    }
                    final long kept = nativeBytesOfCopy(source);
                    Assert.assertTrue(kept > 0);
                    try (LiveViewCheckpointOutputKeyDomain copy = new LiveViewCheckpointOutputKeyDomain()) {
                        final long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
                        Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + kept);
                        try {
                            copy.copyFrom(source);
                        } finally {
                            Unsafe.setRssMemLimit(0);
                        }
                        Assert.assertEquals(keyCount, copy.size());
                        Assert.assertEquals(
                                "[keys=" + keyCount + ']',
                                kept,
                                Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM) - before
                        );
                        for (int i = 0; i < keyCount; i++) {
                            Assert.assertTrue(LiveViewCheckpointTestKeys.contains(copy, key(i)));
                        }
                    }
                    try (LiveViewCheckpointOutputKeyDomain copy = new LiveViewCheckpointOutputKeyDomain()) {
                        final long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
                        Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + kept - 1);
                        try {
                            copy.copyFrom(source);
                            Assert.fail("a ceiling below the copy's bytes must refuse it [keys=" + keyCount + ']');
                        } catch (CairoException e) {
                            Assert.assertTrue(e.isOutOfMemory());
                        } finally {
                            Unsafe.setRssMemLimit(0);
                        }
                        Assert.assertEquals(0, copy.size());
                        Assert.assertEquals(
                                "a refused copy must keep nothing",
                                before,
                                Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM)
                        );
                    }
                }
            }
        });
    }

    @Test
    public void testACopyOwnsItsKeys() throws Exception {
        // The hazard the ownership exists for: a parked capture holds Q while the keyed
        // replay or plan it was copied from is cleared and refilled for the next repair.
        // The refill lands on exactly the storage the first fill used, so a copy that
        // shared it would silently start answering for the new keys.
        TestUtils.assertMemoryLeak(() -> {
            try (
                    LiveViewCheckpointOutputKeyDomain source = new LiveViewCheckpointOutputKeyDomain();
                    LiveViewCheckpointOutputKeyDomain copy = new LiveViewCheckpointOutputKeyDomain()
            ) {
                for (int i = 0; i < 100; i++) {
                    add(source, key(i));
                }
                copy.copyFrom(source);
                source.clear();
                for (int i = 1_000; i < 1_100; i++) {
                    add(source, key(i));
                }
                Assert.assertEquals(100, copy.size());
                for (int i = 0; i < 100; i++) {
                    Assert.assertTrue("the copy lost key " + i, LiveViewCheckpointTestKeys.contains(copy, key(i)));
                    Assert.assertFalse("the copy answers for the refill's key " + (1_000 + i), LiveViewCheckpointTestKeys.contains(copy, key(1_000 + i)));
                    Assert.assertTrue(LiveViewCheckpointTestKeys.contains(source, key(1_000 + i)));
                    Assert.assertFalse(LiveViewCheckpointTestKeys.contains(source, key(i)));
                }

                // Freeing the source's storage outright must not reach the copy either.
                source.restoreInitialCapacity();
                source.close();
                for (int i = 0; i < 100; i++) {
                    Assert.assertTrue(LiveViewCheckpointTestKeys.contains(copy, key(i)));
                }
            }
        });
    }

    @Test
    public void testAKeyIsMatchedByContentRatherThanByIdentity() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (LiveViewCheckpointOutputKeyDomain domain = new LiveViewCheckpointOutputKeyDomain()) {
                add(domain, new byte[]{1, 2, 3});

                // The probing side never holds the bytes the domain was given: a seal
                // encodes the key afresh off its own map record. Matching on identity would
                // answer false here and silently drop every key from the published root.
                Assert.assertTrue(LiveViewCheckpointTestKeys.contains(domain, new byte[]{1, 2, 3}));
                Assert.assertFalse(LiveViewCheckpointTestKeys.contains(domain, new byte[]{1, 2, 4}));
                // A prefix is a different key, and so is the same content one byte longer.
                Assert.assertFalse(LiveViewCheckpointTestKeys.contains(domain, new byte[]{1, 2}));
                Assert.assertFalse(LiveViewCheckpointTestKeys.contains(domain, new byte[]{1, 2, 3, 0}));
            }
        });
    }

    @Test
    public void testAnAbortedKeyLeavesNoTrace() throws Exception {
        // A producer whose encoder throws part-way drops the key it began. Neither the
        // key nor the bytes it had written may reach the domain.
        TestUtils.assertMemoryLeak(() -> {
            try (LiveViewCheckpointOutputKeyDomain domain = new LiveViewCheckpointOutputKeyDomain()) {
                add(domain, new byte[]{1});
                final MemoryA sink = domain.beginKey();
                sink.putByte((byte) 2);
                sink.putByte((byte) 3);
                domain.abortKey();
                Assert.assertEquals(1, domain.size());
                Assert.assertFalse(LiveViewCheckpointTestKeys.contains(domain, new byte[]{2, 3}));
                Assert.assertFalse(LiveViewCheckpointTestKeys.contains(domain, new byte[]{2}));

                // A key begun and never ended - its encoder threw with nobody to abort - is
                // dropped by the next begin.
                domain.beginKey().putByte((byte) 9);
                add(domain, new byte[]{4, 5});
                Assert.assertEquals(2, domain.size());
                Assert.assertTrue(LiveViewCheckpointTestKeys.contains(domain, new byte[]{4, 5}));
                Assert.assertFalse(LiveViewCheckpointTestKeys.contains(domain, new byte[]{9}));
                Assert.assertFalse(LiveViewCheckpointTestKeys.contains(domain, new byte[]{9, 4, 5}));
                Assert.assertTrue(LiveViewCheckpointTestKeys.contains(domain, new byte[]{1}));
                int walked = 0;
                for (int slot = 0, n = domain.getSlotCount(); slot < n; slot++) {
                    if (domain.isSlotUsed(slot)) {
                        final byte[] key = keyAt(domain, slot);
                        Assert.assertTrue(Arrays.toString(key), Arrays.equals(new byte[]{1}, key) || Arrays.equals(new byte[]{4, 5}, key));
                        walked++;
                    }
                }
                Assert.assertEquals("the walk must see exactly the two committed keys", 2, walked);
            }
        });
    }

    @Test
    public void testAnEmptyKeyIsAKeyLikeAnyOther() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (LiveViewCheckpointOutputKeyDomain domain = new LiveViewCheckpointOutputKeyDomain()) {
                Assert.assertFalse(LiveViewCheckpointTestKeys.contains(domain, new byte[0]));
                add(domain, new byte[0]);
                Assert.assertTrue(LiveViewCheckpointTestKeys.contains(domain, new byte[0]));
                Assert.assertEquals(1, domain.size());
                Assert.assertFalse(domain.isEmpty());
                // ...and it must not answer for every other key as a null-ish slot marker would.
                Assert.assertFalse(LiveViewCheckpointTestKeys.contains(domain, new byte[]{0}));
            }
        });
    }

    @Test
    public void testCloseAndRestoreInitialCapacityFreeEveryNativeByte() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
            try (LiveViewCheckpointOutputKeyDomain domain = new LiveViewCheckpointOutputKeyDomain()) {
                Assert.assertEquals("a domain holds nothing before its first key", baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM));
                for (int i = 0; i < 2_000; i++) {
                    add(domain, key(i));
                }
                Assert.assertTrue(Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM) > baseline);

                // What the keyed replay does past its retention bound: back to the lazy,
                // empty state, holding no native memory at all.
                domain.restoreInitialCapacity();
                Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM));
                Assert.assertEquals(0, domain.size());
                Assert.assertEquals(0, domain.getSlotCount());
                Assert.assertFalse(LiveViewCheckpointTestKeys.contains(domain, key(0)));

                // ...and still usable.
                add(domain, key(7));
                Assert.assertTrue(LiveViewCheckpointTestKeys.contains(domain, key(7)));
                Assert.assertEquals(1, domain.size());

                // clear() keeps the storage it grew, close() frees it, and a second close is
                // a no-op.
                domain.clear();
                Assert.assertTrue(Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM) > baseline);
                domain.close();
                Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM));
                domain.close();
                Assert.assertEquals(0, domain.size());
                Assert.assertFalse(LiveViewCheckpointTestKeys.contains(domain, key(7)));
            }
            Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM));
        });
    }

    @Test
    public void testCopyFromReplacesTheDomainWholesale() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (
                    LiveViewCheckpointOutputKeyDomain source = new LiveViewCheckpointOutputKeyDomain();
                    LiveViewCheckpointOutputKeyDomain target = new LiveViewCheckpointOutputKeyDomain()
            ) {
                add(source, new byte[]{7});
                add(source, new byte[]{8});
                add(target, new byte[]{9});
                target.copyFrom(source);

                Assert.assertEquals(2, target.size());
                Assert.assertTrue(LiveViewCheckpointTestKeys.contains(target, new byte[]{7}));
                Assert.assertTrue(LiveViewCheckpointTestKeys.contains(target, new byte[]{8}));
                Assert.assertFalse("copyFrom replaces rather than merges", LiveViewCheckpointTestKeys.contains(target, new byte[]{9}));

                // The capture owns its copy: the plan is refilled by the next repair while a
                // parked capture still owes its publication.
                source.clear();
                Assert.assertTrue(LiveViewCheckpointTestKeys.contains(target, new byte[]{7}));
                Assert.assertEquals(0, source.size());
                Assert.assertTrue(source.isEmpty());

                // Copying an empty domain that never allocated empties the target too.
                try (LiveViewCheckpointOutputKeyDomain empty = new LiveViewCheckpointOutputKeyDomain()) {
                    target.copyFrom(empty);
                    Assert.assertTrue(target.isEmpty());
                    Assert.assertFalse(LiveViewCheckpointTestKeys.contains(target, new byte[]{7}));
                }
            }
        });
    }

    @Test
    public void testCopyingAWideDomainIntoAFreshOneAllocatesNoHeapPerKey() throws Exception {
        // A repair capture takes its own copy of Q into a domain of its own, once per
        // capture. A copy that re-inserted key by key into heap slot arrays would charge
        // every capture heap in proportion to the key domain.
        TestUtils.assertMemoryLeak(() -> {
            try (
                    TestUtils.ThreadMetricsScope<ThreadMXBean> scope = TestUtils.threadAllocationScope();
                    LiveViewCheckpointOutputKeyDomain source = new LiveViewCheckpointOutputKeyDomain()
            ) {
                final ThreadMXBean threadMXBean = scope.getBean();
                for (int i = 0; i < WIDE_DOMAIN_SIZE; i++) {
                    add(source, key(i));
                }
                for (int i = 0; i < COPY_COUNT; i++) {
                    try (LiveViewCheckpointOutputKeyDomain warmUp = new LiveViewCheckpointOutputKeyDomain()) {
                        warmUp.copyFrom(source);
                        Assert.assertEquals(WIDE_DOMAIN_SIZE, warmUp.size());
                    }
                }

                final long threadId = Thread.currentThread().threadId();
                final long before = threadMXBean.getThreadAllocatedBytes(threadId);
                long copiedKeys = 0;
                for (int i = 0; i < COPY_COUNT; i++) {
                    try (LiveViewCheckpointOutputKeyDomain copy = new LiveViewCheckpointOutputKeyDomain()) {
                        copy.copyFrom(source);
                        copiedKeys += copy.size();
                    }
                }
                final long allocated = threadMXBean.getThreadAllocatedBytes(threadId) - before;

                Assert.assertEquals((long) COPY_COUNT * WIDE_DOMAIN_SIZE, copiedKeys);
                Assert.assertTrue(
                        "copying a " + WIDE_DOMAIN_SIZE + "-key domain " + COPY_COUNT + " times allocated "
                                + allocated + " heap bytes; a capture's copy of Q must not cost heap per key",
                        allocated < WIDE_ALLOCATION_LIMIT_BYTES
                );
            }
        });
    }

    @Test
    public void testFillingProbingIteratingAndCopyingAWideDomainAllocatesNoHeapPerKey() throws Exception {
        // Every operation a repair runs on Q, over a domain as wide as the case's bound is
        // narrow: encoding keys in place, copying them in, probing them and walking them,
        // and copying the whole domain into one that already holds a domain this wide.
        TestUtils.assertMemoryLeak(() -> {
            final long keysAddress = Unsafe.malloc((long) WIDE_DOMAIN_SIZE * WIDE_KEY_BYTES, MemoryTag.NATIVE_DEFAULT);
            try (
                    TestUtils.ThreadMetricsScope<ThreadMXBean> scope = TestUtils.threadAllocationScope();
                    LiveViewCheckpointOutputKeyDomain encoded = new LiveViewCheckpointOutputKeyDomain();
                    LiveViewCheckpointOutputKeyDomain copied = new LiveViewCheckpointOutputKeyDomain();
                    LiveViewCheckpointOutputKeyDomain target = new LiveViewCheckpointOutputKeyDomain()
            ) {
                final ThreadMXBean threadMXBean = scope.getBean();
                for (int i = 0; i < WIDE_DOMAIN_SIZE; i++) {
                    final long address = keysAddress + (long) i * WIDE_KEY_BYTES;
                    Unsafe.putLong(address, wideKeyHead(i));
                    Unsafe.putInt(address + Long.BYTES, i);
                }
                long checksum = 0;
                for (int round = 0; round < 2; round++) {
                    checksum = runWideRound(encoded, copied, target, keysAddress);
                }

                final long threadId = Thread.currentThread().threadId();
                final long before = threadMXBean.getThreadAllocatedBytes(threadId);
                long measuredChecksum = 0;
                for (int round = 0; round < COPY_COUNT; round++) {
                    measuredChecksum += runWideRound(encoded, copied, target, keysAddress);
                }
                final long allocated = threadMXBean.getThreadAllocatedBytes(threadId) - before;

                Assert.assertEquals(checksum * COPY_COUNT, measuredChecksum);
                Assert.assertEquals(WIDE_DOMAIN_SIZE, target.size());
                Assert.assertTrue(
                        COPY_COUNT + " rounds over a " + WIDE_DOMAIN_SIZE + "-key domain allocated " + allocated
                                + " heap bytes; filling, probing, walking and copying Q must not allocate per key",
                        allocated < WIDE_ALLOCATION_LIMIT_BYTES
                );
            } finally {
                Unsafe.free(keysAddress, (long) WIDE_DOMAIN_SIZE * WIDE_KEY_BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testIterationOrderMatchesTheHeapTable() throws Exception {
        // The anchor restore inserts Q's keys into the anchor map in Q's slot order, so the
        // native table must place every key in the slot the heap table it replaced did, for
        // the same sequence of adds, duplicate adds, clears and restores. The oracle is a
        // copy of that heap table.
        TestUtils.assertMemoryLeak(() -> {
            final Rnd rnd = new Rnd(42, 7);
            final ObjList<byte[]> pool = new ObjList<>();
            for (int i = 0; i < 3_000; i++) {
                pool.add(poolKey(rnd, i));
            }
            final HeapOutputKeyDomain oracle = new HeapOutputKeyDomain();
            try (
                    LiveViewCheckpointOutputKeyDomain domain = new LiveViewCheckpointOutputKeyDomain();
                    LiveViewCheckpointOutputKeyDomain copy = new LiveViewCheckpointOutputKeyDomain()
            ) {
                for (int step = 0; step < 40_000; step++) {
                    final int op = rnd.nextInt(1_000);
                    if (op == 0) {
                        domain.clear();
                        oracle.clear();
                    } else if (op == 1) {
                        domain.restoreInitialCapacity();
                        oracle.restoreInitialCapacity();
                    } else {
                        // Skewed towards a narrow band so duplicates are common.
                        final int index = op < 500 ? rnd.nextInt(64) : rnd.nextInt(pool.size());
                        final byte[] key = pool.getQuick(index);
                        if (op % 2 == 0) {
                            add(domain, key);
                        } else {
                            addNative(domain, key);
                        }
                        oracle.add(key);
                    }
                    Assert.assertEquals("size at step " + step, oracle.size(), domain.size());
                    if (step % 997 == 0 || step == 39_999) {
                        assertSameSlotOrder("step " + step, oracle, domain);
                        // A copy holds its source's keys at the geometry the heap table reached
                        // when it re-added them into the fresh domain every repair copies into:
                        // sized for the keys it holds - not for what the source grew to, nor
                        // for what the copy held before. It is allocated at once rather than
                        // grown, so where two colliding keys land may differ, and no consumer
                        // of a copy reads that.
                        final HeapOutputKeyDomain oracleCopy = new HeapOutputKeyDomain();
                        oracleCopy.copyFrom(oracle);
                        try (LiveViewCheckpointOutputKeyDomain freshCopy = new LiveViewCheckpointOutputKeyDomain()) {
                            freshCopy.copyFrom(domain);
                            assertSameKeysAndGeometry("fresh copy at step " + step, oracleCopy, freshCopy);
                        }
                        copy.copyFrom(domain);
                        assertSameKeysAndGeometry("reused copy at step " + step, oracleCopy, copy);
                    }
                }
            }
        });
    }

    @Test
    public void testProbingTheDomainAllocatesNothing() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (
                    TestUtils.ThreadMetricsScope<ThreadMXBean> scope = TestUtils.threadAllocationScope();
                    LiveViewCheckpointOutputKeyDomain domain = new LiveViewCheckpointOutputKeyDomain()
            ) {
                final ThreadMXBean threadMXBean = scope.getBean();
                for (int i = 0; i < DOMAIN_SIZE; i++) {
                    add(domain, key(i));
                }
                // Half hits, half misses, so neither the found nor the not-found probe path
                // can escape the measurement. The probes sit in native memory, as a seal's
                // freshly encoded keys do.
                final int probeCount = 256;
                final int probeBytes = key(0).length;
                final long probes = Unsafe.malloc((long) probeCount * probeBytes, MemoryTag.NATIVE_DEFAULT);
                final long threadId = Thread.currentThread().threadId();
                final long allocated;
                int hits = 0;
                try {
                    for (int i = 0; i < probeCount; i++) {
                        final byte[] probe = key(i % 2 == 0 ? i : DOMAIN_SIZE + i);
                        for (int b = 0; b < probeBytes; b++) {
                            Unsafe.putByte(probes + (long) i * probeBytes + b, probe[b]);
                        }
                    }

                    // Warm up so the measured window sees steady-state behaviour rather than
                    // class loading and first-call resolution.
                    int warmUpHits = 0;
                    for (int i = 0; i < 20_000; i++) {
                        if (domain.contains(probes + (long) (i & (probeCount - 1)) * probeBytes, probeBytes)) {
                            warmUpHits++;
                        }
                    }
                    Assert.assertTrue("the warm-up must actually hit the domain", warmUpHits > 0);

                    final long before = threadMXBean.getThreadAllocatedBytes(threadId);
                    for (int i = 0; i < PROBE_COUNT; i++) {
                        if (domain.contains(probes + (long) (i & (probeCount - 1)) * probeBytes, probeBytes)) {
                            hits++;
                        }
                    }
                    allocated = threadMXBean.getThreadAllocatedBytes(threadId) - before;
                } finally {
                    Unsafe.free(probes, (long) probeCount * probeBytes, MemoryTag.NATIVE_DEFAULT);
                }

                Assert.assertEquals("half the probes are hits by construction", PROBE_COUNT / 2, hits);
                Assert.assertTrue(
                        "probing the output key domain allocated " + allocated + " bytes over "
                                + PROBE_COUNT + " probes; the seal probes it once per key per function"
                                + " root, so a per-probe wrapper is charged to every publication",
                        allocated < PROBE_ALLOCATION_LIMIT_BYTES
                );
            }
        });
    }

    @Test
    public void testTheDomainHoldsEveryKeyAcrossItsOwnGrowth() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (LiveViewCheckpointOutputKeyDomain domain = new LiveViewCheckpointOutputKeyDomain()) {
                Assert.assertTrue(domain.isEmpty());
                for (int i = 0; i < DOMAIN_SIZE; i++) {
                    add(domain, key(i));
                }
                Assert.assertEquals(DOMAIN_SIZE, domain.size());
                for (int i = 0; i < DOMAIN_SIZE; i++) {
                    Assert.assertTrue("key " + i + " was lost", LiveViewCheckpointTestKeys.contains(domain, key(i)));
                    Assert.assertFalse("key " + i + " must not answer beyond the domain", LiveViewCheckpointTestKeys.contains(domain, key(DOMAIN_SIZE + i)));
                }

                // Re-adding is idempotent rather than a second entry, whichever way the key
                // arrives, and a duplicate takes no storage. A key encoded in place needs room
                // before the domain can tell it is a duplicate, so the first duplicate round
                // may grow the storage by that room once; ten thousand more duplicates after
                // it must not grow it at all.
                for (int i = 0; i < DOMAIN_SIZE; i++) {
                    add(domain, key(i));
                }
                final long usedBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
                for (int round = 0; round < 20; round++) {
                    for (int i = 0; i < DOMAIN_SIZE; i++) {
                        if ((round & 1) == 0) {
                            add(domain, key(i));
                        } else {
                            addNative(domain, key(i));
                        }
                    }
                }
                Assert.assertEquals(DOMAIN_SIZE, domain.size());
                Assert.assertEquals(usedBefore, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM));

                domain.clear();
                Assert.assertEquals(0, domain.size());
                Assert.assertTrue(domain.isEmpty());
                Assert.assertFalse(LiveViewCheckpointTestKeys.contains(domain, key(0)));

                // ...and the cleared domain must be reusable, not merely empty.
                add(domain, key(0));
                Assert.assertEquals(1, domain.size());
                Assert.assertTrue(LiveViewCheckpointTestKeys.contains(domain, key(0)));
            }
        });
    }

    /**
     * Encodes {@code key} straight into the domain, the way the keyed replay and the ROWS
     * discovery produce Q.
     */
    private static void add(LiveViewCheckpointOutputKeyDomain domain, byte[] key) {
        final MemoryA sink = domain.beginKey();
        for (byte b : key) {
            sink.putByte(b);
        }
        domain.commitKey();
    }

    /**
     * Copies {@code key} in from native memory the domain does not own.
     */
    private static void addNative(LiveViewCheckpointOutputKeyDomain domain, byte[] key) {
        final long size = Math.max(1, key.length);
        final long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < key.length; i++) {
                Unsafe.putByte(address + i, key[i]);
            }
            domain.add(address, key.length);
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void assertSameKeysAndGeometry(
            String step,
            HeapOutputKeyDomain oracle,
            LiveViewCheckpointOutputKeyDomain domain
    ) {
        final ObjList<String> expected = new ObjList<>();
        for (int slot = 0, n = oracle.keys.length; slot < n; slot++) {
            final byte[] key = oracle.keys[slot];
            if (key != null) {
                expected.add(Arrays.toString(key));
            }
        }
        final ObjList<String> actual = new ObjList<>();
        for (int slot = 0, n = domain.getSlotCount(); slot < n; slot++) {
            if (domain.isSlotUsed(slot)) {
                actual.add(Arrays.toString(keyAt(domain, slot)));
            }
        }
        expected.sort(String::compareTo);
        actual.sort(String::compareTo);
        Assert.assertEquals(step + ": size", oracle.size(), domain.size());
        if (domain.size() > 0) {
            Assert.assertEquals(step + ": slot count", oracle.keys.length, domain.getSlotCount());
        }
        Assert.assertEquals(step + ": keys", expected.toString(), actual.toString());
    }

    private static void assertSameSlotOrder(String step, HeapOutputKeyDomain oracle, LiveViewCheckpointOutputKeyDomain domain) {
        final StringBuilder expected = new StringBuilder();
        for (int slot = 0, n = oracle.keys.length; slot < n; slot++) {
            final byte[] key = oracle.keys[slot];
            if (key != null) {
                expected.append(slot).append('=').append(Arrays.toString(key)).append('\n');
            }
        }
        final StringBuilder actual = new StringBuilder();
        for (int slot = 0, n = domain.getSlotCount(); slot < n; slot++) {
            if (domain.isSlotUsed(slot)) {
                actual.append(slot).append('=').append(Arrays.toString(keyAt(domain, slot))).append('\n');
            }
        }
        if (domain.getSlotCount() != 0) {
            Assert.assertEquals(step + ": slot count", oracle.keys.length, domain.getSlotCount());
        }
        Assert.assertEquals(step + ": slot order", expected.toString(), actual.toString());
    }

    /**
     * One encoded partition key, shaped like the ones the codec writes: a few bytes whose
     * leading ones repeat across the domain, so probing exercises real collisions rather
     * than a perfect spread.
     */
    private static byte[] key(int i) {
        return new byte[]{(byte) (i & 7), (byte) (i >>> 3), (byte) (i >>> 11), (byte) i};
    }

    private static byte[] keyAt(LiveViewCheckpointOutputKeyDomain domain, int slot) {
        final long address = domain.getKeyAddress(slot);
        final byte[] key = new byte[domain.getKeyLength(slot)];
        for (int i = 0; i < key.length; i++) {
            key[i] = Unsafe.getByte(address + i);
        }
        return key;
    }

    /**
     * @return the native bytes a fresh domain holds once it has copied {@code source}
     */
    private static long nativeBytesOfCopy(LiveViewCheckpointOutputKeyDomain source) {
        final long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
        try (LiveViewCheckpointOutputKeyDomain copy = new LiveViewCheckpointOutputKeyDomain()) {
            copy.copyFrom(source);
            Assert.assertEquals(source.size(), copy.size());
            return Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM) - before;
        }
    }

    /**
     * A key from a pool with shared prefixes, lengths from 0 to 40 and high-bit bytes, so
     * the pool holds real hash collisions as well as duplicates.
     */
    private static byte[] poolKey(Rnd rnd, int i) {
        final byte[] key = new byte[i % 41];
        for (int b = 0; b < key.length; b++) {
            key[b] = b < 3 ? (byte) (0xF0 + (i & 3)) : (byte) rnd.nextInt(256);
        }
        return key;
    }

    private static long runWideRound(
            LiveViewCheckpointOutputKeyDomain encoded,
            LiveViewCheckpointOutputKeyDomain copied,
            LiveViewCheckpointOutputKeyDomain target,
            long keysAddress
    ) {
        encoded.clear();
        copied.clear();
        for (int i = 0; i < WIDE_DOMAIN_SIZE; i++) {
            final MemoryA sink = encoded.beginKey();
            sink.putLong(wideKeyHead(i));
            sink.putInt(i);
            encoded.commitKey();
            copied.add(keysAddress + (long) i * WIDE_KEY_BYTES, WIDE_KEY_BYTES);
        }
        long checksum = 0;
        for (int i = 0; i < WIDE_DOMAIN_SIZE; i++) {
            if (encoded.contains(keysAddress + (long) i * WIDE_KEY_BYTES, WIDE_KEY_BYTES)) {
                checksum++;
            }
        }
        for (int slot = 0, n = copied.getSlotCount(); slot < n; slot++) {
            if (copied.isSlotUsed(slot)) {
                checksum += Unsafe.getInt(copied.getKeyAddress(slot) + Long.BYTES) + copied.getKeyLength(slot);
            }
        }
        target.copyFrom(encoded);
        return checksum + target.size();
    }

    private static long wideKeyHead(int i) {
        return 0x5A5A_0000_0000_0000L | ((long) (i & 15) << 32) | (i >>> 4);
    }

    /**
     * The heap table the native domain replaced, kept as the order oracle: the same
     * geometry, growth schedule and probe sequence over {@code byte[]} keys.
     */
    private static final class HeapOutputKeyDomain {
        private static final double LOAD_FACTOR = 0.4;
        private static final int MIN_INITIAL_CAPACITY = 16;
        private int capacity;
        private int free;
        private byte[][] keys;
        private int mask;

        private HeapOutputKeyDomain() {
            restoreInitialCapacity();
        }

        private void add(byte[] key) {
            final int index = keyIndex(key);
            if (index < 0) {
                return;
            }
            keys[index] = key;
            if (--free < 1) {
                rehash();
            }
        }

        private void clear() {
            Arrays.fill(keys, null);
            free = capacity;
        }

        // The heap table's copy, verbatim: it re-added the source's keys, in the source's
        // slot order, to its own table.
        private void copyFrom(HeapOutputKeyDomain other) {
            clear();
            for (final byte[] key : other.keys) {
                if (key != null) {
                    add(key);
                }
            }
        }

        private int keyIndex(byte[] key) {
            int index = Hash.spread(Arrays.hashCode(key)) & mask;
            while (true) {
                final byte[] slot = keys[index];
                if (slot == null) {
                    return index;
                }
                if (Arrays.equals(slot, key)) {
                    return -index - 1;
                }
                index = (index + 1) & mask;
            }
        }

        private void rehash() {
            final byte[][] oldKeys = keys;
            capacity *= 2;
            free = capacity;
            final int slotCount = Numbers.ceilPow2((int) (capacity / LOAD_FACTOR));
            keys = new byte[slotCount][];
            mask = slotCount - 1;
            for (final byte[] key : oldKeys) {
                if (key != null) {
                    keys[keyIndex(key)] = key;
                    free--;
                }
            }
        }

        private void restoreInitialCapacity() {
            capacity = MIN_INITIAL_CAPACITY;
            free = capacity;
            final int slotCount = Numbers.ceilPow2((int) (capacity / LOAD_FACTOR));
            keys = new byte[slotCount][];
            mask = slotCount - 1;
        }

        private int size() {
            return capacity - free;
        }
    }
}
