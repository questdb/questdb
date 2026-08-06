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
import io.questdb.cairo.lv.LiveViewCheckpointOutputKeyDomain;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.lang.management.ManagementFactory;

/**
 * Coverage for {@code Q}, the output key domain a localized repair publishes against.
 * <p>
 * Two properties carry it, and they pull in opposite directions. The domain has to match
 * an encoded partition key by its CONTENT - the key a seal probes with is a fresh array
 * built off a map record, never the array the repair plan put in - and it has to do that
 * without allocating, because every seal loop probes it once per key per function root,
 * so the probe count is the key domain times the roots the boundary writes.
 */
public class LiveViewCheckpointOutputKeyDomainTest {

    private static final int DOMAIN_SIZE = 512;
    // Well above any per-probe allocation, well below what one wrapper per probe costs:
    // a 16-byte-header wrapper over 200k probes is several megabytes.
    private static final long PROBE_ALLOCATION_LIMIT_BYTES = 64 * 1024;
    private static final int PROBE_COUNT = 200_000;

    @Test
    public void testAKeyIsMatchedByContentRatherThanByIdentity() {
        final LiveViewCheckpointOutputKeyDomain domain = new LiveViewCheckpointOutputKeyDomain();
        domain.add(new byte[]{1, 2, 3});

        // The probing side never holds the array the domain was given: a seal encodes the
        // key afresh off its own map record. Matching on identity would answer false here
        // and silently drop every key from the published root.
        Assert.assertTrue(domain.contains(new byte[]{1, 2, 3}));
        Assert.assertFalse(domain.contains(new byte[]{1, 2, 4}));
        // A prefix is a different key, and so is the same content one byte longer.
        Assert.assertFalse(domain.contains(new byte[]{1, 2}));
        Assert.assertFalse(domain.contains(new byte[]{1, 2, 3, 0}));
    }

    @Test
    public void testAnEmptyKeyIsAKeyLikeAnyOther() {
        final LiveViewCheckpointOutputKeyDomain domain = new LiveViewCheckpointOutputKeyDomain();
        Assert.assertFalse(domain.contains(new byte[0]));
        domain.add(new byte[0]);
        Assert.assertTrue(domain.contains(new byte[0]));
        Assert.assertEquals(1, domain.size());
        Assert.assertFalse(domain.isEmpty());
        // ...and it must not answer for every other key as a null-ish slot marker would.
        Assert.assertFalse(domain.contains(new byte[]{0}));
    }

    @Test
    public void testCopyFromReplacesTheDomainWholesale() {
        final LiveViewCheckpointOutputKeyDomain source = new LiveViewCheckpointOutputKeyDomain();
        source.add(new byte[]{7});
        source.add(new byte[]{8});

        final LiveViewCheckpointOutputKeyDomain target = new LiveViewCheckpointOutputKeyDomain();
        target.add(new byte[]{9});
        target.copyFrom(source);

        Assert.assertEquals(2, target.size());
        Assert.assertTrue(target.contains(new byte[]{7}));
        Assert.assertTrue(target.contains(new byte[]{8}));
        Assert.assertFalse("copyFrom replaces rather than merges", target.contains(new byte[]{9}));

        // The capture owns its copy: the plan is refilled by the next repair while a
        // parked capture still owes its publication.
        source.clear();
        Assert.assertTrue(target.contains(new byte[]{7}));
        Assert.assertEquals(0, source.size());
        Assert.assertTrue(source.isEmpty());
    }

    @Test
    public void testProbingTheDomainAllocatesNothing() {
        final ThreadMXBean threadMXBean = enableThreadAllocationProfiling();
        final LiveViewCheckpointOutputKeyDomain domain = new LiveViewCheckpointOutputKeyDomain();
        for (int i = 0; i < DOMAIN_SIZE; i++) {
            domain.add(key(i));
        }
        // Half hits, half misses, so neither the found nor the not-found probe path can
        // escape the measurement.
        final byte[][] probes = new byte[256][];
        for (int i = 0; i < probes.length; i++) {
            probes[i] = key(i % 2 == 0 ? i : DOMAIN_SIZE + i);
        }

        // Warm up so the measured window sees steady-state behaviour rather than class
        // loading and first-call resolution.
        int warmUpHits = 0;
        for (int i = 0; i < 20_000; i++) {
            if (domain.contains(probes[i & (probes.length - 1)])) {
                warmUpHits++;
            }
        }
        Assert.assertTrue("the warm-up must actually hit the domain", warmUpHits > 0);

        final long threadId = Thread.currentThread().threadId();
        final long before = threadMXBean.getThreadAllocatedBytes(threadId);
        int hits = 0;
        for (int i = 0; i < PROBE_COUNT; i++) {
            if (domain.contains(probes[i & (probes.length - 1)])) {
                hits++;
            }
        }
        final long allocated = threadMXBean.getThreadAllocatedBytes(threadId) - before;

        Assert.assertEquals("half the probes are hits by construction", PROBE_COUNT / 2, hits);
        Assert.assertTrue(
                "probing the output key domain allocated " + allocated + " bytes over "
                        + PROBE_COUNT + " probes; the seal probes it once per key per function"
                        + " root, so a per-probe wrapper is charged to every publication",
                allocated < PROBE_ALLOCATION_LIMIT_BYTES
        );
    }

    @Test
    public void testTheDomainHoldsEveryKeyAcrossItsOwnGrowth() {
        final LiveViewCheckpointOutputKeyDomain domain = new LiveViewCheckpointOutputKeyDomain();
        Assert.assertTrue(domain.isEmpty());
        for (int i = 0; i < DOMAIN_SIZE; i++) {
            domain.add(key(i));
        }
        Assert.assertEquals(DOMAIN_SIZE, domain.size());
        for (int i = 0; i < DOMAIN_SIZE; i++) {
            Assert.assertTrue("key " + i + " was lost", domain.contains(key(i)));
            Assert.assertFalse("key " + i + " must not answer beyond the domain", domain.contains(key(DOMAIN_SIZE + i)));
        }

        // Re-adding is idempotent rather than a second entry.
        for (int i = 0; i < DOMAIN_SIZE; i++) {
            domain.add(key(i));
        }
        Assert.assertEquals(DOMAIN_SIZE, domain.size());

        domain.clear();
        Assert.assertEquals(0, domain.size());
        Assert.assertTrue(domain.isEmpty());
        Assert.assertFalse(domain.contains(key(0)));

        // ...and the cleared domain must be reusable, not merely empty.
        domain.add(key(0));
        Assert.assertEquals(1, domain.size());
        Assert.assertTrue(domain.contains(key(0)));
    }

    /**
     * Turns on per-thread allocation accounting, or skips the calling test when the JVM
     * does not offer it.
     */
    private static ThreadMXBean enableThreadAllocationProfiling() {
        final java.lang.management.ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();
        Assume.assumeTrue("thread allocation profiling unavailable", mxBean instanceof ThreadMXBean);
        final ThreadMXBean threadMXBean = (ThreadMXBean) mxBean;
        Assume.assumeTrue(threadMXBean.isThreadAllocatedMemorySupported());
        if (!threadMXBean.isThreadAllocatedMemoryEnabled()) {
            threadMXBean.setThreadAllocatedMemoryEnabled(true);
        }
        return threadMXBean;
    }

    /**
     * One encoded partition key, shaped like the ones the codec writes: a few bytes whose
     * leading ones repeat across the domain, so probing exercises real collisions rather
     * than a perfect spread.
     */
    private static byte[] key(int i) {
        return new byte[]{(byte) (i & 7), (byte) (i >>> 3), (byte) (i >>> 11), (byte) i};
    }
}
