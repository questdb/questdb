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

package io.questdb.test.std;

import io.questdb.cairo.CairoException;
import io.questdb.std.MemoryTag;
import io.questdb.std.ResidentMemoryReader;
import io.questdb.std.Unsafe;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * The point of the residency change, pinned.
 * <p>
 * QuestDB's global RSS guard used to compare {@code ram.usage.limit.bytes}
 * against {@link Unsafe#getRssMemUsed()} — QuestDB's own <em>accounted</em>
 * native total. That is not process residency. Measured at the moment a
 * 384 MB-capped server was kernel OOM-killed: accounted was 126 MiB against a
 * 230 MiB threshold, while the kernel reported 374 MiB of anon-rss — 97% of the
 * cap. The guard could not fire, because the gap was JVM heap and JVM-internal
 * native, which QuestDB never allocates and can never account for.
 * <p>
 * The guard now works from sampled real residency, extrapolated with accounted
 * growth between samples.
 */
public class ResidentMemoryGuardTest {

    @After
    public void tearDown() {
        // Both are process-global; leaking either would corrupt unrelated tests.
        Unsafe.setRssMemLimit(0);
        Unsafe.clearResidentSampleForTests();
    }

    @Test
    public void testExtrapolationFloorsANegativeDelta() {
        // Frees outpacing allocations must never drag the estimate below the
        // last known real residency — that would re-open the same blind spot.
        Assert.assertEquals(
                800L,
                Unsafe.computeEffectiveResidentMemUsed(800L, 500L, 300L)
        );
    }

    @Test
    public void testExtrapolationAddsAccountedGrowthToTheSample() {
        // 800 sampled, accounted has grown 300 -> 450 since, so +150.
        Assert.assertEquals(
                950L,
                Unsafe.computeEffectiveResidentMemUsed(800L, 300L, 450L)
        );
    }

    @Test
    public void testFallsBackToAccountedBeforeAnySampleLands() {
        // Until the periodic sampler has run once there is nothing better to
        // use, and the guard must behave exactly as it did before this change.
        Assert.assertEquals(
                12345L,
                Unsafe.computeEffectiveResidentMemUsed(ResidentMemoryReader.UNKNOWN_RESIDENT_BYTES, 999L, 12345L)
        );
    }

    @Test
    public void testGuardTripsOnRealResidencyThatAccountingCannotSee() {
        final long accounted = Unsafe.getRssMemUsed();

        // The scenario that killed a real server: residency far above the
        // limit, while QuestDB's own accounting sits comfortably below it.
        final long residency = accounted + 512 * 1024 * 1024L;
        final long limit = accounted + 256 * 1024 * 1024L;

        Unsafe.updateResidentSample(residency);
        Unsafe.setRssMemLimit(limit);

        Assert.assertTrue(
                "precondition: accounting alone must sit UNDER the limit, otherwise this test would" +
                        " have passed before the change and proves nothing",
                accounted < limit
        );
        Assert.assertTrue(
                "precondition: real residency must sit OVER the limit",
                Unsafe.getEffectiveResidentMemUsed() > limit
        );

        try {
            long ptr = Unsafe.malloc(1024, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(ptr, 1024, MemoryTag.NATIVE_DEFAULT);
            Assert.fail("allocation should have been refused: residency is over the limit");
        } catch (CairoException e) {
            Assert.assertTrue(
                    "expected the RSS limit message, got: " + e.getFlyweightMessage(),
                    e.getFlyweightMessage().toString().contains("global RSS memory limit exceeded")
            );
        }
    }

    @Test
    public void testNoLimitConfiguredMeansNoGuard() {
        // Zero-regression: the shipped default is no limit, and then even an
        // absurd residency sample must not refuse anything.
        Unsafe.setRssMemLimit(0);
        Unsafe.updateResidentSample(Long.MAX_VALUE / 2);

        long ptr = Unsafe.malloc(1024, MemoryTag.NATIVE_DEFAULT);
        Unsafe.free(ptr, 1024, MemoryTag.NATIVE_DEFAULT);
    }
}
