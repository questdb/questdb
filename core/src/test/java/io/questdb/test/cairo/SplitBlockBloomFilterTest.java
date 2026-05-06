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

package io.questdb.test.cairo;

import io.questdb.cairo.idx.SplitBlockBloomFilter;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;

public class SplitBlockBloomFilterTest {

    @Test
    public void testAllocateClearsMemory() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int size = 1024;
            long addr = SplitBlockBloomFilter.allocate(size);
            try {
                int positives = 0;
                for (int i = 0; i < 10_000; i++) {
                    long hash = SplitBlockBloomFilter.hashKey(i);
                    if (SplitBlockBloomFilter.mightContain(addr, size, hash)) {
                        positives++;
                    }
                }
                Assert.assertEquals("empty filter must report false for all probes", 0, positives);
            } finally {
                SplitBlockBloomFilter.free(addr, size);
            }
        });
    }

    @Test
    public void testComputeSizeBoundaries() {
        Assert.assertEquals(SplitBlockBloomFilter.BLOCK_SIZE, SplitBlockBloomFilter.computeSize(0, 0.01));
        Assert.assertEquals(SplitBlockBloomFilter.BLOCK_SIZE, SplitBlockBloomFilter.computeSize(-100, 0.01));

        // Size grows with ndv
        int s100 = SplitBlockBloomFilter.computeSize(100, 0.01);
        int s10000 = SplitBlockBloomFilter.computeSize(10_000, 0.01);
        Assert.assertTrue(s10000 > s100);

        // Size shrinks as fpp relaxes
        int sTight = SplitBlockBloomFilter.computeSize(10_000, 0.001);
        int sLoose = SplitBlockBloomFilter.computeSize(10_000, 0.1);
        Assert.assertTrue(sTight > sLoose);

        // Size is always a multiple of BLOCK_SIZE
        Assert.assertEquals(0, s100 % SplitBlockBloomFilter.BLOCK_SIZE);
        Assert.assertEquals(0, s10000 % SplitBlockBloomFilter.BLOCK_SIZE);

        // Capped at 128 MB
        int sHuge = SplitBlockBloomFilter.computeSize(Integer.MAX_VALUE, 0.0001);
        Assert.assertTrue(sHuge <= 128 * 1024 * 1024);
    }

    @Test
    public void testFprWithinBound() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int ndv = 10_000;
            double targetFpp = 0.01;
            int size = SplitBlockBloomFilter.computeSize(ndv, targetFpp);
            long addr = SplitBlockBloomFilter.allocate(size);
            try {
                for (int i = 0; i < ndv; i++) {
                    SplitBlockBloomFilter.insert(addr, size, SplitBlockBloomFilter.hashKey(i));
                }
                int falsePositives = 0;
                int probes = 100_000;
                for (int i = ndv; i < ndv + probes; i++) {
                    if (SplitBlockBloomFilter.mightContain(addr, size, SplitBlockBloomFilter.hashKey(i))) {
                        falsePositives++;
                    }
                }
                double measuredFpr = (double) falsePositives / probes;
                Assert.assertTrue(
                        "measured FPR=" + measuredFpr + " exceeds 3x target=" + targetFpp,
                        measuredFpr < targetFpp * 3.0
                );
            } finally {
                SplitBlockBloomFilter.free(addr, size);
            }
        });
    }

    @Test
    public void testHashKeyDistinctOutputs() {
        // xxHash64 must distinguish small neighbouring inputs with very high probability.
        // Collecting 10k hashes and requiring they are all distinct is a weak check that
        // nevertheless catches pathological drifts (constant output, fixed-point, etc.).
        HashSet<Long> seen = new HashSet<>(16_384);
        for (int i = 0; i < 10_000; i++) {
            seen.add(SplitBlockBloomFilter.hashKey(i));
        }
        Assert.assertEquals(10_000, seen.size());
    }

    @Test
    public void testInsertNoFalseNegatives() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int ndv = 5_000;
            int size = SplitBlockBloomFilter.computeSize(ndv, 0.01);
            long addr = SplitBlockBloomFilter.allocate(size);
            try {
                for (int i = 0; i < ndv; i++) {
                    SplitBlockBloomFilter.insert(addr, size, SplitBlockBloomFilter.hashKey(i));
                }
                // Every inserted key must be reported as present (no false negatives).
                for (int i = 0; i < ndv; i++) {
                    Assert.assertTrue(
                            "false negative for key=" + i,
                            SplitBlockBloomFilter.mightContain(addr, size, SplitBlockBloomFilter.hashKey(i))
                    );
                }
            } finally {
                SplitBlockBloomFilter.free(addr, size);
            }
        });
    }
}
