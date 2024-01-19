/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.griffin.engine.groupby.hyperloglog;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.hyperloglog.HyperLogLog;
import io.questdb.std.Hash;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractTest;
import org.junit.Test;

import static io.questdb.test.griffin.engine.groupby.hyperloglog.HyperLogLogTestUtils.assertCardinality;
import static io.questdb.test.tools.TestUtils.assertMemoryLeak;
import static org.junit.Assert.*;

public class HyperLogLogTest extends AbstractTest {

    @Test
    public void testInvalidPrecision() {
        assertThrows(IllegalArgumentException.class, () -> new HyperLogLog(3));
        assertThrows(IllegalArgumentException.class, () -> new HyperLogLog(19));
    }

    @Test
    public void testHighCardinality() throws Exception {
        CairoConfiguration config = new DefaultCairoConfiguration(root) {
            @Override
            public long getGroupByAllocatorDefaultChunkSize() {
                return 64;
            }
        };
        for (int precision = 4; precision <= 18; precision++) {
            int finalPrecision = precision;
            assertMemoryLeak(() -> {
                try (GroupByAllocator allocator = new GroupByAllocator(config)) {
                    HyperLogLog hll = new HyperLogLog(finalPrecision);
                    hll.setAllocator(allocator);
                    hll.of(0);

                    int exactCardinality = 10000000;

                    for (int i = 0; i < exactCardinality; i++) {
                        hll.add(Hash.murmur3ToLong(i));
                    }

                    long estimatedCardinality = hll.computeCardinality();
                    assertCardinality(exactCardinality, finalPrecision, estimatedCardinality);
                }
            });
        }
    }

    @Test
    public void testLowCardinality() throws Exception {
        CairoConfiguration config = new DefaultCairoConfiguration(root) {
            @Override
            public long getGroupByAllocatorDefaultChunkSize() {
                return 64;
            }
        };
        for (int precision = 4; precision <= 18; precision++) {
            int finalPrecision = precision;
            assertMemoryLeak(() -> {
                try (GroupByAllocator allocator = new GroupByAllocator(config)) {
                    HyperLogLog hll = new HyperLogLog(finalPrecision);
                    hll.setAllocator(allocator);
                    hll.of(0);

                    int exactCardinality = 10000;
                    int maxRepetitions = 100;
                    Rnd rnd = new Rnd();

                    for (int i = 0; i < exactCardinality; i++) {
                        int n = rnd.nextInt(maxRepetitions) + 1;
                        for (int j = 0; j < n; j++) {
                            hll.add(Hash.murmur3ToLong(i));
                        }
                    }

                    long estimatedCardinality = hll.computeCardinality();
                    assertCardinality(exactCardinality, finalPrecision, estimatedCardinality);
                }
            });
        }
    }

    @Test
    public void testMergeDenseWithDense() throws Exception {
        CairoConfiguration config = new DefaultCairoConfiguration(root) {
            @Override
            public long getGroupByAllocatorDefaultChunkSize() {
                return 64;
            }
        };
        for (int precision = 4; precision <= 18; precision++) {
            int finalPrecision = precision;
            assertMemoryLeak(() -> {
                try (GroupByAllocator allocator = new GroupByAllocator(config)) {
                    Rnd rnd = new Rnd();

                    HyperLogLog expectedHll = new HyperLogLog(finalPrecision);
                    expectedHll.setAllocator(allocator);
                    expectedHll.of(0);

                    HyperLogLog hllA = new HyperLogLog(finalPrecision);
                    hllA.setAllocator(allocator);
                    hllA.of(0);
                    while (hllA.isSparse()) {
                        long hash = rnd.nextLong();
                        expectedHll.add(hash);
                        hllA.add(hash);
                    }

                    HyperLogLog hllB = new HyperLogLog(finalPrecision);
                    hllB.setAllocator(allocator);
                    hllB.of(0);
                    while (hllB.isSparse()) {
                        long hash = rnd.nextLong();
                        expectedHll.add(hash);
                        hllB.add(hash);
                    }

                    long mergedPtr = HyperLogLog.merge(hllA, hllB);
                    HyperLogLog mergedHll = new HyperLogLog(finalPrecision);
                    mergedHll.setAllocator(allocator);
                    mergedHll.of(mergedPtr);

                    assertEquals(expectedHll.computeCardinality(), mergedHll.computeCardinality());
                }
            });
        }
    }

    @Test
    public void testMergeSparseWithSparse() throws Exception {
        CairoConfiguration config = new DefaultCairoConfiguration(root) {
            @Override
            public long getGroupByAllocatorDefaultChunkSize() {
                return 64;
            }
        };
        for (int precision = 11; precision <= 18; precision++) {
            int finalPrecision = precision;
            assertMemoryLeak(() -> {
                try (GroupByAllocator allocator = new GroupByAllocator(config)) {
                    Rnd rnd = new Rnd();

                    HyperLogLog expectedHll = new HyperLogLog(finalPrecision);
                    expectedHll.setAllocator(allocator);
                    expectedHll.of(0);

                    HyperLogLog larger = new HyperLogLog(finalPrecision);
                    larger.setAllocator(allocator);
                    larger.of(0);
                    for (int i = 0; i < 150; i++) {
                        long hash = rnd.nextLong();
                        expectedHll.add(hash);
                        larger.add(hash);
                    }
                    assertTrue(larger.isSparse());

                    HyperLogLog smaller = new HyperLogLog(finalPrecision);
                    smaller.setAllocator(allocator);
                    smaller.of(0);
                    for (int i = 0; i < 100; i++) {
                        long hash = rnd.nextLong();
                        expectedHll.add(hash);
                        smaller.add(hash);
                    }
                    assertTrue(smaller.isSparse());

                    // Test if merging larger with smaller works correctly.
                    long mergedPtr = HyperLogLog.merge(larger, smaller);
                    HyperLogLog mergedHll = new HyperLogLog(finalPrecision);
                    mergedHll.setAllocator(allocator);
                    mergedHll.of(mergedPtr);
                    assertEquals(expectedHll.computeCardinality(), mergedHll.computeCardinality());

                    // Test if merging smaller with larger works correctly.
                    mergedPtr = HyperLogLog.merge(smaller, larger);
                    mergedHll.of(mergedPtr);
                    assertEquals(expectedHll.computeCardinality(), mergedHll.computeCardinality());
                }
            });
        }
    }

    @Test
    public void testMergeSparseWithDense() throws Exception {
        CairoConfiguration config = new DefaultCairoConfiguration(root) {
            @Override
            public long getGroupByAllocatorDefaultChunkSize() {
                return 64;
            }
        };
        for (int precision = 11; precision <= 18; precision++) {
            int finalPrecision = precision;
            assertMemoryLeak(() -> {
                try (GroupByAllocator allocator = new GroupByAllocator(config)) {
                    Rnd rnd = new Rnd();

                    HyperLogLog expectedHll = new HyperLogLog(finalPrecision);
                    expectedHll.setAllocator(allocator);
                    expectedHll.of(0);

                    HyperLogLog sparse = new HyperLogLog(finalPrecision);
                    sparse.setAllocator(allocator);
                    sparse.of(0);
                    for (int i = 0; i < 100; i++) {
                        long hash = rnd.nextLong();
                        expectedHll.add(hash);
                        sparse.add(hash);
                    }
                    assertTrue(sparse.isSparse());

                    HyperLogLog dense = new HyperLogLog(finalPrecision);
                    dense.setAllocator(allocator);
                    dense.of(0);
                    while (dense.isSparse()) {
                        long hash = rnd.nextLong();
                        expectedHll.add(hash);
                        dense.add(hash);
                    }

                    // Test if merging sparse with dense works correctly.
                    long mergedPtr = HyperLogLog.merge(sparse, dense);
                    HyperLogLog mergedHll = new HyperLogLog(finalPrecision);
                    mergedHll.setAllocator(allocator);
                    mergedHll.of(mergedPtr);
                    assertEquals(expectedHll.computeCardinality(), mergedHll.computeCardinality());

                    // Test if merging dense with sparse works correctly.
                    mergedPtr = HyperLogLog.merge(dense, sparse);
                    mergedHll.of(mergedPtr);
                    assertEquals(expectedHll.computeCardinality(), mergedHll.computeCardinality());
                }
            });
        }
    }

    @Test
    public void testAddInvalidatesCachedCardinality() throws Exception {
        CairoConfiguration config = new DefaultCairoConfiguration(root) {
            @Override
            public long getGroupByAllocatorDefaultChunkSize() {
                return 64;
            }
        };
        for (int precision = 4; precision <= 18; precision++) {
            int finalPrecision = precision;
            assertMemoryLeak(() -> {
                try (GroupByAllocator allocator = new GroupByAllocator(config)) {
                    Rnd rnd = new Rnd();

                    HyperLogLog hll = new HyperLogLog(finalPrecision);
                    hll.setAllocator(allocator);
                    hll.of(0);

                    hll.add(Hash.murmur3ToLong(rnd.nextLong()));
                    assertEquals(1, hll.computeCardinality());
                    assertEquals(1, hll.computeCardinality());

                    // add should invalidate cache
                    hll.add(Hash.murmur3ToLong(rnd.nextLong()));
                    assertEquals(2, hll.computeCardinality());
                }
            });
        }
    }
}
