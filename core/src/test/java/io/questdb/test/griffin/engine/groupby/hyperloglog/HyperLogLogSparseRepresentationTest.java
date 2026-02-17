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

package io.questdb.test.griffin.engine.groupby.hyperloglog;

import io.questdb.griffin.engine.groupby.FastGroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.hyperloglog.HyperLogLogDenseRepresentation;
import io.questdb.griffin.engine.groupby.hyperloglog.HyperLogLogSparseRepresentation;
import io.questdb.std.Hash;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractTest;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HyperLogLogSparseRepresentationTest extends AbstractTest {

    @Test
    public void testComputeCardinality() throws Exception {
        for (int precision = 11; precision <= 18; precision++) {
            int finalPrecision = precision;
            assertMemoryLeak(() -> {
                try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                    HyperLogLogSparseRepresentation hll = new HyperLogLogSparseRepresentation(finalPrecision);
                    hll.setAllocator(allocator);
                    hll.of(0);

                    int exactCardinality = 0;
                    while (!hll.isFull()) {
                        hll.add(Hash.murmur3ToLong(exactCardinality++));
                    }

                    assertTrue(exactCardinality > 0);
                    long estimatedCardinality = hll.computeCardinality();
                    HyperLogLogTestUtils.assertCardinality(exactCardinality, finalPrecision, estimatedCardinality);
                }
            });
        }
    }

    @Test
    public void testComputeCardinalityWithRepetitions() throws Exception {
        for (int precision = 11; precision <= 18; precision++) {
            int finalPrecision = precision;
            assertMemoryLeak(() -> {
                try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                    HyperLogLogSparseRepresentation hll = new HyperLogLogSparseRepresentation(finalPrecision);
                    hll.setAllocator(allocator);
                    hll.of(0);

                    int maxRepetitions = 100;
                    Rnd rnd = new Rnd();

                    int exactCardinality = 0;
                    while (!hll.isFull()) {
                        int n = rnd.nextInt(maxRepetitions) + 1;
                        for (int j = 0; j < n; j++) {
                            hll.add(Hash.murmur3ToLong(exactCardinality));
                        }
                        exactCardinality++;
                    }

                    assertTrue(exactCardinality > 0);
                    long estimatedCardinality = hll.computeCardinality();
                    HyperLogLogTestUtils.assertCardinality(exactCardinality, finalPrecision, estimatedCardinality);
                }
            });
        }
    }

    @Test
    public void testConvertToDense() throws Exception {
        for (int precision = 11; precision <= 18; precision++) {
            int finalPrecision = precision;
            assertMemoryLeak(() -> {
                try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                    HyperLogLogSparseRepresentation sparse = new HyperLogLogSparseRepresentation(finalPrecision);
                    sparse.setAllocator(allocator);
                    sparse.of(0);

                    HyperLogLogDenseRepresentation dense = new HyperLogLogDenseRepresentation(finalPrecision);
                    dense.setAllocator(allocator);
                    dense.of(0);

                    int exactCardinality = 0;
                    while (!sparse.isFull()) {
                        long hash = Hash.murmur3ToLong(exactCardinality++);
                        sparse.add(hash);
                        dense.add(hash);
                    }

                    assertTrue(exactCardinality > 0);
                    HyperLogLogDenseRepresentation converted = new HyperLogLogDenseRepresentation(finalPrecision);
                    sparse.convertToDense(converted);
                    assertEquals(dense.computeCardinality(), converted.computeCardinality());
                }
            });
        }
    }
}
