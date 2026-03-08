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
import io.questdb.std.Hash;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractTest;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;
import static org.junit.Assert.assertEquals;

public class HyperLogLogDenseRepresentationTest extends AbstractTest {

    @Test
    public void testEstimateBias() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                HyperLogLogDenseRepresentation hll = new HyperLogLogDenseRepresentation(11);
                hll.setAllocator(allocator);
                hll.of(0);

                assertEquals(1385.3877, hll.estimateBias(1577.3042), 0.001);
                assertEquals(1411.0810, hll.estimateBias(1476), 0.001);
                assertEquals(-7.3095, hll.estimateBias(10230.92), 0.001);
                assertEquals(1411.0810, hll.estimateBias(1501.1), 0.001);
                assertEquals(-7.30953, hll.estimateBias(10137.2), 0.001);
            }
        });
    }

    @Test
    public void testHighCardinality() throws Exception {
        for (int precision = 4; precision <= 18; precision++) {
            int finalPrecision = precision;
            assertMemoryLeak(() -> {
                try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                    HyperLogLogDenseRepresentation hll = new HyperLogLogDenseRepresentation(finalPrecision);
                    hll.setAllocator(allocator);
                    hll.of(0);

                    int exactCardinality = 10000000;

                    for (int i = 0; i < exactCardinality; i++) {
                        hll.add(Hash.murmur3ToLong(i));
                    }

                    long estimatedCardinality = hll.computeCardinality();
                    HyperLogLogTestUtils.assertCardinality(exactCardinality, finalPrecision, estimatedCardinality);
                }
            });
        }
    }

    @Test
    public void testLowCardinality() throws Exception {
        for (int precision = 4; precision <= 18; precision++) {
            int finalPrecision = precision;
            assertMemoryLeak(() -> {
                try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                    HyperLogLogDenseRepresentation hll = new HyperLogLogDenseRepresentation(finalPrecision);
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
                    HyperLogLogTestUtils.assertCardinality(exactCardinality, finalPrecision, estimatedCardinality);
                }
            });
        }
    }
}
