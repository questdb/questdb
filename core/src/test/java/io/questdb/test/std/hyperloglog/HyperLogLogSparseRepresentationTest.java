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

package io.questdb.test.std.hyperloglog;

import io.questdb.std.Hash;
import io.questdb.std.Rnd;
import io.questdb.std.hyperloglog.HyperLogLogDenseRepresentation;
import io.questdb.std.hyperloglog.HyperLogLogRepresentation;
import io.questdb.std.hyperloglog.HyperLogLogSparseRepresentation;
import org.junit.Test;

import static io.questdb.test.std.hyperloglog.HyperLogLogTestUtils.assertCardinality;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HyperLogLogSparseRepresentationTest {

    @Test
    public void testConvertToDense() {
        for (int precision = 11; precision <= 18; precision++) {
            HyperLogLogSparseRepresentation sparse = new HyperLogLogSparseRepresentation(precision);
            HyperLogLogDenseRepresentation dense = new HyperLogLogDenseRepresentation(precision);

            int exactCardinality = 0;
            while (!sparse.isFull()) {
                long hash = Hash.murmur3ToLong(exactCardinality++);
                sparse.add(hash);
                dense.add(hash);
            }

            assertTrue(exactCardinality > 0);
            HyperLogLogRepresentation converted = sparse.convertToDense();
            assertEquals(dense.computeCardinality(), converted.computeCardinality());
        }
    }

    @Test
    public void testComputeCardinality() {
        for (int precision = 11; precision <= 18; precision++) {
            HyperLogLogSparseRepresentation hll = new HyperLogLogSparseRepresentation(precision);

            int exactCardinality = 0;
            while (!hll.isFull()) {
                hll.add(Hash.murmur3ToLong(exactCardinality++));
            }

            assertTrue(exactCardinality > 0);
            long estimatedCardinality = hll.computeCardinality();
            assertCardinality(exactCardinality, precision, estimatedCardinality);
        }
    }

    @Test
    public void testComputeCardinalityWithRepetitions() {
        for (int precision = 11; precision <= 18; precision++) {
            HyperLogLogSparseRepresentation hll = new HyperLogLogSparseRepresentation(precision);
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
            assertCardinality(exactCardinality, precision, estimatedCardinality);
        }
    }

    @Test
    public void testClear() {
        Rnd rnd = new Rnd();
        HyperLogLogSparseRepresentation hll = new HyperLogLogSparseRepresentation(14);
        assertEquals(0, hll.computeCardinality());

        hll.add(rnd.nextLong());
        assertEquals(1, hll.computeCardinality());

        hll.clear();
        assertEquals(0, hll.computeCardinality());

        hll.add(rnd.nextLong());
        assertEquals(1, hll.computeCardinality());
    }
}
