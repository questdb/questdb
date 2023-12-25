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
import io.questdb.std.hyperloglog.HyperLogLog;
import org.junit.Test;

import static io.questdb.test.std.hyperloglog.HyperLogLogTestUtils.assertCardinality;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class HyperLogLogTest {

    @Test
    public void testInvalidPrecision() {
        assertThrows(IllegalArgumentException.class, () -> new HyperLogLog(3));
        assertThrows(IllegalArgumentException.class, () -> new HyperLogLog(19));
    }

    @Test
    public void testClear() {
        Rnd rnd = new Rnd();
        HyperLogLog hll = new HyperLogLog(14);
        assertEquals(0, hll.computeCardinality());

        hll.add(rnd.nextLong());
        assertEquals(1, hll.computeCardinality());

        hll.clear();
        assertEquals(0, hll.computeCardinality());

        hll.add(rnd.nextLong());
        assertEquals(1, hll.computeCardinality());
    }

    @Test
    public void testHighCardinality() {
        for (int precision = 4; precision <= 18; precision++) {
            HyperLogLog hll = new HyperLogLog(precision);
            int exactCardinality = 10000000;

            for (int i = 0; i < exactCardinality; i++) {
                hll.add(Hash.murmur3ToLong(i));
            }

            long estimatedCardinality = hll.computeCardinality();
            assertCardinality(exactCardinality, precision, estimatedCardinality);
        }
    }

    @Test
    public void testLowCardinality() {
        for (int precision = 4; precision <= 18; precision++) {
            HyperLogLog hll = new HyperLogLog(precision);
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
            assertCardinality(exactCardinality, precision, estimatedCardinality);
        }
    }
}
