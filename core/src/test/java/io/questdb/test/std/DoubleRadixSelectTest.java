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

package io.questdb.test.std;

import io.questdb.std.DoubleSort;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class DoubleRadixSelectTest {

    @Test
    public void testSelectsAsASortDoes() {
        assertSelectsAsASortDoes(200_000);
    }

    @Test
    public void testSelectsAsASortDoesWideFirstStep() {
        // above 2^18 values the first step counts by the keys' 16 highest bits
        assertSelectsAsASortDoes(600_000);
    }

    private static void assertSelectsAsASortDoes(int n) {
        final Rnd rnd = new Rnd();
        final long ptr = Unsafe.malloc(8L * n, MemoryTag.NATIVE_DEFAULT);
        try {
            final double[] values = new double[n];
            for (int dist = 0; dist < 6; dist++) {
                for (int i = 0; i < n; i++) {
                    values[i] = switch (dist) {
                        case 0 -> rnd.nextDouble();                       // spread
                        case 1 -> rnd.nextInt(20) * 100.0;                 // few values, many ties
                        case 2 -> rnd.nextDouble() * 2e6 - 1e6;            // both signs
                        case 3 -> rnd.nextBoolean() ? 0.0 : -0.0;          // signed zeros only
                        case 4 -> 140 + rnd.nextInt(5000) / 1000.0;        // a narrow price range
                        default -> i < n / 2 ? 7.0 : rnd.nextLong() * 1e-300; // half one value
                    };
                }
                final double[] sorted = values.clone();
                Arrays.sort(sorted);
                for (long k : new long[]{0, 1, n / 3, n / 2 - 1, n / 2, n - 2, n - 1}) {
                    for (int i = 0; i < n; i++) {
                        Unsafe.putDouble(ptr + 8L * i, values[i]);
                    }
                    if (k < n - 1) {
                        final double[] next = new double[1];
                        Assert.assertEquals("dist " + dist + " k " + k, sorted[(int) k], DoubleSort.radixSelect(ptr, 0, n - 1, k, next), 0.0);
                        Assert.assertEquals("dist " + dist + " next after k " + k, sorted[(int) k + 1], next[0], 0.0);
                    }
                    for (int i = 0; i < n; i++) {
                        Unsafe.putDouble(ptr + 8L * i, values[i]);
                    }
                    Assert.assertEquals("dist " + dist + " k " + k, sorted[(int) k], DoubleSort.radixSelect(ptr, 0, n - 1, k), 0.0);
                }
            }
        } finally {
            Unsafe.free(ptr, 8L * n, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
