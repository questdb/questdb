/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.Decimals;
import io.questdb.test.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for Decimal256 storage with variable byte length based on precision
 */
public class DecimalsTest extends AbstractTest {
    @Test(expected = IllegalArgumentException.class)
    public void testGetStorageSizeInvalid() {
        int ignored = Decimals.getStorageSizePow2(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetStorageSizeInvalidTooBig() {
        int ignored = Decimals.getStorageSizePow2(100);
    }

    @Test
    public void testGetStorageSizeCombinatorics() {
        // Combinations of precision -> storage size needed (in bytes)
        int[][] combinations = {
                {1, 0},
                {2, 0},
                {3, 1},
                {4, 1},
                {5, 2},
                {9, 2},
                {10, 3},
                {18, 3},
                {19, 4},
                {38, 4},
                {39, 5},
                {76, 5},
        };

        for (int[] combination : combinations) {
            int precision = combination[0];
            int storageSizeNeeded = combination[1];
            Assert.assertEquals("Expected " + storageSizeNeeded + " pow 2 bytes needed for decimal of precision " + precision, storageSizeNeeded, Decimals.getStorageSizePow2(precision));
        }
    }
}