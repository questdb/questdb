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

package io.questdb.test;

import org.junit.Test;

import static io.questdb.MemoryConfigurationImpl.resolveRamUsageLimit;
import static org.junit.Assert.assertEquals;

public class MemoryConfigurationTest {

    @Test
    public void testLimit0Bytes0Percent() {
        long limitBytes = 0;
        long limitPercent = 0;
        long ramSize = 1000;
        assertEquals(0, resolveRamUsageLimit(limitBytes, limitPercent, ramSize));
    }

    @Test
    public void testLimit0Bytes90Percent() {
        long limitBytes = 0;
        long limitPercent = 90;
        long ramSize = 1000;
        assertEquals(900, resolveRamUsageLimit(limitBytes, limitPercent, ramSize));
    }

    @Test
    public void testLimit5Bytes0Percent() {
        long limitBytes = 5;
        long limitPercent = 0;
        long ramSize = 1000;
        assertEquals(5, resolveRamUsageLimit(limitBytes, limitPercent, ramSize));
    }

    @Test
    public void testLimit5Bytes90Percent() {
        long limitBytes = 5;
        long limitPercent = 90;
        long ramSize = 1000;
        assertEquals(5, resolveRamUsageLimit(limitBytes, limitPercent, ramSize));
    }

    @Test
    public void testLimit900Bytes5Percent() {
        long limitBytes = 900;
        long limitPercent = 5;
        long ramSize = 1000;
        assertEquals(50, resolveRamUsageLimit(limitBytes, limitPercent, ramSize));
    }

    @Test
    public void testLimit900Bytes5PercentRamSizeUnknown() {
        long limitBytes = 900;
        long limitPercent = 5;
        long ramSize = -1;
        assertEquals(900, resolveRamUsageLimit(limitBytes, limitPercent, ramSize));
    }
}
