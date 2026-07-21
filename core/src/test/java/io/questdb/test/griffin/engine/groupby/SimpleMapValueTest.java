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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

public class SimpleMapValueTest {
    private static final int VALUE_SIZE = 32;

    @Test
    public void testAllocationsDoNotShareCacheLines() {
        final int columnCount = 1;
        final int valueCount = 64;
        final long allocationSize = (long) VALUE_SIZE * columnCount + Misc.CACHE_LINE_SIZE;
        final long memUsedBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FAST_MAP);
        final SimpleMapValue[] values = new SimpleMapValue[valueCount];
        try {
            for (int i = 0; i < valueCount; i++) {
                values[i] = new SimpleMapValue(columnCount);
            }
            Assert.assertEquals(
                    (long) valueCount * allocationSize,
                    Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FAST_MAP) - memUsedBefore
            );

            for (int i = 0; i < valueCount; i++) {
                final long lo = cacheLine(values[i].getAddress(0));
                final long hi = cacheLine(values[i].getAddress(0) + VALUE_SIZE - 1L);
                for (int j = i + 1; j < valueCount; j++) {
                    final long otherLo = cacheLine(values[j].getAddress(0));
                    final long otherHi = cacheLine(values[j].getAddress(0) + VALUE_SIZE - 1L);
                    Assert.assertTrue(
                            "SimpleMapValue live regions share a cache line [i=" + i + ", j=" + j + ']',
                            hi < otherLo || otherHi < lo
                    );
                }
            }
        } finally {
            for (int i = 0; i < valueCount; i++) {
                values[i] = Misc.free(values[i]);
            }
        }
        Assert.assertEquals(memUsedBefore, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FAST_MAP));
    }

    private static long cacheLine(long address) {
        return address & -((long) Misc.CACHE_LINE_SIZE);
    }
}
