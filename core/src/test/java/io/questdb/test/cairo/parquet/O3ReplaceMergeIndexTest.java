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

package io.questdb.test.cairo.parquet;

import io.questdb.cairo.O3PartitionJob;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class O3ReplaceMergeIndexTest extends AbstractCairoTest {
    private static final long DATA_BIT = 1L << 63;

    @Test
    public void testEmptyO3DropsRangeFromMiddle() throws Exception {
        assertIndex(
                new long[]{10, 20, 30, 40, 50},
                new long[]{},
                20, 40,
                "10:d0,50:d4"
        );
    }

    @Test
    public void testO3OnlyWhenRangeCoversAllRows() throws Exception {
        assertIndex(
                new long[]{20, 30},
                new long[]{25},
                10, 40,
                "25:o0"
        );
    }

    @Test
    public void testPrefixO3Suffix() throws Exception {
        assertIndex(
                new long[]{10, 20, 30, 40, 50},
                new long[]{25, 35},
                20, 40,
                "10:d0,25:o0,35:o1,50:d4"
        );
    }

    @Test
    public void testRangeAtHeadKeepsSuffix() throws Exception {
        assertIndex(
                new long[]{10, 10, 20, 30},
                new long[]{5},
                5, 10,
                "5:o0,20:d2,30:d3"
        );
    }

    @Test
    public void testRangeAtTailKeepsDuplicatePrefix() throws Exception {
        assertIndex(
                new long[]{10, 20, 20, 30},
                new long[]{35},
                21, 40,
                "10:d0,20:d1,20:d2,35:o0"
        );
    }

    private static void assertIndex(long[] data, long[] o3, long replaceLo, long replaceHi, String expected) throws Exception {
        assertMemoryLeak(() -> {
            final long dataSize = Math.max(1, data.length) * 8L;
            final long o3Size = Math.max(1, o3.length) * 16L;
            final long destSize = (data.length + o3.length) * 16L + 16;
            final long dataAddr = Unsafe.malloc(dataSize, MemoryTag.NATIVE_O3);
            final long o3Addr = Unsafe.malloc(o3Size, MemoryTag.NATIVE_O3);
            final long destAddr = Unsafe.malloc(destSize, MemoryTag.NATIVE_O3);
            try {
                for (int i = 0; i < data.length; i++) {
                    Unsafe.putLong(dataAddr + i * 8L, data[i]);
                }
                for (int i = 0; i < o3.length; i++) {
                    Unsafe.putLong(o3Addr + i * 16L, o3[i]);
                    Unsafe.putLong(o3Addr + i * 16L + 8, i);
                }
                final long n = O3PartitionJob.createReplaceMergeIndex(
                        dataAddr, data.length, o3Addr, 0, o3.length - 1, replaceLo, replaceHi, destAddr
                );
                StringBuilder sb = new StringBuilder();
                for (long i = 0; i < n; i++) {
                    if (i > 0) {
                        sb.append(',');
                    }
                    long ts = Unsafe.getLong(destAddr + i * 16);
                    long idx = Unsafe.getLong(destAddr + i * 16 + 8);
                    sb.append(ts).append(':');
                    if ((idx & DATA_BIT) != 0) {
                        sb.append('d').append(idx & ~DATA_BIT);
                    } else {
                        sb.append('o').append(idx);
                    }
                }
                Assert.assertEquals(expected, sb.toString());
            } finally {
                Unsafe.free(dataAddr, dataSize, MemoryTag.NATIVE_O3);
                Unsafe.free(o3Addr, o3Size, MemoryTag.NATIVE_O3);
                Unsafe.free(destAddr, destSize, MemoryTag.NATIVE_O3);
            }
        });
    }
}
