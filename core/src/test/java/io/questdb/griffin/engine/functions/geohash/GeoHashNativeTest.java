/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.functions.geohash;

import io.questdb.griffin.engine.table.LatestByArguments;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import org.junit.Assert;
import org.junit.Test;

public class GeoHashNativeTest {
    @Test
    public void testIota() {
        final long N = 511;
        final long K = 42;
        try (DirectLongList list = new DirectLongList(N, MemoryTag.NATIVE_LONG_LIST)) {
            list.setPos(list.getCapacity());
            for (int i = 1; i < N; i++) {
                GeoHashNative.iota(list.getAddress(), i, K);
                for (int j = 0; j < i; j++) {
                    Assert.assertEquals(j + K, list.get(j));
                }
            }
        }
    }

    @Test
    public void testSlideFoundBlocks() {
        int keyCount = 20;

        DirectLongList rows = new DirectLongList(keyCount, MemoryTag.NATIVE_LONG_LIST);
        rows.setCapacity(keyCount);

        GeoHashNative.iota(rows.getAddress(), rows.getCapacity(), 0);

        final int workerCount = 5;

        final long chunkSize = (keyCount + workerCount - 1) / workerCount;
        final int taskCount = (int) ((keyCount + chunkSize - 1) / chunkSize);

        final long argumentsAddress = LatestByArguments.allocateMemoryArray(taskCount);
        try {
            for (long i = 0; i < taskCount; ++i) {
                final long klo = i * chunkSize;
                final long khi = Long.min(klo + chunkSize, keyCount);
                final long argsAddress = argumentsAddress + i * LatestByArguments.MEMORY_SIZE;
                LatestByArguments.setRowsAddress(argsAddress, rows.getAddress());
                LatestByArguments.setRowsCapacity(argsAddress, rows.getCapacity());
                LatestByArguments.setKeyLo(argsAddress, klo);
                LatestByArguments.setKeyHi(argsAddress, khi);
                LatestByArguments.setRowsSize(argsAddress, 0);

                // 0, 2, 4, 0, 2 ...
                // zero, half, full
                long sz = (i % 3) * 2;
                LatestByArguments.setFilteredSize(argsAddress, sz);
            }
            final long rowCount = GeoHashNative.slideFoundBlocks(argumentsAddress, taskCount);
            Assert.assertEquals(8, rowCount);

            Assert.assertEquals(4, rows.get(0));
            Assert.assertEquals(5, rows.get(1));
            Assert.assertEquals(8, rows.get(2));
            Assert.assertEquals(9, rows.get(3));
            Assert.assertEquals(10, rows.get(4));
            Assert.assertEquals(11, rows.get(5));
            Assert.assertEquals(16, rows.get(6));
            Assert.assertEquals(17, rows.get(7));

        } finally {
            rows.close();
            LatestByArguments.releaseMemoryArray(argumentsAddress, taskCount);
        }

    }
}
