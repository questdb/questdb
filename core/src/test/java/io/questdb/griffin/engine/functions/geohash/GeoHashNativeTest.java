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

package io.questdb.griffin.engine.functions.geohash;

import io.questdb.cairo.AbstractCairoTest;
import io.questdb.cairo.BitmapIndexFwdReader;
import io.questdb.cairo.BitmapIndexTest;
import io.questdb.cairo.BitmapIndexWriter;
import io.questdb.griffin.engine.table.LatestByArguments;
import io.questdb.std.DirectLongList;
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

public class GeoHashNativeTest extends AbstractCairoTest {
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
    public void testLatestByAndFilterPrefixShouldNotAccessUnmappedMemory() {
        Path path = new Path().of(configuration.getRoot());

        // allocate and map 1-page index
        long pageSize = Files.PAGE_SIZE;
        int N = (int) ((pageSize - 64) / 32);
        int keyCount = N + 1; // +1 is important here

        final DirectLongList rows = new DirectLongList(keyCount, MemoryTag.NATIVE_LONG_LIST);
        long argsAddress = LatestByArguments.allocateMemoryArray(1);

        try {
            BitmapIndexTest.create(configuration, path, "x", 64);

            rows.setCapacity(keyCount);
            GeoHashNative.iota(rows.getAddress(), rows.getCapacity(), 0);

            LatestByArguments.setRowsAddress(argsAddress, rows.getAddress());
            LatestByArguments.setRowsCapacity(argsAddress, rows.getCapacity());
            LatestByArguments.setKeyLo(argsAddress, 0);
            LatestByArguments.setKeyHi(argsAddress, keyCount);
            LatestByArguments.setRowsSize(argsAddress, 0);

            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                for (int i = 0; i < N; i++) {
                    writer.add(i, i);
                }
            }

            try (BitmapIndexFwdReader indexReader = new BitmapIndexFwdReader(configuration, path, "x", COLUMN_NAME_TXN_NONE, 0)) {
                GeoHashNative.latestByAndFilterPrefix(
                        indexReader.getKeyBaseAddress(),
                        indexReader.getKeyMemorySize(),
                        indexReader.getValueBaseAddress(),
                        indexReader.getValueMemorySize(),
                        argsAddress,
                        indexReader.getUnIndexedNullCount(),
                        255,
                        0,
                        0,
                        indexReader.getValueBlockCapacity(),
                        0,
                        0,
                        0,
                        0
                );
            }
        } finally {
            rows.close();
            path.close();
            LatestByArguments.releaseMemoryArray(argsAddress, 1);
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
