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

package io.questdb.griffin.engine.functions.geohash;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.std.Rows;

public class GeoHashNative {

    public static native long iota(long address, long size, long init);

    public static void latestByAndFilterPrefix(
            PageFrameMemoryPool frameMemoryPool,
            long keysMemory,
            long keysMemorySize,
            long valuesMemory,
            long valuesMemorySize,
            long argsMemory,
            long unIndexedNullCount,
            long maxValue,
            long minValue,
            int frameIndex,
            int blockValueCountMod,
            int geoHashColumnIndex,
            int geoHashColumnType,
            long prefixesAddress,
            long prefixesCount
    ) {
        long geoHashColumnAddress = 0;
        // hashColumnIndex can be -1 for latest by part only (no prefixes to match)
        if (geoHashColumnIndex > -1) {
            final PageFrameMemory frameMemory = frameMemoryPool.navigateTo(frameIndex);
            geoHashColumnAddress = frameMemory.getPageAddress(geoHashColumnIndex);
        }

        final int geoHashColumnSize = ColumnType.isGeoHash(geoHashColumnType) ? getPow2SizeOfGeoHashType(geoHashColumnType) : -1;
        assert geoHashColumnIndex == -1 || geoHashColumnSize != -1 : "no within filter or within on geohash column expected";

        latestByAndFilterPrefix(
                keysMemory,
                keysMemorySize,
                valuesMemory,
                valuesMemorySize,
                argsMemory,
                unIndexedNullCount,
                maxValue,
                minValue,
                // Invert page frame indexes, so that they grow asc in time order.
                // That's to be able to do post-processing (sorting) of the result set.
                Rows.MAX_SAFE_PARTITION_INDEX - frameIndex,
                blockValueCountMod,
                geoHashColumnAddress,
                geoHashColumnSize,
                prefixesAddress,
                prefixesCount
        );
    }

    public static native long slideFoundBlocks(long argsAddress, long argsCount);

    private static int getPow2SizeOfGeoHashType(int type) {
        return 1 << ColumnType.pow2SizeOfBits(ColumnType.getGeoHashBits(type));
    }

    private static native void latestByAndFilterPrefix(
            long keysMemory,
            long keysMemorySize,
            long valuesMemory,
            long valuesMemorySize,
            long argsMemory,
            long unIndexedNullCount,
            long maxValue,
            long minValue,
            int invertedFrameIndex,
            int blockValueCountMod,
            long geoHashColumnAddress,
            int geoHashColumnSize,
            long prefixesAddress,
            long prefixesCount
    );
}
