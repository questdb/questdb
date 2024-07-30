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

package io.questdb.griffin.engine.functions.geohash;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryPool;

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
            int hashColumnIndex,
            int hashColumnType,
            long prefixesAddress,
            long prefixesCount
    ) {
        long hashColumnAddress = 0;
        // hashColumnIndex can be -1 for latest by part only (no prefixes to match)
        if (hashColumnIndex > -1) {
            // TODO(puzpuzpuz): this has to be per-task
            final PageFrameMemory frameMemory = frameMemoryPool.navigateTo(frameIndex);
            hashColumnAddress = frameMemory.getPageAddress(hashColumnIndex);
        }

        // -1 must be dead case here
        final int hashColumnSize = ColumnType.isGeoHash(hashColumnType) ? getPow2SizeOfGeoHashType(hashColumnType) : -1;

        latestByAndFilterPrefix(
                keysMemory,
                keysMemorySize,
                valuesMemory,
                valuesMemorySize,
                argsMemory,
                unIndexedNullCount,
                maxValue,
                minValue,
                frameIndex,
                blockValueCountMod,
                hashColumnAddress,
                hashColumnSize,
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
            int frameIndex,
            int blockValueCountMod,
            long hashColumnAddress,
            int hashColumnSize,
            long prefixesAddress,
            long prefixesCount
    );
}
