/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.std;

public class BitmapIndexUtilsNative {
    public static int findFirstLastInFrame(
            int outIndex,
            long rowIdLo,
            long rowIdHi,
            long timestampColAddress,
            long frameBaseOffset,
            long symbolIndexAddress,
            long symbolIndexCount,
            long symbolIndexPosition,
            long samplePeriodsAddress,
            int samplePeriodsCount,
            long samplePeriodIndexOffset,
            long rowIdOutAddress,
            int outSize) {
        if (symbolIndexAddress > 0) {
            return findFirstLastInFrame0(
                    outIndex,
                    rowIdLo,
                    rowIdHi,
                    timestampColAddress,
                    frameBaseOffset,
                    symbolIndexAddress,
                    symbolIndexCount,
                    symbolIndexPosition,
                    samplePeriodsAddress,
                    samplePeriodsCount,
                    samplePeriodIndexOffset,
                    rowIdOutAddress,
                    outSize
            );
        } else {
            return findFirstLastInFrameNoFilter0(
                    outIndex,
                    rowIdLo,
                    rowIdHi,
                    timestampColAddress,
                    frameBaseOffset,
                    samplePeriodsAddress,
                    samplePeriodsCount,
                    samplePeriodIndexOffset,
                    rowIdOutAddress,
                    outSize
            );
        }
    }

    public static void latestScanBackward(long keysMemory, long keysMemorySize, long valuesMemory,
                                          long valuesMemorySize, long argsMemory, long unIndexedNullCount,
                                          long maxValue, long minValue,
                                          int partitionIndex, int blockValueCountMod) {
        assert keysMemory > 0;
        assert keysMemorySize > 0;
        assert valuesMemory > 0;
        assert valuesMemorySize > 0;
        assert argsMemory > 0;
        assert partitionIndex >= 0;
        assert blockValueCountMod + 1 == Numbers.ceilPow2(blockValueCountMod + 1);

        latestScanBackward0(keysMemory, keysMemorySize, valuesMemory, valuesMemorySize, argsMemory, unIndexedNullCount,
                maxValue, minValue, partitionIndex, blockValueCountMod);
    }

    private static native void latestScanBackward0(long keysMemory, long keysMemorySize, long valuesMemory,
                                                   long valuesMemorySize, long argsMemory, long unIndexedNullCount,
                                                   long maxValue, long minValue,
                                                   int partitionIndex, int blockValueCountMod);

    private static native int findFirstLastInFrame0(
            int outIndex,
            long rowIdLo,
            long rowIdHi,
            long timestampColAddress,
            long frameBaseOffset,
            long symbolIndexAddress,
            long symbolIndexCount,
            long symbolIndexPosition,
            long samplePeriodsAddress,
            int samplePeriodCount,
            long samplePeriodIndexOffset,
            long rowIdOutAddress,
            int outSize);

    private static native int findFirstLastInFrameNoFilter0(
            int outIndex,
            long rowIdLo,
            long rowIdHi,
            long timestampColAddress,
            long frameBaseOffset,
            long samplePeriodsAddress,
            int samplePeriodCount,
            long samplePeriodIndexOffset,
            long rowIdOutAddress,
            int outSize);
}
