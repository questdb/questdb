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

package io.questdb.std;

public final class Vect {

    public static native double avgDouble(long pDouble, long count);

    public static native double avgInt(long pInt, long count);

    public static native double avgLong(long pLong, long count);

    public static native long binarySearch64Bit(long pData, long value, long low, long high, int scanDirection);

    public static native long binarySearchIndexT(long pData, long value, long low, long high, int scanDirection);

    public static long boundedBinarySearch64Bit(long pData, long value, long low, long high, int scanDirection) {
        long index = binarySearch64Bit(pData, value, low, high, scanDirection);
        if (index < 0) {
            return (-index - 1) - 1;
        }
        return index;
    }

    public static long boundedBinarySearchIndexT(long pData, long value, long low, long high, int scanDirection) {
        long index = binarySearchIndexT(pData, value, low, high, scanDirection);
        if (index < 0) {
            return (-index - 1) - 1;
        }
        return index;
    }

    public static native void copyFromTimestampIndex(long pIndex, long indexLo, long indexHi, long pTs);

    public static native void flattenIndex(long pIndex, long count);

    private static native void freeMergedIndex(long pIndex);

    public static void freeMergedIndex(long pIndex, long indexSize) {
        freeMergedIndex(pIndex);
        Unsafe.recordMemAlloc(-indexSize, MemoryTag.NATIVE_O3);
    }

    public static native long getPerformanceCounter(int index);

    public static native int getPerformanceCountersCount();

    public static native int getSupportedInstructionSet();

    public static String getSupportedInstructionSetName() {
        int inst = getSupportedInstructionSet();
        String base;
        if (inst >= 10) {
            base = "AVX512";
        } else if (inst >= 8) {
            base = "AVX2";
        } else if (inst >= 5) {
            base = "SSE4.1";
        } else if (inst >= 2) {
            base = "SSE2";
        } else {
            base = "Vanilla";
        }
        return " [" + base + "," + Vect.getSupportedInstructionSet() + "]";
    }

    public static native void indexReshuffle16Bit(long pSrc, long pDest, long pIndex, long count);

    public static native void indexReshuffle256Bit(long pSrc, long pDest, long pIndex, long count);

    public static native void indexReshuffle32Bit(long pSrc, long pDest, long pIndex, long count);

    public static native void indexReshuffle64Bit(long pSrc, long pDest, long pIndex, long count);

    public static native void indexReshuffle8Bit(long pSrc, long pDest, long pIndex, long count);

    public static native long makeTimestampIndex(long pData, long low, long high, long pIndex);

    public static native double maxDouble(long pDouble, long count);

    public static native int maxInt(long pInt, long count);

    public static native long maxLong(long pLong, long count);

    public static void memcpy(long dst, long src, long len) {
        // the split length was determined experimentally
        // using 'MemCopyBenchmark' bench
        if (len < 4096) {
            Unsafe.getUnsafe().copyMemory(src, dst, len);
        } else {
            memcpy0(src, dst, len);
        }
    }

    public static native void memmove(long dst, long src, long len);

    public static native void memset(long dst, long len, int value);

    //caller must call freeMergedIndex !!!
    private static native long mergeLongIndexesAsc(long pIndexStructArray, int count);

    public static long mergeLongIndexesAsc(long pIndexStructArray, int count, long indexSize) {
        Unsafe.recordMemAlloc(indexSize, MemoryTag.NATIVE_O3);
        return mergeLongIndexesAsc(pIndexStructArray, count);
    }

    public static native void mergeShuffle16Bit(long pSrc1, long pSrc2, long pDest, long pIndex, long count);

    public static native void mergeShuffle256Bit(long pSrc1, long pSrc2, long pDest, long pIndex, long count);

    public static native void mergeShuffle32Bit(long pSrc1, long pSrc2, long pDest, long pIndex, long count);

    public static native void mergeShuffle64Bit(long pSrc1, long pSrc2, long pDest, long pIndex, long count);

    public static native void mergeShuffle8Bit(long pSrc1, long pSrc2, long pDest, long pIndex, long count);

    //caller must call freeMergedIndexes !!!
    public static native long mergeTwoLongIndexesAsc(long pIndex1, long index1Count, long pIndex2, long index2Count);

    public static native double minDouble(long pDouble, long count);

    public static native int minInt(long pInt, long count);

    public static native long minLong(long pLong, long count);

    public static native void oooCopyIndex(long mergeIndexAddr, long mergeIndexSize, long dstAddr);

    public static native void oooMergeCopyBinColumn(
            long mergeIndexAddr,
            long mergeIndexSize,
            long srcDataFixAddr,
            long srcDataVarAddr,
            long srcOooFixAddr,
            long srcOooVarAddr,
            long dstFixAddr,
            long dstVarAddr,
            long dstVarOffset
    );

    public static native void oooMergeCopyStrColumn(
            long mergeIndexAddr,
            long mergeIndexSize,
            long srcDataFixAddr,
            long srcDataVarAddr,
            long srcOooFixAddr,
            long srcOooVarAddr,
            long dstFixAddr,
            long dstVarAddr,
            long dstVarOffset
    );

    public static native void quickSortLongIndexAscInPlace(long pLongData, long count);

    public static native void radixSortLongIndexAscInPlace(long pLongData, long count, long pCpy);

    public static native void resetPerformanceCounters();

    public static native void setMemoryDouble(long pData, double value, long count);

    public static native void setMemoryFloat(long pData, float value, long count);

    public static native void setMemoryInt(long pData, int value, long count);

    public static native void setMemoryLong(long pData, long value, long count);

    public static native void setMemoryShort(long pData, short value, long count);

    public static native void setVarColumnRefs32Bit(long address, long initialOffset, long count);

    public static native void setVarColumnRefs64Bit(long address, long initialOffset, long count);

    public static native void shiftCopyFixedSizeColumnData(long shift, long src, long srcLo, long srcHi, long dstAddr);

    public static native long shiftTimestampIndex(long pSrc, long count, long pDest);

    /**
     * Sorts assuming 128-bit integers.
     * Can be used to sort pairs of longs from DirectLongList when both longs are positive
     *
     * @param pLongData memory address
     * @param count     count of 128bit integers to sort
     */
    public static native void sort128BitAscInPlace(long pLongData, long count);

    public static native void sortLongIndexAscInPlace(long pLongData, long count);

    public static native void sortULongAscInPlace(long pLongData, long count);

    public static native long sortVarColumn(
            long mergedTimestampsAddr,
            long valueCount,
            long srcDataAddr,
            long srcIndxAddr,
            long tgtDataAddr,
            long tgtIndxAdd
    );

    public static native double sumDouble(long pDouble, long count);

    public static native double sumDoubleKahan(long pDouble, long count);

    public static native double sumDoubleNeumaier(long pDouble, long count);

    public static native long sumInt(long pInt, long count);

    public static native long sumLong(long pLong, long count);

    private static native void memcpy0(long src, long dst, long len);
}
