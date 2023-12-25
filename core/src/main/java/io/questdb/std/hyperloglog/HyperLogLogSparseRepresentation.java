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

package io.questdb.std.hyperloglog;

import java.util.Arrays;

public class HyperLogLogSparseRepresentation implements HyperLogLogRepresentation {
    private static final int SPARSE_PRECISION = 25;
    private static final int SPARSE_SET_INITIAL_CAPACITY = 16;
    private static final int TMP_LIST_CAPACITY = 256;

    private final int densePrecision;
    private final int sparseSetMaxSize;
    private final int sparseRegisterCount;
    private final long leadingZerosMask;
    private final long encodeMask;

    private int[] sparseSet;
    private int sparseSetEndIdx = 0;
    private final int[] tmpList;
    private int tmpListEndIdx = 0;
    private HyperLogLogDenseRepresentation cachedDenseRepresentation = null;

    public HyperLogLogSparseRepresentation(int densePrecision) {
        this.sparseRegisterCount = 1 << SPARSE_PRECISION;
        this.tmpList = new int[TMP_LIST_CAPACITY];
        this.sparseSet = new int[SPARSE_SET_INITIAL_CAPACITY];
        this.sparseSetMaxSize = calculateSparseSetMaxSize(densePrecision);
        this.densePrecision = densePrecision;
        this.leadingZerosMask = 1L << (densePrecision - 1);
        this.encodeMask = (0x8000000000000000L >> (SPARSE_PRECISION - densePrecision - 1)) >>> (densePrecision);
    }

    public static int calculateSparseSetMaxSize(int densePrecision) {
        // Each register in the dense representation has the size of 1 byte.
        int denseRepresentationInBytes = 1 << densePrecision;
        int tmpListMaxSizeInBytes = TMP_LIST_CAPACITY << 2;
        // We divide the number of bytes by 4 because each element of the sparse set has the size of 4 bytes.
        return Math.max((denseRepresentationInBytes - tmpListMaxSizeInBytes) >> 2, 0);
    }

    @Override
    public void add(long hash) {
        int encodedHash = encodeHash(hash);
        tmpList[tmpListEndIdx++] = encodedHash;
        if (tmpListEndIdx == tmpList.length) {
            mergeTempListWithSparseSet();
        }
    }

    @Override
    public long computeCardinality() {
        mergeTempListWithSparseSet();
        return linearCounting(sparseRegisterCount, (sparseRegisterCount - sparseSetEndIdx));
    }

    private long linearCounting(int total, int empty) {
        return Math.round(total * Math.log(total / (double) empty));
    }

    @Override
    public boolean isFull() {
        return sparseSetEndIdx >= sparseSetMaxSize;
    }

    @Override
    public HyperLogLogRepresentation convertToDense() {
        mergeTempListWithSparseSet();
        HyperLogLogDenseRepresentation denseRepresentation = cachedDenseRepresentation;
        if (denseRepresentation == null) {
            denseRepresentation = new HyperLogLogDenseRepresentation(densePrecision);
            cachedDenseRepresentation = denseRepresentation;
        } else {
            denseRepresentation.clear();
        }
        for (int i = 0; i < sparseSetEndIdx; i++) {
            int encoded = sparseSet[i];
            int idx = decodeDenseIndex(encoded);
            byte leadingZeros = (byte) decodeNumberOfLeadingZeros(encoded);
            denseRepresentation.add(idx, leadingZeros);
        }
        return denseRepresentation;
    }

    @Override
    public void clear() {
        sparseSetEndIdx = 0;
        tmpListEndIdx = 0;
    }

    private int encodeHash(long hash) {
        // This is a modified version of the encoding proposed in the paper. Potentially, it consumes more memory
        // but allows for fewer branches while merging the temp list with the sparse set.
        //
        // There are two differences compared to the original version:
        // - The least significant bit is set to 0 when the number of leading zeros is expressed as an integer.
        // - The 7 least significant bits are set to 1 when the number of leading zeros can be calculated based
        // on the register index.
        int registerIdx = computeRegisterIndex(hash) << 7;
        if ((hash & encodeMask) == 0) {
            int leadingZeros = computeNumberOfLeadingZeros(hash) << 1;
            return registerIdx | leadingZeros;
        }
        return registerIdx | 0x7F;
    }

    private static int computeRegisterIndex(long hash) {
        return (int) (hash >>> (Long.SIZE - SPARSE_PRECISION));
    }

    private int computeNumberOfLeadingZeros(long hash) {
        return Long.numberOfLeadingZeros((hash << densePrecision) | leadingZerosMask) + 1;
    }

    private int decodeDenseIndex(int encoded) {
        int sparseIndex = decodeSparseIndex(encoded);
        return sparseIndex >>> (SPARSE_PRECISION - densePrecision);
    }

    private static int decodeSparseIndex(int encoded) {
        return encoded >>> 7;
    }

    private int decodeNumberOfLeadingZeros(int encoded) {
        if ((encoded & 1) == 0) {
            return (encoded >>> 1) & 0x3F;
        } else {
            return Integer.numberOfLeadingZeros(encoded << densePrecision) + 1;
        }
    }

    private void mergeTempListWithSparseSet() {
        ensureCapacity();

        Arrays.sort(tmpList, 0, tmpListEndIdx);

        int oldSparseSetIdx = sparseSetEndIdx - 1;
        int tmpListIdx = tmpListEndIdx - 1;
        int newSparseSetIdx = sparseSetEndIdx + tmpListEndIdx - 1;
        while (tmpListIdx >= 0) {
            if (oldSparseSetIdx >= 0 && sparseSet[oldSparseSetIdx] > tmpList[tmpListIdx]) {
                sparseSet[newSparseSetIdx--] = sparseSet[oldSparseSetIdx--];
            } else {
                sparseSet[newSparseSetIdx--] = tmpList[tmpListIdx--];
            }
        }

        int newLen = sparseSetEndIdx + tmpListEndIdx;
        int newSparseSetEndIdx = newLen > 0 ? 1 : 0;
        // remove duplicates
        for (int i = 1; i < newLen; i++) {
            int prev = decodeSparseIndex(sparseSet[newSparseSetEndIdx - 1]);
            int curr = decodeSparseIndex(sparseSet[i]);
            if (prev != curr) {
                sparseSet[newSparseSetEndIdx++] = sparseSet[i];
            }
        }

        sparseSetEndIdx = newSparseSetEndIdx;
        tmpListEndIdx = 0;
    }

    private void ensureCapacity() {
        int newLen = sparseSetEndIdx + tmpListEndIdx;
        if (sparseSet.length < newLen) {
            int[] newSparseSet = new int[newLen];
            System.arraycopy(sparseSet, 0, newSparseSet, 0, sparseSetEndIdx);
            sparseSet = newSparseSet;
        }
    }
}
