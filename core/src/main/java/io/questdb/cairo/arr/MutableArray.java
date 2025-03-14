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

package io.questdb.cairo.arr;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;

public class MutableArray extends ArrayView {

    public final void setDimLen(int dimension, int length) {
        if (length < 0) {
            throw CairoException.nonCritical()
                    .put("dimension length must not be negative [dim=").put(dimension)
                    .put(", dimLen=").put(length)
                    .put(']');
        }
        if (length > DIM_MAX_LEN) {
            throw CairoException.nonCritical()
                    .put("dimension length out of range [dim=").put(dimension)
                    .put(", dimLen=").put(length)
                    .put(", maxLen=").put(DIM_MAX_LEN);

        }
        shape.set(dimension, length);
    }

    public final void setType(int encodedType) {
        assert ColumnType.isArray(encodedType);
        this.type = encodedType;
        shape.clear();
        strides.clear();
        flatViewLength = 0;
        int nDims = ColumnType.decodeArrayDimensionality(encodedType);
        shape.checkCapacity(nDims);
        strides.checkCapacity(nDims);
        for (int i = 0; i < nDims; i++) {
            shape.add(0);
        }
    }

    private int maxPossibleElemCount() {
        short elemType = ColumnType.decodeArrayElementType(this.type);
        return Integer.MAX_VALUE >> (elemType != ColumnType.UNDEFINED ? ColumnType.pow2SizeOf(elemType) : 0);
    }

    protected final void resetToDefaultStrides() {
        resetToDefaultStrides(Integer.MAX_VALUE >> 3, -1);
    }

    protected final void resetToDefaultStrides(int maxArrayElemCount, int errorPos) {
        assert maxArrayElemCount <= maxPossibleElemCount() : "maxArrayElemCount > " + maxPossibleElemCount();

        final int nDims = shape.size();
        strides.clear();
        for (int i = 0; i < nDims; i++) {
            strides.add(0);
        }

        // An empty array can have various shapes, such as (100_000_000, 100_000_000, 0).
        // Avoid initializing the strides in this case, because that may erroneously fail with "array is too large".
        if (isEmpty()) {
            this.flatViewLength = 0;
            return;
        }

        int stride = 1;
        for (int i = nDims - 1; i >= 0; i--) {
            int dimLen = shape.get(i);
            strides.set(i, stride);
            try {
                stride = Math.multiplyExact(stride, dimLen);
                if (stride > maxArrayElemCount) {
                    throw new ArithmeticException();
                }
            } catch (ArithmeticException e) {
                throw CairoException.nonCritical().position(errorPos)
                        .put("array element count exceeds max [max=")
                        .put(maxArrayElemCount)
                        .put(", shape=")
                        .put(shape)
                        .put(']');
            }
        }
        this.flatViewLength = stride;
    }
}
