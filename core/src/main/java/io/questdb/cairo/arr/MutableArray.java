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

    public final void resetToDefaultStrides() {
        resetToDefaultStrides(Integer.MAX_VALUE >> 3, -1);
    }

    public final void resetToDefaultStrides(int maxArrayElemCount, int errorPos) {
        assert maxArrayElemCount <= Integer.MAX_VALUE >> ColumnType.pow2SizeOf(ColumnType.decodeArrayElementType(this.type))
                : "maxArrayElemCount > " +
                (Integer.MAX_VALUE >> ColumnType.pow2SizeOf(ColumnType.decodeArrayElementType(this.type)));

        final int nDims = shape.size();
        strides.clear();
        for (int i = 0; i < nDims; i++) {
            strides.add(0);
        }

        int stride = 1;
        for (int i = nDims - 1; i >= 0; i--) {
            int dimLen = shape.get(i);
            if (dimLen == 0) {
                throw new IllegalStateException("Zero dimLen at " + i);
            }
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

    public final void setDimLen(int dimension, int length) {
        if (length <= 0) {
            throw CairoException.nonCritical()
                    .put("dimension length must be positive [dim=").put(dimension)
                    .put(", dimLen=").put(length)
                    .put(']');
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
        for (int i = 0; i < nDims; i++) {
            shape.add(0);
        }
    }
}
