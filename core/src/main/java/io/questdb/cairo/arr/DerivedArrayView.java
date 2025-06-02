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

/**
 * A view over an array. Does not own the backing flat array. The array contents can't
 * be mutated through this view, but the view itself can be: you can change what slice
 * of the underlying flat array it represents, as well as transpose it.
 */
public class DerivedArrayView extends ArrayView {

    /**
     * Adds extra dimensions to the array view.
     * For example, a 1D array [1, 2, 3] with one dimension added becomes a 2D array [[1, 2, 3]].
     * New dimensions are added at the beginning with length 1.
     *
     * @param count Number of dimensions to add
     */
    public void addDimensions(int count) {
        if (count == 0) {
            return;
        }

        if (count + getDimCount() > ColumnType.ARRAY_NDIMS_LIMIT) {
            throw CairoException.nonCritical()
                    .put("cannot add ")
                    .put(count)
                    .put(" dimensions, would exceed maximum array dimensions (")
                    .put(ColumnType.ARRAY_NDIMS_LIMIT)
                    .put(")");
        }

        // new stride is the same as the outermost current stride
        int newStride = getStride(0);

        shape.rshift(count);
        strides.rshift(count);
        for (int i = 0; i < count; i++) {
            shape.setQuick(i, 1); // new dimensions have all length 1
            strides.setQuick(i, newStride); // new dimensions have the same stride as the outermost existing dimension
        }

        // Update the type to reflect the new dimension count
        type = ColumnType.encodeArrayType(getElemType(), getDimCount() + count);
    }

    public void flattenDim(int dim, int argPos) {
        final int nDims = getDimCount();
        assert dim >= 0 && dim < nDims : "dim out of range: " + dim + ", nDims: " + nDims;
        if (getStride(dim) == 1 && getDimLen(dim) > 1) {
            throw CairoException.nonCritical()
                    .position(argPos)
                    .put("cannot flatten dim with stride = 1 and length > 1 [dim=").put(dim + 1)
                    .put(", dimLen=").put(getDimLen(dim))
                    .put(", nDims=").put(nDims).put(']');
        }
        final int dimToFlattenInto;
        if (dim == 0) {
            dimToFlattenInto = 1;
        } else if (dim == nDims - 1) {
            dimToFlattenInto = dim - 1;
        } else {
            dimToFlattenInto = getStride(dim - 1) < getStride(dim + 1) ? dim - 1 : dim + 1;
        }
        isVanilla = false;
        shape.set(dimToFlattenInto, shape.get(dimToFlattenInto) * shape.get(dim));
        removeDim(dim);
    }

    public void of(ArrayView other) {
        this.type = other.getType();
        this.flatView = other.flatView();
        this.flatViewOffset = other.getFlatViewOffset();
        this.flatViewLength = other.getFlatViewLength();
        this.isVanilla = other.isVanilla;
        shape.clear();
        strides.clear();
        int nDims = other.getDimCount();
        for (int i = 0; i < nDims; i++) {
            shape.add(other.getDimLen(i));
            strides.add(other.getStride(i));
        }
    }

    public void removeDim(int dim) {
        assert dim >= 0 && dim < shape.size() : "dim out of range: " + dim;
        isVanilla = false;
        shape.removeIndex(dim);
        strides.removeIndex(dim);
        type = ColumnType.encodeArrayType(getElemType(), getDimCount() - 1);
    }

    public void slice(int dim, int lo, int hi, int argPos) {
        if (dim < 0 || dim >= getDimCount()) {
            throw CairoException.nonCritical().position(argPos)
                    .put("array dimension doesn't exist [dim=").put(dim + 1)
                    .put(", nDims=").put(getDimCount()).put(']');
        }
        int dimLen = getDimLen(dim);
        if (hi > dimLen) {
            hi = dimLen;
        }
        if (lo < 0 || hi < 0) {
            // Report bounds + 1 because that's what the user entered, the caller subtracted 1
            // to align with Postgres' 1-based array indexing
            throw CairoException.nonCritical()
                    .position(argPos)
                    .put("array slice bounds must be positive [dim=").put(dim + 1)
                    .put(", dimLen=").put(dimLen)
                    .put(", lowerBound=").put(lo + 1)
                    .put(", upperBound=").put(hi + 1)
                    .put(']');
        }
        if (lo == 0 && hi == dimLen) {
            return;
        }
        isVanilla = false;
        if (lo < hi) {
            flatViewOffset += lo * getStride(dim);
            shape.set(dim, hi - lo);
        } else {
            shape.set(dim, 0);
        }
    }

    public void subArray(int dim, int index, int argPos) {
        slice(dim, index, index + 1, argPos);
        if (getDimLen(dim) != 0) {
            removeDim(dim);
        } else {
            shape.set(0, 0);
        }
    }

    public void transpose() {
        isVanilla = false;
        strides.reverse();
        shape.reverse();
    }
}
