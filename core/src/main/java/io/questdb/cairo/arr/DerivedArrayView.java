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
import io.questdb.std.IntList;

/**
 * A view over an array. Does not own the backing flat array. The array contents can't
 * be mutated through this view, but the view itself can be: you can change what slice
 * of the underlying flat array it represents, as well as transpose it.
 */
public class DerivedArrayView extends ArrayView {

    public static void computeBroadcastShape(ArrayView left, ArrayView right, IntList out, int leftArgPosition) {
        out.clear();
        int nDimsA = left.getDimCount();
        int nDimsB = right.getDimCount();
        int maxDims = Math.max(nDimsA, nDimsB);
        out.setPos(maxDims);

        for (int i = 0; i < maxDims; i++) {
            int posA = nDimsA - 1 - i;
            int posB = nDimsB - 1 - i;
            int dimLenA = (posA >= 0) ? left.shape.get(posA) : 1;
            int dimLenB = (posB >= 0) ? right.shape.get(posB) : 1;
            if (dimLenA == dimLenB) {
                out.setQuick(maxDims - 1 - i, dimLenA);
            } else if (dimLenA == 1) {
                out.setQuick(maxDims - 1 - i, dimLenB);
            } else if (dimLenB == 1) {
                out.setQuick(maxDims - 1 - i, dimLenA);
            } else {
                throw CairoException.nonCritical().position(leftArgPosition)
                        .put("arrays have incompatible shapes [leftShape=").put(left.shapeToString())
                        .put(", rightShape=").put(right.shapeToString())
                        .put(']');
            }
        }
    }

    /**
     * Prepends extra dimensions to this array view. For example, a 1D array {@code
     * [1, 2, 3]} with one prepended dimension becomes a 2D array {@code
     * [[1], [2], [3]]}. The appended dimensions have length 1.
     * <p>
     * <strong>NOTE:</strong> the new dimensions have stride set to zero. This has no
     * effect as long as the dimension length is left at 1, and is the correct value
     * for all dimension lengths in a broadcast operation, causing the same element to
     * appear at all positions along the dimension with no additional storage.
     *
     * @param count Number of dimensions to add
     */
    public void appendDimensions(int count) {
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
        for (int i = 0; i < count; i++) {
            shape.add(1);
            strides.add(0);
        }
        this.type = ColumnType.encodeArrayType(getElemType(), getDimCount() + count);
    }

    public void broadcast(IntList targetShape) {
        int targetDims = targetShape.size();
        int originalDims = getDimCount();
        assert targetDims >= originalDims;
        if (originalDims < targetDims) {
            prependDimensions(targetDims - originalDims);
        }

        boolean changed = false;
        for (int i = 0; i < targetDims; i++) {
            if (shape.getQuick(i) == 1 && targetShape.getQuick(i) != 1) {
                strides.setQuick(i, 0);
                shape.setQuick(i, targetShape.getQuick(i));
                changed = true;
            }
        }

        if (changed) {
            isVanilla = false;
        }
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

    public void ofNull() {
        type = ColumnType.NULL;
        flatView = null;
        flatViewLength = 0;
        flatViewOffset = 0;
        shape.clear();
        strides.clear();
        isVanilla = true;
    }

    /**
     * Prepends extra dimensions to this array view. For example, a 1D array {@code
     * [1, 2, 3]} with one prepended dimension becomes a 2D array {@code
     * [[1, 2, 3]]}. The prepended dimensions have length 1.
     * <p>
     * <strong>NOTE:</strong> the new dimensions have stride set to zero. This has no
     * effect as long as the dimension length is left at 1, and is the correct value
     * for all dimension lengths in a broadcast operation, causing the same sub-array
     * to repeat at all positions along the dimension with no additional storage.
     */
    public void prependDimensions(int count) {
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

        shape.rshift(count);
        strides.rshift(count);
        for (int i = 0; i < count; i++) {
            shape.setQuick(i, 1); // all new dimensions have length 1
            strides.setQuick(i, 0); // all new strides set to 0
        }

        // Update the type to reflect the new dimension count
        type = ColumnType.encodeArrayType(getElemType(), getDimCount() + count);
    }

    public void removeDim(int dim) {
        assert dim >= 0 && dim < shape.size() : "dim out of range: " + dim;
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

        if (isVanilla) {
            for (int i = 0; i < dim; i++) {
                if (shape.getQuick(i) > 1) {
                    isVanilla = false;
                }
            }
        }

        if (lo < hi) {
            flatViewOffset += lo * getStride(dim);
            flatViewLength = flatViewLength / dimLen * (hi - lo);
            shape.set(dim, hi - lo);
        } else {
            shape.set(dim, 0);
            isVanilla = true;
            flatViewLength = 0;
        }
    }

    public void subArray(int dim, int index, int argPos) {
        slice(dim, index, index + 1, argPos);
        if (getDimLen(dim) != 0) {
            removeDim(dim);
        } else {
            ofNull();
        }
    }

    public void transpose() {
        if (isVanilla && getDimCount() > 1) {
            isVanilla = false;
        }
        strides.reverse();
        shape.reverse();
    }
}
