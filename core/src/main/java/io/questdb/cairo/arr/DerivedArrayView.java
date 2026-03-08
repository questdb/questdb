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

    /**
     * Takes two n-dimensional arrays, and fills the {@code shapeOut} list with the
     * shape of the array that tightly fits both of them, broadcasting the smaller
     * array's dimensions if necessary.
     * <p>
     * The resulting shape is as long as the longer of the two arrays' shapes.
     * <p>
     * The shape lists are aligned at the right end, matching up the rightmost
     * number in each (i.e., the matching deepest dimensions of each array).
     * <p>
     * The missing numbers in the shorter shape are set to 1.
     * <p>
     * Where both numbers are present, they must either be equal, or one of them
     * must be 1.
     * <p>
     * The resulting number at matching position is the larger of the two numbers.
     * <p>
     * Examples:
     * <pre>
     *     [ 1 ]      [ 1 ]      [ 1 ]    [ 1 2 ]    [ 2 2 ]
     *     [ 2 ]    [ 1 2 ]    [ 2 2 ]    [ 2 1 ]    [ 2 3 ]
     *     -----    -------    -------    -------   --------
     *     [ 2 ]    [ 1 2 ]    [ 2 2 ]    [ 2 2 ]    illegal!
     * </pre>
     *
     * @param left            the left array
     * @param right           the right array
     * @param shapeOut        the target list for the shape
     * @param leftArgPosition position of the left array in SQL syntax, to report error if needed
     */
    public static void computeBroadcastShape(ArrayView left, ArrayView right, IntList shapeOut, int leftArgPosition) {
        int nDimsA = left.getDimCount();
        int nDimsB = right.getDimCount();
        int maxDims = Math.max(nDimsA, nDimsB);
        shapeOut.setPos(maxDims);

        for (int i = 1; i <= maxDims; i++) {
            int posA = nDimsA - i;
            int posB = nDimsB - i;
            int dimLenA = (posA >= 0) ? left.shape.get(posA) : 1;
            int dimLenB = (posB >= 0) ? right.shape.get(posB) : 1;
            if (dimLenA == dimLenB) {
                shapeOut.setQuick(maxDims - i, dimLenA);
            } else if (dimLenA == 1) {
                shapeOut.setQuick(maxDims - i, dimLenB);
            } else if (dimLenB == 1) {
                shapeOut.setQuick(maxDims - i, dimLenA);
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

    /**
     * Broadcasts this array's shape into the target shape. See the broadcasting rules
     * {@linkplain #computeBroadcastShape here}
     * <p>
     * <strong>NOTE:</strong> this method is unsafe to use with an invalid target shape.
     * It only uses {@code assert} and doesn't throw an exception in production code.
     */
    public void broadcast(IntList targetShape) {
        int targetDims = targetShape.size();
        int originalDims = getDimCount();
        assert targetDims >= originalDims;
        if (originalDims < targetDims) {
            prependDimensions(targetDims - originalDims);
        }

        boolean changed = false;
        for (int i = 0; i < targetDims; i++) {
            int thisDimLen = shape.getQuick(i);
            int targetDimLen = targetShape.getQuick(i);
            if (targetDimLen != 1) {
                if (thisDimLen == 1) {
                    strides.setQuick(i, 0);
                    shape.setQuick(i, targetDimLen);
                    changed = true;
                } else {
                    assert thisDimLen == targetDimLen : "incompatible target shape";
                }
            }
        }

        if (changed) {
            isVanilla = false;
        }
    }

    /**
     * Flattens this array to a 1D array. Works only for a vanilla array, because a
     * non-vanilla array can't be flattened through shape/stride manipulation.
     */
    public void flatten() {
        assert isVanilla : "flatten() only allowed on a vanilla array";
        shape.clear();
        shape.add(getCardinality());
        strides.clear();
        strides.add(1);
        type = ColumnType.encodeArrayType(getElemType(), 1);
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

    /**
     * Takes a slice of this array at the provided dimension, and the provided
     * lower and upper bounds for the index at that dimension. After the slicing
     * operation, the dimension {@code dim} is constrained to indices 0..(hi - lo)
     * (upper-exclusive), and the new index 0 corresponds to the previous index
     * {@code lo}.
     *
     * @param dim the dimension to slice
     * @param lo  lower bound of the slice
     * @param hi  upper bound of the slice (exclusive)
     */
    public void slice(int dim, int lo, int hi) {
        assert lo >= 0 && hi >= 0 : "lo and hi must be non-negative";

        int dimLen = getDimLen(dim);
        if (hi > dimLen) {
            hi = dimLen;
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

    /**
     * Extracts a sub-array from this array. It does so by first taking a slice of the
     * array at dimension {@code dim} that contains just the provided {@code index},
     * and then removing the dimension {@code dim} from the array's shape.
     *
     * @param dim   the dimension at which the sub-array is found
     * @param index the index of the sub-array within that dimension
     */
    public void subArray(int dim, int index) {
        slice(dim, index, index + 1);
        if (getDimLen(dim) != 0) {
            removeDim(dim);
        } else {
            ofNull();
        }
    }

    /**
     * Transposes this array, reversing its shape and strides.
     */
    public void transpose() {
        if (isVanilla && getDimCount() > 1) {
            isVanilla = false;
        }
        strides.reverse();
        shape.reverse();
    }

    private void removeDim(int dim) {
        assert dim >= 0 && dim < shape.size() : "dim out of range: " + dim;
        shape.removeIndex(dim);
        strides.removeIndex(dim);
        type = ColumnType.encodeArrayType(getElemType(), getDimCount() - 1);
    }
}
