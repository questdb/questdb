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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.Utf8Sequence;

/**
 * This class represents a flat array of numbers with a hierarchical addressing
 * scheme applied to it, which makes it usable as an N-dimensional array accessed
 * with N indexes. For example, here's a 4x3x2 array:
 * <pre>
 * {
 *     { {1, 2}, {3, 4}, {5, 6} },
 *     { {7, 8}, {9, 0}, {1, 2} },
 *     { {3, 4}, {5, 6}, {7, 8} },
 *     { {9, 0}, {1, 2}, {3, 4} }
 * }
 * </pre>
 * Its backing flat array looks like this:
 * <code>[1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4]</code>.
 * <p>
 * Hierarchical subdivision of the flat array, this example uses a 2x3x2 array (dots
 * represent elements, bars demarcate array slots):
 * <pre>
 * dim 0: |. . . . . .|. . . . . .| -- stride = 6, len = 2
 * dim 1: |. .|. .|. .|. .|. .|. .| -- stride = 2, len = 3
 * dim 2: |.|.|.|.|.|.|.|.|.|.|.|.| -- stride = 1, len = 2
 * </pre>
 * Formula to access a member with syntax `arr[i,j,k]`:
 * <pre>
 *     flatIndex = 6i + 2j + k
 * </pre>
 * Note that this just sums up the contributions at each dimension. We can perform
 * it in any order.
 *
 * <h2>Transpose</h2>
 * <p>
 * Transposing the array changes the formula so that we apply the strides in
 * reverse:
 * <pre>
 *     flatIndex = i + 2j + 6k
 * </pre>
 * <p>
 * In fact, we can order the dimensions any way we want -- it's all linear and the
 * order doesn't matter to the calculation. It only affects the meaning of each
 * coordinate in the array access expression.
 *
 * <h2>Slice</h2>
 * <p>
 * Slicing the array means limiting the range of an index. Example: `arr[1:2]`.
 * This constrains index `i` to be at least 1. But, we don't expose that to the
 * user; instead we keep the index zero-based, and add the lower bound to it as a
 * constant:
 * <pre>
 *     flatIndex = 6(1 + i) + 2j + k
 * </pre>
 * Now we can extract the constant:
 * <pre>
 *     flatIndex = 6 + 6i + 2j + k
 *     flatIndex = flatOffset + 6i + 2j + k
 * </pre>
 * And this is the full formula we use in the code. If we perform another slicing
 * on top of this, we get another constant, and add it to the existing
 * <i>flatOffset</i>.
 * <p>
 * <strong>NOTE:</strong> We use zero-based indexes here, but SQL uses one-based
 * array indexes!
 *
 * <h2>Flatten a Dimension</h2>
 * <p>
 * Flattening means eliminating a dimension from our addressing scheme. As an
 * example, let's flatten the dimension 0, but we can do it for any dimension
 * except the lowest one (with stride = 1). We'll be left with this:
 *
 * <pre>
 * dim 1: |. .|. .|. .|. .|. .|. .| -- stride = 2, len = 6
 * dim 2: |.|.|.|.|.|.|.|.|.|.|.|.| -- stride = 1, len = 2
 * </pre>
 * We can see the strides stayed the same, but we had to make the dimension 1
 * longer. We had to multiply its previous length by the length of the removed
 * dimension.
 * <p>
 * This will work regardless of the order of dimensions -- just find the dimension
 * with the next-finer stride and modify its length!
 * <p>
 * Our code only inspects the two neighboring dimensions, and chooses the one with
 * the finer stride. This is OK because we use either the "regular" dimension order
 * ("row-major" -- strides in descending order), or the "transposed" order
 * ("column-major", strides in ascending order), so the next-finer stride must be in
 * one of the neighboring dimensions.
 *
 * <h2>Take a Sub-Array</h2>
 * <p>
 * We can take a sub-array on any dimension. Examples for a 3D-array:
 * <ul>
 *     <li><code>arr[0]</code> removes the first dimension, and returns the 2D
 *          sub-array at index 0 of the removed dimension.
 *     <li><code>arr[0:, 1]</code> removes the middle dimension, taking, for each of
 *          the outer indices, the array at index 1 in the middle dimension, and returns
 *          the resulting 2D sub-array.
 * </ul>
 * Taking a sub-array is a composition of two operations: first slice it to a
 * single element in that dimension, then flatten the dimension. Because the
 * only allowed index at the dimension is 0 after slicing, we don't need to perform
 * a general flattening, we can simply remove the dimension without adjusting the
 * length of any other dimension. For the same reason, while general flattening is
 * not allowed on the dimension with stride 1, in this special case it is fine.
 */
public abstract class ArrayView implements QuietCloseable {

    /**
     * Maximum size of any given dimension.
     * <p>Why:
     * <ul>
     *   <li>Our buffers are at most Integer.MAX_VALUE bytes long</li>
     *   <li>Our largest datatype has 8 bytes</li>
     * </ul>
     * Assuming a 1-D array, <code>Integer.MAX_VALUE / Long.BYTES</code> gives us
     * a maximum of 2^28 - 1
     */
    public static final int DIM_MAX_LEN = (1 << 28) - 1;

    protected final IntList shape = new IntList(0);
    protected final IntList strides = new IntList(0);
    protected FlatArrayView flatView;
    protected int flatViewLength;
    protected int flatViewOffset;

    // indicates whether the array elements are contiguous in memory.
    protected boolean isVanilla = true;
    protected int type = ColumnType.UNDEFINED;

    /**
     * Appends all the values of this array to the supplied memory block,
     * in row-major order.
     */
    public final void appendDataToMem(MemoryA mem) {
        // We need isEmpty() check to protect us from running appendToMemRecursive() for an
        // almost unbounded number of steps, e.g., on an array of shape (100_000_000, 100_000_000, 0).
        // We also need it to protect from ptr == 0 in flatView.
        if (isNull() || isEmpty()) {
            return;
        }
        if (isVanilla) {
            flatView.appendToMemFlat(mem, flatViewOffset, flatViewLength);
        } else {
            appendToMemRecursive(0, 0, mem);
        }
    }

    public final void appendShapeToMem(MemoryA mem) {
        for (int i = 0, dim = getDimCount(); i < dim; ++i) {
            mem.putInt(getDimLen(i));
        }
    }

    public final boolean arrayEquals(ArrayView other) {
        if (type != other.type || shapeDiffers(other)) {
            return false;
        }
        // We need this check to protect from running arrayEqualsRecursive() for an almost unbounded
        // number of steps, e.g., on an array of shape (100_000_000, 100_000_000, 0).
        // We also need it to protect from ptr == 0 in flatView.
        if (isEmpty()) {
            return true;
        }
        if (isVanilla && other.isVanilla) {
            FlatArrayView flatViewLeft = flatView;
            FlatArrayView flatViewRight = other.flatView;

            int length = flatViewLength;
            if (length != other.flatViewLength) {
                return false;
            }
            switch (getElemType()) {
                case ColumnType.DOUBLE:
                    for (int i = 0; i < length; i++) {
                        if (!Numbers.equals(
                                flatViewLeft.getDoubleAtAbsIndex(flatViewOffset + i),
                                flatViewRight.getDoubleAtAbsIndex(other.flatViewOffset + i))
                        ) {
                            return false;
                        }
                    }
                    break;
                case ColumnType.LONG:
                    for (int i = 0; i < length; i++) {
                        if (flatViewLeft.getLongAtAbsIndex(flatViewOffset + i) !=
                                flatViewRight.getLongAtAbsIndex(other.flatViewOffset + i)
                        ) {
                            return false;
                        }
                    }
                    break;
                case ColumnType.NULL:
                    // The other array can only be NULL at this point. Empty array creation is disallowed by
                    // the SQL parser. By the time we are here we would have verified that both arrays
                    // have the same dimensionality. When this element type is NULL, it means dimensionality is 0
                    // (e.g. empty array). The only other array that is allowed to be empty would also have
                    // element type as NULL
                    return true;
                default:
                    throw new UnsupportedOperationException("Implemented only for DOUBLE and LONG");
            }
            return true;
        }
        return arrayEqualsRecursive(0, 0, other, 0);
    }

    /**
     * Performs binary search for a specified double value in a 1D array view.
     *
     * <p><b>Important Requirements:</b>
     * <ul>
     *   <li>The array <b>must be sorted</b> in either ascending or descending order</li>
     *   <li>The array <b>must not contain null values</b></li>
     * </ul>
     *
     * <p>The method automatically detects the sort order by comparing the first and
     * last elements.
     */
    public final int binarySearchDoubleValue1DArray(double value, boolean forwardScan) {
        if (isNull() || isEmpty()) {
            return 0;
        }
        int stride = getStride(0);

        int low = 0;
        int high = getDimLen(0) - 1;
        // empty array
        if (low > high) return 0;

        // determine sort direction
        double first = getDouble(low);
        double last = getDouble(high * stride);
        boolean ascending = first <= last;
        if (isVanilla) {
            return flatView.binarySearchDouble(value, flatViewOffset, flatViewLength, ascending, forwardScan);
        } else {
            while (low <= high) {
                int mid = low + (high - low) / 2;
                double midVal = getDouble(mid * stride);
                if (Math.abs(midVal - value) <= Numbers.DOUBLE_TOLERANCE) {
                    if (forwardScan) {
                        while (low < mid) {
                            int m = low + (mid - low) / 2;
                            if (Math.abs(getDouble(m * stride) - value) <= Numbers.DOUBLE_TOLERANCE) {
                                mid = m;
                            } else {
                                low = m + 1;
                            }
                        }
                        return low;
                    } else {
                        while (mid < high) {
                            int m = mid + (high - mid + 1) / 2;
                            if (Math.abs(getDouble(m * stride) - value) <= Numbers.DOUBLE_TOLERANCE) {
                                mid = m;
                            } else {
                                high = m - 1;
                            }
                        }
                        return mid;
                    }
                }

                if (ascending) {
                    if (midVal < value) {
                        low = mid + 1;
                    } else {
                        high = mid - 1;
                    }
                } else {
                    if (midVal > value) {
                        low = mid + 1;
                    } else {
                        high = mid - 1;
                    }
                }
            }
        }

        return -(low + 1);
    }

    /**
     * Convenience that downcasts {@link #flatView()} into {@link BorrowedFlatArrayView}.
     * If called on the wrong implementation, this call will fail with a cast exception.
     */
    public final BorrowedFlatArrayView borrowedFlatView() {
        return (BorrowedFlatArrayView) flatView;
    }

    @Override
    public void close() {
    }

    /**
     * Returns a view into the flat array that stores the elements of this array.
     */
    public final FlatArrayView flatView() {
        return flatView;
    }

    /**
     * Returns the number of elements in this array.
     *
     * @return the number of elements in this array
     */
    public int getCardinality() {
        if (isVanilla) {
            return flatViewLength;
        }
        int cardinality = 1;
        for (int i = 0; i < shape.size(); i++) {
            cardinality *= shape.getQuick(i);
        }
        return cardinality;
    }

    /**
     * Returns the number of dimensions in this array (i.e., its dimensionality).
     */
    public final int getDimCount() {
        return ColumnType.decodeWeakArrayDimensionality(type);
    }

    /**
     * Returns the number of elements in the given dimension (sub-arrays or leaf values)
     */
    public final int getDimLen(int dimension) {
        assert dimension >= 0 && dimension < shape.size();
        return shape.getQuick(dimension);
    }

    /**
     * Returns the {@code double} value at the supplied flat index in this array.
     * When accessing an array element at coordinates (i, j, k, ...), use this formula
     * to get its flat index:
     * <pre>
     *     flatIndex = i*stride0 + j*stride1 + k*stride2 + ...
     * </pre>
     * <strong>NOTE:</strong> the calculated index is <i>relative:</i> a flat index of
     * zero corresponds to the element at {@link #getFlatViewOffset flatViewOffset} in the
     * backing flat array. We discourage using {@link FlatArrayView#getDoubleAtAbsIndex}
     * directly, because it is easy to forget adding the offset, and it is non-obvious
     * from looking at the code that it's broken that way.
     */
    public final double getDouble(int flatIndex) {
        return flatView.getDoubleAtAbsIndex(flatViewOffset + flatIndex);
    }

    /**
     * Returns the type tag of this array's elements, as one of the {@link ColumnType}
     * constants.
     */
    public final short getElemType() {
        return ColumnType.decodeArrayElementType(type);
    }

    /**
     * Returns the number of elements in the backing flat view. For a {@linkplain
     * #isVanilla vanilla} array, it is equal to the total number of elements in
     * this array.
     * <p>
     * <strong>NOTE:</strong> This value is not the same as {@code flatView().length()}.
     * It tells you which part of the underlying flat view this array is using, as
     * opposed to its actual physical length.
     */
    public final int getFlatViewLength() {
        return flatViewLength;
    }

    /**
     * Returns the index of the underlying flat array at which the first element
     * of this array view is located.
     */
    public final int getFlatViewOffset() {
        return flatViewOffset;
    }

    public final int getHi() {
        return flatViewOffset + flatViewLength;
    }

    public final int getLo() {
        return flatViewOffset;
    }

    /**
     * Returns the {@code long} value at the supplied flat index in this array.
     * When accessing an array element at coordinates (i, j, k, ...), use this formula
     * to get its flat index:
     * <pre>
     *     flatIndex = i*stride0 + j*stride1 + k*stride2 + ...
     * </pre>
     * <strong>NOTE:</strong> the calculated index is <i>relative:</i> a flat index of
     * zero corresponds to the element at {@link #getFlatViewOffset} in the backing
     * flat array. We discourage using {@link FlatArrayView#getLongAtAbsIndex}
     * directly, because it is easy to forget adding the offset, and it is non-obvious
     * from looking at the code that it's broken that way. Using absolute indexing is
     * OK within a branch covered by an {@link #isVanilla()} check.
     */
    public final long getLong(int flatIndex) {
        return flatView.getLongAtAbsIndex(flatViewOffset + flatIndex);
    }

    /**
     * Returns the stride for the given dimension. You need this when calculating the
     * flat index of an array element from its coordinates.
     */
    public final int getStride(int dimension) {
        assert dimension >= 0 && dimension < strides.size();
        return strides.getQuick(dimension);
    }

    /**
     * Returns the encoded array type, as specified in {@link
     * ColumnType#encodeArrayType(short, int)}.
     */
    public final int getType() {
        return type;
    }

    /**
     * Returns the number of bytes this array would occupy when laid out in its
     * vanilla form. This includes the metadata (type and shape) as well as the data.
     */
    public final long getVanillaMemoryLayoutSize() {
        if (isNull()) {
            return 0;
        }
        long elemSize = ColumnType.sizeOf(ColumnType.decodeArrayElementType(type));
        long intBytes = Integer.BYTES; // using this to avoid (long) casts below
        return intBytes + // type
                getDimCount() * intBytes // shape
                + getCardinality() * elemSize; // data
    }

    /**
     * Returns the varchar element at the specified flat index.
     * <p>
     * <b>Important:</b> The returned {@link Utf8Sequence} is a flyweight object that may be
     * reused internally. Callers must NOT cache the returned reference as it may be invalidated
     * or point to different data on subsequent calls.
     *
     * @param flatIndex the flat (linear) index into the array
     * @return the varchar at the specified index, or {@code null} if the element is NULL
     */
    public final Utf8Sequence getVarchar(int flatIndex) {
        return flatView.getVarcharAt(flatViewOffset + flatIndex);
    }

    /**
     * Tells whether this is an empty array, which means its length along at least one
     * dimension is zero. Empty arrays of different shapes are not considered equal.
     */
    public final boolean isEmpty() {
        for (int i = 0; i < shape.size(); i++) {
            if (shape.getQuick(i) == 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * Tells whether this object represents an array-typed NULL value.
     */
    public final boolean isNull() {
        return ColumnType.isNull(type);
    }

    /**
     * Tells whether this array is a "vanilla array". A vanilla array's shape and
     * strides directly describe the physical layout of the underlying flat array. The
     * main reason to know this is when you're about to iterate over all the array
     * elements. For a vanilla array, you can use the underlying {@link #flatView()
     * flatView}, iterate over its {@link #getFlatViewLength() flatViewLength} indices
     * starting at {@link #getFlatViewOffset() flatOffset}, and you'll iterate over the
     * whole array in row-major order.
     * <p>
     * On a non-vanilla array, you must calculate each element's flat index from its
     * coordinates, applying the array's strides. A non-vanilla array arises when you
     * perform a shape change on a vanilla array, such as slicing, taking a sub-array,
     * transposing, flattening a dimension etc.
     * <p>
     * {@code ArrayView} implementations (such as {@link DirectArray} and {@link
     * FunctionArray}) directly reflect the underlying flat array and are thus always
     * vanilla. It is illegal to change their shape. The way to transform the array
     * shape is to first construct a {@link DerivedArrayView} from it, and then perform
     * a shape change.
     * <p>
     * The derived array view shares the original array's underlying flat array, but
     * after a shape change, some elements in the flat array are no longer a part of
     * the derived view. They remain accessible by their flat index, but if your code
     * does that, it's broken. (On a transposed array, all indices remain valid, but
     * iterating over the flat array no longer corresponds to row-major traversal.)
     */
    public boolean isVanilla() {
        return isVanilla;
    }

    /**
     * Tells whether this array has the same shape as the other one, or not.
     * <p>
     * <strong>NOTE:</strong> arrays of the same shape do not necessarily have the same
     * strides.
     */
    public final boolean shapeDiffers(ArrayView other) {
        int nDims = shape.size();
        IntList otherShape = other.shape;
        if (otherShape.size() != nDims) {
            return true;
        }
        for (int i = 0; i < nDims; i++) {
            if (shape.getQuick(i) != otherShape.getQuick(i)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the string representation of this array's shape, as a
     * list of numbers <code>[len0, len1, ...]</code>.
     */
    public final String shapeToString() {
        return shape.toString();
    }

    private void appendToMemRecursive(int dim, int flatIndex, MemoryA mem) {
        short elemType = getElemType();
        assert elemType == ColumnType.DOUBLE || elemType == ColumnType.LONG : "implemented only for long and double";

        final int count = getDimLen(dim);
        final int stride = getStride(dim);
        final boolean atDeepestDim = dim == getDimCount() - 1;
        if (atDeepestDim) {
            switch (elemType) {
                case ColumnType.LONG:
                    for (int i = 0; i < count; i++) {
                        mem.putLong(getLong(flatIndex));
                        flatIndex += stride;
                    }
                    break;
                case ColumnType.DOUBLE:
                    for (int i = 0; i < count; i++) {
                        mem.putDouble(getDouble(flatIndex));
                        flatIndex += stride;
                    }
                    break;
            }
        } else {
            for (int i = 0; i < count; i++) {
                appendToMemRecursive(dim + 1, flatIndex, mem);
                flatIndex += stride;
            }
        }
    }

    private boolean arrayEqualsRecursive(int dim, int flatIndexThis, ArrayView other, int flatIndexOther) {
        assert ColumnType.isDouble(getElemType()) : "implemented only for double";

        final int count = getDimLen(dim);
        final int strideThis = getStride(dim);
        final int strideOther = other.getStride(dim);
        final boolean atDeepestDim = dim == getDimCount() - 1;
        if (atDeepestDim) {
            for (int i = 0; i < count; i++) {
                if (getDouble(flatIndexThis) != other.getDouble(flatIndexOther)) {
                    return false;
                }
                flatIndexThis += strideThis;
                flatIndexOther += strideOther;
            }
        } else {
            for (int i = 0; i < count; i++) {
                if (!arrayEqualsRecursive(dim + 1, flatIndexThis, other, flatIndexOther)) {
                    return false;
                }
                flatIndexThis += strideThis;
                flatIndexOther += strideOther;
            }
        }
        return true;
    }
}
