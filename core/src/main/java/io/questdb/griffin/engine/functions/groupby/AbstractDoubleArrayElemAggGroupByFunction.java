/*+*****************************************************************************
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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.BorrowedArray;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.ArrayFunction;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.ArrayConstant;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.NotNull;

/**
 * Shared base for element-wise array aggregate GROUP BY functions
 * (array_elem_max, array_elem_min, array_elem_sum, array_elem_avg).
 * <p>
 * Supports N-dimensional arrays. Per-group state in MapValue uses 3 LONG slots:
 * <pre>
 * valueIndex + 0: LONG  ptr to compact block (0 = NULL group)
 * valueIndex + 1: LONG  accFlatCardinality — number of meaningful elements
 * valueIndex + 2: LONG  capacity — allocated element count (&gt;= accFlatCardinality)
 * </pre>
 *
 * <h2>Compact block layout</h2>
 * <pre>
 * [int dataSize][int shape[0]] ... [int shape[nDims-1]][double data[capacity]]
 *  └─ headerSize bytes ──────────────────────────────┘
 * </pre>
 * {@code dataSize = accFlatCardinality * Double.BYTES} — covers meaningful data only, not the
 * overallocated tail. This is what {@link ArrayTypeDriver#getCompactPlainValue} reads.
 *
 * <h2>Sentinel values</h2>
 * <ul>
 *   <li>{@code ptr == 0}: all inputs seen so far were NULL; group result is NULL.</li>
 *   <li>{@code Double.NaN} in a data slot: no finite value has been accumulated at that position yet.
 *       First finite value replaces NaN outright; subsequent values go through {@link #accumulateOne}.</li>
 * </ul>
 *
 * <h2>Overallocation</h2>
 * Allocation is expensive at every dimensionality, and nD shape changes additionally require
 * a coordinate remap of the entire accumulator. To reduce realloc frequency, data blocks are
 * overallocated by a factor of {@code pow(1.5, nDims)} (1D: 1.5x, 2D: 2.25x, 3D: 3.375x).
 * {@code capacity} tracks the allocated element count separately from {@code accFlatCardinality} so
 * 1D growth can often extend in-place without reallocation.
 *
 * <h2>Shape growth</h2>
 * When a new input has a larger dimension than the current accumulator, the shape grows to
 * the per-dimension max. For 1D, if the new flat cardinality fits within capacity, positions are
 * extended in-place (no remap needed — flat indices are unchanged). For nD, or when capacity
 * is exceeded, a new block is allocated and old values are remapped via coordinate conversion.
 *
 * <h2>Extensibility</h2>
 * Subclasses must implement {@link #accumulateOne} and {@link #mergeOne} for the element-wise
 * aggregation operation. The avg variant additionally overrides {@link #accumulateInput} and
 * {@link #mergeValues} for count tracking, and uses the hook methods ({@link #onComputeFirst},
 * {@link #onShapeGrow}, {@link #onMergeShallowCopy}) to maintain its per-element count arrays
 * in sync with shape changes.
 */
public abstract class AbstractDoubleArrayElemAggGroupByFunction extends ArrayFunction implements GroupByFunction, UnaryFunction {

    /**
     * Base for the overallocation factor {@code pow(OVERALLOC_BASE, nDims)}: 1D → 1.5x, 2D → 2.25x, 3D → 3.375x.
     */
    private static final double OVERALLOC_BASE = 1.5;
    /**
     * Beyond this flat cardinality, overallocation is skipped and exact capacity is used.
     */
    private static final int OVERALLOC_CAP = 1024;

    /**
     * Scratch: current accumulator shape, read from the compact block header.
     */
    protected final int[] accShape;
    /**
     * Scratch: row-major strides for the current accumulator shape.
     */
    protected final int[] accStrides;
    /**
     * The single array-column argument to this aggregate function.
     */
    protected final Function arg;
    /**
     * Scratch: coordinate vector for flat-index ↔ multi-dim conversions.
     */
    protected final int[] coords;
    /**
     * Byte size of the compact block header: {@code Integer.BYTES * (1 + nDims)}.
     */
    protected final int headerSize;
    /**
     * Scratch: shape of the current input array.
     */
    protected final int[] inputShape;
    /**
     * Number of array dimensions (1 for {@code DOUBLE[]}, 2 for {@code DOUBLE[][]}, etc.).
     */
    protected final int nDims;
    /**
     * Scratch: the grown shape = per-dimension max of accShape and inputShape.
     */
    protected final int[] newShape;
    /**
     * Scratch: row-major strides for the grown (or input) shape.
     */
    protected final int[] newStrides;
    /**
     * Reusable view returned from {@link #getArray} — points into the compact block.
     */
    private final BorrowedArray borrowedArray = new BorrowedArray();
    /**
     * Maximum total element count for the accumulator array, from {@link CairoConfiguration#maxArrayElementCount()}.
     */
    private final int maxArrayElementCount;
    /**
     * Precomputed overallocation factor: {@code pow(OVERALLOC_BASE, nDims)}.
     */
    private final double overallocFactor;
    /**
     * Group-by memory allocator, set before any compute calls.
     */
    protected GroupByAllocator allocator;
    /**
     * Index of this function's first slot in the MapValue.
     */
    protected int valueIndex;

    public AbstractDoubleArrayElemAggGroupByFunction(@NotNull Function arg, @NotNull CairoConfiguration configuration) {
        this.arg = arg;
        this.type = arg.getType();
        this.maxArrayElementCount = configuration.maxArrayElementCount();
        // Weak-dim array types (bind variables with unresolved dimensionality) cannot
        // reach GROUP BY aggregate args: column refs always carry strong dims, UNION
        // rejects array bind variables, and data binding resolves dims before execution.
        // decodeArrayDimensionality asserts dims > 0 as a safety net.
        this.nDims = ColumnType.decodeArrayDimensionality(type);
        this.headerSize = Integer.BYTES * (1 + nDims);
        this.accShape = new int[nDims];
        this.inputShape = new int[nDims];
        this.newShape = new int[nDims];
        this.accStrides = new int[nDims];
        this.newStrides = new int[nDims];
        this.coords = new int[nDims];
        this.overallocFactor = Math.pow(OVERALLOC_BASE, nDims);
    }

    @Override
    public void close() {
        Misc.free(arg);
    }

    /**
     * Initializes a new group with the first non-null input array.
     * Allocates the compact block with overallocation, copies input values,
     * NaN-fills the overallocated tail, and notifies subclasses via {@link #onComputeFirst}.
     */
    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        ArrayView array = arg.getArray(record);
        if (array == null || array.isNull() || array.getCardinality() == 0) {
            setNull(mapValue);
            return;
        }
        int flatCardinality = array.getCardinality();
        int capacity = flatCardinality <= OVERALLOC_CAP
                ? (int) Math.min(Integer.MAX_VALUE, Math.max(flatCardinality, (long) (flatCardinality * overallocFactor)))
                : flatCardinality;
        long blockSize = headerSize + (long) capacity * Double.BYTES;
        long ptr = allocator.malloc(blockSize);

        writeHeaderFromArray(ptr, array, flatCardinality);
        long dataPtr = ptr + headerSize;
        if (array.isVanilla()) {
            for (int i = 0; i < flatCardinality; i++) {
                Unsafe.putDouble(dataPtr + (long) i * Double.BYTES, array.getDouble(i));
            }
        } else {
            // Non-vanilla: iterate by coordinate, use array strides to read, write in row-major order.
            for (int d = 0; d < nDims; d++) {
                inputShape[d] = array.getDimLen(d);
                coords[d] = 0;
            }
            int outFi = 0;
            do {
                int inputFi = 0;
                for (int d = 0; d < nDims; d++) {
                    inputFi += coords[d] * array.getStride(d);
                }
                Unsafe.putDouble(dataPtr + (long) outFi * Double.BYTES, array.getDouble(inputFi));
                outFi++;
            } while (ArrayView.incrementCoords(coords, inputShape));
        }
        nanFill(dataPtr, flatCardinality, capacity);

        mapValue.putLong(valueIndex, ptr);
        mapValue.putLong(valueIndex + 1, flatCardinality);
        mapValue.putLong(valueIndex + 2, capacity);
        onComputeFirst(mapValue, array, flatCardinality, capacity);
    }

    /**
     * Aggregates a subsequent row into an existing group.
     * <p>
     * If the input shape exceeds the accumulator shape in any dimension, the accumulator is
     * grown (1D in-place if capacity allows, otherwise full realloc + remap). After any
     * shape adjustment, values are accumulated via {@link #accumulateInput}.
     */
    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        ArrayView array = arg.getArray(record);
        if (array == null || array.isNull() || array.getCardinality() == 0) {
            return;
        }
        long ptr = mapValue.getLong(valueIndex);
        if (ptr == 0) {
            computeFirst(mapValue, record, rowId);
            return;
        }

        readShapeFromHeader(ptr, accShape);
        for (int d = 0; d < nDims; d++) {
            inputShape[d] = array.getDimLen(d);
        }
        boolean shapeChanged = computeMaxShape();
        if (shapeChanged) {
            growAccumulator(mapValue, (int) mapValue.getLong(valueIndex + 1));
        }

        accumulateInput(mapValue.getLong(valueIndex) + headerSize, array, shapeChanged ? newShape : accShape, mapValue);
    }

    @Override
    public Function getArg() {
        return arg;
    }

    /**
     * Returns the accumulated result as an array view.
     * For max/min/sum the compact block already holds the final values;
     * avg overrides this to divide sum by count.
     */
    @Override
    public ArrayView getArray(Record rec) {
        long ptr = rec.getLong(valueIndex);
        if (ptr == 0) {
            return ArrayConstant.NULL;
        }
        ArrayTypeDriver.getCompactPlainValue(ptr, type, nDims, borrowedArray);
        return borrowedArray;
    }

    @Override
    public int getValueIndex() {
        return valueIndex;
    }

    @Override
    public void initValueIndex(int valueIndex) {
        this.valueIndex = valueIndex;
    }

    @Override
    public void initValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.LONG); // ptr
        columnTypes.add(ColumnType.LONG); // accFlatCardinality
        columnTypes.add(ColumnType.LONG); // capacity
        initExtraValueTypes(columnTypes);
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isScalar() {
        return false;
    }

    @Override
    public boolean isThreadSafe() {
        return false;
    }

    /**
     * Merges two partial group results from parallel execution.
     * <p>
     * If dest is empty, shallow-copies src's pointer and slots (no data copy).
     * Otherwise, grows dest's shape if needed (same logic as {@link #computeNext}),
     * then combines values via {@link #mergeValues}.
     */
    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        long srcPtr = srcValue.getLong(valueIndex);
        if (srcPtr == 0) {
            return;
        }
        long destPtr = destValue.getLong(valueIndex);
        if (destPtr == 0) {
            // Dest empty: shallow-copy src's block pointer and metadata.
            destValue.putLong(valueIndex, srcPtr);
            destValue.putLong(valueIndex + 1, srcValue.getLong(valueIndex + 1));
            destValue.putLong(valueIndex + 2, srcValue.getLong(valueIndex + 2));
            onMergeShallowCopy(destValue, srcValue);
            return;
        }

        int srcFlatCardinality = (int) srcValue.getLong(valueIndex + 1);
        readShapeFromHeader(destPtr, accShape);
        readShapeFromHeader(srcPtr, inputShape);
        boolean shapeChanged = computeMaxShape();
        if (shapeChanged) {
            growAccumulator(destValue, (int) destValue.getLong(valueIndex + 1));
        }

        long destDataPtr = destValue.getLong(valueIndex) + headerSize;
        mergeValues(destDataPtr, srcPtr + headerSize, shapeChanged ? newShape : accShape, inputShape, srcFlatCardinality, destValue, srcValue);
    }

    @Override
    public void setAllocator(GroupByAllocator allocator) {
        this.allocator = allocator;
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putLong(valueIndex, 0);
        mapValue.putLong(valueIndex + 1, 0);
        mapValue.putLong(valueIndex + 2, 0);
        setNullExtra(mapValue);
    }

    @Override
    public boolean supportsParallelism() {
        return UnaryFunction.super.supportsParallelism();
    }

    /**
     * Checks whether growing from {@link #accShape} to {@link #newShape} requires a
     * coordinate remap of existing data.
     * <p>
     * Remap is unnecessary when all row-major strides agree on dimensions with more
     * than one element — i.e., only dimensions with {@code accShape[d] <= 1} or the
     * outermost varying dimension grew. Examples: (1,10)→(1,20), (10,1)→(20,1),
     * (10,5)→(20,5) all preserve flat indices.
     * <p>
     * When this returns true, {@link #accStrides} is populated as a side effect.
     */
    private boolean checkNeedsRemap() {
        if (nDims <= 1) {
            return false;
        }
        ArrayView.computeRowMajorStrides(accShape, accStrides);
        for (int d = 0; d < nDims; d++) {
            if (accShape[d] > 1 && accStrides[d] != newStrides[d]) {
                return true;
            }
        }
        return false;
    }

    /**
     * Computes {@link #newShape} as the per-dimension max of {@link #accShape} and
     * {@link #inputShape}. Both must be populated before calling.
     *
     * @return true if any dimension of newShape exceeds accShape
     */
    private boolean computeMaxShape() {
        boolean changed = false;
        for (int d = 0; d < nDims; d++) {
            newShape[d] = Math.max(accShape[d], inputShape[d]);
            if (newShape[d] > ArrayView.DIM_MAX_LEN) {
                throw CairoException.nonCritical()
                        .put("array dimension length out of range [dim=").put(d)
                        .put(", dimLen=").put(newShape[d])
                        .put(", maxLen=").put(ArrayView.DIM_MAX_LEN)
                        .put(']');
            }
            if (newShape[d] != accShape[d]) {
                changed = true;
            }
        }
        return changed;
    }

    /**
     * Grows the accumulator's compact block to fit {@link #newShape}.
     * <p>
     * Three cases based on {@link #checkNeedsRemap()} and capacity:
     * <ol>
     *   <li>No remap + fits in capacity → in-place NaN-fill tail</li>
     *   <li>No remap + capacity exceeded → new block, bulk copy, NaN-fill tail</li>
     *   <li>Remap needed → new block, NaN-fill all, coordinate remap</li>
     * </ol>
     * Updates the 3 base MapValue slots (ptr, accFlatCardinality, capacity) and calls
     * {@link #onShapeGrow} so subclasses can grow their auxiliary arrays.
     */
    private void growAccumulator(MapValue mapValue, int accFlatCardinality) {
        int newFlatCardinality = ArrayView.computeRowMajorStrides(newShape, newStrides);
        if (newFlatCardinality > maxArrayElementCount) {
            throw CairoException.nonCritical()
                    .put("array element count exceeds max [max=").put(maxArrayElementCount)
                    .put(", cardinality=").put(newFlatCardinality)
                    .put(']');
        }
        long ptr = mapValue.getLong(valueIndex);
        long capacity = mapValue.getLong(valueIndex + 2);
        boolean needsRemap = checkNeedsRemap();

        if (!needsRemap && newFlatCardinality <= capacity) {
            // In-place extension: flat indices unchanged, NaN-fill the tail.
            // Aux buffers (counts, compensation) already have correct zero-fill
            // in [oldCardinality, capacity) from their initial allocation, so
            // no onShapeGrow call is needed.
            long dataPtr = ptr + headerSize;
            nanFill(dataPtr, accFlatCardinality, newFlatCardinality);
            writeHeader(ptr, newShape, newFlatCardinality);
            mapValue.putLong(valueIndex + 1, newFlatCardinality);
        } else {
            long newCapacity = newFlatCardinality <= OVERALLOC_CAP
                    ? Math.max(newFlatCardinality, (long) (newFlatCardinality * overallocFactor))
                    : newFlatCardinality;
            long newBlockSize = headerSize + newCapacity * Double.BYTES;
            long newPtr = allocator.malloc(newBlockSize);
            writeHeader(newPtr, newShape, newFlatCardinality);
            long newDataPtr = newPtr + headerSize;

            if (needsRemap) {
                nanFill(newDataPtr, 0, newCapacity);
                remapData(ptr + headerSize, accFlatCardinality, accStrides, newDataPtr, newStrides);
            } else {
                // Flat layout preserved: bulk copy + NaN-fill tail.
                Unsafe.copyMemory(ptr + headerSize, newDataPtr, (long) accFlatCardinality * Double.BYTES);
                nanFill(newDataPtr, accFlatCardinality, newCapacity);
            }

            mapValue.putLong(valueIndex, newPtr);
            mapValue.putLong(valueIndex + 1, newFlatCardinality);
            mapValue.putLong(valueIndex + 2, newCapacity);
            onShapeGrow(mapValue, accFlatCardinality, newCapacity, needsRemap);
        }
    }

    /**
     * Copies each element from the old data layout to the new data layout,
     * translating flat indices through coordinates to account for changed strides.
     */
    private void remapData(long oldDataPtr, int oldFlatCardinality, int[] oldStrides, long newDataPtr, int[] targetStrides) {
        for (int fi = 0; fi < oldFlatCardinality; fi++) {
            ArrayView.flatIndexToCoords(fi, oldStrides, coords);
            int newFi = ArrayView.coordsToFlatIndex(coords, targetStrides);
            Unsafe.putDouble(
                    newDataPtr + (long) newFi * Double.BYTES,
                    Unsafe.getDouble(oldDataPtr + (long) fi * Double.BYTES)
            );
        }
    }

    /**
     * Writes the compact block header: dataSize (in bytes) followed by nDims shape integers.
     * Callers must ensure flatCardinality does not exceed maxArrayElementCount.
     */
    private void writeHeader(long ptr, int[] shape, int flatCardinality) {
        Unsafe.putInt(ptr, flatCardinality * Double.BYTES);
        long shapePtr = ptr + Integer.BYTES;
        for (int d = 0; d < nDims; d++) {
            Unsafe.putInt(shapePtr + (long) d * Integer.BYTES, shape[d]);
        }
    }

    /**
     * Writes the compact block header, reading shape dimensions from an ArrayView.
     * Callers must ensure flatCardinality does not exceed maxArrayElementCount.
     */
    private void writeHeaderFromArray(long ptr, ArrayView array, int flatCardinality) {
        Unsafe.putInt(ptr, flatCardinality * Double.BYTES);
        long shapePtr = ptr + Integer.BYTES;
        for (int d = 0; d < nDims; d++) {
            Unsafe.putInt(shapePtr + (long) d * Integer.BYTES, array.getDimLen(d));
        }
    }

    /**
     * Fills data positions {@code [from, to)} with {@code Double.NaN}.
     */
    static void nanFill(long dataPtr, long from, long to) {
        if (to > from) {
            Vect.setMemoryDouble(dataPtr + from * Double.BYTES, Double.NaN, to - from);
        }
    }

    /**
     * Fills double positions {@code [from, to)} with {@code 0.0}.
     */
    static void zeroFillDoubles(long ptr, long from, long to) {
        if (to > from) {
            Vect.setMemoryDouble(ptr + from * Double.BYTES, 0.0, to - from);
        }
    }

    /**
     * Accumulates values from a single input array into the group's data buffer.
     * <p>
     * For 1D, flat indices match between input and accumulator so values are combined directly.
     * For nD, each input flat index is converted to coordinates (via input strides), then mapped
     * to the accumulator's flat index (via accumulator strides), because the two may have
     * different shapes and therefore different flat layouts.
     * <p>
     * NaN input values are skipped. If the accumulator position holds NaN (no prior finite value),
     * the input value replaces it outright; otherwise {@link #accumulateOne} is called.
     * <p>
     * The avg subclass overrides this to additionally track per-position counts.
     *
     * @param dataPtr         pointer to the accumulator's data section (past the header)
     * @param array           the input array for this row
     * @param currentAccShape the accumulator's current shape (after any growth)
     * @param mapValue        the group's MapValue (for subclass access to extra slots)
     */
    protected void accumulateInput(long dataPtr, ArrayView array, int[] currentAccShape, MapValue mapValue) {
        int inputCardinality = array.getCardinality();
        onBeforeAccumulate(mapValue);
        if (array.isVanilla() && innerDimsMatch(inputShape, currentAccShape)) {
            // Flat path: input is vanilla row-major and inner dimensions match, so flat indices are identical.
            for (int i = 0; i < inputCardinality; i++) {
                double inputVal = array.getDouble(i);
                if (Numbers.isFinite(inputVal)) {
                    accumulateOne(dataPtr, i, inputVal);
                }
            }
        } else {
            // Coordinate path: iterate input coordinates, use array strides for non-vanilla support.
            ArrayView.computeRowMajorStrides(currentAccShape, accStrides);
            for (int d = 0; d < nDims; d++) {
                coords[d] = 0;
            }
            do {
                int inputFi = 0;
                for (int d = 0; d < nDims; d++) {
                    inputFi += coords[d] * array.getStride(d);
                }
                double inputVal = array.getDouble(inputFi);
                if (Numbers.isFinite(inputVal)) {
                    int accFi = ArrayView.coordsToFlatIndex(coords, accStrides);
                    accumulateOne(dataPtr, accFi, inputVal);
                }
            } while (ArrayView.incrementCoords(coords, inputShape));
        }
    }

    /**
     * Accumulates a single finite input value at the given accumulator position.
     *
     * @param dataPtr  pointer to the accumulator's data section
     * @param accFi    flat index in the accumulator
     * @param inputVal the finite input value to accumulate
     */
    protected abstract void accumulateOne(long dataPtr, int accFi, double inputVal);

    /**
     * Allows subclasses to register additional MapValue slots (e.g. count, countPtr for avg).
     *
     * @param columnTypes the column types to append additional slots to
     */
    protected void initExtraValueTypes(ArrayColumnTypes columnTypes) {
    }

    /**
     * Returns true when inner dimensions (d &ge; 1) match between two shapes.
     * When they match, row-major strides agree and flat indices are identical
     * between the two layouts — only dimension 0 (the outermost) may differ,
     * meaning one array may simply be shorter. This allows a flat linear scan
     * without coordinate conversion.
     * <p>
     * For {@code nDims <= 1} the loop body never executes, returning true — which
     * is correct since 1D always uses the flat path.
     *
     * @param shape1 first shape array
     * @param shape2 second shape array
     * @return true if inner dimensions match
     */
    protected boolean innerDimsMatch(int[] shape1, int[] shape2) {
        for (int d = 1; d < nDims; d++) {
            if (shape1[d] != shape2[d]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Merges a single finite src value into the dest at the given flat indices.
     *
     * @param destDataPtr pointer to dest's data section
     * @param destFi      flat index in dest's layout
     * @param srcVal      finite value from src (already checked by caller)
     * @param srcFi       flat index in src's layout (for subclass compensation lookup)
     */
    protected abstract void mergeOne(long destDataPtr, int destFi, double srcVal, int srcFi);

    /**
     * Merges src's accumulated values into dest during parallel GROUP BY.
     * Same coordinate-mapping logic as {@link #accumulateInput} but reads from
     * off-heap src data instead of an {@link ArrayView}.
     * <p>
     * The avg subclass overrides this to additionally merge per-position counts.
     *
     * @param destDataPtr        pointer to dest's data section
     * @param srcDataPtr         pointer to src's data section
     * @param destShape          dest's current shape (after any growth)
     * @param srcShape           src's shape
     * @param srcFlatCardinality number of elements in src's data
     * @param destValue          dest's MapValue (for subclass access to extra slots)
     * @param srcValue           src's MapValue (for subclass access to extra slots)
     */
    protected void mergeValues(long destDataPtr, long srcDataPtr, int[] destShape, int[] srcShape, int srcFlatCardinality, MapValue destValue, MapValue srcValue) {
        onBeforeMerge(destValue, srcValue);
        if (innerDimsMatch(srcShape, destShape)) {
            // Flat path: inner dimensions match, so flat indices are identical.
            for (int i = 0; i < srcFlatCardinality; i++) {
                double srcVal = Unsafe.getDouble(srcDataPtr + (long) i * Double.BYTES);
                if (Numbers.isFinite(srcVal)) {
                    mergeOne(destDataPtr, i, srcVal, i);
                }
            }
        } else {
            ArrayView.computeRowMajorStrides(destShape, accStrides);
            ArrayView.computeRowMajorStrides(srcShape, newStrides);
            for (int fi = 0; fi < srcFlatCardinality; fi++) {
                double srcVal = Unsafe.getDouble(srcDataPtr + (long) fi * Double.BYTES);
                if (Numbers.isFinite(srcVal)) {
                    ArrayView.flatIndexToCoords(fi, newStrides, coords);
                    int destFi = ArrayView.coordsToFlatIndex(coords, accStrides);
                    mergeOne(destDataPtr, destFi, srcVal, fi);
                }
            }
        }
    }

    /**
     * Hook called before the accumulation loop in {@link #accumulateInput}.
     * Subclasses can cache MapValue slot values into instance fields for use in
     * {@link #accumulateOne}.
     *
     * @param mapValue the map value for the current group
     */
    protected void onBeforeAccumulate(MapValue mapValue) {
    }

    /**
     * Hook called before the merge loop in {@link #mergeValues}.
     * Subclasses can cache MapValue slot values into instance fields for use in
     * {@link #mergeOne}.
     *
     * @param destValue the destination map value to merge into
     * @param srcValue  the source map value to merge from
     */
    protected void onBeforeMerge(MapValue destValue, MapValue srcValue) {
    }

    /**
     * Called after the first non-null array is stored in a new group.
     * Base slots (ptr, accFlatCardinality, capacity) have already been written.
     * Subclasses can use this to initialize auxiliary state (e.g. per-element counts).
     *
     * @param mapValue        the group's MapValue
     * @param array           the first non-null input array
     * @param flatCardinality flat element count of the array
     * @param capacity        allocated element count
     */
    protected void onComputeFirst(MapValue mapValue, ArrayView array, int flatCardinality, int capacity) {
    }

    /**
     * Called when merge shallow-copies src into an empty dest.
     * Subclasses should copy their extra MapValue slots from src to dest.
     *
     * @param destValue the destination map value
     * @param srcValue  the source map value
     */
    protected void onMergeShallowCopy(MapValue destValue, MapValue srcValue) {
    }

    /**
     * Called when the accumulator shape grows and a new data block is allocated
     * (from either computeNext or merge). Not called for in-place extensions
     * where the growth fits within the overallocated capacity, since auxiliary
     * buffers already have correct zero-fill in the tail.
     * <p>
     * When {@code needsRemap} is true, {@link #accStrides} and {@link #newStrides}
     * are populated and coordinate conversion is required. When false, old flat
     * indices are preserved and data can be bulk-copied.
     *
     * @param mapValue           the group's MapValue (base slots already updated)
     * @param oldFlatCardinality number of elements before growth
     * @param newCapacity        allocated element count after growth
     * @param needsRemap         true if old and new flat layouts differ (strides changed)
     */
    protected void onShapeGrow(MapValue mapValue, int oldFlatCardinality, long newCapacity, boolean needsRemap) {
    }

    /**
     * Reads the nDims shape integers from a compact block header into the given array.
     *
     * @param ptr   pointer to the start of the compact block header
     * @param shape array to receive the shape dimensions
     */
    protected void readShapeFromHeader(long ptr, int[] shape) {
        long shapePtr = ptr + Integer.BYTES;
        for (int d = 0; d < nDims; d++) {
            shape[d] = Unsafe.getInt(shapePtr + (long) d * Integer.BYTES);
        }
    }

    /**
     * Allows subclasses to zero their extra MapValue slots when a group is set to null.
     *
     * @param mapValue the group's MapValue to reset
     */
    protected void setNullExtra(MapValue mapValue) {
    }
}
