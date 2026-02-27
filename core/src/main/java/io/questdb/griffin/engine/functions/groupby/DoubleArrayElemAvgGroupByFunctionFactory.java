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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.constants.ArrayConstant;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

public class DoubleArrayElemAvgGroupByFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "array_elem_avg(D[])";
    }

    @Override
    public boolean isGroupBy() {
        return true;
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        return new DoubleArrayElemAvgGroupByFunction(args.getQuick(0), configuration);
    }

    /**
     * Element-wise average aggregate for DOUBLE[] arrays (N-dimensional).
     * <p>
     * Extends the base class (which handles shape management, overalloc, and remap)
     * and adds per-position count tracking needed to compute {@code sum / count} at
     * read time.
     *
     * <h3>Count tracking modes</h3>
     * To avoid allocating a per-element count array in the common case (all inputs
     * same-shape with no NaN values), counts operate in one of two modes:
     * <ul>
     *   <li><b>Uniform</b> ({@code count >= 0}): all positions share the same scalar
     *       count. This is the initial mode and stays active as long as every input
     *       array matches the accumulator shape and contains no NaN values.</li>
     *   <li><b>Variable</b> ({@code count == VARIABLE_MODE}): per-position counts in a
     *       separate {@code long[capacity]} array at {@code countPtr}. Transition happens
     *       on first NaN encounter or shape mismatch (which implies some positions are
     *       not covered by all inputs).</li>
     * </ul>
     *
     * <h3>Extra MapValue slots</h3>
     * <pre>
     * valueIndex + 3 (COUNT_SLOT):     LONG  count (&gt;= 0 = uniform, -1 = variable)
     * valueIndex + 4 (COUNT_PTR_SLOT): LONG  countPtr (variable mode: ptr to long[capacity])
     * valueIndex + 5 (COMP_SLOT):      LONG  compensationPtr (ptr to double[capacity])
     * </pre>
     */
    private static final class DoubleArrayElemAvgGroupByFunction extends AbstractDoubleArrayElemAggGroupByFunction {
        /** Offset from valueIndex for the Kahan compensation buffer pointer. */
        private static final int COMP_SLOT = 5;
        /** Offset from valueIndex for the uniform count / variable-mode sentinel. */
        private static final int COUNT_PTR_SLOT = 4;
        /** Offset from valueIndex for the per-element count array pointer. */
        private static final int COUNT_SLOT = 3;
        /** Sentinel stored in COUNT_SLOT to indicate per-element (variable) count mode. */
        private static final long VARIABLE_MODE = -1;
        /** Output array returned from {@link #getArray}; populated with sum/count per position. */
        private final DirectArray arrayOut;

        public DoubleArrayElemAvgGroupByFunction(@NotNull Function arg, CairoConfiguration configuration) {
            super(arg);
            this.arrayOut = new DirectArray(configuration);
            this.arrayOut.setType(type);
        }

        /**
         * Accumulates input values into the sum buffer with Kahan compensated summation
         * and updates per-position counts.
         * <p>
         * Before accumulating, checks whether a transition from uniform to variable
         * count mode is needed (shape mismatch or NaN in input). In variable mode,
         * only positions with finite input values get their count incremented.
         * In uniform mode, all positions are assumed to receive a value and the
         * scalar count is incremented once at the end.
         */
        @Override
        protected void accumulateInput(long dataPtr, ArrayView array, int[] currentAccShape, MapValue mapValue) {
            int inputFlatLen = array.getFlatViewLength();
            long count = mapValue.getLong(valueIndex + COUNT_SLOT);
            long countPtr = mapValue.getLong(valueIndex + COUNT_PTR_SLOT);
            long compPtr = mapValue.getLong(valueIndex + COMP_SLOT);

            // Transition to variable mode if input shape differs or has NaN
            if (count != VARIABLE_MODE) {
                boolean needsVariable = false;
                for (int d = 0; d < nDims; d++) {
                    if (array.getDimLen(d) != currentAccShape[d]) {
                        needsVariable = true;
                        break;
                    }
                }
                if (!needsVariable) {
                    for (int i = 0; i < inputFlatLen; i++) {
                        if (!Numbers.isFinite(array.getDouble(i))) {
                            needsVariable = true;
                            break;
                        }
                    }
                }
                if (needsVariable) {
                    long capacity = mapValue.getLong(valueIndex + 2);
                    countPtr = transitionToVariable((int) mapValue.getLong(valueIndex + 1), count, capacity);
                    count = VARIABLE_MODE;
                    mapValue.putLong(valueIndex + COUNT_SLOT, count);
                    mapValue.putLong(valueIndex + COUNT_PTR_SLOT, countPtr);
                }
            }

            if (array.isVanilla() && innerDimsMatch(inputShape, currentAccShape)) {
                // Flat path: input is vanilla row-major and inner dimensions match, so flat indices are identical.
                for (int i = 0; i < inputFlatLen; i++) {
                    double inputVal = array.getDouble(i);
                    if (Numbers.isFinite(inputVal)) {
                        long addr = dataPtr + (long) i * Double.BYTES;
                        double accVal = Unsafe.getUnsafe().getDouble(addr);
                        if (Numbers.isFinite(accVal)) {
                            long compAddr = compPtr + (long) i * Double.BYTES;
                            double c = Unsafe.getUnsafe().getDouble(compAddr);
                            double y = inputVal - c;
                            double t = accVal + y;
                            Unsafe.getUnsafe().putDouble(compAddr, (t - accVal) - y);
                            Unsafe.getUnsafe().putDouble(addr, t);
                        } else {
                            Unsafe.getUnsafe().putDouble(addr, inputVal);
                        }
                        if (count == VARIABLE_MODE) {
                            long countAddr = countPtr + (long) i * Long.BYTES;
                            Unsafe.getUnsafe().putLong(countAddr, Unsafe.getUnsafe().getLong(countAddr) + 1);
                        }
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
                        long addr = dataPtr + (long) accFi * Double.BYTES;
                        double accVal = Unsafe.getUnsafe().getDouble(addr);
                        if (Numbers.isFinite(accVal)) {
                            long compAddr = compPtr + (long) accFi * Double.BYTES;
                            double c = Unsafe.getUnsafe().getDouble(compAddr);
                            double y = inputVal - c;
                            double t = accVal + y;
                            Unsafe.getUnsafe().putDouble(compAddr, (t - accVal) - y);
                            Unsafe.getUnsafe().putDouble(addr, t);
                        } else {
                            Unsafe.getUnsafe().putDouble(addr, inputVal);
                        }
                        if (count == VARIABLE_MODE) {
                            long countAddr = countPtr + (long) accFi * Long.BYTES;
                            Unsafe.getUnsafe().putLong(countAddr, Unsafe.getUnsafe().getLong(countAddr) + 1);
                        }
                    }
                } while (ArrayView.incrementCoords(coords, inputShape));
            }
            if (count != VARIABLE_MODE) {
                mapValue.putLong(valueIndex + COUNT_SLOT, count + 1);
            }
        }

        @Override
        public void close() {
            super.close();
            Misc.free(arrayOut);
        }

        @Override
        protected double combine(double accVal, double inputVal) {
            return accVal + inputVal;
        }

        /**
         * Returns the average array for a group by dividing accumulated sums by counts.
         * Reads the shape from the compact block header, then for each position computes
         * {@code sum[i] / count[i]} (or {@code NaN} if count is zero or sum is NaN).
         */
        @Override
        public ArrayView getArray(Record rec) {
            long ptr = rec.getLong(valueIndex);
            if (ptr == 0) {
                return ArrayConstant.NULL;
            }
            int accFlatLen = (int) rec.getLong(valueIndex + 1);
            long count = rec.getLong(valueIndex + COUNT_SLOT);
            long countPtr = rec.getLong(valueIndex + COUNT_PTR_SLOT);
            long dataPtr = ptr + headerSize;

            readShapeFromHeader(ptr, accShape);
            for (int d = 0; d < nDims; d++) {
                arrayOut.setDimLen(d, accShape[d]);
            }
            arrayOut.applyShape();

            for (int i = 0; i < accFlatLen; i++) {
                double sum = Unsafe.getUnsafe().getDouble(dataPtr + (long) i * Double.BYTES);
                if (Numbers.isFinite(sum)) {
                    long c = count == VARIABLE_MODE
                            ? Unsafe.getUnsafe().getLong(countPtr + (long) i * Long.BYTES)
                            : count;
                    arrayOut.putDouble(i, c > 0 ? sum / c : Double.NaN);
                } else {
                    arrayOut.putDouble(i, Double.NaN);
                }
            }
            return arrayOut;
        }

        @Override
        public String getName() {
            return "array_elem_avg";
        }

        @Override
        protected void initExtraValueTypes(ArrayColumnTypes columnTypes) {
            columnTypes.add(ColumnType.LONG); // count (uniform scalar or VARIABLE_MODE)
            columnTypes.add(ColumnType.LONG); // countPtr (variable mode per-element counts)
            columnTypes.add(ColumnType.LONG); // compensationPtr (Kahan compensation buffer)
        }

        /**
         * Merges src's sums and counts into dest during parallel GROUP BY.
         * <p>
         * Four cases for count merging, based on whether each side is uniform or variable:
         * <ul>
         *   <li>Both uniform, same shape: add scalar counts.</li>
         *   <li>Dest variable + src variable: add per-element counts with coordinate mapping.</li>
         *   <li>Dest variable + src uniform: add srcCount to positions where src had finite values.</li>
         *   <li>Dest uniform + src variable (or shape mismatch): transition dest to variable first.</li>
         * </ul>
         */
        @Override
        protected void mergeValues(long destDataPtr, long srcDataPtr, int[] destShape, int[] srcShape,
                int srcFlatLen, MapValue destValue, MapValue srcValue) {
            long destCount = destValue.getLong(valueIndex + COUNT_SLOT);
            long destCountPtr = destValue.getLong(valueIndex + COUNT_PTR_SLOT);
            long srcCount = srcValue.getLong(valueIndex + COUNT_SLOT);
            long srcCountPtr = srcValue.getLong(valueIndex + COUNT_PTR_SLOT);
            long destCompPtr = destValue.getLong(valueIndex + COMP_SLOT);
            long srcCompPtr = srcValue.getLong(valueIndex + COMP_SLOT);

            boolean needsVariable = destCount == VARIABLE_MODE || srcCount == VARIABLE_MODE;
            if (!needsVariable) {
                for (int d = 0; d < nDims; d++) {
                    if (destShape[d] != srcShape[d]) {
                        needsVariable = true;
                        break;
                    }
                }
            }
            if (needsVariable && destCount != VARIABLE_MODE) {
                long destCapacity = destValue.getLong(valueIndex + 2);
                destCountPtr = transitionToVariable((int) destValue.getLong(valueIndex + 1), destCount, destCapacity);
                destCount = VARIABLE_MODE;
            }

            boolean flatPath = innerDimsMatch(srcShape, destShape);
            if (!flatPath) {
                ArrayView.computeRowMajorStrides(destShape, accStrides);
                ArrayView.computeRowMajorStrides(srcShape, newStrides);
            }

            // Merge sums (Kahan compensated)
            if (flatPath) {
                for (int i = 0; i < srcFlatLen; i++) {
                    double srcVal = Unsafe.getUnsafe().getDouble(srcDataPtr + (long) i * Double.BYTES);
                    if (Numbers.isFinite(srcVal)) {
                        long addr = destDataPtr + (long) i * Double.BYTES;
                        double destVal = Unsafe.getUnsafe().getDouble(addr);
                        if (Numbers.isFinite(destVal)) {
                            double srcComp = Unsafe.getUnsafe().getDouble(srcCompPtr + (long) i * Double.BYTES);
                            double y = srcVal - srcComp;
                            double t = destVal + y;
                            Unsafe.getUnsafe().putDouble(destCompPtr + (long) i * Double.BYTES, (t - destVal) - y);
                            Unsafe.getUnsafe().putDouble(addr, t);
                        } else {
                            Unsafe.getUnsafe().putDouble(addr, srcVal);
                            Unsafe.getUnsafe().putDouble(destCompPtr + (long) i * Double.BYTES,
                                    Unsafe.getUnsafe().getDouble(srcCompPtr + (long) i * Double.BYTES));
                        }
                    }
                }
            } else {
                for (int fi = 0; fi < srcFlatLen; fi++) {
                    double srcVal = Unsafe.getUnsafe().getDouble(srcDataPtr + (long) fi * Double.BYTES);
                    if (Numbers.isFinite(srcVal)) {
                        ArrayView.flatIndexToCoords(fi, newStrides, coords);
                        int destFi = ArrayView.coordsToFlatIndex(coords, accStrides);
                        long addr = destDataPtr + (long) destFi * Double.BYTES;
                        double destVal = Unsafe.getUnsafe().getDouble(addr);
                        if (Numbers.isFinite(destVal)) {
                            double srcComp = Unsafe.getUnsafe().getDouble(srcCompPtr + (long) fi * Double.BYTES);
                            double y = srcVal - srcComp;
                            double t = destVal + y;
                            Unsafe.getUnsafe().putDouble(destCompPtr + (long) destFi * Double.BYTES, (t - destVal) - y);
                            Unsafe.getUnsafe().putDouble(addr, t);
                        } else {
                            Unsafe.getUnsafe().putDouble(addr, srcVal);
                            Unsafe.getUnsafe().putDouble(destCompPtr + (long) destFi * Double.BYTES,
                                    Unsafe.getUnsafe().getDouble(srcCompPtr + (long) fi * Double.BYTES));
                        }
                    }
                }
            }

            // Merge counts
            if (needsVariable) {
                if (srcCount == VARIABLE_MODE) {
                    if (flatPath) {
                        for (int i = 0; i < srcFlatLen; i++) {
                            long srcC = Unsafe.getUnsafe().getLong(srcCountPtr + (long) i * Long.BYTES);
                            long destAddr = destCountPtr + (long) i * Long.BYTES;
                            Unsafe.getUnsafe().putLong(destAddr, Unsafe.getUnsafe().getLong(destAddr) + srcC);
                        }
                    } else {
                        for (int fi = 0; fi < srcFlatLen; fi++) {
                            ArrayView.flatIndexToCoords(fi, newStrides, coords);
                            int destFi = ArrayView.coordsToFlatIndex(coords, accStrides);
                            long srcC = Unsafe.getUnsafe().getLong(srcCountPtr + (long) fi * Long.BYTES);
                            long destAddr = destCountPtr + (long) destFi * Long.BYTES;
                            Unsafe.getUnsafe().putLong(destAddr, Unsafe.getUnsafe().getLong(destAddr) + srcC);
                        }
                    }
                } else {
                    // Src uniform: add srcCount to positions where src had finite values
                    if (flatPath) {
                        for (int i = 0; i < srcFlatLen; i++) {
                            double srcVal = Unsafe.getUnsafe().getDouble(srcDataPtr + (long) i * Double.BYTES);
                            if (Numbers.isFinite(srcVal)) {
                                long destAddr = destCountPtr + (long) i * Long.BYTES;
                                Unsafe.getUnsafe().putLong(destAddr, Unsafe.getUnsafe().getLong(destAddr) + srcCount);
                            }
                        }
                    } else {
                        for (int fi = 0; fi < srcFlatLen; fi++) {
                            double srcVal = Unsafe.getUnsafe().getDouble(srcDataPtr + (long) fi * Double.BYTES);
                            if (Numbers.isFinite(srcVal)) {
                                ArrayView.flatIndexToCoords(fi, newStrides, coords);
                                int destFi = ArrayView.coordsToFlatIndex(coords, accStrides);
                                long destAddr = destCountPtr + (long) destFi * Long.BYTES;
                                Unsafe.getUnsafe().putLong(destAddr, Unsafe.getUnsafe().getLong(destAddr) + srcCount);
                            }
                        }
                    }
                }
                destValue.putLong(valueIndex + COUNT_SLOT, VARIABLE_MODE);
                destValue.putLong(valueIndex + COUNT_PTR_SLOT, destCountPtr);
            } else {
                destValue.putLong(valueIndex + COUNT_SLOT, destCount + srcCount);
            }
        }

        /**
         * Initializes count and compensation state for a new group.
         * If all input values are finite, starts in uniform mode (count=1).
         * Otherwise, allocates a per-element count array and marks each position 0 or 1.
         * Always allocates a zero-filled compensation buffer for Kahan summation.
         */
        @Override
        protected void onComputeFirst(MapValue mapValue, ArrayView array, int flatLen, int capacity) {
            // Read from the accumulator (already linearized by computeFirst) rather than
            // the original array, so non-vanilla inputs are handled correctly.
            // NaN values in the sum buffer serve as "no data yet" sentinels: computeNext()
            // replaces them on first finite value, and getArray() skips division when !isFinite.
            // The count[i]=0 invariant is coupled to this — both must agree on "no data".
            long dataPtr = mapValue.getLong(valueIndex) + headerSize;
            boolean allFinite = true;
            for (int i = 0; i < flatLen; i++) {
                if (!Numbers.isFinite(Unsafe.getUnsafe().getDouble(dataPtr + (long) i * Double.BYTES))) {
                    allFinite = false;
                    break;
                }
            }
            if (allFinite) {
                mapValue.putLong(valueIndex + COUNT_SLOT, 1);
                mapValue.putLong(valueIndex + COUNT_PTR_SLOT, 0);
            } else {
                long countPtr = allocator.malloc((long) capacity * Long.BYTES);
                for (int i = 0; i < flatLen; i++) {
                    Unsafe.getUnsafe().putLong(countPtr + (long) i * Long.BYTES,
                            Numbers.isFinite(Unsafe.getUnsafe().getDouble(dataPtr + (long) i * Double.BYTES)) ? 1 : 0);
                }
                zeroFillLongs(countPtr, flatLen, capacity);
                mapValue.putLong(valueIndex + COUNT_SLOT, VARIABLE_MODE);
                mapValue.putLong(valueIndex + COUNT_PTR_SLOT, countPtr);
            }
            long compPtr = allocator.malloc((long) capacity * Double.BYTES);
            zeroFillDoubles(compPtr, 0, capacity);
            mapValue.putLong(valueIndex + COMP_SLOT, compPtr);
        }

        /** Copies count, countPtr, and compensationPtr slots from src to dest when dest was empty. */
        @Override
        protected void onMergeShallowCopy(MapValue destValue, MapValue srcValue) {
            destValue.putLong(valueIndex + COUNT_SLOT, srcValue.getLong(valueIndex + COUNT_SLOT));
            destValue.putLong(valueIndex + COUNT_PTR_SLOT, srcValue.getLong(valueIndex + COUNT_PTR_SLOT));
            destValue.putLong(valueIndex + COMP_SLOT, srcValue.getLong(valueIndex + COMP_SLOT));
        }

        /** Remaps count and compensation arrays when shape grows. Always transitions to variable count mode. */
        @Override
        protected void onShapeGrow(MapValue mapValue, int oldFlatLen, long newCapacity, boolean needsRemap) {
            long count = mapValue.getLong(valueIndex + COUNT_SLOT);
            long countPtr = mapValue.getLong(valueIndex + COUNT_PTR_SLOT);
            countPtr = growCounts(countPtr, count, count == VARIABLE_MODE, oldFlatLen, newCapacity, needsRemap);
            mapValue.putLong(valueIndex + COUNT_SLOT, VARIABLE_MODE);
            mapValue.putLong(valueIndex + COUNT_PTR_SLOT, countPtr);

            long oldCompPtr = mapValue.getLong(valueIndex + COMP_SLOT);
            long newCompPtr = allocator.malloc(newCapacity * Double.BYTES);
            zeroFillDoubles(newCompPtr, 0, (int) newCapacity);
            if (!needsRemap) {
                Unsafe.getUnsafe().copyMemory(oldCompPtr, newCompPtr, (long) oldFlatLen * Double.BYTES);
            } else {
                for (int fi = 0; fi < oldFlatLen; fi++) {
                    ArrayView.flatIndexToCoords(fi, accStrides, coords);
                    int newFi = ArrayView.coordsToFlatIndex(coords, newStrides);
                    Unsafe.getUnsafe().putDouble(
                            newCompPtr + (long) newFi * Double.BYTES,
                            Unsafe.getUnsafe().getDouble(oldCompPtr + (long) fi * Double.BYTES)
                    );
                }
            }
            mapValue.putLong(valueIndex + COMP_SLOT, newCompPtr);
        }

        @Override
        protected void setNullExtra(MapValue mapValue) {
            mapValue.putLong(valueIndex + COUNT_SLOT, 0);
            mapValue.putLong(valueIndex + COUNT_PTR_SLOT, 0);
            mapValue.putLong(valueIndex + COMP_SLOT, 0);
        }

        /**
         * Allocates a new count array of size {@code newCapacity} and remaps old counts into it.
         * <p>
         * When {@code needsRemap} is false, flat indices are preserved so counts can be
         * bulk-copied (or bulk-filled for uniform mode). When true, uses coordinate
         * conversion via {@link #accStrides}/{@link #newStrides} (already populated by
         * the base class).
         *
         * @param oldCountPtr  pointer to the old count array (ignored if uniform mode)
         * @param uniformCount scalar count value if in uniform mode
         * @param isVariable   true if currently in variable mode
         * @param oldFlatLen   number of elements in the old count array
         * @param newCapacity  allocated size for the new count array
         * @param needsRemap   true if flat layout changed (strides differ)
         * @return pointer to the new count array
         */
        private long growCounts(long oldCountPtr, long uniformCount, boolean isVariable,
                int oldFlatLen, long newCapacity, boolean needsRemap) {
            long newCountPtr = allocator.malloc(newCapacity * Long.BYTES);
            zeroFillLongs(newCountPtr, 0, (int) newCapacity);
            if (!needsRemap) {
                if (isVariable) {
                    Unsafe.getUnsafe().copyMemory(oldCountPtr, newCountPtr, (long) oldFlatLen * Long.BYTES);
                } else {
                    for (int i = 0; i < oldFlatLen; i++) {
                        Unsafe.getUnsafe().putLong(newCountPtr + (long) i * Long.BYTES, uniformCount);
                    }
                }
            } else {
                // Coordinate remap (accStrides/newStrides set by base class)
                for (int fi = 0; fi < oldFlatLen; fi++) {
                    ArrayView.flatIndexToCoords(fi, accStrides, coords);
                    int newFi = ArrayView.coordsToFlatIndex(coords, newStrides);
                    long val = isVariable
                            ? Unsafe.getUnsafe().getLong(oldCountPtr + (long) fi * Long.BYTES)
                            : uniformCount;
                    Unsafe.getUnsafe().putLong(newCountPtr + (long) newFi * Long.BYTES, val);
                }
            }
            return newCountPtr;
        }

        /**
         * Transitions from uniform to variable count mode.
         * Allocates a {@code long[capacity]} array and fills positions {@code [0, currentFlatLen)}
         * with the current uniform count, zero-filling the overallocated tail.
         *
         * @return pointer to the new per-element count array
         */
        private long transitionToVariable(int currentFlatLen, long uniformCount, long capacity) {
            long countPtr = allocator.malloc(capacity * Long.BYTES);
            for (int i = 0; i < currentFlatLen; i++) {
                Unsafe.getUnsafe().putLong(countPtr + (long) i * Long.BYTES, uniformCount);
            }
            zeroFillLongs(countPtr, currentFlatLen, (int) capacity);
            return countPtr;
        }

        /** Fills long values at positions {@code [from, to)} with zero. */
        private static void zeroFillLongs(long ptr, int from, int to) {
            for (int i = from; i < to; i++) {
                Unsafe.getUnsafe().putLong(ptr + (long) i * Long.BYTES, 0);
            }
        }
    }
}
