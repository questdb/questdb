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
import io.questdb.std.Vect;
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
     * read time. Always maintains a per-element {@code long[capacity]} count array.
     *
     * <h3>Extra MapValue slots</h3>
     * <pre>
     * valueIndex + 3 (COUNT_PTR_SLOT): LONG  countPtr (ptr to long[capacity])
     * valueIndex + 4 (COMP_SLOT):      LONG  compensationPtr (ptr to double[capacity])
     * </pre>
     */
    private static final class DoubleArrayElemAvgGroupByFunction extends AbstractDoubleArrayElemAggGroupByFunction {
        private static final int COMP_SLOT = 4;
        private static final int COUNT_PTR_SLOT = 3;
        private final DirectArray arrayOut;

        public DoubleArrayElemAvgGroupByFunction(@NotNull Function arg, @NotNull CairoConfiguration configuration) {
            super(arg, configuration);
            this.arrayOut = new DirectArray(configuration);
            this.arrayOut.setType(type);
        }

        @Override
        public void close() {
            super.close();
            Misc.free(arrayOut);
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
            int accFlatCardinality = (int) rec.getLong(valueIndex + 1);
            long countPtr = rec.getLong(valueIndex + COUNT_PTR_SLOT);
            long dataPtr = ptr + headerSize;

            readShapeFromHeader(ptr, accShape);
            for (int d = 0; d < nDims; d++) {
                arrayOut.setDimLen(d, accShape[d]);
            }
            arrayOut.applyShape();

            for (int i = 0; i < accFlatCardinality; i++) {
                double sum = Unsafe.getDouble(dataPtr + (long) i * Double.BYTES);
                if (Numbers.isFinite(sum)) {
                    long c = Unsafe.getLong(countPtr + (long) i * Long.BYTES);
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

        private static void zeroFillLongs(long ptr, long from, long to) {
            if (to > from) {
                Vect.setMemoryLong(ptr + from * Long.BYTES, 0, to - from);
            }
        }

        /**
         * Accumulates input values into the sum buffer with Kahan compensated summation
         * and increments per-position counts for finite values.
         */
        @Override
        protected void accumulateInput(long dataPtr, ArrayView array, int[] currentAccShape, MapValue mapValue) {
            int inputCardinality = array.getCardinality();
            long countPtr = mapValue.getLong(valueIndex + COUNT_PTR_SLOT);
            long compPtr = mapValue.getLong(valueIndex + COMP_SLOT);

            if (array.isVanilla() && innerDimsMatch(inputShape, currentAccShape)) {
                for (int i = 0; i < inputCardinality; i++) {
                    double inputVal = array.getDouble(i);
                    if (Numbers.isFinite(inputVal)) {
                        long addr = dataPtr + (long) i * Double.BYTES;
                        double accVal = Unsafe.getDouble(addr);
                        if (Numbers.isFinite(accVal)) {
                            long compAddr = compPtr + (long) i * Double.BYTES;
                            double c = Unsafe.getDouble(compAddr);
                            double y = inputVal - c;
                            double t = accVal + y;
                            Unsafe.putDouble(compAddr, (t - accVal) - y);
                            Unsafe.putDouble(addr, t);
                        } else {
                            Unsafe.putDouble(addr, inputVal);
                        }
                        long countAddr = countPtr + (long) i * Long.BYTES;
                        Unsafe.putLong(countAddr, Unsafe.getLong(countAddr) + 1);
                    }
                }
            } else {
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
                        double accVal = Unsafe.getDouble(addr);
                        if (Numbers.isFinite(accVal)) {
                            long compAddr = compPtr + (long) accFi * Double.BYTES;
                            double c = Unsafe.getDouble(compAddr);
                            double y = inputVal - c;
                            double t = accVal + y;
                            Unsafe.putDouble(compAddr, (t - accVal) - y);
                            Unsafe.putDouble(addr, t);
                        } else {
                            Unsafe.putDouble(addr, inputVal);
                        }
                        long countAddr = countPtr + (long) accFi * Long.BYTES;
                        Unsafe.putLong(countAddr, Unsafe.getLong(countAddr) + 1);
                    }
                } while (ArrayView.incrementCoords(coords, inputShape));
            }
        }

        @Override
        protected void accumulateOne(long dataPtr, int accFi, double inputVal) {
            throw new UnsupportedOperationException("avg overrides accumulateInput directly");
        }

        @Override
        protected void initExtraValueTypes(ArrayColumnTypes columnTypes) {
            columnTypes.add(ColumnType.LONG); // countPtr (per-element counts)
            columnTypes.add(ColumnType.LONG); // compensationPtr (Kahan compensation buffer)
        }

        @Override
        protected void mergeOne(long destDataPtr, int destFi, double srcVal, int srcFi) {
            throw new UnsupportedOperationException("avg overrides mergeValues directly");
        }

        /**
         * Merges src's sums and counts into dest during parallel GROUP BY.
         */
        @Override
        protected void mergeValues(long destDataPtr, long srcDataPtr, int[] destShape, int[] srcShape,
                                   int srcFlatCardinality, MapValue destValue, MapValue srcValue) {
            long destCountPtr = destValue.getLong(valueIndex + COUNT_PTR_SLOT);
            long srcCountPtr = srcValue.getLong(valueIndex + COUNT_PTR_SLOT);
            long destCompPtr = destValue.getLong(valueIndex + COMP_SLOT);
            long srcCompPtr = srcValue.getLong(valueIndex + COMP_SLOT);

            boolean flatPath = innerDimsMatch(srcShape, destShape);
            if (!flatPath) {
                ArrayView.computeRowMajorStrides(destShape, accStrides);
                ArrayView.computeRowMajorStrides(srcShape, newStrides);
            }

            // Merge sums (Kahan compensated) and counts
            if (flatPath) {
                for (int i = 0; i < srcFlatCardinality; i++) {
                    double srcVal = Unsafe.getDouble(srcDataPtr + (long) i * Double.BYTES);
                    if (Numbers.isFinite(srcVal)) {
                        long addr = destDataPtr + (long) i * Double.BYTES;
                        double destVal = Unsafe.getDouble(addr);
                        if (Numbers.isFinite(destVal)) {
                            long destCompAddr = destCompPtr + (long) i * Double.BYTES;
                            double destComp = Unsafe.getDouble(destCompAddr);
                            double srcComp = Unsafe.getDouble(srcCompPtr + (long) i * Double.BYTES);
                            double y = (srcVal - srcComp) - destComp;
                            double t = destVal + y;
                            Unsafe.putDouble(destCompAddr, (t - destVal) - y);
                            Unsafe.putDouble(addr, t);
                        } else {
                            Unsafe.putDouble(addr, srcVal);
                            Unsafe.putDouble(destCompPtr + (long) i * Double.BYTES,
                                    Unsafe.getDouble(srcCompPtr + (long) i * Double.BYTES));
                        }
                    }
                    long srcC = Unsafe.getLong(srcCountPtr + (long) i * Long.BYTES);
                    long destAddr = destCountPtr + (long) i * Long.BYTES;
                    Unsafe.putLong(destAddr, Unsafe.getLong(destAddr) + srcC);
                }
            } else {
                for (int fi = 0; fi < srcFlatCardinality; fi++) {
                    double srcVal = Unsafe.getDouble(srcDataPtr + (long) fi * Double.BYTES);
                    ArrayView.flatIndexToCoords(fi, newStrides, coords);
                    int destFi = ArrayView.coordsToFlatIndex(coords, accStrides);
                    if (Numbers.isFinite(srcVal)) {
                        long addr = destDataPtr + (long) destFi * Double.BYTES;
                        double destVal = Unsafe.getDouble(addr);
                        if (Numbers.isFinite(destVal)) {
                            long destCompAddr = destCompPtr + (long) destFi * Double.BYTES;
                            double destComp = Unsafe.getDouble(destCompAddr);
                            double srcComp = Unsafe.getDouble(srcCompPtr + (long) fi * Double.BYTES);
                            double y = (srcVal - srcComp) - destComp;
                            double t = destVal + y;
                            Unsafe.putDouble(destCompAddr, (t - destVal) - y);
                            Unsafe.putDouble(addr, t);
                        } else {
                            Unsafe.putDouble(addr, srcVal);
                            Unsafe.putDouble(destCompPtr + (long) destFi * Double.BYTES,
                                    Unsafe.getDouble(srcCompPtr + (long) fi * Double.BYTES));
                        }
                    }
                    long srcC = Unsafe.getLong(srcCountPtr + (long) fi * Long.BYTES);
                    long destAddr = destCountPtr + (long) destFi * Long.BYTES;
                    Unsafe.putLong(destAddr, Unsafe.getLong(destAddr) + srcC);
                }
            }
        }

        /**
         * Initializes per-element count and compensation state for a new group.
         * Allocates a per-element count array (1 for finite positions, 0 for NaN)
         * and a zero-filled compensation buffer for Kahan summation.
         */
        @Override
        protected void onComputeFirst(MapValue mapValue, ArrayView array, int flatCardinality, int capacity) {
            long dataPtr = mapValue.getLong(valueIndex) + headerSize;
            long countPtr = allocator.malloc((long) capacity * Long.BYTES);
            for (int i = 0; i < flatCardinality; i++) {
                Unsafe.putLong(countPtr + (long) i * Long.BYTES,
                        Numbers.isFinite(Unsafe.getDouble(dataPtr + (long) i * Double.BYTES)) ? 1 : 0);
            }
            zeroFillLongs(countPtr, flatCardinality, capacity);
            mapValue.putLong(valueIndex + COUNT_PTR_SLOT, countPtr);

            long compPtr = allocator.malloc((long) capacity * Double.BYTES);
            zeroFillDoubles(compPtr, 0, capacity);
            mapValue.putLong(valueIndex + COMP_SLOT, compPtr);
        }

        @Override
        protected void onMergeShallowCopy(MapValue destValue, MapValue srcValue) {
            destValue.putLong(valueIndex + COUNT_PTR_SLOT, srcValue.getLong(valueIndex + COUNT_PTR_SLOT));
            destValue.putLong(valueIndex + COMP_SLOT, srcValue.getLong(valueIndex + COMP_SLOT));
        }

        /**
         * Remaps count and compensation arrays when shape grows.
         */
        @Override
        protected void onShapeGrow(MapValue mapValue, int oldFlatCardinality, long newCapacity, boolean needsRemap) {
            long oldCountPtr = mapValue.getLong(valueIndex + COUNT_PTR_SLOT);
            long newCountPtr = allocator.malloc(newCapacity * Long.BYTES);
            zeroFillLongs(newCountPtr, 0, newCapacity);
            if (!needsRemap) {
                Unsafe.copyMemory(oldCountPtr, newCountPtr, (long) oldFlatCardinality * Long.BYTES);
            } else {
                for (int fi = 0; fi < oldFlatCardinality; fi++) {
                    ArrayView.flatIndexToCoords(fi, accStrides, coords);
                    int newFi = ArrayView.coordsToFlatIndex(coords, newStrides);
                    Unsafe.putLong(
                            newCountPtr + (long) newFi * Long.BYTES,
                            Unsafe.getLong(oldCountPtr + (long) fi * Long.BYTES)
                    );
                }
            }
            mapValue.putLong(valueIndex + COUNT_PTR_SLOT, newCountPtr);

            long oldCompPtr = mapValue.getLong(valueIndex + COMP_SLOT);
            long newCompPtr = allocator.malloc(newCapacity * Double.BYTES);
            zeroFillDoubles(newCompPtr, 0, newCapacity);
            if (!needsRemap) {
                Unsafe.copyMemory(oldCompPtr, newCompPtr, (long) oldFlatCardinality * Double.BYTES);
            } else {
                for (int fi = 0; fi < oldFlatCardinality; fi++) {
                    ArrayView.flatIndexToCoords(fi, accStrides, coords);
                    int newFi = ArrayView.coordsToFlatIndex(coords, newStrides);
                    Unsafe.putDouble(
                            newCompPtr + (long) newFi * Double.BYTES,
                            Unsafe.getDouble(oldCompPtr + (long) fi * Double.BYTES)
                    );
                }
            }
            mapValue.putLong(valueIndex + COMP_SLOT, newCompPtr);
        }

        @Override
        protected void setNullExtra(MapValue mapValue) {
            mapValue.putLong(valueIndex + COUNT_PTR_SLOT, 0);
            mapValue.putLong(valueIndex + COMP_SLOT, 0);
        }
    }
}
