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
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

public class DoubleArrayElemSumGroupByFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "array_elem_sum(D[])";
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
        return new DoubleArrayElemSumGroupByFunction(args.getQuick(0), configuration);
    }

    /**
     * Element-wise sum aggregate with Kahan compensated summation.
     * <p>
     * Maintains a per-position compensation buffer ({@code double[capacity]}) in a
     * separate off-heap allocation to reduce floating-point error accumulation over
     * many rows, matching QuestDB's scalar {@code ksum()} behaviour.
     *
     * <h3>Extra MapValue slot</h3>
     * <pre>
     * valueIndex + 3 (COMP_SLOT): LONG  compensationPtr (ptr to double[capacity])
     * </pre>
     */
    private static final class DoubleArrayElemSumGroupByFunction extends AbstractDoubleArrayElemAggGroupByFunction {
        private static final int COMP_SLOT = 3;
        private long compensationPtr;
        private long srcCompensationPtr;

        public DoubleArrayElemSumGroupByFunction(@NotNull Function arg, @NotNull CairoConfiguration configuration) {
            super(arg, configuration);
        }

        @Override
        public String getName() {
            return "array_elem_sum";
        }

        @Override
        protected void accumulateOne(long dataPtr, int accFi, double inputVal) {
            long addr = dataPtr + (long) accFi * Double.BYTES;
            double accVal = Unsafe.getDouble(addr);
            if (Numbers.isFinite(accVal)) {
                long compAddr = compensationPtr + (long) accFi * Double.BYTES;
                double c = Unsafe.getDouble(compAddr);
                double y = inputVal - c;
                double t = accVal + y;
                Unsafe.putDouble(compAddr, (t - accVal) - y);
                Unsafe.putDouble(addr, t);
            } else {
                Unsafe.putDouble(addr, inputVal);
            }
        }

        @Override
        protected void initExtraValueTypes(ArrayColumnTypes columnTypes) {
            columnTypes.add(ColumnType.LONG); // compensationPtr
        }

        @Override
        protected void mergeOne(long destDataPtr, int destFi, double srcVal, int srcFi) {
            long destAddr = destDataPtr + (long) destFi * Double.BYTES;
            double destVal = Unsafe.getDouble(destAddr);
            if (Numbers.isFinite(destVal)) {
                long destCompAddr = compensationPtr + (long) destFi * Double.BYTES;
                double destComp = Unsafe.getDouble(destCompAddr);
                double srcComp = Unsafe.getDouble(srcCompensationPtr + (long) srcFi * Double.BYTES);
                double y = (srcVal - srcComp) - destComp;
                double t = destVal + y;
                Unsafe.putDouble(destCompAddr, (t - destVal) - y);
                Unsafe.putDouble(destAddr, t);
            } else {
                Unsafe.putDouble(destAddr, srcVal);
                Unsafe.putDouble(
                        compensationPtr + (long) destFi * Double.BYTES,
                        Unsafe.getDouble(srcCompensationPtr + (long) srcFi * Double.BYTES)
                );
            }
        }

        @Override
        protected void onBeforeAccumulate(MapValue mapValue) {
            compensationPtr = mapValue.getLong(valueIndex + COMP_SLOT);
        }

        @Override
        protected void onBeforeMerge(MapValue destValue, MapValue srcValue) {
            compensationPtr = destValue.getLong(valueIndex + COMP_SLOT);
            srcCompensationPtr = srcValue.getLong(valueIndex + COMP_SLOT);
        }

        @Override
        protected void onComputeFirst(MapValue mapValue, ArrayView array, int flatCardinality, int capacity) {
            long compPtr = allocator.malloc((long) capacity * Double.BYTES);
            zeroFillDoubles(compPtr, 0, capacity);
            mapValue.putLong(valueIndex + COMP_SLOT, compPtr);
        }

        @Override
        protected void onMergeShallowCopy(MapValue destValue, MapValue srcValue) {
            destValue.putLong(valueIndex + COMP_SLOT, srcValue.getLong(valueIndex + COMP_SLOT));
        }

        @Override
        protected void onShapeGrow(MapValue mapValue, int oldFlatCardinality, long newCapacity, boolean needsRemap) {
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
            mapValue.putLong(valueIndex + COMP_SLOT, 0);
        }
    }
}
