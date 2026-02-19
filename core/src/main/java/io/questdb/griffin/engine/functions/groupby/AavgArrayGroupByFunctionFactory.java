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
import io.questdb.cairo.sql.ArrayFunction;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.ArrayConstant;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

public class AavgArrayGroupByFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "aavg(D[])";
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
        return new AavgArrayGroupByFunction(args.getQuick(0), configuration);
    }

    /**
     * Element-wise average aggregate for DOUBLE[] arrays.
     * <p>
     * Per-group state in MapValue uses 4 LONG slots:
     * <pre>
     * valueIndex:     LONG → ptr to compact block [int dataSize][int dim0][double sum[N]]
     * valueIndex + 1: LONG → N (accumulator length)
     * valueIndex + 2: LONG → count (>= 0 = uniform scalar count, -1 = variable mode)
     * valueIndex + 3: LONG → countPtr (variable mode: ptr to long count[N])
     * </pre>
     */
    private static class AavgArrayGroupByFunction extends ArrayFunction implements GroupByFunction, UnaryFunction {
        private static final int HEADER_SIZE = Integer.BYTES + Integer.BYTES;
        private static final long VARIABLE_MODE = -1;
        private final Function arg;
        private final DirectArray arrayOut;
        private GroupByAllocator allocator;
        private int valueIndex;

        public AavgArrayGroupByFunction(@NotNull Function arg, CairoConfiguration configuration) {
            this.arg = arg;
            this.type = arg.getType();
            this.arrayOut = new DirectArray(configuration);
            this.arrayOut.setType(type);
        }

        @Override
        public void close() {
            Misc.free(arg);
            Misc.free(arrayOut);
        }

        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
            ArrayView array = arg.getArray(record);
            if (array == null || array.isNull() || array.getFlatViewLength() == 0) {
                mapValue.putLong(valueIndex, 0);
                mapValue.putLong(valueIndex + 1, 0);
                mapValue.putLong(valueIndex + 2, 0);
                mapValue.putLong(valueIndex + 3, 0);
                return;
            }
            int n = array.getFlatViewLength();
            long blockSize = HEADER_SIZE + (long) n * Double.BYTES;
            long ptr = allocator.malloc(blockSize);
            Unsafe.getUnsafe().putInt(ptr, n * Double.BYTES);
            Unsafe.getUnsafe().putInt(ptr + Integer.BYTES, n);
            long dataPtr = ptr + HEADER_SIZE;
            boolean allFinite = true;
            for (int i = 0; i < n; i++) {
                double val = array.getDouble(i);
                Unsafe.getUnsafe().putDouble(dataPtr + (long) i * Double.BYTES, val);
                if (!Numbers.isFinite(val)) {
                    allFinite = false;
                }
            }
            mapValue.putLong(valueIndex, ptr);
            mapValue.putLong(valueIndex + 1, n);
            if (allFinite) {
                mapValue.putLong(valueIndex + 2, 1);
                mapValue.putLong(valueIndex + 3, 0);
            } else {
                long countPtr = allocator.malloc((long) n * Long.BYTES);
                for (int i = 0; i < n; i++) {
                    double val = Unsafe.getUnsafe().getDouble(dataPtr + (long) i * Double.BYTES);
                    Unsafe.getUnsafe().putLong(countPtr + (long) i * Long.BYTES, Numbers.isFinite(val) ? 1 : 0);
                }
                mapValue.putLong(valueIndex + 2, VARIABLE_MODE);
                mapValue.putLong(valueIndex + 3, countPtr);
            }
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            ArrayView array = arg.getArray(record);
            if (array == null || array.isNull() || array.getFlatViewLength() == 0) {
                return;
            }
            long ptr = mapValue.getLong(valueIndex);
            if (ptr == 0) {
                computeFirst(mapValue, record, rowId);
                return;
            }
            int accLen = (int) mapValue.getLong(valueIndex + 1);
            int inputLen = array.getFlatViewLength();
            long count = mapValue.getLong(valueIndex + 2);
            long countPtr = mapValue.getLong(valueIndex + 3);

            if (inputLen > accLen) {
                long oldBlockSize = HEADER_SIZE + (long) accLen * Double.BYTES;
                long newBlockSize = HEADER_SIZE + (long) inputLen * Double.BYTES;
                ptr = allocator.realloc(ptr, oldBlockSize, newBlockSize);
                Unsafe.getUnsafe().putInt(ptr, inputLen * Double.BYTES);
                Unsafe.getUnsafe().putInt(ptr + Integer.BYTES, inputLen);
                long dataPtr = ptr + HEADER_SIZE;
                for (int i = accLen; i < inputLen; i++) {
                    Unsafe.getUnsafe().putDouble(dataPtr + (long) i * Double.BYTES, Double.NaN);
                }
                mapValue.putLong(valueIndex, ptr);
                mapValue.putLong(valueIndex + 1, inputLen);
                // transition to variable mode if not already
                if (count != VARIABLE_MODE) {
                    countPtr = transitionToVariable(accLen, count, inputLen);
                    count = VARIABLE_MODE;
                    mapValue.putLong(valueIndex + 2, count);
                    mapValue.putLong(valueIndex + 3, countPtr);
                } else {
                    long oldCountSize = (long) accLen * Long.BYTES;
                    long newCountSize = (long) inputLen * Long.BYTES;
                    countPtr = allocator.realloc(countPtr, oldCountSize, newCountSize);
                    for (int i = accLen; i < inputLen; i++) {
                        Unsafe.getUnsafe().putLong(countPtr + (long) i * Long.BYTES, 0);
                    }
                    mapValue.putLong(valueIndex + 3, countPtr);
                }
                accLen = inputLen;
            }

            // check if we need to transition to variable mode
            boolean needsVariable = false;
            if (count != VARIABLE_MODE) {
                if (inputLen != accLen) {
                    needsVariable = true;
                } else {
                    for (int i = 0; i < inputLen; i++) {
                        if (!Numbers.isFinite(array.getDouble(i))) {
                            needsVariable = true;
                            break;
                        }
                    }
                }
                if (needsVariable) {
                    countPtr = transitionToVariable(accLen, count, accLen);
                    count = VARIABLE_MODE;
                    mapValue.putLong(valueIndex + 2, count);
                    mapValue.putLong(valueIndex + 3, countPtr);
                }
            }

            long dataPtr = ptr + HEADER_SIZE;
            for (int i = 0; i < inputLen; i++) {
                double inputVal = array.getDouble(i);
                if (Numbers.isFinite(inputVal)) {
                    long addr = dataPtr + (long) i * Double.BYTES;
                    double accVal = Unsafe.getUnsafe().getDouble(addr);
                    if (Numbers.isFinite(accVal)) {
                        Unsafe.getUnsafe().putDouble(addr, accVal + inputVal);
                    } else {
                        Unsafe.getUnsafe().putDouble(addr, inputVal);
                    }
                    if (count == VARIABLE_MODE) {
                        long countAddr = countPtr + (long) i * Long.BYTES;
                        Unsafe.getUnsafe().putLong(countAddr, Unsafe.getUnsafe().getLong(countAddr) + 1);
                    }
                }
            }
            if (count != VARIABLE_MODE) {
                mapValue.putLong(valueIndex + 2, count + 1);
            }
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public ArrayView getArray(Record rec) {
            long ptr = rec.getLong(valueIndex);
            if (ptr == 0) {
                return ArrayConstant.NULL;
            }
            int n = (int) rec.getLong(valueIndex + 1);
            long count = rec.getLong(valueIndex + 2);
            long countPtr = rec.getLong(valueIndex + 3);
            long dataPtr = ptr + HEADER_SIZE;

            arrayOut.setDimLen(0, n);
            arrayOut.applyShape();
            for (int i = 0; i < n; i++) {
                double sum = Unsafe.getUnsafe().getDouble(dataPtr + (long) i * Double.BYTES);
                if (Numbers.isFinite(sum)) {
                    long c;
                    if (count == VARIABLE_MODE) {
                        c = Unsafe.getUnsafe().getLong(countPtr + (long) i * Long.BYTES);
                    } else {
                        c = count;
                    }
                    arrayOut.putDouble(i, c > 0 ? sum / c : Double.NaN);
                } else {
                    arrayOut.putDouble(i, Double.NaN);
                }
            }
            return arrayOut;
        }

        @Override
        public String getName() {
            return "aavg";
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
            columnTypes.add(ColumnType.LONG); // length
            columnTypes.add(ColumnType.LONG); // count (uniform) or -1 (variable)
            columnTypes.add(ColumnType.LONG); // countPtr (variable mode)
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

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            long srcPtr = srcValue.getLong(valueIndex);
            if (srcPtr == 0) {
                return;
            }
            long destPtr = destValue.getLong(valueIndex);
            if (destPtr == 0) {
                destValue.putLong(valueIndex, srcPtr);
                destValue.putLong(valueIndex + 1, srcValue.getLong(valueIndex + 1));
                destValue.putLong(valueIndex + 2, srcValue.getLong(valueIndex + 2));
                destValue.putLong(valueIndex + 3, srcValue.getLong(valueIndex + 3));
                return;
            }

            int destLen = (int) destValue.getLong(valueIndex + 1);
            int mergeLen = (int) srcValue.getLong(valueIndex + 1);
            long destCount = destValue.getLong(valueIndex + 2);
            long destCountPtr = destValue.getLong(valueIndex + 3);
            long srcCount = srcValue.getLong(valueIndex + 2);
            long srcCountPtr = srcValue.getLong(valueIndex + 3);

            boolean needsVariable = destCount == VARIABLE_MODE || srcCount == VARIABLE_MODE || destLen != mergeLen;

            if (mergeLen > destLen) {
                long oldBlockSize = HEADER_SIZE + (long) destLen * Double.BYTES;
                long newBlockSize = HEADER_SIZE + (long) mergeLen * Double.BYTES;
                destPtr = allocator.realloc(destPtr, oldBlockSize, newBlockSize);
                Unsafe.getUnsafe().putInt(destPtr, mergeLen * Double.BYTES);
                Unsafe.getUnsafe().putInt(destPtr + Integer.BYTES, mergeLen);
                long dataPtr = destPtr + HEADER_SIZE;
                for (int i = destLen; i < mergeLen; i++) {
                    Unsafe.getUnsafe().putDouble(dataPtr + (long) i * Double.BYTES, Double.NaN);
                }
                destValue.putLong(valueIndex, destPtr);
                destValue.putLong(valueIndex + 1, mergeLen);
            }

            if (needsVariable) {
                if (destCount != VARIABLE_MODE) {
                    int newLen = Math.max(destLen, mergeLen);
                    destCountPtr = transitionToVariable(destLen, destCount, newLen);
                    destCount = VARIABLE_MODE;
                } else if (mergeLen > destLen) {
                    long oldSize = (long) destLen * Long.BYTES;
                    long newSize = (long) mergeLen * Long.BYTES;
                    destCountPtr = allocator.realloc(destCountPtr, oldSize, newSize);
                    for (int i = destLen; i < mergeLen; i++) {
                        Unsafe.getUnsafe().putLong(destCountPtr + (long) i * Long.BYTES, 0);
                    }
                }
            }

            long destDataPtr = destPtr + HEADER_SIZE;
            long srcDataPtr = srcPtr + HEADER_SIZE;
            for (int i = 0; i < mergeLen; i++) {
                double srcVal = Unsafe.getUnsafe().getDouble(srcDataPtr + (long) i * Double.BYTES);
                if (Numbers.isFinite(srcVal)) {
                    long addr = destDataPtr + (long) i * Double.BYTES;
                    double destVal = Unsafe.getUnsafe().getDouble(addr);
                    if (Numbers.isFinite(destVal)) {
                        Unsafe.getUnsafe().putDouble(addr, destVal + srcVal);
                    } else {
                        Unsafe.getUnsafe().putDouble(addr, srcVal);
                    }
                }
            }

            if (needsVariable) {
                if (srcCount == VARIABLE_MODE) {
                    for (int i = 0; i < mergeLen; i++) {
                        long srcC = Unsafe.getUnsafe().getLong(srcCountPtr + (long) i * Long.BYTES);
                        long destAddr = destCountPtr + (long) i * Long.BYTES;
                        Unsafe.getUnsafe().putLong(destAddr, Unsafe.getUnsafe().getLong(destAddr) + srcC);
                    }
                } else {
                    for (int i = 0; i < mergeLen; i++) {
                        double srcVal = Unsafe.getUnsafe().getDouble(srcDataPtr + (long) i * Double.BYTES);
                        if (Numbers.isFinite(srcVal)) {
                            long destAddr = destCountPtr + (long) i * Long.BYTES;
                            Unsafe.getUnsafe().putLong(destAddr, Unsafe.getUnsafe().getLong(destAddr) + srcCount);
                        }
                    }
                }
                destValue.putLong(valueIndex + 2, VARIABLE_MODE);
                destValue.putLong(valueIndex + 3, destCountPtr);
            } else {
                destValue.putLong(valueIndex + 2, destCount + srcCount);
            }
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
            mapValue.putLong(valueIndex + 3, 0);
        }

        @Override
        public boolean supportsParallelism() {
            return UnaryFunction.super.supportsParallelism();
        }

        private long transitionToVariable(int currentLen, long uniformCount, int newLen) {
            long countPtr = allocator.malloc((long) newLen * Long.BYTES);
            for (int i = 0; i < currentLen; i++) {
                Unsafe.getUnsafe().putLong(countPtr + (long) i * Long.BYTES, uniformCount);
            }
            for (int i = currentLen; i < newLen; i++) {
                Unsafe.getUnsafe().putLong(countPtr + (long) i * Long.BYTES, 0);
            }
            return countPtr;
        }
    }
}
