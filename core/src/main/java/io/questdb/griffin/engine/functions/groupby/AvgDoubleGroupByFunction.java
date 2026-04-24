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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.groupby.FlyweightPackedMapValue;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.NotNull;

public class AvgDoubleGroupByFunction extends DoubleFunction implements GroupByFunction, UnaryFunction {
    private final Function arg;
    private final boolean isArgNotNull;
    private final int argColumnIndex;
    private int valueIndex;

    public AvgDoubleGroupByFunction(@NotNull Function arg) {
        this.arg = arg;
        this.isArgNotNull = arg != null && arg.isNotNull();
        this.argColumnIndex = GroupByUtils.directArgColumnIndex(arg, ColumnType.DOUBLE);
    }

    @Override
    public void computeBatch(MapValue mapValue, long dataAddr, int rowCount, long startRowId) {
        if (rowCount > 0) {
            final long countPtr = mapValue.getAddress(valueIndex + 1);
            final long prevCount = mapValue.getLong(valueIndex + 1);
            final double batchSum = Vect.sumDoubleAcc(dataAddr, rowCount, countPtr);
            // sumDoubleAcc overwrites *countPtr with the batch count
            final long batchCount = mapValue.getLong(valueIndex + 1);
            if (batchCount > 0) {
                final double prevSum = mapValue.getDouble(valueIndex);
                if (prevCount > 0) {
                    mapValue.putDouble(valueIndex, prevSum + batchSum);
                } else {
                    mapValue.putDouble(valueIndex, batchSum);
                }
                mapValue.putLong(valueIndex + 1, prevCount + batchCount);
            } else {
                mapValue.putLong(valueIndex + 1, prevCount);
            }
        }
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        final double d = arg.getDouble(record);
        if (isArgNotNull || !Double.isNaN(d)) {
            mapValue.putDouble(valueIndex, d);
            mapValue.putLong(valueIndex + 1, 1L);
        } else {
            mapValue.putDouble(valueIndex, 0);
            mapValue.putLong(valueIndex + 1, 0);
        }
    }

    @Override
    public void computeKeyedBatch(
            PageFrameMemoryRecord record,
            FlyweightPackedMapValue mapValue,
            long baseValueAddr,
            long batchAddr,
            long rowCount,
            long baseRowId
    ) {
        // Two-slot layout: [sum:double][count:long]. setEmpty seeds (NaN, 0), so the
        // etalon copied into new entries is (NaN, 0). A new entry with a finite value
        // must take the computeFirst path (value, 1) rather than (NaN+value, 1); once
        // past the first row the sum is either real or 0 and we can add unconditionally.
        final long sumOffset = mapValue.getOffset(valueIndex);
        final long countOffset = mapValue.getOffset(valueIndex + 1);
        // Fast path: arg is a direct double column with data on the current frame.
        // Zero page address means a column top; fall through to the record-based path.
        final long argAddr = argColumnIndex >= 0 ? record.getPageAddress(argColumnIndex) : 0;
        if (argAddr != 0) {
            for (long i = 0; i < rowCount; i++) {
                final long encoded = Unsafe.getUnsafe().getLong(batchAddr + (i << 3));
                final long rowIndex = Map.decodeBatchRowIndex(encoded);
                final double value = Unsafe.getUnsafe().getDouble(argAddr + (rowIndex << 3));
                final long valueBase = baseValueAddr + Map.decodeBatchOffset(encoded);
                applyAvg(valueBase + sumOffset, valueBase + countOffset, value, Map.isNewBatchEntry(encoded), isArgNotNull);
            }
        } else {
            for (long i = 0; i < rowCount; i++) {
                final long encoded = Unsafe.getUnsafe().getLong(batchAddr + (i << 3));
                record.setRowIndex(Map.decodeBatchRowIndex(encoded));
                final double value = arg.getDouble(record);
                final long valueBase = baseValueAddr + Map.decodeBatchOffset(encoded);
                applyAvg(valueBase + sumOffset, valueBase + countOffset, value, Map.isNewBatchEntry(encoded), isArgNotNull);
            }
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final double d = arg.getDouble(record);
        if (isArgNotNull || !Double.isNaN(d)) {
            mapValue.addDouble(valueIndex, d);
            mapValue.addLong(valueIndex + 1, 1L);
        }
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public double getDouble(Record rec) {
        return rec.getDouble(valueIndex) / rec.getLong(valueIndex + 1);
    }

    @Override
    public String getName() {
        return "avg";
    }

    @Override
    public int getSampleByFlags() {
        return GroupByFunction.SAMPLE_BY_FILL_ALL;
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
        columnTypes.add(ColumnType.DOUBLE);
        columnTypes.add(ColumnType.LONG);
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isThreadSafe() {
        return UnaryFunction.super.isThreadSafe();
    }

    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        final double srcSum = srcValue.getDouble(valueIndex);
        final long srcCount = srcValue.getLong(valueIndex + 1);
        if (srcCount > 0) {
            final double destSum = destValue.getDouble(valueIndex);
            final long destCount = destValue.getLong(valueIndex + 1);
            if (destCount > 0) {
                destValue.putDouble(valueIndex, destSum + srcSum);
                destValue.putLong(valueIndex + 1, destCount + srcCount);
            } else {
                destValue.putDouble(valueIndex, srcSum);
                destValue.putLong(valueIndex + 1, srcCount);
            }
        }
    }

    @Override
    public void setDouble(MapValue mapValue, double value) {
        mapValue.putDouble(valueIndex, value);
        mapValue.putLong(valueIndex + 1, 1);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putDouble(valueIndex, Double.NaN);
        mapValue.putLong(valueIndex + 1, 0);
    }

    @Override
    public boolean supportsBatchComputation() {
        return !isArgNotNull;
    }

    @Override
    public boolean supportsParallelism() {
        return UnaryFunction.super.supportsParallelism();
    }

    private static void applyAvg(long sumAddr, long countAddr, double value, boolean isNew, boolean isArgNotNull) {
        if (isNew) {
            if (isArgNotNull || !Double.isNaN(value)) {
                Unsafe.getUnsafe().putDouble(sumAddr, value);
                Unsafe.getUnsafe().putLong(countAddr, 1L);
            } else {
                // Overwrite the etalon's (NaN, 0) with the computeFirst NaN-case state.
                Unsafe.getUnsafe().putDouble(sumAddr, 0);
                Unsafe.getUnsafe().putLong(countAddr, 0L);
            }
        } else if (isArgNotNull || !Double.isNaN(value)) {
            Unsafe.getUnsafe().putDouble(sumAddr, Unsafe.getUnsafe().getDouble(sumAddr) + value);
            Unsafe.getUnsafe().putLong(countAddr, Unsafe.getUnsafe().getLong(countAddr) + 1L);
        }
    }
}
