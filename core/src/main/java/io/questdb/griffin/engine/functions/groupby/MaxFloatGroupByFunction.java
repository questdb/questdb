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
import io.questdb.griffin.engine.functions.FloatFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.groupby.FlyweightPackedMapValue;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

public class MaxFloatGroupByFunction extends FloatFunction implements GroupByFunction, UnaryFunction {
    private final Function arg;
    private final int argColumnIndex;
    private int valueIndex;

    public MaxFloatGroupByFunction(@NotNull Function arg) {
        this.arg = arg;
        this.argColumnIndex = GroupByUtils.directArgColumnIndex(arg, ColumnType.FLOAT);
    }

    @Override
    public void computeBatch(MapValue mapValue, long dataAddr, int rowCount, long startRowId) {
        if (rowCount > 0) {
            final long hi = dataAddr + rowCount * (long) Float.BYTES;
            float max = Float.NaN;
            for (; dataAddr < hi; dataAddr += Float.BYTES) {
                float value = Unsafe.getFloat(dataAddr);
                if (value > max || Numbers.isNull(max)) {
                    max = value;
                }
            }
            final float existing = mapValue.getFloat(valueIndex);
            if (max > existing || Numbers.isNull(existing)) {
                mapValue.putFloat(valueIndex, max);
            }
        }
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        mapValue.putFloat(valueIndex, arg.getFloat(record));
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
        // Mirrors computeNext: replace the accumulator only when the new value is
        // strictly greater, or when the accumulator is still the null sentinel (NaN/inf).
        final long valueColumnOffset = mapValue.getOffset(valueIndex);
        // Fast path: arg is a direct float column with data on the current frame.
        // Zero page address means a column top; fall through to the record-based path.
        final long argAddr = argColumnIndex >= 0 ? record.getPageAddress(argColumnIndex) : 0;
        if (argAddr != 0) {
            for (long i = 0; i < rowCount; i++) {
                final long encoded = Unsafe.getLong(batchAddr + (i << 3));
                final long rowIndex = Map.decodeBatchRowIndex(encoded);
                final float value = Unsafe.getFloat(argAddr + (rowIndex << 2));
                final long addr = baseValueAddr + Map.decodeBatchOffset(encoded) + valueColumnOffset;
                final float current = Unsafe.getFloat(addr);
                if (value > current || Numbers.isNull(current)) {
                    Unsafe.putFloat(addr, value);
                }
            }
        } else {
            for (long i = 0; i < rowCount; i++) {
                final long encoded = Unsafe.getLong(batchAddr + (i << 3));
                record.setRowIndex(Map.decodeBatchRowIndex(encoded));
                final float value = arg.getFloat(record);
                final long addr = baseValueAddr + Map.decodeBatchOffset(encoded) + valueColumnOffset;
                final float current = Unsafe.getFloat(addr);
                if (value > current || Numbers.isNull(current)) {
                    Unsafe.putFloat(addr, value);
                }
            }
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        float max = mapValue.getFloat(valueIndex);
        float next = arg.getFloat(record);
        if (next > max || Numbers.isNull(max)) {
            mapValue.putFloat(valueIndex, next);
        }
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public float getFloat(Record rec) {
        return rec.getFloat(valueIndex);
    }

    @Override
    public String getName() {
        return "max";
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
        columnTypes.add(ColumnType.FLOAT);
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
        float srcMax = srcValue.getFloat(valueIndex);
        float destMax = destValue.getFloat(valueIndex);
        if (srcMax > destMax || Numbers.isNull(destMax)) {
            destValue.putFloat(valueIndex, srcMax);
        }
    }

    @Override
    public void setFloat(MapValue mapValue, float value) {
        mapValue.putFloat(valueIndex, value);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putFloat(valueIndex, Float.NaN);
    }

    @Override
    public boolean supportsBatchComputation() {
        return true;
    }

    @Override
    public boolean supportsParallelism() {
        return UnaryFunction.super.supportsParallelism();
    }
}
