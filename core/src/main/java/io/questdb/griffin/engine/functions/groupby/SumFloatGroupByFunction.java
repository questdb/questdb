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
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

public class SumFloatGroupByFunction extends FloatFunction implements GroupByFunction, UnaryFunction {
    private final Function arg;
    private final boolean isArgNotNull;
    private final int argColumnIndex;
    private int valueIndex;

    public SumFloatGroupByFunction(@NotNull Function arg) {
        this.arg = arg;
        this.isArgNotNull = arg != null && arg.isNotNull();
        this.argColumnIndex = GroupByUtils.directArgColumnIndex(arg, ColumnType.FLOAT);
    }

    @Override
    public void computeBatch(MapValue mapValue, long dataAddr, int rowCount, long startRowId) {
        if (rowCount > 0) {
            float acc = 0.0f;
            boolean hasValue = false;
            final long hi = dataAddr + rowCount * (long) Float.BYTES;
            for (; dataAddr < hi; dataAddr += Float.BYTES) {
                final float value = Unsafe.getFloat(dataAddr);
                if (isArgNotNull || !Float.isNaN(value)) {
                    acc += value;
                    hasValue = true;
                }
            }
            if (hasValue) {
                final float existing = mapValue.getFloat(valueIndex);
                if (!Float.isNaN(existing)) {
                    mapValue.putFloat(valueIndex, existing + acc);
                } else {
                    mapValue.putFloat(valueIndex, acc);
                }
            }
        }
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        final float value = arg.getFloat(record);
        mapValue.putFloat(valueIndex, value);
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
        final long valueColumnOffset = mapValue.getOffset(valueIndex);
        // Fast path: arg is a direct float column with data on the current frame.
        // Zero page address means a column top; fall through to the record-based path.
        final long argAddr = argColumnIndex >= 0 ? record.getPageAddress(argColumnIndex) : 0;
        if (argAddr != 0) {
            for (long i = 0; i < rowCount; i++) {
                final long encoded = Unsafe.getLong(batchAddr + (i << 3));
                final long rowIndex = Map.decodeBatchRowIndex(encoded);
                final float value = Unsafe.getFloat(argAddr + (rowIndex << 2));
                if (isArgNotNull || !Float.isNaN(value)) {
                    final long addr = baseValueAddr + Map.decodeBatchOffset(encoded) + valueColumnOffset;
                    final float current = Unsafe.getFloat(addr);
                    Unsafe.putFloat(addr, !Float.isNaN(current) ? current + value : value);
                }
            }
        } else {
            for (long i = 0; i < rowCount; i++) {
                final long encoded = Unsafe.getLong(batchAddr + (i << 3));
                record.setRowIndex(Map.decodeBatchRowIndex(encoded));
                final float value = arg.getFloat(record);
                if (isArgNotNull || !Float.isNaN(value)) {
                    final long addr = baseValueAddr + Map.decodeBatchOffset(encoded) + valueColumnOffset;
                    final float current = Unsafe.getFloat(addr);
                    Unsafe.putFloat(addr, !Float.isNaN(current) ? current + value : value);
                }
            }
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final float value = arg.getFloat(record);
        if (isArgNotNull || !Float.isNaN(value)) {
            final float sum = mapValue.getFloat(valueIndex);
            if (!Float.isNaN(sum)) {
                mapValue.putFloat(valueIndex, sum + value);
            } else {
                mapValue.putFloat(valueIndex, value);
            }
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
        return "sum";
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
        final float srcSum = srcValue.getFloat(valueIndex);
        if (!Float.isNaN(srcSum)) {
            final float destSum = destValue.getFloat(valueIndex);
            if (!Float.isNaN(destSum)) {
                destValue.putFloat(valueIndex, destSum + srcSum);
            } else {
                destValue.putFloat(valueIndex, srcSum);
            }
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
        return !isArgNotNull;
    }

    @Override
    public boolean supportsParallelism() {
        return UnaryFunction.super.supportsParallelism();
    }
}
