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
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.groupby.FlyweightPackedMapValue;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.NotNull;

public class MinLongGroupByFunction extends LongFunction implements GroupByFunction, UnaryFunction {
    private final Function arg;
    private final int argColumnIndex;
    private int valueIndex;

    public MinLongGroupByFunction(@NotNull Function arg) {
        this.arg = arg;
        this.argColumnIndex = GroupByUtils.directArgColumnIndex(arg, ColumnType.LONG);
    }

    @Override
    public void computeBatch(MapValue mapValue, long dataAddr, int rowCount, long startRowId) {
        if (rowCount > 0) {
            final long batchMin = Vect.minLong(dataAddr, rowCount);
            if (batchMin != Numbers.LONG_NULL) {
                final long existing = mapValue.getLong(valueIndex);
                if (batchMin < existing || existing == Numbers.LONG_NULL) {
                    mapValue.putLong(valueIndex, batchMin);
                }
            }
        }
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        mapValue.putLong(valueIndex, arg.getLong(record));
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
        // Fast path: arg is a direct long column with data on the current frame.
        // Zero page address means a column top; fall through to the record-based path.
        final long argAddr = argColumnIndex >= 0 ? record.getPageAddress(argColumnIndex) : 0;
        if (argAddr != 0) {
            for (long i = 0; i < rowCount; i++) {
                final long encoded = Unsafe.getLong(batchAddr + (i << 3));
                final long rowIndex = Map.decodeBatchRowIndex(encoded);
                final long value = Unsafe.getLong(argAddr + (rowIndex << 3));
                if (value != Numbers.LONG_NULL) {
                    final long addr = baseValueAddr + Map.decodeBatchOffset(encoded) + valueColumnOffset;
                    final long current = Unsafe.getLong(addr);
                    Unsafe.putLong(addr, current != Numbers.LONG_NULL ? Math.min(current, value) : value);
                }
            }
        } else {
            for (long i = 0; i < rowCount; i++) {
                final long encoded = Unsafe.getLong(batchAddr + (i << 3));
                record.setRowIndex(Map.decodeBatchRowIndex(encoded));
                final long value = arg.getLong(record);
                if (value != Numbers.LONG_NULL) {
                    final long addr = baseValueAddr + Map.decodeBatchOffset(encoded) + valueColumnOffset;
                    final long current = Unsafe.getLong(addr);
                    Unsafe.putLong(addr, current != Numbers.LONG_NULL ? Math.min(current, value) : value);
                }
            }
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        mapValue.minLong(valueIndex, arg.getLong(record));
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public long getLong(Record rec) {
        return rec.getLong(valueIndex);
    }

    @Override
    public String getName() {
        return "min";
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
        long srcMin = srcValue.getLong(valueIndex);
        long destMin = destValue.getLong(valueIndex);
        if (srcMin != Numbers.LONG_NULL && (srcMin < destMin || destMin == Numbers.LONG_NULL)) {
            destValue.putLong(valueIndex, srcMin);
        }
    }

    @Override
    public void setLong(MapValue mapValue, long value) {
        mapValue.putLong(valueIndex, value);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putLong(valueIndex, Numbers.LONG_NULL);
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
