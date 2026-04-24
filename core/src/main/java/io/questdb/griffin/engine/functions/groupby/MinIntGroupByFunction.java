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
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.groupby.FlyweightPackedMapValue;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.NotNull;

public class MinIntGroupByFunction extends IntFunction implements GroupByFunction, UnaryFunction {
    private final Function arg;
    private final boolean isArgNotNull;
    private final int argColumnIndex;
    private int valueIndex;

    public MinIntGroupByFunction(@NotNull Function arg) {
        this.arg = arg;
        this.isArgNotNull = arg != null && arg.isNotNull();
        this.argColumnIndex = GroupByUtils.directArgColumnIndex(arg, ColumnType.INT);
    }

    @Override
    public void computeBatch(MapValue mapValue, long dataAddr, int rowCount, long startRowId) {
        if (rowCount > 0) {
            final int batchMin = Vect.minInt(dataAddr, rowCount);
            if (isArgNotNull || batchMin != Numbers.INT_NULL) {
                final int existing = mapValue.getInt(valueIndex);
                if (batchMin < existing || existing == Numbers.INT_NULL) {
                    mapValue.putInt(valueIndex, batchMin);
                }
            }
        }
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        mapValue.putInt(valueIndex, arg.getInt(record));
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
        // Fast path: arg is a direct int column with data on the current frame.
        // Zero page address means a column top; fall through to the record-based path.
        final long argAddr = argColumnIndex >= 0 ? record.getPageAddress(argColumnIndex) : 0;
        if (argAddr != 0) {
            for (long i = 0; i < rowCount; i++) {
                final long encoded = Unsafe.getLong(batchAddr + (i << 3));
                final long rowIndex = Map.decodeBatchRowIndex(encoded);
                final int value = Unsafe.getInt(argAddr + (rowIndex << 2));
                if (isArgNotNull || value != Numbers.INT_NULL) {
                    final long addr = baseValueAddr + Map.decodeBatchOffset(encoded) + valueColumnOffset;
                    final int current = Unsafe.getInt(addr);
                    Unsafe.putInt(addr, current != Numbers.INT_NULL ? Math.min(current, value) : value);
                }
            }
        } else {
            for (long i = 0; i < rowCount; i++) {
                final long encoded = Unsafe.getLong(batchAddr + (i << 3));
                record.setRowIndex(Map.decodeBatchRowIndex(encoded));
                final int value = arg.getInt(record);
                if (isArgNotNull || value != Numbers.INT_NULL) {
                    final long addr = baseValueAddr + Map.decodeBatchOffset(encoded) + valueColumnOffset;
                    final int current = Unsafe.getInt(addr);
                    Unsafe.putInt(addr, current != Numbers.INT_NULL ? Math.min(current, value) : value);
                }
            }
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        mapValue.minInt(valueIndex, arg.getInt(record));
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public int getInt(Record rec) {
        return rec.getInt(valueIndex);
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
        columnTypes.add(ColumnType.INT);
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
        int srcMin = srcValue.getInt(valueIndex);
        int destMin = destValue.getInt(valueIndex);
        if (srcMin != Numbers.INT_NULL && (srcMin < destMin || destMin == Numbers.INT_NULL)) {
            destValue.putInt(valueIndex, srcMin);
        }
    }

    @Override
    public void setInt(MapValue mapValue, int value) {
        mapValue.putInt(valueIndex, value);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putInt(valueIndex, Numbers.INT_NULL);
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
