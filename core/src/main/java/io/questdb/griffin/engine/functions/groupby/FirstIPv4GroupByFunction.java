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
import io.questdb.griffin.engine.functions.IPv4Function;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.groupby.FlyweightPackedMapValue;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

public class FirstIPv4GroupByFunction extends IPv4Function implements GroupByFunction, UnaryFunction {
    protected final Function arg;
    protected final int argColumnIndex;
    protected int valueIndex;

    public FirstIPv4GroupByFunction(@NotNull Function arg) {
        this.arg = arg;
        this.argColumnIndex = GroupByUtils.directArgColumnIndex(arg, ColumnType.IPv4);
    }

    @Override
    public void computeBatch(MapValue mapValue, long dataAddr, int rowCount, long startRowId) {
        if (rowCount > 0) {
            long existingRowId = mapValue.getLong(valueIndex);
            if (startRowId < existingRowId || existingRowId == Numbers.LONG_NULL) {
                mapValue.putLong(valueIndex, startRowId);
                mapValue.putInt(valueIndex + 1, Unsafe.getInt(dataAddr));
            }
        }
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        mapValue.putLong(valueIndex, rowId);
        mapValue.putInt(valueIndex + 1, arg.getIPv4(record));
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
        // setEmpty pre-seeds rowId = LONG_NULL, so new entries win the "first" comparison
        // as long as we keep the explicit LONG_NULL check.
        final long rowIdOffset = mapValue.getOffset(valueIndex);
        final long valueColumnOffset = mapValue.getOffset(valueIndex + 1);
        // Fast path: arg is a direct IPv4 column with data on the current frame.
        // Zero page address means a column top; fall through to the record-based path.
        final long argAddr = argColumnIndex >= 0 ? record.getPageAddress(argColumnIndex) : 0;
        if (argAddr != 0) {
            for (long i = 0; i < rowCount; i++) {
                final long encoded = Unsafe.getLong(batchAddr + (i << 3));
                final long rowIndex = Map.decodeBatchRowIndex(encoded);
                final long rowId = baseRowId + rowIndex;
                final long entryBase = baseValueAddr + Map.decodeBatchOffset(encoded);
                final long existingRowId = Unsafe.getLong(entryBase + rowIdOffset);
                if (existingRowId == Numbers.LONG_NULL || rowId < existingRowId) {
                    Unsafe.putLong(entryBase + rowIdOffset, rowId);
                    Unsafe.putInt(entryBase + valueColumnOffset, Unsafe.getInt(argAddr + (rowIndex << 2)));
                }
            }
        } else {
            for (long i = 0; i < rowCount; i++) {
                final long encoded = Unsafe.getLong(batchAddr + (i << 3));
                final long rowIndex = Map.decodeBatchRowIndex(encoded);
                final long rowId = baseRowId + rowIndex;
                final long entryBase = baseValueAddr + Map.decodeBatchOffset(encoded);
                final long existingRowId = Unsafe.getLong(entryBase + rowIdOffset);
                if (existingRowId == Numbers.LONG_NULL || rowId < existingRowId) {
                    record.setRowIndex(rowIndex);
                    Unsafe.putLong(entryBase + rowIdOffset, rowId);
                    Unsafe.putInt(entryBase + valueColumnOffset, arg.getIPv4(record));
                }
            }
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        if (rowId < mapValue.getLong(valueIndex)) {
            computeFirst(mapValue, record, rowId);
        }
    }

    @Override
    public Function getArg() {
        return this.arg;
    }

    @Override
    public int getIPv4(Record rec) {
        return rec.getIPv4(valueIndex + 1);
    }

    @Override
    public String getName() {
        return "first";
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
        columnTypes.add(ColumnType.LONG); // row id
        columnTypes.add(ColumnType.IPv4); // value
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
        long srcRowId = srcValue.getLong(valueIndex);
        long destRowId = destValue.getLong(valueIndex);
        if (srcRowId != Numbers.LONG_NULL && (srcRowId < destRowId || destRowId == Numbers.LONG_NULL)) {
            destValue.putLong(valueIndex, srcRowId);
            destValue.putInt(valueIndex + 1, srcValue.getIPv4(valueIndex + 1));
        }
    }

    @Override
    public void setInt(MapValue mapValue, int value) {
        // This method is used to define interpolated points and to init
        // an empty value, so it's ok to reset the row id field here.
        mapValue.putLong(valueIndex, Numbers.LONG_NULL);
        mapValue.putInt(valueIndex + 1, value);
    }

    @Override
    public void setNull(MapValue mapValue) {
        setInt(mapValue, Numbers.IPv4_NULL);
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
