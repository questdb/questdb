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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.groupby.FlyweightPackedMapValue;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.NotNull;

public class CountIntGroupByFunction extends AbstractCountGroupByFunction {
    private final int argColumnIndex;

    public CountIntGroupByFunction(@NotNull Function arg) {
        super(arg);
        this.argColumnIndex = GroupByUtils.directArgColumnIndex(arg, ColumnType.INT);
    }

    @Override
    public void computeBatch(MapValue mapValue, long dataAddr, int rowCount, long startRowId) {
        if (rowCount > 0) {
            final long nonNullCount = Vect.countInt(dataAddr, rowCount);
            if (nonNullCount > 0) {
                mapValue.addLong(valueIndex, nonNullCount);
            }
        }
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        final int value = arg.getInt(record);
        if (value != Numbers.INT_NULL) {
            mapValue.putLong(valueIndex, 1);
        } else {
            mapValue.putLong(valueIndex, 0);
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
        // setEmpty stores 0, matching the identity for count; non-null rows add 1.
        final long valueColumnOffset = mapValue.getOffset(valueIndex);
        // Fast path: arg is a direct int column with data on the current frame.
        // Zero page address means a column top; fall through to the record-based path.
        final long argAddr = argColumnIndex >= 0 ? record.getPageAddress(argColumnIndex) : 0;
        if (argAddr != 0) {
            for (long i = 0; i < rowCount; i++) {
                final long encoded = Unsafe.getUnsafe().getLong(batchAddr + (i << 3));
                final long rowIndex = Map.decodeBatchRowIndex(encoded);
                final int value = Unsafe.getUnsafe().getInt(argAddr + (rowIndex << 2));
                if (value != Numbers.INT_NULL) {
                    final long addr = baseValueAddr + Map.decodeBatchOffset(encoded) + valueColumnOffset;
                    Unsafe.getUnsafe().putLong(addr, Unsafe.getUnsafe().getLong(addr) + 1);
                }
            }
        } else {
            for (long i = 0; i < rowCount; i++) {
                final long encoded = Unsafe.getUnsafe().getLong(batchAddr + (i << 3));
                record.setRowIndex(Map.decodeBatchRowIndex(encoded));
                final int value = arg.getInt(record);
                if (value != Numbers.INT_NULL) {
                    final long addr = baseValueAddr + Map.decodeBatchOffset(encoded) + valueColumnOffset;
                    Unsafe.getUnsafe().putLong(addr, Unsafe.getUnsafe().getLong(addr) + 1);
                }
            }
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final int value = arg.getInt(record);
        if (value != Numbers.INT_NULL) {
            mapValue.addLong(valueIndex, 1);
        }
    }

    @Override
    public int getComputeBatchArgType() {
        return ColumnType.INT;
    }

    @Override
    public boolean supportsBatchComputation() {
        return true;
    }
}
