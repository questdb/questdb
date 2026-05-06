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

import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.groupby.FlyweightPackedMapValue;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

public class LastDateGroupByFunction extends FirstDateGroupByFunction {
    public LastDateGroupByFunction(@NotNull Function arg) {
        super(arg);
    }

    @Override
    public void computeBatch(MapValue mapValue, long dataAddr, int rowCount, long startRowId) {
        if (rowCount > 0) {
            long lastRowId = startRowId + rowCount - 1;
            long existingRowId = mapValue.getLong(valueIndex);
            if (lastRowId > existingRowId || existingRowId == Numbers.LONG_NULL) {
                mapValue.putLong(valueIndex, lastRowId);
                mapValue.putDate(valueIndex + 1, Unsafe.getLong(dataAddr + ((long) rowCount - 1) * Long.BYTES));
            }
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
        // setEmpty pre-seeds rowId = LONG_NULL (= Long.MIN_VALUE), so the first real
        // rowId always exceeds it. No explicit null branch needed.
        final long rowIdOffset = mapValue.getOffset(valueIndex);
        final long valueColumnOffset = mapValue.getOffset(valueIndex + 1);
        // Fast path: arg is a direct date column with data on the current frame.
        // Zero page address means a column top; fall through to the record-based path.
        final long argAddr = argColumnIndex >= 0 ? record.getPageAddress(argColumnIndex) : 0;
        if (argAddr != 0) {
            for (long i = 0; i < rowCount; i++) {
                final long encoded = Unsafe.getLong(batchAddr + (i << 3));
                final long rowIndex = Map.decodeBatchRowIndex(encoded);
                final long rowId = baseRowId + rowIndex;
                final long entryBase = baseValueAddr + Map.decodeBatchOffset(encoded);
                if (rowId > Unsafe.getLong(entryBase + rowIdOffset)) {
                    Unsafe.putLong(entryBase + rowIdOffset, rowId);
                    Unsafe.putLong(entryBase + valueColumnOffset, Unsafe.getLong(argAddr + (rowIndex << 3)));
                }
            }
        } else {
            for (long i = 0; i < rowCount; i++) {
                final long encoded = Unsafe.getLong(batchAddr + (i << 3));
                final long rowIndex = Map.decodeBatchRowIndex(encoded);
                final long rowId = baseRowId + rowIndex;
                final long entryBase = baseValueAddr + Map.decodeBatchOffset(encoded);
                if (rowId > Unsafe.getLong(entryBase + rowIdOffset)) {
                    record.setRowIndex(rowIndex);
                    Unsafe.putLong(entryBase + rowIdOffset, rowId);
                    Unsafe.putLong(entryBase + valueColumnOffset, arg.getDate(record));
                }
            }
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        if (rowId > mapValue.getLong(valueIndex)) {
            computeFirst(mapValue, record, rowId);
        }
    }

    @Override
    public String getName() {
        return "last";
    }

    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        long srcRowId = srcValue.getLong(valueIndex);
        long destRowId = destValue.getLong(valueIndex);
        if (srcRowId > destRowId) {
            destValue.putLong(valueIndex, srcRowId);
            destValue.putDate(valueIndex + 1, srcValue.getDate(valueIndex + 1));
        }
    }
}
