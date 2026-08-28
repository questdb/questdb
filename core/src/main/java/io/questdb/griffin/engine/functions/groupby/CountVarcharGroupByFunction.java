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
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.groupby.FlyweightPackedMapValue;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

public class CountVarcharGroupByFunction extends AbstractCountGroupByFunction {

    public CountVarcharGroupByFunction(@NotNull Function arg) {
        super(arg);
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        if (arg.getVarcharSize(record) != TableUtils.NULL_LEN) {
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
        // setEmpty stores 0, so the etalon seed matches the identity element for count
        // and new entries need no branching. Each non-null row adds 1.
        // Varchar storage uses an aux+data layout that does not admit a direct-column
        // fast path, so we go through the record every iteration.
        final long valueColumnOffset = mapValue.getOffset(valueIndex);
        for (long i = 0; i < rowCount; i++) {
            final long encoded = Unsafe.getLong(batchAddr + (i << 3));
            record.setRowIndex(Map.decodeBatchRowIndex(encoded));
            if (arg.getVarcharSize(record) != TableUtils.NULL_LEN) {
                final long addr = baseValueAddr + Map.decodeBatchOffset(encoded) + valueColumnOffset;
                Unsafe.putLong(addr, Unsafe.getLong(addr) + 1);
            }
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        if (arg.getVarcharSize(record) != TableUtils.NULL_LEN) {
            mapValue.addLong(valueIndex, 1);
        }
    }

    @Override
    public int getComputeBatchArgType() {
        return ColumnType.VARCHAR;
    }
}
