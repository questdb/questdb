/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.griffin.engine.table.aggr;

import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.map.Unordered8Map;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.table.AsyncGroupByAtom;
import io.questdb.std.DirectLongList;
import io.questdb.std.Hash;
import io.questdb.std.Unsafe;

public class LongKeyCountAggregator extends AbstractAggregator {
    private final int columnIndex;

    public LongKeyCountAggregator(int columnIndex) {
        this.columnIndex = columnIndex;
    }

    @Override
    public void aggregateFilteredNonSharded(
            PageFrameMemoryRecord record,
            DirectLongList rows,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            AsyncGroupByAtom.MapFragment fragment,
            RecordSink mapSink
    ) {
        final Unordered8Map map = (Unordered8Map) fragment.reopenMap();
        final long startAddr = record.getPageAddress(columnIndex);
        for (long p = 0, n = rows.size(); p < n; p++) {
            final long r = rows.get(p);
            final long addr = startAddr + (r << 3);
            final long v = Unsafe.getUnsafe().getLong(addr);

            final MapKey key = map.withKey();
            key.putLong(v);

            MapValue value = key.createValue();
            // Unordered8Map zeroes all keys and values, so we can add 1 even in case of a new value.
            long valueAddr = value.getStartAddress() + Unordered8Map.KEY_SIZE;
            Unsafe.getUnsafe().putLong(valueAddr, Unsafe.getUnsafe().getLong(valueAddr) + 1);
        }
    }

    @Override
    public void aggregateFilteredSharded(
            PageFrameMemoryRecord record,
            DirectLongList rows,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            AsyncGroupByAtom.MapFragment fragment,
            RecordSink mapSink
    ) {
        final long startAddr = record.getPageAddress(columnIndex);
        for (long p = 0, n = rows.size(); p < n; p++) {
            final long r = rows.get(p);
            final long addr = startAddr + (r << 3);
            final long v = Unsafe.getUnsafe().getLong(addr);

            final long hashCode = Hash.hashLong64(v);

            final Unordered8Map shard = (Unordered8Map) fragment.getShardMap(hashCode);
            final MapKey shardKey = shard.withKey();
            shardKey.putLong(v);

            MapValue shardValue = shardKey.createValue(hashCode);
            // Unordered8Map zeroes all keys and values, so we can add 1 even in case of a new value.
            long valueAddr = shardValue.getStartAddress() + Unordered8Map.KEY_SIZE;
            Unsafe.getUnsafe().putLong(valueAddr, Unsafe.getUnsafe().getLong(valueAddr) + 1);
        }
    }

    @Override
    protected void aggregateNonSharded(
            PageFrameMemoryRecord record,
            long frameRowCount,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            AsyncGroupByAtom.MapFragment fragment,
            RecordSink mapSink
    ) {
        final Unordered8Map map = (Unordered8Map) fragment.reopenMap();
        final long startAddr = record.getPageAddress(columnIndex);
        final long lim = startAddr + 8 * frameRowCount;
        for (long addr = startAddr; addr < lim; addr += 8) {
            final long v = Unsafe.getUnsafe().getLong(addr);

            final MapKey key = map.withKey();
            key.putLong(v);

            MapValue value = key.createValue();
            // Unordered8Map zeroes all keys and values, so we can add 1 even in case of a new value.
            long valueAddr = value.getStartAddress() + Unordered8Map.KEY_SIZE;
            Unsafe.getUnsafe().putLong(valueAddr, Unsafe.getUnsafe().getLong(valueAddr) + 1);
        }
    }

    @Override
    protected void aggregateSharded(
            PageFrameMemoryRecord record,
            long frameRowCount,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            AsyncGroupByAtom.MapFragment fragment,
            RecordSink mapSink
    ) {
        final long startAddr = record.getPageAddress(columnIndex);
        final long lim = startAddr + 8 * frameRowCount;
        for (long addr = startAddr; addr < lim; addr += 8) {
            final long v = Unsafe.getUnsafe().getLong(addr);

            final long hashCode = Hash.hashLong64(v);

            final Unordered8Map shard = (Unordered8Map) fragment.getShardMap(hashCode);
            final MapKey shardKey = shard.withKey();
            shardKey.putLong(v);

            MapValue shardValue = shardKey.createValue(hashCode);
            // Unordered8Map zeroes all keys and values, so we can add 1 even in case of a new value.
            long valueAddr = shardValue.getStartAddress() + Unordered8Map.KEY_SIZE;
            Unsafe.getUnsafe().putLong(valueAddr, Unsafe.getUnsafe().getLong(valueAddr) + 1);
        }
    }
}
