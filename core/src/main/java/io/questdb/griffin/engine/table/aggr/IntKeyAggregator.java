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
import io.questdb.cairo.map.Unordered4Map;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.table.AsyncGroupByAtom;
import io.questdb.std.DirectLongList;
import io.questdb.std.Hash;
import io.questdb.std.Unsafe;

public class IntKeyAggregator extends AbstractAggregator {
    private final int columnIndex;

    public IntKeyAggregator(int columnIndex) {
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
        final Unordered4Map map = (Unordered4Map) fragment.reopenMap();
        final long startAddr = record.getPageAddress(columnIndex);
        for (long p = 0, n = rows.size(); p < n; p++) {
            final long r = rows.get(p);
            final long addr = startAddr + (r << 2);
            final int v = Unsafe.getUnsafe().getInt(addr);

            final MapKey key = map.withKey();
            key.putInt(v);

            record.setRowIndex(r);

            MapValue value = key.createValue();
            if (value.isNew()) {
                functionUpdater.updateNew(value, record, baseRowId + r);
            } else {
                functionUpdater.updateExisting(value, record, baseRowId + r);
            }
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
            final long addr = startAddr + (r << 2);
            final int v = Unsafe.getUnsafe().getInt(addr);

            final long hashCode = Hash.hashInt64(v);

            final Unordered4Map shard = (Unordered4Map) fragment.getShardMap(hashCode);
            final MapKey shardKey = shard.withKey();
            shardKey.putInt(v);

            record.setRowIndex(r);

            MapValue shardValue = shardKey.createValue(hashCode);
            if (shardValue.isNew()) {
                functionUpdater.updateNew(shardValue, record, baseRowId + r);
            } else {
                functionUpdater.updateExisting(shardValue, record, baseRowId + r);
            }
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
        final Unordered4Map map = (Unordered4Map) fragment.reopenMap();
        final long startAddr = record.getPageAddress(columnIndex);
        for (long r = 0, addr = startAddr; r < frameRowCount; r++, addr += 4) {
            final int v = Unsafe.getUnsafe().getInt(addr);

            final MapKey key = map.withKey();
            key.putInt(v);

            record.setRowIndex(r);

            MapValue value = key.createValue();
            if (value.isNew()) {
                functionUpdater.updateNew(value, record, baseRowId + r);
            } else {
                functionUpdater.updateExisting(value, record, baseRowId + r);
            }
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
        for (long r = 0, addr = startAddr; r < frameRowCount; r++, addr += 4) {
            final int v = Unsafe.getUnsafe().getInt(addr);

            final long hashCode = Hash.hashInt64(v);

            final Unordered4Map shard = (Unordered4Map) fragment.getShardMap(hashCode);
            final MapKey shardKey = shard.withKey();
            shardKey.putInt(v);

            record.setRowIndex(r);

            MapValue shardValue = shardKey.createValue(hashCode);
            if (shardValue.isNew()) {
                functionUpdater.updateNew(shardValue, record, baseRowId + r);
            } else {
                functionUpdater.updateExisting(shardValue, record, baseRowId + r);
            }
        }
    }
}
