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
import io.questdb.cairo.map.UnorderedVarcharMap;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.table.AsyncGroupByAtom;
import io.questdb.std.DirectLongList;
import io.questdb.std.Hash;
import io.questdb.std.str.Utf8Sequence;

public class VarcharKeyAggregator extends AbstractAggregator {
    private static final long NULL_HASH_CODE = Hash.hashMem64(0, 0);

    private final int columnIndex;

    public VarcharKeyAggregator(int columnIndex) {
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
        final UnorderedVarcharMap map = (UnorderedVarcharMap) fragment.reopenMap();
        for (long p = 0, n = rows.size(); p < n; p++) {
            final long r = rows.get(p);
            record.setRowIndex(r);
            final Utf8Sequence v = record.getVarcharA(columnIndex);

            final MapKey key = map.withKey();
            key.putVarchar(v);

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
        for (long p = 0, n = rows.size(); p < n; p++) {
            final long r = rows.get(p);
            record.setRowIndex(r);
            final Utf8Sequence v = record.getVarcharA(columnIndex);

            final long hashCode = v != null ? Hash.hashMem64(v.ptr(), v.size()) : NULL_HASH_CODE;

            final UnorderedVarcharMap shard = (UnorderedVarcharMap) fragment.getShardMap(hashCode);
            final MapKey shardKey = shard.withKey();
            shardKey.putVarchar(v);

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
        final UnorderedVarcharMap map = (UnorderedVarcharMap) fragment.reopenMap();
        for (long r = 0; r < frameRowCount; r++) {
            record.setRowIndex(r);
            final Utf8Sequence v = record.getVarcharA(columnIndex);

            final MapKey key = map.withKey();
            key.putVarchar(v);

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
        for (long r = 0; r < frameRowCount; r++) {
            record.setRowIndex(r);
            final Utf8Sequence v = record.getVarcharA(columnIndex);

            final long hashCode = v != null ? Hash.hashMem64(v.ptr(), v.size()) : NULL_HASH_CODE;

            final UnorderedVarcharMap shard = (UnorderedVarcharMap) fragment.getShardMap(hashCode);
            final MapKey shardKey = shard.withKey();
            shardKey.putVarchar(v);

            MapValue shardValue = shardKey.createValue(hashCode);
            if (shardValue.isNew()) {
                functionUpdater.updateNew(shardValue, record, baseRowId + r);
            } else {
                functionUpdater.updateExisting(shardValue, record, baseRowId + r);
            }
        }
    }
}
