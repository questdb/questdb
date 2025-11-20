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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.map.OrderedMap;
import io.questdb.cairo.map.Unordered2Map;
import io.questdb.cairo.map.Unordered4Map;
import io.questdb.cairo.map.Unordered8Map;
import io.questdb.cairo.map.UnorderedVarcharMap;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameReducer;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class AsyncGroupByReducer implements PageFrameReducer {
    private final int singleColumnIndex;

    public AsyncGroupByReducer(int singleColumnIndex) {
        this.singleColumnIndex = singleColumnIndex;
    }

    @Override
    public void reduce(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = task.getFrameRowCount();
        assert frameRowCount > 0;
        final AsyncGroupByAtom atom = task.getFrameSequence(AsyncGroupByAtom.class).getAtom();

        final PageFrameMemory frameMemory = task.populateFrameMemory();
        record.init(frameMemory);

        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);
        final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
        final AsyncGroupByAtom.MapFragment fragment = atom.getFragment(slotId);
        final RecordSink mapSink = atom.getMapSink(slotId);
        try {
            if (atom.isSharded()) {
                fragment.shard();
            }

            record.setRowIndex(0);
            long baseRowId = record.getRowId();

            if (fragment.isNotSharded()) {
                if (atom.getFragment(slotId).getMap() instanceof UnorderedVarcharMap) {
                    aggregateNonShardedVarcharKey(record, frameRowCount, baseRowId, functionUpdater, fragment, singleColumnIndex);
                } else if (atom.getFragment(slotId).getMap() instanceof Unordered2Map) {
                    aggregateNonShardedShortKey(record, frameRowCount, baseRowId, functionUpdater, fragment, singleColumnIndex);
                } else if (atom.getFragment(slotId).getMap() instanceof Unordered4Map) {
                    aggregateNonShardedIntKey(record, frameRowCount, baseRowId, functionUpdater, fragment, singleColumnIndex);
                } else if (atom.getFragment(slotId).getMap() instanceof Unordered8Map) {
                    aggregateNonShardedLongKey(record, frameRowCount, baseRowId, functionUpdater, fragment, singleColumnIndex);
                } else {
                    assert atom.getFragment(slotId).getMap() instanceof OrderedMap : "unexpected map class: " + atom.getFragment(slotId).getMap().getClass();
                    aggregateNonShardedGeneric(record, frameRowCount, baseRowId, functionUpdater, fragment, mapSink);
                }
            } else {
                // single short key can't be shared, so we don't need a special case here
                if (atom.getFragment(slotId).getMap() instanceof UnorderedVarcharMap) {
                    aggregateShardedVarcharKey(record, frameRowCount, baseRowId, functionUpdater, fragment, singleColumnIndex);
                } else if (atom.getFragment(slotId).getMap() instanceof Unordered4Map) {
                    aggregateShardedIntKey(record, frameRowCount, baseRowId, functionUpdater, fragment, singleColumnIndex);
                } else if (atom.getFragment(slotId).getMap() instanceof Unordered8Map) {
                    aggregateShardedLongKey(record, frameRowCount, baseRowId, functionUpdater, fragment, singleColumnIndex);
                } else {
                    assert atom.getFragment(slotId).getMap() instanceof OrderedMap : "unexpected map class: " + atom.getFragment(slotId).getMap().getClass();
                    aggregateShardedGeneric(record, frameRowCount, baseRowId, functionUpdater, fragment, mapSink);
                }
            }

            atom.requestSharding(fragment);
        } finally {
            atom.release(slotId);
        }
    }

    private static void aggregateNonShardedGeneric(
            PageFrameMemoryRecord record,
            long frameRowCount,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            AsyncGroupByAtom.MapFragment fragment,
            RecordSink mapSink
    ) {
        final Map map = fragment.reopenMap();
        for (long r = 0; r < frameRowCount; r++) {
            record.setRowIndex(r);

            final MapKey key = map.withKey();
            mapSink.copy(record, key);
            MapValue value = key.createValue();
            if (value.isNew()) {
                functionUpdater.updateNew(value, record, baseRowId + r);
            } else {
                functionUpdater.updateExisting(value, record, baseRowId + r);
            }
        }
    }

    private static void aggregateNonShardedIntKey(
            PageFrameMemoryRecord record,
            long frameRowCount,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            AsyncGroupByAtom.MapFragment fragment,
            int columnIndex
    ) {
        assert columnIndex != -1;
        final Unordered4Map map = (Unordered4Map) fragment.reopenMap();
        for (long r = 0, p = record.getPageAddress(columnIndex); r < frameRowCount; r++, p += 4) {
            final int key = Unsafe.getUnsafe().getInt(p);
            MapValue value = map.createValueWithKey(key);
            record.setRowIndex(r);
            if (value.isNew()) {
                functionUpdater.updateNew(value, record, baseRowId + r);
            } else {
                functionUpdater.updateExisting(value, record, baseRowId + r);
            }
        }
    }

    private static void aggregateNonShardedLongKey(
            PageFrameMemoryRecord record,
            long frameRowCount,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            AsyncGroupByAtom.MapFragment fragment,
            int columnIndex
    ) {
        assert columnIndex != -1;
        final Unordered8Map map = (Unordered8Map) fragment.reopenMap();
        for (long r = 0, p = record.getPageAddress(columnIndex); r < frameRowCount; r++, p += 8) {
            final long key = Unsafe.getUnsafe().getLong(p);
            MapValue value = map.createValueWithKey(key);
            record.setRowIndex(r);
            if (value.isNew()) {
                functionUpdater.updateNew(value, record, baseRowId + r);
            } else {
                functionUpdater.updateExisting(value, record, baseRowId + r);
            }
        }
    }

    private static void aggregateNonShardedShortKey(
            PageFrameMemoryRecord record,
            long frameRowCount,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            AsyncGroupByAtom.MapFragment fragment,
            int columnIndex
    ) {
        assert columnIndex != -1;
        final Unordered2Map map = (Unordered2Map) fragment.reopenMap();
        for (long r = 0, p = record.getPageAddress(columnIndex); r < frameRowCount; r++, p += 2) {
            final short key = Unsafe.getUnsafe().getShort(p);
            MapValue value = map.createValueWithKey(key);
            record.setRowIndex(r);
            if (value.isNew()) {
                functionUpdater.updateNew(value, record, baseRowId + r);
            } else {
                functionUpdater.updateExisting(value, record, baseRowId + r);
            }
        }
    }

    private static void aggregateNonShardedVarcharKey(
            PageFrameMemoryRecord record,
            long frameRowCount,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            AsyncGroupByAtom.MapFragment fragment,
            int columnIndex
    ) {
        assert columnIndex != -1;
        final UnorderedVarcharMap map = (UnorderedVarcharMap) fragment.reopenMap();
        for (long r = 0; r < frameRowCount; r++) {
            record.setRowIndex(r);
            final MapKey key = map.withKey();
            key.putVarchar(record.getVarcharA(columnIndex));
            MapValue value = key.createValue();
            if (value.isNew()) {
                functionUpdater.updateNew(value, record, baseRowId + r);
            } else {
                functionUpdater.updateExisting(value, record, baseRowId + r);
            }
        }
    }

    private static void aggregateShardedGeneric(
            PageFrameMemoryRecord record,
            long frameRowCount,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            AsyncGroupByAtom.MapFragment fragment,
            RecordSink mapSink
    ) {
        // The first map is used to write keys.
        final Map lookupShard = fragment.getShards().getQuick(0);
        for (long r = 0; r < frameRowCount; r++) {
            record.setRowIndex(r);

            final MapKey lookupKey = lookupShard.withKey();
            mapSink.copy(record, lookupKey);
            lookupKey.commit();
            final long hashCode = lookupKey.hash();

            final Map shard = fragment.getShardMap(hashCode);
            final MapKey shardKey;
            if (shard != lookupShard) {
                shardKey = shard.withKey();
                shardKey.copyFrom(lookupKey);
            } else {
                shardKey = lookupKey;
            }

            MapValue shardValue = shardKey.createValue(hashCode);
            if (shardValue.isNew()) {
                functionUpdater.updateNew(shardValue, record, baseRowId + r);
            } else {
                functionUpdater.updateExisting(shardValue, record, baseRowId + r);
            }
        }
    }

    private static void aggregateShardedIntKey(
            PageFrameMemoryRecord record,
            long frameRowCount,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            AsyncGroupByAtom.MapFragment fragment,
            int columnIndex
    ) {
        assert columnIndex != -1;
        for (long r = 0, p = record.getPageAddress(columnIndex); r < frameRowCount; r++, p += 4) {
            final int key = Unsafe.getUnsafe().getInt(p);
            final long hashCode = Unordered4Map.hashKey(key);
            final Unordered4Map shard = (Unordered4Map) fragment.getShardMap(hashCode);

            MapValue shardValue = shard.createValueWithKey(key, hashCode);
            record.setRowIndex(r);
            if (shardValue.isNew()) {
                functionUpdater.updateNew(shardValue, record, baseRowId + r);
            } else {
                functionUpdater.updateExisting(shardValue, record, baseRowId + r);
            }
        }
    }

    private static void aggregateShardedLongKey(
            PageFrameMemoryRecord record,
            long frameRowCount,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            AsyncGroupByAtom.MapFragment fragment,
            int columnIndex
    ) {
        assert columnIndex != -1;
        for (long r = 0, p = record.getPageAddress(columnIndex); r < frameRowCount; r++, p += 8) {
            final long key = Unsafe.getUnsafe().getLong(p);
            final long hashCode = Unordered8Map.hashKey(key);
            final Unordered8Map shard = (Unordered8Map) fragment.getShardMap(hashCode);

            MapValue shardValue = shard.createValueWithKey(key, hashCode);
            record.setRowIndex(r);
            if (shardValue.isNew()) {
                functionUpdater.updateNew(shardValue, record, baseRowId + r);
            } else {
                functionUpdater.updateExisting(shardValue, record, baseRowId + r);
            }
        }
    }

    private static void aggregateShardedVarcharKey(
            PageFrameMemoryRecord record,
            long frameRowCount,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            AsyncGroupByAtom.MapFragment fragment,
            int columnIndex
    ) {
        assert columnIndex != -1;
        // The first map is used to write keys.
        final Map lookupShard = fragment.getShards().getQuick(0);
        for (long r = 0; r < frameRowCount; r++) {
            record.setRowIndex(r);

            final MapKey lookupKey = lookupShard.withKey();
            lookupKey.putVarchar(record.getVarcharA(columnIndex));
            lookupKey.commit();
            final long hashCode = lookupKey.hash();

            final Map shard = fragment.getShardMap(hashCode);
            final MapKey shardKey;
            if (shard != lookupShard) {
                shardKey = shard.withKey();
                shardKey.copyFrom(lookupKey);
            } else {
                shardKey = lookupKey;
            }

            MapValue shardValue = shardKey.createValue(hashCode);
            if (shardValue.isNew()) {
                functionUpdater.updateNew(shardValue, record, baseRowId + r);
            } else {
                functionUpdater.updateExisting(shardValue, record, baseRowId + r);
            }
        }
    }
}
