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
            fragment.resetLocalStats();

            if (atom.isSharded()) {
                fragment.shard();
            }

            record.setRowIndex(0);
            final long baseRowId = record.getRowId();
            final Map map = atom.getFragment(slotId).getMap();
            if (fragment.isNotSharded()) {
                // Check if we can apply a fast-path for single column GROUP BY.
                if (map instanceof UnorderedVarcharMap) {
                    aggregateNonShardedVarcharKey(record, frameRowCount, baseRowId, functionUpdater, fragment, singleColumnIndex);
                } else if (map instanceof Unordered2Map) {
                    aggregateNonShardedShortKey(frameMemory, record, frameRowCount, baseRowId, functionUpdater, fragment, singleColumnIndex);
                } else if (map instanceof Unordered4Map) {
                    aggregateNonShardedIntKey(frameMemory, record, frameRowCount, baseRowId, functionUpdater, fragment, singleColumnIndex);
                } else if (map instanceof Unordered8Map) {
                    aggregateNonShardedLongKey(frameMemory, record, frameRowCount, baseRowId, functionUpdater, fragment, singleColumnIndex);
                } else {
                    aggregateNonShardedGeneric(record, frameRowCount, baseRowId, functionUpdater, fragment, mapSink);
                }
            } else {
                if (map instanceof UnorderedVarcharMap) {
                    aggregateShardedVarcharKey(record, frameRowCount, baseRowId, functionUpdater, fragment, singleColumnIndex);
                } else if (map instanceof Unordered2Map) {
                    aggregateShardedShortKey(frameMemory, record, frameRowCount, baseRowId, functionUpdater, fragment, singleColumnIndex);
                } else if (map instanceof Unordered4Map) {
                    aggregateShardedIntKey(frameMemory, record, frameRowCount, baseRowId, functionUpdater, fragment, singleColumnIndex);
                } else if (map instanceof Unordered8Map) {
                    aggregateShardedLongKey(frameMemory, record, frameRowCount, baseRowId, functionUpdater, fragment, singleColumnIndex);
                } else {
                    aggregateShardedGeneric(record, frameRowCount, baseRowId, functionUpdater, fragment, mapSink);
                }
            }

            atom.maybeEnableSharding(fragment);
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
            if (!value.isNew()) {
                functionUpdater.updateExisting(value, record, baseRowId + r);
            } else {
                functionUpdater.updateNew(value, record, baseRowId + r);
            }
        }
    }

    private static void aggregateNonShardedIntKey(
            PageFrameMemory frameMemory,
            PageFrameMemoryRecord record,
            long frameRowCount,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            AsyncGroupByAtom.MapFragment fragment,
            int columnIndex
    ) {
        assert columnIndex != -1;
        final Unordered4Map map = (Unordered4Map) fragment.reopenMap();
        long addr = frameMemory.getPageAddress(columnIndex);
        if (addr != 0) {
            for (long r = 0; r < frameRowCount; r++, addr += 4) {
                final int key = Unsafe.getUnsafe().getInt(addr);
                MapValue value = map.createValueWithKey(key);
                record.setRowIndex(r);
                if (!value.isNew()) {
                    functionUpdater.updateExisting(value, record, baseRowId + r);
                } else {
                    functionUpdater.updateNew(value, record, baseRowId + r);
                }
            }
        } else { // column top
            if (frameRowCount > 0) {
                record.setRowIndex(0);
                final int key = record.getInt(columnIndex);
                MapValue value = map.createValueWithKey(key);
                if (!value.isNew()) {
                    functionUpdater.updateExisting(value, record, baseRowId);
                } else {
                    functionUpdater.updateNew(value, record, baseRowId);
                }
                for (long r = 1; r < frameRowCount; r++) {
                    record.setRowIndex(r);
                    functionUpdater.updateExisting(value, record, baseRowId + r);
                }
            }
        }
    }

    private static void aggregateNonShardedLongKey(
            PageFrameMemory frameMemory,
            PageFrameMemoryRecord record,
            long frameRowCount,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            AsyncGroupByAtom.MapFragment fragment,
            int columnIndex
    ) {
        assert columnIndex != -1;
        final Unordered8Map map = (Unordered8Map) fragment.reopenMap();
        long addr = frameMemory.getPageAddress(columnIndex);
        if (addr != 0) {
            for (long r = 0; r < frameRowCount; r++, addr += 8) {
                final long key = Unsafe.getUnsafe().getLong(addr);
                MapValue value = map.createValueWithKey(key);
                record.setRowIndex(r);
                if (!value.isNew()) {
                    functionUpdater.updateExisting(value, record, baseRowId + r);
                } else {
                    functionUpdater.updateNew(value, record, baseRowId + r);
                }
            }
        } else { // column top
            if (frameRowCount > 0) {
                record.setRowIndex(0);
                final long key = record.getLong(columnIndex);
                MapValue value = map.createValueWithKey(key);
                if (!value.isNew()) {
                    functionUpdater.updateExisting(value, record, baseRowId);
                } else {
                    functionUpdater.updateNew(value, record, baseRowId);
                }
                for (long r = 1; r < frameRowCount; r++) {
                    record.setRowIndex(r);
                    functionUpdater.updateExisting(value, record, baseRowId + r);
                }
            }
        }
    }

    private static void aggregateNonShardedShortKey(
            PageFrameMemory frameMemory,
            PageFrameMemoryRecord record,
            long frameRowCount,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            AsyncGroupByAtom.MapFragment fragment,
            int columnIndex
    ) {
        assert columnIndex != -1;
        final Unordered2Map map = (Unordered2Map) fragment.reopenMap();
        long addr = frameMemory.getPageAddress(columnIndex);
        if (addr != 0) {
            for (long r = 0; r < frameRowCount; r++, addr += 2) {
                final short key = Unsafe.getUnsafe().getShort(addr);
                MapValue value = map.createValueWithKey(key);
                record.setRowIndex(r);
                if (!value.isNew()) {
                    functionUpdater.updateExisting(value, record, baseRowId + r);
                } else {
                    functionUpdater.updateNew(value, record, baseRowId + r);
                }
            }
        } else { // column top
            if (frameRowCount > 0) {
                record.setRowIndex(0);
                final short key = record.getShort(columnIndex);
                MapValue value = map.createValueWithKey(key);
                if (!value.isNew()) {
                    functionUpdater.updateExisting(value, record, baseRowId);
                } else {
                    functionUpdater.updateNew(value, record, baseRowId);
                }
                for (long r = 1; r < frameRowCount; r++) {
                    record.setRowIndex(r);
                    functionUpdater.updateExisting(value, record, baseRowId + r);
                }
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
            if (!value.isNew()) {
                functionUpdater.updateExisting(value, record, baseRowId + r);
            } else {
                functionUpdater.updateNew(value, record, baseRowId + r);
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
            if (!shardValue.isNew()) {
                functionUpdater.updateExisting(shardValue, record, baseRowId + r);
            } else {
                functionUpdater.updateNew(shardValue, record, baseRowId + r);
            }
        }
    }

    private static void aggregateShardedIntKey(
            PageFrameMemory frameMemory,
            PageFrameMemoryRecord record,
            long frameRowCount,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            AsyncGroupByAtom.MapFragment fragment,
            int columnIndex
    ) {
        assert columnIndex != -1;
        long addr = frameMemory.getPageAddress(columnIndex);
        if (addr != 0) {
            for (long r = 0; r < frameRowCount; r++, addr += 4) {
                final int key = Unsafe.getUnsafe().getInt(addr);
                final long hashCode = Unordered4Map.hashKey(key);
                final Unordered4Map shard = (Unordered4Map) fragment.getShardMap(hashCode);

                MapValue shardValue = shard.createValueWithKey(key, hashCode);
                record.setRowIndex(r);
                if (!shardValue.isNew()) {
                    functionUpdater.updateExisting(shardValue, record, baseRowId + r);
                } else {
                    functionUpdater.updateNew(shardValue, record, baseRowId + r);
                }
            }
        } else { // column top
            if (frameRowCount > 0) {
                record.setRowIndex(0);
                final int key = record.getInt(columnIndex);
                final long hashCode = Unordered4Map.hashKey(key);
                final Unordered4Map shard = (Unordered4Map) fragment.getShardMap(hashCode);
                MapValue shardValue = shard.createValueWithKey(key, hashCode);
                if (!shardValue.isNew()) {
                    functionUpdater.updateExisting(shardValue, record, baseRowId);
                } else {
                    functionUpdater.updateNew(shardValue, record, baseRowId);
                }
                for (long r = 1; r < frameRowCount; r++) {
                    record.setRowIndex(r);
                    functionUpdater.updateExisting(shardValue, record, baseRowId + r);
                }
            }
        }
    }

    private static void aggregateShardedLongKey(
            PageFrameMemory frameMemory,
            PageFrameMemoryRecord record,
            long frameRowCount,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            AsyncGroupByAtom.MapFragment fragment,
            int columnIndex
    ) {
        assert columnIndex != -1;
        long addr = frameMemory.getPageAddress(columnIndex);
        if (addr != 0) {
            for (long r = 0; r < frameRowCount; r++, addr += 8) {
                final long key = Unsafe.getUnsafe().getLong(addr);
                final long hashCode = Unordered8Map.hashKey(key);
                final Unordered8Map shard = (Unordered8Map) fragment.getShardMap(hashCode);

                MapValue shardValue = shard.createValueWithKey(key, hashCode);
                record.setRowIndex(r);
                if (!shardValue.isNew()) {
                    functionUpdater.updateExisting(shardValue, record, baseRowId + r);
                } else {
                    functionUpdater.updateNew(shardValue, record, baseRowId + r);
                }
            }
        } else { // column top
            if (frameRowCount > 0) {
                record.setRowIndex(0);
                final long key = record.getLong(columnIndex);
                final long hashCode = Unordered8Map.hashKey(key);
                final Unordered8Map shard = (Unordered8Map) fragment.getShardMap(hashCode);
                MapValue shardValue = shard.createValueWithKey(key, hashCode);
                if (!shardValue.isNew()) {
                    functionUpdater.updateExisting(shardValue, record, baseRowId);
                } else {
                    functionUpdater.updateNew(shardValue, record, baseRowId);
                }
                for (long r = 1; r < frameRowCount; r++) {
                    record.setRowIndex(r);
                    functionUpdater.updateExisting(shardValue, record, baseRowId + r);
                }
            }
        }
    }

    private static void aggregateShardedShortKey(
            PageFrameMemory frameMemory,
            PageFrameMemoryRecord record,
            long frameRowCount,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            AsyncGroupByAtom.MapFragment fragment,
            int columnIndex
    ) {
        assert columnIndex != -1;
        long addr = frameMemory.getPageAddress(columnIndex);
        if (addr != 0) {
            for (long r = 0; r < frameRowCount; r++, addr += 2) {
                final short key = Unsafe.getUnsafe().getShort(addr);
                final long hashCode = Unordered2Map.hashKey(key);
                final Unordered2Map shard = (Unordered2Map) fragment.getShardMap(hashCode);

                MapValue shardValue = shard.createValueWithKey(key);
                record.setRowIndex(r);
                if (!shardValue.isNew()) {
                    functionUpdater.updateExisting(shardValue, record, baseRowId + r);
                } else {
                    functionUpdater.updateNew(shardValue, record, baseRowId + r);
                }
            }
        } else { // column top
            if (frameRowCount > 0) {
                record.setRowIndex(0);
                final short key = record.getShort(columnIndex);
                final long hashCode = Unordered2Map.hashKey(key);
                final Unordered2Map shard = (Unordered2Map) fragment.getShardMap(hashCode);
                MapValue shardValue = shard.createValueWithKey(key);
                if (!shardValue.isNew()) {
                    functionUpdater.updateExisting(shardValue, record, baseRowId);
                } else {
                    functionUpdater.updateNew(shardValue, record, baseRowId);
                }
                for (long r = 1; r < frameRowCount; r++) {
                    record.setRowIndex(r);
                    functionUpdater.updateExisting(shardValue, record, baseRowId + r);
                }
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
            if (!shardValue.isNew()) {
                functionUpdater.updateExisting(shardValue, record, baseRowId + r);
            } else {
                functionUpdater.updateNew(shardValue, record, baseRowId + r);
            }
        }
    }
}
