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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameReducer;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.DirectLongList;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.griffin.engine.table.AsyncFilterUtils.applyCompiledFilter;
import static io.questdb.griffin.engine.table.AsyncFilterUtils.applyFilter;

public class AsyncGroupByFilteredReducer implements PageFrameReducer {
    private final int singleColumnIndex;

    public AsyncGroupByFilteredReducer(int singleColumnIndex) {
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
        final DirectLongList rows = task.getFilteredRows();
        final PageFrameSequence<AsyncGroupByAtom> frameSequence = task.getFrameSequence(AsyncGroupByAtom.class);

        final PageFrameMemory frameMemory = task.populateFrameMemory();
        record.init(frameMemory);

        rows.clear();

        final long frameRowCount = task.getFrameRowCount();
        assert frameRowCount > 0;
        final AsyncGroupByAtom atom = frameSequence.getAtom();

        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == frameSequence;
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);
        final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
        final AsyncGroupByAtom.MapFragment fragment = atom.getFragment(slotId);
        final CompiledFilter compiledFilter = atom.getCompiledFilter();
        final Function filter = atom.getFilter(slotId);
        final RecordSink mapSink = atom.getMapSink(slotId);
        try {
            if (compiledFilter == null || frameMemory.hasColumnTops()) {
                // Use Java-based filter when there is no compiled filter or in case of a page frame with column tops.
                applyFilter(filter, rows, record, frameRowCount);
            } else {
                applyCompiledFilter(compiledFilter, atom.getBindVarMemory(), atom.getBindVarFunctions(), task);
            }

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
                    aggregateNonShardedVarcharKey(record, rows, baseRowId, functionUpdater, fragment, singleColumnIndex);
                } else if (map instanceof Unordered2Map) {
                    aggregateNonShardedShortKey(frameMemory, record, rows, baseRowId, functionUpdater, fragment, singleColumnIndex);
                } else if (map instanceof Unordered4Map) {
                    aggregateNonShardedIntKey(frameMemory, record, rows, baseRowId, functionUpdater, fragment, singleColumnIndex);
                } else if (map instanceof Unordered8Map) {
                    aggregateNonShardedLongKey(frameMemory, record, rows, baseRowId, functionUpdater, fragment, singleColumnIndex);
                } else {
                    aggregateNonShardedGeneric(record, rows, baseRowId, functionUpdater, fragment, mapSink);
                }
            } else {
                if (map instanceof UnorderedVarcharMap) {
                    aggregateShardedVarcharKey(record, rows, baseRowId, functionUpdater, fragment, singleColumnIndex);
                } else if (map instanceof Unordered2Map) {
                    aggregateShardedShortKey(frameMemory, record, rows, baseRowId, functionUpdater, fragment, singleColumnIndex);
                } else if (map instanceof Unordered4Map) {
                    aggregateShardedIntKey(frameMemory, record, rows, baseRowId, functionUpdater, fragment, singleColumnIndex);
                } else if (map instanceof Unordered8Map) {
                    aggregateShardedLongKey(frameMemory, record, rows, baseRowId, functionUpdater, fragment, singleColumnIndex);
                } else {
                    aggregateShardedGeneric(record, rows, baseRowId, functionUpdater, fragment, mapSink);
                }
            }

            atom.maybeEnableSharding(fragment);
        } finally {
            atom.release(slotId);
        }
    }

    private static void aggregateNonShardedGeneric(
            PageFrameMemoryRecord record,
            DirectLongList rows,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            AsyncGroupByAtom.MapFragment fragment,
            RecordSink mapSink
    ) {
        final Map map = fragment.reopenMap();
        for (long p = 0, n = rows.size(); p < n; p++) {
            long r = rows.get(p);
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
            DirectLongList rows,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            AsyncGroupByAtom.MapFragment fragment,
            int columnIndex
    ) {
        assert columnIndex != -1;
        final Unordered4Map map = (Unordered4Map) fragment.reopenMap();
        final long addr = frameMemory.getPageAddress(columnIndex);
        if (addr != 0) {
            for (long i = 0, n = rows.size(); i < n; i++) {
                final long r = rows.get(i);
                final int key = Unsafe.getUnsafe().getInt(addr + (r << 2));
                MapValue value = map.createValueWithKey(key);
                record.setRowIndex(r);
                if (!value.isNew()) {
                    functionUpdater.updateExisting(value, record, baseRowId + r);
                } else {
                    functionUpdater.updateNew(value, record, baseRowId + r);
                }
            }
        } else { // column top
            if (rows.size() > 0) {
                long r = rows.get(0);
                record.setRowIndex(r);
                final int key = record.getInt(columnIndex);
                MapValue value = map.createValueWithKey(key);
                if (!value.isNew()) {
                    functionUpdater.updateExisting(value, record, baseRowId + r);
                } else {
                    functionUpdater.updateNew(value, record, baseRowId + r);
                }
                for (long i = 1, n = rows.size(); i < n; i++) {
                    r = rows.get(i);
                    record.setRowIndex(r);
                    functionUpdater.updateExisting(value, record, baseRowId + r);
                }
            }
        }
    }

    private static void aggregateNonShardedLongKey(
            PageFrameMemory frameMemory,
            PageFrameMemoryRecord record,
            DirectLongList rows,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            AsyncGroupByAtom.MapFragment fragment,
            int columnIndex
    ) {
        assert columnIndex != -1;
        final Unordered8Map map = (Unordered8Map) fragment.reopenMap();
        final long addr = frameMemory.getPageAddress(columnIndex);
        if (addr != 0) {
            for (long i = 0, n = rows.size(); i < n; i++) {
                final long r = rows.get(i);
                final long key = Unsafe.getUnsafe().getLong(addr + (r << 3));
                MapValue value = map.createValueWithKey(key);
                record.setRowIndex(r);
                if (!value.isNew()) {
                    functionUpdater.updateExisting(value, record, baseRowId + r);
                } else {
                    functionUpdater.updateNew(value, record, baseRowId + r);
                }
            }
        } else { // column top
            if (rows.size() > 0) {
                long r = rows.get(0);
                record.setRowIndex(r);
                final long key = record.getLong(columnIndex);
                MapValue value = map.createValueWithKey(key);
                if (!value.isNew()) {
                    functionUpdater.updateExisting(value, record, baseRowId + r);
                } else {
                    functionUpdater.updateNew(value, record, baseRowId + r);
                }
                for (long i = 1, n = rows.size(); i < n; i++) {
                    r = rows.get(i);
                    record.setRowIndex(r);
                    functionUpdater.updateExisting(value, record, baseRowId + r);
                }
            }
        }
    }

    private static void aggregateNonShardedShortKey(
            PageFrameMemory frameMemory,
            PageFrameMemoryRecord record,
            DirectLongList rows,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            AsyncGroupByAtom.MapFragment fragment,
            int columnIndex
    ) {
        assert columnIndex != -1;
        final Unordered2Map map = (Unordered2Map) fragment.reopenMap();
        final long addr = frameMemory.getPageAddress(columnIndex);
        if (addr != 0) {
            for (long i = 0, n = rows.size(); i < n; i++) {
                final long r = rows.get(i);
                final short key = Unsafe.getUnsafe().getShort(addr + (r << 1));
                MapValue value = map.createValueWithKey(key);
                record.setRowIndex(r);
                if (!value.isNew()) {
                    functionUpdater.updateExisting(value, record, baseRowId + r);
                } else {
                    functionUpdater.updateNew(value, record, baseRowId + r);
                }
            }
        } else { // column top
            if (rows.size() > 0) {
                long r = rows.get(0);
                record.setRowIndex(r);
                final short key = record.getShort(columnIndex);
                MapValue value = map.createValueWithKey(key);
                if (!value.isNew()) {
                    functionUpdater.updateExisting(value, record, baseRowId + r);
                } else {
                    functionUpdater.updateNew(value, record, baseRowId + r);
                }
                for (long i = 1, n = rows.size(); i < n; i++) {
                    r = rows.get(i);
                    record.setRowIndex(r);
                    functionUpdater.updateExisting(value, record, baseRowId + r);
                }
            }
        }
    }

    private static void aggregateNonShardedVarcharKey(
            PageFrameMemoryRecord record,
            DirectLongList rows,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            AsyncGroupByAtom.MapFragment fragment,
            int columnIndex
    ) {
        assert columnIndex != -1;
        final UnorderedVarcharMap map = (UnorderedVarcharMap) fragment.reopenMap();
        for (long i = 0, n = rows.size(); i < n; i++) {
            final long r = rows.get(i);
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
            DirectLongList rows,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            AsyncGroupByAtom.MapFragment fragment,
            RecordSink mapSink
    ) {
        // The first map is used to write keys.
        final Map lookupShard = fragment.getShards().getQuick(0);
        for (long p = 0, n = rows.size(); p < n; p++) {
            long r = rows.get(p);
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
            DirectLongList rows,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            AsyncGroupByAtom.MapFragment fragment,
            int columnIndex
    ) {
        assert columnIndex != -1;
        final long addr = frameMemory.getPageAddress(columnIndex);
        if (addr != 0) {
            for (long i = 0, n = rows.size(); i < n; i++) {
                final long r = rows.get(i);
                final int key = Unsafe.getUnsafe().getInt(addr + (r << 2));
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
            if (rows.size() > 0) {
                long r = rows.get(0);
                record.setRowIndex(r);
                final int key = record.getInt(columnIndex);
                final long hashCode = Unordered4Map.hashKey(key);
                final Unordered4Map shard = (Unordered4Map) fragment.getShardMap(hashCode);
                final MapValue shardValue = shard.createValueWithKey(key, hashCode);
                record.setRowIndex(r);
                if (!shardValue.isNew()) {
                    functionUpdater.updateExisting(shardValue, record, baseRowId + r);
                } else {
                    functionUpdater.updateNew(shardValue, record, baseRowId + r);
                }
                for (long i = 1, n = rows.size(); i < n; i++) {
                    r = rows.get(i);
                    record.setRowIndex(r);
                    functionUpdater.updateExisting(shardValue, record, baseRowId + r);
                }
            }
        }
    }

    private static void aggregateShardedLongKey(
            PageFrameMemory frameMemory,
            PageFrameMemoryRecord record,
            DirectLongList rows,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            AsyncGroupByAtom.MapFragment fragment,
            int columnIndex
    ) {
        assert columnIndex != -1;
        final long addr = frameMemory.getPageAddress(columnIndex);
        if (addr != 0) {
            for (long i = 0, n = rows.size(); i < n; i++) {
                final long r = rows.get(i);
                final long key = Unsafe.getUnsafe().getLong(addr + (r << 3));
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
            if (rows.size() > 0) {
                long r = rows.get(0);
                record.setRowIndex(r);
                final long key = record.getLong(columnIndex);
                final long hashCode = Unordered8Map.hashKey(key);
                final Unordered8Map shard = (Unordered8Map) fragment.getShardMap(hashCode);
                final MapValue shardValue = shard.createValueWithKey(key, hashCode);
                record.setRowIndex(r);
                if (!shardValue.isNew()) {
                    functionUpdater.updateExisting(shardValue, record, baseRowId + r);
                } else {
                    functionUpdater.updateNew(shardValue, record, baseRowId + r);
                }
                for (long i = 1, n = rows.size(); i < n; i++) {
                    r = rows.get(i);
                    record.setRowIndex(r);
                    functionUpdater.updateExisting(shardValue, record, baseRowId + r);
                }
            }
        }
    }

    private static void aggregateShardedShortKey(
            PageFrameMemory frameMemory,
            PageFrameMemoryRecord record,
            DirectLongList rows,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            AsyncGroupByAtom.MapFragment fragment,
            int columnIndex
    ) {
        assert columnIndex != -1;
        final long addr = frameMemory.getPageAddress(columnIndex);
        if (addr != 0) {
            for (long i = 0, n = rows.size(); i < n; i++) {
                final long r = rows.get(i);
                final short key = Unsafe.getUnsafe().getShort(addr + (r << 1));
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
            if (rows.size() > 0) {
                long r = rows.get(0);
                record.setRowIndex(r);
                final short key = record.getShort(columnIndex);
                final long hashCode = Unordered2Map.hashKey(key);
                final Unordered2Map shard = (Unordered2Map) fragment.getShardMap(hashCode);
                final MapValue shardValue = shard.createValueWithKey(key);
                record.setRowIndex(r);
                if (!shardValue.isNew()) {
                    functionUpdater.updateExisting(shardValue, record, baseRowId + r);
                } else {
                    functionUpdater.updateNew(shardValue, record, baseRowId + r);
                }
                for (long i = 1, n = rows.size(); i < n; i++) {
                    r = rows.get(i);
                    record.setRowIndex(r);
                    functionUpdater.updateExisting(shardValue, record, baseRowId + r);
                }
            }
        }
    }

    private static void aggregateShardedVarcharKey(
            PageFrameMemoryRecord record,
            DirectLongList rows,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            AsyncGroupByAtom.MapFragment fragment,
            int columnIndex
    ) {
        assert columnIndex != -1;
        // The first map is used to write keys.
        final Map lookupShard = fragment.getShards().getQuick(0);
        for (long i = 0, n = rows.size(); i < n; i++) {
            final long r = rows.get(i);
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
