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

package io.questdb.griffin.engine.functions.window;

import io.questdb.cairo.RecordSink;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import org.jetbrains.annotations.Nullable;

public abstract class BasePartitionedBivariateWindowFunction extends BaseBivariateWindowFunction implements Reopenable {
    // Reusable second partition-state Map for the frontier sweep; ping-pongs with
    // map so a sweep never allocates. See BasePartitionedWindowFunction.
    protected Map compactionScratch;
    // Non-final so retainPartitions can swap the Map instance. Single-writer
    // (refresh worker), no synchronization needed.
    protected Map map;
    // The per-query MemoryTracker bound by setMemoryTracker. Retained so
    // retainPartitions can bind it on the lazily-created compaction scratch too.
    // See BasePartitionedWindowFunction.
    protected MemoryTracker memoryTracker;
    protected final VirtualRecord partitionByRecord;
    protected final RecordSink partitionBySink;
    // Live-view tombstone bookkeeping; mirrors BasePartitionedWindowFunction.
    // Subclasses set tombstoneValueIndex in their constructor.
    protected int tombstoneValueIndex = -1;
    protected long tombstoneCount;

    public BasePartitionedBivariateWindowFunction(
            Map map,
            VirtualRecord partitionByRecord,
            RecordSink partitionBySink,
            Function argY,
            Function argX
    ) {
        super(argY, argX);
        this.map = map;
        this.partitionByRecord = partitionByRecord;
        this.partitionBySink = partitionBySink;
        // Start the map closed (lazy), matching the openOnInit=false pattern used
        // elsewhere for tracker-aware state: the owning cursor binds a per-query
        // MemoryTracker via setMemoryTracker() and reopen() then allocates the backing
        // under it, with reset() freeing it at cursor close, symmetric on the
        // per-query counter. Direct callers (e.g. unit tests) must reopen() before use.
        if (map != null) {
            map.close();
        }
    }

    @Override
    public void close() {
        super.close();
        Misc.free(map);
        Misc.free(compactionScratch);
        Misc.freeObjList(partitionByRecord.getFunctions());
    }

    @Override
    public long getTombstoneCount() {
        return tombstoneCount;
    }

    @Override
    public int getTombstoneValueIndex() {
        return tombstoneValueIndex;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        super.init(symbolTableSource, executionContext);
        Function.init(partitionByRecord.getFunctions(), symbolTableSource, executionContext, null);
    }

    @Override
    public void initPartitionBy(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        super.initPartitionBy(symbolTableSource, executionContext);
        Function.init(partitionByRecord.getFunctions(), symbolTableSource, executionContext, null);
    }

    @Override
    public void markPartitionAlive(Record record) {
        if (tombstoneValueIndex < 0 || tombstoneCount == 0) {
            return;
        }
        partitionByRecord.of(record);
        MapKey key = map.withKey();
        key.put(partitionByRecord, partitionBySink);
        MapValue value = key.findValue();
        if (value != null && value.getByte(tombstoneValueIndex) == 1) {
            value.putByte(tombstoneValueIndex, (byte) 0);
            tombstoneCount--;
        }
    }

    /**
     * Empties the partition-state map and zeroes the tombstone counter before the
     * live-view snapshot framework rehydrates partitions. Mirrors
     * {@link BasePartitionedWindowFunction#onCheckpointRestoreBegin()}.
     */
    @Override
    public void onCheckpointRestoreBegin() {
        Misc.clear(map);
        tombstoneCount = 0;
    }

    @Override
    public void reopen() {
        if (map != null) {
            map.reopen();
        }
        tombstoneCount = 0;
    }

    @Override
    public void reset() {
        Misc.free(map);
        compactionScratch = Misc.free(compactionScratch);
        tombstoneCount = 0;
    }

    @Override
    public void retainPartitions(Map survivingKeys, RecordSink survivingKeySink) {
        if (compactionScratch == null) {
            compactionScratch = newCompactionScratch();
            if (compactionScratch == null) {
                return;
            }
            bindScratchTracker();
        } else {
            compactionScratch.clear();
        }
        PartitionStateEvictor.rebuildKeepingMembers(map, compactionScratch, survivingKeys, survivingKeySink);
        Map old = map;
        map = compactionScratch;
        compactionScratch = old;
        tombstoneCount = 0;
    }

    @Override
    public void setMemoryTracker(@Nullable MemoryTracker tracker) {
        // Retain the tracker so retainPartitions can charge the lazily-created
        // compaction scratch to it. The live map (which may itself be a scratch
        // promoted by a prior swap) is tracked here directly.
        this.memoryTracker = tracker;
        if (map != null) {
            map.setMemoryTracker(tracker);
        }
    }

    @Override
    public void toTop() {
        super.toTop();
        Misc.clear(map);
        tombstoneCount = 0;
    }

    /**
     * Charges the freshly created compaction scratch to the per-query tracker.
     * Mirrors {@link BasePartitionedWindowFunction#bindScratchTracker()}: free the
     * untracked open backing, bind the tracker, then reopen so the scratch's malloc
     * and free stay symmetric on the per-query counter after the ping-pong swap. A
     * no-op when no tracker is bound.
     */
    private void bindScratchTracker() {
        if (memoryTracker == null || compactionScratch == null) {
            return;
        }
        compactionScratch.close();
        compactionScratch.setMemoryTracker(memoryTracker);
        compactionScratch.reopen();
    }

    /**
     * Mirrors {@link BasePartitionedWindowFunction#newCompactionScratch()}: a fresh
     * empty Map with this function's layout, or {@code null} to opt out of the
     * live-view frontier sweep.
     */
    protected Map newCompactionScratch() {
        return null;
    }
}
