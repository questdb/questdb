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
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;

public abstract class BasePartitionedWindowFunction extends BaseWindowFunction implements Reopenable {
    protected final VirtualRecord partitionByRecord;
    protected final RecordSink partitionBySink;
    // Reusable second partition-state Map for the frontier sweep. Allocated once
    // (lazily, via newCompactionScratch) the first time retainPartitions runs, then
    // cleared and reused on every subsequent sweep -- the two maps ping-pong so a
    // sweep never allocates. Null until the first sweep, or for functions that opt
    // out (newCompactionScratch returns null).
    protected Map compactionScratch;
    // Non-final so retainPartitions can swap the partition state Map during
    // the anchor-driven frontier sweep.
    protected Map map;
    // Live-view tombstone bookkeeping. Subclasses set tombstoneValueIndex in
    // their constructor (= the BYTE slot index in the partition state map's
    // value layout); -1 means "no tombstone tracking" (non-LV mode or
    // function not yet migrated). tombstoneCount tracks the number of
    // tombstoned entries in this function's map. markPartitionAlive reads it
    // for its hot-path early-exit; retainPartitions resets it after a sweep.
    // Single-writer (refresh worker), not volatile.
    protected int tombstoneValueIndex = -1;
    protected long tombstoneCount;

    public BasePartitionedWindowFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
        super(arg);
        this.map = map;
        this.partitionByRecord = partitionByRecord;
        this.partitionBySink = partitionBySink;
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
        Function.init(partitionByRecord.getFunctions(), symbolTableSource, executionContext, null);
    }

    /**
     * Generic markPartitionAlive impl shared across every partitioned window
     * function that carries a tombstone bit. The hot-path early-exit
     * (tombstoneCount == 0) keeps the per-row overhead to a single field load
     * plus a predicted-not-taken branch in steady state. The Map lookup only
     * fires when at least one tombstoned entry exists, which means
     * processRow saw an anchor cross on some partition in the recent past.
     * <p>
     * Subclasses that need to clear additional per-partition scratch state
     * may override; most do not.
     */
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
     * live-view snapshot framework rehydrates partitions. Native-memory-backed
     * subclasses (ring/deque functions) override to also reset their backing arena
     * and free list, calling {@code super.onSnapshotRestoreBegin()}.
     */
    @Override
    public void onSnapshotRestoreBegin() {
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
    public void retainPartitions(Map survivingKeys) {
        if (compactionScratch == null) {
            // First sweep: allocate the reusable second map once. A null factory
            // result means the function opts out of frontier compaction; its map
            // keeps every partition (still correct -- a behind-frontier partition
            // that revives does so in a new bucket and resetPartition zeroes it).
            compactionScratch = newCompactionScratch();
            if (compactionScratch == null) {
                return;
            }
        } else {
            // Discard the previous sweep's old map (held here as scratch) before
            // reuse. Clearing up front -- rather than after the swap -- keeps the
            // scratch consistent even if a prior sweep threw mid-rebuild.
            compactionScratch.clear();
        }
        PartitionStateEvictor.rebuildKeepingMembers(map, compactionScratch, survivingKeys);
        // Ping-pong: the rebuilt scratch becomes the live map; the old live map
        // becomes the scratch for the next sweep. No allocation, no free.
        Map old = map;
        map = compactionScratch;
        compactionScratch = old;
        tombstoneCount = 0;
    }

    @Override
    public void reset() {
        Misc.free(map);
        compactionScratch = Misc.free(compactionScratch);
        tombstoneCount = 0;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val(getName());
        if (arg != null) {
            sink.val('(').val(arg).val(')');
        } else {
            sink.val("(*)");
        }
        if (isIgnoreNulls()) {
            sink.val(" ignore nulls");
        }
        sink.val(" over (");
        sink.val("partition by ");
        sink.val(partitionByRecord.getFunctions());
        sink.val(')');
    }

    @Override
    public void toTop() {
        super.toTop();
        Misc.clear(map);
        tombstoneCount = 0;
    }

    /**
     * Returns a fresh, empty partition-state {@link Map} with this function's exact
     * key/value layout, or {@code null} to opt out of the live-view frontier sweep.
     * Anchored (UNBOUNDED PRECEDING ... CURRENT ROW) functions override this so
     * {@link #retainPartitions(Map)} can rebuild the map keeping only the partitions
     * the anchor map kept. The default {@code null} leaves the map untouched.
     */
    protected Map newCompactionScratch() {
        return null;
    }
}
