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
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.Nullable;

public abstract class BasePartitionedWindowFunction extends BaseWindowFunction implements Reopenable {
    protected final VirtualRecord partitionByRecord;
    protected final RecordSink partitionBySink;
    // Generation of the checkpoint root checkpointLogicalStateBytes and
    // checkpointDirtyPartitions are relative to. LONG_NULL until the first seal
    // publishes; a repair, truncate or compaction moves the timeline's generation
    // past it without this function having produced the new root, and the mismatch
    // is what keeps the next seal off the incremental path.
    protected long checkpointBaselineGeneration = Numbers.LONG_NULL;
    // Deduplicated partition keys touched since the last durable checkpoint. One
    // entry per distinct key, so the footprint scales with the checkpoint cadence
    // rather than with the batch: raising
    // cairo.live.view.checkpoint.max.duration.micros trades seal cost for both
    // latency and memory, charged to cairo.live.view.refresh.memory.limit.bytes. A
    // view whose max timestamp stops advancing never publishes and grows the map
    // until the tracker trips.
    protected Map checkpointDirtyPartitions;
    protected long checkpointLogicalStateBytes;
    // Reusable second partition-state Map for the frontier sweep. Allocated once
    // (lazily, via newCompactionScratch) the first time retainPartitions runs, then
    // cleared and reused on every subsequent sweep -- the two maps ping-pong so a
    // sweep never allocates. Null until the first sweep, or for functions that opt
    // out (newCompactionScratch returns null).
    protected Map compactionScratch;
    // True once a sweep has put evicted keys into checkpointDirtyPartitions and the seal
    // has not consumed them yet. What it decides is whether dropping the dirty set also
    // hands the backing memory back - see clearCheckpointDirtyPartitions.
    protected boolean hasCheckpointEvictionsRecorded;
    protected boolean isCheckpointFullScanRequired = true;
    // Non-final so retainPartitions can swap the partition state Map during
    // the anchor-driven frontier sweep.
    protected Map map;
    // The per-query MemoryTracker bound by setMemoryTracker. Retained so
    // retainPartitions can bind it on the lazily-created compaction scratch too,
    // and so the binding survives the ping-pong swap that promotes the scratch to
    // the live map. Null until the owning cursor binds one (or for direct callers).
    protected MemoryTracker memoryTracker;
    // Live-view tombstone bookkeeping. Subclasses set tombstoneValueIndex in
    // their constructor (= the BYTE slot index in the partition state map's
    // value layout); -1 means "no tombstone tracking" (non-LV mode or
    // function not yet migrated). tombstoneCount tracks the number of
    // tombstoned entries in this function's map. markPartitionAlive reads it
    // for its hot-path early-exit; retainPartitions resets it after a sweep.
    // Single-writer (refresh worker), not volatile.
    protected long tombstoneCount;
    protected int tombstoneValueIndex = -1;
    // The fused window-state slots this function reads out of LiveViewWindow's one map
    // value, or -1 when it owns its state as it always has. The counter is present in
    // every accumulator family a plan admits, so it doubles as the "am I fused" answer;
    // the sum is -1 for a bare-counter component. Installed by the plan through
    // bindWindowStateSlots and cleared the same way, both on the refresh worker.
    protected int windowStateNonNullCountSlot = -1;
    protected int windowStateSumSlot = -1;

    public BasePartitionedWindowFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
        super(arg);
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
    public void bindWindowStateSlots(int sumSlot, int nonNullCountSlot) {
        this.windowStateSumSlot = sumSlot;
        this.windowStateNonNullCountSlot = nonNullCountSlot;
    }

    @Override
    public void close() {
        super.close();
        Misc.free(map);
        Misc.free(compactionScratch);
        Misc.free(checkpointDirtyPartitions);
        Misc.freeObjList(partitionByRecord.getFunctions());
    }

    @Override
    public long getCheckpointBaselineGeneration() {
        return checkpointBaselineGeneration;
    }

    @Override
    public Map getCheckpointDirtyPartitionMap() {
        return checkpointDirtyPartitions;
    }

    @Override
    public long getCheckpointLogicalStateBytes() {
        return checkpointLogicalStateBytes;
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
    public boolean isCheckpointFullScanRequired() {
        return isCheckpointFullScanRequired;
    }

    @Override
    public boolean isWindowStateOwned() {
        return windowStateNonNullCountSlot >= 0;
    }

    /**
     * Records the sweep's eviction of {@code record}'s partition in the same dirty map
     * the ordinary marking writes to, reusing the state layout's tombstone slot as the
     * per-key eviction marker. Per-key is what makes the seal's relaxed
     * missing-live-value branch safe: a dirty key that lost its state to anything other
     * than this sweep still carries a {@code 0} there and still raises.
     * <p>
     * Declines - returning false - when the function tracks no tombstone slot or opts out
     * of the scratch map, which is exactly the population that never freezes incrementally
     * anyway.
     */
    @Override
    public boolean markCheckpointPartitionEvicted(Record record, RecordSink keySink) {
        if (isWindowStateOwned()) {
            // The window holds this function's state in its own map, so the sweep drops
            // the accumulator by dropping the fused entry and records the removal in the
            // window's one dirty set. There is nothing of this function's left to record,
            // and true is the honest answer: the removal is tracked, just not here.
            return true;
        }
        if (tombstoneValueIndex < 0) {
            return false;
        }
        hasCheckpointEvictionsRecorded = true;
        if (checkpointDirtyPartitions == null) {
            checkpointDirtyPartitions = newCompactionScratch();
            if (checkpointDirtyPartitions == null) {
                return false;
            }
            if (memoryTracker != null) {
                checkpointDirtyPartitions.close();
                checkpointDirtyPartitions.setMemoryTracker(memoryTracker);
                checkpointDirtyPartitions.reopen();
            }
        }
        // The key columns come off the anchor map's own record, so this writes through
        // the caller's sink rather than partitionBySink.
        final MapKey key = checkpointDirtyPartitions.withKey();
        key.put(record, keySink);
        key.createValue().putByte(tombstoneValueIndex, (byte) 1);
        return true;
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
     * may override; most do not. An override that keeps the checkpoint dirty
     * tracking must call {@link #markCheckpointPartitionDirty(Record)} on every
     * path through the method - see that method's contract.
     */
    @Override
    public void markPartitionAlive(Record record) {
        if (isWindowStateOwned()) {
            // Nothing of this function's is tombstoned or marked dirty any more: the
            // window loads the one value this row touches, keeps it alive and marks it
            // once, for the group.
            return;
        }
        markCheckpointPartitionDirty(record);
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

    @Override
    public void onCheckpointPersisted(long logicalStateBytes, long generation) {
        checkpointBaselineGeneration = generation;
        checkpointLogicalStateBytes = logicalStateBytes;
        isCheckpointFullScanRequired = false;
        clearCheckpointDirtyPartitions();
    }

    /**
     * Empties the partition-state map and zeroes the tombstone counter before the
     * live-view snapshot framework rehydrates partitions. Native-memory-backed
     * subclasses (ring/deque functions) override to also reset their backing arena
     * and free list, calling {@code super.onCheckpointRestoreBegin()}.
     */
    @Override
    public void onCheckpointRestoreBegin() {
        if (map != null) {
            // On a fresh restart the lazy per-partition map is still closed: the
            // live-view restore path (restoreFromHead) runs before any cursor
            // of()/ofIncremental reopens it. reopen() allocates the backing when
            // closed and is a no-op when already open, so restoreCheckpointState's
            // createValue() always has a live map. The subsequent first
            // ofIncremental reopen() is then a no-op and preserves this state.
            map.reopen();
            map.clear();
        }
        tombstoneCount = 0;
        checkpointBaselineGeneration = Numbers.LONG_NULL;
        checkpointLogicalStateBytes = 0;
        isCheckpointFullScanRequired = true;
        clearCheckpointDirtyPartitions();
    }

    @Override
    public void reopen() {
        // A fused function's map stays closed: the window allocated one value layout for
        // the whole group and reopening this one would charge the per-view tracker for a
        // map no row ever writes to. The legacy-checkpoint adapter is the one path that
        // reopens it, and it closes it again as soon as it has hoisted the state across.
        if (map != null && !isWindowStateOwned()) {
            map.reopen();
        }
        tombstoneCount = 0;
    }

    @Override
    public void retainPartitions(Map survivingKeys, RecordSink survivingKeySink) {
        // Every caller other than the frontier sweep removes keys without naming them,
        // so it gets the conservative complete freeze.
        retainPartitions(survivingKeys, survivingKeySink, false);
    }

    @Override
    public void retainPartitions(
            Map survivingKeys,
            RecordSink survivingKeySink,
            boolean checkpointRemovalsRecorded
    ) {
        if (isWindowStateOwned()) {
            // The sweep rebuilt the window's fused map, and this function's accumulator
            // rode across inside the entries it kept. There is no second map to prune.
            return;
        }
        if (!checkpointRemovalsRecorded) {
            // The removals are nowhere the seal can read them, so only a complete freeze
            // finds the keys the root still holds and this map no longer does.
            checkpointBaselineGeneration = Numbers.LONG_NULL;
            isCheckpointFullScanRequired = true;
        }
        if (compactionScratch == null) {
            // First sweep: allocate the reusable second map once. A null factory
            // result means the function opts out of frontier compaction; its map
            // keeps every partition (still correct -- a behind-frontier partition
            // that revives does so in a new bucket and resetPartition zeroes it).
            compactionScratch = newCompactionScratch();
            if (compactionScratch == null) {
                return;
            }
            bindScratchTracker();
        } else {
            // Discard the previous sweep's old map (held here as scratch) before
            // reuse. Clearing up front -- rather than after the swap -- keeps the
            // scratch consistent even if a prior sweep threw mid-rebuild.
            compactionScratch.clear();
        }
        PartitionStateEvictor.rebuildKeepingMembers(map, compactionScratch, survivingKeys, survivingKeySink);
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
        checkpointDirtyPartitions = Misc.free(checkpointDirtyPartitions);
        hasCheckpointEvictionsRecorded = false;
        tombstoneCount = 0;
        checkpointBaselineGeneration = Numbers.LONG_NULL;
        checkpointLogicalStateBytes = 0;
        isCheckpointFullScanRequired = true;
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
        if (checkpointDirtyPartitions != null) {
            checkpointDirtyPartitions.setMemoryTracker(tracker);
        }
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
        // isOpen() rather than a null test: a fused function's map is closed for the
        // whole of its life, and clearing a closed map would walk backing it no longer
        // holds.
        if (map != null && map.isOpen()) {
            map.clear();
        }
        clearCheckpointDirtyPartitions();
        tombstoneCount = 0;
        checkpointBaselineGeneration = Numbers.LONG_NULL;
        checkpointLogicalStateBytes = 0;
        isCheckpointFullScanRequired = true;
    }

    /**
     * Adds the current row's partition to the checkpoint dirty set. The scratch map
     * has the same key layout as the state map, so the existing partition sink can
     * populate it without allocating or serialising a key on every input row.
     * <p>
     * <b>Call this on every path through {@link #markPartitionAlive(Record)} or on
     * none at all.</b> A seal that finds a dirty map freezes exactly the keys it
     * names and leaves every other entry of the persistent root alone, so a key
     * whose state moved without being marked keeps the root's stale image - a wrong
     * result that only surfaces on a restart. Opting out is fail-safe by
     * construction: the map is created here, so a function that never calls this
     * leaves it null, {@link #getCheckpointDirtyPartitionMap()} returns null, and
     * every seal full-scans. There is no correct middle ground, and a partial mark
     * is indistinguishable from a complete one at the seal.
     */
    protected void markCheckpointPartitionDirty(Record record) {
        if (checkpointDirtyPartitions == null) {
            // The value slots this borrows from the state layout are padding - the
            // seal reads only the key back. Reusing newCompactionScratch() is what
            // puts those keys at getCheckpointKeyStartIndex(), which is the index the
            // seal encodes them from; a narrower layout would have to carry that
            // index too, through every one of the subclasses that override the
            // factory.
            checkpointDirtyPartitions = newCompactionScratch();
            if (checkpointDirtyPartitions == null) {
                return;
            }
            if (memoryTracker != null) {
                checkpointDirtyPartitions.close();
                checkpointDirtyPartitions.setMemoryTracker(memoryTracker);
                checkpointDirtyPartitions.reopen();
            }
        }
        partitionByRecord.of(record);
        MapKey key = checkpointDirtyPartitions.withKey();
        key.put(partitionByRecord, partitionBySink);
        final MapValue value = key.createValue();
        if (tombstoneValueIndex >= 0) {
            // Unconditionally, including on an entry that already existed: this row is
            // what turns a key the sweep evicted earlier in the cadence back into an
            // upsert. Writing it on a fresh entry also keeps the marker off whatever
            // bytes the map's backing happened to hold - createValue() zero-fills on
            // no implementation.
            value.putByte(tombstoneValueIndex, (byte) 0);
        }
    }

    /**
     * Charges the freshly created compaction scratch to the per-query tracker.
     * {@link #newCompactionScratch()} returns an OPEN map allocated under no tracker,
     * so - mirroring the deferred lifecycle the constructor gives the primary map -
     * free that untracked backing (still on the null tracker), bind the tracker, then
     * reopen to reallocate under it. This keeps the scratch's malloc and free
     * symmetric on the per-query counter once the ping-pong swap promotes it to the
     * live map. A no-op when no tracker is bound.
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
     * Empties the checkpoint dirty set, handing its backing memory back when the frontier
     * sweep is what grew it.
     * <p>
     * {@link Map#clear()} keeps the capacity, which is what a cadence wants: the dirty set
     * holds roughly the same touched-key count every time, so re-growing it per cadence
     * would be pure churn. A sweep breaks that - it adds one entry per evicted key on top
     * of the touched ones, and the trigger fires only when at least half the anchor map is
     * reclaimable, so the peak is a multiple of the steady state and then stays resident
     * against {@code cairo.live.view.refresh.memory.limit.bytes} for the view's lifetime.
     * {@link Map#restoreInitialCapacity()} is the only primitive that gives it back -
     * {@code setKeyCapacity} grows only - so the sweep-inflated cadence pays a re-grow next
     * time and every other cadence keeps today's behaviour exactly.
     */
    private void clearCheckpointDirtyPartitions() {
        if (checkpointDirtyPartitions == null) {
            return;
        }
        if (hasCheckpointEvictionsRecorded && checkpointDirtyPartitions.isOpen()) {
            checkpointDirtyPartitions.restoreInitialCapacity();
        }
        // Unconditionally, and after the shrink rather than instead of it: OrderedMap's
        // restoreInitialCapacity() clears only as a side effect of actually reallocating,
        // so a map already at its initial capacity would keep every entry and the next
        // seal would freeze the same removals a second time.
        checkpointDirtyPartitions.clear();
        hasCheckpointEvictionsRecorded = false;
    }

    /**
     * Returns a fresh, empty partition-state {@link Map} with this function's exact
     * key/value layout, or {@code null} to opt out of the live-view frontier sweep.
     * Anchored (UNBOUNDED PRECEDING ... CURRENT ROW) functions override this so
     * {@link #retainPartitions(Map, RecordSink)} can rebuild the map keeping only the
     * partitions the anchor map kept. The default {@code null} leaves the map untouched.
     */
    protected Map newCompactionScratch() {
        return null;
    }
}
