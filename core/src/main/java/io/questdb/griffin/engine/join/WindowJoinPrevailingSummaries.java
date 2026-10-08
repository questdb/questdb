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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.std.DirectIntIntHashMap;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Rows;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The prevailing-row backward scan of a window join, shared by every page frame of the master.
 * <p>
 * A window join with INCLUDE PREVAILING needs, per master page frame, the last slave row of each
 * key before the frame's slave span. {@link WindowJoinPrevailingCache} finds it by scanning the
 * slave backwards from the span, which costs the distance back to the key's previous row - for a
 * rare key, a long way - and every page frame repeats it.
 * <p>
 * This class splits the slave's time frames into blocks of consecutive frames and keeps, per block,
 * the last row of each key that can join (a master key whose value the slave's symbol table holds)
 * inside it. A block's summary is filled lazily, by a backward scan from the block's end that stops
 * as soon as it has met the key a lookup asks for; the next lookup that needs an older row resumes
 * the scan where it stopped. A block is therefore scanned at most once, and only as far as the
 * lookups of this query need: no further than the plain scan of a single page frame would go into
 * it. The summaries then serve every page frame: the backward walk past the page frame's own block
 * becomes one array read per block. The answer is the row the plain scan would find, since a
 * block's last row of a key is the first one a backward scan meets there.
 * <p>
 * Memory: the row id array holds blocks x joinable keys entries, at most {@link #MAX_ENTRIES}; two
 * key-to-slot int arrays span up to the highest joinable master and slave key. They are allocated
 * by the first lookup that needs a block and charged to the query's tracker. If the tracker
 * refuses them, the summaries switch off for the query and every
 * lookup answers {@link #UNAVAILABLE}: the caller falls back to the plain scan, so the summaries
 * never make a query fail that the plain scan serves.
 * <p>
 * Thread safety: a block is scanned under a lock (striped over the blocks); a reader that needs a
 * block another worker is scanning waits for the lock, checking the query's circuit breaker, and then
 * finds the scan advanced, or advances it itself. A finished block is published by a volatile state
 * write and read without the lock. A scan that throws (the circuit breaker) leaves the block
 * consistent: what it found stays, and the next reader resumes from the last row it completed.
 */
public class WindowJoinPrevailingSummaries implements QuietCloseable {
    // total summary entries (blocks x keys) a query may hold; more keys or frames get bigger blocks
    static final long MAX_ENTRIES = 2 * 1024 * 1024;
    /**
     * Returned by {@link #lastRowIdInBlock} when the summaries are off for the query: the memory
     * tracker refused them. The caller falls back to the plain backward scan.
     */
    static final long UNAVAILABLE = -2;
    // the scan consults the circuit breaker once per this many rows (a power of two)
    private static final int CIRCUIT_BREAKER_CHECK_ROWS = 1024;
    private static final int LOCK_STRIPES = 64;
    private static final long LOCK_WAIT_SLICE_NANOS = TimeUnit.MILLISECONDS.toNanos(1);
    private static final int STATE_COMPLETE = 2;
    private static final int STATE_EMPTY = 0;
    private static final int STATE_PARTIAL = 1;
    private final ReentrantLock[] locks = new ReentrantLock[LOCK_STRIPES];
    private final DirectLongList rowIds = new DirectLongList(0, MemoryTag.NATIVE_DEFAULT, true);
    // Two int arrays, -1 for a key that cannot join: the slot of toSymbolMapKey(master key), at
    // masterSlotsAddress, and the slot of toSymbolMapKey(slave key), at slaveSlotsAddress. Arrays,
    // not maps: filling them costs a pass over the lookup map, with no hashing.
    private final DirectLongList slotArrays = new DirectLongList(0, MemoryTag.NATIVE_DEFAULT, true);
    private int blockCount;
    // per block, under its lock: joinable keys found so far, and the next row the scan reads
    private int[] foundCounts = new int[0];
    private int frameCount;
    private int framesPerBlock;
    // the block's scan resumes at this frame, at this row or, for Long.MAX_VALUE, the frame's last row
    private int[] frontierFrames = new int[0];
    private long[] frontierRows = new long[0];
    private int joinableKeyCount;
    private int masterSlotCount;
    private long masterSlotsAddress;
    private @Nullable MemoryTracker memoryTracker;
    private int slaveSlotCount;
    private long slaveSlotsAddress;
    private @Nullable DirectIntIntHashMap slaveSymbolLookupMap;
    private AtomicIntegerArray states;
    // set once, under this monitor, by the first lookup; volatile so that readers see the arrays
    private volatile boolean isAllocated;
    private volatile boolean isUnavailable;

    public WindowJoinPrevailingSummaries() {
        for (int i = 0; i < LOCK_STRIPES; i++) {
            locks[i] = new ReentrantLock();
        }
    }

    @Override
    public void close() {
        Misc.free(rowIds);
        Misc.free(slotArrays);
        isAllocated = false;
        isUnavailable = false;
        blockCount = 0;
        joinableKeyCount = 0;
        slaveSymbolLookupMap = null;
    }

    public int getBlockCount() {
        return blockCount;
    }

    public int getBlockOf(int frameIndex) {
        return frameIndex / framesPerBlock;
    }

    public int getFirstFrameOf(int block) {
        return block * framesPerBlock;
    }

    public int getFramesPerBlock() {
        return framesPerBlock;
    }

    public boolean isEnabled() {
        return blockCount > 0 && !isUnavailable;
    }

    /**
     * Returns the last slave row id of {@code masterKey} in {@code block}, {@link Long#MIN_VALUE}
     * when the block holds none, or {@link #UNAVAILABLE} when the summaries are off for the query.
     * Scans the block as far as needed first, if no worker has yet. The helper's position is not
     * restored; the caller restores its bookmark.
     */
    public long lastRowIdInBlock(
            int block,
            int masterKey,
            WindowJoinTimeFrameHelper slaveTimeFrameHelper,
            Record slaveRecord,
            int slaveSymbolIndex,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker
    ) {
        if (!ensureAllocated()) {
            return UNAVAILABLE;
        }
        final int masterMapKey = AsyncWindowJoinFastAtom.toSymbolMapKey(masterKey);
        final int slot = masterMapKey < masterSlotCount ? Unsafe.getInt(masterSlotsAddress + ((long) masterMapKey << 2)) : -1;
        if (slot < 0) {
            // the slave's symbol table does not hold the key: no slave row can match it
            return Long.MIN_VALUE;
        }
        if (states.get(block) == STATE_COMPLETE) {
            return Unsafe.getLong(entryAddress(block, slot));
        }
        final ReentrantLock lock = locks[block % LOCK_STRIPES];
        lockInterruptibly(lock, circuitBreaker);
        try {
            return scanUntilFound(block, slot, slaveTimeFrameHelper, slaveRecord, slaveSymbolIndex, circuitBreaker);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Sizes the summaries for a slave of {@code frameCount} time frames and the keys of
     * {@code slaveSymbolLookupMap} (slave key -> master key), the keys that can join. Allocates
     * nothing: the first lookup that needs a block does. Leaves the summaries disabled (every lookup
     * falls back to the plain scan) when there are no frames or keys, or the keys alone exceed the
     * entry budget. The lookup map must not change while the summaries are in use.
     */
    public void of(int frameCount, DirectIntIntHashMap slaveSymbolLookupMap, @Nullable MemoryTracker memoryTracker) {
        // free under the tracker that charged it, then charge this query
        rowIds.close();
        slotArrays.close();
        isAllocated = false;
        isUnavailable = false;
        this.frameCount = frameCount;
        this.slaveSymbolLookupMap = slaveSymbolLookupMap;
        this.memoryTracker = memoryTracker;
        this.joinableKeyCount = slaveSymbolLookupMap.size();
        this.blockCount = 0;
        if (frameCount <= 0 || joinableKeyCount == 0 || joinableKeyCount > MAX_ENTRIES) {
            return;
        }
        final long maxBlocks = Math.max(1, MAX_ENTRIES / joinableKeyCount);
        framesPerBlock = (int) Math.max(1, (frameCount + maxBlocks - 1) / maxBlocks);
        final int blocks = (frameCount + framesPerBlock - 1) / framesPerBlock;
        if (states == null || states.length() < blocks) {
            states = new AtomicIntegerArray(blocks);
            foundCounts = new int[blocks];
            frontierFrames = new int[blocks];
            frontierRows = new long[blocks];
        } else {
            for (int i = 0; i < blocks; i++) {
                states.set(i, STATE_EMPTY);
            }
        }
        this.blockCount = blocks;
    }

    private static void lockInterruptibly(ReentrantLock lock, SqlExecutionCircuitBreaker circuitBreaker) {
        // wait for the worker scanning the block, but stay cancellable
        try {
            while (!lock.tryLock(LOCK_WAIT_SLICE_NANOS, TimeUnit.NANOSECONDS)) {
                circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw CairoException.nonCritical().put("interrupted while waiting for a window join prevailing summary");
        }
    }

    private void allocate() {
        assert slaveSymbolLookupMap != null;
        rowIds.setMemoryTracker(memoryTracker);
        rowIds.setCapacity((long) blockCount * joinableKeyCount);
        int maxSlaveMapKey = 0;
        int maxMasterMapKey = 0;
        final long capacity = slaveSymbolLookupMap.capacity();
        for (long index = 0; index < capacity; index++) {
            final int slaveMapKey = slaveSymbolLookupMap.keyAt(index);
            if (slaveMapKey != 0) {
                maxSlaveMapKey = Math.max(maxSlaveMapKey, slaveMapKey);
                maxMasterMapKey = Math.max(maxMasterMapKey, AsyncWindowJoinFastAtom.toSymbolMapKey(slaveSymbolLookupMap.valueAt(-index - 1)));
            }
        }
        masterSlotCount = maxMasterMapKey + 1;
        slaveSlotCount = maxSlaveMapKey + 1;
        final long slotBytes = 4L * (masterSlotCount + slaveSlotCount);
        slotArrays.setMemoryTracker(memoryTracker);
        slotArrays.setCapacity((slotBytes + 7) >>> 3);
        masterSlotsAddress = slotArrays.getAddress();
        slaveSlotsAddress = masterSlotsAddress + 4L * masterSlotCount;
        // all bits set: -1 in every slot
        Vect.memset(masterSlotsAddress, slotBytes, -1);
        int slot = 0;
        for (long index = 0; index < capacity; index++) {
            final int slaveMapKey = slaveSymbolLookupMap.keyAt(index);
            if (slaveMapKey != 0) {
                final int masterMapKey = AsyncWindowJoinFastAtom.toSymbolMapKey(slaveSymbolLookupMap.valueAt(-index - 1));
                Unsafe.putInt(slaveSlotsAddress + ((long) slaveMapKey << 2), slot);
                Unsafe.putInt(masterSlotsAddress + ((long) masterMapKey << 2), slot);
                slot++;
            }
        }
        assert slot == joinableKeyCount;
    }

    // Allocates the row id array and the slot arrays on first use; false when the tracker refused them.
    private boolean ensureAllocated() {
        if (isAllocated) {
            return true;
        }
        if (isUnavailable) {
            return false;
        }
        synchronized (this) {
            if (!isAllocated && !isUnavailable) {
                try {
                    allocate();
                    isAllocated = true;
                } catch (CairoException e) {
                    if (!e.isOutOfMemory()) {
                        throw e;
                    }
                    // the query's memory limit has no room for them: serve it by the plain scan
                    rowIds.close();
                    slotArrays.close();
                    isUnavailable = true;
                }
            }
            return isAllocated;
        }
    }

    private long entryAddress(int block, int slot) {
        return rowIds.getAddress() + (((long) block * joinableKeyCount + slot) << 3);
    }

    // Under the block's lock: returns the key's last row in the block, scanning further back from
    // where the block's scan stopped until the scan meets it or reaches the block's start.
    private long scanUntilFound(
            int block,
            int slot,
            WindowJoinTimeFrameHelper slaveTimeFrameHelper,
            Record slaveRecord,
            int slaveSymbolIndex,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        final long base = entryAddress(block, 0);
        final int frameLo = getFirstFrameOf(block);
        switch (states.get(block)) {
            case STATE_COMPLETE:
                return Unsafe.getLong(base + ((long) slot << 3));
            case STATE_EMPTY:
                for (int i = 0; i < joinableKeyCount; i++) {
                    Unsafe.putLong(base + ((long) i << 3), Long.MIN_VALUE);
                }
                foundCounts[block] = 0;
                frontierFrames[block] = Math.min(frameLo + framesPerBlock, frameCount) - 1;
                frontierRows[block] = Long.MAX_VALUE;
                states.set(block, STATE_PARTIAL);
                break;
            default:
                final long rowId = Unsafe.getLong(base + ((long) slot << 3));
                if (rowId != Long.MIN_VALUE) {
                    return rowId;
                }
        }

        final long slaveSlotsAddress = this.slaveSlotsAddress;
        final int slaveSlotCount = this.slaveSlotCount;
        int found = foundCounts[block];
        int frameIndex = frontierFrames[block];
        long r = frontierRows[block];
        int rowsScanned = 0;
        try {
            // backwards, so the first row met for a key is its last one in the block
            for (; frameIndex >= frameLo; frameIndex--, r = Long.MAX_VALUE) {
                if (slaveTimeFrameHelper.openFrame(frameIndex) <= 0) {
                    continue;
                }
                slaveTimeFrameHelper.recordAt(frameIndex, 0);
                final long rowLo = slaveTimeFrameHelper.getTimeFrameRowLo();
                // the key column's memory, when the frame has it as plain values: read it directly
                final long keyAddress = slaveRecord instanceof PageFrameMemoryRecord frameRecord ? frameRecord.getPageAddress(slaveSymbolIndex) : 0;
                for (r = Math.min(r, slaveTimeFrameHelper.getTimeFrameRowHi() - 1); r >= rowLo; r--) {
                    if ((rowsScanned++ & (CIRCUIT_BREAKER_CHECK_ROWS - 1)) == 0) {
                        circuitBreaker.statefulThrowExceptionIfTripped();
                    }
                    final int slaveKey;
                    if (keyAddress != 0) {
                        slaveKey = Unsafe.getInt(keyAddress + (r << 2));
                    } else {
                        slaveTimeFrameHelper.recordAtRowIndex(r);
                        slaveKey = slaveRecord.getInt(slaveSymbolIndex);
                    }
                    final int slaveMapKey = AsyncWindowJoinFastAtom.toSymbolMapKey(slaveKey);
                    final int s = slaveMapKey < slaveSlotCount ? Unsafe.getInt(slaveSlotsAddress + ((long) slaveMapKey << 2)) : -1;
                    if (s >= 0) {
                        final long address = base + ((long) s << 3);
                        if (Unsafe.getLong(address) == Long.MIN_VALUE) {
                            final long rowId = Rows.toRowID(frameIndex, r);
                            Unsafe.putLong(address, rowId);
                            if (++found == joinableKeyCount) {
                                // every joinable key has its row: the rest of the block adds nothing
                                states.set(block, STATE_COMPLETE);
                                return Unsafe.getLong(base + ((long) slot << 3));
                            }
                            if (s == slot) {
                                r--;
                                return rowId;
                            }
                        }
                    }
                }
            }
            // reached the block's start: the keys not found have no row in it
            states.set(block, STATE_COMPLETE);
            return Long.MIN_VALUE;
        } finally {
            // the rows above (frameIndex, r) are summarised; a scan that threw resumes at r
            foundCounts[block] = found;
            frontierFrames[block] = frameIndex;
            frontierRows[block] = r;
        }
    }
}
