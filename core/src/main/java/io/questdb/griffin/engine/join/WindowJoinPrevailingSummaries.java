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

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.std.DirectBitSet;
import io.questdb.std.DirectIntIntHashMap;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Rows;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * The prevailing-row backward scan of a window join, shared by every page frame of the master.
 * <p>
 * A window join with INCLUDE PREVAILING needs, per master page frame, the last slave row of each
 * key before the frame's slave span. {@link WindowJoinPrevailingCache} finds it by scanning the
 * slave backwards from the span, which costs the distance back to the key's previous row - for a
 * rare key, a long way - and every page frame repeats it.
 * <p>
 * This class splits the slave's time frames into blocks of consecutive frames and keeps, per block,
 * the last row of every master key inside it. A block is summarised once, by whichever worker first
 * needs it, and then serves every page frame: the backward walk past the page frame's own block
 * becomes one array read per block instead of a scan of its rows. The answer is the row the plain
 * scan would find, since a block's last row of a key is the first one a backward scan meets there.
 * <p>
 * Thread safety: a block is built under a CAS on its state and published by a volatile write, so
 * readers see it whole; a reader that finds a block being built waits for it. Building never
 * waits on anything else, so there is no deadlock. A builder that fails resets the state, and the
 * next reader builds the block itself.
 */
public class WindowJoinPrevailingSummaries implements QuietCloseable {
    // total summary entries (blocks x keys) a query may hold; more keys or frames get bigger blocks
    static final long MAX_ENTRIES = 2 * 1024 * 1024;
    private static final int STATE_BUILDING = 1;
    private static final int STATE_EMPTY = 0;
    private static final int STATE_READY = 2;
    private final DirectBitSet joinableSlots = new DirectBitSet(64, MemoryTag.NATIVE_BIT_SET, true);
    private final DirectLongList rowIds = new DirectLongList(0, MemoryTag.NATIVE_DEFAULT, true);
    private int blockCount;
    private int frameCount;
    private int framesPerBlock;
    private int joinableKeyCount;
    // slot = AsyncWindowJoinFastAtom.toSymbolMapKey(masterKey) - 1: the NULL key is 0, key k is k + 1
    private int keyCount;
    private @Nullable MemoryTracker memoryTracker;
    // the row id array is allocated by the first lookup that needs a block, not up front: a query
    // whose prevailing rows all lie close to their page frames never pays for it
    private volatile boolean isAllocated;
    private AtomicIntegerArray states;

    @Override
    public void close() {
        Misc.free(rowIds);
        isAllocated = false;
        Misc.free(joinableSlots);
        blockCount = 0;
        keyCount = 0;
        joinableKeyCount = 0;
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
        return blockCount > 0;
    }

    /**
     * Returns the last slave row id of {@code masterKey} in {@code block}, or {@link Long#MIN_VALUE}
     * when the block holds none, summarising the block first if no worker has yet. The helper's
     * position is not restored; the caller restores its bookmark.
     */
    public long lastRowIdInBlock(
            int block,
            int masterKey,
            WindowJoinTimeFrameHelper slaveTimeFrameHelper,
            Record slaveRecord,
            int slaveSymbolIndex,
            DirectIntIntHashMap slaveSymbolLookupMap
    ) {
        final int slot = AsyncWindowJoinFastAtom.toSymbolMapKey(masterKey) - 1;
        if (slot >= keyCount || !joinableSlots.get(slot)) {
            return Long.MIN_VALUE;
        }
        ensureAllocated();
        ensureBuilt(block, slaveTimeFrameHelper, slaveRecord, slaveSymbolIndex, slaveSymbolLookupMap);
        return Unsafe.getLong(entryAddress(block, slot));
    }

    /**
     * Records that a master key has a slave counterpart, so a block can stop summarising once it
     * has seen them all. Called while the lookup map is built, before any block is.
     */
    public void markJoinable(int masterKey) {
        final int slot = AsyncWindowJoinFastAtom.toSymbolMapKey(masterKey) - 1;
        if (slot < keyCount && !joinableSlots.getAndSet(slot)) {
            joinableKeyCount++;
        }
    }

    /**
     * Sizes the summaries for a slave of {@code frameCount} time frames and a master symbol table of
     * {@code masterSymbolCount} keys. Leaves them disabled (every lookup falls back to the plain scan)
     * when there are no frames or the keys alone exceed the entry budget.
     */
    public void of(int frameCount, int masterSymbolCount, @Nullable MemoryTracker memoryTracker) {
        this.frameCount = frameCount;
        this.keyCount = masterSymbolCount + 1;
        this.blockCount = 0;
        this.joinableKeyCount = 0;
        if (frameCount <= 0 || keyCount > MAX_ENTRIES) {
            return;
        }
        final long maxBlocks = Math.max(1, MAX_ENTRIES / keyCount);
        framesPerBlock = (int) Math.max(1, (frameCount + maxBlocks - 1) / maxBlocks);
        final int blocks = (frameCount + framesPerBlock - 1) / framesPerBlock;
        // free under the tracker that charged it, then charge this query
        rowIds.close();
        isAllocated = false;
        this.memoryTracker = memoryTracker;
        joinableSlots.reserve(keyCount);
        joinableSlots.clear();
        if (states == null || states.length() < blocks) {
            states = new AtomicIntegerArray(blocks);
        } else {
            for (int i = 0; i < blocks; i++) {
                states.set(i, STATE_EMPTY);
            }
        }
        this.blockCount = blocks;
    }

    private void build(
            int block,
            WindowJoinTimeFrameHelper slaveTimeFrameHelper,
            Record slaveRecord,
            int slaveSymbolIndex,
            DirectIntIntHashMap slaveSymbolLookupMap
    ) {
        final long base = entryAddress(block, 0);
        for (int slot = 0; slot < keyCount; slot++) {
            Unsafe.putLong(base + ((long) slot << 3), Long.MIN_VALUE);
        }
        final int frameLo = getFirstFrameOf(block);
        final int frameHi = Math.min(frameLo + framesPerBlock, frameCount);
        int found = 0;
        // backwards, so the first row met for a key is its last one in the block
        for (int frameIndex = frameHi - 1; frameIndex >= frameLo && found < joinableKeyCount; frameIndex--) {
            if (slaveTimeFrameHelper.openFrame(frameIndex) <= 0) {
                continue;
            }
            slaveTimeFrameHelper.recordAt(frameIndex, 0);
            final long rowLo = slaveTimeFrameHelper.getTimeFrameRowLo();
            for (long r = slaveTimeFrameHelper.getTimeFrameRowHi() - 1; r >= rowLo; r--) {
                slaveTimeFrameHelper.recordAtRowIndex(r);
                final int slaveKey = slaveRecord.getInt(slaveSymbolIndex);
                final int matchingMasterKey = slaveSymbolLookupMap.get(AsyncWindowJoinFastAtom.toSymbolMapKey(slaveKey));
                if (matchingMasterKey != StaticSymbolTable.VALUE_NOT_FOUND) {
                    final long address = base + ((long) (AsyncWindowJoinFastAtom.toSymbolMapKey(matchingMasterKey) - 1) << 3);
                    if (Unsafe.getLong(address) == Long.MIN_VALUE) {
                        Unsafe.putLong(address, Rows.toRowID(frameIndex, r));
                        if (++found == joinableKeyCount) {
                            break;
                        }
                    }
                }
            }
        }
    }

    private void ensureAllocated() {
        if (!isAllocated) {
            synchronized (this) {
                if (!isAllocated) {
                    rowIds.setMemoryTracker(memoryTracker);
                    rowIds.setCapacity((long) blockCount * keyCount);
                    isAllocated = true;
                }
            }
        }
    }

    private void ensureBuilt(
            int block,
            WindowJoinTimeFrameHelper slaveTimeFrameHelper,
            Record slaveRecord,
            int slaveSymbolIndex,
            DirectIntIntHashMap slaveSymbolLookupMap
    ) {
        while (true) {
            final int state = states.get(block);
            if (state == STATE_READY) {
                return;
            }
            if (state == STATE_EMPTY && states.compareAndSet(block, STATE_EMPTY, STATE_BUILDING)) {
                boolean built = false;
                try {
                    build(block, slaveTimeFrameHelper, slaveRecord, slaveSymbolIndex, slaveSymbolLookupMap);
                    built = true;
                } finally {
                    states.set(block, built ? STATE_READY : STATE_EMPTY);
                }
                return;
            }
            Os.pause();
        }
    }

    private long entryAddress(int block, int slot) {
        return rowIds.getAddress() + (((long) block * keyCount + slot) << 3);
    }
}
