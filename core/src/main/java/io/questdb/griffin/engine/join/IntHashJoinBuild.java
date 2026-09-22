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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.engine.CompressedOffsets;
import io.questdb.std.DirectLongList;
import io.questdb.std.Hash;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

/**
 * Owner-built INT lookup for fused hash join aggregation. No native memory is
 * allocated by construction. {@link #open} binds the execution's tracker before
 * allocation; any append/build failure closes the entire partial build.
 * <p>
 * Linear probing at a maximum load of 1/2. An eight-byte slot holds an INT key
 * and an unsigned compressed payload offset. References encode an eight-byte
 * aligned row offset divided by eight plus one; zero marks an unused slot.
 * Zero, negative and INT_NULL keys need no special representation and null keys
 * match each other. Widening a slot head only scales its unsigned value; duplicate
 * advances need no offset decoding.
 * <p>
 * The rows themselves, their links and the ids of their build rows live in
 * {@link HashJoinRowHeap}, which {@link MapHashJoinBuild} shares; see there for the
 * row layout and the heap bound. Probes read payload columns through the build's
 * {@link HashJoinPayloadSource}. A SYMBOL join key is stored as the build's own symbol key;
 * the probe translates its key into that domain through {@link SymbolKeyTranslator}.
 * <p>
 * Hash tables and rows use tracked native buffers. Growth accounts for both old and
 * new allocations and is cancellable. Frozen views borrow these buffers until close;
 * see {@link FrozenHashJoinBuild}.
 * <p>
 * The key table doubles from its initial size, rehashing every key it holds, unless the
 * caller bounds the distinct keys up front; a key count hint buys the whole table before
 * the first row. Only the caller can bound them: a row count bounds them too, but a table
 * sized by the rows of a key that repeats holds mostly empty slots, costs memory and scatters
 * every lookup over them.
 */
public final class IntHashJoinBuild implements Closeable {
    private static final int MAX_SLOTS = 1 << 30;
    private static final long MAX_BUFFER_SIZE = 1L << 48;
    private static final int SLOT_SIZE = 8;
    // Rehashing is not bounded by a row; it checks the breaker once per MiB it touches.
    private static final int KEY_SLOTS_PER_CHECK = (int) (HashJoinBuffer.COPY_CHUNK_SIZE / SLOT_SIZE);
    private final HashJoinRowHeap heap;
    private final int initialSlots;
    private final HashJoinBuffer keys = new HashJoinBuffer(MAX_BUFFER_SIZE);
    private final Frozen reusableFrozen;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private Frozen frozen;
    private int keyCount;
    private int keySlotCount;
    private boolean open;

    /** A build with payload columns stores row ids for probes to read them through; see the class docs. */
    @TestOnly
    public IntHashJoinBuild(boolean hasPayload, int initialSlots, long initialRowCapacity) {
        this(hasPayload, initialSlots, initialRowCapacity, false);
    }

    /**
     * Reusable mode is for a factory that drains all consumers before reopening.
     * Its snapshot is a flyweight; retained probes must explicitly reopen for each
     * execution. The ordinary constructor keeps execution-specific snapshots.
     */
    public IntHashJoinBuild(boolean hasPayload, int initialSlots, long initialRowCapacity, boolean reusable) {
        if (initialSlots < 2 || initialSlots > MAX_SLOTS || Integer.bitCount(initialSlots) != 1) {
            throw new IllegalArgumentException("invalid hash join build capacity");
        }
        this.initialSlots = initialSlots;
        heap = new HashJoinRowHeap(hasPayload, initialRowCapacity);
        reusableFrozen = reusable ? new Frozen() : null;
    }

    /** Appends one row. On failure all execution allocations are released. */
    public void append(int key, long rowId) {
        requireBuilding();
        try {
            appendRow(key, rowId);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    /**
     * Appends every row of the page frame the record is bound to, keyed by the INT key column.
     * The caller checks cancellation at frame boundaries. On failure all execution allocations
     * are released.
     */
    public void appendFrame(PageFrameMemoryRecord record, int keyColumn, long rowCount) {
        requireBuilding();
        try {
            for (long r = 0; r < rowCount; r++) {
                record.setRowIndex(r);
                appendRow(record.getInt(keyColumn), record.getRowId());
            }
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    /** The filtered twin of {@link #appendFrame(PageFrameMemoryRecord, int, long)}: appends the listed rows only. */
    public void appendFrame(PageFrameMemoryRecord record, int keyColumn, DirectLongList rows) {
        requireBuilding();
        try {
            for (long p = 0, n = rows.size(); p < n; p++) {
                record.setRowIndex(rows.get(p));
                appendRow(record.getInt(keyColumn), record.getRowId());
            }
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    /**
     * Consumes a borrowed cursor once, keeping the id of each row, and freezes. Probes read payload
     * columns through the source, so the caller keeps whatever the source reads open until close.
     * The hints are those of {@link #reserve(long, long)}. A SYMBOL key column keeps its own key,
     * which is the domain the probe translates into.
     */
    public FrozenHashJoinBuild.IntKeyed build(
            RecordCursor cursor,
            int keyColumn,
            long rowCountHint,
            long keyCountHint,
            @Nullable HashJoinPayloadSource payloads
    ) {
        reserve(rowCountHint, keyCountHint);
        try {
            final Record record = cursor.getRecord();
            final boolean hasRowId = heap.hasRowId();
            // The source cursor checks the breaker at its frame boundaries.
            while (cursor.hasNext()) {
                appendRow(record.getInt(keyColumn), hasRowId ? record.getRowId() : 0);
            }
        } catch (Throwable th) {
            close();
            throw th;
        }
        return freeze(payloads);
    }

    /** Only call after every probe is drained and aggregate output is finished. */
    @Override
    public void close() {
        if (frozen != null) {
            // Do not retain the borrowed source past its execution.
            frozen.payloads = null;
            frozen = null;
        }
        open = false;
        keys.close();
        heap.close();
        keyCount = keySlotCount = 0;
        circuitBreaker = null;
    }

    /** Ends mutation of a build without payload columns. */
    public FrozenHashJoinBuild.IntKeyed freeze() {
        return freeze(null);
    }

    /**
     * Ends mutation. Probes read payload columns through the borrowed source until close.
     * Publication to probe workers is the caller's responsibility.
     */
    public FrozenHashJoinBuild.IntKeyed freeze(@Nullable HashJoinPayloadSource payloads) {
        requireBuilding();
        try {
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
            if (payloads == null && heap.hasRowId()) {
                throw new IllegalArgumentException("hash join build with payload columns requires a payload source");
            }
            frozen = reusableFrozen != null ? reusableFrozen : new Frozen();
            frozen.of(payloads);
            return frozen;
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    public long getSizeInBytes() {
        return keys.capacity + heap.getSizeInBytes();
    }

    /** Reopens a closed skeleton for a fresh execution. */
    public void open(@Nullable MemoryTracker memoryTracker, SqlExecutionCircuitBreaker circuitBreaker) {
        if (open) {
            throw new IllegalStateException("hash join build is already open");
        }
        this.circuitBreaker = circuitBreaker;
        keys.of(memoryTracker, circuitBreaker);
        heap.of(memoryTracker, circuitBreaker);
        try {
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
            keys.allocate((long) initialSlots * SLOT_SIZE, true);
            keySlotCount = initialSlots;
            open = true;
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    /**
     * Presizes the execution's storage. A positive row hint is the number of rows the build will
     * append and presizes the row heap. A positive key hint bounds the distinct keys among them and
     * presizes the key table for that many, so that no rehash runs below it; -1 leaves the table to
     * grow. On failure all execution allocations are released.
     */
    public void reserve(long rowCountHint, long keyCountHint) {
        requireBuilding();
        try {
            if (rowCountHint > 0) {
                heap.reserve(rowCountHint);
            }
            if (keyCountHint > 0) {
                reserveKeys(keyCountHint);
            }
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    private static long findKeySlot(long base, int slots, int key) {
        int index = (int) Hash.hashInt64(key) & (slots - 1);
        long address = base + (long) index * SLOT_SIZE;
        while (Unsafe.getInt(address + 4) != 0 && Unsafe.getInt(address) != key) {
            index = (index + 1) & (slots - 1);
            address = base + (long) index * SLOT_SIZE;
        }
        return address;
    }

    private static long toRowLink(int head) {
        return CompressedOffsets.uncompressAligned8(head);
    }

    // The caller owns failure cleanup. Growth checks the breaker per MiB of rehashed or copied memory.
    private void appendRow(int key, long rowId) {
        long slot = findKeySlot(keys.address, keySlotCount, key);
        int previous = Unsafe.getInt(slot + 4);
        if (previous == 0 && keyCount == keySlotCount / 2) {
            if (keySlotCount == MAX_SLOTS) {
                throw CairoException.nonCritical().put("hash join build capacity overflow");
            }
            growKeyTable(keySlotCount * 2);
            slot = findKeySlot(keys.address, keySlotCount, key);
        }
        final long offset = heap.append(rowId, toRowLink(previous));
        Unsafe.putInt(slot, key);
        Unsafe.putInt(slot + 4, CompressedOffsets.compressBiased8(offset));
        if (previous == 0) {
            keyCount++;
        }
    }

    // Rehashes into a table of the given power-of-two slot count, larger than the current one.
    private void growKeyTable(int slots) {
        // Separate destination keeps both allocations charged throughout rehashing.
        final HashJoinBuffer dest = keys.scratch();
        try {
            dest.allocate((long) slots * SLOT_SIZE, true);
            for (int i = 0, n = keySlotCount; i < n; i++) {
                if ((i & (KEY_SLOTS_PER_CHECK - 1)) == 0) {
                    circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
                }
                long src = keys.address + (long) i * SLOT_SIZE;
                int value = Unsafe.getInt(src + 4);
                if (value != 0) {
                    int key = Unsafe.getInt(src);
                    int index = (int) Hash.hashInt64(key) & (slots - 1);
                    long target = dest.address + (long) index * SLOT_SIZE;
                    while (Unsafe.getInt(target + 4) != 0) {
                        index = (index + 1) & (slots - 1);
                        target = dest.address + (long) index * SLOT_SIZE;
                    }
                    Unsafe.putInt(target, key);
                    Unsafe.putInt(target + 4, value);
                }
            }
            keys.take(dest);
            keySlotCount = slots;
        } finally {
            dest.close();
        }
    }

    private void requireBuilding() {
        if (!open || frozen != null) {
            throw new IllegalStateException("hash join build is not mutable");
        }
    }

    // A table holds at most half its slots, as appendRow() grows it. A hint past the largest
    // table is not an error here: the keys it bounds may still fit, so growth decides that.
    private void reserveKeys(long keyCount) {
        final int slots = Numbers.ceilPow2(2 * (int) Math.min(keyCount, MAX_SLOTS / 2));
        if (slots > keySlotCount) {
            growKeyTable(slots);
        }
    }

    private class Frozen implements FrozenHashJoinBuild.IntKeyed {
        private long generation;
        private long handleBase;
        private long keysAddress;
        private int keysCount;
        private long rowsAddress;
        private long rowsCount;
        private HashJoinPayloadSource payloads;
        private long size;
        private int slots;

        @Override
        public long getKeyCount() {
            return keysCount;
        }

        @Override
        public long getRowCount() {
            return rowsCount;
        }

        @Override
        public long getSizeInBytes() {
            return size;
        }

        @Override
        public FrozenHashJoinBuild.IntProbe newProbe() {
            if (frozen != this) {
                throw new IllegalStateException("hash join build has expired");
            }
            return new View();
        }

        private void of(HashJoinPayloadSource payloads) {
            handleBase = heap.nextHandleBase();
            keysAddress = keys.address;
            keysCount = keyCount;
            slots = keySlotCount;
            rowsAddress = heap.getAddress();
            rowsCount = heap.getRowCount();
            size = IntHashJoinBuild.this.getSizeInBytes();
            this.payloads = payloads;
            generation = heap.freeze();
        }

        private class View extends AbstractHashJoinProbe implements FrozenHashJoinBuild.IntProbe {
            private long lookupKeysAddress;
            private int lookupMask;

            private View() {
                super(heap);
                try {
                    reopen();
                } catch (Throwable th) {
                    close();
                    throw th;
                }
            }

            @Override
            public void find(int key) {
                assert isCurrent();
                long slot = findKeySlot(keysAddress, slots, key);
                next = toRowLink(Unsafe.getInt(slot + 4));
            }

            @Override
            public boolean findSingleUnchecked(int key) {
                assert keysCount == rowsCount;
                if (rowsCount == 0) {
                    assert isCurrent();
                    next = 0;
                    return false;
                }
                final int head = findHead(key);
                next = 0;
                if (head == 0) {
                    return false;
                }
                positionAt(CompressedOffsets.uncompressBiased8(head));
                return true;
            }

            @Override
            public void findUnchecked(int key) {
                next = toRowLink(findHead(key));
            }

            @Override
            public void reopen() {
                if (frozen != Frozen.this) {
                    throw new IllegalStateException("hash join build has expired");
                }
                // This view is rebound after the previous execution drains. Cache the
                // immutable native lookup metadata for its entire acquired lifetime.
                lookupMask = slots - 1;
                lookupKeysAddress = keysAddress;
                ofSnapshot(payloads, handleBase, rowsAddress, rowsCount, generation);
            }

            private int findCollision(int key, long address) {
                final long base = lookupKeysAddress;
                final long limit = base + ((long) lookupMask + 1) * SLOT_SIZE;
                int head;
                do {
                    address += SLOT_SIZE;
                    if (address == limit) {
                        address = base;
                    }
                } while ((head = Unsafe.getInt(address + 4)) != 0 && Unsafe.getInt(address) != key);
                return head;
            }

            private int findHead(int key) {
                assert isCurrent();
                final int mask = lookupMask;
                final long base = lookupKeysAddress;
                long address = base + ((long) ((int) Hash.hashInt64(key) & mask)) * SLOT_SIZE;
                int head = Unsafe.getInt(address + 4);
                if (head != 0 && Unsafe.getInt(address) != key) {
                    head = findCollision(key, address);
                }
                return head;
            }
        }
    }
}
