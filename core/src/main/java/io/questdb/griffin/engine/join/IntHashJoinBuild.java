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
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.engine.CompressedOffsets;
import io.questdb.std.Hash;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTracker;
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
 * The rows themselves, their links and their payload records live in
 * {@link HashJoinRowHeap}, which {@link MapHashJoinBuild} shares; see there for the
 * row layout, the heap bound and how SYMBOL payloads resolve. A SYMBOL join key
 * arrives already translated by {@link SymbolKeyTranslator}.
 * <p>
 * Hash tables and rows use tracked native buffers. Growth accounts for both old and
 * new allocations and is cancellable. Frozen views borrow these buffers until close;
 * see {@link FrozenHashJoinBuild}.
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

    /** Payload types/indexes are in the same order; indexes address the source record. */
    @TestOnly
    public IntHashJoinBuild(ColumnTypes payloadTypes, IntList sourceColumns, int initialSlots, long initialRowCapacity) {
        this(payloadTypes, sourceColumns, initialSlots, initialRowCapacity, false);
    }

    /**
     * Reusable mode is for a factory that drains all consumers before reopening.
     * Its snapshot is a flyweight; retained probes must explicitly reopen for each
     * execution. The ordinary constructor keeps execution-specific snapshots.
     */
    public IntHashJoinBuild(ColumnTypes payloadTypes, IntList sourceColumns, int initialSlots, long initialRowCapacity, boolean reusable) {
        if (initialSlots < 2 || initialSlots > MAX_SLOTS || Integer.bitCount(initialSlots) != 1) {
            throw new IllegalArgumentException("invalid hash join build capacity or payload mapping");
        }
        this.initialSlots = initialSlots;
        heap = new HashJoinRowHeap(payloadTypes, sourceColumns, initialRowCapacity);
        reusableFrozen = reusable ? new Frozen() : null;
    }

    /** Copies one row. On failure all execution allocations are released. */
    public void append(int key, Record record) {
        requireBuilding();
        try {
            appendRow(key, record);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    /** Consumes a borrowed INT-keyed cursor once. The caller retains ownership of the cursor. */
    public FrozenHashJoinBuild.IntKeyed build(RecordCursor cursor, int keyColumn) {
        return build(cursor, keyColumn, -1, null);
    }

    /**
     * Consumes a borrowed cursor once and resolves SYMBOL payloads through it until close,
     * so the caller keeps the cursor open until then. A nonnegative hint is the remaining
     * row count of a freshly acquired cursor. A translator maps SYMBOL keys into the probe
     * key domain; rows whose key the probe dictionary lacks cannot match and are skipped.
     */
    public FrozenHashJoinBuild.IntKeyed build(RecordCursor cursor, int keyColumn, long rowCountHint, @Nullable SymbolKeyTranslator keyTranslator) {
        requireBuilding();
        try {
            if (rowCountHint > 0) {
                heap.reserve(rowCountHint);
            }
            final Record record = cursor.getRecord();
            // The source cursor checks the breaker at its frame boundaries.
            if (keyTranslator == null) {
                while (cursor.hasNext()) {
                    appendRow(record.getInt(keyColumn), record);
                }
            } else {
                while (cursor.hasNext()) {
                    final int key = keyTranslator.translate(record.getInt(keyColumn));
                    if (key != SymbolTable.VALUE_NOT_FOUND) {
                        appendRow(key, record);
                    }
                }
            }
            return freeze(cursor);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    /** Only call after every probe is drained and aggregate output is finished. */
    @Override
    public void close() {
        if (frozen != null) {
            // Do not retain the borrowed source past its execution.
            frozen.symbols = null;
            frozen = null;
        }
        open = false;
        keys.close();
        heap.close();
        keyCount = keySlotCount = 0;
        circuitBreaker = null;
    }

    /** Ends mutation of a build without SYMBOL payloads. */
    public FrozenHashJoinBuild.IntKeyed freeze() {
        return freeze(null);
    }

    /**
     * Ends mutation. Views resolve SYMBOL payloads through the borrowed source until close.
     * Publication to probe workers is the caller's responsibility.
     */
    public FrozenHashJoinBuild.IntKeyed freeze(@Nullable SymbolTableSource symbolSource) {
        requireBuilding();
        try {
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
            if (symbolSource == null && heap.hasSymbolPayload()) {
                throw new IllegalArgumentException("hash join build with SYMBOL payload requires a symbol source");
            }
            frozen = reusableFrozen != null ? reusableFrozen : new Frozen();
            frozen.of(symbolSource);
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
    private void appendRow(int key, Record record) {
        long slot = findKeySlot(keys.address, keySlotCount, key);
        int previous = Unsafe.getInt(slot + 4);
        if (previous == 0 && keyCount == keySlotCount / 2) {
            growKeyTable();
            keySlotCount *= 2;
            slot = findKeySlot(keys.address, keySlotCount, key);
        }
        final long offset = heap.append(record, toRowLink(previous));
        Unsafe.putInt(slot, key);
        Unsafe.putInt(slot + 4, CompressedOffsets.compressBiased8(offset));
        if (previous == 0) {
            keyCount++;
        }
    }

    private void growKeyTable() {
        final int slots = keySlotCount;
        if (slots == MAX_SLOTS) {
            throw CairoException.nonCritical().put("hash join build capacity overflow");
        }
        // Separate destination keeps both allocations charged throughout rehashing.
        final HashJoinBuffer dest = keys.scratch();
        try {
            dest.allocate((long) slots * 2 * SLOT_SIZE, true);
            for (int i = 0; i < slots; i++) {
                if ((i & (KEY_SLOTS_PER_CHECK - 1)) == 0) {
                    circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
                }
                long src = keys.address + (long) i * SLOT_SIZE;
                int value = Unsafe.getInt(src + 4);
                if (value != 0) {
                    int key = Unsafe.getInt(src);
                    int index = (int) Hash.hashInt64(key) & (slots * 2 - 1);
                    long target = dest.address + (long) index * SLOT_SIZE;
                    while (Unsafe.getInt(target + 4) != 0) {
                        index = (index + 1) & (slots * 2 - 1);
                        target = dest.address + (long) index * SLOT_SIZE;
                    }
                    Unsafe.putInt(target, key);
                    Unsafe.putInt(target + 4, value);
                }
            }
            keys.take(dest);
        } finally {
            dest.close();
        }
    }

    private void requireBuilding() {
        if (!open || frozen != null) {
            throw new IllegalStateException("hash join build is not mutable");
        }
    }

    private class Frozen implements FrozenHashJoinBuild.IntKeyed {
        private long generation;
        private long handleBase;
        private long keysAddress;
        private int keysCount;
        private long rowsAddress;
        private long rowsCount;
        private long size;
        private int slots;
        private SymbolTableSource symbols;

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

        private void of(SymbolTableSource symbolSource) {
            handleBase = heap.nextHandleBase();
            keysAddress = keys.address;
            keysCount = keyCount;
            slots = keySlotCount;
            rowsAddress = heap.getAddress();
            rowsCount = heap.getRowCount();
            size = IntHashJoinBuild.this.getSizeInBytes();
            symbols = symbolSource;
            generation = heap.freeze();
        }

        private class View extends AbstractHashJoinProbe implements FrozenHashJoinBuild.IntProbe {
            private long lookupKeysAddress;
            private int lookupMask;

            private View() {
                super(heap);
                reopen();
            }

            @Override
            public void find(int key) {
                assert isCurrent();
                long slot = findKeySlot(keysAddress, slots, key);
                next = toRowLink(Unsafe.getInt(slot + 4));
                record.address = 0;
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
                record.address = payloadRowsAddress + CompressedOffsets.uncompressBiased8(head);
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
                ofSnapshot(symbols, handleBase, rowsAddress, rowsCount, generation);
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
