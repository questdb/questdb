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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.engine.CompressedOffsets;
import io.questdb.std.Hash;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.DirectString;
import org.jetbrains.annotations.Nullable;

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
 * match each other. Rows retain eight-byte previous-match links (byte offset
 * plus eight, zero for chain end), followed by naturally aligned typed payloads.
 * Widening a slot head only scales its unsigned value; duplicate advances need
 * no offset decoding. The row heap is bounded by
 * {@link CompressedOffsets#MAX_ALIGNED8_HEAP_SIZE} before allocation or encoding.
 * Duplicate iteration is in reverse input order, as in the light join's LongChain.
 * <p>
 * SYMBOLs are interned by text in one owned UTF-16 dictionary shared by all payload
 * columns. Source symbol IDs and record/string flyweights are never retained.
 * Hash tables, rows, dictionary indexes and characters all use tracked native
 * buffers. Growth accounts for both old and new allocations and is cancellable.
 * Frozen views borrow these buffers until close; see {@link FrozenHashJoinBuild}.
 */
public final class IntHashJoinBuild implements Closeable {
    // Growth loops (rehash, clear, copy) are not bounded by a row; they check the breaker once per MiB they touch.
    private static final long COPY_CHUNK_SIZE = 1024 * 1024;
    private static final int MAX_SLOTS = 1 << 30;
    private static final long MAX_BUFFER_SIZE = 1L << 48;
    private static final int SLOT_SIZE = 8;
    private static final int KEY_SLOTS_PER_CHECK = (int) (COPY_CHUNK_SIZE / SLOT_SIZE);
    private static final int SYMBOL_SLOT_SIZE = 16;
    private static final int SYMBOL_SLOTS_PER_CHECK = (int) (COPY_CHUNK_SIZE / SYMBOL_SLOT_SIZE);
    private final int initialSlots;
    private final long initialRowCapacity;
    private final Buffer keys = new Buffer();
    private final int[] offsets;
    private final Buffer rows = new Buffer();
    private final Buffer scratch = new Buffer();
    private final Frozen reusableFrozen;
    private final int rowSize;
    private final int[] sourceColumns;
    private final ObjList<SymbolTable> sourceSymbols = new ObjList<>();
    private final Buffer symbolChars = new Buffer();
    private final Buffer symbolEntries = new Buffer();
    private final Buffer symbolSlots = new Buffer();
    private final int[] types;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private Frozen frozen;
    private int keyCount;
    private int keySlotCount;
    @Nullable
    private MemoryTracker memoryTracker;
    private boolean open;
    private long nextHandleBase;
    private long rowBytes;
    private long symbolBytes;
    private int symbolCount;
    private int symbolSlotCount;

    /** Payload types/indexes are in the same order; indexes address the source record. */
    public IntHashJoinBuild(ColumnTypes payloadTypes, IntList sourceColumns, int initialSlots, long initialRowCapacity) {
        this(payloadTypes, sourceColumns, initialSlots, initialRowCapacity, false);
    }

    /**
     * Reusable mode is for a factory that drains all consumers before reopening.
     * Its snapshot is a flyweight; retained probes must explicitly reopen for each
     * execution. The ordinary constructor keeps execution-specific snapshots.
     */
    public IntHashJoinBuild(ColumnTypes payloadTypes, IntList sourceColumns, int initialSlots, long initialRowCapacity, boolean reusable) {
        if (initialSlots < 2 || initialSlots > MAX_SLOTS || Integer.bitCount(initialSlots) != 1
                || initialRowCapacity < 1 || initialRowCapacity > CompressedOffsets.MAX_ALIGNED8_HEAP_SIZE
                || payloadTypes.getColumnCount() != sourceColumns.size()) {
            throw new IllegalArgumentException("invalid hash join build capacity or payload mapping");
        }
        this.initialSlots = initialSlots;
        this.initialRowCapacity = initialRowCapacity;
        int columnCount = payloadTypes.getColumnCount();
        this.offsets = new int[columnCount];
        this.sourceColumns = new int[columnCount];
        this.sourceSymbols.setAll(columnCount, null);
        this.types = new int[columnCount];
        long offset = Long.BYTES;
        for (int i = 0; i < columnCount; i++) {
            int type = ColumnType.tagOf(payloadTypes.getColumnType(i));
            int size = switch (type) {
                case ColumnType.BOOLEAN, ColumnType.BYTE -> 1;
                case ColumnType.SHORT, ColumnType.CHAR -> 2;
                case ColumnType.INT, ColumnType.FLOAT, ColumnType.SYMBOL -> 4;
                case ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP, ColumnType.DOUBLE -> 8;
                default -> throw new IllegalArgumentException("unsupported hash join payload type: " + ColumnType.nameOf(type));
            };
            offset = (offset + size - 1) & -size;
            if (offset + size > Integer.MAX_VALUE - 7 || sourceColumns.getQuick(i) < 0) {
                throw new IllegalArgumentException("invalid hash join payload layout");
            }
            offsets[i] = (int) offset;
            this.sourceColumns[i] = sourceColumns.getQuick(i);
            types[i] = type;
            offset += size;
        }
        rowSize = (int) ((offset + 7) & -8L);
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

    /** Consumes a borrowed cursor once. The caller retains ownership of the cursor. */
    public FrozenHashJoinBuild build(RecordCursor cursor, int keyColumn) {
        return build(cursor, keyColumn, -1);
    }

    /** A nonnegative hint is the remaining row count of a freshly acquired cursor. */
    public FrozenHashJoinBuild build(RecordCursor cursor, int keyColumn, long rowCountHint) {
        requireBuilding();
        try {
            if (rowCountHint > 0) {
                circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
                if (rowCountHint > (CompressedOffsets.MAX_ALIGNED8_HEAP_SIZE - rowBytes) / rowSize) {
                    throw CairoException.nonCritical().put("hash join build buffer overflow");
                }
                rows.ensure(rowBytes + rowCountHint * rowSize, initialRowCapacity);
            }
            // Independent views read the source's native dictionary. Calling getSymA
            // on a cached table record would retain one Java String per symbol.
            for (int i = 0; i < types.length; i++) {
                if (types[i] == ColumnType.SYMBOL) {
                    sourceSymbols.setQuick(i, cursor.newSymbolTable(sourceColumns[i]));
                }
            }
            Record record = cursor.getRecord();
            // The source cursor checks the breaker at its frame boundaries.
            while (cursor.hasNext()) {
                appendRow(record.getInt(keyColumn), record);
            }
            return freeze();
        } catch (Throwable th) {
            close();
            throw th;
        } finally {
            for (int i = 0; i < sourceSymbols.size(); i++) {
                sourceSymbols.setQuick(i, Misc.freeIfCloseable(sourceSymbols.getQuick(i)));
            }
        }
    }

    /** Only call after every probe is drained and aggregate output is finished. */
    @Override
    public void close() {
        frozen = null;
        open = false;
        keys.close();
        rows.close();
        symbolSlots.close();
        symbolEntries.close();
        symbolChars.close();
        rowBytes = symbolBytes = 0;
        keyCount = keySlotCount = symbolCount = symbolSlotCount = 0;
        memoryTracker = null;
        circuitBreaker = null;
    }

    /** Ends mutation. Publication to probe workers is the caller's responsibility. */
    public FrozenHashJoinBuild freeze() {
        requireBuilding();
        try {
            circuitBreaker.statefulThrowExceptionIfTripped();
            // Text interning is build-only; readers resolve IDs through symbolEntries.
            symbolSlots.close();
            frozen = reusableFrozen != null ? reusableFrozen : new Frozen();
            frozen.of();
            return frozen;
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    public long getSizeInBytes() {
        return keys.capacity + rows.capacity + symbolSlots.capacity + symbolEntries.capacity + symbolChars.capacity;
    }

    /** Reopens a closed skeleton for a fresh execution, with fresh symbol IDs. */
    public void open(@Nullable MemoryTracker memoryTracker, SqlExecutionCircuitBreaker circuitBreaker) {
        if (open) {
            throw new IllegalStateException("hash join build is already open");
        }
        this.memoryTracker = memoryTracker;
        this.circuitBreaker = circuitBreaker;
        try {
            circuitBreaker.statefulThrowExceptionIfTripped();
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

    // The caller owns failure cleanup. Growth checks the breaker per MiB of rehashed or copied memory.
    private void appendRow(int key, Record record) {
        long slot = findKeySlot(keys.address, keySlotCount, key);
        int previous = Unsafe.getInt(slot + 4);
        if (previous == 0 && keyCount == keySlotCount / 2) {
            growKeyTable();
            keySlotCount *= 2;
            slot = findKeySlot(keys.address, keySlotCount, key);
        }
        final long offset = rowBytes;
        final long required = offset + rowSize;
        rows.ensure(required, initialRowCapacity);
        long address = rows.address + offset;
        for (int i = 0; i < types.length; i++) {
            long dest = address + offsets[i];
            int column = sourceColumns[i];
            switch (types[i]) {
                case ColumnType.BOOLEAN -> Unsafe.putByte(dest, (byte) (record.getBool(column) ? 1 : 0));
                case ColumnType.BYTE -> Unsafe.putByte(dest, record.getByte(column));
                case ColumnType.SHORT -> Unsafe.putShort(dest, record.getShort(column));
                case ColumnType.CHAR -> Unsafe.putChar(dest, record.getChar(column));
                case ColumnType.INT -> Unsafe.putInt(dest, record.getInt(column));
                case ColumnType.LONG -> Unsafe.putLong(dest, record.getLong(column));
                case ColumnType.DATE -> Unsafe.putLong(dest, record.getDate(column));
                case ColumnType.TIMESTAMP -> Unsafe.putLong(dest, record.getTimestamp(column));
                case ColumnType.FLOAT -> Unsafe.putFloat(dest, record.getFloat(column));
                case ColumnType.DOUBLE -> Unsafe.putDouble(dest, record.getDouble(column));
                case ColumnType.SYMBOL -> Unsafe.putInt(dest, intern(sourceSymbols.getQuick(i) != null
                        ? sourceSymbols.getQuick(i).valueOf(record.getInt(column))
                        : record.getSymA(column)));
                default -> throw new AssertionError();
            }
        }
        Unsafe.putLong(address, toRowLink(previous));
        Unsafe.putInt(slot, key);
        Unsafe.putInt(slot + 4, CompressedOffsets.compressBiased8(offset));
        if (previous == 0) {
            keyCount++;
        }
        rowBytes = required;
    }

    private void growKeyTable() {
        final Buffer table = keys;
        final int slots = keySlotCount;
        if (slots == MAX_SLOTS) {
            throw CairoException.nonCritical().put("hash join build capacity overflow");
        }
        // Separate destination keeps both allocations charged throughout rehashing.
        Buffer dest = scratch;
        try {
            dest.allocate((long) slots * 2 * SLOT_SIZE, true);
            for (int i = 0; i < slots; i++) {
                if ((i & (KEY_SLOTS_PER_CHECK - 1)) == 0) {
                    circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
                }
                long src = table.address + (long) i * SLOT_SIZE;
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
            table.close();
            table.take(dest);
        } finally {
            dest.close();
        }
    }

    private void growSymbolTable() {
        final Buffer table = symbolSlots;
        final int slots = symbolSlotCount;
        if (slots == MAX_SLOTS) {
            throw CairoException.nonCritical().put("hash join build capacity overflow");
        }
        // Separate destination keeps both allocations charged throughout rehashing.
        Buffer dest = scratch;
        try {
            dest.allocate((long) slots * 2 * SYMBOL_SLOT_SIZE, true);
            for (int i = 0; i < slots; i++) {
                if ((i & (SYMBOL_SLOTS_PER_CHECK - 1)) == 0) {
                    circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
                }
                long src = table.address + (long) i * SYMBOL_SLOT_SIZE;
                long value = Unsafe.getLong(src + 8);
                if (value != 0) {
                    int key = Unsafe.getInt(src);
                    // Symbol hashes can repeat, so rehash must seek an EMPTY slot.
                    int index = (int) Hash.hashInt64(key) & (slots * 2 - 1);
                    long target = dest.address + (long) index * SYMBOL_SLOT_SIZE;
                    while (Unsafe.getLong(target + 8) != 0) {
                        index = (index + 1) & (slots * 2 - 1);
                        target = dest.address + (long) index * SYMBOL_SLOT_SIZE;
                    }
                    Unsafe.putInt(target, key);
                    Unsafe.putLong(target + 8, value);
                }
            }
            table.close();
            table.take(dest);
        } finally {
            dest.close();
        }
    }

    private int intern(@Nullable CharSequence value) {
        if (value == null) {
            return SymbolTable.VALUE_IS_NULL;
        }
        int len = value.length();
        int hash = 0;
        for (int i = 0; i < len; i++) {
            hash = 31 * hash + value.charAt(i);
        }
        if (symbolSlotCount == 0) {
            symbolSlots.allocate((long) initialSlots * SYMBOL_SLOT_SIZE, true);
            symbolSlotCount = initialSlots;
        }
        int index = (int) Hash.hashInt64(hash) & (symbolSlotCount - 1);
        long slot = symbolSlots.address + (long) index * SYMBOL_SLOT_SIZE;
        long entry;
        while ((entry = Unsafe.getLong(slot + 8)) != 0) {
            if (Unsafe.getInt(slot) == hash && symbolEquals((int) entry - 1, value)) {
                return (int) entry - 1;
            }
            index = (index + 1) & (symbolSlotCount - 1);
            slot = symbolSlots.address + (long) index * SYMBOL_SLOT_SIZE;
        }
        if (symbolCount == symbolSlotCount / 2) {
            growSymbolTable();
            symbolSlotCount *= 2;
            index = (int) Hash.hashInt64(hash) & (symbolSlotCount - 1);
            slot = symbolSlots.address + (long) index * SYMBOL_SLOT_SIZE;
            while (Unsafe.getLong(slot + 8) != 0) {
                index = (index + 1) & (symbolSlotCount - 1);
                slot = symbolSlots.address + (long) index * SYMBOL_SLOT_SIZE;
            }
        }
        symbolEntries.ensure(((long) symbolCount + 1) * 16, 64);
        symbolChars.ensure(symbolBytes + (long) len * 2, 64);
        for (int i = 0; i < len; i++) {
            Unsafe.putChar(symbolChars.address + symbolBytes + (long) i * 2, value.charAt(i));
        }
        entry = symbolEntries.address + (long) symbolCount * 16;
        Unsafe.putLong(entry, symbolBytes);
        Unsafe.putInt(entry + 8, len);
        Unsafe.putInt(slot, hash);
        Unsafe.putLong(slot + 8, (long) symbolCount + 1);
        symbolBytes += (long) len * 2;
        return symbolCount++;
    }

    private void requireBuilding() {
        if (!open || frozen != null) {
            throw new IllegalStateException("hash join build is not mutable");
        }
    }

    private boolean symbolEquals(int key, CharSequence value) {
        long entry = symbolEntries.address + (long) key * 16;
        int len = Unsafe.getInt(entry + 8);
        if (len != value.length()) {
            return false;
        }
        long address = symbolChars.address + Unsafe.getLong(entry);
        for (int i = 0; i < len; i++) {
            if (Unsafe.getChar(address + (long) i * 2) != value.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    private static long toRowLink(int head) {
        return CompressedOffsets.uncompressAligned8(head);
    }

    private class Buffer implements Closeable {
        private long address;
        private long capacity;

        public void allocate(long size, boolean clear) {
            address = Unsafe.malloc(size, MemoryTag.NATIVE_JOIN_MAP, memoryTracker);
            capacity = size;
            if (clear) {
                for (long offset = 0; offset < size; offset += COPY_CHUNK_SIZE) {
                    circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
                    Vect.memset(address + offset, Math.min(size - offset, COPY_CHUNK_SIZE), 0);
                }
            }
        }

        @Override
        public void close() {
            if (address != 0) {
                address = Unsafe.free(address, capacity, MemoryTag.NATIVE_JOIN_MAP, memoryTracker);
                capacity = 0;
            }
        }

        public void ensure(long required, long initialCapacity) {
            final long limit = this == rows ? CompressedOffsets.MAX_ALIGNED8_HEAP_SIZE : MAX_BUFFER_SIZE;
            if (required > limit || required < 0) {
                throw CairoException.nonCritical().put("hash join build buffer overflow");
            }
            if (required <= capacity) {
                return;
            }
            Buffer dest = scratch;
            try {
                dest.allocate(Math.max(required, Math.min(limit, Math.max(initialCapacity, capacity * 2))), false);
                for (long offset = 0; offset < capacity; offset += COPY_CHUNK_SIZE) {
                    circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
                    Unsafe.copyMemory(address + offset, dest.address + offset, Math.min(capacity - offset, COPY_CHUNK_SIZE));
                }
                close();
                take(dest);
            } finally {
                dest.close();
            }
        }

        public void take(Buffer other) {
            address = other.address;
            capacity = other.capacity;
            other.address = other.capacity = 0;
        }
    }

    private class Frozen implements FrozenHashJoinBuild {
        private long charsAddress;
        private long entriesAddress;
        private long keysAddress;
        private int keysCount;
        private int slots;
        private long rowsAddress;
        private long rowsCount;
        private long size;
        private int symbolsCount;
        private long generation;
        private long handleBase;

        private void of() {
            if (nextHandleBase > Long.MAX_VALUE - rowBytes - 1) {
                throw CairoException.nonCritical().put("hash join handle capacity overflow");
            }
            handleBase = nextHandleBase;
            nextHandleBase += rowBytes + 1;
            charsAddress = symbolChars.address;
            entriesAddress = symbolEntries.address;
            keysAddress = keys.address;
            keysCount = keyCount;
            slots = keySlotCount;
            rowsAddress = rows.address;
            rowsCount = rowBytes / rowSize;
            size = IntHashJoinBuild.this.getSizeInBytes();
            symbolsCount = symbolCount;
            generation++;
        }

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
        public Probe newProbe() {
            if (frozen != this) {
                throw new IllegalStateException("hash join build has expired");
            }
            return new View();
        }

        private class Symbols implements SymbolTable, QuietCloseable {
            private final ObjList<Symbols> pool;
            private long symbolGeneration;
            private boolean closed;

            private Symbols(ObjList<Symbols> pool) {
                this.pool = pool;
                reopen();
            }

            @Override
            public void close() {
                if (!closed && pool != null) {
                    closed = true;
                    pool.add(this);
                }
            }

            private void reopen() {
                closed = false;
                symbolGeneration = generation;
            }
            private final DirectString a = new DirectString();
            private final DirectString b = new DirectString();

            @Override
            public boolean supportsKeyValueAccess() {
                return true;
            }

            @Override
            public CharSequence valueBOf(int key) {
                return value(key, b);
            }

            @Override
            public CharSequence valueOf(int key) {
                return value(key, a);
            }

            private CharSequence value(int key, DirectString sink) {
                assert frozen == Frozen.this && symbolGeneration == generation && !closed;
                if (key == VALUE_IS_NULL) {
                    return null;
                }
                if (key < 0 || key >= symbolsCount) {
                    throw new IllegalArgumentException("invalid hash join symbol key");
                }
                long entry = entriesAddress + (long) key * 16;
                return sink.of(charsAddress + Unsafe.getLong(entry), Unsafe.getInt(entry + 8));
            }
        }

        private class View implements Probe {
            private final PayloadRecord record = new PayloadRecord();
            private final Symbols[] symbolTables = new Symbols[types.length];
            private final ObjList<Symbols> symbolPool = new ObjList<>();
            private int lookupMask;
            private long lookupKeysAddress;
            private long payloadRowsAddress;
            private long probeGeneration;
            private long next;

            private View() {
                for (int i = 0; i < types.length; i++) {
                    if (types[i] == ColumnType.SYMBOL) {
                        symbolTables[i] = new Symbols(null);
                    }
                }
                reopen();
            }

            @Override
            public void reopen() {
                if (frozen != Frozen.this) {
                    throw new IllegalStateException("hash join build has expired");
                }
                probeGeneration = generation;
                // This view is rebound after the previous execution drains. Cache the
                // immutable native lookup metadata for its entire acquired lifetime.
                lookupMask = slots - 1;
                lookupKeysAddress = keysAddress;
                payloadRowsAddress = rowsAddress;
                next = 0;
                record.address = 0;
                for (int i = 0; i < symbolTables.length; i++) {
                    if (symbolTables[i] != null) {
                        symbolTables[i].reopen();
                    }
                }
            }

            @Override
            public void find(int key) {
                assert frozen == Frozen.this && probeGeneration == generation;
                long slot = findKeySlot(keysAddress, slots, key);
                next = toRowLink(Unsafe.getInt(slot + 4));
                record.address = 0;
            }

            @Override
            public boolean findSingleUnchecked(int key) {
                assert keysCount == rowsCount;
                if (rowsCount == 0) {
                    assert frozen == Frozen.this && probeGeneration == generation;
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
            public Record getRecord() {
                return record;
            }

            @Override
            public SymbolTable getSymbolTable(int columnIndex) {
                assert types[columnIndex] == ColumnType.SYMBOL;
                return symbolTables[columnIndex];
            }

            @Override
            public boolean hasNext() {
                return next != 0;
            }

            @Override
            public SymbolTable newSymbolTable(int columnIndex) {
                assert types[columnIndex] == ColumnType.SYMBOL;
                Symbols symbols = symbolPool.size() > 0 ? symbolPool.getLast() : new Symbols(symbolPool);
                if (symbolPool.size() > 0) {
                    symbolPool.remove(symbolPool.size() - 1);
                }
                symbols.reopen();
                return symbols;
            }

            @Override
            public long next() {
                assert frozen == Frozen.this && probeGeneration == generation;
                if (next == 0) {
                    throw new IllegalStateException("hash join probe is exhausted");
                }
                long handle = handleBase + next - 8;
                record.address = payloadRowsAddress + next - 8;
                next = Unsafe.getLong(record.address);
                return handle;
            }

            @Override
            public void recordAt(long handle) {
                assert frozen == Frozen.this && probeGeneration == generation;
                long offset = handle - handleBase;
                assert offset >= 0 && offset % rowSize == 0 && offset / rowSize < rowsCount;
                record.address = rowsAddress + offset;
            }

            private int findHead(int key) {
                assert frozen == Frozen.this && probeGeneration == generation;
                final int mask = lookupMask;
                final long base = lookupKeysAddress;
                long address = base + ((long) ((int) Hash.hashInt64(key) & mask)) * SLOT_SIZE;
                int head = Unsafe.getInt(address + 4);
                if (head != 0 && Unsafe.getInt(address) != key) {
                    head = findCollision(key, address);
                }
                return head;
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

            private class PayloadRecord implements Record {
                private long address;

                @Override
                public boolean getBool(int col) {
                    return Unsafe.getByte(at(col)) != 0;
                }

                @Override
                public byte getByte(int col) {
                    return Unsafe.getByte(at(col));
                }

                @Override
                public char getChar(int col) {
                    return Unsafe.getChar(at(col));
                }

                @Override
                public long getDate(int col) {
                    return getLong(col);
                }

                @Override
                public double getDouble(int col) {
                    return Unsafe.getDouble(at(col));
                }

                @Override
                public float getFloat(int col) {
                    return Unsafe.getFloat(at(col));
                }

                @Override
                public int getInt(int col) {
                    return Unsafe.getInt(at(col));
                }

                @Override
                public long getLong(int col) {
                    return Unsafe.getLong(at(col));
                }

                @Override
                public short getShort(int col) {
                    return Unsafe.getShort(at(col));
                }

                @Override
                public CharSequence getSymA(int col) {
                    return symbolTables[col].valueOf(getInt(col));
                }

                @Override
                public CharSequence getSymB(int col) {
                    return symbolTables[col].valueBOf(getInt(col));
                }

                @Override
                public long getTimestamp(int col) {
                    return getLong(col);
                }

                private long at(int col) {
                    assert frozen == Frozen.this && probeGeneration == generation && address != 0;
                    return address + offsets[col];
                }
            }
        }
    }
}
