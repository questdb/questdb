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
import io.questdb.std.Hash;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
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
 * Linear probing at a maximum load of 1/2. A slot holds an INT key and a LONG
 * payload offset plus one (zero marks an unused slot). Thus zero, negative and
 * INT_NULL keys need no special representation and null keys match each other.
 * Rows hold a previous-match offset plus one followed by aligned typed payloads.
 * Duplicate iteration is in reverse input order, as in the light join's LongChain.
 * <p>
 * SYMBOLs are interned by text in one owned UTF-16 dictionary shared by all payload
 * columns. Source symbol IDs and record/string flyweights are never retained.
 * Hash tables, rows, dictionary indexes and characters all use tracked native
 * buffers. Growth accounts for both old and new allocations and is cancellable.
 * See docs/parallel-hash-join-group-by-build.md for bounds and lifetime details.
 */
public final class IntHashJoinBuild implements Closeable {
    private static final long COPY_CHUNK_SIZE = 1024 * 1024;
    private static final int MAX_SLOTS = 1 << 30;
    private static final long MAX_BUFFER_SIZE = 1L << 48;
    private static final int SLOT_SIZE = 16;
    private final int initialSlots;
    private final long initialRowCapacity;
    private final Buffer keys = new Buffer();
    private final int[] offsets;
    private final Buffer rows = new Buffer();
    private final int rowSize;
    private final int[] sourceColumns;
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
    private long rowCount;
    private long rowBytes;
    private long symbolBytes;
    private int symbolCount;
    private int symbolSlotCount;

    /** Payload types/indexes are in the same order; indexes address the source record. */
    public IntHashJoinBuild(ColumnTypes payloadTypes, IntList sourceColumns, int initialSlots, long initialRowCapacity) {
        if (initialSlots < 2 || initialSlots > MAX_SLOTS || Integer.bitCount(initialSlots) != 1
                || initialRowCapacity < 1 || initialRowCapacity > MAX_BUFFER_SIZE
                || payloadTypes.getColumnCount() != sourceColumns.size()) {
            throw new IllegalArgumentException("invalid hash join build capacity or payload mapping");
        }
        this.initialSlots = initialSlots;
        this.initialRowCapacity = initialRowCapacity;
        int columnCount = payloadTypes.getColumnCount();
        this.offsets = new int[columnCount];
        this.sourceColumns = new int[columnCount];
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
    }

    /** Copies one row. On failure all execution allocations are released. */
    public void append(int key, Record record) {
        requireBuilding();
        try {
            circuitBreaker.statefulThrowExceptionIfTripped();
            long slot = findKeySlot(keys.address, keySlotCount, key, circuitBreaker);
            if (Unsafe.getLong(slot + 8) == 0 && keyCount == keySlotCount / 2) {
                growTable(keys, keySlotCount);
                keySlotCount *= 2;
                slot = findKeySlot(keys.address, keySlotCount, key, circuitBreaker);
            }
            rows.ensure(rowBytes + rowSize, initialRowCapacity);
            long address = rows.address + rowBytes;
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
                    case ColumnType.SYMBOL -> Unsafe.putInt(dest, intern(record.getSymA(column)));
                    default -> throw new AssertionError();
                }
            }
            long previous = Unsafe.getLong(slot + 8);
            Unsafe.putLong(address, previous);
            Unsafe.putInt(slot, key);
            Unsafe.putLong(slot + 8, rowBytes + 1);
            if (previous == 0) {
                keyCount++;
            }
            rowBytes += rowSize;
            rowCount++;
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    /** Consumes a borrowed cursor once. The caller retains ownership of the cursor. */
    public FrozenHashJoinBuild build(RecordCursor cursor, int keyColumn) {
        requireBuilding();
        try {
            Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                append(record.getInt(keyColumn), record);
            }
            return freeze();
        } catch (Throwable th) {
            close();
            throw th;
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
        rowCount = rowBytes = symbolBytes = 0;
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
            frozen = new Frozen();
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

    private static long findKeySlot(long base, int slots, int key, SqlExecutionCircuitBreaker circuitBreaker) {
        int index = (int) Hash.hashInt64(key) & (slots - 1);
        long address = base + (long) index * SLOT_SIZE;
        while (Unsafe.getLong(address + 8) != 0 && Unsafe.getInt(address) != key) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            index = (index + 1) & (slots - 1);
            address = base + (long) index * SLOT_SIZE;
        }
        return address;
    }

    private void growTable(Buffer table, int slots) {
        if (slots == MAX_SLOTS) {
            throw CairoException.nonCritical().put("hash join build capacity overflow");
        }
        // Separate destination keeps both allocations charged throughout rehashing.
        Buffer dest = new Buffer();
        try {
            dest.allocate((long) slots * 2 * SLOT_SIZE, true);
            for (int i = 0; i < slots; i++) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                long src = table.address + (long) i * SLOT_SIZE;
                long value = Unsafe.getLong(src + 8);
                if (value != 0) {
                    int key = Unsafe.getInt(src);
                    // Symbol hashes can repeat, so rehash must seek an EMPTY slot.
                    int index = (int) Hash.hashInt64(key) & (slots * 2 - 1);
                    long target = dest.address + (long) index * SLOT_SIZE;
                    while (Unsafe.getLong(target + 8) != 0) {
                        circuitBreaker.statefulThrowExceptionIfTripped();
                        index = (index + 1) & (slots * 2 - 1);
                        target = dest.address + (long) index * SLOT_SIZE;
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
            if ((i & 1023) == 0) {
                circuitBreaker.statefulThrowExceptionIfTripped();
            }
            hash = 31 * hash + value.charAt(i);
        }
        if (symbolSlotCount == 0) {
            symbolSlots.allocate((long) initialSlots * SLOT_SIZE, true);
            symbolSlotCount = initialSlots;
        }
        int index = (int) Hash.hashInt64(hash) & (symbolSlotCount - 1);
        long slot = symbolSlots.address + (long) index * SLOT_SIZE;
        long entry;
        while ((entry = Unsafe.getLong(slot + 8)) != 0) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            if (Unsafe.getInt(slot) == hash && symbolEquals((int) entry - 1, value)) {
                return (int) entry - 1;
            }
            index = (index + 1) & (symbolSlotCount - 1);
            slot = symbolSlots.address + (long) index * SLOT_SIZE;
        }
        if (symbolCount == symbolSlotCount / 2) {
            growTable(symbolSlots, symbolSlotCount);
            symbolSlotCount *= 2;
            index = (int) Hash.hashInt64(hash) & (symbolSlotCount - 1);
            slot = symbolSlots.address + (long) index * SLOT_SIZE;
            while (Unsafe.getLong(slot + 8) != 0) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                index = (index + 1) & (symbolSlotCount - 1);
                slot = symbolSlots.address + (long) index * SLOT_SIZE;
            }
        }
        symbolEntries.ensure(((long) symbolCount + 1) * 16, 64);
        symbolChars.ensure(symbolBytes + (long) len * 2, 64);
        for (int i = 0; i < len; i++) {
            if ((i & 1023) == 0) {
                circuitBreaker.statefulThrowExceptionIfTripped();
            }
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
            if ((i & 1023) == 0) {
                circuitBreaker.statefulThrowExceptionIfTripped();
            }
            if (Unsafe.getChar(address + (long) i * 2) != value.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    private class Buffer implements Closeable {
        private long address;
        private long capacity;

        public void allocate(long size, boolean clear) {
            address = Unsafe.malloc(size, MemoryTag.NATIVE_JOIN_MAP, memoryTracker);
            capacity = size;
            if (clear) {
                for (long offset = 0; offset < size; offset += COPY_CHUNK_SIZE) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
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
            if (required > MAX_BUFFER_SIZE || required < 0) {
                throw CairoException.nonCritical().put("hash join build buffer overflow");
            }
            if (required <= capacity) {
                return;
            }
            Buffer dest = new Buffer();
            try {
                dest.allocate(Math.max(required, Math.min(MAX_BUFFER_SIZE, Math.max(initialCapacity, capacity * 2))), false);
                for (long offset = 0; offset < capacity; offset += COPY_CHUNK_SIZE) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
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
        private final long charsAddress = symbolChars.address;
        private final long entriesAddress = symbolEntries.address;
        private final long keysAddress = keys.address;
        private final int keysCount = keyCount;
        private final int slots = keySlotCount;
        private final long rowsAddress = rows.address;
        private final long rowsCount = rowCount;
        private final long size = IntHashJoinBuild.this.getSizeInBytes();
        private final int symbolsCount = symbolCount;

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
        public Probe newProbe(SqlExecutionCircuitBreaker circuitBreaker) {
            if (frozen != this) {
                throw new IllegalStateException("hash join build has expired");
            }
            return new View(circuitBreaker);
        }

        private class Symbols implements SymbolTable {
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
                assert frozen == Frozen.this;
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
            private final SqlExecutionCircuitBreaker circuitBreaker;
            private final PayloadRecord record = new PayloadRecord();
            private final SymbolTable[] symbolTables = new SymbolTable[types.length];
            private long next;

            private View(SqlExecutionCircuitBreaker circuitBreaker) {
                this.circuitBreaker = circuitBreaker;
                for (int i = 0; i < types.length; i++) {
                    if (types[i] == ColumnType.SYMBOL) {
                        symbolTables[i] = new Symbols();
                    }
                }
            }

            @Override
            public void find(int key) {
                assert frozen == Frozen.this;
                circuitBreaker.statefulThrowExceptionIfTripped();
                long slot = findKeySlot(keysAddress, slots, key, circuitBreaker);
                next = Unsafe.getLong(slot + 8);
                record.address = 0;
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
                return new Symbols();
            }

            @Override
            public long next() {
                assert frozen == Frozen.this;
                if (next == 0) {
                    throw new IllegalStateException("hash join probe is exhausted");
                }
                circuitBreaker.statefulThrowExceptionIfTripped();
                long handle = next - 1;
                recordAt(handle);
                next = Unsafe.getLong(record.address);
                return handle;
            }

            @Override
            public void recordAt(long handle) {
                assert frozen == Frozen.this;
                assert handle >= 0 && handle % rowSize == 0 && handle / rowSize < rowsCount;
                record.address = rowsAddress + handle;
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
                    assert frozen == Frozen.this && address != 0;
                    return address + offsets[col];
                }
            }
        }
    }
}
