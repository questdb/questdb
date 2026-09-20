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
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.engine.CompressedOffsets;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

/**
 * Payload rows of a hash join build: the lookup structure holds keys, this holds the rows
 * those keys point at. {@link IntHashJoinBuild} keys it with an open-addressed INT table and
 * {@link MapHashJoinBuild} with a {@link io.questdb.cairo.map.Map}; both store the same
 * compressed offset of a chain head and share every byte of the row layout below.
 * <p>
 * A row is an eight-byte previous-match link (a byte offset plus eight, zero for a chain end)
 * followed by naturally aligned typed payloads. The heap is bounded by
 * {@link CompressedOffsets#MAX_ALIGNED8_HEAP_SIZE} before allocation or encoding, so every
 * row offset round trips through {@link CompressedOffsets#compressBiased8(long)}. Duplicate
 * iteration follows the links in reverse input order, as the light join's LongChain does.
 * <p>
 * SYMBOL payloads store the source's symbol keys, like INT payloads; {@link PayloadRecord}
 * resolves them through the build source's symbol tables, which the source keeps valid until
 * the build closes.
 * <p>
 * The heap is owner-built and frozen for the execution. {@link #freeze()} publishes it and
 * hands out the generation that every payload record asserts against, so that a record of an
 * expired execution faults instead of reading a freed or re-filled row.
 */
final class HashJoinRowHeap implements Closeable {
    private final boolean hasSymbolPayload;
    private final long initialCapacity;
    private final int[] offsets;
    private final HashJoinBuffer rows = new HashJoinBuffer(CompressedOffsets.MAX_ALIGNED8_HEAP_SIZE);
    private final int rowSize;
    private final int[] sourceColumns;
    private final int[] types;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private long generation;
    private long nextHandleBase;
    private long rowBytes;

    /** Payload types and indexes are in the same order; indexes address the source record. */
    HashJoinRowHeap(ColumnTypes payloadTypes, IntList sourceColumns, long initialCapacity) {
        if (initialCapacity < 1 || initialCapacity > CompressedOffsets.MAX_ALIGNED8_HEAP_SIZE
                || payloadTypes.getColumnCount() != sourceColumns.size()) {
            throw new IllegalArgumentException("invalid hash join build capacity or payload mapping");
        }
        this.initialCapacity = initialCapacity;
        final int columnCount = payloadTypes.getColumnCount();
        this.offsets = new int[columnCount];
        this.sourceColumns = new int[columnCount];
        this.types = new int[columnCount];
        boolean hasSymbolPayload = false;
        long offset = Long.BYTES;
        for (int i = 0; i < columnCount; i++) {
            final int type = ColumnType.tagOf(payloadTypes.getColumnType(i));
            final int size = switch (type) {
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
            hasSymbolPayload |= type == ColumnType.SYMBOL;
            offset += size;
        }
        this.hasSymbolPayload = hasSymbolPayload;
        rowSize = (int) ((offset + 7) & -8L);
    }

    /**
     * Copies one row and returns its byte offset, which the caller compresses into the key's
     * chain head. {@code link} is the row link of the match this row displaces: zero for a new
     * key, otherwise the previous head widened through
     * {@link CompressedOffsets#uncompressAligned8(int)}.
     */
    long append(Record record, long link) {
        final long offset = rowBytes;
        final long required = offset + rowSize;
        rows.ensure(required, initialCapacity);
        final long address = rows.address + offset;
        for (int i = 0; i < types.length; i++) {
            final long dest = address + offsets[i];
            final int column = sourceColumns[i];
            switch (types[i]) {
                case ColumnType.BOOLEAN -> Unsafe.putByte(dest, (byte) (record.getBool(column) ? 1 : 0));
                case ColumnType.BYTE -> Unsafe.putByte(dest, record.getByte(column));
                case ColumnType.SHORT -> Unsafe.putShort(dest, record.getShort(column));
                case ColumnType.CHAR -> Unsafe.putChar(dest, record.getChar(column));
                // A SYMBOL payload keeps the source key; records resolve it through the source.
                case ColumnType.INT, ColumnType.SYMBOL -> Unsafe.putInt(dest, record.getInt(column));
                case ColumnType.LONG -> Unsafe.putLong(dest, record.getLong(column));
                case ColumnType.DATE -> Unsafe.putLong(dest, record.getDate(column));
                case ColumnType.TIMESTAMP -> Unsafe.putLong(dest, record.getTimestamp(column));
                case ColumnType.FLOAT -> Unsafe.putFloat(dest, record.getFloat(column));
                case ColumnType.DOUBLE -> Unsafe.putDouble(dest, record.getDouble(column));
                default -> throw new AssertionError();
            }
        }
        Unsafe.putLong(address, link);
        rowBytes = required;
        return offset;
    }

    /** Releases the rows and expires every record of this execution. */
    @Override
    public void close() {
        rows.close();
        rowBytes = 0;
        generation++;
        circuitBreaker = null;
    }

    /** Ends mutation and returns the generation that this execution's records assert against. */
    long freeze() {
        return ++generation;
    }

    long getAddress() {
        return rows.address;
    }

    int getColumnCount() {
        return types.length;
    }

    long getGeneration() {
        return generation;
    }

    long getRowCount() {
        return rowBytes / rowSize;
    }

    int getRowSize() {
        return rowSize;
    }

    /** Allocated native bytes, including unused capacity. */
    long getSizeInBytes() {
        return rows.capacity;
    }

    /** The build source's column index behind payload column {@code col}. */
    int getSourceColumn(int col) {
        return sourceColumns[col];
    }

    /** True when at least one payload column is a SYMBOL, so that a build needs a symbol source. */
    boolean hasSymbolPayload() {
        return hasSymbolPayload;
    }

    boolean isSymbol(int col) {
        return types[col] == ColumnType.SYMBOL;
    }

    /** Each probe needs its own record: they carry independent addresses and symbol views. */
    PayloadRecord newRecord() {
        return new PayloadRecord();
    }

    /** Takes the handle base of one execution, so that handles never repeat across executions. */
    long nextHandleBase() {
        if (nextHandleBase > Long.MAX_VALUE - rowBytes - 1) {
            throw CairoException.nonCritical().put("hash join handle capacity overflow");
        }
        final long handleBase = nextHandleBase;
        nextHandleBase += rowBytes + 1;
        return handleBase;
    }

    /** Binds the execution that charges and cancels this heap's allocations. */
    void of(@Nullable MemoryTracker memoryTracker, SqlExecutionCircuitBreaker circuitBreaker) {
        this.circuitBreaker = circuitBreaker;
        rows.of(memoryTracker, circuitBreaker);
    }

    /** Presizes the heap for a known row count, so that appends of that many rows do not grow it. */
    void reserve(long rowCount) {
        circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
        if (rowCount > (CompressedOffsets.MAX_ALIGNED8_HEAP_SIZE - rowBytes) / rowSize) {
            throw CairoException.nonCritical().put("hash join build buffer overflow");
        }
        rows.ensure(rowBytes + rowCount * rowSize, initialCapacity);
    }

    /**
     * Payload columns of one row, read through the copied heap. The record is slot-local: it
     * carries its own address and its own symbol table views, which only the build source can
     * hand out per slot.
     */
    final class PayloadRecord implements Record {
        private final SymbolTable[] symbolTables = new SymbolTable[types.length];
        // Written by the probe on every advance; zero until the first match is positioned.
        long address;
        private long probeGeneration;

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

        public SymbolTable getSymbolTable(int col) {
            assert probeGeneration == generation && types[col] == ColumnType.SYMBOL;
            return symbolTables[col];
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

        /** Drops the execution's symbol views; the record is usable again after {@link #of}. */
        void clear() {
            address = 0;
            // No generation the heap hands out is negative, so a read before the next of()
            // trips an assertion rather than reaching an expired execution's rows.
            probeGeneration = -1;
            for (int i = 0; i < symbolTables.length; i++) {
                symbolTables[i] = null;
            }
        }

        /**
         * Rebinds the record to the frozen execution that {@code generation} names. Symbol views
         * of the previous execution's source expired with it, so each one is taken anew; only a
         * build with SYMBOL payloads needs a source.
         */
        void of(@Nullable SymbolTableSource symbols, long generation) {
            this.probeGeneration = generation;
            address = 0;
            for (int i = 0; i < types.length; i++) {
                if (types[i] == ColumnType.SYMBOL) {
                    symbolTables[i] = symbols.newSymbolTable(sourceColumns[i]);
                }
            }
        }

        private long at(int col) {
            assert address != 0 && probeGeneration == generation;
            return address + offsets[col];
        }
    }
}
