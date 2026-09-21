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
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

/**
 * Payload rows of a hash join build: the lookup structure holds keys, this holds the rows
 * those keys point at. {@link IntHashJoinBuild} keys it with an open-addressed INT table and
 * {@link MapHashJoinBuild} with a {@link io.questdb.cairo.map.Map}; both store the same
 * compressed offset of a chain head and share every byte of the row layout below.
 * <p>
 * A row is an eight-byte previous-match link (a byte offset plus eight, zero for a chain end)
 * followed by typed payloads, each aligned to its own size up to eight bytes; a sixteen- or
 * thirty-two-byte payload is a run of longs, so eight is its natural alignment too. The heap is
 * bounded by {@link CompressedOffsets#MAX_ALIGNED8_HEAP_SIZE} before allocation or encoding, so
 * every row offset round trips through {@link CompressedOffsets#compressBiased8(long)}. Duplicate
 * iteration follows the links in reverse input order, as the light join's LongChain does.
 * <p>
 * SYMBOL payloads store the source's symbol keys, like INT payloads; {@link PayloadRecord}
 * resolves them through the build source's symbol tables, which the source keeps valid until
 * the build closes. A DECIMAL payload stores its raw words and nothing else: the precision and
 * the scale ride in the payload column's type, which the joined metadata carries.
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
    // Scratch sinks of append(), which the owner alone runs: the two wide decimals read through a
    // sink and nothing else. A parallel build would need one pair per builder.
    private Decimal128 decimal128;
    private Decimal256 decimal256;
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
            final int size = payloadSize(type);
            offset = align(offset, size);
            if (offset + size > Integer.MAX_VALUE - 7 || sourceColumns.getQuick(i) < 0) {
                throw new IllegalArgumentException("invalid hash join payload layout");
            }
            offsets[i] = (int) offset;
            this.sourceColumns[i] = sourceColumns.getQuick(i);
            types[i] = type;
            hasSymbolPayload |= type == ColumnType.SYMBOL;
            if (type == ColumnType.DECIMAL128 && decimal128 == null) {
                decimal128 = new Decimal128();
            }
            if (type == ColumnType.DECIMAL256 && decimal256 == null) {
                decimal256 = new Decimal256();
            }
            offset += size;
        }
        this.hasSymbolPayload = hasSymbolPayload;
        rowSize = (int) alignRow(offset);
    }

    /**
     * Bytes of one row with these payload types, laid out as the constructor lays them out, so
     * that a planner can bound a build's heap by its row count before choosing to build.
     */
    static long getRowSize(ColumnTypes payloadTypes) {
        long offset = Long.BYTES;
        for (int i = 0, n = payloadTypes.getColumnCount(); i < n; i++) {
            final int size = payloadSize(ColumnType.tagOf(payloadTypes.getColumnType(i)));
            offset = align(offset, size) + size;
        }
        return alignRow(offset);
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
                case ColumnType.IPv4 -> Unsafe.putInt(dest, record.getIPv4(column));
                case ColumnType.GEOBYTE -> Unsafe.putByte(dest, record.getGeoByte(column));
                case ColumnType.GEOSHORT -> Unsafe.putShort(dest, record.getGeoShort(column));
                case ColumnType.GEOINT -> Unsafe.putInt(dest, record.getGeoInt(column));
                case ColumnType.GEOLONG -> Unsafe.putLong(dest, record.getGeoLong(column));
                // Lo first, then hi: the layout every fixed-size record reads a LONG128 from.
                case ColumnType.UUID -> {
                    Unsafe.putLong(dest, record.getLong128Lo(column));
                    Unsafe.putLong(dest + Long.BYTES, record.getLong128Hi(column));
                }
                case ColumnType.LONG256 -> {
                    final Long256 value = record.getLong256A(column);
                    Unsafe.putLong(dest, value.getLong0());
                    Unsafe.putLong(dest + Long.BYTES, value.getLong1());
                    Unsafe.putLong(dest + Long.BYTES * 2, value.getLong2());
                    Unsafe.putLong(dest + Long.BYTES * 3, value.getLong3());
                }
                case ColumnType.DECIMAL8 -> Unsafe.putByte(dest, record.getDecimal8(column));
                case ColumnType.DECIMAL16 -> Unsafe.putShort(dest, record.getDecimal16(column));
                case ColumnType.DECIMAL32 -> Unsafe.putInt(dest, record.getDecimal32(column));
                case ColumnType.DECIMAL64 -> Unsafe.putLong(dest, record.getDecimal64(column));
                // The scale rides in the payload column's type, so the heap stores raw words only.
                case ColumnType.DECIMAL128 -> {
                    record.getDecimal128(column, decimal128);
                    Unsafe.putLong(dest, decimal128.getHigh());
                    Unsafe.putLong(dest + Long.BYTES, decimal128.getLow());
                }
                case ColumnType.DECIMAL256 -> {
                    record.getDecimal256(column, decimal256);
                    Decimal256.put(decimal256, dest);
                }
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

    // A wider payload is a run of longs, so eight bytes is its natural alignment; aligning it to its
    // own size would pad the row without making any read cheaper.
    private static long align(long offset, int size) {
        final int align = Math.min(size, Long.BYTES);
        return (offset + align - 1) & -align;
    }

    private static long alignRow(long offset) {
        return (offset + 7) & -8L;
    }

    private static int payloadSize(int type) {
        return switch (type) {
            case ColumnType.BOOLEAN, ColumnType.BYTE, ColumnType.GEOBYTE, ColumnType.DECIMAL8 -> 1;
            case ColumnType.SHORT, ColumnType.CHAR, ColumnType.GEOSHORT, ColumnType.DECIMAL16 -> 2;
            case ColumnType.INT, ColumnType.FLOAT, ColumnType.SYMBOL, ColumnType.IPv4,
                 ColumnType.GEOINT, ColumnType.DECIMAL32 -> 4;
            case ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP, ColumnType.DOUBLE,
                 ColumnType.GEOLONG, ColumnType.DECIMAL64 -> 8;
            case ColumnType.UUID, ColumnType.DECIMAL128 -> 16;
            case ColumnType.LONG256, ColumnType.DECIMAL256 -> 32;
            default -> throw new IllegalArgumentException("unsupported hash join payload type: " + ColumnType.nameOf(type));
        };
    }

    /**
     * Payload columns of one row, read through the copied heap. The record is slot-local: it
     * carries its own address and its own symbol table views, which only the build source can
     * hand out per slot.
     */
    final class PayloadRecord implements Record {
        // One flyweight per LONG256 column, and a second set for getLong256B(), whose contract is
        // that it survives a getLong256A() on the same column. Entries of other columns stay null,
        // as the symbol table views of a column that is not a SYMBOL do.
        private final Long256Impl[] long256A = new Long256Impl[types.length];
        private final Long256Impl[] long256B = new Long256Impl[types.length];
        private final SymbolTable[] symbolTables = new SymbolTable[types.length];
        // Written by the probe on every advance; zero until the first match is positioned.
        long address;
        private long probeGeneration;

        private PayloadRecord() {
            for (int i = 0; i < types.length; i++) {
                if (types[i] == ColumnType.LONG256) {
                    long256A[i] = new Long256Impl();
                    long256B[i] = new Long256Impl();
                }
            }
        }

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
        public void getDecimal128(int col, Decimal128 sink) {
            final long address = at(col);
            sink.ofRaw(Unsafe.getLong(address), Unsafe.getLong(address + Long.BYTES));
        }

        @Override
        public short getDecimal16(int col) {
            return getShort(col);
        }

        @Override
        public void getDecimal256(int col, Decimal256 sink) {
            sink.ofRawAddress(at(col));
        }

        @Override
        public int getDecimal32(int col) {
            return getInt(col);
        }

        @Override
        public long getDecimal64(int col) {
            return getLong(col);
        }

        @Override
        public byte getDecimal8(int col) {
            return getByte(col);
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
        public byte getGeoByte(int col) {
            return getByte(col);
        }

        @Override
        public int getGeoInt(int col) {
            return getInt(col);
        }

        @Override
        public long getGeoLong(int col) {
            return getLong(col);
        }

        @Override
        public short getGeoShort(int col) {
            return getShort(col);
        }

        @Override
        public int getIPv4(int col) {
            return getInt(col);
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
        public long getLong128Hi(int col) {
            return Unsafe.getLong(at(col) + Long.BYTES);
        }

        @Override
        public long getLong128Lo(int col) {
            return Unsafe.getLong(at(col));
        }

        @Override
        public void getLong256(int col, CharSink<?> sink) {
            Numbers.appendLong256FromUnsafe(at(col), sink);
        }

        @Override
        public Long256 getLong256A(int col) {
            final Long256Impl value = long256A[col];
            value.fromAddress(at(col));
            return value;
        }

        @Override
        public Long256 getLong256B(int col) {
            final Long256Impl value = long256B[col];
            value.fromAddress(at(col));
            return value;
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
