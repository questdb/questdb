/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cairo.map;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.Numbers;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Provides Record access interface for FastMap key-value pairs with fixed-size keys.
 * <p>
 * Uses an offsets array to speed up key and value column look-ups.
 */
final class Unordered2MapRecord implements MapRecord {
    private final long[] columnOffsets;
    private final Long256Impl[] keyLong256A;
    private final Long256Impl[] keyLong256B;
    private final Unordered2MapValue value;
    private final long[] valueOffsets;
    private final long valueSize;
    private long limit;
    private long startAddress;
    private IntList symbolTableIndex;
    private RecordCursor symbolTableResolver;

    Unordered2MapRecord(
            long valueSize,
            long[] valueOffsets,
            Unordered2MapValue value,
            @NotNull @Transient ColumnTypes keyTypes,
            @Nullable @Transient ColumnTypes valueTypes
    ) {
        this.valueSize = valueSize;
        this.valueOffsets = valueOffsets;
        this.value = value;
        this.value.linkRecord(this); // provides feature to position this record at location of map value

        int nColumns;
        int keyIndexOffset;
        if (valueTypes != null) {
            keyIndexOffset = valueTypes.getColumnCount();
            nColumns = keyTypes.getColumnCount() + valueTypes.getColumnCount();
        } else {
            keyIndexOffset = 0;
            nColumns = keyTypes.getColumnCount();
        }

        columnOffsets = new long[nColumns];

        Long256Impl[] long256A = null;
        Long256Impl[] long256B = null;
        long offset = 0;
        for (int i = 0, n = keyTypes.getColumnCount(); i < n; i++) {
            final int columnType = keyTypes.getColumnType(i);
            if (ColumnType.tagOf(columnType) == ColumnType.LONG256) {
                if (long256A == null) {
                    long256A = new Long256Impl[nColumns];
                    long256B = new Long256Impl[nColumns];
                }
                long256A[i + keyIndexOffset] = new Long256Impl();
                long256B[i + keyIndexOffset] = new Long256Impl();
            }
            final int size = ColumnType.sizeOf(columnType);
            if (size <= 0) {
                throw CairoException.nonCritical().put("key type is not supported: ").put(ColumnType.nameOf(columnType));
            }
            columnOffsets[i + keyIndexOffset] = offset;
            offset += size;
        }

        assert offset <= Unordered2Map.KEY_SIZE;
        offset = Unordered2Map.KEY_SIZE;
        if (valueTypes != null) {
            for (int i = 0, n = valueTypes.getColumnCount(); i < n; i++) {
                int columnType = valueTypes.getColumnType(i);
                if (ColumnType.tagOf(columnType) == ColumnType.LONG256) {
                    if (long256A == null) {
                        long256A = new Long256Impl[nColumns];
                        long256B = new Long256Impl[nColumns];
                    }
                    long256A[i] = new Long256Impl();
                    long256B[i] = new Long256Impl();
                }
                final int size = ColumnType.sizeOf(columnType);
                if (size <= 0) {
                    throw CairoException.nonCritical().put("value type is not supported: ").put(ColumnType.nameOf(columnType));
                }
                columnOffsets[i] = offset;
                offset += size;
            }
        }

        this.keyLong256A = long256A;
        this.keyLong256B = long256B;
    }

    private Unordered2MapRecord(
            long valueSize,
            long[] valueOffsets,
            long[] columnOffsets,
            Long256Impl[] keyLong256A,
            Long256Impl[] keyLong256B
    ) {
        this.valueSize = valueSize;
        this.valueOffsets = valueOffsets;
        this.columnOffsets = columnOffsets;
        this.value = new Unordered2MapValue(valueSize, valueOffsets);
        this.keyLong256A = keyLong256A;
        this.keyLong256B = keyLong256B;
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @Override
    public Unordered2MapRecord clone() {
        final Long256Impl[] long256A;
        final Long256Impl[] long256B;

        if (this.keyLong256A != null) {
            int n = this.keyLong256A.length;
            long256A = new Long256Impl[n];
            long256B = new Long256Impl[n];

            for (int i = 0; i < n; i++) {
                if (this.keyLong256A[i] != null) {
                    long256A[i] = new Long256Impl();
                    long256B[i] = new Long256Impl();
                }
            }
        } else {
            long256A = null;
            long256B = null;
        }
        return new Unordered2MapRecord(valueSize, valueOffsets, columnOffsets, long256A, long256B);
    }

    @Override
    public void copyToKey(MapKey destKey) {
        Unordered2Map.Key destBaseKey = (Unordered2Map.Key) destKey;
        destBaseKey.copyFromRawKey(startAddress);
    }

    @Override
    public void copyValue(MapValue destValue) {
        Unordered2MapValue destFastValue = (Unordered2MapValue) destValue;
        destFastValue.copyRawValue(startAddress + Unordered2Map.KEY_SIZE);
    }

    @Override
    public boolean getBool(int columnIndex) {
        return Unsafe.getBool(addressOfColumn(columnIndex));
    }

    @Override
    public byte getByte(int columnIndex) {
        return Unsafe.getUnsafe().getByte(addressOfColumn(columnIndex));
    }

    @Override
    public char getChar(int columnIndex) {
        return Unsafe.getUnsafe().getChar(addressOfColumn(columnIndex));
    }

    @Override
    public void getDecimal128(int col, Decimal128 sink) {
        final long addr = addressOfColumn(col);
        sink.ofRaw(
                Unsafe.getUnsafe().getLong(addr),
                Unsafe.getUnsafe().getLong(addr + 8L)
        );
    }

    @Override
    public short getDecimal16(int col) {
        return Unsafe.getUnsafe().getShort(addressOfColumn(col));
    }

    @Override
    public void getDecimal256(int col, Decimal256 sink) {
        sink.ofRawAddress(addressOfColumn(col));
    }

    @Override
    public int getDecimal32(int col) {
        return Unsafe.getUnsafe().getInt(addressOfColumn(col));
    }

    @Override
    public long getDecimal64(int col) {
        return Unsafe.getUnsafe().getLong(addressOfColumn(col));
    }

    @Override
    public byte getDecimal8(int col) {
        return Unsafe.getUnsafe().getByte(addressOfColumn(col));
    }

    @Override
    public double getDouble(int columnIndex) {
        return Unsafe.getUnsafe().getDouble(addressOfColumn(columnIndex));
    }

    @Override
    public float getFloat(int columnIndex) {
        return Unsafe.getUnsafe().getFloat(addressOfColumn(columnIndex));
    }

    @Override
    public byte getGeoByte(int columnIndex) {
        return getByte(columnIndex);
    }

    @Override
    public int getGeoInt(int columnIndex) {
        return getInt(columnIndex);
    }

    @Override
    public long getGeoLong(int columnIndex) {
        return getLong(columnIndex);
    }

    @Override
    public short getGeoShort(int columnIndex) {
        return getShort(columnIndex);
    }

    @Override
    public int getIPv4(int columnIndex) {
        return Unsafe.getUnsafe().getInt(addressOfColumn(columnIndex));
    }

    @Override
    public int getInt(int columnIndex) {
        return Unsafe.getUnsafe().getInt(addressOfColumn(columnIndex));
    }

    @Override
    public long getLong(int columnIndex) {
        return Unsafe.getUnsafe().getLong(addressOfColumn(columnIndex));
    }

    @Override
    public long getLong128Hi(int columnIndex) {
        return Unsafe.getUnsafe().getLong(addressOfColumn(columnIndex) + Long.BYTES);
    }

    @Override
    public long getLong128Lo(int columnIndex) {
        return Unsafe.getUnsafe().getLong(addressOfColumn(columnIndex));
    }

    @Override
    public void getLong256(int columnIndex, CharSink<?> sink) {
        Numbers.appendLong256FromUnsafe(addressOfColumn(columnIndex), sink);
    }

    @Override
    public Long256 getLong256A(int columnIndex) {
        return getLong256Generic(keyLong256A, columnIndex);
    }

    @Override
    public Long256 getLong256B(int columnIndex) {
        return getLong256Generic(keyLong256B, columnIndex);
    }

    @Override
    public long getRowId() {
        // Important invariant: we assume that the map doesn't grow after the first getRowId() call.
        // Otherwise, row ids returned by this method may no longer point at a valid memory address.
        return startAddress;
    }

    @Override
    public short getShort(int columnIndex) {
        return Unsafe.getUnsafe().getShort(addressOfColumn(columnIndex));
    }

    @Override
    public CharSequence getSymA(int columnIndex) {
        return symbolTableResolver.getSymbolTable(symbolTableIndex.getQuick(columnIndex)).valueOf(getInt(columnIndex));
    }

    @Override
    public CharSequence getSymB(int columnIndex) {
        return symbolTableResolver.getSymbolTable(symbolTableIndex.getQuick(columnIndex)).valueBOf(getInt(columnIndex));
    }

    @Override
    public MapValue getValue() {
        return value.of(startAddress, limit, false);
    }

    @Override
    public long keyHashCode() {
        return 0; // no-op
    }

    public void of(long address) {
        this.startAddress = address;
    }

    public void setLimit(long limit) {
        this.limit = limit;
    }

    @Override
    public void setSymbolTableResolver(RecordCursor resolver, IntList symbolTableIndex) {
        this.symbolTableResolver = resolver;
        this.symbolTableIndex = symbolTableIndex;
    }

    private long addressOfColumn(int index) {
        return startAddress + columnOffsets[index];
    }

    @NotNull
    private Long256 getLong256Generic(Long256Impl[] keyLong256, int columnIndex) {
        Long256Impl long256 = keyLong256[columnIndex];
        long256.fromAddress(addressOfColumn(columnIndex));
        return long256;
    }
}
