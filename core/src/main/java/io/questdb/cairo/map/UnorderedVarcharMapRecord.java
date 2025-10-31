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
import io.questdb.std.Hash;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.Numbers;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Provides Record access interface for UnorderedVarcharMap key-value pairs.
 * <p>
 * Uses an offsets array to speed up key and value column look-ups.
 */
final class UnorderedVarcharMapRecord implements MapRecord {
    private final long[] columnOffsets;
    private final Long256Impl[] keyLong256A;
    private final Long256Impl[] keyLong256B;
    private final DirectUtf8String usA;
    private final DirectUtf8String usB;
    private final UnorderedVarcharMapValue value;
    private final long[] valueOffsets;
    private final long valueSize;
    private long limit;
    private long startAddress;
    private IntList symbolTableIndex;
    private RecordCursor symbolTableResolver;

    UnorderedVarcharMapRecord(
            long valueSize,
            long[] valueOffsets,
            UnorderedVarcharMapValue value,
            @Nullable @Transient ColumnTypes valueTypes
    ) {
        this.valueSize = valueSize;
        this.valueOffsets = valueOffsets;
        this.value = value;
        this.value.linkRecord(this); // provides feature to position this record at location of map value

        int nColumns;
        if (valueTypes != null) {
            nColumns = valueTypes.getColumnCount() + 1;
        } else {
            nColumns = 1;
        }

        columnOffsets = new long[nColumns];

        Long256Impl[] long256A = null;
        Long256Impl[] long256B = null;
        long offset = UnorderedVarcharMap.KEY_SIZE;
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
        this.usA = new DirectUtf8String();
        this.usB = new DirectUtf8String();
    }

    private UnorderedVarcharMapRecord(
            long valueSize,
            long[] valueOffsets,
            long[] columnOffsets,
            Long256Impl[] keyLong256A,
            Long256Impl[] keyLong256B
    ) {
        this.valueSize = valueSize;
        this.valueOffsets = valueOffsets;
        this.columnOffsets = columnOffsets;
        this.value = new UnorderedVarcharMapValue(valueSize, valueOffsets);
        this.keyLong256A = keyLong256A;
        this.keyLong256B = keyLong256B;
        this.usA = new DirectUtf8String();
        this.usB = new DirectUtf8String();
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @Override
    public UnorderedVarcharMapRecord clone() {
        final Long256Impl[] long256A;
        final Long256Impl[] long256B;

        if (keyLong256A != null) {
            int n = keyLong256A.length;
            long256A = new Long256Impl[n];
            long256B = new Long256Impl[n];

            for (int i = 0; i < n; i++) {
                if (keyLong256A[i] != null) {
                    long256A[i] = new Long256Impl();
                    long256B[i] = new Long256Impl();
                }
            }
        } else {
            long256A = null;
            long256B = null;
        }
        return new UnorderedVarcharMapRecord(valueSize, valueOffsets, columnOffsets, long256A, long256B);
    }

    @Override
    public void copyToKey(MapKey destKey) {
        UnorderedVarcharMap.Key destBaseKey = (UnorderedVarcharMap.Key) destKey;
        destBaseKey.copyFromStartAddress(startAddress);
    }

    @Override
    public void copyValue(MapValue destValue) {
        UnorderedVarcharMapValue destVarcharValue = (UnorderedVarcharMapValue) destValue;
        destVarcharValue.copyRawValue(startAddress + UnorderedVarcharMap.KEY_SIZE);
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
    public @Nullable Utf8Sequence getVarcharA(int col) {
        return getVarchar0(col, usA);
    }

    @Override
    public @Nullable Utf8Sequence getVarcharB(int col) {
        return getVarchar0(col, usB);
    }

    @Override
    public int getVarcharSize(int col) {
        long address = addressOfColumn(col);
        int sizeWithFlags = Unsafe.getUnsafe().getInt(address + 4);
        boolean isNull = UnorderedVarcharMap.isSizeNull(sizeWithFlags);
        if (isNull) {
            return -1;
        }
        return sizeWithFlags & UnorderedVarcharMap.MASK_FLAGS_FROM_SIZE;
    }

    @Override
    public long keyHashCode() {
        int lenAndFlags = Unsafe.getUnsafe().getInt(startAddress + 4);
        long ptr = Unsafe.getUnsafe().getLong(startAddress + 8) & UnorderedVarcharMap.PTR_MASK;

        int size = lenAndFlags & UnorderedVarcharMap.MASK_FLAGS_FROM_SIZE;
        if (size > 0) {
            return Hash.hashMem64(ptr, size);
        }
        // null or empty key -> hash is 0
        return 0;
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
        long address = addressOfColumn(columnIndex);
        Long256Impl long256 = keyLong256[columnIndex];
        long256.fromAddress(address);
        return long256;
    }

    private DirectUtf8String getVarchar0(int col, DirectUtf8String us) {
        long address = addressOfColumn(col);
        long packedHashSizeFlags = Unsafe.getUnsafe().getLong(address);
        byte flags = UnorderedVarcharMap.unpackFlags(packedHashSizeFlags);
        if (UnorderedVarcharMap.isNull(flags)) {
            return null;
        }
        boolean isAscii = UnorderedVarcharMap.isAscii(flags);
        long size = UnorderedVarcharMap.unpackSize(packedHashSizeFlags);
        long ptrWithUnstableFlag = Unsafe.getUnsafe().getLong(address + Long.BYTES);
        long ptr = ptrWithUnstableFlag & UnorderedVarcharMap.PTR_MASK;
        return us.of(ptr, ptr + size, isAscii);
    }
}
