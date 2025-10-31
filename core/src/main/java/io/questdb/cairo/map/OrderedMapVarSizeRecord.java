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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.BorrowedArray;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.DirectBinarySequence;
import io.questdb.std.Hash;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.Numbers;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Provides Record access interface for FastMap key-value pairs with var-size keys.
 * <p>
 * Uses an offsets array to speed up value column look-ups.
 * Key column offsets are calculated dynamically since keys are var-size.
 * The last accessed key column offset is cached to speed up sequential access.
 */
final class OrderedMapVarSizeRecord implements OrderedMapRecord {
    private final BorrowedArray[] arrays;
    private final DirectBinarySequence[] bs;
    private final DirectString[] csA;
    private final DirectString[] csB;
    private final Interval[] intervals;
    private final Long256Impl[] keyLong256A;
    private final Long256Impl[] keyLong256B;
    private final ColumnTypes keyTypes;
    private final int splitIndex;
    private final DirectUtf8String[] usA;
    private final DirectUtf8String[] usB;
    private final OrderedMapValue value;
    private final long[] valueOffsets;
    private final long valueSize;
    private long keyAddress;
    private int keySize = -1;
    private int lastKeyIndex = -1;
    private int lastKeyOffset = -1;
    private long limit;
    private long startAddress; // key-value pair start address
    private IntList symbolTableIndex;
    private RecordCursor symbolTableResolver;
    private long valueAddress;

    OrderedMapVarSizeRecord(
            long valueSize,
            long[] valueOffsets,
            OrderedMapValue value,
            @NotNull @Transient ColumnTypes keyTypes,
            @Nullable @Transient ColumnTypes valueTypes
    ) {
        this.valueSize = valueSize;
        this.valueOffsets = valueOffsets;
        this.value = value;
        this.value.linkRecord(this); // provides feature to position this record at location of map value
        this.splitIndex = valueOffsets != null ? valueOffsets.length : 0;

        int nColumns;
        int keyIndexOffset;
        if (valueTypes != null) {
            keyIndexOffset = valueTypes.getColumnCount();
            nColumns = keyTypes.getColumnCount() + valueTypes.getColumnCount();
        } else {
            keyIndexOffset = 0;
            nColumns = keyTypes.getColumnCount();
        }

        DirectString[] csA = null;
        DirectString[] csB = null;
        DirectUtf8String[] usA = null;
        DirectUtf8String[] usB = null;
        DirectBinarySequence[] bs = null;
        Long256Impl[] long256A = null;
        Long256Impl[] long256B = null;
        BorrowedArray[] arrays = null;
        Interval[] intervals = null;

        final ArrayColumnTypes keyTypesCopy = new ArrayColumnTypes();
        for (int i = 0, n = keyTypes.getColumnCount(); i < n; i++) {
            final int columnType = keyTypes.getColumnType(i);
            keyTypesCopy.add(columnType);
            switch (ColumnType.tagOf(columnType)) {
                case ColumnType.STRING:
                    if (csA == null) {
                        csA = new DirectString[nColumns];
                        csB = new DirectString[nColumns];
                    }
                    csA[i + keyIndexOffset] = new DirectString();
                    csB[i + keyIndexOffset] = new DirectString();
                    break;
                case ColumnType.VARCHAR:
                    if (usA == null) {
                        usA = new DirectUtf8String[nColumns];
                        usB = new DirectUtf8String[nColumns];
                    }
                    usA[i + keyIndexOffset] = new DirectUtf8String();
                    usB[i + keyIndexOffset] = new DirectUtf8String();
                    break;
                case ColumnType.BINARY:
                    if (bs == null) {
                        bs = new DirectBinarySequence[nColumns];
                    }
                    bs[i + keyIndexOffset] = new DirectBinarySequence();
                    break;
                case ColumnType.LONG256:
                    if (long256A == null) {
                        long256A = new Long256Impl[nColumns];
                        long256B = new Long256Impl[nColumns];
                    }
                    long256A[i + keyIndexOffset] = new Long256Impl();
                    long256B[i + keyIndexOffset] = new Long256Impl();
                    break;
                case ColumnType.INTERVAL:
                    if (intervals == null) {
                        intervals = new Interval[nColumns];
                    }
                    intervals[i + keyIndexOffset] = new Interval();
                    break;
                case ColumnType.ARRAY:
                    if (arrays == null) {
                        arrays = new BorrowedArray[nColumns];
                    }
                    arrays[i + keyIndexOffset] = new BorrowedArray();
                    break;
                default:
                    break;
            }
        }
        this.keyTypes = keyTypesCopy;

        if (valueTypes != null) {
            for (int i = 0, n = valueTypes.getColumnCount(); i < n; i++) {
                if (ColumnType.tagOf(valueTypes.getColumnType(i)) == ColumnType.LONG256) {
                    if (long256A == null) {
                        long256A = new Long256Impl[nColumns];
                        long256B = new Long256Impl[nColumns];
                    }
                    long256A[i] = new Long256Impl();
                    long256B[i] = new Long256Impl();
                }
            }
        }

        this.csA = csA;
        this.csB = csB;
        this.usA = usA;
        this.usB = usB;
        this.bs = bs;
        this.keyLong256A = long256A;
        this.keyLong256B = long256B;
        this.arrays = arrays;
        this.intervals = intervals;
    }

    private OrderedMapVarSizeRecord(
            long valueSize,
            long[] valueOffsets,
            ColumnTypes keyTypes,
            int splitIndex,
            DirectString[] csA,
            DirectString[] csB,
            DirectUtf8String[] usA,
            DirectUtf8String[] usB,
            DirectBinarySequence[] bs,
            Long256Impl[] keyLong256A,
            Long256Impl[] keyLong256B,
            BorrowedArray[] arrays,
            Interval[] intervals
    ) {
        this.valueSize = valueSize;
        this.valueOffsets = valueOffsets;
        this.keyTypes = keyTypes;
        this.splitIndex = splitIndex;
        this.value = new OrderedMapValue(valueSize, valueOffsets);
        this.csA = csA;
        this.csB = csB;
        this.usA = usA;
        this.usB = usB;
        this.bs = bs;
        this.keyLong256A = keyLong256A;
        this.keyLong256B = keyLong256B;
        this.arrays = arrays;
        this.intervals = intervals;
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @Override
    public OrderedMapVarSizeRecord clone() {
        final DirectString[] csA;
        final DirectString[] csB;
        final DirectUtf8String[] usA;
        final DirectUtf8String[] usB;
        final DirectBinarySequence[] bs;
        final Long256Impl[] long256A;
        final Long256Impl[] long256B;
        final BorrowedArray[] arrays;
        final Interval[] intervals;

        // csA and csB are pegged, checking one for null should be enough
        if (this.csA != null) {
            int n = this.csA.length;
            csA = new DirectString[n];
            csB = new DirectString[n];

            for (int i = 0; i < n; i++) {
                if (this.csA[i] != null) {
                    csA[i] = new DirectString();
                    csB[i] = new DirectString();
                }
            }
        } else {
            csA = null;
            csB = null;
        }

        if (this.usA != null) {
            int n = this.usA.length;
            usA = new DirectUtf8String[n];
            usB = new DirectUtf8String[n];

            for (int i = 0; i < n; i++) {
                if (this.usA[i] != null) {
                    usA[i] = new DirectUtf8String();
                    usB[i] = new DirectUtf8String();
                }
            }
        } else {
            usA = null;
            usB = null;
        }

        if (this.bs != null) {
            int n = this.bs.length;
            bs = new DirectBinarySequence[n];
            for (int i = 0; i < n; i++) {
                if (this.bs[i] != null) {
                    bs[i] = new DirectBinarySequence();
                }
            }
        } else {
            bs = null;
        }

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

        if (this.intervals != null) {
            int n = this.intervals.length;
            intervals = new Interval[n];
            for (int i = 0; i < n; i++) {
                if (this.intervals[i] != null) {
                    intervals[i] = new Interval();
                }
            }
        } else {
            intervals = null;
        }

        if (this.arrays != null) {
            int n = this.arrays.length;
            arrays = new BorrowedArray[n];
            for (int i = 0; i < n; i++) {
                if (this.arrays[i] != null) {
                    arrays[i] = new BorrowedArray();
                }
            }
        } else {
            arrays = null;
        }

        return new OrderedMapVarSizeRecord(valueSize, valueOffsets, keyTypes, splitIndex, csA, csB, usA, usB, bs, long256A, long256B, arrays, intervals);
    }

    @Override
    public void copyToKey(MapKey destKey) {
        OrderedMap.VarSizeKey destFastKey = (OrderedMap.VarSizeKey) destKey;
        int keySize = Unsafe.getUnsafe().getInt(startAddress);
        destFastKey.copyFromRawKey(keyAddress, keySize);
    }

    @Override
    public void copyValue(MapValue destValue) {
        OrderedMapValue destFastValue = (OrderedMapValue) destValue;
        destFastValue.copyRawValue(valueAddress);
    }

    @Override
    public ArrayView getArray(int index, int columnType) {
        long address = addressOfColumn(index);
        BorrowedArray ba = arrays[index];
        return ArrayTypeDriver.getPlainValue(address, ba);
    }

    @Override
    public BinarySequence getBin(int columnIndex) {
        long address = addressOfColumn(columnIndex);
        int len = Unsafe.getUnsafe().getInt(address);
        if (len == TableUtils.NULL_LEN) {
            return null;
        }
        DirectBinarySequence bs = this.bs[columnIndex];
        bs.of(address + Integer.BYTES, len);
        return bs;
    }

    @Override
    public long getBinLen(int columnIndex) {
        return Unsafe.getUnsafe().getInt(addressOfColumn(columnIndex));
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
    public short getDecimal16(int columnIndex) {
        return Unsafe.getUnsafe().getShort(addressOfColumn(columnIndex));
    }

    @Override
    public void getDecimal256(int col, Decimal256 sink) {
        sink.ofRawAddress(addressOfColumn(col));
    }

    @Override
    public int getDecimal32(int columnIndex) {
        return Unsafe.getUnsafe().getInt(addressOfColumn(columnIndex));
    }

    @Override
    public long getDecimal64(int columnIndex) {
        return Unsafe.getUnsafe().getLong(addressOfColumn(columnIndex));
    }

    @Override
    public byte getDecimal8(int columnIndex) {
        return Unsafe.getUnsafe().getByte(addressOfColumn(columnIndex));
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
    public Interval getInterval(int columnIndex) {
        long address = addressOfColumn(columnIndex);
        long lo = Unsafe.getUnsafe().getLong(address);
        long hi = Unsafe.getUnsafe().getLong(address + Long.BYTES);
        Interval interval = this.intervals[columnIndex];
        return interval.of(lo, hi);
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
        // Important invariant: we assume that the FastMap doesn't grow after the first getRowId() call.
        // Otherwise, row ids returned by this method may no longer point at a valid memory address.
        return startAddress;
    }

    @Override
    public short getShort(int columnIndex) {
        return Unsafe.getUnsafe().getShort(addressOfColumn(columnIndex));
    }

    @Override
    public CharSequence getStrA(int columnIndex) {
        return getStr0(columnIndex, csA[columnIndex]);
    }

    @Override
    public CharSequence getStrB(int columnIndex) {
        return getStr0(columnIndex, csB[columnIndex]);
    }

    @Override
    public int getStrLen(int columnIndex) {
        return Unsafe.getUnsafe().getInt(addressOfColumn(columnIndex));
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
        return value.of(startAddress, valueAddress, limit, false);
    }

    @Override
    public Utf8Sequence getVarcharA(int columnIndex) {
        return getVarchar0(columnIndex, usA[columnIndex]);
    }

    @Override
    public Utf8Sequence getVarcharB(int columnIndex) {
        return getVarchar0(columnIndex, usB[columnIndex]);
    }

    @Override
    public int getVarcharSize(int columnIndex) {
        long address = addressOfColumn(columnIndex);
        return VarcharTypeDriver.getPlainValueSize(address);
    }

    @Override
    public long keyHashCode() {
        int keySize = Unsafe.getUnsafe().getInt(startAddress);
        return Hash.hashMem64(startAddress + Integer.BYTES, keySize);
    }

    public int keySize() {
        return keySize;
    }

    @Override
    public void of(long address) {
        this.startAddress = address;
        this.keyAddress = address + Integer.BYTES;
        this.keySize = Unsafe.getUnsafe().getInt(address);
        this.valueAddress = address + Integer.BYTES + keySize;
        this.lastKeyIndex = -1;
        this.lastKeyOffset = -1;
    }

    @Override
    public void setLimit(long limit) {
        this.limit = limit;
    }

    @Override
    public void setSymbolTableResolver(RecordCursor resolver, IntList symbolTableIndex) {
        this.symbolTableResolver = resolver;
        this.symbolTableIndex = symbolTableIndex;
    }

    private long addressOfColumn(int index) {
        // Column indexes start with value fields followed by key fields.
        // The key-value pair layout is [key len, key data, value data].
        if (index < splitIndex) {
            return valueAddress + valueOffsets[index];
        }
        if (index == splitIndex) {
            return keyAddress;
        }
        return addressOfKeyColumn(index - splitIndex);
    }

    private long addressOfKeyColumn(int index) {
        long addr = keyAddress;
        int i = 0;
        if (lastKeyIndex > -1 && index >= lastKeyIndex) {
            addr += lastKeyOffset;
            i = lastKeyIndex;
        }
        while (i < index) {
            final int columnType = keyTypes.getColumnType(i);
            final int size = ColumnType.sizeOf(columnType);
            if (size > 0) {
                // Fixed-size type.
                addr += size;
            } else if (ColumnType.isArray(columnType)) {
                addr += ArrayTypeDriver.getPlainValueSize(addr);
            } else {
                // var-size type: string or varchar, binary
                assert columnType == ColumnType.STRING || columnType == ColumnType.VARCHAR || columnType == ColumnType.BINARY;
                final int len = Unsafe.getUnsafe().getInt(addr);
                addr += Integer.BYTES;
                if (len != TableUtils.NULL_LEN) {
                    if (ColumnType.isString(columnType)) {
                        addr += (long) len << 1;
                    } else {
                        // This is varchar or binary.
                        // Varchar's ASCII flag is signaled with the highest bit,
                        // so make sure to remove it.
                        addr += len & Integer.MAX_VALUE;
                    }
                }
            }
            i++;
        }
        lastKeyOffset = (int) (addr - keyAddress);
        lastKeyIndex = i;
        return addr;
    }

    @NotNull
    private Long256 getLong256Generic(Long256Impl[] keyLong256, int columnIndex) {
        Long256Impl long256 = keyLong256[columnIndex];
        long256.fromAddress(addressOfColumn(columnIndex));
        return long256;
    }

    private CharSequence getStr0(int index, DirectString cs) {
        long address = addressOfColumn(index);
        int len = Unsafe.getUnsafe().getInt(address);
        return len == TableUtils.NULL_LEN ? null : cs.of(address + Integer.BYTES, address + Integer.BYTES + len * 2L);
    }

    private Utf8Sequence getVarchar0(int index, DirectUtf8String us) {
        long address = addressOfColumn(index);
        return VarcharTypeDriver.getPlainValue(address, us);
    }
}
