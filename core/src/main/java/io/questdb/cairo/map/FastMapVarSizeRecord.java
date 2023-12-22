/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.std.*;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.CharSinkBase;
import io.questdb.std.str.DirectString;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Provides Record access interface for FastMap key-value pairs with var-size keys.
 * <p>
 * Uses an offsets array to speed up value column look-ups.
 * Key column offsets are calculated dynamically since keys are var-size.
 * The last accessed key column offset is cached to speed up sequential access.
 */
final class FastMapVarSizeRecord implements FastMapRecord {
    private final DirectBinarySequence[] bs;
    private final DirectString[] csA;
    private final DirectString[] csB;
    private final Long256Impl[] keyLong256A;
    private final Long256Impl[] keyLong256B;
    private final ColumnTypes keyTypes;
    private final int splitIndex;
    private final FastMapValue value;
    private final long[] valueOffsets;
    private final int valueSize;
    private long keyAddress;
    private int lastKeyIndex = -1;
    private int lastKeyOffset = -1;
    private long limit;
    private long startAddress; // key-value pair start address
    private IntList symbolTableIndex;
    private RecordCursor symbolTableResolver;
    private long valueAddress;

    FastMapVarSizeRecord(
            int valueSize,
            long[] valueOffsets,
            FastMapValue value,
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
        DirectBinarySequence[] bs = null;
        Long256Impl[] long256A = null;
        Long256Impl[] long256B = null;

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
        this.bs = bs;
        this.keyLong256A = long256A;
        this.keyLong256B = long256B;
    }

    private FastMapVarSizeRecord(
            int valueSize,
            long[] valueOffsets,
            ColumnTypes keyTypes,
            int splitIndex,
            DirectString[] csA,
            DirectString[] csB,
            DirectBinarySequence[] bs,
            Long256Impl[] keyLong256A,
            Long256Impl[] keyLong256B
    ) {
        this.valueSize = valueSize;
        this.valueOffsets = valueOffsets;
        this.keyTypes = keyTypes;
        this.splitIndex = splitIndex;
        this.value = new FastMapValue(valueSize, valueOffsets);
        this.csA = csA;
        this.csB = csB;
        this.bs = bs;
        this.keyLong256A = keyLong256A;
        this.keyLong256B = keyLong256B;
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @Override
    public FastMapRecord clone() {
        final DirectString[] csA;
        final DirectString[] csB;
        final DirectBinarySequence[] bs;
        final Long256Impl[] long256A;
        final Long256Impl[] long256B;

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
        return new FastMapVarSizeRecord(valueSize, valueOffsets, keyTypes, splitIndex, csA, csB, bs, long256A, long256B);
    }

    @Override
    public void copyToKey(MapKey destKey) {
        FastMap.VarSizeKey destFastKey = (FastMap.VarSizeKey) destKey;
        int keySize = Unsafe.getUnsafe().getInt(startAddress);
        destFastKey.copyFromRawKey(keyAddress, keySize);
    }

    @Override
    public void copyValue(MapValue destValue) {
        FastMapValue destFastValue = (FastMapValue) destValue;
        destFastValue.copyRawValue(valueAddress);
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
    public void getLong256(int columnIndex, CharSinkBase<?> sink) {
        long address = addressOfColumn(columnIndex);
        final long a = Unsafe.getUnsafe().getLong(address);
        final long b = Unsafe.getUnsafe().getLong(address + Long.BYTES);
        final long c = Unsafe.getUnsafe().getLong(address + Long.BYTES * 2);
        final long d = Unsafe.getUnsafe().getLong(address + Long.BYTES * 3);
        Numbers.appendLong256(a, b, c, d, sink);
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
    public CharSequence getStr(int columnIndex) {
        return getStr0(columnIndex, csA[columnIndex]);
    }

    @Override
    public void getStr(int columnIndex, CharSink sink) {
        long address = addressOfColumn(columnIndex);
        int len = Unsafe.getUnsafe().getInt(address);
        address += Integer.BYTES;
        for (int i = 0; i < len; i++) {
            sink.put(Unsafe.getUnsafe().getChar(address));
            address += Character.BYTES;
        }
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
    public CharSequence getSym(int columnIndex) {
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
    public int keyHashCode() {
        int keySize = Unsafe.getUnsafe().getInt(startAddress);
        return Hash.hashMem32(startAddress + Integer.BYTES, keySize);
    }

    @Override
    public void of(long address) {
        this.startAddress = address;
        this.keyAddress = address + Integer.BYTES;
        int keySize = Unsafe.getUnsafe().getInt(address);
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
            } else {
                // Var-size type: string or binary.
                final int len = Unsafe.getUnsafe().getInt(addr);
                addr += Integer.BYTES;
                if (len != TableUtils.NULL_LEN) {
                    if (ColumnType.isString(columnType)) {
                        addr += (long) len << 1;
                    } else {
                        addr += len;
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
        long address = addressOfColumn(columnIndex);
        Long256Impl long256 = keyLong256[columnIndex];
        long256.setAll(
                Unsafe.getUnsafe().getLong(address),
                Unsafe.getUnsafe().getLong(address + Long.BYTES),
                Unsafe.getUnsafe().getLong(address + Long.BYTES * 2),
                Unsafe.getUnsafe().getLong(address + Long.BYTES * 3)
        );
        return long256;
    }

    private CharSequence getStr0(int index, DirectString cs) {
        long address = addressOfColumn(index);
        int len = Unsafe.getUnsafe().getInt(address);
        return len == TableUtils.NULL_LEN ? null : cs.of(address + Integer.BYTES, address + Integer.BYTES + len * 2L);
    }
}
