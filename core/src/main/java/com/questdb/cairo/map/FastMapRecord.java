/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cairo.map;

import com.questdb.cairo.ColumnType;
import com.questdb.cairo.ColumnTypes;
import com.questdb.cairo.TableUtils;
import com.questdb.cairo.sql.RecordCursor;
import com.questdb.std.*;
import com.questdb.std.str.CharSink;
import com.questdb.std.str.DirectCharSequence;
import org.jetbrains.annotations.NotNull;

final class FastMapRecord implements MapRecord {
    private final int split;
    private final int keyDataOffset;
    private final int keyBlockOffset;
    private final int[] valueOffsets;
    private final DirectCharSequence[] csA;
    private final DirectCharSequence[] csB;
    private final DirectBinarySequence[] bs;
    private final Long256Impl[] long256A;
    private final Long256Impl[] long256B;
    private final FastMapValue value;
    private long address0;
    private long address1;
    private long address2;
    private RecordCursor symbolTableResolver;
    private IntList symbolTableIndex;

    FastMapRecord(
            int[] valueOffsets,
            int split,
            int keyDataOffset,
            int keyBlockOffset,
            FastMapValue value,
            @Transient ColumnTypes keyTypes) {
        this.valueOffsets = valueOffsets;
        this.split = split;
        this.keyBlockOffset = keyBlockOffset;
        this.keyDataOffset = keyDataOffset;
        this.value = value;
        this.value.linkRecord(this); // provides feature to position this record at location of map value

        int n = keyTypes.getColumnCount();

        DirectCharSequence[] csA = null;
        DirectCharSequence[] csB = null;
        DirectBinarySequence[] bs = null;
        Long256Impl[] long256A = null;
        Long256Impl[] long256B = null;

        for (int i = 0; i < n; i++) {
            switch (keyTypes.getColumnType(i)) {
                case ColumnType.STRING:
                    if (csA == null) {
                        csA = new DirectCharSequence[n + split];
                        csB = new DirectCharSequence[n + split];
                    }
                    csA[i + split] = new DirectCharSequence();
                    csB[i + split] = new DirectCharSequence();
                    break;
                case ColumnType.BINARY:
                    if (bs == null) {
                        bs = new DirectBinarySequence[n + split];
                    }
                    bs[i + split] = new DirectBinarySequence();
                    break;
                case ColumnType.LONG256:
                    if (long256A == null) {
                        long256A = new Long256Impl[n + split];
                        long256B = new Long256Impl[n + split];
                    }
                    long256A[i + split] = new Long256Impl();
                    long256B[i + split] = new Long256Impl();
                    break;
                default:
                    break;
            }
        }

        this.csA = csA;
        this.csB = csB;
        this.bs = bs;
        this.long256A = long256A;
        this.long256B = long256B;
    }

    private FastMapRecord(
            int[] valueOffsets,
            int split,
            int keyDataOffset,
            int keyBlockOffset,
            DirectCharSequence[] csA,
            DirectCharSequence[] csB,
            DirectBinarySequence[] bs,
            Long256Impl[] long256A,
            Long256Impl[] long256B
    ) {

        this.valueOffsets = valueOffsets;
        this.split = split;
        this.keyBlockOffset = keyBlockOffset;
        this.keyDataOffset = keyDataOffset;
        this.value = new FastMapValue(valueOffsets);
        this.csA = csA;
        this.csB = csB;
        this.bs = bs;
        this.long256A = long256A;
        this.long256B = long256B;
    }

    @Override
    public BinarySequence getBin(int columnIndex) {
        long address = addressOfColumn(columnIndex);
        int len = Unsafe.getUnsafe().getInt(address);
        if (len == TableUtils.NULL_LEN) {
            return null;
        }
        DirectBinarySequence bs = this.bs[columnIndex];
        bs.of(address + 4, len);
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
    public double getDouble(int columnIndex) {
        return Unsafe.getUnsafe().getDouble(addressOfColumn(columnIndex));
    }

    @Override
    public float getFloat(int columnIndex) {
        return Unsafe.getUnsafe().getFloat(addressOfColumn(columnIndex));
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
    public long getRowId() {
        return address0;
    }

    @Override
    public short getShort(int columnIndex) {
        return Unsafe.getUnsafe().getShort(addressOfColumn(columnIndex));
    }

    @Override
    public char getChar(int columnIndex) {
        return Unsafe.getUnsafe().getChar(addressOfColumn(columnIndex));
    }

    @Override
    public void getLong256(int columnIndex, CharSink sink) {
        long address = addressOfColumn(columnIndex);
        final long a = Unsafe.getUnsafe().getLong(address);
        final long b = Unsafe.getUnsafe().getLong(address + Long256.BYTES);
        final long c = Unsafe.getUnsafe().getLong(address + Long256.BYTES * 2);
        final long d = Unsafe.getUnsafe().getLong(address + Long256.BYTES * 3);
        if (a == 0 && b == 0 && c == 0 && d == 0) {
            return;
        }
        sink.put("0x");
        Numbers.putSignificantHexToSink(sink, d);
        Numbers.putSignificantHexToSink(sink, c);
        Numbers.putSignificantHexToSink(sink, b);
        Numbers.putSignificantHexToSink(sink, a);
    }

    @Override
    public Long256 getLong256A(int columnIndex) {
        return getLong256Generic(long256A, columnIndex);
    }

    @NotNull
    private Long256 getLong256Generic(Long256Impl[] array, int columnIndex) {
        long address = addressOfColumn(columnIndex);
        Long256Impl long256 = array[columnIndex];
        long256.setLong0(Unsafe.getUnsafe().getLong(address));
        long256.setLong1(Unsafe.getUnsafe().getLong(address + Long.BYTES));
        long256.setLong2(Unsafe.getUnsafe().getLong(address + Long.BYTES * 2));
        long256.setLong3(Unsafe.getUnsafe().getLong(address + Long.BYTES * 3));
        return long256;
    }

    @Override
    public Long256 getLong256B(int columnIndex) {
        return getLong256Generic(long256B, columnIndex);
    }

    @Override
    public CharSequence getStr(int columnIndex) {
        assert columnIndex < csA.length;
        return getStr0(columnIndex, Unsafe.arrayGet(csA, columnIndex));
    }

    @Override
    public void getStr(int columnIndex, CharSink sink) {
        long address = addressOfColumn(columnIndex);
        int len = Unsafe.getUnsafe().getInt(address);
        address += 4;
        for (int i = 0; i < len; i++) {
            sink.put(Unsafe.getUnsafe().getChar(address));
            address += 2;
        }
    }

    @Override
    public CharSequence getStrB(int columnIndex) {
        return getStr0(columnIndex, Unsafe.arrayGet(csB, columnIndex));
    }

    @Override
    public int getStrLen(int columnIndex) {
        return Unsafe.getUnsafe().getInt(addressOfColumn(columnIndex));
    }

    @Override
    public CharSequence getSym(int col) {
        return symbolTableResolver.getSymbolTable(symbolTableIndex.getQuick(col)).value(getInt(col));
    }

    @Override
    public MapValue getValue() {
        return value.of(address0, false);
    }

    @Override
    public void setSymbolTableResolver(RecordCursor resolver, IntList symbolTableIndex) {
        this.symbolTableResolver = resolver;
        this.symbolTableIndex = symbolTableIndex;
    }

    private long addressOfColumn(int index) {

        if (index < split) {
            return address0 + Unsafe.arrayGet(valueOffsets, index);
        }

        if (index == split) {
            return address1;
        }

        return Unsafe.getUnsafe().getInt(address2 + (index - split - 1) * 4) + address0;
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @Override
    protected MapRecord clone() {
        final DirectCharSequence[] csA;
        final DirectCharSequence[] csB;
        final DirectBinarySequence[] bs;
        final Long256Impl[] long256A;
        final Long256Impl[] long256B;

        // csA and csB are pegged, checking one for null should be enough
        if (this.csA != null) {
            int n = this.csA.length;
            csA = new DirectCharSequence[n];
            csB = new DirectCharSequence[n];

            for (int i = 0; i < n; i++) {
                if (Unsafe.arrayGet(this.csA, i) != null) {
                    Unsafe.arrayPut(csA, i, new DirectCharSequence());
                    Unsafe.arrayPut(csB, i, new DirectCharSequence());
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
                if (Unsafe.arrayGet(this.bs, i) != null) {
                    Unsafe.arrayPut(bs, i, new DirectBinarySequence());
                }
            }
        } else {
            bs = null;
        }

        if (this.long256A != null) {
            int n = this.long256A.length;
            long256A = new Long256Impl[n];
            long256B = new Long256Impl[n];

            for (int i = 0; i < n; i++) {
                if (Unsafe.arrayGet(this.long256A, i) != null) {
                    Unsafe.arrayPut(csA, i, new Long256Impl());
                    Unsafe.arrayPut(csB, i, new Long256Impl());
                }
            }
        } else {
            long256A = null;
            long256B = null;
        }
        return new FastMapRecord(valueOffsets, split, keyDataOffset, keyBlockOffset, csA, csB, bs, long256A, long256B);
    }

    private CharSequence getStr0(int index, DirectCharSequence cs) {
        long address = addressOfColumn(index);
        int len = Unsafe.getUnsafe().getInt(address);
        return len == TableUtils.NULL_LEN ? null : cs.of(address + 4, address + 4 + len * 2);
    }

    void of(long address) {
        this.address0 = address;
        this.address1 = address + keyDataOffset;
        this.address2 = address + keyBlockOffset;
    }

    private static class DirectBinarySequence implements BinarySequence {
        private long address;
        private long len;

        @Override
        public byte byteAt(long index) {
            return Unsafe.getUnsafe().getByte(address + index);
        }

        @Override
        public long length() {
            return len;
        }

        public void of(long address, long len) {
            this.address = address;
            this.len = len;
        }
    }
}
