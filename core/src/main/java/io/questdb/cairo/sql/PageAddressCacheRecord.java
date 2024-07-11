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

package io.questdb.cairo.sql;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.vm.NullMemoryCMR;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.std.*;
import io.questdb.std.str.*;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

public class PageAddressCacheRecord implements Record, Closeable {

    private final MemoryCR.ByteSequenceView bsview = new MemoryCR.ByteSequenceView();
    private final StableDirectString csviewA = new StableDirectString();
    private final StableDirectString csviewB = new StableDirectString();
    private final Long256Impl long256A = new Long256Impl();
    private final Long256Impl long256B = new Long256Impl();
    private final ObjList<SymbolTable> symbolTableCache = new ObjList<>();
    private final Utf8SplitString utf8ViewA = new Utf8SplitString(true);
    private final Utf8SplitString utf8ViewB = new Utf8SplitString(true);
    private int frameIndex;
    private PageAddressCache pageAddressCache;
    private long rowIndex;
    // Makes it possible to determine real row id, not one relative to page.
    private SymbolTableSource symbolTableSource;

    public PageAddressCacheRecord(PageAddressCacheRecord other) {
        this.symbolTableSource = other.symbolTableSource;
        this.pageAddressCache = other.pageAddressCache;
        this.frameIndex = other.frameIndex;
        this.rowIndex = other.rowIndex;
    }

    public PageAddressCacheRecord() {
    }

    @Override
    public void close() {
        Misc.freeObjListIfCloseable(symbolTableCache);
    }

    @Override
    public BinarySequence getBin(int columnIndex) {
        final long dataPageAddress = pageAddressCache.getPageAddress(frameIndex, columnIndex);
        if (dataPageAddress == 0) {
            return NullMemoryCMR.INSTANCE.getBin(0);
        }
        final long indexPageAddress = pageAddressCache.getAuxPageAddress(frameIndex, columnIndex);
        final long offset = Unsafe.getUnsafe().getLong(indexPageAddress + (rowIndex << 3));
        final long pageLimit = pageAddressCache.getPageSize(frameIndex, columnIndex);
        return getBin(dataPageAddress, offset, pageLimit, bsview);
    }

    @Override
    public long getBinLen(int columnIndex) {
        final long dataPageAddress = pageAddressCache.getPageAddress(frameIndex, columnIndex);
        if (dataPageAddress == 0) {
            return NullMemoryCMR.INSTANCE.getBinLen(0);
        }
        final long indexPageAddress = pageAddressCache.getAuxPageAddress(frameIndex, columnIndex);
        final long offset = Unsafe.getUnsafe().getLong(indexPageAddress + (rowIndex << 3));
        return Unsafe.getUnsafe().getLong(dataPageAddress + offset);
    }

    @Override
    public boolean getBool(int columnIndex) {
        final long address = pageAddressCache.getPageAddress(frameIndex, columnIndex);
        if (address == 0) {
            return NullMemoryCMR.INSTANCE.getBool(0);
        }
        return Unsafe.getUnsafe().getByte(address + rowIndex) == 1;
    }

    @Override
    public byte getByte(int columnIndex) {
        final long address = pageAddressCache.getPageAddress(frameIndex, columnIndex);
        if (address == 0) {
            return NullMemoryCMR.INSTANCE.getByte(0);
        }
        return Unsafe.getUnsafe().getByte(address + rowIndex);
    }

    @Override
    public char getChar(int columnIndex) {
        final long address = pageAddressCache.getPageAddress(frameIndex, columnIndex);
        if (address == 0) {
            return NullMemoryCMR.INSTANCE.getChar(0);
        }
        return Unsafe.getUnsafe().getChar(address + (rowIndex << 1));
    }

    @Override
    public double getDouble(int columnIndex) {
        final long address = pageAddressCache.getPageAddress(frameIndex, columnIndex);
        if (address == 0) {
            return NullMemoryCMR.INSTANCE.getDouble(0);
        }
        return Unsafe.getUnsafe().getDouble(address + (rowIndex << 3));
    }

    @Override
    public float getFloat(int columnIndex) {
        final long address = pageAddressCache.getPageAddress(frameIndex, columnIndex);
        if (address == 0) {
            return NullMemoryCMR.INSTANCE.getFloat(0);
        }
        return Unsafe.getUnsafe().getFloat(address + (rowIndex << 2));
    }

    @Override
    public byte getGeoByte(int columnIndex) {
        final long address = pageAddressCache.getPageAddress(frameIndex, columnIndex);
        if (address == 0) {
            return NullMemoryCMR.INSTANCE.getByte(0);
        }
        return Unsafe.getUnsafe().getByte(address + rowIndex);
    }

    @Override
    public int getGeoInt(int columnIndex) {
        final long address = pageAddressCache.getPageAddress(frameIndex, columnIndex);
        if (address == 0) {
            return NullMemoryCMR.INSTANCE.getInt(0);
        }
        return Unsafe.getUnsafe().getInt(address + (rowIndex << 2));
    }

    @Override
    public long getGeoLong(int columnIndex) {
        final long address = pageAddressCache.getPageAddress(frameIndex, columnIndex);
        if (address == 0) {
            return NullMemoryCMR.INSTANCE.getLong(0);
        }
        return Unsafe.getUnsafe().getLong(address + (rowIndex << 3));
    }

    @Override
    public short getGeoShort(int columnIndex) {
        final long address = pageAddressCache.getPageAddress(frameIndex, columnIndex);
        if (address == 0) {
            return NullMemoryCMR.INSTANCE.getShort(0);
        }
        return Unsafe.getUnsafe().getShort(address + (rowIndex << 1));
    }

    @Override
    public int getIPv4(int columnIndex) {
        final long address = pageAddressCache.getPageAddress(frameIndex, columnIndex);
        if (address == 0) {
            return NullMemoryCMR.INSTANCE.getIPv4(0);
        }
        return Unsafe.getUnsafe().getInt(address + (rowIndex << 2));
    }

    @Override
    public int getInt(int columnIndex) {
        final long address = pageAddressCache.getPageAddress(frameIndex, columnIndex);
        if (address == 0) {
            return NullMemoryCMR.INSTANCE.getInt(0);
        }
        return Unsafe.getUnsafe().getInt(address + (rowIndex << 2));
    }

    @Override
    public long getLong(int columnIndex) {
        final long address = pageAddressCache.getPageAddress(frameIndex, columnIndex);
        if (address == 0) {
            return NullMemoryCMR.INSTANCE.getLong(0);
        }
        return Unsafe.getUnsafe().getLong(address + (rowIndex << 3));
    }

    @Override
    public long getLong128Hi(int col) {
        long address = pageAddressCache.getPageAddress(frameIndex, col);
        if (address == 0) {
            return NullMemoryCMR.INSTANCE.getLong128Hi();
        }
        return Unsafe.getUnsafe().getLong(address + (rowIndex << 4) + Long.BYTES);
    }

    @Override
    public long getLong128Lo(int col) {
        long address = pageAddressCache.getPageAddress(frameIndex, col);
        if (address == 0) {
            return NullMemoryCMR.INSTANCE.getLong128Lo();
        }
        return Unsafe.getUnsafe().getLong(address + (rowIndex << 4));
    }

    @Override
    public void getLong256(int columnIndex, CharSink<?> sink) {
        final long address = pageAddressCache.getPageAddress(frameIndex, columnIndex);
        if (address == 0) {
            NullMemoryCMR.INSTANCE.getLong256(0, sink);
            return;
        }
        getLong256(address + rowIndex * Long256.BYTES, sink);
    }

    @Override
    public Long256 getLong256A(int columnIndex) {
        getLong256(columnIndex, long256A);
        return long256A;
    }

    @Override
    public Long256 getLong256B(int columnIndex) {
        getLong256(columnIndex, long256B);
        return long256B;
    }

    @Override
    public long getRowId() {
        return Rows.toRowID(frameIndex, rowIndex);
    }

    @Override
    public short getShort(int columnIndex) {
        final long address = pageAddressCache.getPageAddress(frameIndex, columnIndex);
        if (address == 0) {
            return NullMemoryCMR.INSTANCE.getShort(0);
        }
        return Unsafe.getUnsafe().getShort(address + (rowIndex << 1));
    }

    @Override
    public CharSequence getStrA(int columnIndex) {
        final long dataPageAddress = pageAddressCache.getPageAddress(frameIndex, columnIndex);
        if (dataPageAddress == 0) {
            return NullMemoryCMR.INSTANCE.getStrA(0);
        }
        final long indexPageAddress = pageAddressCache.getAuxPageAddress(frameIndex, columnIndex);
        final long offset = Unsafe.getUnsafe().getLong(indexPageAddress + (rowIndex << 3));
        final long size = pageAddressCache.getPageSize(frameIndex, columnIndex);
        return getStrA(dataPageAddress, offset, size, csviewA);
    }

    @Override
    public CharSequence getStrB(int columnIndex) {
        final long dataPageAddress = pageAddressCache.getPageAddress(frameIndex, columnIndex);
        if (dataPageAddress == 0) {
            return NullMemoryCMR.INSTANCE.getStrB(0);
        }
        final long indexPageAddress = pageAddressCache.getAuxPageAddress(frameIndex, columnIndex);
        final long offset = Unsafe.getUnsafe().getLong(indexPageAddress + (rowIndex << 3));
        final long size = pageAddressCache.getPageSize(frameIndex, columnIndex);
        return getStrA(dataPageAddress, offset, size, csviewB);
    }

    @Override
    public int getStrLen(int columnIndex) {
        final long dataPageAddress = pageAddressCache.getPageAddress(frameIndex, columnIndex);
        if (dataPageAddress == 0) {
            return NullMemoryCMR.INSTANCE.getStrLen(0);
        }
        final long indexPageAddress = pageAddressCache.getAuxPageAddress(frameIndex, columnIndex);
        final long offset = Unsafe.getUnsafe().getLong(indexPageAddress + (rowIndex << 3));
        return Unsafe.getUnsafe().getInt(dataPageAddress + offset);
    }

    @Override
    public CharSequence getSymA(int columnIndex) {
        final long address = pageAddressCache.getPageAddress(frameIndex, columnIndex);
        int key = NullMemoryCMR.INSTANCE.getInt(0);
        if (address != 0) {
            key = Unsafe.getUnsafe().getInt(address + (rowIndex << 2));
        }
        return getSymbolTable(columnIndex).valueOf(key);
    }

    @Override
    public CharSequence getSymB(int columnIndex) {
        final long address = pageAddressCache.getPageAddress(frameIndex, columnIndex);
        final int key = Unsafe.getUnsafe().getInt(address + (rowIndex << 2));
        return getSymbolTable(columnIndex).valueBOf(key);
    }

    @Override
    public long getUpdateRowId() {
        return pageAddressCache.toTableRowID(frameIndex, rowIndex);
    }

    @Override
    public Utf8Sequence getVarcharA(int columnIndex) {
        return getVarchar(columnIndex, utf8ViewA);
    }

    @Override
    public Utf8Sequence getVarcharB(int columnIndex) {
        return getVarchar(columnIndex, utf8ViewB);
    }

    @Override
    public int getVarcharSize(int columnIndex) {
        final long auxPageAddress = pageAddressCache.getAuxPageAddress(frameIndex, columnIndex);
        if (auxPageAddress == 0) {
            // Column top.
            return TableUtils.NULL_LEN;
        }
        return VarcharTypeDriver.getValueSize(auxPageAddress, rowIndex);
    }

    public void of(SymbolTableSource symbolTableSource, PageAddressCache pageAddressCache) {
        this.symbolTableSource = symbolTableSource;
        this.pageAddressCache = pageAddressCache;
        frameIndex = 0;
        rowIndex = 0;
        Misc.freeObjListIfCloseable(symbolTableCache);
        symbolTableCache.clear();
    }

    public void setFrameIndex(int frameIndex) {
        this.frameIndex = frameIndex;
    }

    public void setRowIndex(long rowIndex) {
        this.rowIndex = rowIndex;
    }

    private BinarySequence getBin(long base, long offset, long pageLimit, MemoryCR.ByteSequenceView view) {
        final long address = base + offset;
        final long len = Unsafe.getUnsafe().getLong(address);
        if (len != TableUtils.NULL_LEN) {
            if (len + Long.BYTES + offset <= pageLimit) {
                return view.of(address + Long.BYTES, len);
            }
            throw CairoException.critical(0)
                    .put("Bin is outside of file boundary [offset=")
                    .put(offset)
                    .put(", len=")
                    .put(len)
                    .put(", pageLimit=")
                    .put(pageLimit)
                    .put(']');
        }
        return null;
    }

    private void getLong256(int columnIndex, Long256Acceptor sink) {
        final long columnAddress = pageAddressCache.getPageAddress(frameIndex, columnIndex);
        if (columnAddress == 0) {
            NullMemoryCMR.INSTANCE.getLong256(0, sink);
            return;
        }
        sink.fromAddress(columnAddress + (rowIndex << 5));
    }

    private void getLong256(long offset, CharSink<?> sink) {
        final long addr = offset + Long.BYTES * 4;
        final long a, b, c, d;
        a = Unsafe.getUnsafe().getLong(addr - Long.BYTES * 4);
        b = Unsafe.getUnsafe().getLong(addr - Long.BYTES * 3);
        c = Unsafe.getUnsafe().getLong(addr - Long.BYTES * 2);
        d = Unsafe.getUnsafe().getLong(addr - Long.BYTES);
        Numbers.appendLong256(a, b, c, d, sink);
    }

    private DirectString getStrA(long base, long offset, long size, DirectString view) {
        final long address = base + offset;
        final int len = Unsafe.getUnsafe().getInt(address);
        if (len != TableUtils.NULL_LEN) {
            if (len + 4 + offset <= size) {
                return view.of(address + Vm.STRING_LENGTH_BYTES, len);
            }
            throw CairoException.critical(0)
                    .put("String is outside of file boundary [offset=")
                    .put(offset)
                    .put(", len=")
                    .put(len)
                    .put(", size=")
                    .put(size)
                    .put(']');
        }
        return null;
    }

    private SymbolTable getSymbolTable(int columnIndex) {
        SymbolTable symbolTable = symbolTableCache.getQuiet(columnIndex);
        if (symbolTable == null) {
            symbolTable = symbolTableSource.newSymbolTable(columnIndex);
            symbolTableCache.extendAndSet(columnIndex, symbolTable);
        }
        return symbolTable;
    }

    @Nullable
    private Utf8Sequence getVarchar(int columnIndex, Utf8SplitString utf8View) {
        final long auxPageAddress = pageAddressCache.getAuxPageAddress(frameIndex, columnIndex);
        if (auxPageAddress == 0) {
            return null; // Column top.
        }
        final long varcharAuxPageLim = pageAddressCache.getVarcharAuxPageLimit(frameIndex, columnIndex);
        final long dataPageAddress = pageAddressCache.getPageAddress(frameIndex, columnIndex);
        final long dataPageLim = dataPageAddress + pageAddressCache.getPageLimit(frameIndex, columnIndex);
        return VarcharTypeDriver.getSplitValue(
                auxPageAddress,
                varcharAuxPageLim,
                dataPageAddress,
                dataPageLim,
                rowIndex,
                utf8View);
    }
}
