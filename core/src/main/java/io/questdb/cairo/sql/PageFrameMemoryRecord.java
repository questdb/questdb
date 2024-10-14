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
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.vm.NullMemoryCMR;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.Long256Acceptor;
import io.questdb.std.Long256Impl;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rows;
import io.questdb.std.Unsafe;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.StableStringSource;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8SplitString;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

/**
 * Must be initialized with a {@link #init(PageFrameMemory)} call
 * for a given page frame before any use.
 */
public class PageFrameMemoryRecord implements Record, StableStringSource, Closeable {
    private final ObjList<MemoryCR.ByteSequenceView> bsViews = new ObjList<>();
    private final ObjList<DirectString> csViewsA = new ObjList<>();
    private final ObjList<DirectString> csViewsB = new ObjList<>();
    private final ObjList<Long256Impl> longs256A = new ObjList<>();
    private final ObjList<Long256Impl> longs256B = new ObjList<>();
    private final ObjList<SymbolTable> symbolTableCache = new ObjList<>();
    private final ObjList<Utf8SplitString> utf8ViewsA = new ObjList<>();
    private final ObjList<Utf8SplitString> utf8ViewsB = new ObjList<>();
    private LongList auxPageAddresses;
    private LongList auxPageSizes;
    private byte frameFormat = -1;
    private int frameIndex = -1;
    private LongList pageAddresses;
    private LongList pageSizes;
    private long rowIdOffset;
    private long rowIndex;
    private boolean stableStrings;
    private SymbolTableSource symbolTableSource;

    public PageFrameMemoryRecord() {
    }

    public PageFrameMemoryRecord(PageFrameMemoryRecord other) {
        this.symbolTableSource = other.symbolTableSource;
        this.rowIndex = other.rowIndex;
        this.frameIndex = other.frameIndex;
        this.frameFormat = other.frameFormat;
        this.rowIdOffset = other.rowIdOffset;
        this.pageAddresses = other.pageAddresses;
        this.auxPageAddresses = other.auxPageAddresses;
        this.pageSizes = other.pageSizes;
        this.auxPageSizes = other.auxPageSizes;
        this.stableStrings = other.stableStrings;
    }

    @Override
    public void close() {
        Misc.freeObjListIfCloseable(symbolTableCache);
        symbolTableCache.clear();
        rowIndex = 0;
        frameIndex = -1;
        rowIdOffset = -1;
        pageAddresses = null;
        auxPageAddresses = null;
        pageSizes = null;
        auxPageSizes = null;
    }

    @Override
    public BinarySequence getBin(int columnIndex) {
        final long dataPageAddress = pageAddresses.getQuick(columnIndex);
        if (dataPageAddress != 0) {
            final long auxPageAddress = auxPageAddresses.getQuick(columnIndex);
            final long auxPageLim = auxPageSizes.getQuick(columnIndex);
            final long auxOffset = rowIndex << 3;
            if (auxPageLim < auxOffset + 8) {
                throw CairoException.critical(0)
                        .put("binary is outside of file boundary [auxOffset=")
                        .put(auxOffset)
                        .put(", auxPageLim=")
                        .put(auxPageLim)
                        .put(']');
            }
            final long dataPageLim = pageSizes.getQuick(columnIndex);
            final long dataOffset = Unsafe.getUnsafe().getLong(auxPageAddress + auxOffset);
            return getBin(dataPageAddress, dataOffset, dataPageLim, bsView(columnIndex));
        }
        return NullMemoryCMR.INSTANCE.getBin(0);
    }

    @Override
    public long getBinLen(int columnIndex) {
        final long dataPageAddress = pageAddresses.getQuick(columnIndex);
        if (dataPageAddress != 0) {
            final long auxPageAddress = auxPageAddresses.getQuick(columnIndex);
            final long auxPageLim = auxPageSizes.getQuick(columnIndex);
            final long auxOffset = rowIndex << 3;
            if (auxPageLim < auxOffset + 8) {
                throw CairoException.critical(0)
                        .put("binary is outside of file boundary [auxOffset=")
                        .put(auxOffset)
                        .put(", auxPageLim=")
                        .put(auxPageLim)
                        .put(']');
            }
            final long dataPageLim = pageSizes.getQuick(columnIndex);
            final long dataOffset = Unsafe.getUnsafe().getLong(auxPageAddress + auxOffset);
            if (dataPageLim < dataOffset + 8) {
                throw CairoException.critical(0)
                        .put("binary is outside of file boundary [dataOffset=")
                        .put(dataOffset)
                        .put(", dataPageLim=")
                        .put(dataPageLim)
                        .put(']');
            }
            return Unsafe.getUnsafe().getLong(dataPageAddress + dataOffset);
        }
        return NullMemoryCMR.INSTANCE.getBinLen(0);
    }

    @Override
    public boolean getBool(int columnIndex) {
        final long address = pageAddresses.getQuick(columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getByte(address + rowIndex) == 1;
        }
        return NullMemoryCMR.INSTANCE.getBool(0);
    }

    @Override
    public byte getByte(int columnIndex) {
        final long address = pageAddresses.getQuick(columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getByte(address + rowIndex);
        }
        return NullMemoryCMR.INSTANCE.getByte(0);
    }

    @Override
    public char getChar(int columnIndex) {
        final long address = pageAddresses.getQuick(columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getChar(address + (rowIndex << 1));
        }
        return NullMemoryCMR.INSTANCE.getChar(0);
    }

    @Override
    public double getDouble(int columnIndex) {
        final long address = pageAddresses.getQuick(columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getDouble(address + (rowIndex << 3));
        }
        return NullMemoryCMR.INSTANCE.getDouble(0);
    }

    @Override
    public float getFloat(int columnIndex) {
        final long address = pageAddresses.getQuick(columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getFloat(address + (rowIndex << 2));
        }
        return NullMemoryCMR.INSTANCE.getFloat(0);
    }

    public int getFrameIndex() {
        return frameIndex;
    }

    @Override
    public byte getGeoByte(int columnIndex) {
        final long address = pageAddresses.getQuick(columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getByte(address + rowIndex);
        }
        return GeoHashes.BYTE_NULL;
    }

    @Override
    public int getGeoInt(int columnIndex) {
        final long address = pageAddresses.getQuick(columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getInt(address + (rowIndex << 2));
        }
        return GeoHashes.INT_NULL;
    }

    @Override
    public long getGeoLong(int columnIndex) {
        final long address = pageAddresses.getQuick(columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getLong(address + (rowIndex << 3));
        }
        return GeoHashes.NULL;
    }

    @Override
    public short getGeoShort(int columnIndex) {
        final long address = pageAddresses.getQuick(columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getShort(address + (rowIndex << 1));
        }
        return GeoHashes.SHORT_NULL;
    }

    @Override
    public int getIPv4(int columnIndex) {
        final long address = pageAddresses.getQuick(columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getInt(address + (rowIndex << 2));
        }
        return NullMemoryCMR.INSTANCE.getIPv4(0);
    }

    @Override
    public int getInt(int columnIndex) {
        final long address = pageAddresses.getQuick(columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getInt(address + (rowIndex << 2));
        }
        return NullMemoryCMR.INSTANCE.getInt(0);
    }

    @Override
    public long getLong(int columnIndex) {
        final long address = pageAddresses.getQuick(columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getLong(address + (rowIndex << 3));
        }
        return NullMemoryCMR.INSTANCE.getLong(0);
    }

    @Override
    public long getLong128Hi(int columnIndex) {
        long address = pageAddresses.getQuick(columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getLong(address + (rowIndex << 4) + Long.BYTES);
        }
        return NullMemoryCMR.INSTANCE.getLong128Hi();
    }

    @Override
    public long getLong128Lo(int columnIndex) {
        long address = pageAddresses.getQuick(columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getLong(address + (rowIndex << 4));
        }
        return NullMemoryCMR.INSTANCE.getLong128Lo();
    }

    @Override
    public void getLong256(int columnIndex, CharSink<?> sink) {
        final long address = pageAddresses.getQuick(columnIndex);
        if (address != 0) {
            getLong256(address + rowIndex * Long256.BYTES, sink);
            return;
        }
        NullMemoryCMR.INSTANCE.getLong256(0, sink);
    }

    @Override
    public Long256 getLong256A(int columnIndex) {
        Long256 long256 = long256A(columnIndex);
        getLong256(columnIndex, long256);
        return long256;
    }

    @Override
    public Long256 getLong256B(int columnIndex) {
        Long256 long256 = long256B(columnIndex);
        getLong256(columnIndex, long256);
        return long256;
    }

    @Override
    public long getLongIPv4(int columnIndex) {
        return Numbers.ipv4ToLong(getIPv4(columnIndex));
    }

    @Override
    public long getRowId() {
        return Rows.toRowID(frameIndex, rowIndex);
    }

    public long getRowIndex() {
        return rowIndex;
    }

    @Override
    public short getShort(int columnIndex) {
        final long address = pageAddresses.getQuick(columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getShort(address + (rowIndex << 1));
        }
        return NullMemoryCMR.INSTANCE.getShort(0);
    }

    @Override
    public CharSequence getStrA(int columnIndex) {
        final long dataPageAddress = pageAddresses.getQuick(columnIndex);
        if (dataPageAddress != 0) {
            final long auxPageAddress = auxPageAddresses.getQuick(columnIndex);
            final long auxPageLim = auxPageSizes.getQuick(columnIndex);
            final long auxOffset = rowIndex << 3;
            if (auxPageLim < auxOffset + 8) {
                throw CairoException.critical(0)
                        .put("string is outside of file boundary [auxOffset=")
                        .put(auxOffset)
                        .put(", auxPageLim=")
                        .put(auxPageLim)
                        .put(']');
            }
            final long dataPageLim = pageSizes.getQuick(columnIndex);
            final long dataOffset = Unsafe.getUnsafe().getLong(auxPageAddress + auxOffset);
            return getStr(dataPageAddress, dataOffset, dataPageLim, csViewA(columnIndex));
        }
        return NullMemoryCMR.INSTANCE.getStrA(0);
    }

    @Override
    public CharSequence getStrB(int columnIndex) {
        final long dataPageAddress = pageAddresses.getQuick(columnIndex);
        if (dataPageAddress != 0) {
            final long auxPageAddress = auxPageAddresses.getQuick(columnIndex);
            final long auxPageLim = auxPageSizes.getQuick(columnIndex);
            final long auxOffset = rowIndex << 3;
            if (auxPageLim < auxOffset + 8) {
                throw CairoException.critical(0)
                        .put("string is outside of file boundary [auxOffset=")
                        .put(auxOffset)
                        .put(", auxPageLim=")
                        .put(auxPageLim)
                        .put(']');
            }
            final long dataPageLim = pageSizes.getQuick(columnIndex);
            final long dataOffset = Unsafe.getUnsafe().getLong(auxPageAddress + auxOffset);
            return getStr(dataPageAddress, dataOffset, dataPageLim, csViewB(columnIndex));
        }
        return NullMemoryCMR.INSTANCE.getStrB(0);
    }

    @Override
    public int getStrLen(int columnIndex) {
        final long dataPageAddress = pageAddresses.getQuick(columnIndex);
        if (dataPageAddress != 0) {
            final long auxPageAddress = auxPageAddresses.getQuick(columnIndex);
            final long auxPageLim = auxPageSizes.getQuick(columnIndex);
            final long auxOffset = rowIndex << 3;
            if (auxPageLim < auxOffset + 8) {
                throw CairoException.critical(0)
                        .put("string is outside of file boundary [auxOffset=")
                        .put(auxOffset)
                        .put(", auxPageLim=")
                        .put(auxPageLim)
                        .put(']');
            }
            final long dataPageLim = pageSizes.getQuick(columnIndex);
            final long dataOffset = Unsafe.getUnsafe().getLong(auxPageAddress + auxOffset);
            if (dataPageLim < dataOffset + 4) {
                throw CairoException.critical(0)
                        .put("string is outside of file boundary [dataOffset=")
                        .put(dataOffset)
                        .put(", dataPageLim=")
                        .put(dataPageLim)
                        .put(']');
            }
            return Unsafe.getUnsafe().getInt(dataPageAddress + dataOffset);
        }
        return NullMemoryCMR.INSTANCE.getStrLen(0);
    }

    @Override
    public CharSequence getSymA(int columnIndex) {
        final long address = pageAddresses.getQuick(columnIndex);
        int key = NullMemoryCMR.INSTANCE.getInt(0);
        if (address != 0) {
            key = Unsafe.getUnsafe().getInt(address + (rowIndex << 2));
        }
        return getSymbolTable(columnIndex).valueOf(key);
    }

    @Override
    public CharSequence getSymB(int columnIndex) {
        final long address = pageAddresses.getQuick(columnIndex);
        final int key = Unsafe.getUnsafe().getInt(address + (rowIndex << 2));
        return getSymbolTable(columnIndex).valueBOf(key);
    }

    @Override
    public long getUpdateRowId() {
        return rowIdOffset + rowIndex;
    }

    @Override
    public Utf8Sequence getVarcharA(int columnIndex) {
        return getVarchar(columnIndex, utf8ViewA(columnIndex));
    }

    @Override
    public Utf8Sequence getVarcharB(int columnIndex) {
        return getVarchar(columnIndex, utf8ViewB(columnIndex));
    }

    @Override
    public int getVarcharSize(int columnIndex) {
        final long auxPageAddress = auxPageAddresses.getQuick(columnIndex);
        if (auxPageAddress != 0) {
            return VarcharTypeDriver.getValueSize(auxPageAddress, rowIndex);
        }
        return TableUtils.NULL_LEN; // Column top.
    }

    public void init(PageFrameMemory frameMemory) {
        this.frameIndex = frameMemory.getFrameIndex();
        this.frameFormat = frameMemory.getFrameFormat();
        this.stableStrings = (frameFormat == PageFrame.NATIVE_FORMAT);
        this.rowIdOffset = frameMemory.getRowIdOffset();
        this.pageAddresses = frameMemory.getPageAddresses();
        this.auxPageAddresses = frameMemory.getAuxPageAddresses();
        this.pageSizes = frameMemory.getPageSizes();
        this.auxPageSizes = frameMemory.getAuxPageSizes();
    }

    @Override
    public boolean isStable() {
        return stableStrings;
    }

    public void of(SymbolTableSource symbolTableSource) {
        close();
        this.symbolTableSource = symbolTableSource;
    }

    public void setRowIndex(long rowIndex) {
        this.rowIndex = rowIndex;
    }

    private MemoryCR.ByteSequenceView bsView(int columnIndex) {
        if (bsViews.getQuiet(columnIndex) == null) {
            bsViews.extendAndSet(columnIndex, new MemoryCR.ByteSequenceView());
        }
        return bsViews.getQuick(columnIndex);
    }

    private DirectString csViewA(int columnIndex) {
        if (csViewsA.getQuiet(columnIndex) == null) {
            csViewsA.extendAndSet(columnIndex, new DirectString(this));
        }
        return csViewsA.getQuick(columnIndex);
    }

    private DirectString csViewB(int columnIndex) {
        if (csViewsB.getQuiet(columnIndex) == null) {
            csViewsB.extendAndSet(columnIndex, new DirectString(this));
        }
        return csViewsB.getQuick(columnIndex);
    }

    private BinarySequence getBin(long base, long offset, long dataLim, MemoryCR.ByteSequenceView view) {
        final long address = base + offset;
        final long len = Unsafe.getUnsafe().getLong(address);
        if (len != TableUtils.NULL_LEN) {
            if (dataLim < offset + len + 8) {
                throw CairoException.critical(0)
                        .put("binary is outside of file boundary [offset=")
                        .put(offset)
                        .put(", len=")
                        .put(len)
                        .put(", dataLim=")
                        .put(dataLim)
                        .put(']');
            }
            return view.of(address + Long.BYTES, len);
        }
        return null;
    }

    private void getLong256(int columnIndex, Long256Acceptor sink) {
        final long columnAddress = pageAddresses.getQuick(columnIndex);
        if (columnAddress != 0) {
            sink.fromAddress(columnAddress + (rowIndex << 5));
            return;
        }
        NullMemoryCMR.INSTANCE.getLong256(0, sink);
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

    private DirectString getStr(long base, long offset, long dataLim, DirectString view) {
        final long address = base + offset;
        final int len = Unsafe.getUnsafe().getInt(address);
        if (len != TableUtils.NULL_LEN) {
            if (dataLim < offset + len + 4) {
                throw CairoException.critical(0)
                        .put("string is outside of file boundary [offset=")
                        .put(offset)
                        .put(", len=")
                        .put(len)
                        .put(", dataLim=")
                        .put(dataLim)
                        .put(']');
            }
            return view.of(address + Vm.STRING_LENGTH_BYTES, len);
        }
        return null; // Column top.
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
        final long auxPageAddress = auxPageAddresses.getQuick(columnIndex);
        if (auxPageAddress != 0) {
            final long auxPageLim = auxPageAddress + auxPageSizes.getQuick(columnIndex);
            final long dataPageAddress = pageAddresses.getQuick(columnIndex);
            final long dataPageLim = dataPageAddress + pageSizes.getQuick(columnIndex);
            return VarcharTypeDriver.getSplitValue(
                    auxPageAddress,
                    auxPageLim,
                    dataPageAddress,
                    dataPageLim,
                    rowIndex,
                    utf8View
            );
        }
        return null; // Column top.
    }

    private Long256Impl long256A(int columnIndex) {
        if (longs256A.getQuiet(columnIndex) == null) {
            longs256A.extendAndSet(columnIndex, new Long256Impl());
        }
        return longs256A.getQuick(columnIndex);
    }

    private Long256Impl long256B(int columnIndex) {
        if (longs256B.getQuiet(columnIndex) == null) {
            longs256B.extendAndSet(columnIndex, new Long256Impl());
        }
        return longs256B.getQuick(columnIndex);
    }

    private Utf8SplitString utf8ViewA(int columnIndex) {
        if (utf8ViewsA.getQuiet(columnIndex) == null) {
            utf8ViewsA.extendAndSet(columnIndex, new Utf8SplitString(this));
        }
        return utf8ViewsA.getQuick(columnIndex);
    }

    private Utf8SplitString utf8ViewB(int columnIndex) {
        if (utf8ViewsB.getQuiet(columnIndex) == null) {
            utf8ViewsB.extendAndSet(columnIndex, new Utf8SplitString(this));
        }
        return utf8ViewsB.getQuick(columnIndex);
    }

    void init(
            int frameIndex,
            byte frameFormat,
            long rowIdOffset,
            LongList pageAddresses,
            LongList auxPageAddresses,
            LongList pageLimits,
            LongList auxPageLimits
    ) {
        this.frameIndex = frameIndex;
        this.frameFormat = frameFormat;
        this.stableStrings = (frameFormat == PageFrame.NATIVE_FORMAT);
        this.rowIdOffset = rowIdOffset;
        this.pageAddresses = pageAddresses;
        this.auxPageAddresses = auxPageAddresses;
        this.pageSizes = pageLimits;
        this.auxPageSizes = auxPageLimits;
    }
}
