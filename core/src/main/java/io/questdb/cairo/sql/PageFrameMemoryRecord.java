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

package io.questdb.cairo.sql;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.BorrowedArray;
import io.questdb.cairo.vm.NullMemoryCMR;
import io.questdb.cairo.vm.Vm;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimal64;
import io.questdb.std.Decimals;
import io.questdb.std.DirectByteSequenceView;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.Long256Acceptor;
import io.questdb.std.Long256Impl;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Rows;
import io.questdb.std.Unsafe;
import io.questdb.std.Uuid;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.datetime.nanotime.NanosFormatUtils;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.StableStringSource;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8SplitString;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.griffin.SqlKeywords;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Must be initialized with a {@link PageFrameMemoryPool#navigateTo(int, PageFrameMemoryRecord)}
 * or {@link #init(PageFrameMemory)} call for a given page frame before any use.
 */
public class PageFrameMemoryRecord implements Record, StableStringSource, QuietCloseable, Mutable {
    public static final byte RECORD_A_LETTER = 0;
    public static final byte RECORD_B_LETTER = 1;
    protected final ObjList<BorrowedArray> arrayBuffers = new ObjList<>();
    protected final ObjList<DirectByteSequenceView> bsViews = new ObjList<>();
    protected final ObjList<DirectString> csViewsA = new ObjList<>();
    protected final ObjList<DirectString> csViewsB = new ObjList<>();
    protected final ObjList<Long256Impl> longs256A = new ObjList<>();
    protected final ObjList<Long256Impl> longs256B = new ObjList<>();
    protected final ObjList<SymbolTable> symbolTableCache = new ObjList<>();
    protected final ObjList<Utf8SplitString> utf8ViewsA = new ObjList<>();
    protected final ObjList<Utf8SplitString> utf8ViewsB = new ObjList<>();
    protected DirectLongList auxPageAddresses;
    protected DirectLongList auxPageSizes;
    protected int columnOffset;
    protected byte frameFormat = -1;
    protected int frameIndex = -1;
    // True when any column in the current frame needs lazy fixed→var conversion.
    protected boolean hasTypeCasts;
    // Letters are used for parquet buffer reference counting in PageFrameMemoryPool.
    // RECORD_A_LETTER (0) stands for record A, RECORD_B_LETTER (1) stands for record B.
    protected byte letter;
    protected DirectLongList pageAddresses;
    protected DirectLongList pageSizes;
    protected long rowIdOffset;
    protected long rowIndex;
    // Per-column source type tag for fixed→var type-cast columns.
    // Set from PageFrameMemoryPool during init(); null when hasTypeCasts is false.
    protected IntList sourceColumnTypes;
    protected boolean stableStrings;
    // Reusable decimal instances for lazy var→decimal conversion.
    private final Decimal128 decimal128Buf = new Decimal128();
    private final Decimal256 decimal256Buf = new Decimal256();
    private final Decimal64 decimal64Buf = new Decimal64();
    // Reusable sinks for lazy fixed→varchar conversion.
    private final StringSink stringSinkA = new StringSink();
    private final StringSink stringSinkB = new StringSink();
    private final Utf8StringSink varcharSinkA = new Utf8StringSink();
    private final Utf8StringSink varcharSinkB = new Utf8StringSink();
    protected SymbolTableSource symbolTableSource;

    public PageFrameMemoryRecord() {
    }

    public PageFrameMemoryRecord(byte letter) {
        this.letter = letter;
    }

    public PageFrameMemoryRecord(PageFrameMemoryRecord other, byte letter) {
        this.symbolTableSource = other.symbolTableSource;
        this.rowIndex = other.rowIndex;
        this.frameIndex = other.frameIndex;
        this.frameFormat = other.frameFormat;
        this.rowIdOffset = other.rowIdOffset;
        this.pageAddresses = other.pageAddresses;
        this.auxPageAddresses = other.auxPageAddresses;
        this.pageSizes = other.pageSizes;
        this.auxPageSizes = other.auxPageSizes;
        this.columnOffset = other.columnOffset;
        this.stableStrings = other.stableStrings;
        this.hasTypeCasts = other.hasTypeCasts;
        this.letter = letter;
    }

    @Override
    public void clear() {
        rowIndex = 0;
        frameIndex = -1;
        rowIdOffset = -1;
        columnOffset = 0;
        pageAddresses = null;
        auxPageAddresses = null;
        pageSizes = null;
        auxPageSizes = null;
    }

    @Override
    public void close() {
        Misc.freeObjListIfCloseable(symbolTableCache);
        symbolTableCache.clear();
        Misc.freeObjList(arrayBuffers);
        clear();
    }

    public long getPageAddress(int columnIndex) {
        return pageAddresses.get(columnOffset + columnIndex);
    }

    @Override
    public ArrayView getArray(int columnIndex, int columnType) {
        final BorrowedArray array = borrowedArray(columnIndex);
        final long auxPageAddress = auxPageAddresses.get(columnOffset + columnIndex);
        if (auxPageAddress != 0) {
            final long auxPageLim = auxPageAddress + auxPageSizes.get(columnOffset + columnIndex);
            final long dataPageAddress = pageAddresses.get(columnOffset + columnIndex);
            final long dataPageLim = dataPageAddress + pageSizes.get(columnOffset + columnIndex);
            array.of(
                    columnType,
                    auxPageAddress,
                    auxPageLim,
                    dataPageAddress,
                    dataPageLim,
                    rowIndex
            );
        } else {
            array.ofNull();
        }
        return array;
    }

    @Override
    public double getArrayDouble1d2d(int columnIndex, int columnType, int idx0, int idx1) {
        return getArrayDouble1d2d0(columnIndex, columnType, idx0, idx1, rowIndex);
    }

    @Override
    public BinarySequence getBin(int columnIndex) {
        final long dataPageAddress = pageAddresses.get(columnOffset + columnIndex);
        if (dataPageAddress != 0) {
            final long auxPageAddress = auxPageAddresses.get(columnOffset + columnIndex);
            final long auxPageLim = auxPageSizes.get(columnOffset + columnIndex);
            final long auxOffset = rowIndex << 3;
            if (auxPageLim < auxOffset + 8) {
                throw CairoException.critical(0)
                        .put("binary is outside of file boundary [auxOffset=")
                        .put(auxOffset)
                        .put(", auxPageLim=")
                        .put(auxPageLim)
                        .put(']');
            }
            final long dataPageLim = pageSizes.get(columnOffset + columnIndex);
            final long dataOffset = Unsafe.getUnsafe().getLong(auxPageAddress + auxOffset);
            return getBin(dataPageAddress, dataOffset, dataPageLim, bsView(columnIndex));
        }
        return NullMemoryCMR.INSTANCE.getBin(0);
    }

    @Override
    public long getBinLen(int columnIndex) {
        final long dataPageAddress = pageAddresses.get(columnOffset + columnIndex);
        if (dataPageAddress != 0) {
            final long auxPageAddress = auxPageAddresses.get(columnOffset + columnIndex);
            final long auxPageLim = auxPageSizes.get(columnOffset + columnIndex);
            final long auxOffset = rowIndex << 3;
            if (auxPageLim < auxOffset + 8) {
                throw CairoException.critical(0)
                        .put("binary is outside of file boundary [auxOffset=")
                        .put(auxOffset)
                        .put(", auxPageLim=")
                        .put(auxPageLim)
                        .put(']');
            }
            final long dataPageLim = pageSizes.get(columnOffset + columnIndex);
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
        if (hasTypeCasts) {
            int srcTag = sourceColumnTypes.getQuick(columnIndex);
            if (srcTag < -1) {
                return convertVarToBool(-srcTag, columnIndex);
            }
        }
        final long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getByte(address + rowIndex) == 1;
        }
        return NullMemoryCMR.INSTANCE.getBool(0);
    }

    @Override
    public byte getByte(int columnIndex) {
        if (hasTypeCasts) {
            int srcTag = sourceColumnTypes.getQuick(columnIndex);
            if (srcTag < -1) {
                return convertVarToByte(-srcTag, columnIndex);
            }
        }
        final long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getByte(address + rowIndex);
        }
        return NullMemoryCMR.INSTANCE.getByte(0);
    }

    @Override
    public char getChar(int columnIndex) {
        if (hasTypeCasts) {
            int srcTag = sourceColumnTypes.getQuick(columnIndex);
            if (srcTag < -1) {
                return convertVarToChar(-srcTag, columnIndex);
            }
        }
        final long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getChar(address + (rowIndex << 1));
        }
        return NullMemoryCMR.INSTANCE.getChar(0);
    }

    @Override
    public void getDecimal128(int columnIndex, Decimal128 sink) {
        if (hasTypeCasts) {
            int srcTag = sourceColumnTypes.getQuick(columnIndex);
            if (srcTag < -1) {
                convertVarToDecimal128(-srcTag, columnIndex, sink);
                return;
            }
        }
        long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            address += (rowIndex << 4);
            sink.ofRaw(
                    Unsafe.getUnsafe().getLong(address),
                    Unsafe.getUnsafe().getLong(address + 8L)
            );
        } else {
            sink.ofRawNull();
        }
    }

    @Override
    public short getDecimal16(int columnIndex) {
        if (hasTypeCasts) {
            int srcTag = sourceColumnTypes.getQuick(columnIndex);
            if (srcTag < -1) {
                return convertVarToDecimal16(-srcTag, columnIndex);
            }
        }
        long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getShort(address + (rowIndex << 1));
        }
        return NullMemoryCMR.INSTANCE.getDecimal16(0);
    }

    @Override
    public void getDecimal256(int columnIndex, Decimal256 sink) {
        if (hasTypeCasts) {
            int srcTag = sourceColumnTypes.getQuick(columnIndex);
            if (srcTag < -1) {
                convertVarToDecimal256(-srcTag, columnIndex, sink);
                return;
            }
        }
        long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            sink.ofRawAddress(address + (rowIndex << 5));
        } else {
            sink.ofRawNull();
        }
    }

    @Override
    public int getDecimal32(int columnIndex) {
        if (hasTypeCasts) {
            int srcTag = sourceColumnTypes.getQuick(columnIndex);
            if (srcTag < -1) {
                return convertVarToDecimal32(-srcTag, columnIndex);
            }
        }
        long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getInt(address + (rowIndex << 2));
        }
        return NullMemoryCMR.INSTANCE.getDecimal32(0);
    }

    @Override
    public long getDecimal64(int columnIndex) {
        if (hasTypeCasts) {
            int srcTag = sourceColumnTypes.getQuick(columnIndex);
            if (srcTag < -1) {
                return convertVarToDecimal64(-srcTag, columnIndex);
            }
        }
        long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getLong(address + (rowIndex << 3));
        }
        return NullMemoryCMR.INSTANCE.getDecimal64(0);
    }

    @Override
    public byte getDecimal8(int columnIndex) {
        if (hasTypeCasts) {
            int srcTag = sourceColumnTypes.getQuick(columnIndex);
            if (srcTag < -1) {
                return convertVarToDecimal8(-srcTag, columnIndex);
            }
        }
        long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getByte(address + rowIndex);
        }
        return NullMemoryCMR.INSTANCE.getDecimal8(0);
    }

    @Override
    public double getDouble(int columnIndex) {
        if (hasTypeCasts) {
            int srcTag = sourceColumnTypes.getQuick(columnIndex);
            if (srcTag < -1) {
                return convertVarToDouble(-srcTag, columnIndex);
            }
        }
        final long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getDouble(address + (rowIndex << 3));
        }
        return NullMemoryCMR.INSTANCE.getDouble(0);
    }

    @Override
    public long getDate(int columnIndex) {
        if (hasTypeCasts) {
            int srcTag = sourceColumnTypes.getQuick(columnIndex);
            if (srcTag < -1) {
                return convertVarToDate(-srcTag, columnIndex);
            }
        }
        return getLong(columnIndex);
    }

    @Override
    public float getFloat(int columnIndex) {
        if (hasTypeCasts) {
            int srcTag = sourceColumnTypes.getQuick(columnIndex);
            if (srcTag < -1) {
                return convertVarToFloat(-srcTag, columnIndex);
            }
        }
        final long address = pageAddresses.get(columnOffset + columnIndex);
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
        final long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getByte(address + rowIndex);
        }
        return GeoHashes.BYTE_NULL;
    }

    @Override
    public int getGeoInt(int columnIndex) {
        final long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getInt(address + (rowIndex << 2));
        }
        return GeoHashes.INT_NULL;
    }

    @Override
    public long getGeoLong(int columnIndex) {
        final long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getLong(address + (rowIndex << 3));
        }
        return GeoHashes.NULL;
    }

    @Override
    public short getGeoShort(int columnIndex) {
        final long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getShort(address + (rowIndex << 1));
        }
        return GeoHashes.SHORT_NULL;
    }

    @Override
    public int getIPv4(int columnIndex) {
        if (hasTypeCasts) {
            int srcTag = sourceColumnTypes.getQuick(columnIndex);
            if (srcTag < -1) {
                return convertVarToIPv4(-srcTag, columnIndex);
            }
        }
        final long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getInt(address + (rowIndex << 2));
        }
        return NullMemoryCMR.INSTANCE.getIPv4(0);
    }

    @Override
    public int getInt(int columnIndex) {
        if (hasTypeCasts) {
            int srcTag = sourceColumnTypes.getQuick(columnIndex);
            if (srcTag < -1) {
                return convertVarToInt(-srcTag, columnIndex);
            }
        }
        final long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getInt(address + (rowIndex << 2));
        }
        return NullMemoryCMR.INSTANCE.getInt(0);
    }

    // 0 means A, 1 means B
    public byte getLetter() {
        return letter;
    }

    @Override
    public long getLong(int columnIndex) {
        if (hasTypeCasts) {
            int srcTag = sourceColumnTypes.getQuick(columnIndex);
            if (srcTag < -1) {
                return convertVarToLong(-srcTag, columnIndex);
            }
        }
        final long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getLong(address + (rowIndex << 3));
        }
        return NullMemoryCMR.INSTANCE.getLong(0);
    }

    @Override
    public long getLong128Hi(int columnIndex) {
        if (hasTypeCasts) {
            int srcTag = sourceColumnTypes.getQuick(columnIndex);
            if (srcTag < -1) {
                // UUID chained conversion is not supported; fall through to direct conversion.
                return convertVarToUuidHi(-srcTag, columnIndex);
            }
        }
        long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getLong(address + (rowIndex << 4) + Long.BYTES);
        }
        return NullMemoryCMR.INSTANCE.getLong128Hi();
    }

    @Override
    public long getLong128Lo(int columnIndex) {
        if (hasTypeCasts) {
            int srcTag = sourceColumnTypes.getQuick(columnIndex);
            if (srcTag < -1) {
                // UUID chained conversion is not supported; fall through to direct conversion.
                return convertVarToUuidLo(-srcTag, columnIndex);
            }
        }
        long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getLong(address + (rowIndex << 4));
        }
        return NullMemoryCMR.INSTANCE.getLong128Lo();
    }

    @Override
    public void getLong256(int columnIndex, CharSink<?> sink) {
        final long address = pageAddresses.get(columnOffset + columnIndex);
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

    @Override
    public short getShort(int columnIndex) {
        if (hasTypeCasts) {
            int srcTag = sourceColumnTypes.getQuick(columnIndex);
            if (srcTag < -1) {
                return convertVarToShort(-srcTag, columnIndex);
            }
        }
        final long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getShort(address + (rowIndex << 1));
        }
        return NullMemoryCMR.INSTANCE.getShort(0);
    }

    @Override
    public CharSequence getStrA(int columnIndex) {
        if (hasTypeCasts) {
            int srcTag = sourceColumnTypes.getQuick(columnIndex);
            if (srcTag >= 0) {
                return convertFixedToStr(srcTag, columnIndex, stringSinkA);
            }
            if (srcTag < -1) {
                return convertVarToStr(-srcTag, columnIndex, stringSinkA);
            }
        }
        return getStr0(columnIndex, csViewA(columnIndex));
    }

    @Override
    public CharSequence getStrB(int columnIndex) {
        if (hasTypeCasts) {
            int srcTag = sourceColumnTypes.getQuick(columnIndex);
            if (srcTag >= 0) {
                return convertFixedToStr(srcTag, columnIndex, stringSinkB);
            }
            if (srcTag < -1) {
                return convertVarToStr(-srcTag, columnIndex, stringSinkB);
            }
        }
        return getStr0(columnIndex, csViewB(columnIndex));
    }

    @Override
    public int getStrLen(int columnIndex) {
        if (hasTypeCasts) {
            int srcTag = sourceColumnTypes.getQuick(columnIndex);
            if (srcTag >= 0) {
                final CharSequence result = convertFixedToStr(srcTag, columnIndex, stringSinkA);
                return result != null ? result.length() : TableUtils.NULL_LEN;
            }
            if (srcTag < -1) {
                final CharSequence result = readVarValueForConversion(-srcTag, columnIndex);
                return result != null ? result.length() : TableUtils.NULL_LEN;
            }
        }
        final long dataPageAddress = pageAddresses.get(columnOffset + columnIndex);
        if (dataPageAddress != 0) {
            final long auxPageAddress = auxPageAddresses.get(columnOffset + columnIndex);
            final long auxPageLim = auxPageSizes.get(columnOffset + columnIndex);
            final long auxOffset = rowIndex << 3;
            if (auxPageLim < auxOffset + 8) {
                throw CairoException.critical(0)
                        .put("string is outside of file boundary [auxOffset=")
                        .put(auxOffset)
                        .put(", auxPageLim=")
                        .put(auxPageLim)
                        .put(']');
            }
            final long dataPageLim = pageSizes.get(columnOffset + columnIndex);
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
        final long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            int key = Unsafe.getUnsafe().getInt(address + (rowIndex << 2));
            return getSymbolTable(columnIndex).valueOf(key);
        }
        return null;
    }

    @Override
    public CharSequence getSymB(int columnIndex) {
        final long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            int key = Unsafe.getUnsafe().getInt(address + (rowIndex << 2));
            return getSymbolTable(columnIndex).valueBOf(key);
        }
        return null;
    }

    @Override
    public long getTimestamp(int columnIndex) {
        if (hasTypeCasts) {
            int srcTag = sourceColumnTypes.getQuick(columnIndex);
            if (srcTag < -1) {
                return convertVarToTimestamp(-srcTag, columnIndex);
            }
        }
        return getLong(columnIndex);
    }

    @Override
    public long getUpdateRowId() {
        return rowIdOffset + rowIndex;
    }

    @Override
    public Utf8Sequence getVarcharA(int columnIndex) {
        if (hasTypeCasts && sourceColumnTypes.getQuick(columnIndex) >= 0) {
            return convertFixedToVarchar(sourceColumnTypes.getQuick(columnIndex), columnIndex, varcharSinkA);
        }
        return getVarchar(columnIndex, utf8ViewA(columnIndex));
    }

    @Override
    public Utf8Sequence getVarcharB(int columnIndex) {
        if (hasTypeCasts && sourceColumnTypes.getQuick(columnIndex) >= 0) {
            return convertFixedToVarchar(sourceColumnTypes.getQuick(columnIndex), columnIndex, varcharSinkB);
        }
        return getVarchar(columnIndex, utf8ViewB(columnIndex));
    }

    @Override
    public int getVarcharSize(int columnIndex) {
        if (hasTypeCasts && sourceColumnTypes.getQuick(columnIndex) >= 0) {
            final Utf8Sequence result = convertFixedToVarchar(sourceColumnTypes.getQuick(columnIndex), columnIndex, varcharSinkA);
            return result != null ? result.size() : TableUtils.NULL_LEN;
        }
        final long auxPageAddress = auxPageAddresses.get(columnOffset + columnIndex);
        if (auxPageAddress != 0) {
            if (frameFormat == PartitionFormat.PARQUET) {
                return VarcharTypeDriver.getSliceValueSize(auxPageAddress, rowIndex);
            }
            return VarcharTypeDriver.getValueSize(auxPageAddress, rowIndex);
        }
        return TableUtils.NULL_LEN; // Column top.
    }

    // Note: this method doesn't break caching in PageFrameMemoryPool
    // as the method assumes that the record can't be used once
    // the frame memory is switched to another frame.
    public void init(PageFrameMemory frameMemory) {
        this.frameIndex = frameMemory.getFrameIndex();
        this.frameFormat = frameMemory.getFrameFormat();
        this.stableStrings = (frameFormat == PartitionFormat.NATIVE);
        this.hasTypeCasts = frameMemory.needsColumnTypeCast();
        this.rowIdOffset = frameMemory.getRowIdOffset();
        this.pageAddresses = frameMemory.getPageAddresses();
        this.auxPageAddresses = frameMemory.getAuxPageAddresses();
        this.pageSizes = frameMemory.getPageSizes();
        this.auxPageSizes = frameMemory.getAuxPageSizes();
        this.columnOffset = frameMemory.getColumnOffset();
        if (this.hasTypeCasts) {
            // Copy source column types from PageFrameMemory.
            final int n = frameMemory.getColumnCount();
            if (this.sourceColumnTypes == null) {
                this.sourceColumnTypes = new IntList(n);
            }
            this.sourceColumnTypes.setAll(n, -1);
            for (int col = 0; col < n; col++) {
                this.sourceColumnTypes.setQuick(col, frameMemory.getSourceColumnType(col));
            }
        }
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

    /**
     * Sets both the absolute row index and the compact row index for late-materialized
     * non-filter columns. In the base class, the compact index is ignored. Overridden
     * by {@link PageFrameFilteredMemoryRecord} to use the compact index for non-filter columns.
     */
    public void setRowIndex(long rowIndex, long compactRowIndex) {
        setRowIndex(rowIndex);
    }

    private @NotNull DirectString csViewA(int columnIndex) {
        DirectString view = csViewsA.getQuiet(columnIndex);
        if (view != null) {
            return view;
        }
        csViewsA.extendAndSet(columnIndex, view = new DirectString(this));
        return view;
    }

    private @NotNull DirectString csViewB(int columnIndex) {
        DirectString view = csViewsB.getQuiet(columnIndex);
        if (view != null) {
            return view;
        }
        csViewsB.extendAndSet(columnIndex, view = new DirectString(this));
        return view;
    }

    private void getLong256(int columnIndex, Long256Acceptor sink) {
        final long columnAddress = pageAddresses.get(columnOffset + columnIndex);
        if (columnAddress != 0) {
            sink.fromAddress(columnAddress + (rowIndex << 5));
            return;
        }
        NullMemoryCMR.INSTANCE.getLong256(0, sink);
    }

    private @NotNull Long256Impl long256A(int columnIndex) {
        Long256Impl long256 = longs256A.getQuiet(columnIndex);
        if (long256 != null) {
            return long256;
        }
        longs256A.extendAndSet(columnIndex, long256 = new Long256Impl());
        return long256;
    }

    private @NotNull Long256Impl long256B(int columnIndex) {
        Long256Impl long256 = longs256B.getQuiet(columnIndex);
        if (long256 != null) {
            return long256;
        }
        longs256B.extendAndSet(columnIndex, long256 = new Long256Impl());
        return long256;
    }

    private @NotNull Utf8SplitString utf8ViewA(int columnIndex) {
        Utf8SplitString view = utf8ViewsA.getQuiet(columnIndex);
        if (view != null) {
            return view;
        }
        utf8ViewsA.extendAndSet(columnIndex, view = new Utf8SplitString(this));
        return view;
    }

    private @NotNull Utf8SplitString utf8ViewB(int columnIndex) {
        Utf8SplitString view = utf8ViewsB.getQuiet(columnIndex);
        if (view != null) {
            return view;
        }
        utf8ViewsB.extendAndSet(columnIndex, view = new Utf8SplitString(this));
        return view;
    }

    private boolean convertVarToBool(int srcTag, int columnIndex) {
        CharSequence cs = readVarValueForConversion(srcTag, columnIndex);
        return cs != null && SqlKeywords.isTrueKeyword(cs);
    }

    private byte convertVarToByte(int srcTag, int columnIndex) {
        CharSequence cs = readVarValueForConversion(srcTag, columnIndex);
        if (cs != null) {
            try {
                return (byte) Numbers.parseInt(cs);
            } catch (NumericException ignore) {
            }
        }
        return 0;
    }

    private long convertVarToDate(int srcTag, int columnIndex) {
        CharSequence cs = readVarValueForConversion(srcTag, columnIndex);
        if (cs != null) {
            try {
                return DateFormatUtils.parseUTCDate(cs);
            } catch (NumericException ignore) {
            }
        }
        return Numbers.LONG_NULL;
    }

    private char convertVarToChar(int srcTag, int columnIndex) {
        CharSequence cs = readVarValueForConversion(srcTag, columnIndex);
        return cs != null && cs.length() > 0 ? cs.charAt(0) : (char) 0;
    }

    private void convertVarToDecimal128(int encoded, int columnIndex, Decimal128 sink) {
        int srcTag = encoded & 0xFF;
        int precision = (encoded >> 8) & 0xFF;
        int scale = (encoded >> 16) & 0xFF;
        CharSequence cs = readVarValueForConversion(srcTag, columnIndex);
        if (cs != null) {
            try {
                decimal128Buf.ofString(cs, precision, scale);
                sink.ofRaw(decimal128Buf.getHigh(), decimal128Buf.getLow());
                return;
            } catch (NumericException ignore) {
            }
        }
        sink.ofRawNull();
    }

    private short convertVarToDecimal16(int encoded, int columnIndex) {
        int srcTag = encoded & 0xFF;
        int precision = (encoded >> 8) & 0xFF;
        int scale = (encoded >> 16) & 0xFF;
        CharSequence cs = readVarValueForConversion(srcTag, columnIndex);
        if (cs != null) {
            try {
                decimal64Buf.ofString(cs, precision, scale);
                return (short) decimal64Buf.getValue();
            } catch (NumericException ignore) {
            }
        }
        return Decimals.DECIMAL16_NULL;
    }

    private void convertVarToDecimal256(int encoded, int columnIndex, Decimal256 sink) {
        int srcTag = encoded & 0xFF;
        int precision = (encoded >> 8) & 0xFF;
        int scale = (encoded >> 16) & 0xFF;
        CharSequence cs = readVarValueForConversion(srcTag, columnIndex);
        if (cs != null) {
            try {
                decimal256Buf.ofString(cs, precision, scale);
                sink.ofRaw(decimal256Buf.getHh(), decimal256Buf.getHl(), decimal256Buf.getLh(), decimal256Buf.getLl());
                return;
            } catch (NumericException ignore) {
            }
        }
        sink.ofRawNull();
    }

    private int convertVarToDecimal32(int encoded, int columnIndex) {
        int srcTag = encoded & 0xFF;
        int precision = (encoded >> 8) & 0xFF;
        int scale = (encoded >> 16) & 0xFF;
        CharSequence cs = readVarValueForConversion(srcTag, columnIndex);
        if (cs != null) {
            try {
                decimal64Buf.ofString(cs, precision, scale);
                return (int) decimal64Buf.getValue();
            } catch (NumericException ignore) {
            }
        }
        return Decimals.DECIMAL32_NULL;
    }

    private long convertVarToDecimal64(int encoded, int columnIndex) {
        int srcTag = encoded & 0xFF;
        int precision = (encoded >> 8) & 0xFF;
        int scale = (encoded >> 16) & 0xFF;
        CharSequence cs = readVarValueForConversion(srcTag, columnIndex);
        if (cs != null) {
            try {
                decimal64Buf.ofString(cs, precision, scale);
                return decimal64Buf.getValue();
            } catch (NumericException ignore) {
            }
        }
        return Decimals.DECIMAL64_NULL;
    }

    private byte convertVarToDecimal8(int encoded, int columnIndex) {
        int srcTag = encoded & 0xFF;
        int precision = (encoded >> 8) & 0xFF;
        int scale = (encoded >> 16) & 0xFF;
        CharSequence cs = readVarValueForConversion(srcTag, columnIndex);
        if (cs != null) {
            try {
                decimal64Buf.ofString(cs, precision, scale);
                return (byte) decimal64Buf.getValue();
            } catch (NumericException ignore) {
            }
        }
        return Decimals.DECIMAL8_NULL;
    }

    private double convertVarToDouble(int srcTag, int columnIndex) {
        CharSequence cs = readVarValueForConversion(srcTag, columnIndex);
        return Numbers.parseDoubleQuiet(cs);
    }

    private float convertVarToFloat(int srcTag, int columnIndex) {
        CharSequence cs = readVarValueForConversion(srcTag, columnIndex);
        if (cs != null) {
            try {
                return Numbers.parseFloat(cs);
            } catch (NumericException ignore) {
            }
        }
        return Float.NaN;
    }

    private int convertVarToIPv4(int srcTag, int columnIndex) {
        CharSequence cs = readVarValueForConversion(srcTag, columnIndex);
        if (cs != null) {
            return Numbers.parseIPv4Quiet(cs);
        }
        return Numbers.IPv4_NULL;
    }

    private int convertVarToInt(int srcTag, int columnIndex) {
        CharSequence cs = readVarValueForConversion(srcTag, columnIndex);
        if (cs != null) {
            return Numbers.parseIntQuiet(cs);
        }
        return Numbers.INT_NULL;
    }

    private long convertVarToLong(int srcTag, int columnIndex) {
        CharSequence cs = readVarValueForConversion(srcTag, columnIndex);
        if (cs != null) {
            return Numbers.parseLongQuiet(cs);
        }
        return Numbers.LONG_NULL;
    }

    private short convertVarToShort(int srcTag, int columnIndex) {
        CharSequence cs = readVarValueForConversion(srcTag, columnIndex);
        if (cs != null) {
            try {
                return (short) Numbers.parseInt(cs);
            } catch (NumericException ignore) {
            }
        }
        return 0;
    }

    private CharSequence convertVarToStr(int srcTag, int columnIndex, StringSink sink) {
        CharSequence cs = readVarValueForConversion(srcTag, columnIndex);
        if (cs == null) {
            return null;
        }
        sink.clear();
        sink.put(cs);
        return sink;
    }

    private long convertVarToTimestamp(int srcTag, int columnIndex) {
        CharSequence cs = readVarValueForConversion(srcTag, columnIndex);
        if (cs != null) {
            try {
                return MicrosFormatUtils.parseTimestamp(cs);
            } catch (NumericException ignore) {
            }
        }
        return Numbers.LONG_NULL;
    }

    private long convertVarToUuidHi(int srcTag, int columnIndex) {
        CharSequence cs = readVarValueForConversion(srcTag, columnIndex);
        if (cs != null) {
            try {
                return Uuid.parseHi(cs);
            } catch (NumericException ignore) {
            }
        }
        return Numbers.LONG_NULL;
    }

    private long convertVarToUuidLo(int srcTag, int columnIndex) {
        CharSequence cs = readVarValueForConversion(srcTag, columnIndex);
        if (cs != null) {
            try {
                return Uuid.parseLo(cs);
            } catch (NumericException ignore) {
            }
        }
        return Numbers.LONG_NULL;
    }

    private CharSequence readVarValueForConversion(int srcTag, int columnIndex) {
        if (srcTag == ColumnType.VARCHAR) {
            Utf8Sequence utf8 = getVarchar(columnIndex, utf8ViewA(columnIndex));
            return utf8 != null ? utf8.asAsciiCharSequence() : null;
        } else {
            return getStr0(columnIndex, csViewA(columnIndex));
        }
    }

    private boolean appendDecimalToSink(int srcType, long address, CharSink<?> sink) {
        final int tag = ColumnType.tagOf(srcType);
        final int precision = ColumnType.getDecimalPrecision(srcType);
        final int scale = ColumnType.getDecimalScale(srcType);
        switch (tag) {
            case ColumnType.DECIMAL8 -> {
                byte val = Unsafe.getUnsafe().getByte(address + rowIndex);
                if (val == Decimals.DECIMAL8_NULL) {
                    return false;
                }
                Decimals.appendNonNull(val, precision, scale, sink);
            }
            case ColumnType.DECIMAL16 -> {
                short val = Unsafe.getUnsafe().getShort(address + (rowIndex << 1));
                if (val == Decimals.DECIMAL16_NULL) {
                    return false;
                }
                Decimals.appendNonNull(val, precision, scale, sink);
            }
            case ColumnType.DECIMAL32 -> {
                int val = Unsafe.getUnsafe().getInt(address + (rowIndex << 2));
                if (val == Decimals.DECIMAL32_NULL) {
                    return false;
                }
                Decimals.appendNonNull(val, precision, scale, sink);
            }
            case ColumnType.DECIMAL64 -> {
                long val = Unsafe.getUnsafe().getLong(address + (rowIndex << 3));
                if (val == Decimals.DECIMAL64_NULL) {
                    return false;
                }
                Decimals.appendNonNull(val, precision, scale, sink);
            }
            case ColumnType.DECIMAL128 -> {
                long hi = Unsafe.getUnsafe().getLong(address + (rowIndex << 4));
                long lo = Unsafe.getUnsafe().getLong(address + (rowIndex << 4) + Long.BYTES);
                if (Decimal128.isNull(hi, lo)) {
                    return false;
                }
                Decimals.appendNonNull(hi, lo, precision, scale, sink);
            }
            case ColumnType.DECIMAL256 -> {
                long base = address + (rowIndex << 5);
                long hh = Unsafe.getUnsafe().getLong(base);
                long hl = Unsafe.getUnsafe().getLong(base + 8L);
                long lh = Unsafe.getUnsafe().getLong(base + 16L);
                long ll = Unsafe.getUnsafe().getLong(base + 24L);
                if (Decimal256.isNull(hh, hl, lh, ll)) {
                    return false;
                }
                Decimals.appendNonNull(hh, hl, lh, ll, precision, scale, sink);
            }
            default -> {
                return false;
            }
        }
        return true;
    }

    /**
     * Converts a fixed-type value to a STRING CharSequence.
     * Returns null for null values. Called only when the column needs a type cast.
     */
    private CharSequence convertFixedToStr(int srcType, int columnIndex, StringSink sink) {
        final long address = pageAddresses.get(columnOffset + columnIndex);
        if (address == 0) {
            return null; // column top
        }
        sink.clear();
        switch (srcType) {
            case ColumnType.BOOLEAN -> sink.put(Unsafe.getUnsafe().getByte(address + rowIndex) != 0);
            case ColumnType.BYTE -> sink.put(Unsafe.getUnsafe().getByte(address + rowIndex));
            case ColumnType.SHORT -> sink.put(Unsafe.getUnsafe().getShort(address + (rowIndex << 1)));
            case ColumnType.CHAR -> {
                char val = Unsafe.getUnsafe().getChar(address + (rowIndex << 1));
                if (val == 0) {
                    return null;
                }
                sink.put(val);
            }
            case ColumnType.INT -> {
                int val = Unsafe.getUnsafe().getInt(address + (rowIndex << 2));
                if (val == Numbers.INT_NULL) {
                    return null;
                }
                sink.put(val);
            }
            case ColumnType.LONG -> {
                long val = Unsafe.getUnsafe().getLong(address + (rowIndex << 3));
                if (val == Numbers.LONG_NULL) {
                    return null;
                }
                sink.put(val);
            }
            case ColumnType.FLOAT -> {
                float val = Unsafe.getUnsafe().getFloat(address + (rowIndex << 2));
                if (Numbers.isNull(val)) {
                    return null;
                }
                sink.put(val);
            }
            case ColumnType.DOUBLE -> {
                double val = Unsafe.getUnsafe().getDouble(address + (rowIndex << 3));
                if (Numbers.isNull(val)) {
                    return null;
                }
                sink.put(val);
            }
            case ColumnType.DATE -> {
                long val = Unsafe.getUnsafe().getLong(address + (rowIndex << 3));
                if (val == Numbers.LONG_NULL) {
                    return null;
                }
                DateFormatUtils.appendDateTime(sink, val);
            }
            case ColumnType.TIMESTAMP -> {
                long val = Unsafe.getUnsafe().getLong(address + (rowIndex << 3));
                if (val == Numbers.LONG_NULL) {
                    return null;
                }
                MicrosFormatUtils.appendDateTimeUSec(sink, val);
            }
            case ColumnType.TIMESTAMP_NANO -> {
                long val = Unsafe.getUnsafe().getLong(address + (rowIndex << 3));
                if (val == Numbers.LONG_NULL) {
                    return null;
                }
                NanosFormatUtils.appendDateTimeNSec(sink, val);
            }
            case ColumnType.IPv4 -> {
                int val = Unsafe.getUnsafe().getInt(address + (rowIndex << 2));
                if (val == Numbers.IPv4_NULL) {
                    return null;
                }
                Numbers.intToIPv4Sink(sink, val);
            }
            case ColumnType.UUID -> {
                long lo = Unsafe.getUnsafe().getLong(address + (rowIndex << 4));
                long hi = Unsafe.getUnsafe().getLong(address + (rowIndex << 4) + Long.BYTES);
                if (lo == Numbers.LONG_NULL && hi == Numbers.LONG_NULL) {
                    return null;
                }
                Numbers.appendUuid(lo, hi, sink);
            }
            default -> {
                if (ColumnType.isDecimal(srcType) && appendDecimalToSink(srcType, address, sink)) {
                    return sink;
                }
                return null;
            }
        }
        return sink;
    }

    /**
     * Converts a fixed-type value to a VARCHAR Utf8Sequence.
     * Returns null for null values. Called only when the column needs a type cast.
     */
    private Utf8Sequence convertFixedToVarchar(int srcType, int columnIndex, Utf8StringSink sink) {
        final long address = pageAddresses.get(columnOffset + columnIndex);
        if (address == 0) {
            return null; // column top
        }
        sink.clear();
        switch (srcType) {
            case ColumnType.BOOLEAN -> sink.put(Unsafe.getUnsafe().getByte(address + rowIndex) != 0);
            case ColumnType.BYTE -> sink.put((int) Unsafe.getUnsafe().getByte(address + rowIndex));
            case ColumnType.SHORT -> sink.put(Unsafe.getUnsafe().getShort(address + (rowIndex << 1)));
            case ColumnType.CHAR -> {
                char val = Unsafe.getUnsafe().getChar(address + (rowIndex << 1));
                if (val == 0) {
                    return null;
                }
                sink.put(val);
            }
            case ColumnType.INT -> {
                int val = Unsafe.getUnsafe().getInt(address + (rowIndex << 2));
                if (val == Numbers.INT_NULL) {
                    return null;
                }
                sink.put(val);
            }
            case ColumnType.LONG -> {
                long val = Unsafe.getUnsafe().getLong(address + (rowIndex << 3));
                if (val == Numbers.LONG_NULL) {
                    return null;
                }
                sink.put(val);
            }
            case ColumnType.FLOAT -> {
                float val = Unsafe.getUnsafe().getFloat(address + (rowIndex << 2));
                if (Numbers.isNull(val)) {
                    return null;
                }
                sink.put(val);
            }
            case ColumnType.DOUBLE -> {
                double val = Unsafe.getUnsafe().getDouble(address + (rowIndex << 3));
                if (Numbers.isNull(val)) {
                    return null;
                }
                sink.put(val);
            }
            case ColumnType.DATE -> {
                long val = Unsafe.getUnsafe().getLong(address + (rowIndex << 3));
                if (val == Numbers.LONG_NULL) {
                    return null;
                }
                DateFormatUtils.appendDateTime(sink, val);
            }
            case ColumnType.TIMESTAMP -> {
                long val = Unsafe.getUnsafe().getLong(address + (rowIndex << 3));
                if (val == Numbers.LONG_NULL) {
                    return null;
                }
                MicrosFormatUtils.appendDateTimeUSec(sink, val);
            }
            case ColumnType.TIMESTAMP_NANO -> {
                long val = Unsafe.getUnsafe().getLong(address + (rowIndex << 3));
                if (val == Numbers.LONG_NULL) {
                    return null;
                }
                NanosFormatUtils.appendDateTimeNSec(sink, val);
            }
            case ColumnType.IPv4 -> {
                int val = Unsafe.getUnsafe().getInt(address + (rowIndex << 2));
                if (val == Numbers.IPv4_NULL) {
                    return null;
                }
                Numbers.intToIPv4Sink(sink, val);
            }
            case ColumnType.UUID -> {
                long lo = Unsafe.getUnsafe().getLong(address + (rowIndex << 4));
                long hi = Unsafe.getUnsafe().getLong(address + (rowIndex << 4) + Long.BYTES);
                if (lo == Numbers.LONG_NULL && hi == Numbers.LONG_NULL) {
                    return null;
                }
                Numbers.appendUuid(lo, hi, sink);
            }
            default -> {
                if (ColumnType.isDecimal(srcType) && appendDecimalToSink(srcType, address, sink)) {
                    return sink;
                }
                return null;
            }
        }
        return sink;
    }

    protected @NotNull BorrowedArray borrowedArray(int columnIndex) {
        BorrowedArray array = arrayBuffers.getQuiet(columnIndex);
        if (array != null) {
            return array;
        }
        arrayBuffers.extendAndSet(columnIndex, array = new BorrowedArray());
        return array;
    }

    protected @NotNull DirectByteSequenceView bsView(int columnIndex) {
        DirectByteSequenceView view = bsViews.getQuiet(columnIndex);
        if (view != null) {
            return view;
        }
        bsViews.extendAndSet(columnIndex, view = new DirectByteSequenceView());
        return view;
    }

    protected double getArrayDouble1d2d0(int columnIndex, int columnType, int idx0, int idx1, long rowIdx) {
        final long auxAddr = auxPageAddresses.get(columnOffset + columnIndex);
        if (auxAddr == 0) {
            return Double.NaN;
        }
        final long auxEntryAddr = auxAddr + ArrayTypeDriver.getAuxVectorOffsetStatic(rowIdx);
        final int sizeBytes = Unsafe.getUnsafe().getInt(auxEntryAddr + Long.BYTES);
        if (sizeBytes == 0) {
            return Double.NaN;
        }
        final long dataOffset = Unsafe.getUnsafe().getLong(auxEntryAddr) & ArrayTypeDriver.OFFSET_MAX;
        final long dataAddr = pageAddresses.get(columnOffset + columnIndex);
        final long shapeAddr = dataAddr + dataOffset;
        final int flatIndex;
        if (ColumnType.decodeArrayDimensionality(columnType) == 1) {
            if (idx0 >= Unsafe.getUnsafe().getInt(shapeAddr)) {
                return Double.NaN;
            }
            flatIndex = idx0;
        } else {
            final int dimLen1 = Unsafe.getUnsafe().getInt(shapeAddr + Integer.BYTES);
            if (idx0 >= Unsafe.getUnsafe().getInt(shapeAddr) || idx1 >= dimLen1) {
                return Double.NaN;
            }
            flatIndex = idx0 * dimLen1 + idx1;
        }
        // 1D and 2D double arrays: values always start at dataOffset + Double.BYTES
        // (1D: 4 bytes shape + 4 bytes padding; 2D: 8 bytes shape + 0 padding)
        return Unsafe.getUnsafe().getDouble(dataAddr + dataOffset + Double.BYTES + (long) flatIndex * Double.BYTES);
    }

    protected BinarySequence getBin(long base, long offset, long dataLim, DirectByteSequenceView view) {
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

    protected void getLong256(long addr, CharSink<?> sink) {
        Numbers.appendLong256FromUnsafe(addr, sink);
    }

    protected DirectString getStr(long base, long offset, long dataLim, DirectString view) {
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

    protected CharSequence getStr0(int columnIndex, DirectString csView) {
        final long dataPageAddress = pageAddresses.get(columnOffset + columnIndex);
        if (dataPageAddress != 0) {
            final long auxPageAddress = auxPageAddresses.get(columnOffset + columnIndex);
            final long auxPageLim = auxPageSizes.get(columnOffset + columnIndex);
            final long auxOffset = rowIndex << 3;
            if (auxPageLim < auxOffset + 8) {
                throw CairoException.critical(0)
                        .put("string is outside of file boundary [auxOffset=")
                        .put(auxOffset)
                        .put(", auxPageLim=")
                        .put(auxPageLim)
                        .put(']');
            }
            final long dataPageLim = pageSizes.get(columnOffset + columnIndex);
            final long dataOffset = Unsafe.getUnsafe().getLong(auxPageAddress + auxOffset);
            return getStr(dataPageAddress, dataOffset, dataPageLim, csView);
        }
        return NullMemoryCMR.INSTANCE.getStrB(0);
    }

    protected SymbolTable getSymbolTable(int columnIndex) {
        SymbolTable symbolTable = symbolTableCache.getQuiet(columnIndex);
        if (symbolTable == null) {
            symbolTable = symbolTableSource.newSymbolTable(columnIndex);
            symbolTableCache.extendAndSet(columnIndex, symbolTable);
        }
        return symbolTable;
    }

    @Nullable
    protected Utf8Sequence getVarchar(int columnIndex, Utf8SplitString utf8View) {
        final long auxPageAddress = auxPageAddresses.get(columnOffset + columnIndex);
        if (auxPageAddress != 0) {
            if (frameFormat == PartitionFormat.PARQUET) {
                Utf8Sequence result = VarcharTypeDriver.getSliceValue(auxPageAddress, rowIndex, utf8View);
                if (result != null) {
                    long ptr = Unsafe.getUnsafe().getLong(auxPageAddress + 16L * rowIndex + 8);
                    if (ptr != 0 && ptr < 65536) {
                        int srcType = hasTypeCasts ? sourceColumnTypes.getQuick(columnIndex) : -1;
                        throw CairoException.critical(0)
                                .put("corrupt varchar slice in parquet frame")
                                .put(", col=").put(columnIndex)
                                .put(", row=").put(rowIndex)
                                .put(", frameIdx=").put(frameIndex)
                                .put(", ptr=").put(ptr)
                                .put(", auxAddr=").put(auxPageAddress)
                                .put(", hasTypeCasts=").put(hasTypeCasts)
                                .put(", srcType=").put(srcType)
                                .put(", dataAddr=").put(pageAddresses.get(columnOffset + columnIndex));
                    }
                }
                return result;
            }
            final long auxPageLim = auxPageAddress + auxPageSizes.get(columnOffset + columnIndex);
            final long dataPageAddress = pageAddresses.get(columnOffset + columnIndex);
            final long dataPageLim = dataPageAddress + pageSizes.get(columnOffset + columnIndex);
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

    void init(
            int frameIndex,
            byte frameFormat,
            long rowIdOffset,
            DirectLongList pageAddresses,
            DirectLongList auxPageAddresses,
            DirectLongList pageLimits,
            DirectLongList auxPageLimits,
            int columnOffset,
            boolean hasTypeCasts,
            IntList sourceColumnTypes
    ) {
        this.frameIndex = frameIndex;
        this.frameFormat = frameFormat;
        this.stableStrings = (frameFormat == PartitionFormat.NATIVE);
        this.hasTypeCasts = hasTypeCasts;
        // The pool passes its own IntList by reference and mutates it in place on every
        // openParquet() call. Snapshot the contents so a later navigateTo() on Record B
        // does not overwrite this record's view of the per-column conversion state.
        if (hasTypeCasts) {
            if (this.sourceColumnTypes == null) {
                this.sourceColumnTypes = new IntList(sourceColumnTypes.size());
            }
            final int n = sourceColumnTypes.size();
            this.sourceColumnTypes.setAll(n, -1);
            for (int c = 0; c < n; c++) {
                this.sourceColumnTypes.setQuick(c, sourceColumnTypes.getQuick(c));
            }
        }
        this.rowIdOffset = rowIdOffset;
        this.pageAddresses = pageAddresses;
        this.auxPageAddresses = auxPageAddresses;
        this.pageSizes = pageLimits;
        this.auxPageSizes = auxPageLimits;
        this.columnOffset = columnOffset;
    }
}
