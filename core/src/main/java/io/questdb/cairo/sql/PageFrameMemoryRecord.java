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
import io.questdb.cairo.ColumnTypeConverter;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.MillisTimestampDriver;
import io.questdb.cairo.NanosTimestampDriver;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.BorrowedArray;
import io.questdb.cairo.vm.NullMemoryCMR;
import io.questdb.cairo.vm.Vm;
import io.questdb.griffin.SqlKeywords;
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
import io.questdb.std.str.CharSink;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.StableStringSource;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8SplitString;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Must be initialized with a {@link PageFrameMemoryPool#navigateTo(int, PageFrameMemoryRecord)}
 * or {@link #init(PageFrameMemory)} call for a given page frame before any use.
 */
public class PageFrameMemoryRecord implements Record, StableStringSource, QuietCloseable, Mutable {
    public static final byte RECORD_A_LETTER = 0;
    public static final byte RECORD_B_LETTER = 1;
    // Per-column flyweight pools. Each list is sized 2 * columnCount: A views live at
    // [0, columnCount), B views at [columnCount, 2 * columnCount). Halving the list count
    // saves an ObjList header per record per pool. Per-column slot-per-column avoids
    // violating the Record flyweight contract that, e.g., getStrA(c1) followed by
    // getStrA(c2) must not invalidate the c1 return value.
    private final ObjList<BorrowedArray> arrayBuffers = new ObjList<>();
    private final ObjList<DirectByteSequenceView> bsViews = new ObjList<>();
    private final ObjList<DirectString> csViews = new ObjList<>();
    // Reusable decimal instances for lazy var->decimal conversion.
    private final Decimal128 decimal128Buf = new Decimal128();
    private final Decimal256 decimal256Buf = new Decimal256();
    private final Decimal64 decimal64Buf = new Decimal64();
    private final ObjList<Long256Impl> longs256 = new ObjList<>();
    private final ObjList<StringSink> stringSinks = new ObjList<>();
    private final ObjList<SymbolTable> symbolTableCache = new ObjList<>();
    // Dedicated sink for UTF-8 -> UTF-16 decoding when reading a parquet VARCHAR value
    // for a lazy var->fixed / var->decimal conversion. Kept separate from the per-column
    // string sinks because those are used as the return buffer of getStrA/getStrB and
    // must not be clobbered by the decode step inside readVarValueForConversion.
    private final StringSink utf16DecodeSink = new StringSink();
    private final ObjList<Utf8SplitString> utf8Views = new ObjList<>();
    private final ObjList<Utf8StringSink> varcharSinks = new ObjList<>();
    // Single-entry cache so the two UUID accessors (getLong128Hi/getLong128Lo) parse a
    // var->UUID value once per row instead of once each. Keyed by (frame, row, column): frameIndex
    // is part of the key so any frame switch -- via either init() overload -- self-invalidates the
    // cache without a per-init reset, since two frames reuse the same in-frame row indexes for
    // different data. clear() resets the key for cross-query record reuse. The fields are only read
    // and written on the UUID-cast accessor path, so a non-cast query never touches them.
    private int uuidCacheColumnIndex = -1;
    private int uuidCacheFrameIndex = -1;
    private long uuidCacheHi;
    private long uuidCacheLo;
    private long uuidCacheRowIndex = -1;
    protected DirectLongList auxPageAddresses;
    protected DirectLongList auxPageSizes;
    // Pool bind generation captured when boundPool was stamped. The pool bumps its
    // generation when it closes buffers that records may still alias (failed decode,
    // bulk release), so a stale generation forces a rebind instead of a freed read.
    protected long boundGeneration;
    // Pool that owns the parquet buffers this record currently points at, or null.
    // PageFrameMemoryPool.navigateTo() uses it to early-return only when the record
    // is still bound to that pool's live buffers for the requested frame.
    protected PageFrameMemoryPool boundPool;
    // Number of columns in the frame; B-side per-column flyweights live at index
    // columnCount + columnIndex, so the value must be set before any getStrB / getVarcharB
    // / getLong256B / getStrB-like helper is called.
    protected int columnCount;
    protected int columnOffset;
    // Per-column leading column-top count for the current parquet frame (reference into the
    // frame's buffers, like pageAddresses); null for native frames. A lazy fixed->var
    // conversion returns NULL for rows below this count, since the decoded source value of a
    // column-top row is an in-band 0 indistinguishable from a real 0.
    protected DirectLongList columnTops;
    protected byte frameFormat = -1;
    protected int frameIndex = -1;
    // True when any column in the current frame needs lazy fixed->var conversion.
    protected boolean hasTypeCasts;
    // Letters are used for parquet buffer reference counting in PageFrameMemoryPool.
    // RECORD_A_LETTER (0) stands for record A, RECORD_B_LETTER (1) stands for record B.
    protected byte letter;
    protected DirectLongList pageAddresses;
    protected DirectLongList pageSizes;
    protected long rowIdOffset;
    protected long rowIndex;
    // Per-column source type tag for fixed->var type-cast columns.
    // Set from PageFrameMemoryPool during init(); null when hasTypeCasts is false.
    protected IntList sourceColumnTypes;
    protected boolean stableStrings;
    protected SymbolTableSource symbolTableSource;
    // Per-column lazy fixed->str/varchar cache: one converter singleton (same for both
    // destinations), packed (precision<<16)|scale args for DECIMAL, and ColumnType.sizeOf
    // width. Null until first type-cast read; invalidated when sourceColumnTypes changes.
    protected IntList typeCastArgs;
    protected ObjList<ColumnTypeConverter.Fixed2VarConverter> typeCastConverters;
    protected IntList typeCastWidth;

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
        this.columnCount = other.columnCount;
        this.columnTops = other.columnTops;
        this.stableStrings = other.stableStrings;
        this.hasTypeCasts = other.hasTypeCasts;
        // Deep-copy the per-column conversion state. Sharing the reference would race
        // with init()'s in-place mutation when this record is later navigated to a
        // different frame. A null source list with hasTypeCasts == true would NPE in
        // any typecast-aware getter called before the first navigateTo() on Record B.
        if (other.sourceColumnTypes != null) {
            this.sourceColumnTypes = new IntList(other.sourceColumnTypes);
        }
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
        boundPool = null;
        boundGeneration = 0;
        columnTops = null;
        // Reset the type-cast gate and its backing data symmetrically: in steady state
        // the gate is what protects readers from a stale sourceColumnTypes, but clearing
        // both keeps the cleared-state invariant honest and avoids relying on every
        // future caller to re-init before the next read.
        hasTypeCasts = false;
        if (sourceColumnTypes != null) {
            sourceColumnTypes.clear();
        }
        invalidateTypeCastConverterCache();
        // Drop the var->UUID parse cache key so a recycled record cannot serve a prior query's
        // value: a fresh query may reuse the same frameIndex with different data. The frameIndex
        // in the cache key invalidates frame switches WITHIN a query on its own; this guards the
        // cross-query reuse where frameIndex repeats. rowIndex is 0 after clear(), so a -1 sentinel
        // here guarantees the next read misses.
        uuidCacheRowIndex = -1;
    }

    @Override
    public void close() {
        Misc.freeObjListIfCloseable(symbolTableCache);
        symbolTableCache.clear();
        Misc.freeObjList(arrayBuffers);
        clear();
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
    public int getArrayDimLen(int columnIndex, int columnType, int dim) {
        assert dim >= 1 && dim <= ColumnType.decodeArrayDimensionality(columnType);
        return getArrayDimLen0(columnIndex, columnType, dim, rowIndex);
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
            final long dataOffset = Unsafe.getLong(auxPageAddress + auxOffset);
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
            final long dataOffset = Unsafe.getLong(auxPageAddress + auxOffset);
            if (dataPageLim < dataOffset + 8) {
                throw CairoException.critical(0)
                        .put("binary is outside of file boundary [dataOffset=")
                        .put(dataOffset)
                        .put(", dataPageLim=")
                        .put(dataPageLim)
                        .put(']');
            }
            return Unsafe.getLong(dataPageAddress + dataOffset);
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
            return Unsafe.getByte(address + rowIndex) == 1;
        }
        return NullMemoryCMR.INSTANCE.getBool(0);
    }

    public long getBoundGeneration() {
        return boundGeneration;
    }

    public PageFrameMemoryPool getBoundPool() {
        return boundPool;
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
            return Unsafe.getByte(address + rowIndex);
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
            return Unsafe.getChar(address + (rowIndex << 1));
        }
        return NullMemoryCMR.INSTANCE.getChar(0);
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
                    Unsafe.getLong(address),
                    Unsafe.getLong(address + 8L)
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
            return Unsafe.getShort(address + (rowIndex << 1));
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
            return Unsafe.getInt(address + (rowIndex << 2));
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
            return Unsafe.getLong(address + (rowIndex << 3));
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
            return Unsafe.getByte(address + rowIndex);
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
            return Unsafe.getDouble(address + (rowIndex << 3));
        }
        return NullMemoryCMR.INSTANCE.getDouble(0);
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
            return Unsafe.getFloat(address + (rowIndex << 2));
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
            return Unsafe.getByte(address + rowIndex);
        }
        return GeoHashes.BYTE_NULL;
    }

    @Override
    public int getGeoInt(int columnIndex) {
        final long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getInt(address + (rowIndex << 2));
        }
        return GeoHashes.INT_NULL;
    }

    @Override
    public long getGeoLong(int columnIndex) {
        final long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getLong(address + (rowIndex << 3));
        }
        return GeoHashes.NULL;
    }

    @Override
    public short getGeoShort(int columnIndex) {
        final long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getShort(address + (rowIndex << 1));
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
            return Unsafe.getInt(address + (rowIndex << 2));
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
            return Unsafe.getInt(address + (rowIndex << 2));
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
            return Unsafe.getLong(address + (rowIndex << 3));
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
            return Unsafe.getLong(address + (rowIndex << 4) + Long.BYTES);
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
            return Unsafe.getLong(address + (rowIndex << 4));
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

    public long getPageAddress(int columnIndex) {
        // A column decoded in its pre-conversion source type (e.g. VARCHAR_SLICE for a
        // STRING->INT cast) has no directly-readable target-typed buffer. Returning 0 makes
        // raw-address fast paths (e.g. GroupByFunction.computeKeyedBatch) fall through to the
        // converting record accessors, as they already do for a column top.
        if (hasTypeCasts && sourceColumnTypes.getQuick(columnIndex) != -1) {
            return 0;
        }
        return pageAddresses.get(columnOffset + columnIndex);
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
            return Unsafe.getShort(address + (rowIndex << 1));
        }
        return NullMemoryCMR.INSTANCE.getShort(0);
    }

    @Override
    public CharSequence getStrA(int columnIndex) {
        if (hasTypeCasts) {
            int srcTag = sourceColumnTypes.getQuick(columnIndex);
            if (srcTag >= 0) {
                return convertFixedToStr(srcTag, columnIndex, stringSinkA(columnIndex));
            }
            if (srcTag < -1) {
                return convertVarToStr(-srcTag, columnIndex, stringSinkA(columnIndex));
            }
        }
        return getStr0(columnIndex, csViewA(columnIndex));
    }

    @Override
    public CharSequence getStrB(int columnIndex) {
        if (hasTypeCasts) {
            int srcTag = sourceColumnTypes.getQuick(columnIndex);
            if (srcTag >= 0) {
                return convertFixedToStr(srcTag, columnIndex, stringSinkB(columnIndex));
            }
            if (srcTag < -1) {
                return convertVarToStr(-srcTag, columnIndex, stringSinkB(columnIndex));
            }
        }
        return getStr0(columnIndex, csViewB(columnIndex));
    }

    @Override
    public int getStrLen(int columnIndex) {
        if (hasTypeCasts) {
            int srcTag = sourceColumnTypes.getQuick(columnIndex);
            if (srcTag >= 0) {
                final CharSequence result = convertFixedToStr(srcTag, columnIndex, stringSinkA(columnIndex));
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
            final long dataOffset = Unsafe.getLong(auxPageAddress + auxOffset);
            if (dataPageLim < dataOffset + 4) {
                throw CairoException.critical(0)
                        .put("string is outside of file boundary [dataOffset=")
                        .put(dataOffset)
                        .put(", dataPageLim=")
                        .put(dataPageLim)
                        .put(']');
            }
            return Unsafe.getInt(dataPageAddress + dataOffset);
        }
        return NullMemoryCMR.INSTANCE.getStrLen(0);
    }

    @Override
    public CharSequence getSymA(int columnIndex) {
        final long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            int key = Unsafe.getInt(address + (rowIndex << 2));
            return getSymbolTable(columnIndex).valueOf(key);
        }
        return null;
    }

    @Override
    public CharSequence getSymB(int columnIndex) {
        final long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            int key = Unsafe.getInt(address + (rowIndex << 2));
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
            return convertFixedToVarchar(sourceColumnTypes.getQuick(columnIndex), columnIndex, varcharSinkA(columnIndex));
        }
        return getVarchar(columnIndex, utf8ViewA(columnIndex));
    }

    @Override
    public Utf8Sequence getVarcharB(int columnIndex) {
        if (hasTypeCasts && sourceColumnTypes.getQuick(columnIndex) >= 0) {
            return convertFixedToVarchar(sourceColumnTypes.getQuick(columnIndex), columnIndex, varcharSinkB(columnIndex));
        }
        return getVarchar(columnIndex, utf8ViewB(columnIndex));
    }

    @Override
    public int getVarcharSize(int columnIndex) {
        if (hasTypeCasts && sourceColumnTypes.getQuick(columnIndex) >= 0) {
            final Utf8Sequence result = convertFixedToVarchar(sourceColumnTypes.getQuick(columnIndex), columnIndex, varcharSinkA(columnIndex));
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
        this.hasTypeCasts = frameMemory.hasColumnTypeCasts();
        this.rowIdOffset = frameMemory.getRowIdOffset();
        this.pageAddresses = frameMemory.getPageAddresses();
        this.auxPageAddresses = frameMemory.getAuxPageAddresses();
        this.pageSizes = frameMemory.getPageSizes();
        this.auxPageSizes = frameMemory.getAuxPageSizes();
        this.columnOffset = frameMemory.getColumnOffset();
        this.columnCount = frameMemory.getColumnCount();
        this.columnTops = frameMemory.getColumnTops();
        if (this.hasTypeCasts) {
            // Copy source column types from PageFrameMemory.
            if (this.sourceColumnTypes == null) {
                this.sourceColumnTypes = new IntList(columnCount);
            }
            this.sourceColumnTypes.setAll(columnCount, -1);
            for (int col = 0; col < columnCount; col++) {
                this.sourceColumnTypes.setQuick(col, frameMemory.getSourceColumnType(col));
            }
        }
        invalidateTypeCastConverterCache();
        // Stamp the owning pool so navigateTo() can early-return only while this
        // record still points at that pool's live buffers. A foreign pool (e.g. a
        // reduce task's) that later frees its buffers leaves boundPool != the
        // navigating pool, forcing a safe rebind.
        this.boundPool = frameMemory.getPool();
        this.boundGeneration = boundPool != null ? boundPool.getBindGeneration() : 0;
    }

    @Override
    public boolean isStable() {
        return stableStrings;
    }

    public void of(SymbolTableSource symbolTableSource) {
        close();
        this.symbolTableSource = symbolTableSource;
    }

    public void setBoundPool(PageFrameMemoryPool boundPool, long boundGeneration) {
        this.boundPool = boundPool;
        this.boundGeneration = boundGeneration;
    }

    /**
     * Positions the record at a row selected by a filter pass. The base class only needs the
     * absolute row index; the compacted index is ignored. {@link PageFrameFilteredMemoryRecord}
     * overrides this to keep the compacted index in sync so non-filter columns read from the
     * compacted buffer.
     * <p>
     * Callers that may receive a {@link PageFrameFilteredMemoryRecord} via polymorphic dispatch
     * must use this method; {@link #setRowIndex(long)} throws {@link UnsupportedOperationException}
     * on a filtered record.
     */
    public void setFilteredRowIndex(long rowIndex, long compactedRowIndex) {
        this.rowIndex = rowIndex;
    }

    public void setRowIndex(long rowIndex) {
        this.rowIndex = rowIndex;
    }

    /**
     * Resolves a row's array entry to the address of its data, which starts with the shape header:
     * one int per dimension. Returns 0 when the entry is NULL or the column is absent from this
     * frame (a column top), which the array accessors map to their own NULL.
     */
    private long arrayDataAddr(int columnIndex, long rowIdx) {
        final long auxAddr = auxPageAddresses.get(columnOffset + columnIndex);
        if (auxAddr == 0) {
            return 0;
        }
        final long auxEntryAddr = auxAddr + ArrayTypeDriver.getAuxVectorOffsetStatic(rowIdx);
        if (Unsafe.getInt(auxEntryAddr + Long.BYTES) == 0) {
            return 0;
        }
        return shapeAddr(columnIndex, auxEntryAddr);
    }

    private ColumnTypeConverter.Fixed2VarConverter cacheTypeCastConverter(int columnIndex, int srcType, int dstType) {
        // The destination only affects the unsupported diagnostic in getFixedToVarConverter,
        // so the resolved singleton is the same for STRING and VARCHAR targets and one
        // converter cache serves both. Caller guarantees this runs at most once per column
        // per frame (it gates on typeCastConverters being null or returning null at index).
        if (typeCastConverters == null) {
            typeCastConverters = new ObjList<>();
            typeCastArgs = new IntList();
            typeCastWidth = new IntList();
        }
        final ColumnTypeConverter.Fixed2VarConverter converter =
                ColumnTypeConverter.getFixedToVarConverter(srcType, dstType);
        typeCastConverters.extendAndSet(columnIndex, converter);
        final int args;
        if (ColumnType.isDecimal(srcType)) {
            args = (ColumnType.getDecimalPrecision(srcType) << 16) | ColumnType.getDecimalScale(srcType);
        } else {
            args = 0;
        }
        typeCastArgs.extendAndSet(columnIndex, args);
        typeCastWidth.extendAndSet(columnIndex, ColumnType.sizeOf(srcType));
        return converter;
    }

    /**
     * Converts a fixed-type value to a STRING CharSequence.
     * Returns null for null values. Called only when the column needs a type cast.
     * The first call per column per frame resolves and caches the
     * {@link ColumnTypeConverter.Fixed2VarConverter}, the packed (precision, scale)
     * args for decimal columns, and the row stride; subsequent rows pay only a flat
     * array load instead of repeating the type dispatch.
     */
    private CharSequence convertFixedToStr(int srcType, int columnIndex, StringSink sink) {
        final long address = pageAddresses.get(columnOffset + columnIndex);
        if (address == 0) {
            return null; // column top
        }
        // Only no-sentinel sources (BOOLEAN/BYTE/SHORT/CHAR) need the explicit column-top
        // count: their column-top rows decode to an in-band 0/false the converter cannot tell
        // from a real value. Sentinel sources store the column top as their null sentinel (and
        // may also have scattered nulls), so the converter already returns null for them.
        if (columnTops != null && ColumnType.isNoNullSentinelFixedType(srcType)
                && rowIndex < columnTops.get(columnOffset + columnIndex)) {
            return null;
        }
        sink.clear();
        ColumnTypeConverter.Fixed2VarConverter converter =
                typeCastConverters != null ? typeCastConverters.getQuiet(columnIndex) : null;
        if (converter == null) {
            converter = cacheTypeCastConverter(columnIndex, srcType, ColumnType.STRING);
        }
        final int args = typeCastArgs.getQuick(columnIndex);
        return converter.convert(
                address + rowIndex * typeCastWidth.getQuick(columnIndex),
                sink,
                args >>> 16,
                args & 0xFFFF
        ) ? sink : null;
    }

    /**
     * Converts a fixed-type value to a VARCHAR Utf8Sequence.
     * Returns null for null values. Called only when the column needs a type cast.
     * See {@link #convertFixedToStr} for the per-column cache shape.
     */
    private Utf8Sequence convertFixedToVarchar(int srcType, int columnIndex, Utf8StringSink sink) {
        final long address = pageAddresses.get(columnOffset + columnIndex);
        if (address == 0) {
            return null; // column top
        }
        // See convertFixedToStr: only no-sentinel sources need the explicit column-top count.
        if (columnTops != null && ColumnType.isNoNullSentinelFixedType(srcType)
                && rowIndex < columnTops.get(columnOffset + columnIndex)) {
            return null;
        }
        sink.clear();
        ColumnTypeConverter.Fixed2VarConverter converter =
                typeCastConverters != null ? typeCastConverters.getQuiet(columnIndex) : null;
        if (converter == null) {
            converter = cacheTypeCastConverter(columnIndex, srcType, ColumnType.VARCHAR);
        }
        final int args = typeCastArgs.getQuick(columnIndex);
        return converter.convert(
                address + rowIndex * typeCastWidth.getQuick(columnIndex),
                sink,
                args >>> 16,
                args & 0xFFFF
        ) ? sink : null;
    }

    private boolean convertVarToBool(int encoded, int columnIndex) {
        CharSequence cs = readVarValueForConversion(encoded & 0xFF, columnIndex);
        return cs != null && SqlKeywords.isTrueKeyword(cs);
    }

    private byte convertVarToByte(int encoded, int columnIndex) {
        CharSequence cs = readVarValueForConversion(encoded & 0xFF, columnIndex);
        if (cs != null) {
            try {
                return (byte) Numbers.parseInt(cs);
            } catch (NumericException ignore) {
            }
        }
        return 0;
    }

    private char convertVarToChar(int encoded, int columnIndex) {
        CharSequence cs = readVarValueForConversion(encoded & 0xFF, columnIndex);
        return cs != null && cs.length() > 0 ? cs.charAt(0) : (char) 0;
    }

    private long convertVarToDate(int encoded, int columnIndex) {
        CharSequence cs = readVarValueForConversion(encoded & 0xFF, columnIndex);
        if (cs != null) {
            try {
                // parseFloorLiteral matches ColumnTypeConverter's STRING->DATE path and
                // accepts higher-precision input (micros/nanos) by truncating extras.
                return MillisTimestampDriver.INSTANCE.parseFloorLiteral(cs);
            } catch (NumericException ignore) {
            }
        }
        return Numbers.LONG_NULL;
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
                long v = decimal64Buf.getValue();
                if (v >= Short.MIN_VALUE + 1 && v <= Short.MAX_VALUE) {
                    return (short) v;
                }
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
                long v = decimal64Buf.getValue();
                if (v >= Integer.MIN_VALUE + 1 && v <= Integer.MAX_VALUE) {
                    return (int) v;
                }
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
                long v = decimal64Buf.getValue();
                if (v >= Byte.MIN_VALUE + 1 && v <= Byte.MAX_VALUE) {
                    return (byte) v;
                }
            } catch (NumericException ignore) {
            }
        }
        return Decimals.DECIMAL8_NULL;
    }

    private double convertVarToDouble(int encoded, int columnIndex) {
        CharSequence cs = readVarValueForConversion(encoded & 0xFF, columnIndex);
        return Numbers.parseDoubleQuiet(cs);
    }

    private float convertVarToFloat(int encoded, int columnIndex) {
        CharSequence cs = readVarValueForConversion(encoded & 0xFF, columnIndex);
        if (cs != null) {
            try {
                return Numbers.parseFloat(cs);
            } catch (NumericException ignore) {
            }
        }
        return Float.NaN;
    }

    private int convertVarToIPv4(int encoded, int columnIndex) {
        CharSequence cs = readVarValueForConversion(encoded & 0xFF, columnIndex);
        if (cs != null) {
            return Numbers.parseIPv4Quiet(cs);
        }
        return Numbers.IPv4_NULL;
    }

    private int convertVarToInt(int encoded, int columnIndex) {
        CharSequence cs = readVarValueForConversion(encoded & 0xFF, columnIndex);
        if (cs != null) {
            return Numbers.parseIntQuiet(cs);
        }
        return Numbers.INT_NULL;
    }

    private long convertVarToLong(int encoded, int columnIndex) {
        CharSequence cs = readVarValueForConversion(encoded & 0xFF, columnIndex);
        if (cs != null) {
            return Numbers.parseLongQuiet(cs);
        }
        return Numbers.LONG_NULL;
    }

    private short convertVarToShort(int encoded, int columnIndex) {
        CharSequence cs = readVarValueForConversion(encoded & 0xFF, columnIndex);
        if (cs != null) {
            try {
                return (short) Numbers.parseInt(cs);
            } catch (NumericException ignore) {
            }
        }
        return 0;
    }

    private CharSequence convertVarToStr(int encoded, int columnIndex, StringSink sink) {
        int srcTag = encoded & 0xFF;
        if (srcTag == ColumnType.VARCHAR) {
            // Decode UTF-8 straight into the destination sink. Going through
            // readVarValueForConversion would route the decode via utf16DecodeSink
            // and then copy from it into sink, which both wastes a copy and creates
            // a latent aliasing risk if the two sinks ever become the same instance.
            Utf8Sequence utf8 = getVarchar(columnIndex, utf8ViewA(columnIndex));
            if (utf8 == null) {
                return null;
            }
            sink.clear();
            Utf8s.utf8ToUtf16(utf8, sink);
            return sink;
        }
        CharSequence cs = getStr0(columnIndex, csViewA(columnIndex));
        if (cs == null) {
            return null;
        }
        sink.clear();
        sink.put(cs);
        return sink;
    }

    private long convertVarToTimestamp(int encoded, int columnIndex) {
        // bits 0-7:  source tag (STRING or VARCHAR)
        // bit  24:   target timestamp precision (0 = micros, 1 = nanos)
        // Without the precision bit the driver would default to micros and
        // ALTER COLUMN c TYPE TIMESTAMP_NS would silently return values 1000x
        // smaller than expected. Mirrors the native ColumnTypeConverter, which
        // dispatches via ColumnType.getTimestampDriver(dstColumnType).
        int srcTag = encoded & 0xFF;
        boolean isNano = (encoded & (1 << 24)) != 0;
        CharSequence cs = readVarValueForConversion(srcTag, columnIndex);
        if (cs != null) {
            try {
                TimestampDriver driver = isNano ? NanosTimestampDriver.INSTANCE : MicrosTimestampDriver.INSTANCE;
                return driver.parseFloorLiteral(cs);
            } catch (NumericException ignore) {
            }
        }
        return Numbers.LONG_NULL;
    }

    private long convertVarToUuidHi(int encoded, int columnIndex) {
        parseVarToUuid(encoded, columnIndex);
        return uuidCacheHi;
    }

    private long convertVarToUuidLo(int encoded, int columnIndex) {
        parseVarToUuid(encoded, columnIndex);
        return uuidCacheLo;
    }

    private @NotNull DirectString csViewA(int columnIndex) {
        DirectString view = csViews.getQuiet(columnIndex);
        if (view != null) {
            return view;
        }
        csViews.extendAndSet(columnIndex, view = new DirectString(this));
        return view;
    }

    private @NotNull DirectString csViewB(int columnIndex) {
        final int slot = columnCount + columnIndex;
        DirectString view = csViews.getQuiet(slot);
        if (view != null) {
            return view;
        }
        csViews.extendAndSet(slot, view = new DirectString(this));
        return view;
    }

    private void invalidateTypeCastConverterCache() {
        // Source types can differ between frames (older partitions may predate an
        // ALTER and still carry the original fixed type), so the cache must drop
        // whenever the frame is repointed. The lists are null on records that have
        // never seen a type-cast column, which is the common case.
        if (typeCastConverters != null) {
            typeCastConverters.clear();
            typeCastArgs.clear();
            typeCastWidth.clear();
        }
    }

    private @NotNull Long256Impl long256A(int columnIndex) {
        Long256Impl long256 = longs256.getQuiet(columnIndex);
        if (long256 != null) {
            return long256;
        }
        longs256.extendAndSet(columnIndex, long256 = new Long256Impl());
        return long256;
    }

    private @NotNull Long256Impl long256B(int columnIndex) {
        final int slot = columnCount + columnIndex;
        Long256Impl long256 = longs256.getQuiet(slot);
        if (long256 != null) {
            return long256;
        }
        longs256.extendAndSet(slot, long256 = new Long256Impl());
        return long256;
    }

    /**
     * Parses the var->UUID value at the current row into {@link #uuidCacheHi}/{@link #uuidCacheLo},
     * so the paired getLong128Hi/getLong128Lo accessors share a single parse per row rather than
     * parsing the string twice. A second call for the same (row, column) is a no-op cache hit.
     * <p>
     * Both halves are parsed together and committed only if both succeed; if either half is
     * malformed both cache values become {@code LONG_NULL}. This mirrors the native
     * {@link ColumnTypeConverter} {@code str2Uuid}, which nulls both halves on any parse failure,
     * so a structurally-valid UUID with one corrupt half reads as NULL on both paths.
     */
    private void parseVarToUuid(int encoded, int columnIndex) {
        if (uuidCacheFrameIndex == frameIndex && uuidCacheRowIndex == rowIndex && uuidCacheColumnIndex == columnIndex) {
            return;
        }
        long hi = Numbers.LONG_NULL;
        long lo = Numbers.LONG_NULL;
        CharSequence cs = readVarValueForConversion(encoded & 0xFF, columnIndex);
        if (cs != null) {
            try {
                // checkDashesAndLength must run first: Uuid.parseHi indexes charAt up to
                // UUID_LENGTH unconditionally and would throw IndexOutOfBoundsException on a
                // short input (the NumericException-only catch would not handle it).
                Uuid.checkDashesAndLength(cs);
                long parsedHi = Uuid.parseHi(cs);
                lo = Uuid.parseLo(cs);
                // Commit hi only after parseLo succeeds, so a corrupt lo leaves both at LONG_NULL.
                hi = parsedHi;
            } catch (NumericException ignore) {
            }
        }
        uuidCacheHi = hi;
        uuidCacheLo = lo;
        uuidCacheFrameIndex = frameIndex;
        uuidCacheRowIndex = rowIndex;
        uuidCacheColumnIndex = columnIndex;
    }

    /**
     * Decodes the var value at {@code columnIndex} of source type {@code srcTag} as a
     * CharSequence that the convertVarToXxx callers feed into Numbers/Uuid/timestamp
     * parsers.
     * <p>
     * The returned CharSequence may be backed by the single shared {@link #utf16DecodeSink}
     * (on the non-ASCII VARCHAR path) or by a per-column DirectString view (STRING path).
     * In both cases the caller must finish consuming the result before invoking
     * readVarValueForConversion again on the same record -- a subsequent call writes into
     * the same sink, which would invalidate any reference still held by the caller.
     * All current convertVarToXxx callers consume the result in a single parse call before
     * returning, so the invariant holds today.
     */
    private CharSequence readVarValueForConversion(int srcTag, int columnIndex) {
        if (srcTag == ColumnType.VARCHAR) {
            Utf8Sequence utf8 = getVarchar(columnIndex, utf8ViewA(columnIndex));
            // utf8ToUtf16OrView gives a zero-alloc view on the ASCII fast path and
            // falls back to decoding into utf16DecodeSink for non-ASCII values.
            // Downstream callers (convertVarToChar, convertVarToStr, Numbers.parseXxx,
            // Uuid.parseHi/Lo, timestamp parsers) rely on real UTF-16 code points to
            // behave the same as the native conversion path.
            return utf8 != null ? Utf8s.utf8ToUtf16OrView(utf8, utf16DecodeSink) : null;
        } else {
            return getStr0(columnIndex, csViewA(columnIndex));
        }
    }

    /**
     * Resolves the address of an array's shape header, which the data entry opens with. The caller
     * must have established that the entry is not a NULL array.
     */
    private long shapeAddr(int columnIndex, long auxEntryAddr) {
        final long dataOffset = Unsafe.getLong(auxEntryAddr) & ArrayTypeDriver.OFFSET_MAX;
        return pageAddresses.get(columnOffset + columnIndex) + dataOffset;
    }

    private @NotNull StringSink stringSinkA(int columnIndex) {
        StringSink sink = stringSinks.getQuiet(columnIndex);
        if (sink != null) {
            return sink;
        }
        stringSinks.extendAndSet(columnIndex, sink = new StringSink());
        return sink;
    }

    private @NotNull StringSink stringSinkB(int columnIndex) {
        final int slot = columnCount + columnIndex;
        StringSink sink = stringSinks.getQuiet(slot);
        if (sink != null) {
            return sink;
        }
        stringSinks.extendAndSet(slot, sink = new StringSink());
        return sink;
    }

    private @NotNull Utf8SplitString utf8ViewA(int columnIndex) {
        Utf8SplitString view = utf8Views.getQuiet(columnIndex);
        if (view != null) {
            return view;
        }
        utf8Views.extendAndSet(columnIndex, view = new Utf8SplitString(this));
        return view;
    }

    private @NotNull Utf8SplitString utf8ViewB(int columnIndex) {
        final int slot = columnCount + columnIndex;
        Utf8SplitString view = utf8Views.getQuiet(slot);
        if (view != null) {
            return view;
        }
        utf8Views.extendAndSet(slot, view = new Utf8SplitString(this));
        return view;
    }

    private @NotNull Utf8StringSink varcharSinkA(int columnIndex) {
        Utf8StringSink sink = varcharSinks.getQuiet(columnIndex);
        if (sink != null) {
            return sink;
        }
        varcharSinks.extendAndSet(columnIndex, sink = new Utf8StringSink());
        return sink;
    }

    private @NotNull Utf8StringSink varcharSinkB(int columnIndex) {
        final int slot = columnCount + columnIndex;
        Utf8StringSink sink = varcharSinks.getQuiet(slot);
        if (sink != null) {
            return sink;
        }
        varcharSinks.extendAndSet(slot, sink = new Utf8StringSink());
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

    /**
     * Reads one int out of the array's shape header. Resolves the column the same way
     * {@link #getArray} does, so a column top or a NULL entry reads as NULL rather than as
     * a length.
     */
    protected int getArrayDimLen0(int columnIndex, int columnType, int dim, long rowIdx) {
        final long auxAddr = auxPageAddresses.get(columnOffset + columnIndex);
        if (auxAddr == 0) {
            return Numbers.INT_NULL;
        }
        final long auxEntryAddr = auxAddr + ArrayTypeDriver.getAuxVectorOffsetStatic(rowIdx);
        // Read the size half only, the way arrayDataAddr() and the native readers do: the aux entry
        // reserves the 32 bits above it, so widening the load would fold whatever lands there into
        // the size.
        final long dataSize = Unsafe.getInt(auxEntryAddr + Long.BYTES) & 0xffff_ffffL;
        if (dataSize == 0) {
            return Numbers.INT_NULL;
        }
        if (ColumnType.decodeArrayDimensionality(columnType) == 1
                && ColumnType.decodeArrayElementType(columnType) == ColumnType.DOUBLE) {
            // The aux entry already carries the answer for a 1D double array: ArrayTypeDriver lays
            // the data entry out as a 4-byte shape, 4 bytes of padding up to the 8-byte element
            // alignment, then the values, so its size is exactly Double.BYTES * (length + 1).
            // Reading the shape instead would touch the data vector, which is a separate cache line
            // per row once an array outgrows one, where the aux vector packs four rows into each.
            // ArrayTypeDriver owns the layout; the assert re-reads the shape so that every array
            // test fails here if it ever moves.
            final int dimLen = (int) ((dataSize >>> 3) - 1);
            assert dimLen == Unsafe.getInt(shapeAddr(columnIndex, auxEntryAddr))
                    : "1D double array length disagrees with its shape header";
            return dimLen;
        }
        return Unsafe.getInt(shapeAddr(columnIndex, auxEntryAddr) + (long) (dim - 1) * Integer.BYTES);
    }

    protected double getArrayDouble1d2d0(int columnIndex, int columnType, int idx0, int idx1, long rowIdx) {
        final long shapeAddr = arrayDataAddr(columnIndex, rowIdx);
        if (shapeAddr == 0) {
            return Double.NaN;
        }
        final int flatIndex;
        if (ColumnType.decodeArrayDimensionality(columnType) == 1) {
            if (idx0 >= Unsafe.getInt(shapeAddr)) {
                return Double.NaN;
            }
            flatIndex = idx0;
        } else {
            final int dimLen1 = Unsafe.getInt(shapeAddr + Integer.BYTES);
            if (idx0 >= Unsafe.getInt(shapeAddr) || idx1 >= dimLen1) {
                return Double.NaN;
            }
            flatIndex = idx0 * dimLen1 + idx1;
        }
        // 1D and 2D double arrays: values always start one Double.BYTES past the shape header
        // (1D: 4 bytes shape + 4 bytes padding; 2D: 8 bytes shape + 0 padding)
        return Unsafe.getDouble(shapeAddr + Double.BYTES + (long) flatIndex * Double.BYTES);
    }

    protected BinarySequence getBin(long base, long offset, long dataLim, DirectByteSequenceView view) {
        final long address = base + offset;
        final long len = Unsafe.getLong(address);
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

    // Subclasses may override (e.g. PageFrameFilteredMemoryRecord routes the index through
    // getRowIndex(columnIndex) so a late-materialized LONG256 reads at the compacted index).
    protected void getLong256(int columnIndex, Long256Acceptor sink) {
        final long columnAddress = pageAddresses.get(columnOffset + columnIndex);
        if (columnAddress != 0) {
            sink.fromAddress(columnAddress + (rowIndex << 5));
            return;
        }
        NullMemoryCMR.INSTANCE.getLong256(0, sink);
    }

    protected void getLong256(long addr, CharSink<?> sink) {
        Numbers.appendLong256FromUnsafe(addr, sink);
    }

    protected DirectString getStr(long base, long offset, long dataLim, DirectString view) {
        final long address = base + offset;
        final int len = Unsafe.getInt(address);
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
            final long dataOffset = Unsafe.getLong(auxPageAddress + auxOffset);
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
                return VarcharTypeDriver.getSliceValue(auxPageAddress, rowIndex, utf8View);
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
            int columnCount,
            boolean hasTypeCasts,
            IntList sourceColumnTypes,
            DirectLongList columnTops
    ) {
        this.frameIndex = frameIndex;
        this.frameFormat = frameFormat;
        this.stableStrings = (frameFormat == PartitionFormat.NATIVE);
        this.hasTypeCasts = hasTypeCasts;
        this.columnTops = columnTops;
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
        this.columnCount = columnCount;
        invalidateTypeCastConverterCache();
    }
}
