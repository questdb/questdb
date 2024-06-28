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

import io.questdb.cairo.*;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.cairo.vm.NullMemoryMR;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.cairo.vm.api.NullMemory;
import io.questdb.std.*;
import io.questdb.std.str.*;
import org.jetbrains.annotations.Nullable;

/**
 * Provides addresses for page frames in both native and Parquet formats.
 * Memory in native page frames is mmapped, so no additional actions are
 * necessary. Parquet frames need to be explicitly deserialized into
 * the in-memory native format before being accessed directly or via a Record.
 * Thus, a {@link #navigateTo(int)} call is required before accessing memory
 * that belongs to a page frame.
 * <p>
 * This cache is thread-unsafe when it comes to Parquet partitions, so it
 * shouldn't be shared between multiple threads.
 */
public class PageFrameMemoryCache implements Mutable, QuietCloseable {

    // Used for deserialized Parquet frame.
    // TODO: add LRU cache for multiple frames
    private final ObjList<MemoryCARW> columnChunks = new ObjList<>();
    // Used for deserialized Parquet columns.
    private final ObjectPool<MemoryCARWImpl> columnChunksPool = new ObjectPool<>(
            () -> (MemoryCARWImpl) Vm.getCARWInstance(128 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_OFFLOAD),
            128
    );
    private final IntList columnTypes = new IntList();
    private final LongList frameAuxPageAddresses = new LongList(); // Holds addresses for non-native frames.
    private final ByteList frameFormats = new ByteList();
    private final LongList framePageAddresses = new LongList(); // Holds addresses for non-native frames.
    private final LongList framePageSizes = new LongList(); // Holds sizes for non-native frames.
    private final LongList frameSizes = new LongList();
    private final long nativeCacheSizeThreshold;
    private final NativeFrameRecord nativeFrameRecord;
    private ObjList<LongList> auxPageAddresses = new ObjList<>();
    private int columnCount;
    private byte frameFormat;
    private int frameIndex;
    private ObjList<LongList> pageAddresses = new ObjList<>();
    private ObjList<LongList> pageSizes = new ObjList<>();
    // Makes it possible to determine real row id, not the one relative to the page.
    private LongList rowIdOffsets = new LongList();

    public PageFrameMemoryCache(CairoConfiguration configuration) {
        this.nativeCacheSizeThreshold = configuration.getSqlJitPageAddressCacheThreshold() / Long.BYTES;
        this.nativeFrameRecord = new NativeFrameRecord(this);
    }

    public PageFrameMemoryCache(PageFrameMemoryCache other) {
        this.nativeCacheSizeThreshold = other.nativeCacheSizeThreshold;
        this.frameSizes.add(other.frameSizes);
        this.frameFormats.addAll(other.frameFormats);
        this.columnTypes.addAll(other.columnTypes);
        this.columnCount = other.columnCount;
        // deep auxPageAddresses copy
        for (int i = 0, n = other.auxPageAddresses.size(); i < n; i++) {
            final LongList otherAuxPageAddresses = other.auxPageAddresses.getQuick(i);
            if (otherAuxPageAddresses != null) {
                final LongList thisAuxPageAddresses = new LongList(otherAuxPageAddresses.size());
                thisAuxPageAddresses.add(otherAuxPageAddresses);
                this.auxPageAddresses.add(thisAuxPageAddresses);
            } else {
                this.auxPageAddresses.add(null);
            }
        }
        // deep pageAddresses copy
        for (int i = 0, n = other.pageAddresses.size(); i < n; i++) {
            final LongList otherPageAddresses = other.pageAddresses.getQuick(i);
            if (otherPageAddresses != null) {
                final LongList thisPageAddresses = new LongList(otherPageAddresses.size());
                thisPageAddresses.add(otherPageAddresses);
                this.pageAddresses.add(thisPageAddresses);
            } else {
                this.pageAddresses.add(null);
            }
        }
        // deep pageSizes copy
        for (int i = 0, n = other.pageSizes.size(); i < n; i++) {
            final LongList otherPageSizes = other.pageSizes.getQuick(i);
            if (otherPageSizes != null) {
                final LongList thisPageSizes = new LongList(otherPageSizes.size());
                thisPageSizes.add(otherPageSizes);
                this.pageSizes.add(thisPageSizes);
            } else {
                this.pageSizes.add(null);
            }
        }
        this.rowIdOffsets.add(other.rowIdOffsets);
        this.nativeFrameRecord = new NativeFrameRecord(this);
    }

    public void add(int frameIndex, @Transient PageFrame frame) {
        if (pageAddresses.size() >= columnCount * (frameIndex + 1)) {
            return; // The page frame is already cached
        }

        if (frame.getFormat() == PageFrame.NATIVE_FORMAT) {
            final LongList framePageAddresses = new LongList(columnCount);
            final LongList frameAuxPageAddresses = new LongList(columnCount);
            final LongList framePageSizes = new LongList(columnCount);
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                framePageAddresses.add(frame.getPageAddress(columnIndex));
                final boolean isVarSize = ColumnType.isVarSize(columnTypes.getQuick(columnIndex));
                frameAuxPageAddresses.add(isVarSize ? frame.getIndexPageAddress(columnIndex) : 0);
                framePageSizes.add(isVarSize ? frame.getPageSize(columnIndex) : 0);
            }
            pageAddresses.add(framePageAddresses);
            auxPageAddresses.add(frameAuxPageAddresses);
            pageSizes.add(framePageSizes);
        } else {
            pageAddresses.add(null);
            auxPageAddresses.add(null);
            pageSizes.add(null);
        }

        frameSizes.add(frame.getPartitionHi() - frame.getPartitionLo());
        frameFormats.add(frame.getFormat());
        rowIdOffsets.add(Rows.toRowID(frame.getPartitionIndex(), frame.getPartitionLo()));
    }

    @Override
    public void clear() {
        frameIndex = -1;
        frameFormat = -1;
        frameSizes.clear();
        frameFormats.clear();
        columnTypes.clear();
        framePageAddresses.clear();
        frameAuxPageAddresses.clear();
        framePageSizes.clear();
        // TODO: threshold logic no longer makes sense
        if (pageAddresses.size() < nativeCacheSizeThreshold) {
            pageAddresses.clear();
            auxPageAddresses.clear();
            pageSizes.clear();
            rowIdOffsets.clear();
        } else {
            pageAddresses = new ObjList<>();
            auxPageAddresses = new ObjList<>();
            pageSizes = new ObjList<>();
            rowIdOffsets = new LongList();
        }
    }

    @Override
    public void close() {
        columnChunks.clear();
        columnChunksPool.closeAndClear();
    }

    public LongList getAuxPageAddresses() {
        if (frameFormat == PageFrame.NATIVE_FORMAT) {
            return auxPageAddresses.getQuick(frameIndex);
        }
        return frameAuxPageAddresses;
    }

    public int getColumnCount() {
        return columnCount;
    }

    public int getFrameIndex() {
        return frameIndex;
    }

    public LongList getPageAddresses() {
        if (frameFormat == PageFrame.NATIVE_FORMAT) {
            return pageAddresses.getQuick(frameIndex);
        }
        return framePageAddresses;
    }

    public LongList getPageSizes() {
        if (frameFormat == PageFrame.NATIVE_FORMAT) {
            return pageSizes.getQuick(frameIndex);
        }
        return framePageSizes;
    }

    public long getRowIdOffset() {
        return rowIdOffsets.getQuick(frameIndex);
    }

    public boolean hasColumnTops() {
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            if (pageAddresses.getQuick(frameIndex).getQuick(columnIndex) == 0
                    // VARCHAR column that contains short strings will have zero data vector,
                    // so for such columns we also need to check that the aux (index) vector is zero.
                    && auxPageAddresses.getQuick(frameIndex).getQuick(columnIndex) == 0) {
                return true;
            }
        }
        return false;
    }

    public boolean isVarSizeColumn(int columnIndex) {
        return ColumnType.isVarSize(columnTypes.getQuick(columnIndex));
    }

    public void navigateTo(int frameIndex) {
        assert pageAddresses.size() >= columnCount * (frameIndex + 1);
        assert pageSizes.size() >= columnCount * (frameIndex + 1);
        assert auxPageAddresses.size() >= columnCount * (frameIndex + 1);

        this.frameIndex = frameIndex;
        this.frameFormat = frameFormats.getQuick(frameIndex);
        if (frameFormat == PageFrame.PARQUET_FORMAT) {
            columnChunks.clear();
            columnChunksPool.clear();
            // TODO: handle missing columns/column tops/etc.
            copyToColumnChunks(0, frameSizes.getQuick(frameIndex));

            // Copy the addresses to frame-local lists.
            framePageAddresses.clear();
            frameAuxPageAddresses.clear();
            framePageSizes.clear();
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                framePageAddresses.add(columnChunks.getQuick(2 * columnIndex).addressOf(0));
                frameAuxPageAddresses.add(columnChunks.getQuick(2 * columnIndex + 1).addressOf(0));
                framePageSizes.add(columnChunks.getQuick(2 * columnIndex).size());
            }
        }
    }

    public MemoryCARW nextColumnChunk() {
        return columnChunksPool.next();
    }

    public void of(@Transient RecordMetadata metadata) {
        this.frameIndex = -1;
        this.frameFormat = -1;
        this.columnCount = metadata.getColumnCount();
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            columnTypes.add(metadata.getColumnType(columnIndex));
        }
    }

    /**
     * TODO: remove when proper Parquet deserialization is in use
     * Copies column values for filtered rows into column memory chunks.
     */
    private void copyToColumnChunks(long rowIdLo, long rowIdHi) {
        for (int i = 0; i < columnTypes.size(); i++) {
            final int columnType = columnTypes.getQuick(i);

            final MemoryCARW dataMem = nextColumnChunk();
            columnChunks.add(dataMem);
            final MemoryCARW auxMem = ColumnType.isVarSize(columnType) ? nextColumnChunk() : NullMemory.INSTANCE;
            columnChunks.add(auxMem);

            switch (ColumnType.tagOf(columnType)) {
                case ColumnType.BOOLEAN:
                    dataMem.jumpTo(0);
                    for (long r = rowIdLo; r < rowIdHi; r++) {
                        nativeFrameRecord.setRowIndex(r);
                        dataMem.putBool(nativeFrameRecord.getBool(i));
                    }
                    break;
                case ColumnType.BYTE:
                case ColumnType.GEOBYTE:
                    dataMem.jumpTo(0);
                    for (long r = rowIdLo; r < rowIdHi; r++) {
                        nativeFrameRecord.setRowIndex(r);
                        dataMem.putByte(nativeFrameRecord.getByte(i));
                    }
                    break;
                case ColumnType.SHORT:
                case ColumnType.GEOSHORT:
                case ColumnType.CHAR: // for memory copying purposes chars are same as shorts
                    dataMem.jumpTo(0);
                    for (long r = rowIdLo; r < rowIdHi; r++) {
                        nativeFrameRecord.setRowIndex(r);
                        dataMem.putShort(nativeFrameRecord.getShort(i));
                    }
                    break;
                case ColumnType.INT:
                case ColumnType.GEOINT:
                case ColumnType.IPv4:
                case ColumnType.SYMBOL:
                case ColumnType.FLOAT: // for memory copying purposes floats are same as ints
                    dataMem.jumpTo(0);
                    for (long r = rowIdLo; r < rowIdHi; r++) {
                        nativeFrameRecord.setRowIndex(r);
                        dataMem.putInt(nativeFrameRecord.getInt(i));
                    }
                    break;
                case ColumnType.LONG:
                case ColumnType.GEOLONG:
                case ColumnType.DATE:
                case ColumnType.TIMESTAMP:
                case ColumnType.DOUBLE: // for memory copying purposes doubles are same as longs
                    dataMem.jumpTo(0);
                    for (long r = rowIdLo; r < rowIdHi; r++) {
                        nativeFrameRecord.setRowIndex(r);
                        dataMem.putLong(nativeFrameRecord.getLong(i));
                    }
                    break;
                case ColumnType.UUID:
                    dataMem.jumpTo(0);
                    for (long r = rowIdLo; r < rowIdHi; r++) {
                        nativeFrameRecord.setRowIndex(r);
                        dataMem.putLong128(nativeFrameRecord.getLong128Lo(i), nativeFrameRecord.getLong128Hi(i));
                    }
                    break;
                case ColumnType.LONG256:
                    dataMem.jumpTo(0);
                    for (long r = rowIdLo; r < rowIdHi; r++) {
                        nativeFrameRecord.setRowIndex(r);
                        Long256 l256 = nativeFrameRecord.getLong256A(i);
                        dataMem.putLong256(l256);
                    }
                    break;
                case ColumnType.STRING:
                    assert auxMem != null;
                    auxMem.jumpTo(0);
                    dataMem.jumpTo(0);
                    StringTypeDriver.INSTANCE.configureAuxMemO3RSS(auxMem);
                    for (long r = rowIdLo; r < rowIdHi; r++) {
                        nativeFrameRecord.setRowIndex(r);
                        CharSequence cs = nativeFrameRecord.getStrA(i);
                        StringTypeDriver.appendValue(auxMem, dataMem, cs);
                    }
                    break;
                case ColumnType.VARCHAR:
                    assert auxMem != null;
                    auxMem.jumpTo(0);
                    dataMem.jumpTo(0);
                    for (long r = rowIdLo; r < rowIdHi; r++) {
                        nativeFrameRecord.setRowIndex(r);
                        Utf8Sequence us = nativeFrameRecord.getVarcharA(i);
                        VarcharTypeDriver.appendValue(auxMem, dataMem, us);
                    }
                    break;
                case ColumnType.BINARY:
                    assert auxMem != null;
                    auxMem.jumpTo(0);
                    dataMem.jumpTo(0);
                    BinaryTypeDriver.INSTANCE.configureAuxMemO3RSS(auxMem);
                    for (long r = rowIdLo; r < rowIdHi; r++) {
                        nativeFrameRecord.setRowIndex(r);
                        BinarySequence bs = nativeFrameRecord.getBin(i);
                        BinaryTypeDriver.appendValue(auxMem, dataMem, bs);
                    }
                    break;
            }
        }
    }

    private long getAuxPageAddress(int columnIndex) {
        if (frameFormat == PageFrame.NATIVE_FORMAT) {
            return auxPageAddresses.getQuick(frameIndex).getQuick(columnIndex);
        }
        return frameAuxPageAddresses.getQuick(columnIndex);
    }

    private long getPageAddress(int columnIndex) {
        if (frameFormat == PageFrame.NATIVE_FORMAT) {
            return pageAddresses.getQuick(frameIndex).getQuick(columnIndex);
        }
        return framePageAddresses.getQuick(columnIndex);
    }

    private long getPageSize(int columnIndex) {
        if (frameFormat == PageFrame.NATIVE_FORMAT) {
            return pageSizes.getQuick(frameIndex).getQuick(columnIndex);
        }
        return framePageSizes.getQuick(columnIndex);
    }

    // TODO: delete once we don't need to copy native frames
    private static class NativeFrameRecord implements Record {

        private final MemoryCR.ByteSequenceView bsview = new MemoryCR.ByteSequenceView();
        private final StableDirectString csviewA = new StableDirectString();
        private final StableDirectString csviewB = new StableDirectString();
        private final Long256Impl long256A = new Long256Impl();
        private final Long256Impl long256B = new Long256Impl();
        private final PageFrameMemoryCache memoryCache;
        private final Utf8SplitString utf8ViewA = new Utf8SplitString(true);
        private final Utf8SplitString utf8ViewB = new Utf8SplitString(true);
        private long rowIndex;

        public NativeFrameRecord(PageFrameMemoryCache memoryCache) {
            this.memoryCache = memoryCache;
        }

        @Override
        public BinarySequence getBin(int columnIndex) {
            final long dataPageAddress = memoryCache.getPageAddress(columnIndex);
            if (dataPageAddress == 0) {
                return NullMemoryMR.INSTANCE.getBin(0);
            }
            final long indexPageAddress = memoryCache.getAuxPageAddress(columnIndex);
            final long offset = Unsafe.getUnsafe().getLong(indexPageAddress + (rowIndex << 3));
            final long pageLimit = memoryCache.getPageSize(columnIndex);
            return getBin(dataPageAddress, offset, pageLimit, bsview);
        }

        @Override
        public long getBinLen(int columnIndex) {
            final long dataPageAddress = memoryCache.getPageAddress(columnIndex);
            if (dataPageAddress == 0) {
                return NullMemoryMR.INSTANCE.getBinLen(0);
            }
            final long indexPageAddress = memoryCache.getAuxPageAddress(columnIndex);
            final long offset = Unsafe.getUnsafe().getLong(indexPageAddress + (rowIndex << 3));
            return Unsafe.getUnsafe().getLong(dataPageAddress + offset);
        }

        @Override
        public boolean getBool(int columnIndex) {
            final long address = memoryCache.getPageAddress(columnIndex);
            if (address == 0) {
                return NullMemoryMR.INSTANCE.getBool(0);
            }
            return Unsafe.getUnsafe().getByte(address + rowIndex) == 1;
        }

        @Override
        public byte getByte(int columnIndex) {
            final long address = memoryCache.getPageAddress(columnIndex);
            if (address == 0) {
                return NullMemoryMR.INSTANCE.getByte(0);
            }
            return Unsafe.getUnsafe().getByte(address + rowIndex);
        }

        @Override
        public char getChar(int columnIndex) {
            final long address = memoryCache.getPageAddress(columnIndex);
            if (address == 0) {
                return NullMemoryMR.INSTANCE.getChar(0);
            }
            return Unsafe.getUnsafe().getChar(address + (rowIndex << 1));
        }

        @Override
        public double getDouble(int columnIndex) {
            final long address = memoryCache.getPageAddress(columnIndex);
            if (address == 0) {
                return NullMemoryMR.INSTANCE.getDouble(0);
            }
            return Unsafe.getUnsafe().getDouble(address + (rowIndex << 3));
        }

        @Override
        public float getFloat(int columnIndex) {
            final long address = memoryCache.getPageAddress(columnIndex);
            if (address == 0) {
                return NullMemoryMR.INSTANCE.getFloat(0);
            }
            return Unsafe.getUnsafe().getFloat(address + (rowIndex << 2));
        }

        @Override
        public byte getGeoByte(int columnIndex) {
            final long address = memoryCache.getPageAddress(columnIndex);
            if (address == 0) {
                return NullMemoryMR.INSTANCE.getByte(0);
            }
            return Unsafe.getUnsafe().getByte(address + rowIndex);
        }

        @Override
        public int getGeoInt(int columnIndex) {
            final long address = memoryCache.getPageAddress(columnIndex);
            if (address == 0) {
                return NullMemoryMR.INSTANCE.getInt(0);
            }
            return Unsafe.getUnsafe().getInt(address + (rowIndex << 2));
        }

        @Override
        public long getGeoLong(int columnIndex) {
            final long address = memoryCache.getPageAddress(columnIndex);
            if (address == 0) {
                return NullMemoryMR.INSTANCE.getLong(0);
            }
            return Unsafe.getUnsafe().getLong(address + (rowIndex << 3));
        }

        @Override
        public short getGeoShort(int columnIndex) {
            final long address = memoryCache.getPageAddress(columnIndex);
            if (address == 0) {
                return NullMemoryMR.INSTANCE.getShort(0);
            }
            return Unsafe.getUnsafe().getShort(address + (rowIndex << 1));
        }

        @Override
        public int getIPv4(int columnIndex) {
            final long address = memoryCache.getPageAddress(columnIndex);
            if (address == 0) {
                return NullMemoryMR.INSTANCE.getIPv4(0);
            }
            return Unsafe.getUnsafe().getInt(address + (rowIndex << 2));
        }

        @Override
        public int getInt(int columnIndex) {
            final long address = memoryCache.getPageAddress(columnIndex);
            if (address == 0) {
                return NullMemoryMR.INSTANCE.getInt(0);
            }
            return Unsafe.getUnsafe().getInt(address + (rowIndex << 2));
        }

        @Override
        public long getLong(int columnIndex) {
            final long address = memoryCache.getPageAddress(columnIndex);
            if (address == 0) {
                return NullMemoryMR.INSTANCE.getLong(0);
            }
            return Unsafe.getUnsafe().getLong(address + (rowIndex << 3));
        }

        @Override
        public long getLong128Hi(int columnIndex) {
            long address = memoryCache.getPageAddress(columnIndex);
            if (address == 0) {
                return NullMemoryMR.INSTANCE.getLong128Hi();
            }
            return Unsafe.getUnsafe().getLong(address + (rowIndex << 4) + Long.BYTES);
        }

        @Override
        public long getLong128Lo(int columnIndex) {
            long address = memoryCache.getPageAddress(columnIndex);
            if (address == 0) {
                return NullMemoryMR.INSTANCE.getLong128Lo();
            }
            return Unsafe.getUnsafe().getLong(address + (rowIndex << 4));
        }

        @Override
        public void getLong256(int columnIndex, CharSink<?> sink) {
            final long address = memoryCache.getPageAddress(columnIndex);
            if (address == 0) {
                NullMemoryMR.INSTANCE.getLong256(0, sink);
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
        public short getShort(int columnIndex) {
            final long address = memoryCache.getPageAddress(columnIndex);
            if (address == 0) {
                return NullMemoryMR.INSTANCE.getShort(0);
            }
            return Unsafe.getUnsafe().getShort(address + (rowIndex << 1));
        }

        @Override
        public CharSequence getStrA(int columnIndex) {
            final long dataPageAddress = memoryCache.getPageAddress(columnIndex);
            if (dataPageAddress == 0) {
                return NullMemoryMR.INSTANCE.getStrA(0);
            }
            final long indexPageAddress = memoryCache.getAuxPageAddress(columnIndex);
            final long offset = Unsafe.getUnsafe().getLong(indexPageAddress + (rowIndex << 3));
            final long size = memoryCache.getPageSize(columnIndex);
            return getStrA(dataPageAddress, offset, size, csviewA);
        }

        @Override
        public CharSequence getStrB(int columnIndex) {
            final long dataPageAddress = memoryCache.getPageAddress(columnIndex);
            if (dataPageAddress == 0) {
                return NullMemoryMR.INSTANCE.getStrB(0);
            }
            final long indexPageAddress = memoryCache.getAuxPageAddress(columnIndex);
            final long offset = Unsafe.getUnsafe().getLong(indexPageAddress + (rowIndex << 3));
            final long size = memoryCache.getPageSize(columnIndex);
            return getStrA(dataPageAddress, offset, size, csviewB);
        }

        @Override
        public int getStrLen(int columnIndex) {
            final long dataPageAddress = memoryCache.getPageAddress(columnIndex);
            if (dataPageAddress == 0) {
                return NullMemoryMR.INSTANCE.getStrLen(0);
            }
            final long indexPageAddress = memoryCache.getAuxPageAddress(columnIndex);
            final long offset = Unsafe.getUnsafe().getLong(indexPageAddress + (rowIndex << 3));
            return Unsafe.getUnsafe().getInt(dataPageAddress + offset);
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
            final long auxPageAddress = memoryCache.getAuxPageAddress(columnIndex);
            if (auxPageAddress == 0) {
                // Column top.
                return TableUtils.NULL_LEN;
            }
            return VarcharTypeDriver.getValueSize(auxPageAddress, rowIndex);
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
            final long columnAddress = memoryCache.getPageAddress(columnIndex);
            if (columnAddress == 0) {
                NullMemoryMR.INSTANCE.getLong256(0, sink);
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

        @Nullable
        private Utf8Sequence getVarchar(int columnIndex, Utf8SplitString utf8View) {
            final long auxPageAddress = memoryCache.getAuxPageAddress(columnIndex);
            if (auxPageAddress == 0) {
                return null; // Column top.
            }
            final long dataPageAddress = memoryCache.getPageAddress(columnIndex);
            return VarcharTypeDriver.getSplitValue(auxPageAddress, dataPageAddress, rowIndex, utf8View);
        }
    }
}
