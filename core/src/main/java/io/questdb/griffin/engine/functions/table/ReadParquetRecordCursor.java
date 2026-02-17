/*******************************************************************************
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

package io.questdb.griffin.engine.functions.table;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.BorrowedArray;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.vm.Vm;
import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.BinarySequence;
import io.questdb.std.DirectBinarySequence;
import io.questdb.std.DirectIntList;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8SplitString;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Page frame cursor for single-threaded read_parquet() SQL function.
 */
public class ReadParquetRecordCursor implements NoRandomAccessRecordCursor {
    private static final Log LOG = LogFactory.getLog(ReadParquetRecordCursor.class);
    private final LongList auxPtrs = new LongList();
    private final DirectIntList columns;
    private final LongList dataPtrs = new LongList();
    private final PartitionDecoder decoder;
    private final FilesFacade ff;
    // doesn't include unsupported columns
    private final RecordMetadata metadata;
    private final ParquetRecord record;
    private final RowGroupBuffers rowGroupBuffers;
    private long addr = 0;
    private int currentRowInRowGroup;
    private long fd = -1;
    private long fileSize = 0;
    private int rowGroupIndex;
    private long rowGroupRowCount;

    public ReadParquetRecordCursor(FilesFacade ff, RecordMetadata metadata) {
        try {
            this.ff = ff;
            this.metadata = metadata;
            this.decoder = new PartitionDecoder();
            this.rowGroupBuffers = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
            this.columns = new DirectIntList(32, MemoryTag.NATIVE_DEFAULT);
            this.record = new ParquetRecord(metadata.getColumnCount());
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    /**
     * Validates that metadata columns can be projected from parquet and optionally populates column mappings.
     *
     * @param columns       if not null, will be populated with (parquetIndex, parquetType) pairs
     * @param columnIndexes if not null, will be populated with metadata column indexes
     * @return true if projection is possible, false otherwise
     */
    public static boolean canProjectMetadata(
            RecordMetadata metadata,
            PartitionDecoder decoder,
            @Nullable DirectIntList columns,
            @Nullable IntList columnIndexes
    ) {
        final PartitionDecoder.Metadata parquetMetadata = decoder.metadata();

        for (int i = 0; i < metadata.getColumnCount(); i++) {
            final int expectedType = metadata.getColumnType(i);
            final CharSequence columnName = metadata.getColumnName(i);
            final int parquetIndex = parquetMetadata.getColumnIndex(columnName);

            if (parquetIndex < 0) {
                return false;
            }

            final int actualType = parquetMetadata.getColumnType(parquetIndex);

            if (ColumnType.isUndefined(actualType)) {
                throw CairoException.nonCritical()
                        .put("could not decode parquet column [name=").put(columnName)
                        .put(", expected=").put(ColumnType.nameOf(expectedType))
                        .put(", actual=").put(ColumnType.nameOf(actualType))
                        .put("]");
            }

            final boolean isSymbolToVarcharConversion = (expectedType == ColumnType.VARCHAR && actualType == ColumnType.SYMBOL);
            if (!isSymbolToVarcharConversion && expectedType != actualType) {
                return false;
            }

            if (columns != null) {
                columns.add(parquetIndex);
                columns.add(actualType);
            }
            if (columnIndexes != null) {
                columnIndexes.add(parquetIndex);
            }
        }

        return true;
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
        PartitionDecoder.Metadata meta = decoder.metadata();
        if (rowGroupIndex < 0) {
            counter.add(meta.getRowCount());
        } else {
            int rgCount = meta.getRowGroupCount();
            long remaining = rowGroupRowCount - currentRowInRowGroup - 1;
            counter.add(remaining);
            for (int i = rowGroupIndex + 1; i < rgCount; i++) {
                counter.add(meta.getRowGroupSize(i));
            }
        }

        // move cursor to the end
        rowGroupIndex = meta.getRowGroupCount();
        rowGroupRowCount = 0;
        currentRowInRowGroup = 0;
    }

    @Override
    public void close() {
        Misc.free(decoder);
        Misc.free(rowGroupBuffers);
        Misc.free(columns);
        Misc.free(record);
        if (fd != -1) {
            ff.close(fd);
            fd = -1;
        }
        if (addr != 0) {
            ff.munmap(addr, fileSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            addr = 0;
        }
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public boolean hasNext() {
        if (++currentRowInRowGroup < rowGroupRowCount) {
            return true;
        }

        try {
            return switchToNextRowGroup();
        } catch (CairoException ex) {
            throw CairoException.nonCritical().put("Error reading. Parquet file is likely corrupted");
        }
    }

    public void of(LPSZ path) {
        // Reopen the file, it could have changed
        this.fd = TableUtils.openRO(ff, path, LOG);
        this.fileSize = ff.length(fd);
        this.addr = TableUtils.mapRO(ff, fd, fileSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
        decoder.of(addr, fileSize, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
        rowGroupBuffers.reopen();
        columns.reopen();
        columns.clear();
        int n = metadata.getColumnCount();
        if (n > 0) {
            columns.setCapacity(2L * n);
            if (!canProjectMetadata(metadata, decoder, columns, null)) {
                // We need to recompile the factory as the Parquet metadata has changed.
                throw TableReferenceOutOfDateException.of(path);
            }
        }

        toTop();
    }

    @Override
    public long preComputedStateSize() {
        return 0;
    }

    @Override
    public long size() {
        return decoder.metadata().getRowCount();
    }

    @Override
    public void skipRows(Counter rowCount) {
        long toSkip = rowCount.get();

        while (toSkip > 0) {
            if (currentRowInRowGroup + 1 >= rowGroupRowCount) {
                try {
                    if (!switchToNextRowGroup()) {
                        return;
                    }
                } catch (CairoException ex) {
                    throw CairoException.nonCritical().put("Error reading. Parquet file is likely corrupted");
                }
                toSkip--;
                rowCount.dec();
                if (toSkip == 0) {
                    return;
                }
            }

            long availableToSkip = rowGroupRowCount - currentRowInRowGroup - 1;
            long skipNow = Math.min(toSkip, availableToSkip);
            currentRowInRowGroup += (int) skipNow;
            toSkip -= skipNow;
            rowCount.dec(skipNow);
        }
    }

    @Override
    public void toTop() {
        rowGroupIndex = -1;
        rowGroupRowCount = -1;
        currentRowInRowGroup = -1;
    }

    private long getStrAddr(int col) {
        long auxPtr = auxPtrs.get(col);
        long dataPtr = dataPtrs.get(col);
        long dataOffset = Unsafe.getUnsafe().getLong(auxPtr + currentRowInRowGroup * 8L);
        return dataPtr + dataOffset;
    }

    private boolean switchToNextRowGroup() {
        dataPtrs.clear();
        auxPtrs.clear();
        if (++rowGroupIndex < decoder.metadata().getRowGroupCount()) {
            final int rowGroupSize = decoder.metadata().getRowGroupSize(rowGroupIndex);
            rowGroupRowCount = decoder.decodeRowGroup(rowGroupBuffers, columns, rowGroupIndex, 0, rowGroupSize);

            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                dataPtrs.add(rowGroupBuffers.getChunkDataPtr(i));
                auxPtrs.add(rowGroupBuffers.getChunkAuxPtr(i));
            }
            currentRowInRowGroup = 0;
            return true;
        }
        return false;
    }

    private class ParquetRecord implements Record, QuietCloseable {
        private final ObjList<BorrowedArray> arrayBuffers;
        private final ObjList<DirectBinarySequence> bsViews;
        private final ObjList<DirectString> csViewsA;
        private final ObjList<DirectString> csViewsB;
        private final ObjList<Long256Impl> longs256A;
        private final ObjList<Long256Impl> longs256B;
        private final ObjList<Utf8SplitString> utf8ViewsA;
        private final ObjList<Utf8SplitString> utf8ViewsB;

        public ParquetRecord(int columnCount) {
            this.bsViews = new ObjList<>(columnCount);
            this.csViewsA = new ObjList<>(columnCount);
            this.csViewsB = new ObjList<>(columnCount);
            this.longs256A = new ObjList<>(columnCount);
            this.longs256B = new ObjList<>(columnCount);
            this.utf8ViewsA = new ObjList<>(columnCount);
            this.utf8ViewsB = new ObjList<>(columnCount);
            this.arrayBuffers = new ObjList<>(columnCount);
        }

        @Override
        public void close() {
            Misc.freeObjList(arrayBuffers);
        }

        @Override
        public ArrayView getArray(int col, int colType) {
            final BorrowedArray array = borrowedArray(col);
            final long auxPageAddress = auxPtrs.getQuick(col);
            if (auxPageAddress != 0) {
                final long dataPageAddress = dataPtrs.getQuick(col);
                array.of(
                        colType,
                        auxPageAddress,
                        Long.MAX_VALUE,
                        dataPageAddress,
                        Long.MAX_VALUE,
                        currentRowInRowGroup
                );
            } else {
                array.ofNull();
            }
            return array;
        }

        @Override
        public BinarySequence getBin(int col) {
            long auxPtr = auxPtrs.get(col);
            long dataPtr = dataPtrs.get(col);
            long dataOffset = Unsafe.getUnsafe().getLong(auxPtr + currentRowInRowGroup * 8L);
            long len = Unsafe.getUnsafe().getLong(dataPtr + dataOffset);
            if (len != TableUtils.NULL_LEN) {
                return bsView(col).of(dataPtr + dataOffset + 8L, len);
            }
            return null;
        }

        @Override
        public long getBinLen(int col) {
            long auxPtr = auxPtrs.get(col);
            long dataPtr = dataPtrs.get(col);
            long dataOffset = Unsafe.getUnsafe().getLong(auxPtr + currentRowInRowGroup * 8L);
            return Unsafe.getUnsafe().getLong(dataPtr + dataOffset);
        }

        @Override
        public boolean getBool(int col) {
            return getByte(col) == 1;
        }

        @Override
        public byte getByte(int col) {
            long dataPtr = dataPtrs.get(col);
            return Unsafe.getUnsafe().getByte(dataPtr + currentRowInRowGroup);
        }

        @Override
        public char getChar(int col) {
            long dataPtr = dataPtrs.get(col);
            return Unsafe.getUnsafe().getChar(dataPtr + currentRowInRowGroup * 2L);
        }

        @Override
        public double getDouble(int col) {
            long dataPtr = dataPtrs.get(col);
            return Unsafe.getUnsafe().getDouble(dataPtr + currentRowInRowGroup * 8L);
        }

        @Override
        public float getFloat(int col) {
            long dataPtr = dataPtrs.get(col);
            return Unsafe.getUnsafe().getFloat(dataPtr + currentRowInRowGroup * 4L);
        }

        @Override
        public byte getGeoByte(int col) {
            return getByte(col);
        }

        @Override
        public int getGeoInt(int col) {
            return getInt(col);
        }

        @Override
        public long getGeoLong(int col) {
            return getLong(col);
        }

        @Override
        public short getGeoShort(int col) {
            return getShort(col);
        }

        @Override
        public int getIPv4(int col) {
            return getInt(col);
        }

        @Override
        public int getInt(int col) {
            long dataPtr = dataPtrs.get(col);
            return Unsafe.getUnsafe().getInt(dataPtr + currentRowInRowGroup * 4L);
        }

        @Override
        public long getLong(int col) {
            long dataPtr = dataPtrs.get(col);
            return Unsafe.getUnsafe().getLong(dataPtr + currentRowInRowGroup * 8L);
        }

        @Override
        public long getLong128Hi(int col) {
            long dataPtr = dataPtrs.get(col);
            return Unsafe.getUnsafe().getLong(dataPtr + currentRowInRowGroup * 16L + 8);
        }

        @Override
        public long getLong128Lo(int col) {
            long dataPtr = dataPtrs.get(col);
            return Unsafe.getUnsafe().getLong(dataPtr + currentRowInRowGroup * 16L);
        }

        @Override
        public void getLong256(int col, CharSink<?> sink) {
            Numbers.appendLong256FromUnsafe(getLong256Addr(col), sink);
        }

        @Override
        public Long256 getLong256A(int col) {
            final Long256Impl long256 = long256A(col);
            long256.fromAddress(getLong256Addr(col));
            return long256;
        }

        @Override
        public Long256 getLong256B(int col) {
            final Long256Impl long256 = long256B(col);
            long256.fromAddress(getLong256Addr(col));
            return long256;
        }

        @Override
        public short getShort(int col) {
            long dataPtr = dataPtrs.get(col);
            return Unsafe.getUnsafe().getShort(dataPtr + currentRowInRowGroup * 2L);
        }

        @Override
        public CharSequence getStrA(int col) {
            return getStr(getStrAddr(col), csViewA(col));
        }

        @Override
        public CharSequence getStrB(int col) {
            return getStr(getStrAddr(col), csViewB(col));
        }

        @Override
        public int getStrLen(int col) {
            return Unsafe.getUnsafe().getInt(getStrAddr(col));
        }

        @Nullable
        @Override
        public Utf8Sequence getVarcharA(int col) {
            long auxPtr = auxPtrs.get(col);
            long dataPtr = dataPtrs.get(col);
            return VarcharTypeDriver.getSplitValue(auxPtr, Long.MAX_VALUE, dataPtr, Long.MAX_VALUE, currentRowInRowGroup, utf8ViewA(col));
        }

        @Nullable
        @Override
        public Utf8Sequence getVarcharB(int col) {
            long auxPtr = auxPtrs.get(col);
            long dataPtr = dataPtrs.get(col);
            return VarcharTypeDriver.getSplitValue(auxPtr, Long.MAX_VALUE, dataPtr, Long.MAX_VALUE, currentRowInRowGroup, utf8ViewB(col));
        }

        @Override
        public int getVarcharSize(int col) {
            long auxPtr = auxPtrs.get(col);
            return VarcharTypeDriver.getValueSize(auxPtr, currentRowInRowGroup);
        }

        private @NotNull BorrowedArray borrowedArray(int col) {
            BorrowedArray array = arrayBuffers.getQuiet(col);
            if (array != null) {
                return array;
            }
            arrayBuffers.extendAndSet(col, array = new BorrowedArray());
            return array;
        }

        private DirectBinarySequence bsView(int columnIndex) {
            if (bsViews.getQuiet(columnIndex) == null) {
                bsViews.extendAndSet(columnIndex, new DirectBinarySequence());
            }
            return bsViews.getQuick(columnIndex);
        }

        private DirectString csViewA(int columnIndex) {
            if (csViewsA.getQuiet(columnIndex) == null) {
                csViewsA.extendAndSet(columnIndex, new DirectString());
            }
            return csViewsA.getQuick(columnIndex);
        }

        private DirectString csViewB(int columnIndex) {
            if (csViewsB.getQuiet(columnIndex) == null) {
                csViewsB.extendAndSet(columnIndex, new DirectString());
            }
            return csViewsB.getQuick(columnIndex);
        }

        private long getLong256Addr(int col) {
            return dataPtrs.get(col) + (long) currentRowInRowGroup * Long256.BYTES;
        }

        private DirectString getStr(long addr, DirectString view) {
            assert addr > 0;
            final int len = Unsafe.getUnsafe().getInt(addr);
            if (len != TableUtils.NULL_LEN) {
                return view.of(addr + Vm.STRING_LENGTH_BYTES, len);
            }
            return null;
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
                utf8ViewsA.extendAndSet(columnIndex, new Utf8SplitString());
            }
            return utf8ViewsA.getQuick(columnIndex);
        }

        private Utf8SplitString utf8ViewB(int columnIndex) {
            if (utf8ViewsB.getQuiet(columnIndex) == null) {
                utf8ViewsB.extendAndSet(columnIndex, new Utf8SplitString());
            }
            return utf8ViewsB.getQuick(columnIndex);
        }
    }
}
