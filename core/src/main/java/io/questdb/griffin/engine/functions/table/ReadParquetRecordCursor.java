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

package io.questdb.griffin.engine.functions.table;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DataUnavailableException;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.vm.Vm;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.BinarySequence;
import io.questdb.std.Chars;
import io.questdb.std.DirectBinarySequence;
import io.questdb.std.DirectIntList;
import io.questdb.std.FilesFacade;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8SplitString;
import org.jetbrains.annotations.Nullable;

public class ReadParquetRecordCursor implements NoRandomAccessRecordCursor {
    private static final Log LOG = LogFactory.getLog(ReadParquetRecordCursor.class);
    private final LongList auxPtrs = new LongList();
    private final DirectIntList columns;
    private final LongList dataPtrs = new LongList();
    private final PartitionDecoder decoder;
    private final FilesFacade ff;
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
            this.record = new ParquetRecord();
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void close() {
        Misc.free(decoder);
        Misc.free(rowGroupBuffers);
        Misc.free(columns);
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
    public boolean hasNext() throws DataUnavailableException {
        if (++currentRowInRowGroup < rowGroupRowCount) {
            return true;
        }

        try {
            return switchToNextRowGroup();
        } catch (CairoException ex) {
            throw CairoException.nonCritical().put("Error reading. Parquet file is likely corrupted");
        }
    }

    public void of(LPSZ path) throws SqlException {
        try {
            // Reopen the file, it could have changed
            this.fd = TableUtils.openRO(ff, path, LOG);
            this.fileSize = ff.length(fd);
            this.addr = TableUtils.mapRO(ff, fd, fileSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            decoder.of(addr, fileSize, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
            if (metadataHasChanged(metadata, decoder)) {
                // We need to recompile the factory as the Parquet metadata has changed.
                throw TableReferenceOutOfDateException.of(path);
            }
            rowGroupBuffers.reopen();
            columns.reopen();
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                columns.add(i);
                columns.add(metadata.getColumnType(i));
            }
            toTop();
        } catch (DataUnavailableException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long size() throws DataUnavailableException {
        return decoder.metadata().rowCount();
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

    private boolean metadataHasChanged(RecordMetadata metadata, PartitionDecoder decoder) {
        if (metadata.getColumnCount() != decoder.metadata().columnCount()) {
            return true;
        }
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            final int metadataType = metadata.getColumnType(i);
            final int decoderType = decoder.metadata().getColumnType(i);

            boolean remappingDetected = symbolToVarcharRemappingDetected(metadataType, decoderType);
            if (remappingDetected) {
                continue;
            }
            if (metadata.getColumnType(i) != decoder.metadata().getColumnType(i)) {
                return true;
            }
        }
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            if (!Chars.equals(metadata.getColumnName(i), decoder.metadata().columnName(i))) {
                return true;
            }
        }
        return false;
    }

    private boolean switchToNextRowGroup() {
        dataPtrs.clear();
        auxPtrs.clear();
        if (++rowGroupIndex < decoder.metadata().rowGroupCount()) {
            final int rowGroupSize = decoder.metadata().rowGroupSize(rowGroupIndex);
            rowGroupRowCount = decoder.decodeRowGroup(rowGroupBuffers, columns, rowGroupIndex, 0, rowGroupSize);

            for (int columnIndex = 0, n = metadata.getColumnCount(); columnIndex < n; columnIndex++) {
                dataPtrs.add(rowGroupBuffers.getChunkDataPtr(columnIndex));
                auxPtrs.add(rowGroupBuffers.getChunkAuxPtr(columnIndex));
            }
            currentRowInRowGroup = 0;
            return true;
        }
        return false;
    }

    private boolean symbolToVarcharRemappingDetected(int metadataType, int decoderType) {
        return metadataType == ColumnType.VARCHAR && decoderType == ColumnType.SYMBOL;
    }

    private class ParquetRecord implements Record {
        private final DirectBinarySequence binarySequence = new DirectBinarySequence();
        private final DirectString directCharSequenceA = new DirectString();
        private final DirectString directCharSequenceB = new DirectString();
        private final Long256Impl long256A = new Long256Impl();
        private final Long256Impl long256B = new Long256Impl();
        private final Utf8SplitString utf8SplitViewA = new Utf8SplitString();
        private final Utf8SplitString utf8SplitViewB = new Utf8SplitString();

        @Override
        public BinarySequence getBin(int col) {
            long auxPtr = auxPtrs.get(col);
            long dataPtr = dataPtrs.get(col);
            long data_offset = Unsafe.getUnsafe().getLong(auxPtr + currentRowInRowGroup * 8L);
            long len = Unsafe.getUnsafe().getLong(dataPtr + data_offset);
            if (len != TableUtils.NULL_LEN) {
                binarySequence.of(dataPtr + data_offset + 8L, len);
                return binarySequence;
            }
            return null;
        }

        @Override
        public long getBinLen(int col) {
            long auxPtr = auxPtrs.get(col);
            long dataPtr = dataPtrs.get(col);
            long data_offset = Unsafe.getUnsafe().getLong(auxPtr + currentRowInRowGroup * 8L);
            return Unsafe.getUnsafe().getLong(dataPtr + data_offset);
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
            long dataPtr = dataPtrs.get(col);
            long offset = (long) currentRowInRowGroup * Long256.BYTES;
            final long a = Unsafe.getUnsafe().getLong(dataPtr + offset);
            final long b = Unsafe.getUnsafe().getLong(dataPtr + offset + Long.BYTES);
            final long c = Unsafe.getUnsafe().getLong(dataPtr + offset + Long.BYTES * 2);
            final long d = Unsafe.getUnsafe().getLong(dataPtr + offset + Long.BYTES * 3);
            Numbers.appendLong256(a, b, c, d, sink);
        }

        @Override
        public Long256 getLong256A(int col) {
            long dataPtr = dataPtrs.get(col);
            long offset = (long) currentRowInRowGroup * Long256.BYTES;
            final long a = Unsafe.getUnsafe().getLong(dataPtr + offset);
            final long b = Unsafe.getUnsafe().getLong(dataPtr + offset + Long.BYTES);
            final long c = Unsafe.getUnsafe().getLong(dataPtr + offset + Long.BYTES * 2);
            final long d = Unsafe.getUnsafe().getLong(dataPtr + offset + Long.BYTES * 3);
            long256A.setAll(a, b, c, d);
            return long256A;
        }

        @Override
        public Long256 getLong256B(int col) {
            long dataPtr = dataPtrs.get(col);
            long offset = (long) currentRowInRowGroup * Long256.BYTES;
            final long a = Unsafe.getUnsafe().getLong(dataPtr + offset);
            final long b = Unsafe.getUnsafe().getLong(dataPtr + offset + Long.BYTES);
            final long c = Unsafe.getUnsafe().getLong(dataPtr + offset + Long.BYTES * 2);
            final long d = Unsafe.getUnsafe().getLong(dataPtr + offset + Long.BYTES * 3);
            long256B.setAll(a, b, c, d);
            return long256B;
        }

        @Override
        public short getShort(int col) {
            long dataPtr = dataPtrs.get(col);
            return Unsafe.getUnsafe().getShort(dataPtr + currentRowInRowGroup * 2L);
        }

        @Override
        public CharSequence getStrA(int col) {
            return getStr(getStrAddr(col), directCharSequenceA);
        }

        @Override
        public CharSequence getStrB(int col) {
            return getStr(getStrAddr(col), directCharSequenceB);
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
            return VarcharTypeDriver.getSplitValue(auxPtr, Long.MAX_VALUE, dataPtr, Long.MAX_VALUE, currentRowInRowGroup, utf8SplitViewA);
        }

        @Nullable
        @Override
        public Utf8Sequence getVarcharB(int col) {
            long auxPtr = auxPtrs.get(col);
            long dataPtr = dataPtrs.get(col);
            return VarcharTypeDriver.getSplitValue(auxPtr, Long.MAX_VALUE, dataPtr, Long.MAX_VALUE, currentRowInRowGroup, utf8SplitViewB);
        }

        @Override
        public int getVarcharSize(int col) {
            long auxPtr = auxPtrs.get(col);
            return VarcharTypeDriver.getValueSize(auxPtr, currentRowInRowGroup);
        }

        private DirectString getStr(long addr, DirectString view) {
            assert addr > 0;
            final int len = Unsafe.getUnsafe().getInt(addr);
            if (len != TableUtils.NULL_LEN) {
                return view.of(addr + Vm.STRING_LENGTH_BYTES, len);
            }
            return null;
        }
    }
}
