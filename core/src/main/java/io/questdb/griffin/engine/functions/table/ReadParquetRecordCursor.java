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
import io.questdb.cairo.DataUnavailableException;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import io.questdb.std.*;
import io.questdb.std.str.*;
import org.jetbrains.annotations.Nullable;

public class ReadParquetRecordCursor implements NoRandomAccessRecordCursor {
    private final LongList auxPtrs = new LongList();
    private final LongList columnChunkBufferPtrs = new LongList();
    private final LongList dataPtrs = new LongList();
    private final PartitionDecoder decoder;
    private final RecordMetadata metadata;
    private final ParquetRecord record;
    private int currentRowInRowGroup;
    private int rowGroup;
    private long rowGroupRowCount;

    public ReadParquetRecordCursor(FilesFacade ff, RecordMetadata metadata) {
        this.metadata = metadata;
        this.decoder = new PartitionDecoder(ff);
        this.record = new ParquetRecord();
    }

    public void close() {
        Misc.free(decoder);
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

    public void of(SqlExecutionContext executionContext, LPSZ path) {
        try {
            // Reopen the file, it could have changed
            decoder.of(path);
            assertMetadataSame(metadata, decoder);
            toTop();
        } catch (DataUnavailableException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long size() throws DataUnavailableException {
        return decoder.getMetadata().rowCount();
    }

    @Override
    public void toTop() {
        rowGroup = -1;
        rowGroupRowCount = -1;
        currentRowInRowGroup = -1;
    }

    private void assertMetadataSame(RecordMetadata metadata, PartitionDecoder decoder) {
        if (metadata.getColumnCount() != decoder.getMetadata().columnCount()) {
            throw CairoException.nonCritical().put("parquet file mismatch vs. the schema read earlier");
        }

        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            if (metadata.getColumnType(i) != decoder.getMetadata().getColumnType(i)) {
                throw new RuntimeException("parquet file mismatch vs. the schema read earlier");
            }
        }
    }

    private long getStrAddr(int col) {
        long auxPtr = auxPtrs.get(col);
        long dataPtr = dataPtrs.get(col);
        long data_offset = Unsafe.getUnsafe().getLong(auxPtr + currentRowInRowGroup * 8L);
        return dataPtr + data_offset;
    }

    private boolean switchToNextRowGroup() {
        columnChunkBufferPtrs.clear();
        dataPtrs.clear();
        auxPtrs.clear();
        if (++rowGroup < decoder.getMetadata().rowGroupCount()) {
            rowGroupRowCount = -1;
            for (int columnIndex = 0, n = metadata.getColumnCount(); columnIndex < n; columnIndex++) {
                int columnType = metadata.getColumnType(columnIndex);
                long columnChunkBufferPtr = decoder.decodeColumnChunk(rowGroup, columnIndex, columnType);
                columnChunkBufferPtrs.add(columnChunkBufferPtr);
                dataPtrs.add(PartitionDecoder.getChunkDataPtr(columnChunkBufferPtr));
                auxPtrs.add(PartitionDecoder.getChunkAuxPtr(columnChunkBufferPtr));

                long rowCount = PartitionDecoder.getRowGroupRowCount(columnChunkBufferPtr);
                if (rowGroupRowCount == -1) {
                    rowGroupRowCount = rowCount;
                } else if (rowGroupRowCount != rowCount) {
                    throw new RuntimeException("Row count mismatch");
                }
            }
            currentRowInRowGroup = 0;
            return true;
        }
        return false;
    }

    private class ParquetRecord implements Record {
        private final DirectBinarySequence binarySequence = new DirectBinarySequence();
        private final DirectString directCharSequenceA = new DirectString();
        private final DirectString directCharSequenceB = new DirectString();
        private final Long256Impl long256A = new Long256Impl();
        private final Long256Impl long256B = new Long256Impl();
        private final Utf8SplitString utf8SplitViewA = new Utf8SplitString(false);
        private final Utf8SplitString utf8SplitViewB = new Utf8SplitString(false);

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
