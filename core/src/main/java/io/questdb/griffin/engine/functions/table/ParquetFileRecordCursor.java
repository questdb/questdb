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

import io.questdb.cairo.DataUnavailableException;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.str.*;
import org.jetbrains.annotations.Nullable;

public class ParquetFileRecordCursor implements NoRandomAccessRecordCursor {
    private final LongList columnChunkBufferPtrs = new LongList();
    private final PartitionDecoder decoder;
    private final RecordMetadata metadata;
    private final Path path;
    private final ParquetRecord record;
    private int currentRowInRowGroup;
    private int rowGroup;
    private long rowGroupRowCount;

    public ParquetFileRecordCursor(FilesFacade ff, Path path, RecordMetadata metadata) {
        this.path = path;
        this.metadata = metadata;
        this.decoder = new PartitionDecoder(ff);
        this.record = new ParquetRecord();
    }

    @Override
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

        return switchToNextRowGroup();
    }

    public void of(SqlExecutionContext executionContext) {
        try {
            // Reopen the file, it could have changed
            decoder.of(path);
            // TODO: compare metadata hasn't changed.
            toTop();
        } catch (DataUnavailableException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long size() throws DataUnavailableException {
        return 0;
    }

    @Override
    public void toTop() {
        rowGroup = -1;
        rowGroupRowCount = -1;
        currentRowInRowGroup = -1;
    }

    private boolean switchToNextRowGroup() {
        columnChunkBufferPtrs.clear();
        if (++rowGroup < decoder.getMetadata().rowGroupCount()) {
            rowGroupRowCount = -1;
            for (int columnId = 0, n = metadata.getColumnCount(); columnId < n; columnId++) {
                long columnChunkBufferPtr = decoder.decodeColumnChunk(rowGroup, columnId, metadata.getColumnType(columnId));
                columnChunkBufferPtrs.add(columnChunkBufferPtr);
                long rowCount = PartitionDecoder.getRowGroupCount(columnChunkBufferPtr);
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
        private final DirectString directCharSequenceA = new DirectString();
        private final DirectString directCharSequenceB = new DirectString();
        private final Utf8SplitString utf8SplitViewA = new Utf8SplitString(false);
        private final Utf8SplitString utf8SplitViewB = new Utf8SplitString(false);

        @Override
        public int getInt(int col) {
            long chunkPtr = columnChunkBufferPtrs.getQuick(col);
            long dataPtr = PartitionDecoder.getChunkDataPtr(chunkPtr);
            return Unsafe.getUnsafe().getInt(dataPtr + currentRowInRowGroup * 4L);
        }

        @Override
        public long getLong(int col) {
            long chunkPtr = columnChunkBufferPtrs.getQuick(col);
            long dataPtr = PartitionDecoder.getChunkDataPtr(chunkPtr);
            return Unsafe.getUnsafe().getLong(dataPtr + currentRowInRowGroup * 8L);
        }

        @Override
        public CharSequence getStrA(int col) {
            long chunkPtr = columnChunkBufferPtrs.getQuick(col);
            long auxPtr = PartitionDecoder.getChunkAuxPtr(chunkPtr);
            long data_offset = Unsafe.getUnsafe().getLong(auxPtr + currentRowInRowGroup * 8L);
            long dataPtr = PartitionDecoder.getChunkDataPtr(chunkPtr);
            return getStr(dataPtr + data_offset, directCharSequenceA);
        }

        @Override
        public CharSequence getStrB(int col) {
            long chunkPtr = columnChunkBufferPtrs.getQuick(col);
            long auxPtr = PartitionDecoder.getChunkAuxPtr(chunkPtr);
            long data_offset = Unsafe.getUnsafe().getLong(auxPtr + currentRowInRowGroup * 8L);
            long dataPtr = PartitionDecoder.getChunkDataPtr(chunkPtr);
            return getStr(dataPtr + data_offset, directCharSequenceB);
        }

        @Override
        public int getStrLen(int col) {
            long chunkPtr = columnChunkBufferPtrs.getQuick(col);
            long auxPtr = PartitionDecoder.getChunkAuxPtr(chunkPtr);
            long data_offset = Unsafe.getUnsafe().getLong(auxPtr + currentRowInRowGroup * 8L);
            long dataPtr = PartitionDecoder.getChunkDataPtr(chunkPtr);
            return Unsafe.getUnsafe().getInt(dataPtr + data_offset);
        }

        @Nullable
        @Override
        public Utf8Sequence getVarcharA(int col) {
            long chunkPtr = columnChunkBufferPtrs.getQuick(col);
            long auxPtr = PartitionDecoder.getChunkAuxPtr(chunkPtr);
            long dataPtr = PartitionDecoder.getChunkDataPtr(chunkPtr);
            return VarcharTypeDriver.getSplitValue(auxPtr, dataPtr, currentRowInRowGroup, utf8SplitViewA);
        }

        @Nullable
        @Override
        public Utf8Sequence getVarcharB(int col) {
            long chunkPtr = columnChunkBufferPtrs.getQuick(col);
            long auxPtr = PartitionDecoder.getChunkAuxPtr(chunkPtr);
            long dataPtr = PartitionDecoder.getChunkDataPtr(chunkPtr);
            return VarcharTypeDriver.getSplitValue(auxPtr, dataPtr, currentRowInRowGroup, utf8SplitViewB);
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
