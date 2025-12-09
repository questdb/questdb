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

import io.questdb.cairo.BitmapIndexReader;
import io.questdb.cairo.DataUnavailableException;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.str.LPSZ;
import org.jetbrains.annotations.Nullable;

import static io.questdb.griffin.engine.functions.table.ReadParquetRecordCursor.metadataHasChanged;

/**
 * Page frame cursor for parallel read_parquet() SQL function.
 */
public class ReadParquetPageFrameCursor implements PageFrameCursor {
    private static final Log LOG = LogFactory.getLog(ReadParquetPageFrameCursor.class);
    private final IntList columnIndexes;
    private final PartitionDecoder decoder;
    private final FilesFacade ff;
    private final ReadParquetPageFrame frame = new ReadParquetPageFrame();
    private final RecordMetadata metadata;
    private long addr = 0;
    private long fd = -1;
    private long fileSize = 0;
    private long rowCount;
    private int rowGroupCount;

    public ReadParquetPageFrameCursor(FilesFacade ff, RecordMetadata metadata) {
        this.ff = ff;
        this.metadata = metadata;
        this.decoder = new PartitionDecoder();
        this.columnIndexes = new IntList();
    }

    @Override
    public void calculateSize(RecordCursor.Counter counter) {
        counter.add(rowCount);
    }

    @Override
    public void close() {
        Misc.free(decoder);
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
    public IntList getColumnIndexes() {
        return columnIndexes;
    }

    @Override
    public long getRemainingRowsInInterval() {
        return 0L;
    }

    @Override
    public StaticSymbolTable getSymbolTable(int columnIndex) {
        return null;
    }

    @Override
    public boolean isExternal() {
        return true;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return null;
    }

    @Override
    public @Nullable PageFrame next(long skipTarget) {
        final int rowGroupIndex = ++frame.rowGroupIndex;
        if (rowGroupIndex < rowGroupCount) {
            frame.rowGroupSize = decoder.metadata().getRowGroupSize(rowGroupIndex);
            frame.partitionLo = frame.partitionHi;
            frame.partitionHi = frame.partitionHi + frame.rowGroupSize;
            return frame;
        }
        return null;
    }

    public void of(LPSZ path) {
        // Reopen the file, it could have changed
        this.fd = TableUtils.openRO(ff, path, LOG);
        this.fileSize = ff.length(fd);
        this.addr = TableUtils.mapRO(ff, fd, fileSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
        decoder.of(addr, fileSize, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
        if (metadataHasChanged(metadata, decoder)) {
            // We need to recompile the factory as the Parquet metadata has changed.
            throw TableReferenceOutOfDateException.of(path);
        }

        columnIndexes.clear();
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            columnIndexes.add(i);
        }
        this.rowCount = decoder.metadata().getRowCount();
        this.rowGroupCount = decoder.metadata().getRowGroupCount();

        toTop();
    }

    @Override
    public long size() throws DataUnavailableException {
        return rowCount;
    }

    @Override
    public boolean supportsSizeCalculation() {
        return true;
    }

    @Override
    public void toTop() {
        frame.clear();
    }

    private class ReadParquetPageFrame implements PageFrame, Mutable {
        private long partitionHi;
        private long partitionLo;
        private int rowGroupIndex;
        private int rowGroupSize;

        @Override
        public void clear() {
            rowGroupIndex = -1;
            rowGroupSize = 0;
            partitionLo = 0;
            partitionHi = 0;
        }

        @Override
        public long getAuxPageAddress(int columnIndex) {
            return 0;
        }

        @Override
        public long getAuxPageSize(int columnIndex) {
            return 0;
        }

        @Override
        public BitmapIndexReader getBitmapIndexReader(int columnIndex, int direction) {
            return null;
        }

        @Override
        public int getColumnCount() {
            return columnIndexes.size();
        }

        @Override
        public byte getFormat() {
            return PartitionFormat.PARQUET;
        }

        @Override
        public long getPageAddress(int columnIndex) {
            return 0;
        }

        @Override
        public long getPageSize(int columnIndex) {
            return 0;
        }

        @Override
        public long getParquetAddr() {
            return addr;
        }

        @Override
        public long getParquetFileSize() {
            return fileSize;
        }

        @Override
        public int getParquetRowGroup() {
            return rowGroupIndex;
        }

        @Override
        public int getParquetRowGroupHi() {
            return rowGroupSize;
        }

        @Override
        public int getParquetRowGroupLo() {
            return 0;
        }

        @Override
        public long getPartitionHi() {
            return partitionHi;
        }

        @Override
        public int getPartitionIndex() {
            return 0;
        }

        @Override
        public long getPartitionLo() {
            return partitionLo;
        }
    }
}
