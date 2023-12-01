/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.duckdb;

import io.questdb.cairo.BitmapIndexReader;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.EmptySymbolMapReader;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.Numbers;
import io.questdb.std.Rows;
import org.jetbrains.annotations.Nullable;

public class DuckDBPageFrameCursor implements PageFrameCursor {
    private final DuckDBPageFrame frame = new DuckDBPageFrame();
    private DuckDBDataChunk chunk;
    private DuckDBResult dataset;
    private int chunkIndex;

    public void of(DuckDBPreparedStatement statement) {
        dataset = statement.execute();
    }

    @Override
    public void close() {
        if (chunk != null) {
            chunk.close();
        }
        dataset.close();
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return EmptySymbolMapReader.INSTANCE;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return EmptySymbolMapReader.INSTANCE;
    }

    @Override
    public long getUpdateRowId(long rowIndex) {
        return Rows.toRowID(chunkIndex, rowIndex);
    }

    @Override
    public @Nullable PageFrame next() {
        chunk = dataset.fetchChunk();
        if (chunk != null) {
            chunkIndex++;
            return frame;
        }
        return null;
    }

    @Override
    public long size() {
        return -1L;
    }

    @Override
    public void toTop() {
        chunkIndex = 0;
    }

    private class DuckDBPageFrame implements PageFrame {
        @Override
        public BitmapIndexReader getBitmapIndexReader(int columnIndex, int direction) {
            return null;
        }

        @Override
        public int getColumnShiftBits(int columnIndex) {
            int type = chunk.getColumnType(columnIndex);
            return Numbers.msb(ColumnType.sizeOf(type));
        }

        @Override
        public long getIndexPageAddress(int columnIndex) {
            return 0;
        }

        @Override
        public long getPageAddress(int columnIndex) {
            return chunk.getColumnDataAddr(columnIndex);
        }

        @Override
        public long getPageSize(int columnIndex) {
            return chunk.getRowCount() << getColumnShiftBits(columnIndex);
        }

        @Override
        public long getPartitionHi() {
            return chunk.getRowCount();
        }

        @Override
        public int getPartitionIndex() {
            return chunkIndex;
        }

        @Override
        public long getPartitionLo() {
            return 0;
        }
    }
}
