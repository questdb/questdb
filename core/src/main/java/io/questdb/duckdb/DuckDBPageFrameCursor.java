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
    private final PageFrameImpl frame = new PageFrameImpl();
    private long chunk;
    private long queryResult;
    private int chunkIndex;

    // this owns queryResult and is responsible for its destruction
    public void of(long queryResult) {
        if (this.queryResult != 0) {
            DuckDB.resultDestroy(this.queryResult);
        }
        this.queryResult = queryResult;
    }

    @Override
    public void close() {
        if (chunk != 0) {
            DuckDB.dataChunkDestroy(chunk);
        }
        if (queryResult != 0) {
            DuckDB.resultDestroy(queryResult);
            queryResult = 0;
        }
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
        if (chunk != 0) {
            DuckDB.dataChunkDestroy(chunk);
        }
        chunk = DuckDB.resultFetchChunk(queryResult);
        if (chunk != 0) {
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

    private class PageFrameImpl implements PageFrame {
        @Override
        public BitmapIndexReader getBitmapIndexReader(int columnIndex, int direction) {
            return null;
        }

        @Override
        public int getColumnShiftBits(int columnIndex) {
            long duckTypes = DuckDB.resultColumnTypes(queryResult, columnIndex);
            int logicalTypeId = DuckDB.decodeLogicalTypeId(duckTypes);
            int questType = DuckDB.getQdbColumnType(logicalTypeId);
            return Numbers.msb(ColumnType.sizeOf(questType));
        }

        @Override
        public long getIndexPageAddress(int columnIndex) {
            return 0;
        }

        @Override
        public long getPageAddress(int columnIndex) {
            long vec = DuckDB.dataChunkGetVector(chunk, columnIndex);
            return DuckDB.vectorGetData(vec);
        }

        @Override
        public long getPageSize(int columnIndex) {
            return DuckDB.dataChunkGetSize(chunk) << getColumnShiftBits(columnIndex);
        }

        @Override
        public long getPartitionHi() {
            return DuckDB.dataChunkGetSize(chunk);
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
